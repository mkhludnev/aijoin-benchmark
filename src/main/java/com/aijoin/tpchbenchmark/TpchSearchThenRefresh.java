package com.aijoin.tpchbenchmark;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.apache.solr.client.solrj.jetty.CloudJettySolrClient;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * TPC-H Q4 under refresh: each round applies one RF1+RF2 pair ({@link TpchRefresh}), untimed, and
 * then runs Q4 through every arm, so what gets measured is what a commit costs the searches that
 * follow it -- {@code {!aijoin}} rebuilds its join index per searcher, stock joins have nothing to
 * rebuild.
 *
 * <p>Per round and arm, at concurrency 1 so the cold cost lands on exactly one query:
 *
 * <ul>
 *   <li>query 0 is <b>cold</b>, the first search on the fresh searcher;
 *   <li>queries 1..n-1 are <b>warm</b>;
 *   <li>query 0 once more at the end is its <b>twin</b>: the same Q4 instance, warm, so cold minus
 *       twin is the commit penalty with query difficulty cancelled out.
 * </ul>
 *
 * <p>The spread of cold costs comes from the number of rounds (one cold sample each), not from
 * queries per round. The query set is seeded {@code RANDOM_SEED + round}, so every arm runs the same
 * queries in a round; the arm order is shuffled per round from a fixed seed. The index is {@link
 * TpchRefresh#reset reset} to the base state first, so separate runs see identical data per round.
 *
 * <p>Output goes to {@code reports/<yyMMdd-HHmm>-tpchq4-<arms>-<repeat>/}: {@code args.txt}, one
 * {@code searchrefresh-<arm>.csv} per arm, and {@code summary.txt}.
 *
 * <p>Usage:
 *
 * <pre>
 *   tpchSearchThenRefresh --solr-url=&lt;url&gt; [--parsers=aijoin-flag,joinnum-flag] [--query-count=20]
 *       [--first-round-query-count=&lt;n&gt;] [--repeat=30] [--tbl-dir=build/tpch-sf1]
 *       [--refresh-dir=&lt;tbl-dir&gt;/refresh] [--no-reset]
 *   tpchSearchThenRefresh --summarize=reports/&lt;run&gt; [--from-round=2]
 * </pre>
 */
public class TpchSearchThenRefresh {

  private static final Set<String> OPTIONS =
      Set.of("solr-url", "parsers", "query-count", "first-round-query-count", "repeat", "tbl-dir",
          "refresh-dir", "summarize", "from-round");
  private static final Set<String> FLAGS = Set.of("no-reset");

  static final String COLD = "cold";
  static final String WARM = "warm";
  static final String TWIN = "twin";

  public static void main(String[] args) throws Exception {
    Map<String, String> opts = parseOptions(args);
    int fromRound = Integer.parseInt(opts.getOrDefault("from-round", "2"));
    if (opts.containsKey("summarize")) {
      System.out.print(summarize(Path.of(opts.get("summarize")), fromRound));
      return;
    }
    if (!opts.containsKey("solr-url")) {
      usage();
    }
    String solrUrl = opts.get("solr-url");
    List<String> parsers =
        new ArrayList<>(Arrays.asList(opts.getOrDefault("parsers", "aijoin-flag,joinnum-flag").split(",")));
    int queryCount = Integer.parseInt(opts.getOrDefault("query-count", "20"));
    int firstRoundQueryCount =
        Integer.parseInt(opts.getOrDefault("first-round-query-count", String.valueOf(queryCount)));
    int repeat = Integer.parseInt(opts.getOrDefault("repeat", "30"));
    Path tblDir = Path.of(opts.getOrDefault("tbl-dir", "build/tpch-sf1"));
    Path refreshDir = Path.of(opts.getOrDefault("refresh-dir", tblDir.resolve("refresh").toString()));
    parsers.forEach(Q4Query::joinLocalParams); // fail fast on a bad arm name
    if (queryCount < 2 || firstRoundQueryCount < 2 || repeat < 1) {
      throw new IllegalArgumentException("query counts must be >= 2 (one cold, then warm), repeat >= 1");
    }
    int pairs = TpchRefresh.availablePairs(refreshDir);
    if (pairs < repeat) {
      throw new IllegalArgumentException(String.format(
          "%s holds %d refresh pairs, --repeat=%d needs one per round: run dbgen -s 1 -U %d there",
          refreshDir, pairs, repeat, repeat));
    }

    Path reportDir = Path.of("reports", String.format("%s-tpchq4-%s-%d",
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyMMdd-HHmm")),
        String.join("_", parsers), repeat));
    Files.createDirectories(reportDir);
    Files.writeString(reportDir.resolve("args.txt"), String.join(" ", args) + System.lineSeparator());
    System.out.println("Reporting to " + reportDir);

    Random order = new Random(TpchConstants.RANDOM_SEED);
    try (CloudJettySolrClient client = new CloudJettySolrClient.Builder(List.of(solrUrl)).build()) {
      if (!opts.containsKey("no-reset")) {
        TpchRefresh.reset(client, tblDir, refreshDir);
        checkOrderCount(client);
      }
      for (int round = 1; round <= repeat; round++) {
        TpchRefresh.apply(client, refreshDir, round);
        checkOrderCount(client);

        Collections.shuffle(parsers, order);
        int n = round == 1 ? firstRoundQueryCount : queryCount;
        List<Q4Query> queries = TpchSearcher.generateQueries(n, TpchConstants.RANDOM_SEED + round);
        for (String parser : parsers) {
          List<TpchSearcher.Result> results = new ArrayList<>(n + 1);
          for (int i = 0; i < n; i++) {
            results.add(TpchSearcher.runOneQuery(client, parser, i, queries.get(i)));
          }
          results.add(TpchSearcher.runOneQuery(client, parser, n, queries.get(0)));
          TpchSearcher.writeCsv(csv(reportDir, parser), round, results,
              r -> r.index() == 0 ? COLD : r.index() == n ? TWIN : WARM, true);

          int[] warm = results.subList(1, n).stream().mapToInt(TpchSearcher.Result::qTimeMs).sorted().toArray();
          System.out.printf("round %2d/%d %-13s cold=%5d ms  warm p50=%4d max=%4d  twin=%4d  numFound=%d%n",
              round, repeat, parser, results.get(0).qTimeMs(), warm[warm.length / 2],
              warm[warm.length - 1], results.get(n).qTimeMs(), results.get(0).numFound());
        }
      }
    }
    String summary = summarize(reportDir, fromRound);
    Files.writeString(reportDir.resolve("summary.txt"), summary);
    System.out.println();
    System.out.print(summary);
  }

  /**
   * The refresh keeps ORDERS at exactly its base size (1500 in, 1500 out); a wrong count means a
   * pair was applied twice, partly, or onto a dirty index.
   */
  private static void checkOrderCount(CloudJettySolrClient client) throws Exception {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("rows", 0);
    long found = client.query(TpchConstants.ORDERS_COLLECTION, params).getResults().getNumFound();
    if (found != TpchConstants.SF1_ORDERS) {
      throw new IllegalStateException(String.format(
          "tpch_orders holds %,d docs, expected %,d -- reset or reindex", found, TpchConstants.SF1_ORDERS));
    }
  }

  private static Path csv(Path reportDir, String parser) {
    return reportDir.resolve("searchrefresh-" + parser + ".csv");
  }

  /** One CSV row, as far as the summary needs it. */
  private record Row(int round, int index, int qTimeMs, String counts, String error, String kind) {}

  /**
   * Summarises every {@code searchrefresh-*.csv} under {@code reportDir} from {@code fromRound} on:
   * cold (query 0 per round), warm (median of the round's warm queries), and the commit penalty
   * cold minus twin -- each as a distribution across rounds -- plus the per-round trend. Also checks
   * that all arms, and each cold/twin pair, agree on the per-priority counts.
   */
  static String summarize(Path reportDir, int fromRound) throws IOException {
    Map<String, List<Row>> byArm = new TreeMap<>();
    try (Stream<Path> files = Files.list(reportDir)) {
      for (Path f : files.filter(p -> p.getFileName().toString().matches("searchrefresh-.+\\.csv")).sorted().toList()) {
        String arm = f.getFileName().toString().replaceAll("^searchrefresh-|\\.csv$", "");
        byArm.put(arm, readRows(f));
      }
    }
    StringBuilder out = new StringBuilder();
    out.append(String.format("TPC-H Q4 search-then-refresh: %s, rounds >= %d, QTime in ms%n%n", reportDir, fromRound));
    out.append(String.format("%-13s %6s | %5s %5s %5s | %5s %5s | %7s %7s | %9s %9s%n",
        "arm", "rounds", "cold", "p90", "max", "warm", "p90", "penalty", "p90", "warm/rd", "cold/rd"));
    out.append(String.format("%-13s %6s | %5s %5s %5s | %5s %5s | %7s %7s | %9s %9s%n",
        "", "", "p50", "", "", "p50", "", "p50", "", "trend", "trend"));

    int mismatches = 0;
    int errors = 0;
    Map<String, String> reference = new HashMap<>();
    for (Map.Entry<String, List<Row>> e : byArm.entrySet()) {
      Map<Integer, List<Row>> rounds = new TreeMap<>();
      for (Row r : e.getValue()) {
        if (r.error() != null) {
          errors++;
          continue;
        }
        // every arm must agree on every query; the twin re-runs query 0, so it must agree with it too
        String key = r.round() + "/" + (r.kind().equals(TWIN) ? 0 : r.index());
        String seen = reference.putIfAbsent(key, r.counts());
        if (seen != null && !seen.equals(r.counts())) {
          mismatches++;
        }
        if (r.round() >= fromRound) {
          rounds.computeIfAbsent(r.round(), k -> new ArrayList<>()).add(r);
        }
      }
      List<Double> cold = new ArrayList<>();
      List<Double> warm = new ArrayList<>();
      List<Double> warmAll = new ArrayList<>();
      List<Double> penalty = new ArrayList<>();
      List<Double> roundNos = new ArrayList<>();
      for (Map.Entry<Integer, List<Row>> rd : rounds.entrySet()) {
        Row c = rd.getValue().stream().filter(r -> r.kind().equals(COLD)).findFirst().orElse(null);
        Row t = rd.getValue().stream().filter(r -> r.kind().equals(TWIN)).findFirst().orElse(null);
        List<Double> w = rd.getValue().stream().filter(r -> r.kind().equals(WARM))
            .map(r -> (double) r.qTimeMs()).sorted().toList();
        if (c == null || t == null || w.isEmpty()) {
          continue;
        }
        roundNos.add((double) rd.getKey());
        cold.add((double) c.qTimeMs());
        warm.add(percentile(w, 50));
        warmAll.addAll(w);
        penalty.add((double) (c.qTimeMs() - t.qTimeMs()));
      }
      if (cold.isEmpty()) {
        continue;
      }
      out.append(String.format("%-13s %6d | %5.0f %5.0f %5.0f | %5.0f %5.0f | %7.0f %7.0f | %8.2f%% %8.2f%%%n",
          e.getKey(), cold.size(),
          percentile(sorted(cold), 50), percentile(sorted(cold), 90), percentile(sorted(cold), 100),
          percentile(sorted(warm), 50), percentile(sorted(warmAll), 90),
          percentile(sorted(penalty), 50), percentile(sorted(penalty), 90),
          relativeSlope(roundNos, warm), relativeSlope(roundNos, cold)));
    }
    out.append(String.format("%ncold    = query 0 of a round, the first search after the refresh commit%n"));
    out.append(String.format("warm    = median of the round's queries 1..n-1 (p90 over all warm queries)%n"));
    out.append(String.format("penalty = cold minus its twin (query 0 re-run warm at the round's end)%n"));
    out.append(String.format("trend   = OLS slope of the per-round value, %% of its mean per round%n"));
    out.append(String.format("%nresult check: %d per-priority count mismatches across arms and cold/twin pairs, %d errors%n",
        mismatches, errors));
    return out.toString();
  }

  private static List<Row> readRows(Path csv) throws IOException {
    List<String> lines = Files.readAllLines(csv, StandardCharsets.UTF_8);
    List<String> header = List.of(lines.get(0).split(",", -1));
    List<Row> rows = new ArrayList<>();
    for (String line : lines.subList(1, lines.size())) {
      String[] f = line.split(",", -1);
      String error = f[header.indexOf("error")];
      rows.add(new Row(
          Integer.parseInt(f[header.indexOf("round")]),
          Integer.parseInt(f[header.indexOf("index")]),
          Integer.parseInt(f[header.indexOf("qtime_ms")]),
          f[header.indexOf("counts")],
          error.isEmpty() ? null : error,
          f[header.indexOf("kind")]));
    }
    return rows;
  }

  private static List<Double> sorted(List<Double> values) {
    return values.stream().sorted().toList();
  }

  private static double percentile(List<Double> sorted, int p) {
    return sorted.get(Math.min(sorted.size() - 1, Math.max(0, (int) Math.ceil(p / 100.0 * sorted.size()) - 1)));
  }

  /** OLS slope of y over x, as a percentage of y's mean. */
  private static double relativeSlope(List<Double> x, List<Double> y) {
    int n = x.size();
    if (n < 2) {
      return Double.NaN;
    }
    double mx = x.stream().mapToDouble(d -> d).average().orElse(0);
    double my = y.stream().mapToDouble(d -> d).average().orElse(0);
    double sxy = 0;
    double sxx = 0;
    for (int i = 0; i < n; i++) {
      sxy += (x.get(i) - mx) * (y.get(i) - my);
      sxx += (x.get(i) - mx) * (x.get(i) - mx);
    }
    return my == 0 || sxx == 0 ? Double.NaN : 100.0 * (sxy / sxx) / my;
  }

  /** {@code --name=value} or {@code --name value}; flags take no value. */
  private static Map<String, String> parseOptions(String[] args) {
    Map<String, String> opts = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      if (!args[i].startsWith("--")) {
        usage();
      }
      String name = args[i].substring(2);
      String value = null;
      int eq = name.indexOf('=');
      if (eq >= 0) {
        value = name.substring(eq + 1);
        name = name.substring(0, eq);
      }
      if (FLAGS.contains(name)) {
        opts.put(name, "");
      } else if (OPTIONS.contains(name)) {
        if (value == null) {
          if (++i >= args.length) {
            usage();
          }
          value = args[i];
        }
        opts.put(name, value);
      } else {
        usage();
      }
    }
    return opts;
  }

  private static void usage() {
    System.err.println(
        "Usage: tpchSearchThenRefresh --solr-url=<url> [--parsers=aijoin-flag,joinnum-flag]"
            + " [--query-count=20] [--first-round-query-count=<n>] [--repeat=30]"
            + " [--tbl-dir=build/tpch-sf1] [--refresh-dir=<tbl-dir>/refresh] [--no-reset]");
    System.err.println("       tpchSearchThenRefresh --summarize=reports/<run> [--from-round=2]");
    System.exit(1);
  }
}
