package com.aijoin.tpchbenchmark;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.jetty.CloudJettySolrClient;
import org.apache.solr.client.solrj.response.FacetField;
import org.apache.solr.client.solrj.response.QueryResponse;

/**
 * Runs TPC-H Q4 against {@code tpch_orders}/{@code tpch_lineitem} through one join parser: each
 * query is generated as SQL by {@link Q4Query} and executed as its SolrJ translation.
 *
 * <p>Same harness shape as {@link com.aijoin.benchmark.Searcher}: a fixed query count at a fixed
 * concurrency (closed loop), the query list drawn up front from {@link TpchConstants#RANDOM_SEED}
 * so query <i>k</i> is the same in every run, and one CSV row per query. The CSV carries the same
 * columns {@code Searcher} writes (plus {@code date} and the per-priority {@code counts}), so {@code
 * ./gradlew search -Pargs="compare a.csv b.csv"} diffs two arms query by query.
 *
 * <p>Usage:
 *
 * <pre>
 *   tpchSearch sql [yyyy-MM-dd]                   print the SQL and its SolrJ translation, no Solr
 *   tpchSearch validate &lt;solrBaseUrl&gt; &lt;parser&gt;...  run the spec's validation query (DATE=1993-07-01)
 *   tpchSearch &lt;solrBaseUrl&gt; &lt;parser&gt; &lt;queryCount&gt; [concurrency] [warmupCount]
 * </pre>
 *
 * where parser is one of {@code join|jointop|aijoin|joinnum}, optionally suffixed {@code -flag} to
 * match the precomputed {@code l_late} flag rather than compare the two dates per doc.
 */
public class TpchSearcher {

  /** Warmup draws from another seed, so the measured set is the same whatever the warmup count. */
  private static final long WARMUP_SEED_OFFSET = 1L;

  /**
   * {@code kind} is empty for a plain run; {@link TpchSearchThenRefresh} marks each row {@code
   * cold}, {@code warm} or {@code twin}.
   */
  static final String CSV_HEADER = "round,index,qtime_ms,wall_ms,numFound,error,date,counts,kind";

  /** One executed query. {@code qTimeMs < 0} marks a failure. */
  record Result(
      int index, LocalDate date, int qTimeMs, long wallMs, long numFound, Map<String, Long> counts,
      String error) {}

  public static void main(String[] args) throws Exception {
    if (args.length >= 1 && args[0].equals("sql")) {
      Q4Query query =
          new Q4Query(args.length > 1 ? LocalDate.parse(args[1]) : TpchConstants.VALIDATION_DATE);
      System.out.println(query.sql());
      for (String parser : List.of("join", "jointop", "aijoin", "joinnum", "aijoin-flag")) {
        System.out.printf(
            "%s -> /%s/select?%s%n", parser, TpchConstants.ORDERS_COLLECTION, query.toSolrParams(parser));
      }
      return;
    }
    if (args.length >= 3 && args[0].equals("validate")) {
      boolean ok = true;
      try (CloudJettySolrClient client = new CloudJettySolrClient.Builder(List.of(args[1])).build()) {
        for (String parser : Arrays.asList(args).subList(2, args.length)) {
          ok &= validate(client, parser);
        }
      }
      System.exit(ok ? 0 : 2);
    }
    if (args.length < 3 || args.length > 5) {
      System.err.println("Usage: tpchSearch sql [yyyy-MM-dd]");
      System.err.println("       tpchSearch validate <solrBaseUrl> <join|jointop|aijoin|joinnum>...");
      System.err.println(
          "       tpchSearch <solrBaseUrl> <join|jointop|aijoin|joinnum> <queryCount> [concurrency] [warmupCount]");
      System.exit(1);
    }
    String solrUrl = args[0];
    String parser = args[1];
    int queryCount = Integer.parseInt(args[2]);
    int concurrency = args.length > 3 ? Integer.parseInt(args[3]) : 1;
    int warmupCount = args.length > 4 ? Integer.parseInt(args[4]) : 0;
    Q4Query.joinLocalParams(parser); // fail fast on a bad arm name
    if (queryCount < 1 || concurrency < 1 || warmupCount < 0) {
      throw new IllegalArgumentException("queryCount and concurrency must be >= 1");
    }

    List<Q4Query> warmup = generateQueries(warmupCount, TpchConstants.RANDOM_SEED + WARMUP_SEED_OFFSET);
    List<Q4Query> measured = generateQueries(queryCount, TpchConstants.RANDOM_SEED);

    try (CloudJettySolrClient client = new CloudJettySolrClient.Builder(List.of(solrUrl)).build()) {
      if (warmupCount > 0) {
        System.out.printf("Warming up: %d queries, concurrency %d (results discarded)...%n",
            warmupCount, concurrency);
        run(client, parser, warmup, concurrency);
      }
      System.out.printf("Running TPC-H Q4 via %s against '%s': %d queries, concurrency %d...%n",
          new Q4Query(TpchConstants.VALIDATION_DATE).existsQuery(parser), solrUrl, queryCount, concurrency);
      long startNanos = System.nanoTime();
      Result[] results = run(client, parser, measured, concurrency);
      double elapsedSec = (System.nanoTime() - startNanos) / 1e9;

      Path csv = Path.of(String.format("results-tpch-q4-%s-c%d.csv", parser, concurrency));
      writeCsv(csv, results);
      printSummary(parser, concurrency, results, elapsedSec, csv);
    }
  }

  /** Runs the spec 2.4.4.4 validation query and checks it against {@code answers/q4.out}. */
  static boolean validate(CloudJettySolrClient client, String parser) {
    Result r = runOneQuery(client, parser, 0, new Q4Query(TpchConstants.VALIDATION_DATE));
    boolean ok = r.error() == null && r.counts().equals(TpchConstants.VALIDATION_ANSWER);
    System.out.printf("{!%s} DATE=%s QTime=%dms%n", parser, TpchConstants.VALIDATION_DATE, r.qTimeMs());
    if (r.error() != null) {
      System.out.println("  FAILED: " + r.error());
      return false;
    }
    for (Map.Entry<String, Long> e : TpchConstants.VALIDATION_ANSWER.entrySet()) {
      Long got = r.counts().get(e.getKey());
      System.out.printf("  %-16s %6d  expected %6d%s%n", e.getKey(), got == null ? 0 : got,
          e.getValue(), e.getValue().equals(got) ? "" : "  <-- MISMATCH");
    }
    System.out.println(ok ? "  VALID: matches answers/q4.out" : "  INVALID: differs from answers/q4.out");
    return ok;
  }

  /** Draws the query list on one thread, so it never depends on execution timing. */
  static List<Q4Query> generateQueries(int count, long seed) {
    Random rnd = new Random(seed);
    List<Q4Query> queries = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      queries.add(Q4Query.random(rnd));
    }
    return queries;
  }

  /** Closed loop: {@code concurrency} workers pull from a shared cursor into disjoint slots. */
  static Result[] run(
      CloudJettySolrClient client, String parser, List<Q4Query> queries, int concurrency)
      throws InterruptedException {
    Result[] results = new Result[queries.size()];
    AtomicInteger cursor = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(concurrency);
    for (int w = 0; w < concurrency; w++) {
      pool.execute(
          () -> {
            int i;
            while ((i = cursor.getAndIncrement()) < queries.size()) {
              results[i] = runOneQuery(client, parser, i, queries.get(i));
            }
          });
    }
    pool.shutdown();
    if (!pool.awaitTermination(1, TimeUnit.HOURS)) {
      pool.shutdownNow();
      throw new IllegalStateException("benchmark did not finish within 1h");
    }
    return results;
  }

  static Result runOneQuery(CloudJettySolrClient client, String parser, int index, Q4Query query) {
    long t0 = System.nanoTime();
    try {
      QueryResponse rsp = client.query(TpchConstants.ORDERS_COLLECTION, query.toSolrParams(parser));
      long wallMs = (System.nanoTime() - t0) / 1_000_000L;
      Map<String, Long> counts = new LinkedHashMap<>();
      FacetField priorities = rsp.getFacetField(TpchConstants.O_ORDERPRIORITY);
      for (FacetField.Count c : priorities.getValues()) {
        counts.put(c.getName(), c.getCount());
      }
      Integer qTime = rsp.getQTime();
      return new Result(index, query.date(), qTime == null ? -1 : qTime, wallMs,
          rsp.getResults().getNumFound(), counts, null);
    } catch (Exception e) {
      long wallMs = (System.nanoTime() - t0) / 1_000_000L;
      System.err.println("Query " + index + " failed: " + e);
      return new Result(index, query.date(), -1, wallMs, -1L, Map.of(), e.toString().replace(',', ';'));
    }
  }

  static void writeCsv(Path path, Result[] results) throws IOException {
    writeCsv(path, 1, Arrays.asList(results), r -> "", false);
  }

  /**
   * Writes one row per query, tagged with {@code round} and {@code kind.apply(result)}. Appending
   * keeps the rows already there and writes the header only for a new file.
   */
  static void writeCsv(
      Path path, int round, List<Result> results, Function<Result, String> kind, boolean append)
      throws IOException {
    boolean header = !append || !Files.exists(path);
    OpenOption[] options =
        append
            ? new OpenOption[] {StandardOpenOption.CREATE, StandardOpenOption.APPEND}
            : new OpenOption[] {StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING};
    try (PrintWriter out =
        new PrintWriter(Files.newBufferedWriter(path, StandardCharsets.UTF_8, options))) {
      if (header) {
        out.println(CSV_HEADER);
      }
      for (Result r : results) {
        String counts =
            r.counts().entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(";"));
        out.printf("%d,%d,%d,%d,%d,%s,%s,%s,%s%n", round, r.index(), r.qTimeMs(), r.wallMs(),
            r.numFound(), r.error() == null ? "" : r.error(), r.date(), counts, kind.apply(r));
      }
    }
  }

  static void printSummary(
      String parser, int concurrency, Result[] results, double elapsedSec, Path csv) {
    long errors = Arrays.stream(results).filter(r -> r.error() != null).count();
    int[] qTimes = Arrays.stream(results).filter(r -> r.error() == null)
        .mapToInt(Result::qTimeMs).sorted().toArray();
    long[] walls = Arrays.stream(results).filter(r -> r.error() == null)
        .mapToLong(Result::wallMs).sorted().toArray();

    System.out.println();
    System.out.println("==== TPC-H Q4 {!" + parser + "} results (concurrency " + concurrency + ") ====");
    System.out.printf("queries run   : %d (errors: %d)%n", results.length, errors);
    System.out.printf("wall clock    : %.1fs -> %.2f queries/s achieved%n",
        elapsedSec, results.length / elapsedSec);
    if (qTimes.length == 0) {
      return;
    }
    System.out.printf("QTime (ms)    : min=%d p50=%d p90=%d p95=%d p99=%d max=%d avg=%.1f%n",
        qTimes[0], qTimes[percentileIndex(qTimes.length, 50)], qTimes[percentileIndex(qTimes.length, 90)],
        qTimes[percentileIndex(qTimes.length, 95)], qTimes[percentileIndex(qTimes.length, 99)],
        qTimes[qTimes.length - 1], Arrays.stream(qTimes).average().orElse(Double.NaN));
    System.out.printf("client (ms)   : min=%d p50=%d p95=%d max=%d  [QTime + network + any queueing]%n",
        walls[0], walls[percentileIndex(walls.length, 50)], walls[percentileIndex(walls.length, 95)],
        walls[walls.length - 1]);
    System.out.printf("per-query CSV : %s%n", csv);
    System.out.printf("%nTo prove result-set equivalence against another arm:%n"
        + "  ./gradlew search -Pargs=\"compare %s results-tpch-q4-<other>-c%d.csv\"%n", csv, concurrency);
  }

  private static int percentileIndex(int n, int p) {
    return Math.min(n - 1, Math.max(0, (int) Math.ceil(p / 100.0 * n) - 1));
  }
}
