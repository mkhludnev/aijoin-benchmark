package com.aijoin.benchmark;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.jetty.CloudJettySolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * Runs a fixed <em>count</em> of products-to-skus semijoin queries at a fixed <em>concurrency</em>,
 * via either {@code {!join score=none}} or {@code {!aijoin}}, and writes one CSV row per query.
 *
 * <p>Two properties make the two parsers comparable, and both are deliberate:
 *
 * <ol>
 *   <li><b>The query list is generated up front, single-threaded, from {@link
 *       Constants#RANDOM_SEED}.</b> Query <i>k</i> is therefore the same query in every run,
 *       whatever the parser, the concurrency, or the thread interleaving. Drawing from a shared
 *       {@link Random} inside the workers -- as this class used to -- would make the query
 *       <em>set</em> depend on timing, and then the two runs could not be diffed at all.
 *   <li><b>Results are recorded per query, indexed by query ordinal</b>, not aggregated on the fly.
 *       Comparing {@code numFound} distributions across two runs proves nothing unless the runs
 *       executed the same queries; comparing them query by query proves result-set equivalence
 *       outright. See the {@code compare} mode below.
 * </ol>
 *
 * <p><b>Why concurrency and not a rate.</b> An open-loop harness that fires at a fixed rate diverges
 * if the rate exceeds what the server can absorb: the queue grows without bound and the run measures
 * queueing rather than the join. Whether a given rate is sustainable depends on how much CPU a
 * single query consumes, which differs by roughly the very factor this benchmark is trying to
 * measure. A closed loop with {@code concurrency} queries in flight cannot diverge -- it degrades
 * into a throughput measurement instead -- so the run is always interpretable. Use {@code
 * concurrency=1} for uncontended service time (the "far from saturation" latency number), and sweep
 * 1, 2, 4, 8 to find where each parser saturates.
 *
 * <p>Usage:
 *
 * <pre>
 *   search &lt;solrBaseUrl&gt; &lt;join|aijoin&gt; &lt;queryCount&gt; [concurrency] [warmupCount]
 *   search compare &lt;resultsA.csv&gt; &lt;resultsB.csv&gt;
 * </pre>
 *
 * <p>Every query filters skus by a few random Color <em>or</em> Size values (never both -- see
 * {@link #buildFromFilter}) joined back to products, further filtered by a few random brands on the
 * products side directly (as an {@code fq}, not through the join).
 */
public class Searcher {

  private static final int MIN_VALUES_PER_FILTER = 2;
  private static final int MAX_VALUES_PER_FILTER = 4;
  private static final int MIN_BRANDS = 1;
  private static final int MAX_BRANDS = 3;

  /** Builds the {@code {!...}} local-params fragment for a parser name. */
  static String localParams(String parser) {
    return switch (parser) {
      case "join" -> "join score=none" + " fromIndex=" + Constants.SKUS_COLLECTION + " from=" + Constants.PRODUCT_ID_FK + " to=" + Constants.PRODUCT_ID;
      case "aijoin" -> "aijoin"+ " fromIndex=" + Constants.SKUS_COLLECTION + " from=" + Constants.PRODUCT_ID_FK + " to=" + Constants.PRODUCT_ID;
      case "joinnum" -> "join score=none" + " fromIndex=" + Constants.SKUS_COLLECTION + " from=" + Constants.PRODUCT_ID_FK_NUM + " to=" + Constants.PRODUCT_ID_NUM;
      case "joinglob" -> "globalOrdinalsJoin score=none joinField=" + Constants.PRODUCT_ID_FK + " which= ";
      default -> throw new IllegalArgumentException(
          "parser must be 'join|aijoin|joinnum|joinglob': " + parser);
    };
  }

  /**
   * Warmup queries are drawn from a different seed than measured ones, so that the measured set
   * stays identical no matter how many warmup queries were requested.
   */
  private static final long WARMUP_SEED_OFFSET = 1L;

  /** One generated query, independent of which parser will execute it. */
  record QuerySpec(String fromFilter, String brandFilter) {}

  /** One executed query. {@code qTimeMs < 0} marks a failure. */
  record Result(int index, int qTimeMs, long wallMs, long numFound, String error) {}

  public static void main(String[] args) throws Exception {
    if (args.length > 0 && args[0].equals("compare")) {
      if (args.length != 3) {
        System.err.println("Usage: search compare <resultsA.csv> <resultsB.csv>");
        System.exit(1);
      }
      System.exit(compare(Path.of(args[1]), Path.of(args[2])) ? 0 : 2);
    }

    if (args.length < 3 || args.length > 5) {
      System.err.println(
          "Usage: search <solrBaseUrl> <join|aijoin|joinnum|joinglob> <queryCount> [concurrency] [warmupCount]");
      System.err.println("       search compare <resultsA.csv> <resultsB.csv>");
      System.exit(1);
    }
    String solrUrl = args[0];
    String parser = args[1];
    int queryCount = Integer.parseInt(args[2]);
    int concurrency = args.length > 3 ? Integer.parseInt(args[3]) : 1;
    int warmupCount = args.length > 4 ? Integer.parseInt(args[4]) : 0;

    String localParams = localParams(parser);
    if (queryCount < 1 || concurrency < 1 || warmupCount < 0) {
      throw new IllegalArgumentException("queryCount and concurrency must be >= 1");
    }

    List<QuerySpec> warmup = generateQueries(warmupCount, Constants.RANDOM_SEED + WARMUP_SEED_OFFSET);
    List<QuerySpec> measured = generateQueries(queryCount, Constants.RANDOM_SEED);

    try (CloudJettySolrClient client = new CloudJettySolrClient.Builder(List.of(solrUrl)).build()) {
      if (warmupCount > 0) {
        System.out.printf(
            "Warming up: %d queries, concurrency %d (results discarded)...%n",
            warmupCount, concurrency);
        run(client, localParams, warmup, concurrency);
      }

      System.out.printf(
          "Running {!%s} against '%s': %d queries, concurrency %d...%n",
          localParams, solrUrl, queryCount, concurrency);
      long startNanos = System.nanoTime();
      Result[] results = run(client, localParams, measured, concurrency);
      double elapsedSec = (System.nanoTime() - startNanos) / 1e9;

      Path csv = Path.of(String.format("results-%s-c%d.csv", parser, concurrency));
      writeCsv(csv, 1, results, false); // a plain search run is one round
      printSummary(parser, concurrency, results, elapsedSec, csv);
    }
  }

  /**
   * Executes {@code specs} with {@code concurrency} workers pulling from a shared cursor, and
   * returns the results indexed by query ordinal. Workers write to disjoint array slots, and
   * {@code awaitTermination} publishes those writes to this thread.
   */
  static Result[] run(
      CloudJettySolrClient client, String localParams, List<QuerySpec> specs, int concurrency)
      throws InterruptedException {
    Result[] results = new Result[specs.size()];
    AtomicInteger cursor = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(concurrency);
    for (int w = 0; w < concurrency; w++) {
      pool.execute(
          () -> {
            int i;
            while ((i = cursor.getAndIncrement()) < specs.size()) {
              results[i] = runOneQuery(client, localParams, i, specs.get(i));
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

  static Result runOneQuery(
      CloudJettySolrClient client, String localParams, int index, QuerySpec spec) {
    String q =
        "{!"
            + localParams.replace("which=","which='"+spec.brandFilter()+"'" )
            + "}"
            + spec.fromFilter();

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.Q, q);
    params.set(CommonParams.ROWS, 0);
    params.set(CommonParams.FQ, spec.brandFilter());
    // force an exact numFound: skip Solr's approximate early-termination count
    params.set(CommonParams.MIN_EXACT_COUNT, Integer.MAX_VALUE);

    long t0 = System.nanoTime();
    try {
      QueryResponse rsp = client.query(localParams.contains("globalOrdinalsJoin")
              ? Constants.PRODSKUS_COLLECTION:Constants.PRODUCTS_COLLECTION, params);
      long wallMs = (System.nanoTime() - t0) / 1_000_000L;
      Integer qTime = rsp.getQTime();
      return new Result(
          index, qTime == null ? -1 : qTime, wallMs, rsp.getResults().getNumFound(), null);
    } catch (Exception e) {
      long wallMs = (System.nanoTime() - t0) / 1_000_000L;
      System.err.println("Query " + index + " failed: " + e);
      return new Result(index, -1, wallMs, -1L, e.toString().replace(',', ';'));
    }
  }

  /**
   * Generates the query list deterministically, on one thread. Must not be called from workers:
   * the whole point is that the sequence does not depend on execution timing.
   */
  static List<QuerySpec> generateQueries(int count, long seed) {
    Random rnd = new Random(seed);
    List<QuerySpec> specs = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      // both draws happen in this fixed order, so specs.get(i) is a pure function of (seed, i)
      String fromFilter = buildFromFilter(rnd);
      String brandFilter = buildBrandFilter(rnd);
      specs.add(new QuerySpec(fromFilter, brandFilter));
    }
    return specs;
  }

  /** Picks Color OR Size (never both), then a few distinct values from that one vocabulary. */
  static String buildFromFilter(Random rnd) {
    boolean useColor = rnd.nextBoolean();
    String field = useColor ? Constants.COLOR_KEYWORD : Constants.SIZE_KEYWORD;
    List<String> vocab = useColor ? Constants.COLORS : Constants.SIZES;
    return field
        + ":"
        + orClause(
            pickDistinct(rnd, vocab, randomCount(rnd, MIN_VALUES_PER_FILTER, MAX_VALUES_PER_FILTER)));
  }

  static String buildBrandFilter(Random rnd) {
    return Constants.BRAND
        + ":"
        + orClause(pickDistinct(rnd, Constants.BRANDS, randomCount(rnd, MIN_BRANDS, MAX_BRANDS)));
  }

  static int randomCount(Random rnd, int min, int max) {
    return min + rnd.nextInt(max - min + 1);
  }

  static List<String> pickDistinct(Random rnd, List<String> vocab, int n) {
    n = Math.min(n, vocab.size());
    LinkedHashSet<String> picked = new LinkedHashSet<>();
    while (picked.size() < n) {
      picked.add(vocab.get(rnd.nextInt(vocab.size())));
    }
    return new ArrayList<>(picked);
  }

  static String orClause(List<String> values) {
    return "(" + values.stream().map(v -> "\"" + v + "\"").collect(Collectors.joining(" OR ")) + ")";
  }

  /**
   * The one per-query CSV format in this project, written by both a plain {@code search} run (a
   * single round) and {@link SearchThanIndex} (many), so {@link #compare} can diff any two result
   * files. The {@code round} column used to be absent here and present there, which the positional
   * reader silently mistook for a shifted numFound.
   */
  static final String CSV_HEADER = "round,index,qtime_ms,wall_ms,numFound,error";

  /**
   * Writes one row per query, tagged with {@code round}. Appending keeps whatever rounds the file
   * already holds and writes the header only for a new file; otherwise the file is replaced.
   */
  static void writeCsv(Path path, int round, Result[] results, boolean append) throws IOException {
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
        out.printf(
            "%d,%d,%d,%d,%d,%s%n",
            round,
            r.index(),
            r.qTimeMs(),
            r.wallMs(),
            r.numFound(),
            r.error() == null ? "" : r.error());
      }
    }
  }

  private static void printSummary(
      String parser, int concurrency, Result[] results, double elapsedSec, Path csv) {
    long errors = Arrays.stream(results).filter(r -> r.error() != null).count();
    int[] qTimes =
        Arrays.stream(results).filter(r -> r.error() == null).mapToInt(Result::qTimeMs).sorted().toArray();
    long[] walls =
        Arrays.stream(results).filter(r -> r.error() == null).mapToLong(Result::wallMs).sorted().toArray();

    System.out.println();
    System.out.println("==== {!" + parser + "} results (concurrency " + concurrency + ") ====");
    System.out.printf("queries run   : %d (errors: %d)%n", results.length, errors);
    System.out.printf(
        "wall clock    : %.1fs -> %.2f queries/s achieved%n",
        elapsedSec, results.length / elapsedSec);
    if (qTimes.length == 0) {
      return;
    }
    System.out.printf(
        "QTime (ms)    : min=%d p50=%d p90=%d p95=%d p99=%d max=%d avg=%.1f%n",
        qTimes[0],
        percentile(qTimes, 50),
        percentile(qTimes, 90),
        percentile(qTimes, 95),
        percentile(qTimes, 99),
        qTimes[qTimes.length - 1],
        Arrays.stream(qTimes).average().orElse(Double.NaN));
    System.out.printf(
        "client (ms)   : min=%d p50=%d p95=%d max=%d  [QTime + network + any queueing]%n",
        walls[0],
        (int) percentile(walls, 50),
        (int) percentile(walls, 95),
        walls[walls.length - 1]);
    System.out.printf("per-query CSV : %s%n", csv);
    System.out.printf(
        "%nTo prove result-set equivalence against the other parser:%n"
            + "  ./gradlew search -Pargs=\"compare %s results-<other>-c%d.csv\"%n",
        csv, concurrency);
  }

  private static int percentile(int[] sorted, int p) {
    return sorted[percentileIndex(sorted.length, p)];
  }

  private static long percentile(long[] sorted, int p) {
    return sorted[percentileIndex(sorted.length, p)];
  }

  private static int percentileIndex(int n, int p) {
    return Math.min(n - 1, Math.max(0, (int) Math.ceil(p / 100.0 * n) - 1));
  }

  /**
   * Diffs two per-query CSVs by query ordinal and reports every {@code numFound} disagreement. This
   * is the check that establishes result-set equivalence: matching aggregate distributions do not,
   * since two runs of different lengths execute different query sets.
   *
   * @return true when the two runs agree on every query
   */
  private static boolean compare(Path a, Path b) throws IOException {
    List<Result> ra = readCsv(a);
    List<Result> rb = readCsv(b);
    int n = Math.min(ra.size(), rb.size());
    if (ra.size() != rb.size()) {
      System.out.printf(
          "WARNING: %s has %d rows, %s has %d -- comparing the first %d%n",
          a, ra.size(), b, rb.size(), n);
    }

    int mismatches = 0;
    long maxDelta = 0;
    for (int i = 0; i < n; i++) {
      long fa = ra.get(i).numFound();
      long fb = rb.get(i).numFound();
      if (fa != fb) {
        mismatches++;
        maxDelta = Math.max(maxDelta, Math.abs(fa - fb));
        if (mismatches <= 20) {
          System.out.printf("  query %d: numFound %d vs %d (delta %d)%n", i, fa, fb, fa - fb);
        }
      }
    }
    System.out.printf(
        "%ncompared %d queries: %d numFound mismatches%s%n",
        n, mismatches, mismatches == 0 ? "" : String.format(", max delta %d", maxDelta));
    System.out.println(
        mismatches == 0
            ? "RESULT-SET EQUIVALENCE HOLDS: identical numFound on every query."
            : "RESULT SETS DIFFER -- the parsers are not returning the same answers.");
    return mismatches == 0;
  }

  /**
   * Reads a per-query CSV by column <em>name</em>, so one reader handles both layouts written in
   * this project: this class's {@code index,qtime_ms,wall_ms,numFound,error} and {@link
   * SearchThanIndex}'s, which prepends a {@code round} column.
   *
   * <p>It used to read them positionally, which silently shifted by one on the round-prefixed
   * files: {@link #compare} then diffed {@code wall_ms} believing it was {@code numFound}, so every
   * query "disagreed" -- by milliseconds -- and the first query of each run looked catastrophic
   * because that is where the cold-start latency lands.
   */
  private static List<Result> readCsv(Path path) throws IOException {
    List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
    if (lines.isEmpty()) {
      return List.of();
    }
    List<String> header = List.of(lines.get(0).split(",", -1));
    int index = column(header, "index", path);
    int qTimeMs = column(header, "qtime_ms", path);
    int wallMs = column(header, "wall_ms", path);
    int numFound = column(header, "numFound", path);
    int error = column(header, "error", path);
    List<Result> out = new ArrayList<>(lines.size() - 1);
    for (String line : lines.subList(1, lines.size())) {
      String[] f = line.split(",", -1);
      out.add(
          new Result(
              Integer.parseInt(f[index]),
              Integer.parseInt(f[qTimeMs]),
              Long.parseLong(f[wallMs]),
              Long.parseLong(f[numFound]),
              f[error].isEmpty() ? null : f[error]));
    }
    return out;
  }

  private static int column(List<String> header, String name, Path path) {
    int at = header.indexOf(name);
    if (at < 0) {
      throw new IllegalArgumentException("no '" + name + "' column in " + path + ": " + header);
    }
    return at;
  }
}
