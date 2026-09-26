package com.aijoin.benchmark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.solr.client.solrj.jetty.CloudJettySolrClient;
import org.apache.solr.common.SolrInputDocument;

/**
 * Alternates a fixed <em>count</em> of search queries (no warmup) with a small amount of indexing
 * churn, repeating the search-then-index cycle <em>repeat</em> times. Each round runs {@code
 * queryCount} products-to-skus semijoin queries at the given {@code concurrency}, appends one CSV
 * row per query to a cumulative results file, then updates one random product and ten random skus so
 * that the next round queries against a slightly-mutated index.
 *
 * <p>Which collections get the updates mirrors which index {@link Searcher} reads for each parser:
 *
 * <ul>
 *   <li>{@code join}, {@code aijoin}, {@code joinnum} read {@code products} and {@code skus}
 *       separately, so those two collections are updated.
 *   <li>{@code joinglob} reads the co-located {@code products_skus} collection, so only it is
 *       updated.
 * </ul>
 *
 * <p>Query generation is delegated to {@link Searcher#generateQueries(int, long)} (same deterministic
 * seed), so the per-round query set is identical across parsers and runs. Pass several parsers
 * comma-separated to run them back-to-back in every round; they all share the same seed for that
 * round ({@code RANDOM_SEED + round}) so their query sets match, and a single update step runs after
 * all parsers have finished their round. The parser order is shuffled every round, so no parser
 * always runs first on a fresh searcher; the shuffle is drawn from a {@code Random} seeded with
 * {@code RANDOM_SEED} at startup, so every run replays the same sequence of orders.
 *
 * <p>Every run writes into its own folder, {@code
 * reports/<yyMMdd-HHmm>-<parsers>-<concurrency>-<repeat>}, with the parsers joined by {@code _} in
 * the order given. It holds the per-parser CSVs plus {@code args.txt}, the run's command-line
 * arguments.
 *
 * <p>Usage:
 *
 * <pre>
 *   searchThenIndex --solr-url=&lt;solrBaseUrl&gt; --parsers=&lt;join,aijoin,joinnum,joinglob,...&gt;
 *                   --query-count=&lt;n&gt; [--concurrency=&lt;n&gt;] [--repeat=&lt;n&gt;] [--no-to-filter]
 *                   [--update=products,skus] [--no-concurrent-search] [--soft-commit]
 * </pre>
 *
 * <p>Each option may also be given as {@code --name value}. {@code --no-to-filter} drops the
 * top-level brand filter on the products (to) side, so the join alone decides numFound; the
 * from-side filters stay exactly as in a filtered run, and results go to a separate {@code
 * -noto.csv} file.
 *
 * <p>{@code --update} picks what the per-round update step churns: {@code products}, {@code skus}
 * or both (the default), comma-separated. The random draws are the same either way, so the docs
 * written for one side are identical whether or not the other side is updated too. {@code
 * --no-concurrent-search} skips the aijoin consistency probe, so no search runs concurrently with
 * the update. {@code --soft-commit} ends each update with a soft commit instead of a hard one:
 * the new docs become visible on a fresh searcher without flushing segments to disk.
 */
public class SearchThenIndex {

  private static final int SKUS_PER_ROUND = 10;

  private static final String USAGE =
      "Usage: searchThenIndex --solr-url=<solrBaseUrl> --parsers=<join,aijoin,joinnum,joinglob,...>"
          + " --query-count=<n> [--concurrency=<n>] [--repeat=<n>] [--no-to-filter]"
          + " [--update=products,skus] [--no-concurrent-search] [--soft-commit]";

  private static final Set<String> OPTIONS =
      Set.of("solr-url", "parsers", "query-count", "concurrency", "repeat", "update");

  /** Options that take no value. */
  private static final Set<String> FLAGS = Set.of("no-to-filter", "no-concurrent-search", "soft-commit");

  private static final Set<String> UPDATE_MODES = Set.of("products", "skus");

  public static void main(String[] args) throws Exception {
    Map<String, String> opts = parseOptions(args);
    if (opts == null
        || !opts.containsKey("solr-url")
        || !opts.containsKey("parsers")
        || !opts.containsKey("query-count")) {
      System.err.println(USAGE);
      System.exit(1);
    }
    String solrUrl = opts.get("solr-url");
    List<String> parsers = new ArrayList<>(Arrays.asList(opts.get("parsers").split(",")));
    int queryCount = Integer.parseInt(opts.get("query-count"));
    int concurrency = Integer.parseInt(opts.getOrDefault("concurrency", "1"));
    int repeat = Integer.parseInt(opts.getOrDefault("repeat", "1"));
    boolean toFilter = !opts.containsKey("no-to-filter");
    boolean concurrentSearch = !opts.containsKey("no-concurrent-search");
    Set<String> updateModes = Set.of(opts.getOrDefault("update", "products,skus").split(","));
    if (!UPDATE_MODES.containsAll(updateModes)) {
      throw new IllegalArgumentException("--update takes products, skus or both, got: " + updateModes);
    }
    boolean updateProducts = updateModes.contains("products");
    boolean updateSkus = updateModes.contains("skus");
    boolean softCommit = opts.containsKey("soft-commit");

    if (queryCount < 1 || concurrency < 1 || repeat < 1) {
      throw new IllegalArgumentException("queryCount, concurrency and repeat must be >= 1");
    }

    Path reportDir =
        Path.of(
            "reports",
            String.format(
                "%s-%s-%d-%d",
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyMMdd-HHmm")),
                String.join("_", parsers),
                concurrency,
                repeat));
    Files.createDirectories(reportDir);
    Files.writeString(reportDir.resolve("args.txt"), String.join(" ", args) + System.lineSeparator());
    System.out.println("Writing reports to " + reportDir);

    try (CloudJettySolrClient client = new CloudJettySolrClient.Builder(List.of(solrUrl)).build()) {
      Random parserOrder = new Random(Constants.RANDOM_SEED);
      for (int round = 1; round <= repeat; round++) {
        Collections.shuffle(parsers, parserOrder);
        System.out.printf("Round %d/%d: parser order %s%n", round, repeat, parsers);
        for (String parser : parsers) {
          long seed = Constants.RANDOM_SEED + round;
          String localParams = Searcher.localParams(parser);
          System.out.printf(
              "Round %d/%d: running {!%s} %d queries at concurrency %d (no warmup)...%n",
              round, repeat, parser, queryCount, concurrency);
          Searcher.Result[] results =
              Searcher.run(client, localParams, generateQueries(queryCount, seed, toFilter), concurrency);
          Path csv =
              reportDir.resolve(
                  String.format(
                      "searchindex-results-%s-c%d%s.csv", parser, concurrency, toFilter ? "" : "-noto"));
          Searcher.writeCsv(csv, round, results, true);
        }

        System.out.printf(
            "Round %d/%d: updating %d products and %d skus (%s commit)...%n",
            round,
            repeat,
            updateProducts ? 1 : 0,
            updateSkus ? SKUS_PER_ROUND : 0,
            softCommit ? "soft" : "hard");
        if (concurrentSearch && parsers.contains("aijoin")) {
          if (!updateDataWithConsistencyProbe(
              client, round, repeat, toFilter, updateProducts, updateSkus, softCommit)) {
            System.out.println("Aborting: aijoin update-consistency probe failed.");
            return;
          }
        } else {
          updateData(client, round, updateProducts, updateSkus, softCommit);
        }
      }
    }
    System.out.println("Search-then-index complete.");
  }

  /**
   * Same queries as {@link Searcher#generateQueries(int, long)} -- the random draws are unchanged --
   * with the brand filter optionally dropped afterwards.
   */
  private static List<Searcher.QuerySpec> generateQueries(int count, long seed, boolean toFilter) {
    List<Searcher.QuerySpec> specs = Searcher.generateQueries(count, seed);
    return toFilter ? specs : specs.stream().map(Searcher.QuerySpec::withoutToFilter).toList();
  }

  /**
   * Parses {@code --name=value} and {@code --name value} options, plus value-less {@link #FLAGS}. Returns null on an unknown option,
   * a missing value, a positional argument or a repeated option.
   */
  private static Map<String, String> parseOptions(String[] args) {
    Map<String, String> opts = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      String arg = args[i];
      if (!arg.startsWith("--")) {
        System.err.println("Unexpected argument: " + arg);
        return null;
      }
      String name = arg.substring(2);
      String value;
      int eq = name.indexOf('=');
      if (FLAGS.contains(name)) {
        value = "";
      } else if (eq >= 0) {
        value = name.substring(eq + 1);
        name = name.substring(0, eq);
      } else if (i + 1 < args.length) {
        value = args[++i];
      } else {
        System.err.println("Missing value for --" + name);
        return null;
      }
      if (!OPTIONS.contains(name) && !FLAGS.contains(name)) {
        System.err.println("Unknown option: --" + name);
        return null;
      }
      if (opts.put(name, value) != null) {
        System.err.println("Duplicate option: --" + name);
        return null;
      }
    }
    return opts;
  }

  /**
   * Runs the per-round update wrapped in an aijoin consistency probe. Before the update we fire one
   * aijoin query built from a seed drawn at random from the run's existing round seeds; we re-fire
   * the same query 1-5 times (random count) while the update runs concurrently; then once more after
   * it. None of these probe queries are written to CSV.
   *
   * <p>The probe asserts that a search concurrent with an update observes only the pre- or
   * post-update state, never a third intermediate numFound. If more than two distinct numFound
   * values are seen, or any probe query fails, we log and return false so the caller aborts the run.
   */
  private static boolean updateDataWithConsistencyProbe(
      CloudJettySolrClient client,
      int round,
      int repeat,
      boolean toFilter,
      boolean updateProducts,
      boolean updateSkus,
      boolean softCommit)
      throws Exception {
    long probeSeed = Constants.RANDOM_SEED + 1 + new Random().nextInt(repeat);
    Searcher.QuerySpec query = generateQueries(1, probeSeed, toFilter).get(0);
    String aijoinParams = Searcher.localParams("aijoin");

    Searcher.Result pre = Searcher.runOneQuery(client, aijoinParams, -1, query);
    if (pre.error() != null) {
      System.err.println("aijoin probe: pre-update query failed: " + pre.error());
      return false;
    }

    Set<Long> numFounds = new LinkedHashSet<>(List.of(pre.numFound()));
    int intraRuns = 1 + new Random().nextInt(5);

    ExecutorService updatePool = Executors.newSingleThreadExecutor();
    Future<?> updateFuture =
        updatePool.submit(
            () -> {
              try {
                updateData(client, round, updateProducts, updateSkus, softCommit);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    for (int k = 0; k < intraRuns; k++) {
      Searcher.Result intra = Searcher.runOneQuery(client, aijoinParams, -1, query);
      if (intra.error() != null) {
        System.err.println("aijoin probe: intra-update query " + k + " failed: " + intra.error());
        updateFuture.cancel(true);
        updatePool.shutdownNow();
        return false;
      }
      numFounds.add(intra.numFound());
    }

    updateFuture.get();
    updatePool.shutdown();

    Searcher.Result post = Searcher.runOneQuery(client, aijoinParams, -1, query);
    if (post.error() != null) {
      System.err.println("aijoin probe: post-update query failed: " + post.error());
      return false;
    }
    numFounds.add(post.numFound());

    System.out.printf(
        "Round %d aijoin probe (seed %d, %d intra-update runs): numFound pre=%d intra=%s post=%d, %d distinct%n",
        round, probeSeed, intraRuns, pre.numFound(), numFounds, post.numFound(), numFounds.size());
    if (numFounds.size() > 3) {
      System.err.println(
          "INCONSISTENT: "
              + numFounds.size()
              + " distinct numFound across a concurrent update: "
              + numFounds);
      return false;
    }
    return true;
  }

  /**
   * Updates one random product and ten random skus once, in <em>every</em> collection -- never only
   * the ones the parsers of this run happen to read.
   *
   * <p>Both halves of that matter for comparability. Churning only the collections a run reads
   * makes the datasets drift apart the moment parsers are benchmarked in separate invocations
   * ({@code joinglob} reads the co-located collection, the others read products/skus), and then
   * numFound differs between parsers for no reason but the data. And the churn is seeded per round
   * rather than randomly, so that separate invocations replay the same mutations: updates are by
   * unique key, so replaying an identical sequence is idempotent and every run walks the index
   * through the same states.
   *
   * <p>{@code updateProducts}/{@code updateSkus} only gate the writes: the random draws happen
   * regardless, so a skus-only round writes exactly the skus a both-sides round would.
   */
  private static void updateData(
      CloudJettySolrClient client,
      int round,
      boolean updateProducts,
      boolean updateSkus,
      boolean softCommit)
      throws Exception {
    List<String> productTargets =
        updateProducts
            ? List.of(Constants.PRODUCTS_COLLECTION, Constants.PRODSKUS_COLLECTION)
            : List.of();
    List<String> skuTargets =
        updateSkus ? List.of(Constants.SKUS_COLLECTION, Constants.PRODSKUS_COLLECTION) : List.of();

    Random rnd = new Random(Constants.RANDOM_SEED + round);

    SolrInputDocument product = newProduct(rnd, rnd.nextInt(Constants.PRODUCT_COUNT));
    for (String c : productTargets) {
      client.add(c, product);
    }

    for (int k = 0; k < SKUS_PER_ROUND; k++) {
      SolrInputDocument sku = newSku(rnd, rnd.nextInt(Constants.SKU_COUNT));
      for (String c : skuTargets) {
        client.add(c, sku);
      }
    }

    for (String c : concat(productTargets, skuTargets)) {
      client.commit(c, true, true, softCommit);
    }
  }

  private static SolrInputDocument newProduct(Random rnd, int idx) {
    SolrInputDocument doc = new SolrInputDocument();
    String prodId = Indexer.productId(idx);
    doc.setField(Constants.PRODUCT_ID, prodId);
    doc.setField(Constants.PRODUCT_ID_FK, prodId);
    doc.setField(Constants.PRODUCT_ID_NUM, idx);
    doc.setField(Constants.TITLE, randomTitle(rnd));
    doc.setField(Constants.BRAND, Constants.BRANDS.get(rnd.nextInt(Constants.BRANDS.size())));
    return doc;
  }

  private static SolrInputDocument newSku(Random rnd, int idx) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField(Constants.SKU_ID, Indexer.skuId(idx));
    int prodIdx = rnd.nextInt(Constants.PRODUCT_COUNT);
    doc.setField(Constants.PRODUCT_ID_FK, Indexer.productId(prodIdx));
    doc.setField(Constants.PRODUCT_ID_FK_NUM, prodIdx);
    doc.setField(Constants.COLOR_KEYWORD, Constants.COLORS.get(rnd.nextInt(Constants.COLORS.size())));
    doc.setField(Constants.SIZE_KEYWORD, Constants.SIZES.get(rnd.nextInt(Constants.SIZES.size())));
    doc.setField(Constants.INVENTORY_STOCK, rnd.nextInt(Constants.MAX_INVENTORY_STOCK + 1));
    return doc;
  }

  private static String randomTitle(Random rnd) {
    int words = 4 + rnd.nextInt(5);
    StringBuilder sb = new StringBuilder();
    for (int w = 0; w < words; w++) {
      if (w > 0) {
        sb.append(' ');
      }
      sb.append(Constants.TITLE_WORDS.get(rnd.nextInt(Constants.TITLE_WORDS.size())));
    }
    return sb.toString();
  }

  private static List<String> concat(List<String> a, List<String> b) {
    List<String> merged = new ArrayList<>(a);
    for (String s : b) {
      if (!merged.contains(s)) {
        merged.add(s);
      }
    }
    return merged;
  }
}