package com.aijoin.benchmark;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
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
 * all parsers have finished their round.
 *
 * <p>Usage:
 *
 * <pre>
 *   searchThenIndex &lt;solrBaseUrl&gt; &lt;join,aijoin,joinnum,joinglob,...&gt; &lt;queryCount&gt; [concurrency] [repeat]
 * </pre>
 */
public class SearchThanIndex {

  private static final int SKUS_PER_ROUND = 10;

  public static void main(String[] args) throws Exception {
    if (args.length < 3 || args.length > 5) {
      System.err.println(
          "Usage: searchThenIndex <solrBaseUrl> <join,aijoin,joinnum,joinglob,...> <queryCount> [concurrency] [repeat]");
      System.exit(1);
    }
    String solrUrl = args[0];
    List<String> parsers = Arrays.asList(args[1].split(","));
    int queryCount = Integer.parseInt(args[2]);
    int concurrency = args.length > 3 ? Integer.parseInt(args[3]) : 1;
    int repeat = args.length > 4 ? Integer.parseInt(args[4]) : 1;

    if (queryCount < 1 || concurrency < 1 || repeat < 1) {
      throw new IllegalArgumentException("queryCount, concurrency and repeat must be >= 1");
    }

    try (CloudJettySolrClient client = new CloudJettySolrClient.Builder(List.of(solrUrl)).build()) {
      for (int round = 1; round <= repeat; round++) {
        for (String parser : parsers) {
          long seed = Constants.RANDOM_SEED + round;
          String localParams = Searcher.localParams(parser);
          System.out.printf(
              "Round %d/%d: running {!%s} %d queries at concurrency %d (no warmup)...%n",
              round, repeat, parser, queryCount, concurrency);
          Searcher.Result[] results =
              Searcher.run(client, localParams, Searcher.generateQueries(queryCount, seed), concurrency);
          Path csv = Path.of(String.format("searchindex-results-%s-c%d.csv", parser, concurrency));
          Searcher.writeCsv(csv, round, results, true);
        }

        System.out.printf("Round %d/%d: updating 1 product and %d skus...%n", round, repeat, SKUS_PER_ROUND);
        updateData(client, round);
      }
    }
    System.out.println("Search-then-index complete.");
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
   */
  private static void updateData(CloudJettySolrClient client, int round) throws Exception {
    List<String> productTargets =
        List.of(Constants.PRODUCTS_COLLECTION, Constants.PRODSKUS_COLLECTION);
    List<String> skuTargets = List.of(Constants.SKUS_COLLECTION, Constants.PRODSKUS_COLLECTION);

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
      client.commit(c);
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
    List<String> merged = new java.util.ArrayList<>(a);
    for (String s : b) {
      if (!merged.contains(s)) {
        merged.add(s);
      }
    }
    return merged;
  }
}