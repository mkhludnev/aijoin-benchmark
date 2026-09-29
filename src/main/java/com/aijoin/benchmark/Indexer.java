package com.aijoin.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.solr.client.solrj.jetty.CloudJettySolrClient;
import org.apache.solr.common.SolrInputDocument;

/**
 * Bulk-indexes {@link Constants#PRODUCT_COUNT} products and {@link Constants#SKU_COUNT} skus in a
 * single pass: each product is sent together with the skus that point at it, all through
 * {@link Constants#PRODUCT_ID_FK} (single-valued, matching {!aijoin}'s M:1 doc mapping -- see
 * {@link Constants}). IDs and field values are deterministic (seeded from {@link
 * Constants#RANDOM_SEED}), so {@link Searcher} can generate matching filter values without querying
 * the index first.
 *
 * <p>Usage: {@code index <solrBaseUrl>}.
 */
public class Indexer {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: index <solrBaseUrl>");
      System.exit(1);
    }
    String solrUrl = args[0];

    try (CloudJettySolrClient client = new CloudJettySolrClient.Builder(List.of(solrUrl)).build()) {
      long start = System.currentTimeMillis();
      indexProductsWithSkus(client);
      double elapsedSec = (System.currentTimeMillis() - start) / 1000.0;
      System.out.printf("Indexing complete in %.1fs%n", elapsedSec);
    }
  }

  private static void indexProductsWithSkus(CloudJettySolrClient client) throws Exception {
    System.out.printf(
        "Indexing %,d products with %,d skus...%n", Constants.PRODUCT_COUNT, Constants.SKU_COUNT);
    runParallel(
        Constants.PRODUCT_COUNT,
        List.of(Constants.PRODUCTS_COLLECTION, Constants.PRODSKUS_COLLECTION),
        List.of(Constants.SKUS_COLLECTION, Constants.PRODSKUS_COLLECTION),
        client,
        (rnd, i) -> {
          // one group per product: the product doc first, then every sku pointing at it
          List<SolrInputDocument> group = new ArrayList<>();
          String prodId = productId(i);
          SolrInputDocument product = new SolrInputDocument();
          product.setField(Constants.PRODUCT_ID, prodId);
          // self-referencing FK, for {!globalOrdinalsJoin} on the colo-index: Lucene's global
          // ordinals join reads one and the same field on both sides, so a product is only
          // reachable as a "to" doc when it carries the join value under PRODUCT_ID_FK too
          product.setField(Constants.PRODUCT_ID_FK, prodId);
          product.setField(Constants.PRODUCT_ID_NUM, i);
          product.setField(Constants.TITLE, randomTitle(rnd));
          product.setField(Constants.BRAND, Constants.BRANDS.get(rnd.nextInt(Constants.BRANDS.size())));
          group.add(product);
          int skuFrom = (int) ((long) i * Constants.SKU_COUNT / Constants.PRODUCT_COUNT);
          int skuTo = (int) ((long) (i + 1) * Constants.SKU_COUNT / Constants.PRODUCT_COUNT);
          for (int s = skuFrom; s < skuTo; s++) {
            SolrInputDocument sku = new SolrInputDocument();
            sku.setField(Constants.SKU_ID, skuId(s));
            // single-valued FK: exactly one product per sku, required by {!aijoin}'s M:1 mapping
            sku.setField(Constants.PRODUCT_ID_FK, prodId);
            sku.setField(Constants.PRODUCT_ID_FK_NUM, i);
            sku.setField(
                Constants.COLOR_KEYWORD, Constants.COLORS.get(rnd.nextInt(Constants.COLORS.size())));
            sku.setField(
                Constants.SIZE_KEYWORD, Constants.SIZES.get(rnd.nextInt(Constants.SIZES.size())));
            sku.setField(
                Constants.INVENTORY_STOCK, rnd.nextInt(Constants.MAX_INVENTORY_STOCK + 1));
            group.add(sku);
          }
          return group;
        });
    client.commit(Constants.PRODUCTS_COLLECTION);
    client.commit(Constants.SKUS_COLLECTION);
    client.commit(Constants.PRODSKUS_COLLECTION);
    System.out.println("Products and skus committed.");
  }

  static String productId(int i) {
    return String.format("P%07d", i);
  }

  static String skuId(int i) {
    return String.format("S%08d", i);
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

  @FunctionalInterface
  private interface DocFactory {
    List<SolrInputDocument> create(Random rnd, int docIndex);
  }

  /** Splits {@code [0, count)} into {@link Constants#INDEXER_THREADS} contiguous ranges, one
   * worker thread per range, each batching {@link Constants#INDEXER_BATCH_SIZE} docs per add. A
   * batch is flushed once it reaches the boundary; a product group that pushes it past the boundary
   * is kept whole. */
  private static void runParallel(
      int count,
      List<String> productCollections,
      List<String> skuCollections,
      CloudJettySolrClient client,
      DocFactory factory)
      throws InterruptedException, ExecutionException {
    ExecutorService pool = Executors.newFixedThreadPool(Constants.INDEXER_THREADS);
    AtomicLong indexed = new AtomicLong();
    int perThread = (count + Constants.INDEXER_THREADS - 1) / Constants.INDEXER_THREADS;
    long startTime = System.currentTimeMillis();
    List<Future<?>> futures = new ArrayList<>();
    for (int t = 0; t < Constants.INDEXER_THREADS; t++) {
      int from = t * perThread;
      int to = Math.min(count, from + perThread);
      int workerSeed = t;
      futures.add(
          pool.submit(
              () -> indexRange(client, productCollections, skuCollections, factory, from, to, workerSeed, indexed, startTime)));
    }
    pool.shutdown();
    for (Future<?> f : futures) {
      f.get();
    }
  }

  private static void indexRange(
      CloudJettySolrClient client,
      List<String> productCollections,
      List<String> skuCollections,
      DocFactory factory,
      int from,
      int to,
      int workerSeed,
      AtomicLong indexed,
      long startTime) {
    Random rnd = new Random(Constants.RANDOM_SEED + workerSeed);
    List<SolrInputDocument> productBatch = new ArrayList<>(Constants.INDEXER_BATCH_SIZE);
    List<SolrInputDocument> skuBatch = new ArrayList<>(Constants.INDEXER_BATCH_SIZE);
    int pending = 0;
    int batchesSinceLog = 0;
    int total = Constants.PRODUCT_COUNT + Constants.SKU_COUNT;
    for (int i = from; i < to; i++) {
      List<SolrInputDocument> group = factory.create(rnd, i);
      productBatch.add(group.get(0));
      for (int g = 1; g < group.size(); g++) {
        skuBatch.add(group.get(g));
      }
      pending += group.size();
      if (pending >= Constants.INDEXER_BATCH_SIZE) {
        flushBatches(client, productCollections, productBatch, skuCollections, skuBatch);
        long done = indexed.addAndGet(pending);
        if (++batchesSinceLog >= 20) {
          logProgress(done, total, startTime);
          batchesSinceLog = 0;
        }
        productBatch = new ArrayList<>(Constants.INDEXER_BATCH_SIZE);
        skuBatch = new ArrayList<>(Constants.INDEXER_BATCH_SIZE);
        pending = 0;
      }
    }
    if (pending > 0) {
      flushBatches(client, productCollections, productBatch, skuCollections, skuBatch);
      logProgress(indexed.addAndGet(pending), total, startTime);
    }
  }

  private static void flushBatches(
      CloudJettySolrClient client,
      List<String> productCollections,
      List<SolrInputDocument> productBatch,
      List<String> skuCollections,
      List<SolrInputDocument> skuBatch) {
    sendBatch(client, productCollections, productBatch);
    sendBatch(client, skuCollections, skuBatch);
  }

  private static void sendBatch(
      CloudJettySolrClient client, List<String> collection, List<SolrInputDocument> batch) {
    try {
      for (String c : collection) {
        client.add(c, batch);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed indexing batch to " + collection, e);
    }
  }

  private static void logProgress(long done, int total, long startTime) {
    double elapsedSec = (System.currentTimeMillis() - startTime) / 1000.0;
    double rate = done / Math.max(elapsedSec, 0.001);
    System.out.printf(
        "  %,d / %,d (%.1f%%), %.0f docs/sec%n", done, total, 100.0 * done / total, rate);
  }
}