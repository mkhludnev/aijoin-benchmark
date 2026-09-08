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
 * Bulk-indexes {@link Constants#PRODUCT_COUNT} products and {@link Constants#SKU_COUNT} skus, each
 * sku pointing at exactly one product via {@link Constants#PRODUCT_ID_FK} (single-valued, matching
 * {!aijoin}'s M:1 doc mapping -- see {@link Constants}). IDs and field values are deterministic
 * (seeded from {@link Constants#RANDOM_SEED}), so {@link Searcher} can generate matching filter
 * values without querying the index first.
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
      indexProducts(client);
      indexSkus(client);
      double elapsedSec = (System.currentTimeMillis() - start) / 1000.0;
      System.out.printf("Indexing complete in %.1fs%n", elapsedSec);
    }
  }

  private static void indexProducts(CloudJettySolrClient client) throws Exception {
    System.out.printf("Indexing %,d products...%n", Constants.PRODUCT_COUNT);
    runParallel(
            Constants.PRODUCT_COUNT,
        List.of(
            Constants.PRODUCTS_COLLECTION, Constants.PRODSKUS_COLLECTION),
        client,
        (rnd, i) -> {
          SolrInputDocument doc = new SolrInputDocument();
          String prodId = productId(i);
          doc.setField(Constants.PRODUCT_ID, prodId);
          // self-referencing FK, for {!globalOrdinalsJoin} on the colo-index: Lucene's global
          // ordinals join reads one and the same field on both sides, so a product is only
          // reachable as a "to" doc when it carries the join value under PRODUCT_ID_FK too
          doc.setField(Constants.PRODUCT_ID_FK, prodId);
          doc.setField(Constants.PRODUCT_ID_NUM, i);
          doc.setField(Constants.TITLE, randomTitle(rnd));
          doc.setField(
              Constants.BRAND, Constants.BRANDS.get(rnd.nextInt(Constants.BRANDS.size())));
          return doc;
        });
    client.commit(Constants.PRODUCTS_COLLECTION);
    client.commit(Constants.PRODSKUS_COLLECTION);
    System.out.println("Products committed.");
  }

  private static void indexSkus(CloudJettySolrClient client) throws Exception {
    System.out.printf("Indexing %,d skus...%n", Constants.SKU_COUNT);
    runParallel(
        Constants.SKU_COUNT,
        List.of(Constants.SKUS_COLLECTION, Constants.PRODSKUS_COLLECTION),
        client,
        (rnd, i) -> {
          SolrInputDocument doc = new SolrInputDocument();
          doc.setField(Constants.SKU_ID, skuId(i));
          // single-valued FK: exactly one product per sku, required by {!aijoin}'s M:1 mapping
          int prodId=rnd.nextInt(Constants.PRODUCT_COUNT);
          doc.setField(Constants.PRODUCT_ID_FK, productId(prodId));

            doc.setField(Constants.PRODUCT_ID_FK_NUM, prodId);
          doc.setField(
              Constants.COLOR_KEYWORD, Constants.COLORS.get(rnd.nextInt(Constants.COLORS.size())));
          doc.setField(
              Constants.SIZE_KEYWORD, Constants.SIZES.get(rnd.nextInt(Constants.SIZES.size())));
          doc.setField(Constants.INVENTORY_STOCK, rnd.nextInt(Constants.MAX_INVENTORY_STOCK + 1));
          return doc;
        });
    client.commit(Constants.SKUS_COLLECTION);
    client.commit(Constants.PRODSKUS_COLLECTION);
    System.out.println("Skus committed.");
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
    SolrInputDocument create(Random rnd, int docIndex);
  }

  /** Splits {@code [0, count)} into {@link Constants#INDEXER_THREADS} contiguous ranges, one
   * worker thread per range, each batching {@link Constants#INDEXER_BATCH_SIZE} docs per add. */
  private static void runParallel(
      int count, List<String> collection, CloudJettySolrClient client, DocFactory factory)
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
              () -> indexRange(client, collection, factory, from, to, workerSeed, indexed, count, startTime)));
    }
    pool.shutdown();
    for (Future<?> f : futures) {
      f.get();
    }
  }

  private static void indexRange(
          CloudJettySolrClient client,
          List<String> collection,
          DocFactory factory,
          int from,
          int to,
          int workerSeed,
          AtomicLong indexed,
          int total,
          long startTime) {
    Random rnd = new Random(Constants.RANDOM_SEED + workerSeed);
    List<SolrInputDocument> batch = new ArrayList<>(Constants.INDEXER_BATCH_SIZE);
    int batchesSinceLog = 0;
    for (int i = from; i < to; i++) {
      batch.add(factory.create(rnd, i));
      if (batch.size() >= Constants.INDEXER_BATCH_SIZE) {
        sendBatch(client, collection, batch);
        long done = indexed.addAndGet(batch.size());
        if (++batchesSinceLog >= 20) {
          logProgress(done, total, startTime);
          batchesSinceLog = 0;
        }
        batch = new ArrayList<>(Constants.INDEXER_BATCH_SIZE);
      }
    }
    if (!batch.isEmpty()) {
      sendBatch(client, collection, batch);
      logProgress(indexed.addAndGet(batch.size()), total, startTime);
    }
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
