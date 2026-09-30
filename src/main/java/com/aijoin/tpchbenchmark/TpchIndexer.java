package com.aijoin.tpchbenchmark;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.solr.client.solrj.jetty.CloudJettySolrClient;
import org.apache.solr.common.SolrInputDocument;

/**
 * Loads dbgen's {@code orders.tbl} and {@code lineitem.tbl} into {@code tpch_orders} and {@code
 * tpch_lineitem}, keeping only the columns Q4 reads. Generate the files at SF=1 with {@code dbgen
 * -s 1 -T o} (orders and lineitem together, so their keys line up) -- see the README.
 *
 * <p>The row counts are checked against SF=1's, so a truncated or wrong-scale file fails the load
 * rather than silently skewing every answer.
 *
 * <p>Usage: {@code tpchIndex <solrBaseUrl> <dirWithTblFiles>}.
 */
public class TpchIndexer {

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: tpchIndex <solrBaseUrl> <dirWithTblFiles>");
      System.exit(1);
    }
    Path dir = Path.of(args[1]);
    try (CloudJettySolrClient client = new CloudJettySolrClient.Builder(List.of(args[0])).build()) {
      long start = System.currentTimeMillis();
      load(client, dir.resolve("orders.tbl"), TpchConstants.ORDERS_COLLECTION,
          TpchConstants.SF1_ORDERS, TpchIndexer::order);
      load(client, dir.resolve("lineitem.tbl"), TpchConstants.LINEITEM_COLLECTION,
          TpchConstants.SF1_LINEITEMS, TpchIndexer::lineitem);
      System.out.printf("Indexing complete in %.1fs%n", (System.currentTimeMillis() - start) / 1000.0);
    }
  }

  /** O_ORDERKEY|O_CUSTKEY|O_ORDERSTATUS|O_TOTALPRICE|O_ORDERDATE|O_ORDERPRIORITY|... */
  static SolrInputDocument order(String[] f) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField(TpchConstants.O_ORDERKEY, f[0]);
    doc.setField(TpchConstants.O_ORDERKEY_NUM, Integer.parseInt(f[0]));
    doc.setField(TpchConstants.O_ORDERDATE, date(f[4]));
    doc.setField(TpchConstants.O_ORDERPRIORITY, f[5]);
    return doc;
  }

  /** L_ORDERKEY|L_PARTKEY|L_SUPPKEY|L_LINENUMBER|...|L_SHIPDATE|L_COMMITDATE|L_RECEIPTDATE|... */
  static SolrInputDocument lineitem(String[] f) {
    SolrInputDocument doc = new SolrInputDocument();
    doc.setField(TpchConstants.L_ID, f[0] + "-" + f[3]);
    doc.setField(TpchConstants.L_ORDERKEY, f[0]);
    doc.setField(TpchConstants.L_ORDERKEY_NUM, Integer.parseInt(f[0]));
    doc.setField(TpchConstants.L_COMMITDATE, date(f[11]));
    doc.setField(TpchConstants.L_RECEIPTDATE, date(f[12]));
    // yyyy-MM-dd strings order the same as the dates they spell
    doc.setField(TpchConstants.L_LATE, f[11].compareTo(f[12]) < 0);
    return doc;
  }

  /** dbgen writes {@code yyyy-MM-dd}; Solr date fields take ISO instants. */
  private static String date(String ymd) {
    return ymd + "T00:00:00Z";
  }

  /**
   * Reads {@code tbl} on this thread and sends batches from {@link TpchConstants#INDEXER_THREADS}
   * workers. The work queue is bounded and overflow runs on the reader, so reading can never get
   * more than a few batches ahead of Solr.
   */
  private static void load(
      CloudJettySolrClient client,
      Path tbl,
      String collection,
      long expectedRows,
      Function<String[], SolrInputDocument> mapper)
      throws Exception {
    System.out.printf("Indexing %s into '%s'...%n", tbl, collection);
    ThreadPoolExecutor pool =
        new ThreadPoolExecutor(
            TpchConstants.INDEXER_THREADS,
            TpchConstants.INDEXER_THREADS,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(TpchConstants.INDEXER_THREADS * 2),
            new ThreadPoolExecutor.CallerRunsPolicy());
    AtomicLong sent = new AtomicLong();
    AtomicReference<Exception> failure = new AtomicReference<>();
    long start = System.currentTimeMillis();
    long rows = 0;
    try (BufferedReader in = Files.newBufferedReader(tbl, StandardCharsets.US_ASCII)) {
      List<SolrInputDocument> batch = new ArrayList<>(TpchConstants.INDEXER_BATCH_SIZE);
      String line;
      while ((line = in.readLine()) != null && failure.get() == null) {
        batch.add(mapper.apply(line.split("\\|")));
        rows++;
        if (batch.size() == TpchConstants.INDEXER_BATCH_SIZE) {
          submit(pool, client, collection, batch, sent, failure, expectedRows, start);
          batch = new ArrayList<>(TpchConstants.INDEXER_BATCH_SIZE);
        }
      }
      if (!batch.isEmpty()) {
        submit(pool, client, collection, batch, sent, failure, expectedRows, start);
      }
    } finally {
      pool.shutdown();
      pool.awaitTermination(1, TimeUnit.HOURS);
    }
    if (failure.get() != null) {
      throw new RuntimeException("Failed indexing into " + collection, failure.get());
    }
    if (rows != expectedRows) {
      throw new IllegalStateException(
          String.format("%s has %,d rows, SF=1 has %,d -- wrong scale factor?", tbl, rows, expectedRows));
    }
    client.commit(collection);
    System.out.printf("'%s': %,d docs committed.%n", collection, rows);
  }

  private static void submit(
      ThreadPoolExecutor pool,
      CloudJettySolrClient client,
      String collection,
      List<SolrInputDocument> batch,
      AtomicLong sent,
      AtomicReference<Exception> failure,
      long total,
      long start) {
    pool.execute(
        () -> {
          try {
            client.add(collection, batch);
          } catch (Exception e) {
            failure.compareAndSet(null, e);
            return;
          }
          long done = sent.addAndGet(batch.size());
          if (done % (TpchConstants.INDEXER_BATCH_SIZE * 100L) < batch.size()) {
            double sec = Math.max((System.currentTimeMillis() - start) / 1000.0, 0.001);
            System.out.printf(
                "  %,d / %,d (%.1f%%), %.0f docs/sec%n", done, total, 100.0 * done / total, done / sec);
          }
        });
  }
}
