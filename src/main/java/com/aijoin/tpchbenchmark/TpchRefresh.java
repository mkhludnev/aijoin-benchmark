package com.aijoin.tpchbenchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.jetty.CloudJettySolrClient;
import org.apache.solr.common.SolrInputDocument;

/**
 * Applies TPC-H refresh pairs (spec 2.6 RF1 New Sales, 2.7 RF2 Old Sales) to the Q4 collections,
 * from the flat files {@code dbgen -s 1 -U <n>} writes: {@code orders.tbl.u<N>} and {@code
 * lineitem.tbl.u<N>} to insert, {@code delete.<N>} holding the order keys to delete. At SF=1 a pair
 * inserts 1500 orders (~5.8-6.1k lineitems) and deletes 1500 others (~6k lineitems).
 *
 * <p>The spec wants an order and its lineitems changed in one transaction; two Solr collections
 * cannot commit together, so the commit order is chosen to keep every intermediate state a valid
 * database as far as Q4 can see: lineitem first -- new lineitems have no order yet, and deleted
 * orders just lose their lineitems, so they drop out of Q4 as if deleted -- then orders.
 *
 * <p>RF1 keys fill the gaps in the base key space and RF2 keys come from the start of it, so the two
 * never meet, and applying pairs 1..r always walks a freshly {@link #reset} index through the same
 * states.
 */
final class TpchRefresh {

  /** Lineitem linenumbers run 1..7 (spec 4.2.3), so an order's lineitem ids are known without a lookup. */
  private static final int MAX_LINENUMBER = 7;

  private TpchRefresh() {}

  /** Number of consecutive refresh pairs 1..n present in {@code dir}. */
  static int availablePairs(Path dir) {
    int n = 0;
    while (Files.exists(dir.resolve("delete." + (n + 1)))
        && Files.exists(dir.resolve("orders.tbl.u" + (n + 1)))
        && Files.exists(dir.resolve("lineitem.tbl.u" + (n + 1)))) {
      n++;
    }
    return n;
  }

  /** Applies refresh pair {@code n}: RF1 and RF2 together, lineitem committed before orders. */
  static void apply(CloudJettySolrClient client, Path dir, int n)
      throws IOException, SolrServerException {
    List<String> deleteKeys = readDeleteKeys(dir.resolve("delete." + n));

    add(client, TpchConstants.LINEITEM_COLLECTION,
        readTbl(dir.resolve("lineitem.tbl.u" + n), TpchIndexer::lineitem));
    client.deleteById(TpchConstants.LINEITEM_COLLECTION, lineitemIds(deleteKeys));
    client.commit(TpchConstants.LINEITEM_COLLECTION);

    add(client, TpchConstants.ORDERS_COLLECTION,
        readTbl(dir.resolve("orders.tbl.u" + n), TpchIndexer::order));
    client.deleteById(TpchConstants.ORDERS_COLLECTION, deleteKeys);
    client.commit(TpchConstants.ORDERS_COLLECTION);
  }

  /**
   * Undoes every refresh pair in {@code refreshDir}, applied or not: deletes all RF1 rows and
   * re-adds the base rows of all RF2 keys from {@code baseDir}'s {@code .tbl} files. Cheap next to a
   * full reload, and it lets separate runs start from the same state.
   */
  static void reset(CloudJettySolrClient client, Path baseDir, Path refreshDir)
      throws IOException, SolrServerException {
    int pairs = availablePairs(refreshDir);
    List<String> insertedKeys = new ArrayList<>();
    Set<String> deletedKeys = new HashSet<>();
    for (int n = 1; n <= pairs; n++) {
      for (SolrInputDocument order : readTbl(refreshDir.resolve("orders.tbl.u" + n), TpchIndexer::order)) {
        insertedKeys.add((String) order.getFieldValue(TpchConstants.O_ORDERKEY));
      }
      deletedKeys.addAll(readDeleteKeys(refreshDir.resolve("delete." + n)));
    }
    System.out.printf("Reset: removing %,d RF1 orders, restoring %,d RF2 orders (%d pairs)...%n",
        insertedKeys.size(), deletedKeys.size(), pairs);

    client.deleteById(TpchConstants.LINEITEM_COLLECTION, lineitemIds(insertedKeys));
    client.deleteById(TpchConstants.ORDERS_COLLECTION, insertedKeys);
    add(client, TpchConstants.LINEITEM_COLLECTION,
        readTbl(baseDir.resolve("lineitem.tbl"), TpchIndexer::lineitem, deletedKeys));
    add(client, TpchConstants.ORDERS_COLLECTION,
        readTbl(baseDir.resolve("orders.tbl"), TpchIndexer::order, deletedKeys));
    client.commit(TpchConstants.LINEITEM_COLLECTION);
    client.commit(TpchConstants.ORDERS_COLLECTION);
  }

  private static List<String> lineitemIds(List<String> orderKeys) {
    List<String> ids = new ArrayList<>(orderKeys.size() * MAX_LINENUMBER);
    for (String key : orderKeys) {
      for (int line = 1; line <= MAX_LINENUMBER; line++) {
        ids.add(key + "-" + line); // ids that don't exist are ignored by deleteById
      }
    }
    return ids;
  }

  /** {@code delete.<N>} holds one {@code <orderkey>|} per line. */
  private static List<String> readDeleteKeys(Path file) throws IOException {
    List<String> keys = new ArrayList<>();
    for (String line : Files.readAllLines(file, StandardCharsets.US_ASCII)) {
      String key = line.split("\\|")[0].trim();
      if (!key.isEmpty()) {
        keys.add(key);
      }
    }
    return keys;
  }

  private static List<SolrInputDocument> readTbl(
      Path file, Function<String[], SolrInputDocument> mapper) throws IOException {
    return readTbl(file, mapper, null);
  }

  /** Maps every row of a dbgen file, or only those whose first column is in {@code keys}. */
  private static List<SolrInputDocument> readTbl(
      Path file, Function<String[], SolrInputDocument> mapper, Set<String> keys) throws IOException {
    List<SolrInputDocument> docs = new ArrayList<>();
    try (BufferedReader in = Files.newBufferedReader(file, StandardCharsets.US_ASCII)) {
      String line;
      while ((line = in.readLine()) != null) {
        if (keys != null && !keys.contains(line.substring(0, line.indexOf('|')))) {
          continue;
        }
        docs.add(mapper.apply(line.split("\\|")));
      }
    }
    return docs;
  }

  private static void add(CloudJettySolrClient client, String collection, List<SolrInputDocument> docs)
      throws IOException, SolrServerException {
    for (int from = 0; from < docs.size(); from += TpchConstants.INDEXER_BATCH_SIZE) {
      client.add(collection, docs.subList(from, Math.min(docs.size(), from + TpchConstants.INDEXER_BATCH_SIZE)));
    }
  }
}
