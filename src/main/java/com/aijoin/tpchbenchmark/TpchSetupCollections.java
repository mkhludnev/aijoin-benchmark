package com.aijoin.tpchbenchmark;

import com.aijoin.benchmark.SetupCollections;
import java.util.List;
import org.apache.solr.client.solrj.jetty.CloudJettySolrClient;

/**
 * Creates the two TPC-H Q4 collections, {@code tpch_orders} and {@code tpch_lineitem} (1 shard, 1
 * replica each), from the {@code configsets/tpch_*} resources. Like {@link SetupCollections} it is
 * destructively re-runnable -- but it only ever deletes these two collections and their configsets,
 * so it can share a node with the products/skus benchmark.
 *
 * <p>Usage: {@code tpchSetupCollections <solrBaseUrl>}.
 */
public class TpchSetupCollections {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: tpchSetupCollections <solrBaseUrl>");
      System.exit(1);
    }
    try (CloudJettySolrClient client = new CloudJettySolrClient.Builder(List.of(args[0])).build()) {
      // collections go first: Solr refuses to delete a configset a live collection still uses
      SetupCollections.deleteCollectionIfPresent(client, TpchConstants.ORDERS_COLLECTION);
      SetupCollections.deleteCollectionIfPresent(client, TpchConstants.LINEITEM_COLLECTION);

      // content-named configs: see uploadVersionedConfigSet for the stale-schema cache it avoids
      String ordersConfig =
          SetupCollections.uploadVersionedConfigSet(client, TpchConstants.ORDERS_COLLECTION);
      String lineitemConfig =
          SetupCollections.uploadVersionedConfigSet(client, TpchConstants.LINEITEM_COLLECTION);

      SetupCollections.createCollection(client, TpchConstants.ORDERS_COLLECTION, ordersConfig);
      SetupCollections.createCollection(client, TpchConstants.LINEITEM_COLLECTION, lineitemConfig);
    }
    System.out.println("TPC-H Q4 setup complete.");
  }
}
