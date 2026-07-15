package com.aijoin.benchmark;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.jetty.CloudJettySolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.ConfigSetAdminRequest;

/**
 * Startup script: creates the {@code products} and {@code skus} collections on an already-running,
 * unauthenticated Solr node -- single shard, one replica each, config uploaded from the {@code
 * configsets/} resources bundled in this project. Idempotent: safe to re-run against an already
 * set up cluster (uploads overwrite, collection creation is skipped if the collection exists).
 *
 * <p>Usage: {@code setupCollections <solrBaseUrl>}, e.g. {@code http://localhost:8983/solr}.
 *
 * <p>One thing this script deliberately does <em>not</em> do: enable Solr's per-segment concurrent
 * search ("segment parallel search"). That's the {@code indexSearcherExecutorThreads} setting in
 * {@code solr.xml} -- a node-level setting read at Solr startup, driven by the {@code
 * solr.searchThreads} system property. It can't be changed remotely through the client APIs used
 * here; start Solr itself with {@code -Dsolr.searchThreads=4} (e.g. via {@code SOLR_OPTS} in {@code
 * solr.in.sh}, or {@code bin/solr start -a "-Dsolr.searchThreads=4"}) before running this script.
 */
public class SetupCollections {

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: setupCollections <solrBaseUrl>");
      System.err.println("  e.g. setupCollections http://localhost:8983/solr");
      System.exit(1);
    }
    String solrUrl = args[0];

    System.out.println(
        "NOTE: segment parallel search (indexSearcherExecutorThreads=4) must be enabled on the"
            + " Solr server itself via -Dsolr.searchThreads=4 at startup; this script cannot set"
            + " it remotely. See the class javadoc for details.");

    try (CloudJettySolrClient client = new CloudJettySolrClient.Builder(List.of(solrUrl)).build()) {
      uploadConfigSet(client, "products");
      uploadConfigSet(client, "skus");

      createCollectionIfAbsent(client, Constants.PRODUCTS_COLLECTION, "products");
      createCollectionIfAbsent(client, Constants.SKUS_COLLECTION, "skus");
    }

    System.out.println("Setup complete.");
  }

  private static void createCollectionIfAbsent(
      CloudJettySolrClient client, String collection, String configName)
      throws SolrServerException, IOException {
    List<String> existing = CollectionAdminRequest.listCollections(client);
    if (existing.contains(collection)) {
      System.out.println("Collection '" + collection + "' already exists, skipping creation.");
      return;
    }
    System.out.println("Creating collection '" + collection + "' (1 shard, 1 replica)...");
    CollectionAdminRequest.createCollection(collection, configName, 1, 1).process(client);
    System.out.println("Created '" + collection + "'.");
  }

  /** Zips {@code configsets/<name>/conf} off the classpath and uploads it as config {@code name}. */
  private static void uploadConfigSet(CloudJettySolrClient client, String name)
      throws IOException, SolrServerException, URISyntaxException {
    URL confUrl = SetupCollections.class.getClassLoader().getResource("configsets/" + name + "/conf");
    if (confUrl == null) {
      throw new IllegalStateException("configset resource not found on classpath: " + name);
    }
    Path confDir = Paths.get(confUrl.toURI());

    Path zip = Files.createTempFile("aijoin-benchmark-" + name + "-", ".zip");
    try {
      zipDirectory(confDir, zip);
      System.out.println("Uploading configset '" + name + "'...");
      new ConfigSetAdminRequest.Upload()
          .setConfigSetName(name)
          .setUploadFile(zip, "application/zip")
          .setOverwrite(true)
          .process(client);
    } finally {
      Files.deleteIfExists(zip);
    }
  }

  private static void zipDirectory(Path sourceDir, Path targetZip) throws IOException {
    try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(targetZip))) {
      try (var files = Files.walk(sourceDir)) {
        for (Path file : (Iterable<Path>) files.filter(Files::isRegularFile)::iterator) {
          zos.putNextEntry(new ZipEntry(sourceDir.relativize(file).toString()));
          Files.copy(file, zos);
          zos.closeEntry();
        }
      }
    }
  }
}
