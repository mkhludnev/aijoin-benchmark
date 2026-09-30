package com.aijoin.benchmark;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
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
 * configsets/} resources bundled in this project. Re-runnable, and destructively so: every
 * collection and configset it owns is deleted first, then recreated from scratch, so nothing a
 * previous run left in ZooKeeper can survive into this one and the bundled schema.xml is always
 * what Solr ends up reading. Indexed data is dropped with the collections -- re-index afterwards.
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
      // collections go first: Solr refuses to delete a configset a live collection still uses
      deleteCollectionIfPresent(client, Constants.PRODUCTS_COLLECTION);
      deleteCollectionIfPresent(client, Constants.SKUS_COLLECTION);
      deleteCollectionIfPresent(client, Constants.PRODSKUS_COLLECTION);

      uploadConfigSet(client, "products");
      uploadConfigSet(client, "skus");

      createCollection(client, Constants.PRODUCTS_COLLECTION, "products");
      createCollection(client, Constants.SKUS_COLLECTION, "skus");
      createCollection(client, Constants.PRODSKUS_COLLECTION, "skus");
    }

    System.out.println("Setup complete.");
  }

  public static void deleteCollectionIfPresent(CloudJettySolrClient client, String collection)
      throws SolrServerException, IOException {
    if (CollectionAdminRequest.listCollections(client).contains(collection)) {
      System.out.println("Deleting collection '" + collection + "'...");
      CollectionAdminRequest.deleteCollection(collection).process(client);
    }
  }

  public static void createCollection(
      CloudJettySolrClient client, String collection, String configName)
      throws SolrServerException, IOException {
    System.out.println("Creating collection '" + collection + "' (1 shard, 1 replica)...");
    CollectionAdminRequest.createCollection(collection, configName, 1, 1).process(client);
    System.out.println("Created '" + collection + "'.");
  }

  /**
   * Drops the config from ZooKeeper so the upload that follows starts from nothing.
   *
   * <p>Uploading over a config that already exists is not enough, whatever the overwrite/cleanup
   * flags say. Plain overwrite replaces only the files in the zip and leaves everything else --
   * that's how a managed-schema.xml, generated back when solrconfig.xml still had no explicit
   * ClassicIndexSchemaFactory, kept being read in place of schema.xml and silently swallowed every
   * schema edit. And cleanup=true, which is supposed to remove exactly those leftovers, deletes the
   * freshly uploaded schema.xml too whenever a schema.xml.bak sits next to it: Solr's
   * ZkConfigSetService#getAllConfigFiles flags an entry as a directory when the previously visited
   * sibling's name merely starts with it, so schema.xml comes back as "schema.xml/", doesn't match
   * the uploaded "schema.xml", and gets cleaned away. Deleting the whole config sidesteps both.
   */
  private static void deleteConfigSetIfPresent(CloudJettySolrClient client, String name)
      throws SolrServerException, IOException {
    if (new ConfigSetAdminRequest.List().process(client).getConfigSets().contains(name)) {
      System.out.println("Deleting configset '" + name + "'...");
      new ConfigSetAdminRequest.Delete().setConfigSetName(name).process(client);
    }
  }

  /**
   * Zips {@code configsets/<name>/conf} off the classpath and uploads it as config {@code name},
   * replacing whatever config of that name was there before.
   */
  public static void uploadConfigSet(CloudJettySolrClient client, String name)
      throws IOException, SolrServerException, URISyntaxException {
    URL confUrl = SetupCollections.class.getClassLoader().getResource("configsets/" + name + "/conf");
    if (confUrl == null) {
      throw new IllegalStateException("configset resource not found on classpath: " + name);
    }
    Path confDir = Paths.get(confUrl.toURI());

    Path zip = Files.createTempFile("aijoin-benchmark-" + name + "-", ".zip");
    try {
      zipDirectory(confDir, zip);
      deleteConfigSetIfPresent(client, name);
      System.out.println("Uploading configset '" + name + "'...");
      new ConfigSetAdminRequest.Upload()
          .setConfigSetName(name)
          .setUploadFile(zip, "application/zip")
          .process(client);
    } finally {
      Files.deleteIfExists(zip);
    }
  }

  /**
   * Uploads {@code configsets/<name>/conf} as config {@code <name>-<content hash>}, dropping every
   * other {@code <name>-*} config, and returns the uploaded name.
   *
   * <p>Deleting and re-uploading a config under one fixed name, as {@link #uploadConfigSet} does, is
   * not enough to get an edited schema.xml read: Solr caches parsed schemas keyed by config name
   * and schema znode version, a fresh upload restarts that version at 0, and so the new collection
   * is handed the previous upload's cached schema -- ZooKeeper holds the edit, the core's file
   * reads back the edit, a RELOAD keeps the stale schema all the same. Naming the config after its
   * content makes every edit a new cache key. Call it only once no collection uses the old configs.
   */
  public static String uploadVersionedConfigSet(CloudJettySolrClient client, String name)
      throws IOException, SolrServerException, URISyntaxException {
    URL confUrl = SetupCollections.class.getClassLoader().getResource("configsets/" + name + "/conf");
    if (confUrl == null) {
      throw new IllegalStateException("configset resource not found on classpath: " + name);
    }
    Path confDir = Paths.get(confUrl.toURI());
    String configName = name + "-" + contentHash(confDir);

    for (String existing : new ConfigSetAdminRequest.List().process(client).getConfigSets()) {
      if (existing.equals(name) || existing.startsWith(name + "-")) {
        System.out.println("Deleting configset '" + existing + "'...");
        new ConfigSetAdminRequest.Delete().setConfigSetName(existing).process(client);
      }
    }
    Path zip = Files.createTempFile("aijoin-benchmark-" + name + "-", ".zip");
    try {
      zipDirectory(confDir, zip);
      System.out.println("Uploading configset '" + configName + "'...");
      new ConfigSetAdminRequest.Upload()
          .setConfigSetName(configName)
          .setUploadFile(zip, "application/zip")
          .process(client);
    } finally {
      Files.deleteIfExists(zip);
    }
    return configName;
  }

  /** First 8 hex digits of a SHA-256 over every file's relative path and bytes, in path order. */
  private static String contentHash(Path dir) throws IOException {
    try (var files = Files.walk(dir)) {
      MessageDigest sha = MessageDigest.getInstance("SHA-256");
      for (Path file : (Iterable<Path>) files.filter(Files::isRegularFile).sorted()::iterator) {
        sha.update(dir.relativize(file).toString().getBytes(StandardCharsets.UTF_8));
        sha.update(Files.readAllBytes(file));
      }
      return HexFormat.of().formatHex(sha.digest()).substring(0, 8);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
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
