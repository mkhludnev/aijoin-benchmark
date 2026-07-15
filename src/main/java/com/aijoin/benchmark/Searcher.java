package com.aijoin.benchmark;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.solr.client.solrj.jetty.CloudJettySolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * Fires one {@code products} query per second (fixed rate, independent of how long each query
 * takes) for a fixed duration, joining to {@code skus} either via {@code {!join score=none}} or
 * {@code {!aijoin}}, and reports QTime and numFound stats at the end. Every query filters skus by a
 * few random Color <em>or</em> Size values (never both -- see {@link #buildFromFilter}) joined back
 * to products, further filtered by a few random brands on the products side directly (as an {@code
 * fq}, not through the join).
 *
 * <p>Usage: {@code search <solrBaseUrl> <join|aijoin> <seconds>}.
 */
public class Searcher {

  private static final int MIN_VALUES_PER_FILTER = 2;
  private static final int MAX_VALUES_PER_FILTER = 4;
  private static final int MIN_BRANDS = 1;
  private static final int MAX_BRANDS = 3;

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: search <solrBaseUrl> <join|aijoin> <seconds>");
      System.exit(1);
    }
    String solrUrl = args[0];
    String parser = args[1];
    int seconds = Integer.parseInt(args[2]);
    String localParams =
        switch (parser) {
          case "join" -> "join score=none";
          case "aijoin" -> "aijoin";
          default -> throw new IllegalArgumentException("parser must be 'join' or 'aijoin': " + parser);
        };

    try (CloudJettySolrClient client = new CloudJettySolrClient.Builder(List.of(solrUrl)).build()) {
      Stats stats = new Stats();
      Random rnd = new Random(Constants.RANDOM_SEED);

      ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
      Runnable task = () -> runOneQuery(client, localParams, rnd, stats);
      System.out.printf(
          "Running {!%s} against '%s' for %ds, one query/second...%n",
          localParams, solrUrl, seconds);
      scheduler.scheduleAtFixedRate(task, 0, 1, TimeUnit.SECONDS);

      Thread.sleep(seconds * 1000L);

      scheduler.shutdown();
      scheduler.awaitTermination(5, TimeUnit.SECONDS);

      stats.printSummary(parser);
    }
  }

  private static void runOneQuery(
      CloudJettySolrClient client, String localParams, Random rnd, Stats stats) {
    try {
      String fromFilter = buildFromFilter(rnd);
      String brandFilter = buildBrandFilter(rnd);
      String q =
          "{!"
              + localParams
              + " fromIndex="
              + Constants.SKUS_COLLECTION
              + " from="
              + Constants.PRODUCT_ID_FK
              + " to="
              + Constants.PRODUCT_ID
              + "}"
              + fromFilter;

      ModifiableSolrParams params = new ModifiableSolrParams();
      params.set(CommonParams.Q, q);
      params.set(CommonParams.ROWS, 0);
      params.set(CommonParams.FQ, brandFilter);
      // force an exact numFound: skip Solr's approximate early-termination count
      params.set(CommonParams.MIN_EXACT_COUNT, Integer.MAX_VALUE);

      QueryResponse rsp = client.query(Constants.PRODUCTS_COLLECTION, params);
      stats.record(rsp.getQTime(), rsp.getResults().getNumFound());
    } catch (Exception e) {
      stats.recordError(e);
    }
  }

  /** Picks Color OR Size (never both), then a few distinct values from that one vocabulary. */
  private static String buildFromFilter(Random rnd) {
    boolean useColor = rnd.nextBoolean();
    String field = useColor ? Constants.COLOR_KEYWORD : Constants.SIZE_KEYWORD;
    List<String> vocab = useColor ? Constants.COLORS : Constants.SIZES;
    return field + ":" + orClause(pickDistinct(rnd, vocab, randomCount(rnd, MIN_VALUES_PER_FILTER, MAX_VALUES_PER_FILTER)));
  }

  private static String buildBrandFilter(Random rnd) {
    return Constants.BRAND
        + ":"
        + orClause(pickDistinct(rnd, Constants.BRANDS, randomCount(rnd, MIN_BRANDS, MAX_BRANDS)));
  }

  private static int randomCount(Random rnd, int min, int max) {
    return min + rnd.nextInt(max - min + 1);
  }

  private static List<String> pickDistinct(Random rnd, List<String> vocab, int n) {
    n = Math.min(n, vocab.size());
    LinkedHashSet<String> picked = new LinkedHashSet<>();
    while (picked.size() < n) {
      picked.add(vocab.get(rnd.nextInt(vocab.size())));
    }
    return new ArrayList<>(picked);
  }

  private static String orClause(List<String> values) {
    return "(" + values.stream().map(v -> "\"" + v + "\"").collect(Collectors.joining(" OR ")) + ")";
  }

  /** Accumulates QTime/numFound across the (serial, single-scheduler-thread) run. */
  private static final class Stats {
    private final List<Integer> qTimes = new ArrayList<>();
    private final List<Long> numFounds = new ArrayList<>();
    private int errors = 0;

    void record(Integer qTime, long numFound) {
      qTimes.add(qTime == null ? -1 : qTime);
      numFounds.add(numFound);
    }

    void recordError(Exception e) {
      errors++;
      System.err.println("Query failed: " + e);
    }

    void printSummary(String parser) {
      System.out.println();
      System.out.println("==== {!" + parser + "} results ====");
      System.out.printf("queries run : %d (errors: %d)%n", qTimes.size(), errors);
      if (qTimes.isEmpty()) {
        return;
      }
      var qtStats = qTimes.stream().mapToInt(Integer::intValue).summaryStatistics();
      var nfStats = numFounds.stream().mapToLong(Long::longValue).summaryStatistics();
      System.out.printf(
          "QTime (ms)  : min=%d max=%d avg=%.1f%n", qtStats.getMin(), qtStats.getMax(), qtStats.getAverage());
      System.out.printf(
          "numFound    : min=%d max=%d avg=%.1f%n", nfStats.getMin(), nfStats.getMax(), nfStats.getAverage());
    }
  }
}
