package com.aijoin.tpchbenchmark;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The SQL baseline for {@link TpchSearcher}: executes the very SQL text {@link Q4Query#sql()}
 * generates, over JDBC, for the same seeded query list, and writes the same CSV -- so {@code
 * ./gradlew search -Pargs="compare ..."} diffs a PostgreSQL run against any Solr arm.
 *
 * <p>There is no server-side QTime over JDBC, so {@code qtime_ms} holds the client wall time too;
 * compare it against the Solr arms' {@code wall_ms}, which carries the same network round trip.
 * The median {@code select 1} round trip is printed so it can be subtracted.
 *
 * <p>Usage: {@code pgSearch <jdbcUrl> <label> <queryCount> [concurrency] [warmupCount]}, e.g.
 * {@code jdbc:postgresql://localhost:15432/tpch?user=gedel&password=tpch bare 50 1 5}; {@code
 * label} names the database setup under test (indexes, settings) in the CSV file name.
 */
public class PgSearcher {

  public static void main(String[] args) throws Exception {
    if (args.length < 3 || args.length > 5) {
      System.err.println("Usage: pgSearch <jdbcUrl> <label> <queryCount> [concurrency] [warmupCount]");
      System.exit(1);
    }
    String jdbcUrl = args[0];
    String label = "pg-" + args[1];
    int queryCount = Integer.parseInt(args[2]);
    int concurrency = args.length > 3 ? Integer.parseInt(args[3]) : 1;
    int warmupCount = args.length > 4 ? Integer.parseInt(args[4]) : 0;

    // same seeds as TpchSearcher, so query k is the same Q4 instance in both
    List<Q4Query> warmup = TpchSearcher.generateQueries(warmupCount, TpchConstants.RANDOM_SEED + 1L);
    List<Q4Query> measured = TpchSearcher.generateQueries(queryCount, TpchConstants.RANDOM_SEED);

    List<Connection> connections = new ArrayList<>();
    try {
      for (int w = 0; w < concurrency; w++) {
        connections.add(DriverManager.getConnection(jdbcUrl));
      }
      System.out.printf("network round trip (select 1, median of 20): %d ms%n",
          roundTripMs(connections.get(0)));
      if (warmupCount > 0) {
        System.out.printf("Warming up: %d queries, concurrency %d (results discarded)...%n",
            warmupCount, concurrency);
        run(connections, warmup);
      }
      System.out.printf("Running TPC-H Q4 SQL on %s: %d queries, concurrency %d...%n",
          label, queryCount, concurrency);
      long startNanos = System.nanoTime();
      TpchSearcher.Result[] results = run(connections, measured);
      double elapsedSec = (System.nanoTime() - startNanos) / 1e9;

      Path csv = Path.of(String.format("results-tpch-q4-%s-c%d.csv", label, concurrency));
      TpchSearcher.writeCsv(csv, results);
      TpchSearcher.printSummary(label, concurrency, results, elapsedSec, csv);
    } finally {
      for (Connection c : connections) {
        c.close();
      }
    }
  }

  /** One connection per worker: a JDBC connection runs one statement at a time. */
  static TpchSearcher.Result[] run(List<Connection> connections, List<Q4Query> queries)
      throws InterruptedException {
    TpchSearcher.Result[] results = new TpchSearcher.Result[queries.size()];
    AtomicInteger cursor = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(connections.size());
    for (Connection connection : connections) {
      pool.execute(
          () -> {
            int i;
            while ((i = cursor.getAndIncrement()) < queries.size()) {
              results[i] = runOneQuery(connection, i, queries.get(i));
            }
          });
    }
    pool.shutdown();
    if (!pool.awaitTermination(1, TimeUnit.HOURS)) {
      pool.shutdownNow();
      throw new IllegalStateException("benchmark did not finish within 1h");
    }
    return results;
  }

  static TpchSearcher.Result runOneQuery(Connection connection, int index, Q4Query query) {
    long t0 = System.nanoTime();
    try (Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery(query.sql())) {
      Map<String, Long> counts = new LinkedHashMap<>();
      long total = 0;
      while (rs.next()) {
        // o_orderpriority is CHAR(15): trim the padding to match Solr's facet values
        counts.put(rs.getString(1).trim(), rs.getLong(2));
        total += rs.getLong(2);
      }
      int wallMs = (int) ((System.nanoTime() - t0) / 1_000_000L);
      return new TpchSearcher.Result(index, query.date(), wallMs, wallMs, total, counts, null);
    } catch (SQLException e) {
      long wallMs = (System.nanoTime() - t0) / 1_000_000L;
      System.err.println("Query " + index + " failed: " + e);
      return new TpchSearcher.Result(
          index, query.date(), -1, wallMs, -1L, Map.of(), e.toString().replace(',', ';'));
    }
  }

  private static long roundTripMs(Connection connection) throws SQLException {
    long[] rtt = new long[20];
    try (Statement st = connection.createStatement()) {
      for (int i = 0; i < rtt.length; i++) {
        long t0 = System.nanoTime();
        st.execute("select 1");
        rtt[i] = (System.nanoTime() - t0) / 1_000_000L;
      }
    }
    Arrays.sort(rtt);
    return rtt[rtt.length / 2];
  }
}
