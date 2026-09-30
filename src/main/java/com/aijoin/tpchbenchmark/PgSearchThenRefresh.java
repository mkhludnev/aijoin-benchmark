package com.aijoin.tpchbenchmark;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.postgresql.PGConnection;

/**
 * The PostgreSQL side of {@link TpchSearchThenRefresh}: the same refresh pairs from the same dbgen
 * files, the same seeded Q4 instances, the same cold/warm/twin rounds and CSV, so {@code
 * tpchSearchThenRefresh --summarize} reads its report and its rows line up with the Solr arms'
 * query for query.
 *
 * <p>Each refresh pair is one transaction, as spec 2.5.2 asks: {@code COPY} the RF1 orders and
 * lineitems, {@code DELETE} the RF2 orders' lineitems and orders. The reset deletes every RF1 and
 * RF2 key, re-copies the RF2 base rows, then {@code VACUUM ANALYZE}s, so every run starts from the
 * base state with no dead tuples.
 *
 * <p>There is no server-side QTime over JDBC: {@code qtime_ms} is the client wall time, which
 * includes the network round trip printed at startup.
 *
 * <p>Usage: {@code pgSearchThenRefresh <jdbcUrl> <label> [repeat=30] [queryCount=20]
 * [tblDir=build/tpch-sf1]}; the label names the database setup, e.g. {@code pk}.
 */
public class PgSearchThenRefresh {

  public static void main(String[] args) throws Exception {
    if (args.length < 2 || args.length > 5) {
      System.err.println("Usage: pgSearchThenRefresh <jdbcUrl> <label> [repeat=30] [queryCount=20] [tblDir=build/tpch-sf1]");
      System.exit(1);
    }
    String jdbcUrl = args[0];
    String arm = "pg-" + args[1];
    int repeat = args.length > 2 ? Integer.parseInt(args[2]) : 30;
    int queryCount = args.length > 3 ? Integer.parseInt(args[3]) : 20;
    Path tblDir = Path.of(args.length > 4 ? args[4] : "build/tpch-sf1");
    Path refreshDir = tblDir.resolve("refresh");
    if (TpchRefresh.availablePairs(refreshDir) < repeat) {
      throw new IllegalArgumentException(refreshDir + " holds fewer than " + repeat + " refresh pairs");
    }

    Path reportDir = Path.of("reports", String.format("%s-tpchq4-%s-%d",
        LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyMMdd-HHmm")), arm, repeat));
    Files.createDirectories(reportDir);
    Files.writeString(reportDir.resolve("args.txt"), String.join(" ", args) + System.lineSeparator());
    System.out.println("Reporting to " + reportDir);

    try (Connection c = DriverManager.getConnection(jdbcUrl)) {
      reset(c, tblDir, refreshDir);
      for (int round = 1; round <= repeat; round++) {
        apply(c, refreshDir, round);
        checkOrderCount(c);

        List<Q4Query> queries = TpchSearcher.generateQueries(queryCount, TpchConstants.RANDOM_SEED + round);
        List<TpchSearcher.Result> results = new ArrayList<>(queryCount + 1);
        for (int i = 0; i < queryCount; i++) {
          results.add(PgSearcher.runOneQuery(c, i, queries.get(i)));
        }
        results.add(PgSearcher.runOneQuery(c, queryCount, queries.get(0)));
        TpchSearcher.writeCsv(reportDir.resolve("searchrefresh-" + arm + ".csv"), round, results,
            r -> r.index() == 0 ? TpchSearchThenRefresh.COLD
                : r.index() == queryCount ? TpchSearchThenRefresh.TWIN : TpchSearchThenRefresh.WARM,
            true);

        int[] warm = results.subList(1, queryCount).stream().mapToInt(TpchSearcher.Result::qTimeMs).sorted().toArray();
        System.out.printf("round %2d/%d %-13s cold=%5d ms  warm p50=%4d max=%4d  twin=%4d  numFound=%d%n",
            round, repeat, arm, results.get(0).qTimeMs(), warm[warm.length / 2], warm[warm.length - 1],
            results.get(queryCount).qTimeMs(), results.get(0).numFound());
      }
      printDeadTuples(c);
    }
    String summary = TpchSearchThenRefresh.summarize(reportDir, 2);
    Files.writeString(reportDir.resolve("summary.txt"), summary);
    System.out.println();
    System.out.print(summary);
  }

  /** RF1 + RF2 for pair {@code n}, in one transaction. */
  static void apply(Connection c, Path refreshDir, int n) throws SQLException, IOException {
    List<Long> deleteKeys = readDeleteKeys(refreshDir.resolve("delete." + n));
    c.setAutoCommit(false);
    try {
      copy(c, "orders", Files.readAllLines(refreshDir.resolve("orders.tbl.u" + n), StandardCharsets.US_ASCII));
      copy(c, "lineitem", Files.readAllLines(refreshDir.resolve("lineitem.tbl.u" + n), StandardCharsets.US_ASCII));
      deleteByOrderKey(c, deleteKeys);
      c.commit();
    } catch (SQLException | IOException e) {
      c.rollback();
      throw e;
    } finally {
      c.setAutoCommit(true);
    }
  }

  /** Back to the base state whatever pairs were applied before, then VACUUM ANALYZE. */
  static void reset(Connection c, Path tblDir, Path refreshDir) throws SQLException, IOException {
    int pairs = TpchRefresh.availablePairs(refreshDir);
    Set<Long> restore = new LinkedHashSet<>();
    List<Long> all = new ArrayList<>();
    for (int n = 1; n <= pairs; n++) {
      for (String line : Files.readAllLines(refreshDir.resolve("orders.tbl.u" + n), StandardCharsets.US_ASCII)) {
        all.add(Long.parseLong(line.substring(0, line.indexOf('|'))));
      }
      restore.addAll(readDeleteKeys(refreshDir.resolve("delete." + n)));
    }
    all.addAll(restore);
    System.out.printf("Reset: removing RF1 rows, restoring %,d RF2 orders (%d pairs)...%n", restore.size(), pairs);
    c.setAutoCommit(false);
    try {
      deleteByOrderKey(c, all); // RF2 keys too, so re-copying their base rows can't hit the primary keys
      copy(c, "orders", baseRows(tblDir.resolve("orders.tbl"), restore));
      copy(c, "lineitem", baseRows(tblDir.resolve("lineitem.tbl"), restore));
      c.commit();
    } catch (SQLException | IOException e) {
      c.rollback();
      throw e;
    } finally {
      c.setAutoCommit(true);
    }
    try (Statement st = c.createStatement()) {
      st.execute("vacuum analyze orders");
      st.execute("vacuum analyze lineitem");
    }
    checkOrderCount(c);
  }

  private static void deleteByOrderKey(Connection c, List<Long> keys) throws SQLException {
    Long[] array = keys.toArray(new Long[0]);
    try (PreparedStatement li = c.prepareStatement("delete from lineitem where l_orderkey = any(?)");
        PreparedStatement o = c.prepareStatement("delete from orders where o_orderkey = any(?)")) {
      li.setArray(1, c.createArrayOf("integer", array));
      li.executeUpdate();
      o.setArray(1, c.createArrayOf("integer", array));
      o.executeUpdate();
    }
  }

  /** COPYs dbgen rows, dropping the trailing '|' dbgen writes after the last column. */
  private static void copy(Connection c, String table, List<String> tblLines) throws SQLException, IOException {
    StringBuilder data = new StringBuilder();
    for (String line : tblLines) {
      data.append(line, 0, line.endsWith("|") ? line.length() - 1 : line.length()).append('\n');
    }
    c.unwrap(PGConnection.class).getCopyAPI()
        .copyIn("copy " + table + " from stdin with (format text, delimiter '|')", new StringReader(data.toString()));
  }

  private static List<String> baseRows(Path tbl, Set<Long> keys) throws IOException {
    List<String> rows = new ArrayList<>();
    try (BufferedReader in = Files.newBufferedReader(tbl, StandardCharsets.US_ASCII)) {
      String line;
      while ((line = in.readLine()) != null) {
        if (keys.contains(Long.parseLong(line.substring(0, line.indexOf('|'))))) {
          rows.add(line);
        }
      }
    }
    return rows;
  }

  private static List<Long> readDeleteKeys(Path file) throws IOException {
    List<Long> keys = new ArrayList<>();
    for (String line : Files.readAllLines(file, StandardCharsets.US_ASCII)) {
      String key = line.split("\\|")[0].trim();
      if (!key.isEmpty()) {
        keys.add(Long.parseLong(key));
      }
    }
    return keys;
  }

  private static void checkOrderCount(Connection c) throws SQLException {
    try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("select count(*) from orders")) {
      rs.next();
      if (rs.getLong(1) != TpchConstants.SF1_ORDERS) {
        throw new IllegalStateException("orders holds " + rs.getLong(1) + " rows, expected " + TpchConstants.SF1_ORDERS);
      }
    }
  }

  /** Dead tuples left by the run, and whether autovacuum/autoanalyze stepped in during it. */
  private static void printDeadTuples(Connection c) throws SQLException {
    try (Statement st = c.createStatement();
        ResultSet rs = st.executeQuery(
            "select relname, n_live_tup, n_dead_tup, last_autovacuum, last_autoanalyze"
                + " from pg_stat_user_tables order by relname")) {
      while (rs.next()) {
        System.out.printf("%s: live=%d dead=%d last_autovacuum=%s last_autoanalyze=%s%n",
            rs.getString(1), rs.getLong(2), rs.getLong(3), rs.getTimestamp(4), rs.getTimestamp(5));
      }
    }
  }
}
