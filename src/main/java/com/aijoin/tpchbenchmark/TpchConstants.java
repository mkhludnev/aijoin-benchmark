package com.aijoin.tpchbenchmark;

import java.time.LocalDate;
import java.util.Map;
import java.util.TreeMap;

/**
 * Collection names, field names and query parameters for the TPC-H Q4 benchmark. Only the columns
 * Q4 reads are indexed: ORDERS(o_orderkey, o_orderdate, o_orderpriority) and LINEITEM(l_orderkey,
 * l_commitdate, l_receiptdate) -- see {@code configsets/tpch_*}.
 */
final class TpchConstants {

  private TpchConstants() {}

  static final String ORDERS_COLLECTION = "tpch_orders";
  static final String LINEITEM_COLLECTION = "tpch_lineitem";

  // orders fields -- the "to" side of the Q4 semijoin
  static final String O_ORDERKEY = "o_orderkey";
  static final String O_ORDERKEY_NUM = "o_orderkey_num";
  static final String O_ORDERDATE = "o_orderdate";
  static final String O_ORDERPRIORITY = "o_orderpriority";

  // lineitem fields -- the "from" side
  static final String L_ID = "id";
  static final String L_ORDERKEY = "l_orderkey";
  static final String L_ORDERKEY_NUM = "l_orderkey_num";
  static final String L_COMMITDATE = "l_commitdate";
  static final String L_RECEIPTDATE = "l_receiptdate";
  /** {@code l_commitdate < l_receiptdate}, precomputed by the indexer. */
  static final String L_LATE = "l_late";

  /** Row counts dbgen produces at SF=1; the indexer checks it read exactly this many. */
  static final long SF1_ORDERS = 1_500_000L;
  static final long SF1_LINEITEMS = 6_001_215L;

  /**
   * Spec 2.4.4.3: DATE is the first day of a randomly selected month between the first month of
   * 1993 and the 10th month of 1997 -- 58 months in all.
   */
  static final LocalDate FIRST_DATE = LocalDate.of(1993, 1, 1);
  static final int DATE_MONTHS = 58;

  /** Spec 2.4.4.4: the validation run's DATE and the answer it must produce (answers/q4.out). */
  static final LocalDate VALIDATION_DATE = LocalDate.of(1993, 7, 1);
  static final Map<String, Long> VALIDATION_ANSWER =
      new TreeMap<>(
          Map.of(
              "1-URGENT", 10594L,
              "2-HIGH", 10476L,
              "3-MEDIUM", 10410L,
              "4-NOT SPECIFIED", 10556L,
              "5-LOW", 10487L));

  /** Deterministic seed for the DATE draws, so query k is the same in every run. */
  static final long RANDOM_SEED = 20260930L;

  static final int INDEXER_BATCH_SIZE = 1000;
  static final int INDEXER_THREADS = 4;
}
