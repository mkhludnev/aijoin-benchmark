package com.aijoin.tpchbenchmark;

import java.time.LocalDate;
import java.util.Random;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.FacetParams;
import org.apache.solr.common.params.ModifiableSolrParams;

/**
 * One instance of TPC-H Q4 (spec 2.4.4), with its substitution parameter bound. It renders both the
 * SQL text -- the definition of what is being asked -- and the SolrJ params that answer it; the
 * translation, clause by clause:
 *
 * <pre>
 *   select o_orderpriority, count(*)          rows=0, facet.field=o_orderpriority
 *   from orders                               collection tpch_orders
 *   where o_orderdate >= date ':1'            fq=o_orderdate:[:1 TO :1+3MONTHS}
 *     and o_orderdate < date ':1' + 3 month
 *     and exists (select * from lineitem      q={!join fromIndex=tpch_lineitem
 *                 where l_orderkey = o_orderkey          from=l_orderkey to=o_orderkey}
 *                   and l_commitdate &lt; l_receiptdate)   {!frange l=0 incl=false}
 *                                                           sub(ms(l_receiptdate),ms(l_commitdate))
 *   group by o_orderpriority                  facet.limit=-1, facet.mincount=1
 *   order by o_orderpriority                  facet.sort=index
 * </pre>
 *
 * <p>{@code exists} is a semijoin, which is exactly what a Solr join computes: each order is
 * counted once however many late lineitems it has. The join sits in {@code q}, and the date range
 * in {@code fq}, as in the products/skus benchmark: a join result intersected with a to-side local
 * filter. {@code l_commitdate < l_receiptdate} compares two columns of one row, which an inverted
 * index cannot answer, so it is a function range over docValues -- evaluated for every lineitem on
 * every query. An arm name ending in {@value #FLAG_SUFFIX} (e.g. {@code aijoin-flag}) instead
 * matches {@code l_late:true}, the same predicate precomputed at index time.
 *
 * @param date the Q4 {@code DATE} parameter, the first day of a month
 */
record Q4Query(LocalDate date) {

  /** The late-lineitem predicate, as a from-side query. */
  static final String LATE_LINEITEM =
      "{!frange l=0 incl=false}sub(ms("
          + TpchConstants.L_RECEIPTDATE
          + "),ms("
          + TpchConstants.L_COMMITDATE
          + "))";

  /** The late-lineitem predicate precomputed by the indexer, as a term query. */
  static final String LATE_FLAG = TpchConstants.L_LATE + ":true";

  /** Arm-name suffix selecting {@link #LATE_FLAG} over {@link #LATE_LINEITEM}. */
  static final String FLAG_SUFFIX = "-flag";

  /** Spec 2.4.4.3: first day of a month drawn uniformly from 1993-01 .. 1997-10. */
  static Q4Query random(Random rnd) {
    return new Q4Query(
        TpchConstants.FIRST_DATE.plusMonths(rnd.nextInt(TpchConstants.DATE_MONTHS)));
  }

  /** The executable query text, as qgen would produce it from {@code queries/4.sql}. */
  String sql() {
    return """
        select
        	o_orderpriority,
        	count(*) as order_count
        from
        	orders
        where
        	o_orderdate >= date '%1$s'
        	and o_orderdate < date '%1$s' + interval '3' month
        	and exists (
        		select
        			*
        		from
        			lineitem
        		where
        			l_orderkey = o_orderkey
        			and l_commitdate < l_receiptdate
        	)
        group by
        	o_orderpriority
        order by
        	o_orderpriority;
        """
        .formatted(date);
  }

  /** {@code o_orderdate >= date AND o_orderdate < date + 3 months}: inclusive lower, exclusive upper. */
  String dateFilter() {
    return TpchConstants.O_ORDERDATE
        + ":["
        + date
        + "T00:00:00Z TO "
        + date.plusMonths(3)
        + "T00:00:00Z}";
  }

  /** The {@code exists (...)} subquery as a join from lineitem to orders, via {@code arm}. */
  String existsQuery(String arm) {
    return "{!" + joinLocalParams(arm) + "}" + (arm.endsWith(FLAG_SUFFIX) ? LATE_FLAG : LATE_LINEITEM);
  }

  /** Builds the SolrJ params answering this query, to run against {@link TpchConstants#ORDERS_COLLECTION}. */
  ModifiableSolrParams toSolrParams(String arm) {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set(CommonParams.Q, existsQuery(arm));
    params.set(CommonParams.FQ, dateFilter());
    params.set(CommonParams.ROWS, 0);
    // force an exact numFound: skip Solr's approximate early-termination count
    params.set(CommonParams.MIN_EXACT_COUNT, Integer.MAX_VALUE);
    params.set(FacetParams.FACET, true);
    params.set(FacetParams.FACET_FIELD, TpchConstants.O_ORDERPRIORITY);
    params.set(FacetParams.FACET_SORT, FacetParams.FACET_SORT_INDEX);
    params.set(FacetParams.FACET_LIMIT, -1);
    params.set(FacetParams.FACET_MINCOUNT, 1);
    return params;
  }

  /** The {@code {!...}} local-params fragment joining lineitem to orders for an arm. */
  static String joinLocalParams(String arm) {
    String parser = arm.endsWith(FLAG_SUFFIX) ? arm.substring(0, arm.length() - FLAG_SUFFIX.length()) : arm;
    String strKeys =
        " fromIndex=" + TpchConstants.LINEITEM_COLLECTION
            + " from=" + TpchConstants.L_ORDERKEY
            + " to=" + TpchConstants.O_ORDERKEY;
    return switch (parser) {
      case "join" -> "join score=none" + strKeys;
      case "jointop" -> "join score=none method=topLevelDV" + strKeys;
      case "aijoin" -> "aijoin" + strKeys;
      case "joinnum" -> "join score=none"
          + " fromIndex=" + TpchConstants.LINEITEM_COLLECTION
          + " from=" + TpchConstants.L_ORDERKEY_NUM
          + " to=" + TpchConstants.O_ORDERKEY_NUM;
      default -> throw new IllegalArgumentException(
          "arm must be 'join|jointop|aijoin|joinnum', optionally with a '-flag' suffix: " + arm);
    };
  }
}
