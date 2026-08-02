# aijoin-benchmark

Compares `{!join score=none fromIndex=skus}` against `{!aijoin fromIndex=skus}` on a
`products`/`skus` cross-collection join, against an already-running SolrCloud node.

`{!aijoin}` is `org.apache.solr.search.join.AIJoinQParserPlugin`, currently only available on a
custom Solr build (it's not in any released Solr version) -- point this benchmark at a `bin/solr`
started from the patched build.

## Data model

- `products` (1,000,000 docs): `ID` (unique key, keyword+docValues), `title` (searchable text),
  `brand` (searchable text).
- `skus` (10,000,000 docs): `ID` (unique key), `PRODUCT_ID_FK` (single-valued FK to a product's
  `ID`), `color_s`/`color_t` and `size_s`/`size_t` (keyword+docValues / searchable text pairs,
  `*_t` copied from `*_s`), `inventory_stock` (searchable int).

The join always goes `skus.PRODUCT_ID_FK -> products.ID`: many skus per product, each with exactly
one product. `AIJoinIndex`'s doc mapping keeps only one to-doc per from-doc (see
`AIJoinUtil#computeDocMapping`'s javadoc), so this direction is required -- the reverse (one product
resolving to many skus) would silently drop matches. Both `PRODUCT_ID_FK` and `ID` need real
per-segment docValues, which `{!aijoin}` reads directly (unlike `{!join}`, which also tolerates
uninverted fields); see `configsets/*/conf/schema.xml`.

See `Constants.java` for the exact field names, vocabularies (brands/colors/sizes), and counts --
the indexer and searcher both read from it, so they always agree on what data exists without the
searcher having to query for it first.

## Raising heap

add into `bin\solr.in.sh`
```
SOLR_HEAP="2g"
```

### Enabling segment-parallel search

"Segment parallel search" (Lucene's per-segment concurrent search, i.e. `indexSearcherExecutorThreads`
in `solr.xml`) is a **node-level** setting read once at Solr startup -- it can't be set remotely
through this project's client APIs. Start Solr itself with:

```
bin/solr start -c -a "-Dsolr.searchThreads=4"
```

(or set `SOLR_OPTS="-Dsolr.searchThreads=4"` in `solr.in.sh`) *before* running `setupCollections`.


## Usage

```
./gradlew setupCollections -Pargs="http://localhost:8983/solr"
./gradlew index            -Pargs="http://localhost:8983/solr"
./gradlew search           -Pargs="http://localhost:8983/solr join   500 1"
./gradlew search           -Pargs="http://localhost:8983/solr aijoin 500 1"
./gradlew search           -Pargs="compare results-join-c1.csv results-aijoin-c1.csv"
```

`setupCollections` uploads the `products`/`skus` configsets and creates both collections (1 shard,
1 replica each) if they don't already exist. `index` bulk-loads the 1M/10M docs.

`search <solrUrl> <join|aijoin> <queryCount> [concurrency] [warmupCount]` runs a fixed number of
queries at a fixed concurrency (default 1, i.e. strictly one query in flight at a time), prints
QTime and client-latency percentiles, and writes one CSV row per query to
`results-<parser>-c<concurrency>.csv`.

Each query filters skus by 2-4 random Color values *or* 2-4 random Size values (never both), joined
back to products, further filtered by 1-3 random brands directly on the products side (`fq`, not
through the join) -- exercising a join result intersected with a local filter, not just the join
alone. `minExactCount` is forced to `Integer.MAX_VALUE` so `numFound` is always exact, and `rows=0`
since only the count matters here.

### Proving the two parsers agree

The query list is generated up front, single-threaded, from `Constants.RANDOM_SEED`, so query *k*
is the same query in every run -- whatever the parser, the concurrency, or the thread interleaving.
That makes the two runs diffable query by query:

```
./gradlew search -Pargs="compare results-join-c1.csv results-aijoin-c1.csv"
```

which reports every `numFound` disagreement (and exits non-zero if there is one). This is the check
that establishes result-set equivalence. Comparing aggregate `numFound` min/max/avg between two runs
does **not**, since runs of different lengths execute different query sets -- see the superseded
results below for what that looks like in practice.

### Why a query count and a concurrency, not a duration and a rate

The harness used to fire at a fixed rate for a fixed duration, via
`Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(task, 0, 1, SECONDS)`. Two
problems, both of which silently corrupted the comparison:

- **A single-threaded scheduler cannot hold a rate it cannot service.** `scheduleAtFixedRate`
  guarantees executions of the same task never overlap, so once a query takes longer than the
  period, the "fixed rate" quietly becomes back-to-back execution. At 1957 ms/query that is
  1/1.957 = 0.51 q/s, which is why `{!join}` completed 250 queries in 500 s while `{!aijoin}`, at
  334 ms, stayed under the 1 s period and completed 501. The two arms ran at different offered
  loads and neither run said so.
- **Different query counts mean different query sets**, so the per-run `numFound` aggregates were
  never comparable in the first place.

Simply adding a thread pool to make it a genuinely open-loop system would not fix this, and could
make it worse: if the offered rate exceeds what the server can absorb, the queue grows without
bound and the run measures queueing rather than the join. Whether 1 q/s is sustainable here depends
on how much CPU a single query consumes, which differs between the two parsers by roughly the
factor being measured.

A closed loop with `concurrency` queries in flight cannot diverge -- it degrades into a throughput
measurement instead -- so every run stays interpretable:

- **`concurrency=1`** is the headline latency number: uncontended service time, zero queueing, which
  is what "far from saturation" actually means.
- **Sweeping `1, 2, 4, 8`** gives the latency-vs-load curve and shows where each parser saturates.
  The summary prints the *achieved* queries/s, so an arm that cannot keep up says so explicitly.

`warmupCount` runs that many queries first and discards the results. They are drawn from a
different seed, so the measured set stays identical whatever warmup you use -- handy for separating
`{!aijoin}`'s lazy join-index build (the large `max` below) from steady-state latency.

### Measuring pruning efficiency

`ToLeafJoinContext` in the patched Solr emits `AIJOIN evt=... key=value` lines at INFO -- no
logging-config change needed. `tools/aijoin_log_summary.py` parses them into a summary table:

```
tools/aijoin_log_summary.py /path/to/solr.log
tools/aijoin_log_summary.py --csv contexts.csv /path/to/solr.log   # also one row per to-segment
```

It reports the join-index build cost (and how much of it landed on the query path rather than at
setup), the a-priori drop rate, how much of a parent segment the approximation actually covers, and
how often the lazy confirmation converged versus held -- then states what each block implies for the
corresponding section of the preprint.

Every line carries `ctx=<id>`, so drains attach to their context exactly and the summary is valid at
any concurrency. Logs from before that field was added fall back to grouping by to-segment, which is
only exact while one query is in flight; the script says when it had to do that, and counts any line
it could not attribute.

Note that `evt=build` carries `cause=`: `eager-create-weight` for the bulk build
`AIJoinIndex#ensureJoinSegments` does at `createWeight` time, and `lazy-to-segment` for the
per-to-segment fallback in `ToLeafJoinContext`. In a steady run the eager path does all the work,
so a log with no `lazy-to-segment` lines is the expected shape -- not a sign of missing
instrumentation.

### Bash notes

ssh -l ... -i ~/.ssh/ssh-key -L 8983:localhost:8983 ...

./gradlew setupCollections -Pargs="http://localhost:8983/solr"
./gradlew index -Pargs="http://localhost:8983/solr"

### Results

Run on a cloud VM with 4 vCPUs, 8G RAM, 2G heap, SSD storage, and `-Dsolr.searchThreads=4`.

<!-- TODO: replace with a fixed-count, concurrency-1 run plus the `compare` output. -->

#### Superseded: the duration-based harness

Kept for the record. These two runs are **not** a valid comparison, for the reasons in
"Why a query count and a concurrency" above: they executed different numbers of queries (250 vs
501), therefore different query sets, at different achieved rates (0.51 vs 1.0 q/s). The `numFound`
rows in particular do not show what they appear to -- the two columns are aggregates over
different samples, so their disagreement is expected and their agreement would prove nothing. The
`{!aijoin}` `max` of 9471 ms is the lazy join-index build on first access, which `warmupCount` now
isolates.

```
 ./gradlew search -Pargs="http://localhost:8983/solr join 500"

> Task :search
Running {!join score=none} against 'http://localhost:8983/solr' for 500s, one query/second...

==== {!join} results ====
queries run : 250 (errors: 0)
QTime (ms)  : min=1338 max=2890 avg=1957.2
numFound    : min=20822 max=87121 avg=50483.5

BUILD SUCCESSFUL in 8m 21s
3 actionable tasks: 1 executed, 2 up-to-date
$ ./gradlew search -Pargs="http://localhost:8983/solr aijoin 500"

> Task :search
Running {!aijoin} against 'http://localhost:8983/solr' for 500s, one query/second...

==== {!aijoin} results ====
queries run : 501 (errors: 0)
QTime (ms)  : min=172 max=9471 avg=334.5
numFound    : min=20792 max=87399 avg=50790.6
```

