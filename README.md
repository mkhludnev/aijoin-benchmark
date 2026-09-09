# aijoin-benchmark

Compares `{!aijoin}` against three stock Solr join implementations on the same
`products`/`skus` join, against an already-running SolrCloud node.

`{!aijoin}` is `org.apache.solr.search.join.AIJoinQParserPlugin`, currently only available on a
custom Solr build (it's not in any released Solr version) -- point this benchmark at a `bin/solr`
started from the patched build.

The four *parser arms*, named by the token you pass on the command line (see
`Searcher#localParams` for the exact local params):

| arm | query | collection queried |
|---|---|---|
| `join` | `{!join score=none fromIndex=skus from=PRODUCT_ID_FK to=ID}` | `products` |
| `aijoin` | `{!aijoin fromIndex=skus from=PRODUCT_ID_FK to=ID}` | `products` |
| `joinnum` | `{!join score=none fromIndex=skus from=PRODUCT_ID_FK_NUM to=ID_NUM}` -- the same stock parser on the numeric key pair instead of the string one | `products` |
| `joinglob` | `{!globalOrdinalsJoin score=none joinField=PRODUCT_ID_FK which='<brand filter>'}` | `products_skus` |

`{!globalOrdinalsJoin}` is a same-core join, so it cannot read a second collection: `joinglob` runs
against `products_skus`, which holds both document kinds co-located, and takes the products-side
brand filter through its `which` parameter rather than as a separate `fq`.

## Data model

- `products` (1,000,000 docs): `ID` (unique key, keyword+docValues), `title` (searchable text),
  `brand` (searchable text).
- `skus` (10,000,000 docs): `ID` (unique key), `PRODUCT_ID_FK` (single-valued FK to a product's
  `ID`), `color_s`/`color_t` and `size_s`/`size_t` (keyword+docValues / searchable text pairs,
  `*_t` copied from `*_s`), `inventory_stock` (searchable int).
- `products_skus` (11,000,000 docs): the same 1M products and 10M skus written a second time into
  one collection, so `{!globalOrdinalsJoin}` -- which joins within a core -- has something to join.
  `Indexer` writes every document to both its own collection and this one, so the two views never
  disagree; ids don't collide (`P%07d` vs `S%08d`), and the `skus` configset doubles as the
  combined schema since it already declares `title`/`brand`/`ID_NUM`. Note that a product here
  carries `PRODUCT_ID_FK` set to *its own* `ID`: a same-field join needs the to-side to carry the
  join value too, or no product would ever be reachable.

Products and skus each carry a numeric mirror of their join key (`ID_NUM` on products,
`PRODUCT_ID_FK_NUM` on skus) alongside the string one, which is what the `joinnum` arm joins on --
same parser, same data, different key type.

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
./gradlew searchThenIndex  -Pargs="http://localhost:8983/solr join,aijoin,joinnum,joinglob 100 4 55"
```

`setupCollections` uploads the `products`/`skus` configsets and creates `products`, `skus` and
`products_skus` (1 shard, 1 replica each). It is re-runnable and destructively so: it deletes every
collection and configset it owns first, then recreates them. `index` bulk-loads the 1M/10M docs
into their own collections and into `products_skus`.

`search <solrUrl> <join|aijoin|joinnum|joinglob> <queryCount> [concurrency] [warmupCount]` runs a fixed number of
queries at a fixed concurrency (default 1, i.e. strictly one query in flight at a time), prints
QTime and client-latency percentiles, and writes one CSV row per query to
`results-<parser>-c<concurrency>.csv`.

Each query filters skus by 2-4 random Color values *or* 2-4 random Size values (never both), joined
back to products, further filtered by 1-3 random brands directly on the products side (`fq`, not
through the join) -- exercising a join result intersected with a local filter, not just the join
alone. `minExactCount` is forced to `Integer.MAX_VALUE` so `numFound` is always exact, and `rows=0`
since only the count matters here.

### Proving the parsers agree

The query list is generated up front, single-threaded, from `Constants.RANDOM_SEED`, so query *k*
is the same query in every run -- whatever the parser, the concurrency, or the thread interleaving.
That makes any two runs diffable query by query:

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

That separation is only valid for a *static* index, though. `{!aijoin}` re-pays a large part of
that build after every commit, not only at startup -- which `search` cannot see and
`searchThenIndex` is built to measure. See "Search-then-index rounds" below.

### Search-then-index rounds

`search` opens a searcher, warms it if asked, and never commits again -- so it measures a parser
against a *static* index. `searchThenIndex` measures the opposite: what a parser costs when the
index keeps moving under it, which is the case `{!aijoin}` is most exposed to, since it builds its
join index lazily per searcher and a commit throws that work away.

```
./gradlew searchThenIndex -Pargs="<solrUrl> <join,aijoin,joinnum,joinglob> <queryCount> [concurrency] [repeat]"
```

Each **round** runs `queryCount` queries per parser, arms back to back, at `concurrency`, with **no
warmup**; then updates one product and ten skus and commits, so the next round starts against a
slightly-mutated index on a fresh searcher. Rows are appended to a cumulative
`searchindex-results-<parser>-c<concurrency>.csv`, tagged with a `round` column -- the same CSV
header `search` writes (a plain run is simply round 1), so `compare` diffs either kind of file.

Three details of `SearchThanIndex` are what make rounds comparable at all:

- **The query set is identical across arms within a round.** The seed is `RANDOM_SEED + round`, so
  every parser in round *r* runs the same queries; the set differs *between* rounds.
- **Every collection is churned, not only the ones this invocation reads.** `joinglob` reads
  `products_skus` and the others read `products`/`skus`, so updating only what a run touches makes
  the datasets diverge the moment the arms are benchmarked in separate invocations -- and then
  `numFound` differs between parsers for no reason but the data.
- **The churn is seeded per round rather than random.** Updates are by unique key, so replaying an
  identical sequence is idempotent: separate invocations walk the index through the same states.

Because there is no warmup and a commit precedes every round, the first `concurrency` queries of a
round always land on a fresh searcher. That is the point -- the recurring post-commit cost is a
thing being measured, not noise -- but it does mean a plain per-round average is not a latency
number, and mixing it with the steady state hides both. See the next section.

### Analysing the rounds

`tools/searchindex_drift.py` reads the cumulative CSVs and answers the two questions they exist for:
what each arm costs once a round is running, and whether that cost *drifts* as commits accumulate.

```
tools/searchindex_drift.py                                        # all searchindex-results-*-c4.csv
tools/searchindex_drift.py --from-round 11 --per-round
tools/searchindex_drift.py --metric wall_ms searchindex-results-*-c8.csv
```

It exists because a raw per-round mean conflates three separate effects, which it splits instead:

- **The post-commit cold burst.** The first `2 x concurrency` queries of each round are reported on
  their own, as an excess over that round's steady median -- the rebuild, sized against what the
  same round costs warm.
- **The end-of-run drain.** The harness is a closed loop, so the last few queries of a round run
  with fewer than `concurrency` in flight and are faster for that reason alone. They are dropped.
- **Query difficulty.** Seeded per round, so a hard round lifts every arm at once and a naive trend
  line reads that as drift. The trend is therefore reported twice: raw, and normalised by the
  *other* arms' medians for the same round (leave-one-out geometric mean). What survives the
  normalisation is drift attributable to the parser itself.

Trends are fitted on the steady window only, as an OLS slope in %/round plus a Spearman rank
correlation; the z-scores are a normal approximation that treats rounds as independent, so read
`|z| < 3` as weak. It also checks `numFound` query-for-query across every file it loads -- the
`compare` check, extended to all arms and all rounds.

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

#### Search-then-index: 55 rounds x 100 queries per arm, concurrency 4

```
./gradlew searchThenIndex -Pargs="http://localhost:8983/solr join,aijoin,joinnum,joinglob 100 4 55"
tools/searchindex_drift.py --from-round 5
```

`numFound` disagreements across the four arms, over all 5,500 queries: **0**. Rounds 1-4 are
warmup (JIT, page cache, `{!aijoin}` stepping down 360 -> ~210 ms), so the tables below start at
round 5.

Steady state -- cold burst and drain tail excluded, QTime in ms:

| arm | p50 | p90 | p95 | p99 | max | vs fastest |
|---|---|---|---|---|---|---|
| `joinglob` | 98 | 129 | 133 | 141 | 171 | 1.0x |
| `aijoin` | 145 | 230 | 272 | 580 | 1050 | 1.5x |
| `joinnum` | 614 | 791 | 866 | 1014 | 1391 | 6.3x |
| `join` | 2938 | 3792 | 3867 | 4025 | 5406 | 30x |

Post-commit cold burst -- median of the round's first 8 queries, minus that round's steady median:

| arm | round 1 | median per round | as % of that arm's steady p50 | trend, ms/round |
|---|---|---|---|---|
| `aijoin` | 8972 | 660 | 456% | -2.8 (z=-0.7) |
| `joinglob` | 342 | 70 | 72% | -0.1 (z=-1.4) |
| `joinnum` | 712 | 110 | 18% | +0.1 (z=+0.1) |
| `join` | 1069 | 46 | 2% | +1.6 (z=+0.4) |

**The `{!aijoin}` cold cost is not a startup cost, it is a per-commit cost.** Round 1 is the
familiar ~9 s lazy build, and `warmupCount` does isolate that -- but every subsequent commit
re-imposes a ~660 ms burst, roughly 4.5x the arm's own steady latency. At this shape (100 queries,
concurrency 4) 8% of `{!aijoin}`'s queries consume **31%** of the round's wall time; for `{!join}`
the same 8 queries are 8% of it. So under continuous churn `{!aijoin}` costs ~5.8 s/round against
`{!join}`'s ~74 s -- still 13x better, but not the 19x the steady-state medians alone suggest.
The burst does *not* grow with accumulated commits, which is the part that matters for a long-lived
index.

Drift, rounds 11-55 (once all four arms are past warmup), on the steady-state median per round:

| arm | raw slope | difficulty-normalised | over the span | Spearman |
|---|---|---|---|---|
| `aijoin` | **+0.54%/rd (z=+3.8)** | **+0.55%/rd (z=+4.6)** | **+27.5%** | rho=+0.65 |
| `join` | -0.07%/rd (z=-2.8) | -0.22%/rd (z=-4.0) | -9.4% | rho=-0.53 |
| `joinglob` | -0.04%/rd (z=-1.0) | -0.18%/rd (z=-2.6) | -7.4% | rho=-0.34 |
| `joinnum` | -0.03%/rd (z=-0.4) | -0.15%/rd (z=-3.3) | -6.5% | rho=-0.46 |

**`{!aijoin}` is the only arm that drifts, and it drifts upward**: its steady median creeps from
~130 ms around round 11 to ~155 ms by round 55, while the other three are flat or drifting slightly
down over the same rounds. It holds up as a real effect -- the raw slope is positive too, so it is
not an artefact of the normalisation; it reproduces on `wall_ms`; and it is insensitive to where the
window starts (+0.52%/rd from round 15, +0.48%/rd from round 20). Absolute stakes are small (25 ms
on an arm still 19x faster than `{!join}`), and 55 rounds is too short a lever to extrapolate a
rate from, so the defensible claim is *significantly non-stationary over 55 commits*, not a number
to project forward.

The mechanism is untested. That the *rebuild* cost stays flat while the *steady* cost grows points
at the per-segment read path rather than the build -- each round commits, so segments accumulate.
A `solr.log` from a long run, through `tools/aijoin_log_summary.py`, would confirm or kill that.


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

