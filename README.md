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
./gradlew search           -Pargs="http://localhost:8983/solr join 30"
./gradlew search           -Pargs="http://localhost:8983/solr aijoin 30"
```

`setupCollections` uploads the `products`/`skus` configsets and creates both collections (1 shard,
1 replica each) if they don't already exist. `index` bulk-loads the 1M/10M docs. `search` runs one
query per second (fixed rate, independent of individual query latency) for the given number of
seconds, then prints QTime and numFound min/max/avg.

Each query filters skus by 2-4 random Color values *or* 2-4 random Size values (never both), joined
back to products, further filtered by 1-3 random brands directly on the products side (`fq`, not
through the join) -- exercising a join result intersected with a local filter, not just the join
alone. `minExactCount` is forced to `Integer.MAX_VALUE` so `numFound` is always exact, and `rows=0`
since only the count matters here.

### Bash notes

ssh -l ... -i ~/.ssh/ssh-key -L 8983:localhost:8983 ...

./gradlew setupCollections -Pargs="http://localhost:8983/solr"
./gradlew index -Pargs="http://localhost:8983/solr"

### Results

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

