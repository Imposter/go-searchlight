# Changelog

Searchlight's releases. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow [Semantic Versioning](https://semver.org/). The release workflow
publishes each version's section here as its GitHub release notes (RELEASING.md).

## [Unreleased]

### Added

- **Nested fields**, Elasticsearch's `nested` type ([docs/api.md](docs/api.md#nested-fields)):
  one document per product with its variants, each matched apart. A mapping types a
  field `{"type": "nested", "fields": {"size": "keyword", "price": "number", ...}}`
  (sub-fields of any type but a point, added later by a mapping patch), and a document
  gives an array of objects, at most 10,000 members. Sub-fields are named by their full
  names (`_v.size`) everywhere.
  - `{"nested": {"path": "_v", "query": ...}}` holds when one object satisfies the whole
    query (a `not` inside applies per object), in queries, `post_filter`, filter
    aggregations and saved queries; `inner_hits` returns each hit's matching members,
    by offset, with the sub-fields asked for (at most 10,000 inner hit objects per
    response: `size` times the `inner_hits` sizes).
  - `terms`, `stats`, `min`, `max` and `cardinality` on a sub-field, with an optional
    `where` choosing the objects: `terms` counts each document once per value. A sort
    on a sub-field orders hits by the least or greatest value among their objects
    (`mode`, `where`), a hit with none last.
  - Each segment indexes a nested field's objects as an object segment of their own,
    in a NESTED section (segment format 4.2; a segment without one is written as
    before, byte for byte); documents, deletes, totals and caches stay the documents'.
  - Search wire level 6 and percolate wire level 3 keep nested reads off peers of an
    earlier release (503 with no other copy). Nodes heartbeat mapping types 2: a
    nested field is refused (409, naming the nodes) while a node of an earlier release
    is live, and once one exists no node can be downgraded below this release
    ([docs/operations.md](docs/operations.md#upgrades-and-rolling-restarts)).

### Changed

- A mapping's or a `PATCH .../mapping`'s invalid field type is now a 400 located at the
  field (`mapping.fields.<name>`, `fields.<name>`), not at `mapping` or `fields`.
- The request cache's key format is 4: entries of earlier releases are not read.

## [1.3.0] - 2026-10-17

Point fields and box queries, and reads that retry a serving copy their view had not listed yet.

### Added

- **Point fields and box queries**, Elasticsearch's `point` field and Lucene's
  `XYPoint` ([docs/api.md](docs/api.md#point-fields)). A mapping types a field
  `{"type": "point", "x": "number"|"date", "y": "number"|"date"}`, and a document gives
  a list of `[x, y]` pairs or one pair, a date axis taking what a date field takes.
  `{"field": "seen", "op": "box", "value": {"x": {"gte": "now-7d/d"}, "y": {"gte": 4}}}`
  matches a document with one point inside on both axes: any of `gt`, `gte`, `lt` and
  `lte` per axis, either axis optional, date math on a date axis (resolved once per
  request by the coordinator, and at each percolation for a saved query). It composes
  in `all`, `any`, `not`, `post_filter`, `filter` and `filters` aggregations and saved
  queries; `exists` works on point fields; aggregations and sorts on them are refused.
  - Each segment indexes a point field in a k-d tree, in a new XY section (segment
    format 4.1, which a 4.0 reader ignores; a segment without a point field is written
    as 4.0, byte for byte as before). A box over 10M points takes 0.2 to 0.6 ms at
    100k matches or fewer, 1.5 to 21 times faster than a scan of the points sorted
    by x, except for a bound on x alone ([docs/architecture.md](docs/architecture.md)).
  - Search wire level 5 and percolate wire level 2: a box search, and a percolation of
    an index with a point field, are not answered by a peer of an earlier release,
    which would refuse the op or misread the mapping; with no other copy they are a
    503 until the upgrade is done.
  - The request cache keys a box by its resolved bounds; a box reading `now`
    unrounded is never cached, as any such date math is.
  - Schema migration 4 adds `sl_nodes.mapping_types`, the mapping field types each
    node reads. A point field (at index creation or by a mapping patch) is refused
    with a 409 naming them while a node of an earlier release is live, which could not
    decode the mapping. Once a point field exists, no node can be downgraded below
    this release ([docs/operations.md](docs/operations.md#upgrades-and-rolling-restarts)).

### Fixed

- **A remote read no longer fails while another copy of its shard serves.** A
  coordinator read the copy table again only when its view listed no serving copy of a
  shard; when the view (which can trail the database by a refresh) listed one, and that
  one failed, a search, percolation, get or count answered 503 "no copy of shard N of
  index X answered" though another copy was serving. Once every copy it knows of has
  failed (or refused it for an older wire level), a read now reads the copy table again,
  once per shard (shards of the request, and concurrent reads, share one read), and
  tries the serving copies it has not tried, a peer back at a new address included. The
  read waits on the re-read for at most a quarter of its remaining time and 1 s, so a
  slow database leaves time for the coordinator's own stale copy to answer instead; the
  registry read itself runs on for the reads after it, and waiting for the routing
  reads' turn now gives up at the request's deadline too. Date math
  stays resolved once and `wait_for_seq` holds. `searchlight_cluster_read_rereads_total`
  counts the re-reads, by `op` and `result` (`found`, `none`, `timeout`).
- **A rejected negated pattern no longer changes how later patterns fold.** Compiling a
  `terms` include or exclude pattern with a negated class (`[^\t]`) appended to the
  process-wide table of case-folded code points, so a later `[A-C]` stopped matching
  `a` until the process restarted, and concurrent searches raced on the table.
- **A Windows node no longer needs `GODEBUG=asyncpreemptoff=1` set.** The Go runtime can
  corrupt a thread's registers when it preempts that thread while Windows is handling an
  exception on it ([golang/go#79249](https://github.com/golang/go/issues/79249), fixed
  upstream for Go 1.28, in no release yet), and the node then crashes with an access
  violation at a small address.
  The Windows release binary now links `asyncpreemptoff=1` in as the runtime's default
  ([tools/ldflags.sh](tools/ldflags.sh), also used by `make build`), and a Windows binary
  built without it, such as by `go install`, starts itself again with the setting, as a
  second process. `make` now needs `sh` on PATH (on Windows, Git Bash's). An
  `asyncpreemptoff` in `GODEBUG` still wins, so `GODEBUG=asyncpreemptoff=0` turns
  preemption back on ([docs/operations.md](docs/operations.md#the-binary)).

## [1.2.0] - 2026-10-17

Fast aggregations: a shard request cache (in memory, optionally Redis), breadth-first `terms`, `min`/`max`, a cardinality `threshold`, global ordinals that survive refreshes, and a `timeout` that bounds the whole shard search.

### Added

- **The shard request cache**, Elasticsearch's, for counts and aggregation-only
  searches ([docs/operations.md](docs/operations.md#request-cache)): a search with
  `size` 0 is answered shard by shard from an identical search's result over the same
  shard contents, and stored otherwise. Keys hold the request (date math resolved,
  never its `timeout`) and what the shard holds, so a refresh that writes, a delete, a
  merge or a remap misses and no hit is ever stale; concurrent identical searches
  compute once, each waiting within its own `timeout`. Never cached: a search with
  hits, a `from` or a `search_after`; date math reading `now` unrounded (`now-7d`;
  `now-7d/d` is cached); a result that timed out.
  - `request_cache: false` on `_search` keeps a search out of it
    ([docs/api.md](docs/api.md#search-and-count)). It travels to peers without a new
    wire level: an older peer has no cache to keep it out of.
  - In memory, up to `request_cache_size` (128 MiB by default; `0` for none).
  - Optionally in front of a Redis (`request_cache_redis_url`, with
    `request_cache_redis_password`, `_prefix`, `_ttl`, `_timeout` and
    `_max_entry_bytes`; the URL and the password also from `_FILE`), shared by every
    node and shard copy and kept across restarts. Its entries are keyed by the shard's
    newest change the copy holds, so writes to other shards and indexes leave them be,
    and by this binary's version. A Redis that is down or slow fails no search: each
    call is bounded (50 ms by default, and a tenth of the search's time left), a breaker
    skips it after 5 failures in a row, and failures are logged and counted. What is
    read back is bounded and checked; Redis is trusted as the database is.
  - Metrics: `searchlight_search_request_cache_lookups_total` (by store and result),
    `_errors_total`, `_evictions_total`, `_stored_bytes_total`,
    `_writes_dropped_total`, `_store_open_ratio`, `_size_bytes` and `_entries`.
- The benchmark sends Searchlight's searches with `request_cache: false`, as it does
  Elasticsearch's.
- **`min` and `max` aggregations**, Elasticsearch's: `{"value": x}`, or `{"value":
  null}` when no document has one (Unix milliseconds on a `date` field, as `stats`
  gives them), at the top level, under `filter`, `filters` and
  `global`, and as a bucket aggregation's sub-aggregation. Each keeps one value per
  bucket; over a dense hit set it reads the point index from the end sought until a
  block holds a hit ([docs/api.md](docs/api.md#search-and-count)).
- **`threshold` on `cardinality`**: `{"cardinality": {"field": "brand", "threshold":
  2}}` answers exactly with the number of distinct values when there are fewer than
  the threshold, else the threshold (1 to 65,536; not Elasticsearch's
  `precision_threshold`, and not with `precision`). A segment stops once it has seen
  that many values, and so does the rest of its shard; a dense hit set is answered
  from the dictionary's document frequencies. One value in several segments or shards
  counts once. 100 cardinality aggregations over a million documents went from about
  850 ms to 14 ms with a threshold of 2.
- **`collect_mode` on `terms`**, `depth_first` or `breadth_first`, as Elasticsearch's.
  Breadth first counts the values, makes each shard's `shard_size` cut, then reads
  the hits again for the sub-aggregations of the values kept alone. Unset, a segment
  whose field holds more distinct values than `shard_size` collects breadth first;
  results are the same either way.

### Changed

- **Global ordinals survive refreshes.** A refresh's or merge's new segment list
  derives a field's global ordinals from the old list's (the kept segments' maps
  renumbered, the new segments' terms looked up and inserted), exactly as a full
  build would make them, when that is cheaper than merging every dictionary again;
  after each refresh or merge the fields aggregated in the last 10 minutes are
  rebuilt in the background. Every build, a search's or a warm's, takes a slot of
  one pool of a quarter of the CPUs, searches' first; a search never waits on a
  queued build, and on a running one at most 250 ms; a shard's field has one build
  in flight, which a newer generation's supersedes; a closing shard cancels its
  builds, waits for them and drops its entries. A build runs once for every search asking, detached from them: one a
  search's deadline stops waiting for finishes and is cached for the next search
  instead of being thrown away and paid again ([docs/architecture.md](docs/architecture.md)). New
  metrics: `searchlight_search_ordinals_builds_total`,
  `searchlight_search_ordinals_build_duration_seconds` and
  `searchlight_search_ordinals_lookups_total`.
- **`timeout` bounds each shard's whole search.** The deadline (the earlier of
  `timeout` and `request_timeout`) now covers each shard's merge of its segments'
  aggregations and the dense counts from the index, not only the segment scan:
  segments stop a tenth of the budget (1 ms to 1 s) early, an aggregation whose
  global ordinals are still building merges by term rather than wait past the
  deadline, and only one the deadline cuts off even so is answered empty, with
  `timed_out: true` ([docs/api.md](docs/api.md#search-and-count)). A search
  cancelled during the merge returns its cancellation.
- `stats`' `min` and `max` read `-0` as `0`, as `min`, `max` and every bucket key do.
- A deadline that passes while a shard reads its hits again for breadth-first
  sub-aggregations leaves that `terms` aggregation empty on the shard, with
  `timed_out`, rather than counts its sub-aggregations do not match; a client's
  cancel then is the search's error.
- **Faster `terms` over many values.** A shard keeps its `shard_size` best terms by
  selection rather than a sort of every term (the reduce its `size` likewise); the
  parts of a large segment collected apart share count arrays, one per part counted
  at once, rather than one per part; breadth-first sub-aggregations collect stats,
  `min` and `max` into arrays by bucket, each hit's value read once; the reduce copies
  a shard's bucket only when another shard returns its term too; and buckets are
  written as JSON without building a map per bucket. Over a million documents and
  150,000 distinct list values: the top 100 terms of 95% of the index from 23 to 10
  ms, of 45% from 35 to 15 ms, and with a `stats` sub-aggregation (breadth first)
  from 190 to 22 ms.

### Fixed

- **A histogram's empty buckets past 2^53 no longer hang the search.** Filling in a
  `histogram` or fixed-interval `date_histogram` with `min_doc_count` 0 (the default)
  counted bucket indices in a float64, which stops advancing past 2^53: buckets at
  1e16 with an interval of 1 kept a reduce goroutine appending forever. Indices past
  2^53 or not finite (a huge value over a tiny interval), and neighbouring keys that
  round together (an offset huge beside the interval), now leave the histogram
  sparse and `"truncated": true`, as one whose empty buckets pass the response's
  65,536; a recomputed grid that starts or ends off the real buckets leaves it as it
  is. Other histograms are filled as before.

### Upgrading

- A search with a `min` or `max` aggregation, or a cardinality `threshold` (search
  wire level 4), is refused with a 503 rather than read from a node of an earlier
  release, which would refuse the aggregation as unknown or estimate past the
  threshold. `collect_mode` needs nothing: an earlier node collects depth first, with
  the same results.

## [1.1.0] - 2026-10-17

Faceting: filter aggregations and `post_filter`, `terms` include and exclude, date math, and `from` paging.

### Added

- **Filter aggregations and `post_filter`**, so a facet panel is one search
  ([docs/api.md](docs/api.md#search-and-count)):
  - `filter` (a query), `filters` (keyed queries, with `other_bucket_key`) and
    `global` (every document of the index, top level only), each holding any other
    aggregation with its metric sub-aggregations. Their filters are planned and
    cached as the query is, checked against the mapping at their own `loc`, and
    reduced across shards and nodes as top-level aggregations are: `terms` error
    bounds and the 65,536-bucket limit apply under them too;
  - `post_filter` on `_search`: the hits, `total`, sort, `search_after` and `next`
    obey it, the aggregations (all but `global`) do not. `_count` refuses it.
- **Date math** on `date` fields, as Elasticsearch writes it: `lt`, `lte`, `gt`, `gte`
  and `between` take `now` or a date, then `+N`/`-N` and `/` rounding in `y M w d h H m
  s` (`{"field": "seen_at", "op": "gte", "value": "now-7d/d"}`), in UTC. Each bound
  rounds as Elasticsearch's range query rounds it. `now` is one instant per request,
  read by the coordinating node and used by every shard and peer. A saved query keeps
  its expression and resolves it at every percolation.
- **`include` and `exclude` on `terms`**, Elasticsearch's, for a facet's type-ahead
  (`"include": "ni.*"`) and its picked values counted in the same search, outside the
  top buckets too ([docs/api.md](docs/api.md#search-and-count)): an array of at most
  1,000 exact values, compared normalized as the field's values are, or a pattern of
  at most 1,000 characters over the whole normalized key, in a subset of Lucene's
  regular expressions (literals, `.`, classes, `*` `+` `?` `{n,m}`, `|`, groups) run
  as RE2 and folded as values are (`"Nike.*"` is `nike.*`). Exclude wins; both apply before `size` and `shard_size`, so the top buckets,
  `sum_other_doc_count` and the error bounds are the chosen values'. Each segment
  decides them once per distinct value, from its dictionary or, with few hits, as
  values are collected. Patterns on number, date and bool fields, patterns that
  cannot fold faithfully (`[ß]`, `[^A-Z]`), Lucene's other operators and partitions
  are refused with a 400 at their `loc`; a null `include` or `exclude` is none.
- **`from` on `_search`**, Elasticsearch's offset paging
  ([docs/api.md](docs/api.md#search-and-count)): `"from": 40, "size": 20` skips the
  first 40 hits in sort order. `from + size` is at most the new `max_result_window`
  setting (10,000 by default, as Elasticsearch's `index.max_result_window`; at most
  1,000,000), past which the search is a 400 at `loc` `from` pointing to
  `search_after`; a `from` past 0 with `search_after` is a 400 at `from`. Each shard
  returns its best `from + size` without bodies, the coordinator drops the first
  `from`, and only the page's bodies are read. `next` is the page's cursor as
  before, so a client can switch to `search_after` at any page; `total` and the
  aggregations are the whole search's.

### Changed

- The 100-aggregation bound counts the aggregations under a `filters` aggregation once
  per bucket.
- A sub-aggregation named after a member of the bucket it is written in (`key`,
  `doc_count`, ...) is refused with a 400, rather than overwritten in the response.
- Problems are located under `aggregations` when the request spells it so.

### Upgrading

- During a rolling upgrade from 1.0.0, a search with a `post_filter` or a `filter`,
  `filters` or `global` aggregation that would read a shard from a 1.0.0 node is
  refused with a 503 (a 1.0.0 node does not apply them) until the shard has a copy
  on an upgraded node; other searches are unaffected
  ([docs/clustering.md](docs/clustering.md#rolling-restarts-and-upgrades)).
- **Date math and older nodes.** Searches with date math need nothing: peers get
  `now` resolved to numbers.
  - A percolation (`_percolate`, `_bulk?percolate=true`) of **every index with a
    `date` field** needs percolate wire level 1, whether or not any saved query uses
    date math. Through an upgraded node, a shard with a copy on an upgraded node
    percolates there. A shard whose other copies are all on 1.0.0 nodes is refused
    with a 503 (`_bulk` commits, with `percolated: false`), unless the coordinating
    node holds a copy of its own that cannot serve now (stale or recovering): the
    request's first read of the shard then falls back to that copy, which answers
    flagged `stale` or says why it cannot. Indexes without a `date`
    field percolate as before.
  - Saving a date-math query is not refused mid-upgrade. A 1.0.0 node percolating its
    own copies matches none of its date-math conditions, and a 1.0.0 node coordinating
    a percolation sends no `now`, so the upgraded peers it asks resolve date math at
    their own clocks. Save such queries once every node is upgraded.
  - Rolling back: a query segment holding a condition that reads `now` has a program
    tag 1.0.0 does not know. A 1.0.0 node refuses it as corrupt and rebuilds the copy,
    where the query then matches nothing; delete such queries, or rewrite them with
    numbers, before rolling back. Segments without such a condition are unchanged
    (format `percolate/3`).
- Likewise a search with a `terms` `include` or `exclude` (search wire level 2) is
  refused with a 503 rather than read from a node of an earlier release, which would
  count every value.
- Likewise a search with a `from` past 0 (search wire level 3) is refused with a 503
  rather than read from a node of an earlier release, which would ignore `from` and
  answer with the first page. Searches without `from` are unaffected. The new
  `max_result_window` setting applies on the node that takes the search; set it
  alike on every node.
- `max_result_window` bounds `from + size`, so with `from` 0 it bounds `size` too, as
  Elasticsearch's does: lowering it below 10,000 refuses (400 at `size`) searches
  with a `size` above it that were answered before.

## [1.0.0] - 2026-10-14

The first release: a search engine that behaves like Elasticsearch for filtering,
sorting, aggregations and percolation, and keeps its durable state in SQL. Licensed
under the [MIT License](LICENSE).

### Added

- **The HTTP API** ([docs/api.md](docs/api.md), contract in
  [api/openapi.yaml](api/openapi.yaml)), with bearer-token auth (read and write scopes):
  - indexes with typed mappings (`keyword`, `text`, `keyword_list`, `number`, `bool`,
    `date`; dynamic, non-indexing or strict), additive mapping changes, and shards and
    `refresh_interval` per index;
  - documents (`PUT`, `GET`, `DELETE`) with optimistic concurrency (`if_seq`) and
    `op_type=create`, and `_bulk`, every operation of a request in one commit;
  - search and count: boolean queries with words, phrases, substrings (`contains`, on
    3-grams), prefixes and trigram similarity, case and Unicode (NFKC) folding;
    sorting, `search_after` paging, and terms, range, histogram and stats
    aggregations;
  - read-your-writes through `refresh=wait_for` and `wait_for_seq`;
  - saved queries and percolation (`_percolate`) of given and stored documents;
  - a field catalogue, and `_cluster/health`, `_cluster/nodes` and `_cluster/shards`.
- **Clustering** on Postgres or MySQL ([docs/clustering.md](docs/clustering.md)): every
  node is the same binary and takes any request. Nodes register in the database and
  find each other there; leases decide which node holds which shard copy; a new or
  restarted copy recovers from a peer's segments, a recovery bundle in the database, or
  the database's documents. Reads pick replicas adaptively and retry; a rolling restart
  hands its copies over before the node stops. `replicas_per_shard` sets copies per
  shard.
- **Three SQL dialects**: Postgres, MySQL 8.0.19 or later, and SQLite (one node). The
  database's changelog is the write-ahead log: a write is acknowledged once its
  transaction commits.
- **Operations**: one static, pure-Go binary for Linux, Windows and macOS; settings as
  flags or `SEARCHLIGHT_*` variables, secrets from `*_FILE`; `/healthz`, `/readyz` and
  Prometheus `/metrics` on a separate admin listener; JSON logs, OpenTelemetry traces
  and metrics; `searchlight healthcheck` for container probes and `searchlight version`.
- **Releases**: binaries for linux/amd64, linux/arm64, windows/amd64, darwin/amd64 and
  darwin/arm64, built with Go 1.25.14, with SHA256SUMS; a multi-arch image,
  `ghcr.io/imposter/searchlight` (linux/amd64 and linux/arm64, distroless, non-root, with
  provenance and an SBOM), each carrying the licence and third-party notices; and deploy
  samples: a three-node Postgres cluster behind nginx and a single SQLite node for Docker
  Compose, and Kubernetes manifests.

### Performance

Against Elasticsearch 8.15.3 on the same 4-vCPU CI host, 1,000,000 documents, Searchlight
on SQLite ([docs/benchmarks.md](docs/benchmarks.md)); every graded target passes:

| Target | Searchlight | Elasticsearch 8 |
| --- | --- | --- |
| Bulk indexing, acknowledged = durable (T1) | 13.5k docs/s | 7.3k docs/s |
| Filter, sort and aggregation latency (T2–T4) | p50 0.5–2.9 ms, p99 1.1–15 ms | p50 1.9–140 ms, p99 4.7 ms–2.2 s |
| Percolation, 100k saved queries (T5) | 3,187 docs/s, server p99 783 µs per doc | 12.6 docs/s |
| Resident memory per million docs (T6) | 2.42 GiB | 4.15 GiB |
| Write to visible, p99 (T7) | 999 ms | 998 ms |
| New replica to serving (T8) | 6.0 s | 28.5 s |
| Node restart to serving (T9) | 1.1 s | — |

Disk per million documents is 2.33 GiB against 1.5 GiB, for 3-grams on every keyword and
text field. Replica failure (T10) is checked by the chaos suite rather than measured, and
scale beyond RAM (T11) is not measured yet; nor is write throughput on Postgres and
MySQL ([#69](https://github.com/Imposter/go-searchlight/issues/69)).

### Upgrade notes

For databases created by builds of `main` before this release
([docs/operations.md](docs/operations.md#upgrades-and-rolling-restarts)):

- **Schema migrations 1–3** run at start, on the first node to start: 1, the initial
  schema; 2, compressed bodies (`body_z`, `payload_z`, `sl_nodes.body_codecs`,
  `sl_features`; zstd compression turns on once every node reads it); 3, change nonces
  (`sl_changes.nonce`). Each only adds columns and tables, in milliseconds, and nodes
  already running an older build keep working beside it. A binary refuses a schema newer than it knows, so
  rolling back means restoring the database.
- **`cluster_token` is required on Postgres and MySQL**: a node there refuses to start
  without it unless `insecure_no_auth` is set. A cluster of several nodes has one
  already; set one on a single node before upgrading it.
- **Segment format 4** is read from format 3 as it is; merges rewrite segments as they
  go.

### Known gaps

From [docs/architecture.md](docs/architecture.md#known-gaps):

- **Background scrub** ([#36](https://github.com/Imposter/go-searchlight/issues/36)): a
  segment is checksum-verified when its copy opens, so bit rot after open is served until
  the copy is next opened.
- **A percolator format change rebuilds the whole copy**
  ([#37](https://github.com/Imposter/go-searchlight/issues/37)): document segments one
  major version old are read as they are (format 4 reads format 3), but a change to the
  saved-query segments' format (`percolate/N`) wipes the shard copy, documents included,
  and rebuilds it, rather than rebuilding only its query segments.
- **Scale beyond RAM and many nodes** (T11): designed for (memory-mapped segments, shards
  across nodes) but not benchmarked yet.
- No relevance scoring (BM25), stemming, synonyms or phonetic matching, and no
  SQLite-to-Postgres migration tool ([#95](https://github.com/Imposter/go-searchlight/issues/95)).

[1.3.0]: https://github.com/Imposter/go-searchlight/releases/tag/v1.3.0
[1.2.0]: https://github.com/Imposter/go-searchlight/releases/tag/v1.2.0
[1.1.0]: https://github.com/Imposter/go-searchlight/releases/tag/v1.1.0
[1.0.0]: https://github.com/Imposter/go-searchlight/releases/tag/v1.0.0
