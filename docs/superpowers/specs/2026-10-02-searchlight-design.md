# Searchlight design

**Status:** approved design, revised 2026-10-02 to aim for Elasticsearch parity on performance and operations.
**Repository:** `github.com/Imposter/go-searchlight`, local at `E:/code/go-searchlight` (no remote yet).

## 1. Purpose and bar

Searchlight is a general-purpose search engine written in Go. It does two things:

- **Forward search:** a query in, matching documents out, with sorting, paging, counts and aggregations.
- **Percolation:** a document in, the saved queries it matches out.

Its durable source of truth is **any SQL database**: Postgres, MySQL or SQLite. scrape-bot is its first client, but nothing scrape-bot-specific lives in the engine.

**The bar is Elasticsearch 8.x on the same hardware.** Searchlight must be as fast or faster at what it does, and at least as easy to run as a cluster. This is proven by a reproducible benchmark that runs both engines side by side (§14), not asserted.

### Success criteria (measured against Elasticsearch 8.x, same hardware and data, §14)

| Area | Target |
|---|---|
| Bulk indexing throughput (docs/s per node) | ≥ Elasticsearch, at equal durability (acknowledged = durable) |
| Filter / boolean search latency, p50 and p99 | ≤ Elasticsearch |
| Sorted and paged search, p50 and p99 | ≤ Elasticsearch |
| Aggregation latency (terms, range, histogram, stats) | ≤ Elasticsearch |
| Percolation throughput (docs/s against 10k / 100k saved queries) | ≥ 10× Elasticsearch's percolator, with p99 per document < 1 ms server time at 100k queries (measured like Elasticsearch's took) |
| Index size on disk and resident memory per million documents | Resident memory ≤ Elasticsearch. Disk is reported, not graded: Searchlight indexes 3-grams of every keyword and text field so substring and similarity search are fast on any field, which Elasticsearch offers only on fields given an n-gram subfield (decided 2026-10-05) |
| Write-to-visible latency | ≤ 1 s by default (the refresh interval), and `refresh=wait_for` like Elasticsearch |
| New replica from zero to serving (10M docs) | ≤ Elasticsearch peer recovery |
| Node restart to serving | seconds, not proportional to index size (segments are reopened, not rebuilt) |
| Replica failure | no lost acknowledged write, no client-visible error with ≥ 2 replicas behind a load balancer |
| Scale | larger than RAM per node, through memory-mapped segments, and many nodes through shards |

### Operations bar ("plug and play")

- **Joining:** a replica needs exactly one setting, `store_url`. It registers itself, discovers its peers, recovers its shards and starts serving with no operator steps.
- **Writes anywhere:** any replica accepts reads and writes, so a dumb load balancer in front is enough. Clients never need to know the topology.
- **Rolling changes:** restarts and upgrades are safe one replica at a time, and readiness gates traffic.
- **Disposable nodes:** losing a replica's disk loses nothing, because a replacement recovers from peers or from the database.
- **No coordination service:** no ZooKeeper, no etcd, no consensus library. The SQL database is the only coordination point (leases, sequence numbers, registry).

### Non-goals (v1)

- BM25 relevance scoring and full-text ranking. Matching is boolean, sorting is by field. This is on the v2 roadmap.
- Nested documents and parent/child joins.
- Interfaces beyond HTTP/JSON: NATS, a SQL dialect, pgwire.
- Stateful alerting. Matching is stateless; clients keep their own match state.
- History and time-series conditions (scrape-bot keeps them).
- Cross-index joins and scripting.
- Log storage and a log explorer (Grafana, Loki-compatible API, a pipe/SQL query language). This is part 2, after v1 ships: [roadmap](2026-10-05-searchlight-part2-logs-roadmap.md).

## 2. Architecture

```
clients ──HTTP/JSON──► any replica (coordinator)
                         │ routes by shard, scatter-gathers searches
             ┌───────────┼───────────────┐
           replica A   replica B      replica C     each hosts a set of shard copies
             │  segments on local disk (mmap), in-memory write buffer
             │  tails the changelog of its shards
             └───────────┴───────────────┴──────► SQL database
                     changelog (write-ahead), documents, queries, registry, leases
```

- **Index → shards.** An index has N primary shards, fixed at creation (default 1; scale out with more). Documents and saved queries hash to a shard by id.
- **Shard copy.** A shard copy on a replica is a **Lucene-style segment set**:
  - immutable, memory-mapped segment files on local disk;
  - an in-memory write buffer that is refreshed into a new segment every `refresh_interval` (1 s);
  - background merges of small segments into larger ones.
- **The SQL database is the write-ahead log and the system of record.** Every write commits the change rows and the documents or queries in one transaction, before it is acknowledged, and every shard copy applies the changelog in sequence order. Local segments are a cache of the database that can always be rebuilt or fetched from a peer.
- **Coordinator role.** Any replica can coordinate. It routes single-document operations to the database and the affected shard, and for searches it scatters to one copy of each shard (local copies first, else a peer over internal HTTP) and gathers the results.

### Packages

| Package | Responsibility |
|---|---|
| `cmd/searchlight` | main, wiring, signals, graceful shutdown |
| `internal/config` | flags and environment, validated |
| `internal/analysis` | normalizers (casefold, whitespace fold, NFKC words, list entries, trigrams) |
| `internal/query` | DSL AST, parse and validate (problem locations), exact matcher over an analyzed document |
| `internal/segment` | the on-disk segment format: term dictionary (FST-style sorted blocks), roaring postings, doc values (columnar, compressed), stored fields (compressed blocks), live-docs bitmap; writer, mmap reader, merge |
| `internal/shard` | one shard copy: write buffer, refresh, segment set and generation, merges, filter cache, per-shard search and percolate |
| `internal/search` | query planning (cost-based leaf order, bitmap and residual), per-shard execution, sort, `search_after`, aggregations, cross-shard reduce |
| `internal/percolate` | anchor extraction, per-shard query index (persisted as a segment kind), candidate generation, verification |
| `internal/cluster` | replica registry, shard allocation by database leases, peer recovery, health, internal RPC |
| `internal/replica` | the changelog tailer and apply loop for each hosted shard |
| `internal/store` | the SQL storage interface, dialect implementations, migrations |
| `internal/api` | public HTTP handlers, limits, auth, problem JSON, `wait_for_seq` and `refresh` |
| `internal/telemetry` | slog, OTel traces and metrics, Prometheus, pprof |

## 3. Data model

### Indexes, mappings and settings

- **An index** has a name, a **mapping** (field → type) and **settings**: `shards`, `replicas_per_shard` (target copies; default "every replica holds every shard"), `refresh_interval`, and `dynamic: true|false|"strict"`.

**Field types:**

| Type | Indexed as | Ops and aggregations |
|---|---|---|
| `keyword` | normalized value (casefold and whitespace fold), its trigrams, doc values | `eq`, `ne`, `in`, `exists`, `contains*`, `starts_with`, `similar`; terms agg, sort |
| `text` | keyword plus word tokens | everything `keyword` supports, plus `words_all` / `words_any` |
| `keyword_list` | normalized entries (a JSON array, or a string split on `", "`), multi-valued doc values | `has`, `has_any`, `has_all`, `empty`, `nonempty`, `exists`; terms agg |
| `number` | float64 doc values, plus a BKD-style point index for ranges | comparisons, `between`, `exists`; range, histogram and stats aggs, sort |
| `bool` | term | `eq`, `ne`, `exists`; terms agg |
| `date` | Unix milliseconds, same as `number` | comparisons; date histogram, sort |

- **Dynamic mapping** (the default) types a field from its first value. Strict mapping refuses unmapped fields. Mapping changes are additive only.
- **Normalization is byte-for-byte compatible with scrape-bot:**
  - text is `" ".join(s.split()).casefold()`;
  - words are NFKC, then non-`\w` runs folded and padded;
  - entries are split on `", "`;
  - NUL becomes U+FFFD;
  - numbers are finite floats only, never bools.

  Parity fixtures from scrape-bot pin these rules.
- **Document:** an id (1–512 bytes) and a JSON object. Its version is the `seq` of its last change, which also supports optimistic concurrency (`if_seq`).
- **Saved query:** an id, a DSL tree and an opaque `meta` JSON object (16 KB or less).

## 4. Query language

The query is a tree of groups and conditions, the same shape scrape-bot uses:

```json
{"all": [...]}  {"any": [...]}  {"not": node}  {"field": "brand", "op": "eq", "value": "Acme"}
```

- **Root.** `{"all": []}` matches every document.
- **Bounds:** at most 4 levels deep (only `all`/`any` count), 50 conditions and 100 nodes. Strings are at most 500 characters with no NUL, and lists at most 200 entries.
- **Operators:**
  - `eq`, `ne`, `in`;
  - `lt`, `lte`, `gt`, `gte`, `between`;
  - `exists`;
  - `contains`, `contains_any`, `contains_all`;
  - `starts_with`;
  - `words_all`, `words_any`;
  - `similar` (`{text, min}`, trigram similarity compatible with pg_trgm);
  - `has`, `has_any`, `has_all`;
  - `empty`, `nonempty`.
- **The `_id` pseudo-field** refers to the document id.
- **Missing fields** match nothing except `exists:false`, `empty` and `ne`.
- **Errors** are problem JSON with `loc` paths such as `query.all.2.any.0.value`.
- **Aggregations** (v1):
  - `terms` (size, min_doc_count), `range`, `histogram`, `date_histogram`, `stats`, `cardinality` (HyperLogLog++);
  - nested one level: a bucket aggregation with metric sub-aggregations.

## 5. HTTP API

The API is in the Elasticsearch style:
- requests and responses are JSON, except `_bulk`, which is NDJSON;
- errors are problem JSON;
- every write answers with its `seq`.

| Endpoint | Purpose |
|---|---|
| `GET /indexes` | List every index |
| `PUT/GET/DELETE /indexes/{i}` | Create (with mapping and settings), inspect, drop an index |
| `PATCH /indexes/{i}/mapping`, `PATCH /indexes/{i}/settings` | Add fields; change `refresh_interval` (applied at once, `-1` for on demand only) or the replica target |
| `PUT/GET/DELETE /indexes/{i}/docs/{id}` | One document (`if_seq`, `refresh=true\|wait_for`) |
| `POST /indexes/{i}/_bulk` | NDJSON upserts and deletes; one transaction per request; `?percolate=true` also returns each upserted document's matching saved queries |
| `POST /indexes/{i}/_search` | `{query, sort, size, search_after, track_total, aggs, fields, timeout}` |
| `POST /indexes/{i}/_count` | Matching count |
| `PUT/GET/DELETE /indexes/{i}/queries/{id}`, `GET /indexes/{i}/queries` | Saved queries |
| `POST /indexes/{i}/_percolate` | Documents, or stored ids, in; matching query ids per document out |
| `GET /indexes/{i}/_fields?entries=N` | Field catalogue with the top list entries |
| `GET /_cluster/health`, `GET /_cluster/nodes`, `GET /_cluster/shards` | Topology, shard placement, lag (the operator view) |
| `GET /healthz`, `GET /readyz`, `GET /metrics` | Liveness, readiness, Prometheus |

- **Consistency:**
  - `refresh=wait_for` returns once the write is searchable on the coordinator's copy;
  - any read takes `?wait_for_seq=N`;
  - by default, writes become visible within `refresh_interval` everywhere.
- **Auth:** bearer tokens, read-only or read-write. The internal peer API uses a separate cluster token.
- **Limits:** request body size, bulk operation count, and per-request deadlines (`timeout`, with partial results flagged like Elasticsearch's `timed_out`).

## 6. Segments and shards (the performance core)

### Segment format (`internal/segment`)

A segment is immutable and written once at refresh or merge time. It is one file per segment plus a small manifest, read through mmap.

| Part | Contents |
|---|---|
| Term dictionary | Per field and kind (value, entry, word, trigram), sorted terms in prefix-compressed blocks with a sparse in-memory index. Lookup is O(log blocks), plus a short scan inside one block. |
| Postings | Roaring bitmaps in serialized form, used directly from mmap with no copy |
| Doc values | Columnar per field: numbers delta- and bit-packed; keywords as ordinals into a per-segment sorted dictionary; multi-valued lists as offsets plus ordinals. Used for sorting, aggregations and residual filters. |
| Points | Per-field blocks of documents sorted by value, with min/max (a BKD-lite), giving range queries without scanning; a partly covered block reads values from the doc-value column |
| Stored fields | s2-compressed blocks of about 4 KB of original JSON, against a per-segment dictionary of the segment's first documents, fetched for hits only |
| Live docs | A roaring bitmap of deleted documents, with a sidecar file per generation (the segment itself is never rewritten) |
| Percolator queries | A segment kind of its own holding saved queries' compiled anchors (§7) |

### Shard copy (`internal/shard`)

- **Write buffer.** Applied changes go into an in-memory buffer that is searchable after the next refresh. Every `refresh_interval` the buffer is refreshed into a new small segment, and a new **generation**, the immutable list of segments, is published through an atomic pointer. Readers hold a generation reference with no locks.
  - Refreshes tick on a fixed grid (`t0 + k·refresh_interval`), so a refresh's own cost never stretches the period. One that overruns skips the ticks it missed rather than running again at once, and refreshes never overlap. Changing `refresh_interval` re-anchors the grid.
  - `refresh=wait_for` waits for the first published generation that covers the write's seq, like Elasticsearch; it never forces a refresh, and never waits for durability. `refresh=true` forces one.
- **Deletes and updates** mark live-docs in older segments and are written as sidecar files.
- **Merges.** A tiered policy, like Lucene's TieredMergePolicy, merges small segments in the background under an I/O and CPU budget. It drops deleted documents and keeps the segment count low.
- **Durability: refresh is visibility, flush is durability** (as in Elasticsearch). The SQL changelog is the write-ahead log, so there is no per-node translog, and nothing a copy publishes needs to be durable on the copy first.
  - **Refresh** writes the buffer's segment without fsync and keeps the deletes it masks in memory, then publishes the generation. It costs CPU and page-cache writes only.
  - **Flush** makes the published generation durable. It runs every `flush_interval` (10 s), at shutdown, at every merge commit and before a peer snapshot. It fsyncs the segment files no flush has synced, writes and fsyncs the deletes sidecars, swaps the local manifest (temp file, fsync, rename, directory fsync), and only then advances `CommittedSeq` to the manifest's `seq`. A failed fsync is never retried (after a write-back error a later fsync can succeed over lost pages): it fails the copy, which reopens from its last flushed manifest and replays.
  - **`CommittedSeq` only ever claims what is durable on disk.** It is the `seq` a copy reports as applied, so the changelog is never pruned past a change some copy could still lose. After a crash a copy reopens the segments of its last flushed manifest and replays the changelog from that manifest's `seq`: at most `flush_interval` of changes.
  - **Files** are removed only once no durable manifest lists them: a sidecar by the first flush whose manifest drops it, a merged-away segment once that flush has run and the last reader has released it. Open removes whatever a crash left that the manifest does not list.
  - **A merge** publishes its segment, then flushes. Its manifest therefore lists the merged segment together with every generation published before it, never an unsynced file, and its inputs can go as soon as readers release them.
  - **A peer snapshot** flushes first, then streams the generation that flush made durable, so a copy never hands out a `seq` it could itself lose.
- **Caches:**
  - a **filter cache**: an LRU of bitmaps for frequent leaves, per segment, invalidated naturally because segments are immutable;
  - a **rank cache** of each segment's id ranks for tied sorts: 256 MiB, at most 64 MiB for one segment;
  - a **global-ordinals cache**: per keyword or list field and segment list, the map from segment ordinals to the shard's sorted union of terms, for terms and cardinality over many of a field's terms; 256 MiB, rebuilt when the segment list changes, at a cost that grows with the field's distinct terms;
  - the OS page cache for mmap.

### Query execution (`internal/search`)

1. **Plan.** Normalize the query, then order leaves by cost (estimated from per-segment term frequencies and doc-value stats). Bitmap-able leaves become roaring operations: AND intersects smallest first, OR unions, NOT takes the complement against live-docs. Value leaves become residual filters over doc values: `contains` with a trigram prefilter, `similar` with trigram candidates and then an exact check, and ranges served by points.
2. **Execute per segment in parallel,** with a goroutine pool sized to GOMAXPROCS. Each segment produces a hit bitmap, per-segment top-k for the sort (a heap over doc values), and partial aggregations.
3. **Reduce** across segments, then across shards on the coordinator. `search_after` paging uses the sort values plus id. Counts are exact up to `track_total`, otherwise reported as `≥`.
4. **Early termination.** A sort on the index's natural order, or a `size` with no total needed, stops as soon as top-k is settled.

## 7. Percolator

The percolator reverse-indexes the saved queries.

- **Anchors per saved query:**
  - **`eq` / `in`:** value keys.
  - **`has*`:** entry keys.
  - **`words_*`:** per phrase, the pair of its two rarest distinct words, or its one word. A phrase is found only where each of its words is, so a matching document holds both halves.
  - **`contains*` / `starts_with`:** the needle's rarest 3-character window. A needle shorter than three characters anchors on "the field has a text".
  - **`similar`** (min > 0): the union of the text's pg_trgm trigram keys. This is sound because a document sharing no trigram has similarity exactly 0 (`SimilarityKeys`), and 0 is below any minimum the matcher accepts (0 < min ≤ 1).
  - **Ranges:** interval anchors.
  - **`exists:true` / `nonempty`:** `present` keys.
  - **`all`:** the lowest-frequency child set. Frequencies come from the shard's document statistics.
  - **`all`, pairs:** when cheaper, a pair of two children whose sets are made only of value, bool, entry or word keys, anchored on every pair of one key from each, at most 16 pairs per group. This is sound because both children hold whenever the group does, so a matching document holds a key of each. A document finds its pairs by walking the partner lists of the pair halves it holds, in time linear in those lists rather than quadratic in its keys. Past a budget, it adds every query any held half belongs to, which is also sound.
  - **`any`:** the union of the children's sets. One unanchorable child makes the whole group unanchorable.
  - **Unanchorable:** `not`, `ne`, `exists:false`, `empty`, and the root `{"all": []}`. These go on the always-check list.
- **Posting filters.** A query anchored on terms alone stores one cheap conjunct of its root with each of its postings. A document holding the term is a candidate only if its value of that field passes the conjunct. This is sound because every match satisfies every conjunct of its root.
  - **Kinds:** a range on a number (`lt` and `gt` kept closed, which only admits a candidate that verification refuses), `eq` on a bool, a text the field must not equal (`ne`, or `not eq`, on a string of at most 15 bytes), or an entry it must not hold (`not has` of one entry).
  - **Choice:** the conjunct that leaves the least to verify, then a range, which prunes the most candidates.
- **Query segments** (format `percolate/3`). Each refresh writes the queries it adds as one file per shard, merged like document segments under a policy of their own (two segments per tier, against ten for documents), because every query segment costs every percolated document a full probe.
  - **Contents:**
    - the hash term dictionary, mapping each term to postings and filtered postings of queries;
    - pair partner lists;
    - an interval tree per numeric field;
    - the always-check list;
    - the stored queries (their JSON, for merges and reads);
    - one compiled program per verification class;
    - every id as its JSON string literal.
  - **Opening.** A segment is memory-mapped, then checked against its CRC32C and validated structurally: every offset, rank, tree link, filter and program, and every id literal against its query's id. A damaged or crafted file is refused, and no accessor of an open one reads out of range. Nothing per query is decoded onto the heap: a segment of a million queries costs its mapped pages.
- **Ranks.** Within a segment, everything percolation reads is numbered by rank, a query's position sorted by id: postings, filters, pairs, intervals, the always list, programs and id literals. Candidates verified in rank order read the segment front to back and match in output order. Records stay numbered by the ordinal the shard deletes and merges by.
- **Verification classes and memo.** Queries whose canonical form is the same share a class: one program, verified once per document, with the verdict memoized for the rest. A `words_*` query's class is its exact JSON, because phrases are read as written. Queries proven on different leaves are in different classes. Open keeps a dense copy of every query's class, which also marks every query whose class's program is empty: such a candidate matches, and a memoized class's members are decided, without their records being read.
- **Programs.** Each class's query is compiled at build time into a compact byte program that names fields by their index in the segment. It mirrors the matcher exactly: the same constant folding, the same reading of every value, children cheapest first. Verification evaluates it over a dense slice of the document's values, laid out once per segment: one map lookup per field, none per candidate.
- **Proven conditions.** A condition at a query's root (a leaf, or the `not` of one) that every route to its candidacy implies holds on every candidate, so the program leaves it out. A route is a term it is anchored on (with its posting filter), or a range anchor. For example, `brand eq + price lte` with its price filter verifies nothing, and neither does `brand in + not condition eq used` with its text filter.
- **Gram prefilter.** Each field with gram terms has a bit filter of them, 16 bits per term (512 bits to 64 Mbit). A window whose bit is clear skips the dictionary.
- **Per document:**
  1. Analyze it for matching: every value the matcher reads, but no `Value.Grams`, which the percolator never reads.
  2. In each query segment, lay out the document's values by field index. Probe with its atoms: values, entries, words, every 3-character window of the whole text, trigram keys, `present` keys and numbers. Atoms are never truncated or sampled. A filtered posting adds its query only if its filter passes.
  3. Add the always-check list.
  4. Verify each candidate, in rank order, with its class's program.
  5. Each segment's matches are a run of ranks, so sorted by id. The runs are merged (a heap over the segments; the least run's ids below the next least run's are found by galloping and copied in one go) straight into the JSON array of ids, which the response copies as it is.
- **One document on several cores.** A request percolating fewer documents than there are threads gives each document an equal share of them as workers, when each worker gets at least 16k saved queries and the workers of the documents already being split leave room on the threads. The document's queries are cut by id into two windows per worker: the pivots are quantiles of ids sampled from every query segment in proportion to its size, and a window holds, in every segment, the ranks whose ids lie between two pivots. Workers take the windows in turn; each runs steps 2 to 5 on its window alone (postings, filtered postings, intervals, pairs and the always-check list are cut to it). Windows hold disjoint id ranges in order, so their arrays concatenate into the answer. The plan is computed once per set of query segment files.
- **Batches.** `_bulk?percolate=true` and `_percolate` with many documents process documents in parallel.
- **Format upgrade.** A copy whose query segments are in an older format (`percolate/2`) does not open (`ErrOlderFormat`). The node wipes it and rebuilds it from a peer or the database, and the new segments are `percolate/3`. A binary rolled back to `percolate/2` refuses a `percolate/3` copy (`ErrNewerFormat`) and leaves its files as they are.
- **Completeness guarantee.** A failure of any of these tests blocks release:
  - A property test checks that the candidates always include every true match, and that the answer equals brute force with `query.Match`.
  - A second checks every program against `query.Compile(...).Match` over random queries and documents, non-candidates included.
  - Cross-checks run the benchmark's shapes and adversarial root conjunctions through segments with deletes, whole and split into windows, and scrape-bot's parity fixtures through the percolator.
- **Where the p99 target is measured** (operator decision, 2026-10-05). The < 1 ms p99 per document at 100k queries (§1) is server time, measured the way Elasticsearch's `took` is: from the request's admission to the last byte of its answer encoded. It covers analysis, matching and encoding, and leaves out the network and the client's decoding. `_percolate` reports it as `took_us` (and `took_ms`, and a `Server-Timing` header). The benchmark reads each engine's own server time the same way.

## 8. Storage (SQL)

### Interface

```go
type Store interface {
    Migrate(ctx) error
    Apply(ctx, batch []Change) (firstSeq, lastSeq int64, err error) // one transaction, ordered seq
    ChangesAfter(ctx, shard ShardID, seq int64, limit int) ([]Change, error)
    ScanShard(ctx, shard ShardID, fn func(Record) error) (asOfSeq int64, err error)
    Registry(ctx) RegistryStore // nodes, heartbeats, shard leases
    Blobs(ctx) BlobStore        // optional segment and snapshot blobs
    Prune(ctx, shard ShardID, belowSeq int64) error
    Ping(ctx) error
}
```

### Tables

The logical schema is the same in every dialect.

| Table | Columns |
|---|---|
| `sl_indexes` | name, mapping, settings |
| `sl_documents` | index, shard, id, body, seq |
| `sl_queries` | index, shard, id, query, meta, seq |
| `sl_changes` | `seq` BIGINT PK, index, shard, kind, id, payload, at; index on (index, shard, seq) |
| `sl_counter` | the sequence row, locked per `Apply` |
| `sl_nodes` | node_id, address, version, heartbeat_at, capacity |
| `sl_shard_copies` | index, shard, node_id, state (`recovering` / `serving` / `retiring`), applied_seq, lease_until |
| `sl_blobs` | optional segment bundles for recovery without a peer |

- **Write ordering.** `Apply` locks the counter row, takes contiguous `seq` values and commits. Visibility order therefore equals `seq` order, and tailers never skip a late commit. Throughput comes from `_bulk` batching and from group commit: concurrent requests on a coordinator are coalesced into one transaction every few milliseconds, the way Elasticsearch amortizes translog fsyncs.
- **Shared logic, per-engine SQL.** The store's logic (transaction shapes, retries, seq allocation under the counter lock, lease fencing, the blob protocol, the Apply guards) is written once; each dialect package owns every statement, spelled its engine's best way (Postgres arrays and RETURNING, MySQL multi-row VALUES and row-alias upserts, SQLite prepared rows and RETURNING). JSON is stored as text and no dialect JSON functions are used.
- **Drivers:** `pgx/v5/stdlib`, `go-sql-driver/mysql`, `modernc.org/sqlite`.
- **Supported versions:** Postgres (tested on 17), MySQL 8.0.19+ (tested on 8.4 LTS, with `max_allowed_packet` of at least 64 MB, the default), SQLite 3.35+ (bundled by modernc.org/sqlite).
- **Migrations** are embedded and run under a lock.

## 9. Cluster: plug-and-play replicas

- **Membership.** On start a node registers in `sl_nodes` and heartbeats every 2 s. A node whose heartbeat is older than 10 s is dead. Peers are discovered from the same table, so the only setting is `store_url`, plus `advertise_address` when behind NAT.
- **SQLite serves one node.** Its single writer cannot be shared fairly by several processes, so a cluster needs Postgres or MySQL: a node refuses to join a SQLite store another live node already uses (only in-process tests may opt out).
- **Shard allocation.** This uses database leases, with no consensus library.
  - Each node runs an allocator loop. For each shard below its target copy count, an eligible node (one not already holding a copy, with spare capacity) claims a copy row with a conditional insert or update. Leases are renewed with heartbeats.
  - When a node dies, its leases expire and other nodes claim those shards.
  - The default target is "every node holds every shard". This is simplest, and any node can serve any read locally.
  - Large indexes set a lower target. The coordinator then scatters to the owners.
- **Peer recovery.** A new copy:
  1. asks a serving peer for its current segment files over the internal API (a streamed, checksummed copy, resumable by file); the peer flushes first and streams the generation it made durable;
  2. falls back to `sl_blobs`, or to `ScanShard` from SQL;
  3. then replays the changelog from the segments' `seq` and switches to `serving`.

  It's like Elasticsearch's file-based peer recovery followed by an operations replay.
- **Writes anywhere.** Any node accepts writes. They go to SQL first, which is the commit point, and every copy applies them through its tailer.
  - **Fast path:** the coordinator's own copies apply immediately on commit.
  - **Push hint:** the coordinator pushes a "new `seq`" hint to the peers holding the shard, which wakes their tailers. Polling still runs as the safety net (Postgres `LISTEN/NOTIFY` where available).
- **Read routing.** The coordinator prefers local copies. Otherwise it picks the least-loaded serving copy whose lag is within `max_lag`, with adaptive replica selection by measured latency, like Elasticsearch's ARS. It retries another copy on failure, so one node failing doesn't produce a client error.
- **Rolling restarts and upgrades.**
  - On shutdown a node marks its copies `retiring`, finishes in-flight requests, writes its manifests and exits.
  - It comes back by reopening its segments and replaying the tail of the changelog.
  - The on-disk format is versioned, and a node refuses segments from a newer major version.
  - A node reads segments of its own major and the one before it (N−1), so an upgrade across one major reopens its segments; merges rewrite them into the new major, and peer recovery prefers peers already on it.
- **Changelog pruning** stays behind the lowest `applied_seq` of any live copy, and behind the oldest retained recovery point. A copy's `applied_seq` is its `CommittedSeq`, what its last flush made durable (§6).

## 10. Failure handling

| Failure | Behaviour |
|---|---|
| Database unreachable | Writes return 503. Reads keep serving from segments, marked `stale`. Readiness goes false after `max_lag`, and the lag metric rises. |
| Node crash | Its leases expire after 10 s, and other nodes take its shards up to the target. Clients retried by the coordinator see no error when ≥ 2 copies exist. |
| Disk lost or corrupt | Segments are checksummed. A bad segment is dropped and the copy re-recovers from a peer or SQL. It never serves a corrupt segment. |
| Apply error | It halts that shard copy, which goes `recovering`, and re-recovers. It never skips a change. |
| Merge interrupted | A merge writes a new file, publishes it, then flushes: until that flush swaps the manifest atomically, the old durable segment set is still valid and none of its files is removed. |
| Database failover (e.g. Postgres primary switch) | The store reconnects with backoff. Sequence numbers are in the database, so they survive. |
| Overload | Per-request deadlines, bulk size caps, a merge I/O budget, and a search thread pool with a bounded queue (429 when full, like Elasticsearch). |

## 11. Observability

- **Logs:** `log/slog` JSON with `trace_id`, `span_id`, `node_id`, `index`, `shard`, `route`, `status` and `duration_ms`.
- **Traces:** OpenTelemetry spans for HTTP, plan, per-segment execution, reduce, percolate (probe and verify), apply, refresh, merge, recovery and SQL. They propagate across internal peer calls, so one trace covers the scatter-gather.
- **Metrics:** OTel, exported via OTLP and Prometheus.
  - Requests: rates, latencies and errors by route.
  - Search: phase timings, segments touched and filter-cache hit ratio.
  - Percolation: candidates, verifications and always-check size.
  - Indexing: rate and group-commit batch size.
  - Refresh and merge: time, merge backlog, segment count per shard.
  - Size: documents, terms, disk and mmap-resident bytes.
  - Replication: lag (in seq and in seconds) per copy, recovery progress, and lease and allocation changes.
  - Database: SQL errors and latency.
- **Profiling:** `pprof` on the admin listener.
- **Operator view:** `GET /_cluster/health`, which is green, yellow or red, defined as in Elasticsearch: all copies serving, the target not met, or a shard without a serving copy.

## 12. Configuration

Only `store_url` is required. Everything else has a production default, and environment variables use the `SEARCHLIGHT_*` prefix.

| Setting | Default and meaning |
|---|---|
| `listen` | public API address |
| `admin_listen` | metrics and pprof |
| `advertise_address` | the address peers use to reach this node |
| `data_dir` | local segments |
| `tokens_file` | API tokens |
| `cluster_token` | auth for the internal peer API |
| `peer_ca_file` | CAs that sign peers' TLS certificates (system roots by default) |
| `lease_ttl` | 30 s on SQLite (a single node), 10 s elsewhere |
| `prune_stall_timeout`, `retiring_retention`, `changelog_retention` | 15 min, 15 min, 24 h: the changelog's prune bounds |
| `refresh_interval` | 1 s |
| `flush_interval` | 10 s: how often a copy makes what refreshes published durable, and so how much changelog a crash replays |
| `max_lag` | 2 s |
| `merge_budget` | I/O and CPU budget for merges |
| `search_threads` | defaults to GOMAXPROCS |
| `gc_heap_floor` | 64 MiB: the heap below which the GC does not run (0 = off; GOGC overrides) |
| `log_level` | log verbosity |

Configuration is validated at start.

## 13. Delivery phases

All phases belong to v1. Each phase ends with its benchmark targets met before the next starts.

1. **Core single node.**
   - Analysis, query, the segment format, a shard with refresh and merge, search with aggregations, the percolator.
   - The SQL store with the changelog and group commit.
   - The HTTP API and telemetry.
   - The Elasticsearch benchmark harness.
2. **Cluster.** Registry, leases and allocation, peer recovery, routing with retries and adaptive selection, sharding with scatter-gather, rolling restarts, cluster health.
3. **Hardening.** Chaos tests, format versioning, blob-store recovery, tuning against the benchmark until every §1 target is met.

## 14. Testing and benchmarking

- **Correctness tests:**
  - Unit tests in every package. Parity fixtures from scrape-bot pin normalization and matching byte for byte.
  - Property tests:
    - search equals brute force over random documents and queries, including after merges and deletes;
    - percolator completeness;
    - convergence of replicas and shards under random interleavings;
    - aggregations equal brute force.
  - A store conformance suite runs on SQLite by default, and on Postgres and MySQL when their URLs are set. It checks sequence ordering under concurrent writers and group commit.
- **Chaos and integration tests** run a three-node cluster in process and over localhost:
  - kill a node mid-bulk, mid-merge and mid-recovery;
  - restart the database;
  - corrupt a segment;
  - a rolling upgrade.

  Invariants: no acknowledged write is lost, replicas converge, and no client error occurs with two or more copies.
- **Benchmark harness** (`bench/`), modelled on Elasticsearch's Rally:
  - **Datasets:** synthetic product catalogues of 1M and 10M documents, scrape-bot-shaped (keywords, lists, numbers, long text), plus a scrape-bot export.
  - **Workloads:** bulk ingest, filter search, sorted search, aggregations, percolation at 10k and 100k saved queries, mixed read and write, recovery time, restart time.
  - **Comparison:** the same workloads run against Elasticsearch 8.x in Docker, on the same machine, with the same data and query semantics translated to the Elasticsearch DSL.
  - **Output:** a report with throughput, p50/p99/p999, disk and RSS, as a table in the README and per release.
  - **CI:** the run goes in CI on Linux runners with Docker (Docker is unavailable on the dev machine; local runs use a remote or WSL host).
- **CI:** `go vet`, `golangci-lint`, `go test -race ./...` (SQLite), and Postgres and MySQL service containers. The benchmark smoke test runs on every PR, and the full benchmark runs on demand and on release.

## 15. Repository layout

```
go-searchlight/
  cmd/searchlight/
  internal/{config,analysis,query,segment,shard,search,percolate,cluster,replica,store,api,telemetry}/
  internal/store/{postgres,mysql,sqlite}/   dialects + embedded migrations
  api/openapi.yaml
  bench/                    harness, datasets, ES translation, docker-compose for ES
  testdata/parity/          fixtures from scrape-bot
  docs/                     spec, API reference, operations, benchmark results
  deploy/                   Dockerfile (distroless), compose and Kubernetes examples
  .github/workflows/        ci.yml, bench.yml
  .golangci.yml  Makefile  go.mod (go 1.25)
```

## 16. scrape-bot integration

This is a separate scrape-bot epic, after Searchlight phase 1.
- **Client and rollout:** a Python client, plus a `[search] engine = "sql" | "searchlight" | "shadow"` setting. Shadow mode runs both engines and logs any difference.
- **Per-batch writes:** the diff engine sends each batch with `_bulk?percolate=true` and derives edges against its own match rows.
- **Search features:**
  - Saved searches become saved queries, with the search id kept in `meta`.
  - Listings and preview call `_search`, and the catalogue calls `_fields`.
  - The `link` condition becomes `has` on a client-filled `_links` field.
- **History searches** stay in the bot's SQL path.

## 17. Decisions

| Item | Decision |
|---|---|
| Write-ahead log | SQL changelog; no per-node translog |
| Durability of acknowledgement | committed to SQL |
| Coordination | database leases only |
| Default copies | every node holds every shard |
| `similar` anchoring | trigram keys (union) when min > 0 |
| Conjunctive anchors | pairs of two `all` children, at most 16 per group, probed in linear time |
| History ops | stay in scrape-bot |
| BM25 / nested documents | v2 |
| Go | 1.25 |
| System of record | SQL for document indexes; no embedded consensus database |
| T6 disk | A deliberate trade-off: 3-grams on every keyword and text field cost more disk than Elasticsearch's index; the bench reports node-local disk but grades only resident memory |
| Logs (part 2) | `stream` indexes with segments in blob storage and SQL as the metastore; Grafana via the Loki API; SLQ query language ([roadmap](2026-10-05-searchlight-part2-logs-roadmap.md)) |
