# Part 2 roadmap: Searchlight Logs (a log explorer)

**Status: parked until part 1 (v1) ships.** Part 1 is everything in
[2026-10-02-searchlight-design.md](2026-10-02-searchlight-design.md): Task 14 tuning, then the final
whole-branch review and release. Nothing here is built before that. Tracking:
[#55](https://github.com/Imposter/go-searchlight/issues/55).

Decided with the operator (2026-10-05):
- **Front end:** Grafana, through a Loki-compatible API, so Explore, logs panels, live tail and dashboards work with no plugin.
- **Query language:** pipes with SQL expressions (Kusto/PPL style), called SLQ. LogQL is parsed into the same plan as a compatibility dialect.
- **Scale:** large. Hundreds of thousands of lines per second, object storage, many nodes. The bar is Loki and Elasticsearch on the same hardware.
- **System of record:** SQL stays the system of record for document indexes, and no embedded consensus database is built. The SQL database holds the one durable copy and coordinates. The nodes' segments are rebuildable caches, so the cluster layer replicates derived data, not a second truth.
## Design sketch (to become the part 2 spec)

### 1. Storage: `stream` indexes, with segments in object storage and SQL as the metastore

This follows the Quickwit/Loki model, and keeps SQL as the only coordinator (spec §1 and §9).

- **Index kind and writes.** An index is created with `kind: "stream"` and a `timestamp_field`. It is append-only with generated ids. The only deletes are retention and admin time-range deletes.
- **Ingest durability.**
  - A coordinator buffers events per stream shard. Indexing ownership comes from the existing lease allocator.
  - It writes compressed batches durably: a short-lived batch blob, plus a catalogue row committed in SQL. **That commit is the ack point.**
  - Sealed, time-partitioned **splits** (segment format 4) are uploaded to the blob store. The batches are pruned once their split is committed.
- **Blob backends** behind `store.BlobStore` (`internal/store/store.go:151`):
  - `sl_blobs` (exists) for small installs;
  - new filesystem and **S3/MinIO** backends for real volume.
- **Split catalogue** `sl_stream_splits`, in all three dialects. Columns: index, shard, min/max timestamp, doc count, size, blob name, label summary, state. Merges swap splits atomically, fenced by lease epoch.
- **Reads.** Any node serves any split. Hot splits are cached on local disk (mmap, LRU); cold splits are fetched on demand. Nodes replicate no durable data.
- **Retention** drops whole splits, with rollover by time or size.
- **Indexing policy per field**, which matters at large scale:
  - Labels (low-cardinality stream keys such as service, level and host) get full postings.
  - The message gets a word index, plus a **per-split bloom/ngram sketch** for needle search instead of full trigram postings. v1's trigram index costs ~133 B/doc on long text.
  - Other fields are kept in stored/columnar form for query-time filtering.
  - Benchmark-driven; configurable per stream.

### 2. Query language: SLQ — pipes, with SQL expressions

The model is Kusto, Splunk SPL and OpenSearch PPL: one string, left to right, where each stage uses SQL-style expressions.

```
logs{service="api", env="prod"}
| where level = 'error' and msg ~ 'timeout|refused' and ts > now() - 1h
| parse json
| where status >= 500
| stats count(), p99(duration_ms) by route, bin(ts, 1m)
| sort count desc | limit 20
```

- **Source and selector:** `index{label=…}`, pushed down to postings and split pruning.
- **Stages:**
  - `where` (SQL boolean expressions: `= != < > in like`, `~` regex, `contains`, `has`, `and/or/not`, functions);
  - `parse json|logfmt|regex '…'|pattern '…'` (query-time extraction);
  - `extend x = expr`;
  - `project`;
  - `stats agg() by …` (count, sum, avg, min, max, percentiles, count_distinct, rate) with `bin(ts, span)`;
  - `sort`;
  - `limit`;
  - `top n by`;
  - `dedup`.
- **Compilation.** The planner turns SLQ into the existing engine plan where it can: filters become `query` conditions and index lookups, `stats` becomes aggregations. Unindexed predicates run as a residual over columns or stored fields. Regex gets a trigram/ngram prefilter, as Zoekt and pg_trgm do.
- **Endpoints:** `POST /_slq` (and `GET` for Grafana). Responses are tabular or log-line results, with time-series frames for `stats … by bin()`.
- **JSON DSL stays.** The existing JSON query DSL remains for programmatic clients, and SLQ is a superset for logs.

### 3. Grafana compatibility, with no plugin needed at first

- **Loki HTTP API** subset:
  - `/loki/api/v1/query` and `query_range`;
  - `labels`, `label/{name}/values`, `series`;
  - `index/volume`;
  - `/tail` (websocket);
  - `/push` (protobuf and JSON).

  **LogQL** is parsed into the same SLQ plan, as a compatibility dialect: Grafana Explore, logs panels, live tail and Loki-style dashboards work out of the box.
- **Ingest compatibility:** Loki push (Promtail, Grafana Alloy, Fluent Bit), **OTLP logs**, and `_bulk`.
- **Later:** a native Searchlight Grafana data source plugin, so SLQ is usable directly in Grafana. It is a follow-up and not required for v2.0.

### 4. Engine features this needs

- Regex matching with an ngram prefilter.
- Percentile sketches (DDSketch or t-digest).
- Rate and over-time range functions.
- Multi-level `stats by`.
- Live tail. **The percolator already is a live filter over incoming documents**: tail = a temporary saved query on the stream plus a websocket fan-out.
- Context queries (N lines before and after a timestamp, via `search_after` both ways).
- High-cardinality dynamic fields (relax `max_index_fields` for streams by keeping unindexed fields columnar).
- Multi-tenancy (`X-Scope-OrgID` → tenant-prefixed indexes).

### 5. Benchmarks

The bar is **Loki and Elasticsearch on the same hardware**, with Quickwit for reference. A log dataset is added to slbench. Metrics:
- ingest lines/s per node at equal durability;
- bytes per line in object storage;
- needle-search and label-filter latency over 1h / 24h / 7d;
- metric-query latency (`count by bin`, `p99`);
- live-tail delay;
- query cost on cold (object-storage) splits.

## Order (after part 1)

1. Storage: stream index kind, ingest durability, split catalogue, blob backends (filesystem, S3), split cache, retention, rollover and merges.
2. SLQ core: parser, planner onto the existing engine, regex with an ngram prefilter, query-time parsing, `stats` with percentiles and `bin()`.
3. Loki API with LogQL → SLQ, plus ingest via Loki push, OTLP and `_bulk`. Grafana works at this point.
4. Live tail on the percolator, context queries, range functions, multi-tenancy.
5. Log benchmarks against Loki and Elasticsearch, then tuning. Later: a native Grafana data source plugin for SLQ.
