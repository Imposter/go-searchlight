# Searchlight design

**Status:** approved design, 2026-10-02
**Repository:** `github.com/Imposter/go-searchlight` (local at `E:/code/go-searchlight`; no remote yet)

## 1. Purpose

Searchlight is a general-purpose search engine written in Go. It works like Elasticsearch, but its durable state lives in **any SQL database**. It does two things:

- **Forward search:** given a query, return the matching documents, sorted and paged, with counts.
- **Percolation:** given a document, return the saved queries it matches. This stays near-instant with tens of thousands of saved queries.

scrape-bot is its first client. No scrape-bot concepts live inside the engine: no sources, snapshots, users or alerts.

### Success criteria

- Percolating a changed document against **10,000+ saved queries** takes well under a millisecond per document, and the time stays roughly flat as queries are added.
- Forward search is at least as fast as scrape-bot's current SQL search (sub-100 ms for typical queries at about 130k documents).
- Postgres, MySQL and SQLite all work as the backing database with no engine code changes.
- Results are identical to scrape-bot's current matcher for its query language (parity fixtures).
- Replicas are disposable. The database is always the source of truth, and a replica rebuilds itself from it.
- Production-grade logging, tracing and metrics come from day one.

### Non-goals (v1)

- Relevance scoring and full-text ranking. Matching is boolean, as in scrape-bot.
- Stateful alerting. Matching is stateless: clients keep their own match state and derive edges.
- History and time-series conditions (`dropped`, `rose`, `lowest_in`, `highest_in`). They stay in scrape-bot.
- Interfaces other than HTTP/JSON (NATS, a SQL dialect, pgwire).
- Indexes larger than RAM. The design leaves room for memory-mapped segments later.
- A Go client library. scrape-bot ships a small Python client.

## 2. Architecture

```
clients ── HTTP/JSON (ES-style) ──► searchlight replica (1..n) ──► any SQL database
                                     api · query · analysis · index
                                     search · percolate · replica
                                     store · telemetry · config
```

There is one binary, `searchlight`. Every replica:

- keeps every index **in memory**: documents, term bitmaps, numeric columns, trigrams, and the percolator's anchor index;
- writes changes to the database in one transaction, together with a **changelog** row carrying a monotonically increasing sequence number;
- **tails the changelog** and applies every change in order, its own included, so all replicas converge;
- writes **snapshots** of its in-memory state periodically. On start it loads the latest snapshot and replays the changelog from there.

### Packages

| Package | Responsibility | Depends on |
|---|---|---|
| `cmd/searchlight` | main: config, wiring, signal handling, graceful shutdown | all |
| `internal/config` | configuration from flags and environment, validated at start | — |
| `internal/analysis` | normalizers: casefold, whitespace fold, NFKC words, list entries, trigrams | `x/text` |
| `internal/query` | DSL AST, JSON parse, validation with problem locations, the exact matcher over an analyzed document | `analysis` |
| `internal/index` | per-index in-memory state: document store, term dictionary → roaring bitmaps, numeric columns, trigram index, mapping | `analysis`, `roaring` |
| `internal/search` | forward execution: compile the AST to bitmap operations plus residual filters, sort, `search_after` paging, counts | `index`, `query` |
| `internal/percolate` | anchor extraction, anchor index, candidate generation, verification | `index`, `query` |
| `internal/replica` | changelog tailer, apply loop, snapshots, recovery, readiness | `store`, `index`, `percolate` |
| `internal/store` | storage interface and dialect implementations, migrations | `database/sql` drivers |
| `internal/api` | HTTP handlers, request limits, auth, problem JSON, `wait_for_seq` | `search`, `percolate`, `replica`, `store` |
| `internal/telemetry` | slog setup, OpenTelemetry tracer and meter, Prometheus `/metrics`, pprof | otel |

Each package can be understood and tested on its own. `store` is the only package that talks to the database, and `api` is the only one that talks HTTP.

## 3. Data model

### Indexes and mappings

An **index** has a name (`[a-z0-9_-]{1,64}`) and a **mapping** from field name to type:

| Type | Stored as | Supports |
|---|---|---|
| `keyword` | normalized text (casefold and whitespace fold), plus its trigrams | `eq`, `ne`, `in`, `exists`, `contains*`, `starts_with`, `similar` |
| `text` | `keyword` plus word tokens | everything `keyword` supports, plus `words_all` / `words_any` |
| `keyword_list` | normalized entries (a JSON array, or a string split on `", "`) | `has`, `has_any`, `has_all`, `empty`, `nonempty`, `exists` |
| `number` | float64 (finite only; booleans are never numbers) | `eq`, `ne`, `in`, `lt`/`lte`/`gt`/`gte`, `between`, `exists` |
| `bool` | true or false | `eq`, `ne`, `exists` |
| `date` | RFC 3339, held as Unix milliseconds | the same comparisons as `number`, `exists` |

- **Dynamic mapping (default).** The first value seen types a field: string → `keyword`, number → `number`, bool → `bool`, array of strings → `keyword_list`. A string field can be promoted to `text` explicitly.
- **Strict mapping** refuses unmapped fields.
- **Mapping changes** are additive only: new fields, or `keyword` promoted to `text`. A type change needs a reindex into a new index.
- **Value mismatches.** A value that does not fit its field's type is stored, but it is invisible to that field's operators, exactly as scrape-bot treats a value of the wrong sort.

### Normalization (parity with scrape-bot)

- **Text:** `" ".join(s.split()).casefold()`. That is Unicode whitespace folding plus full casefold, which turns `ß` into `ss`.
- **Words:** NFKC, then normalize, then runs of non-word characters (`\w`, Unicode) collapsed to a single space, padded as `" w1 w2 "`.
- **List entries:** split on `", "`, strip, drop blanks, normalize each.
- **NUL:** `\x00` becomes U+FFFD on input.
- **Numbers:** finite floats only. NaN, ±Inf, integers too large for a float, and bools are not numbers.
- **Parity:** fixtures generated from scrape-bot's Python normalizers and matcher (`testdata/parity/`) pin these rules byte for byte.

### Documents and saved queries

- **Document:** `{id: string (1–512 bytes), body: JSON object}`. Its version is the `seq` of its last change.
- **Saved query:** `{id: string, query: DSL tree, meta: JSON (opaque to the engine, ≤ 16 KB)}`. Clients put their own identifiers in `meta`; scrape-bot stores its search id and owner there.

## 4. Query language

The tree is the same as scrape-bot's:

```json
{"all": [node, …]}   {"any": [node, …]}   {"not": node}
{"field": "brand", "op": "eq", "value": "Acme"}
```

- **Root.** `{"all": []}` matches every document. A nested empty group is invalid.
- **Bounds:**
  - depth ≤ 4, where only `all`/`any` count as levels;
  - ≤ 50 leaves;
  - ≤ 100 nodes;
  - string values ≤ 500 characters, with no NUL;
  - list values ≤ 200 entries.
- **Ops:**
  - `eq`, `ne`, `in`;
  - `lt`, `lte`, `gt`, `gte`, `between` (inclusive);
  - `exists` (true or false);
  - `contains`, `contains_any`, `contains_all`;
  - `starts_with`;
  - `words_all`, `words_any` (whole words or phrases);
  - `similar` (`{text, min}`, pg_trgm-compatible trigram similarity, 0 < min ≤ 1);
  - `has`, `has_any`, `has_all`;
  - `empty`, `nonempty`.
- **Missing fields** match nothing, except three cases:
  - `exists: false` matches;
  - `empty` matches;
  - `ne` matches, because `ne` is exactly `not eq`.
- **Validation errors** are returned as problems with `loc` paths such as `query.all.2.any.0.value`, in the same shape scrape-bot uses.
- **Pseudo-field.** `_id` refers to the document id and supports the keyword ops.

## 5. HTTP API (v1)

Everything is JSON, except `_bulk`, which takes NDJSON. Errors are `application/problem+json` with a `problems: [{loc, message}]` list. Every write response carries `seq`.

| Method and path | Purpose |
|---|---|
| `PUT /indexes/{i}` | Create an index with `{mapping, settings}`; returns 409 if it exists |
| `GET /indexes/{i}` / `DELETE /indexes/{i}` | Index info (mapping, counts) / drop the index |
| `PATCH /indexes/{i}/mapping` | Add fields or promote a keyword to text |
| `PUT /indexes/{i}/docs/{id}` | Upsert a document |
| `GET /indexes/{i}/docs/{id}` / `DELETE /indexes/{i}/docs/{id}` | Read / delete a document |
| `POST /indexes/{i}/_bulk` | NDJSON of `{"upsert":{"id"…}}\n{body}` and `{"delete":{"id"…}}` lines; one transaction per request; `?percolate=true` also returns, for each upserted document, the ids of the saved queries it matches |
| `POST /indexes/{i}/_search` | `{query, sort:[{field, order}], size (≤1000), search_after, track_total (bool or int cap), fields}` → `{hits:[{id, body?, sort}], total, total_relation, next}` |
| `POST /indexes/{i}/_count` | `{query}` → `{count}` |
| `PUT /indexes/{i}/queries/{id}` | Save or replace a query `{query, meta}` |
| `GET /indexes/{i}/queries/{id}` / `DELETE …` / `GET /indexes/{i}/queries?after=&limit=` | Read / delete / list saved queries |
| `POST /indexes/{i}/_percolate` | `{documents:[{id?, body}]}` or `{ids:[…]}` → `{results:[{id, matches:[query_id…]}]}` |
| `GET /indexes/{i}/_fields?entries=N` | Fields, their types, and the top N entries of list fields with counts (scrape-bot's catalogue) |
| `GET /healthz` / `GET /readyz` | Liveness / readiness (caught up within the lag threshold, and the store reachable) |
| `GET /metrics` | Prometheus |

- **Read-your-writes:** every read endpoint accepts `?wait_for_seq=N`. It waits until the replica has applied N, up to a timeout (default 5 s), then answers 503 `not_caught_up`.
- **Auth:** `Authorization: Bearer <token>`, with tokens from configuration. Each token is either read-only or read-write. Health endpoints are unauthenticated.
- **Limits:** request body ≤ 16 MB, `_bulk` ≤ 10,000 operations, and a per-request deadline (default 10 s, enforced through the context).

## 6. Index internals

For each index:

- **Document store.** A slice of documents indexed by a dense internal `uint32` ordinal. A map goes from external id to ordinal. Deleted ordinals are tombstoned and reused after compaction.
- **Term dictionary.** `(field, kind, term) → roaring bitmap` of ordinals. `kind` is one of `value`, `entry`, `word`, or `trigram`. Equality, `in`, `has*`, `words_*` and `_id` are answered with bitmaps alone.
- **Numeric columns.** Per number or date field, a sorted array of `(value, ordinal)` for range queries (binary search, then a bitmap), plus a per-ordinal value array for sorting.
- **Presence bitmaps.** One per field, for `exists`, `empty`, `nonempty` and `ne`.
- **Concurrency.** Readers use the current immutable **generation**: an atomic pointer to segment structures. The apply loop builds the next generation copy-on-write per changed structure and swaps it in. Readers never take locks.

### Forward execution

1. Compile the AST to a plan. Bitmap-able leaves become bitmap operations: AND is intersect, OR is union, NOT is the complement against the live set.
2. Leaves that need the value itself become residual filters: `contains`, `starts_with` (with trigram prefiltering), `similar` (trigram candidates, then exact similarity), and substring ranges.
3. Order the bitmaps by estimated cardinality.
4. Sort with numeric or keyword columns. Page with `search_after` (`sort` values plus id); `from` is not supported.
5. Counts: exact up to `track_total`, otherwise reported as `≥`.

## 7. Percolator

### Anchor extraction

The goal is to reduce each saved query to the smallest set of exact keys that every matching document must contain.

- **Leaf anchors:**
  - `eq` on a keyword or text field, and `in`: `value` keys;
  - `has*`: `entry` keys;
  - `words_*`: `word` keys (the first word of a phrase);
  - `contains*` and `starts_with`: the rarest `trigram` of the needle;
  - number and date ranges: interval anchors;
  - `_id`: value keys;
  - `exists:true` and `nonempty`: `present` keys (field present, or field holding at least one entry).
- **`all`:** pick the child anchor set with the lowest estimated frequency, using term frequencies from the document index. One required anchor is enough.
- **`any`:** the union of every child's anchors. If any child cannot be anchored, the whole group cannot.
- **Not anchorable:**
  - `not`, `ne`, `exists:false`, `empty`;
  - `similar` (v1);
  - the root `{"all": []}`.

  A query with no anchors goes on the **always-check list**.
- **Interval anchors.** Each number or date field has an interval index: a sorted endpoint list plus a stabbing query. A document's value produces the queries whose interval contains it.

### Per-document percolation

1. Analyze the document into atoms: field values, entries, words, **every** trigram of `text` and `keyword` values, `present` keys, and numeric values. Atoms are never truncated or sampled, because a dropped atom could hide a true match.
2. Look up candidate query ids for every atom, then add the always-check list.
3. Verify each candidate with the exact matcher (`internal/query`) against the analyzed document, in process.

Completeness is guaranteed: for every query and every document, the candidates must include every true match. A property test checks this over random queries and documents, and any violation is a release blocker.

Cost per document is about the atom lookups plus the candidate verifications. It is independent of the total number of saved queries, except for the always-check list. Its size is exposed as a metric, and its share is reported per index.

## 8. Storage

### Interface (`internal/store`)

```go
type Store interface {
    Migrate(ctx context.Context) error
    Apply(ctx context.Context, batch []Change) (seq int64, err error) // one transaction
    ChangesAfter(ctx context.Context, seq int64, limit int) ([]Change, error)
    LoadIndex(ctx context.Context, index string, fn func(Record) error) (seq int64, err error)
    PutSnapshot(ctx context.Context, s SnapshotMeta, r io.Reader) error // optional blob store
    LatestSnapshot(ctx context.Context, index string) (SnapshotMeta, io.ReadCloser, error)
    PruneChanges(ctx context.Context, belowSeq int64) error
    Ping(ctx context.Context) error
}
```

### Tables

The same logical schema is used in every dialect:

| Table | Columns |
|---|---|
| `sl_indexes` | `name` PK, `mapping` JSON text, `settings` JSON text, `created_at` |
| `sl_documents` | `index`, `id`, `body` JSON text, `seq`; PK `(index, id)` |
| `sl_queries` | `index`, `id`, `query` JSON text, `meta` JSON text, `seq`; PK `(index, id)` |
| `sl_changes` | `seq` BIGINT PK, `index`, `kind` (`doc_upsert`, `doc_delete`, `query_upsert`, `query_delete`, `index_create`, `index_drop`, `mapping`), `id`, `payload`, `at` |
| `sl_counter` | one row: `next_seq` |
| `sl_snapshots` | `index`, `seq`, `format`, `blob` (optional), `created_at` |
| `sl_replicas` | `replica_id` PK, `applied_seq`, `snapshot_seq`, `heartbeat_at` (feeds changelog pruning and lag reporting) |

- **Ordered sequence numbers.** Every `Apply` locks `sl_counter` (`SELECT … FOR UPDATE`, which on SQLite is the write lock) and takes the next sequence numbers from it. Writers are therefore serialized, and changes become visible in sequence order. The tailer can never skip a sequence number that commits late. Throughput comes from batching in `_bulk`.
- **Portability.** Only portable SQL is used: `INSERT`, `UPDATE`, `DELETE`, `SELECT … WHERE seq > ? ORDER BY seq LIMIT ?`, and an upsert spelled per dialect. JSON is stored as text, so no dialect JSON functions are needed.
- **Drivers:** `pgx/v5/stdlib`, `go-sql-driver/mysql` and `modernc.org/sqlite` (pure Go, no cgo).
- **Migrations:** embedded per-dialect SQL files with a `sl_schema_version` table, applied at start under a lock.

## 9. Replication, snapshots, consistency

- **Apply loop.** A single goroutine per replica polls `ChangesAfter(applied, 1000)`, with a 50 ms idle backoff (Postgres can use `LISTEN/NOTIFY` as a wake-up hint). It applies the changes in order and publishes new generations. Writes made by this replica are applied the same way; there is no separate in-memory write path.
- **Snapshots.** Taken every N changes (default 100k) or T minutes (default 15), and at shutdown. A snapshot serializes the index structures (gob plus roaring's native format) with the `seq` it covers. It is written to a local directory, or to `sl_snapshots` when it is enabled, so a fresh replica can start fast.
- **Recovery.** Load the newest valid snapshot, falling back to `LoadIndex` if there is none, then replay `seq > snapshot.seq`.
- **Changelog pruning** stays behind the oldest snapshot `seq` that any live replica reports. Replicas report through `sl_replicas`, a heartbeat table, and entries older than the retention window are never needed.
- **Readiness.** A replica is ready once it is caught up within `max_lag` (default 2 s) and the store answers.

## 10. Failure handling

| Failure | Behaviour |
|---|---|
| Database unreachable | Writes return 503 `store_unavailable`. Reads keep serving from memory, with `stale: true` in responses and the lag metric rising. Readiness goes false after `max_lag`. |
| Change cannot be applied | Cannot happen for validated writes. If it does (corruption), the apply loop stops, readiness goes false, and the error is logged with the `seq`. It never skips. |
| Snapshot corrupt | Fall back to the next older snapshot, or to a full load. A metric and a log record it. |
| Memory pressure | Metrics for per-index bytes, documents and terms. The limits (`max_docs_per_index`, request caps) are configurable. |
| Request too large or slow | 413 for size, or 503 `deadline_exceeded` through the context. Every handler respects cancellation. |
| Shutdown | Stop accepting requests, drain in-flight ones (30 s), take a final snapshot, close the store. |

## 11. Observability

- **Logs:** `log/slog` JSON. Every request log has `trace_id`, `span_id`, `index`, `route`, `status` and `duration_ms`. The level is configurable, and debug adds plan and percolator detail.
- **Traces:** OpenTelemetry spans for HTTP (otelhttp), parse, plan, execute, percolate (anchor lookup, verify), the store calls (`database/sql` instrumentation), the apply loop batches and snapshots. The OTLP exporter is set from the standard `OTEL_*` environment variables.
- **Metrics** (OTel, exported via OTLP and Prometheus `/metrics`):
  - `searchlight_http_requests_total`, `searchlight_http_request_duration_seconds` (by route and status);
  - `searchlight_search_duration_seconds`, `searchlight_search_phase_seconds` (by phase);
  - `searchlight_percolate_documents_total`, `searchlight_percolate_candidates`, `searchlight_percolate_verifications`, `searchlight_percolate_always_check` (by index);
  - `searchlight_index_documents`, `searchlight_index_terms`, `searchlight_index_bytes`, `searchlight_index_queries`;
  - `searchlight_replica_applied_seq`, `searchlight_replica_lag_seconds`, `searchlight_replica_apply_duration_seconds`;
  - `searchlight_snapshot_age_seconds`, `searchlight_snapshot_duration_seconds`;
  - `searchlight_store_errors_total`.
- **Profiling:** `pprof` on a separate admin listener, enabled by a flag.

## 12. Configuration

Configuration comes from flags with environment equivalents (`SEARCHLIGHT_*`):

| Setting | Meaning |
|---|---|
| `listen` | API listener |
| `admin_listen` | metrics and pprof listener |
| `store_url` | `postgres://…`, `mysql://…`, `sqlite:///path` |
| `tokens_file` | API tokens |
| `snapshot_dir` | local snapshot directory |
| `snapshot_every_changes` | snapshot after N changes |
| `snapshot_every` | snapshot after a time interval |
| `max_lag` | readiness lag threshold |
| `poll_interval` | changelog poll interval |
| `request_timeout` | per-request deadline |
| `max_body_bytes` | request body cap |
| `log_level` | log level |

All of it is validated at start, and an invalid configuration exits with a clear message.

## 13. Testing

- **Unit tests** in every package. Analysis and query are table-driven.
- **Parity fixtures** (`testdata/parity/*.json`) are generated by a script in scrape-bot from its Python normalizers and matcher: inputs plus expected normalized forms and match results. Go must reproduce them exactly.
- **Property tests** (`testing/quick`, seeded):
  - Forward search equals a brute-force matcher over all documents.
  - Percolation: the candidates include every true match, and the results equal brute force.
  - Replica convergence: random interleavings of writes across two replicas end in identical generations.
- **Store conformance suite** runs on SQLite by default, and on Postgres and MySQL when `SEARCHLIGHT_TEST_PG_URL` / `SEARCHLIGHT_TEST_MYSQL_URL` are set (no Docker required). It covers the sequence ordering under concurrent writers, upserts, pruning and snapshots.
- **API tests** with `httptest` cover every endpoint, the problem JSON and `wait_for_seq`.
- **Benchmarks** (`go test -bench`):
  - percolate at 1k, 10k and 100k saved queries;
  - forward search at 130k and 1M documents;
  - apply throughput.
- **CI:** GitHub Actions running `go vet`, `golangci-lint` and `go test -race ./...` on SQLite, plus Postgres and MySQL service containers in CI.

## 14. Repository layout

```
go-searchlight/
  cmd/searchlight/            main
  internal/{config,analysis,query,index,search,percolate,replica,store,api,telemetry}/
  internal/store/{postgres,mysql,sqlite}/  dialects + embedded migrations
  api/openapi.yaml            HTTP contract
  testdata/parity/            fixtures from scrape-bot
  docs/                       this spec, API reference, operations
  deploy/                     Dockerfile (distroless), compose example
  .github/workflows/ci.yml
  .golangci.yml  Makefile  go.mod (go 1.25)
```

The layout follows the conventions of scrape-bot's `egress/gateway`: `cmd/` + `internal/`, OTel, and `slog`.

## 15. scrape-bot integration (separate scrape-bot epic, after engine v1)

- A Python client in scrape-bot. A setting `[search] engine = "sql" | "searchlight"` chooses the engine, with a `shadow` mode that runs both and logs any difference.
- The diff engine sends each batch's normalized items with `_bulk?percolate=true` and derives edges against its own `watch_search_matches`.
- Saved searches become engine queries, with the search id in `meta`. Listings and preview call `_search`, and the catalogue calls `_fields`.
- History searches stay in the bot's SQL path.
- The `link` condition becomes `has` on a client-filled `_links` keyword list.

## 16. Open items, decided for v1

| Item | Decision |
|---|---|
| History ops | Stay in scrape-bot |
| `similar` | Not anchored; it goes on the always-check list (v1) |
| Default sort | `_id` ascending |
| Go version | 1.25, the installed toolchain |
