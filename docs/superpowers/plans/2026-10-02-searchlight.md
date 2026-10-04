# Searchlight Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development. Each task is a self-contained, independently testable module with the interface contract below. Build the full feature set; do not stub features for "later".

**Goal:** Build Searchlight as specified in `docs/superpowers/specs/2026-10-02-searchlight-design.md`, the full v1 feature set: a single-node engine, the cluster, and hardening plus a benchmark against Elasticsearch.

**Architecture:**
- Lucene-style immutable, memory-mapped segments per shard, with a refreshable write buffer and tiered merges.
- Forward search and aggregations, plus a reverse-indexed percolator.
- The SQL database (Postgres, MySQL or SQLite) is the write-ahead log and the system of record, and every shard copy tails its changelog.
- Plug-and-play cluster: registry and shard leases in SQL, peer recovery, routing with retries.

**Tech stack:**
- Go 1.25
- `github.com/RoaringBitmap/roaring/v2`
- `golang.org/x/text`
- `github.com/klauspost/compress/zstd`
- `golang.org/x/exp/mmap`, or `syscall` mmap behind a small interface (works on Windows and Linux)
- `jackc/pgx/v5/stdlib`, `go-sql-driver/mysql`, `modernc.org/sqlite`
- OpenTelemetry (`otel`, `otelhttp`, the Prometheus exporter, the OTLP exporters)
- `log/slog`
- `net/http` and its routing

**Spec:** `docs/superpowers/specs/2026-10-02-searchlight-design.md`. Every task reads it.

## Global Constraints

- **Module and toolchain:** module `github.com/Imposter/go-searchlight`, `go 1.25`. Pure Go with no cgo; it must build on Windows and Linux.
- **Layout:** `cmd/searchlight`, `internal/<pkg>`. Each package has one responsibility. Packages depend only along the arrows in the task contracts below, with no cycles.
- **Parity:** normalization and matching are byte-for-byte compatible with scrape-bot, as pinned by `testdata/parity/*.json`.
- **Durability:** a write is acknowledged only after its SQL transaction commits. Sequence numbers are contiguous and commit in order.
- **Readers:** they never take locks. Generations are published through `atomic.Pointer`, and segments are immutable.
- **Logging and telemetry:** every exported long-running operation takes a `context.Context` and honours cancellation. Logs go through `slog` (JSON) with trace ids. Every public operation has an OTel span and the metrics named in spec §11.
- **Tests:**
  - `go test -race ./...` is green on SQLite with no external services.
  - Postgres and MySQL tests run when `SEARCHLIGHT_TEST_PG_URL` / `SEARCHLIGHT_TEST_MYSQL_URL` are set.
  - Tests bind 127.0.0.1 with ephemeral ports.
- **Lint:** `go vet` and `golangci-lint` are clean. Add `.golangci.yml` in Task 1.
- **Commits:** conventional subjects, by explicit path. End every commit with:
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`
  `Claude-Session: https://claude.ai/code/session_01H1UyQR6szrER8nN86UGgmS`
- **No pushing.** The repo stays local until the operator says otherwise.

## Review Focus

These are the inputs most likely to break the engine for a real user. Each is pinned by a test in the owning task.

1. **Non-ASCII and edge text:** `ß`, `İ`, ligatures, NUL, empty strings, 500-character values, and `", "`-joined lists with blank entries. Analysis and segments must round-trip them exactly (Tasks 2, 4).
2. **Deletes and updates across segments and merges:** a document updated many times, deleted, then re-added must appear exactly once, with the latest body, in search, aggregations and percolation (Tasks 5, 6, 7).
3. **Concurrent writers:** many concurrent `_bulk` requests on several nodes must produce contiguous, in-order sequence numbers, with no lost or duplicated change on any copy (Tasks 8, 9, 11).
4. **Node loss mid-operation:** killing a node mid-bulk, mid-merge or mid-recovery loses no acknowledged write, and the copies converge (Tasks 11, 13).
5. **Oversized and hostile requests:** huge bodies, deep trees, 10,001 bulk lines and slow clients get a 413, 422 or 429 rather than a crash or out-of-memory (Task 10).

---

## Task 1: Repository foundation (config, telemetry, CI)

**Files:**
- `go.mod`, `Makefile` (build, test, lint, bench, parity)
- `.golangci.yml`, `.github/workflows/ci.yml` (vet, lint, `test -race` on SQLite, with Postgres and MySQL service containers)
- `internal/config/config.go` + tests
- `internal/telemetry/{logging,tracing,metrics,admin}.go` + tests
- `cmd/searchlight/main.go`: a skeleton that loads config, sets up telemetry and serves `/healthz` and `/metrics` on the admin listener
- `README.md`: a stub with build and run instructions

**Produces:**
- `config.Config` holds every setting in spec §12, plus `config.Load(args []string, env func(string) string) (Config, error)`. Validation errors name the setting.
- `telemetry.Setup(ctx, cfg) (*telemetry.T, error)`, where `T` has `.Logger *slog.Logger`, `.Tracer trace.Tracer`, `.Meter metric.Meter`, `.AdminHandler http.Handler` (`/metrics`, `/debug/pprof/*` when enabled) and `.Shutdown(ctx) error`.
- `telemetry.WithRequest(ctx) *slog.Logger` returns a logger with trace and span ids.

**Tests:**
- config defaults, environment overrides and invalid values;
- the logger emits JSON with trace ids inside a span;
- `/metrics` serves the Prometheus format.

**Done when:** `make build test lint` passes.

## Task 2: Schema and analysis (normalizers, mappings, analyzed documents, parity fixtures)

**Files:**
- `internal/analysis/{normalize,words,entries,trigram,number,similarity}.go` + tests
- `internal/schema/{mapping,analyze}.go` + tests
- `tools/parity/gen.py`: run with `uv run --project E:/code/scrape_bot python tools/parity/gen.py`. It imports scrape-bot's `conditions`, `search_index` and `search_oracle` and writes `testdata/parity/{normalize,words,entries,numbers,similarity,match}.json`.
- `testdata/parity/*.json`, committed.

**Produces:**
- **`analysis`:**
  - `Clean(s string) string`: NUL becomes U+FFFD.
  - `Normalize(s string) string`: whitespace fold plus full casefold, using `x/text/cases.Fold` with a check against the parity fixtures.
  - `Words(s string) string`: NFKC, then `\w` runs, padded `" a b "`.
  - `Entries(s string) []string`: split on `", "`, strip, drop blanks.
  - `Number(v any) (float64, bool)`: finite only, never bool, and false for ints that overflow a float.
  - `Trigrams(s string) []string`: the pg_trgm-compatible set used for similarity.
  - `Substrings3(s string) []string`: every 3-rune substring of the normalized text, used for `contains` and `starts_with` anchors and prefilters.
  - `Similarity(a, b string) float64`: pg_trgm semantics.
- **`schema`:**
  - `type FieldType uint8`, with the constants `Keyword`, `Text`, `KeywordList`, `Number`, `Bool`, `Date`.
  - `type Mapping struct{ Fields map[string]FieldType; Dynamic DynamicMode }`, plus `Merge(update)` (additive only, otherwise an error).
  - `type Value struct{ Present bool; Text *string; Words string; Number *float64; Bool *bool; Entries []string; Grams []string }`.
  - `type Doc struct{ ID string; Fields map[string]Value; Body []byte }`. It also holds the pseudo-field `_id`.
  - `Analyze(m *Mapping, id string, body []byte) (Doc, MappingUpdate, error)`. Dynamic typing and strict refusal follow spec §3. A value that doesn't fit its field's type is kept with `Present=true` and the typed parts nil.

**Tests:**
- every parity fixture is reproduced exactly;
- property tests: `Normalize` is idempotent, and `Entries(Normalize(x))` is stable;
- the Review Focus 1 inputs.

## Task 3: Query language and the exact matcher

**Files:**
- `internal/query/{ast,parse,validate,match,walk,problems}.go` + tests

**Consumes:** `schema.Doc`, `schema.Mapping`, `analysis.*`.

**Produces:**
- **Types:** `type Node interface{ isNode() }`, with `All{Children []Node}`, `Any{Children []Node}`, `Not{Child Node}`, and `Leaf{Field, Op string, Value json.RawMessage, parsed values}`.
- **`Parse(raw []byte) (Node, []Problem)`:** strict JSON shapes, bounds from spec §4, `loc` paths. `Problem{Loc, Message string}`.
- **`Validate(n Node, m *schema.Mapping) []Problem`:** ops per field type, value shapes, NUL refused.
- **`Match(n Node, d *schema.Doc) bool`:** the exact semantics, including missing fields.
- **`Walk(n Node, fn func(path string, n Node) bool)`**, and **`Canonical(n Node) []byte`** for dedup and cache keys.

**Tests:**
- the parity match fixtures from Task 2;
- table tests for every op and every missing-field rule;
- bounds and problem locs.

## Task 4: Segment format (write, mmap read, merge)

**Files:**
- `internal/segment/{format,writer,reader,termdict,postings,docvalues,points,stored,livedocs,merge,mmap_unix,mmap_windows,checksum}.go` + tests + benchmarks

**Consumes:** `schema.Doc`, `schema.FieldType`.

**Produces:**
- **`Build(dir string, docs []schema.Doc, opts BuildOptions) (Meta, error)`:** writes one immutable segment file plus a manifest entry, with a format version and a checksum.
- **`Open(path string) (*Reader, error)`**, with these methods:
  - `NumDocs() uint32`
  - `Postings(field string, kind TermKind, term string) *roaring.Bitmap`
  - `TermFreq(field, kind, term) uint32`
  - `Terms(field, kind, prefix string, fn)`
  - `Numbers(field) NumericColumn`, which has `Value(ord) (float64, bool)`, `Range(lo, hi, incLo, incHi) *roaring.Bitmap` (points) and `Stats()`
  - `Keywords(field) KeywordColumn`, for sort and terms aggregations (ordinals plus a dictionary)
  - `Entries(field) MultiColumn`
  - `Present(field) *roaring.Bitmap`
  - `Stored(ord) ([]byte, error)`
  - `ID(ord) string`, `Ord(id) (uint32, bool)`
  - `Close()`
- **`TermKind`:** `KindValue`, `KindEntry`, `KindWord`, `KindGram`.
- **`LiveDocs`:** a deletes sidecar per generation, `WriteDeletes(segment, gen, *roaring.Bitmap)`, `LoadDeletes`.
- **`Merge(dir string, inputs []*Reader, deletes []*roaring.Bitmap) (Meta, error)`:** live documents only.

**Tests:**
- round trip of every field type and kind;
- the Review Focus 1 inputs;
- a corrupted file is detected by its checksum;
- merge equals a rebuild from the live documents;
- benchmarks: build 100k docs/s, and postings lookup.

## Task 5: Shard (write buffer, refresh, generations, deletes, merges, filter cache, manifest)

**Files:**
- `internal/shard/{shard,buffer,generation,refresh,merge_policy,filter_cache,manifest,percolator_segments}.go` + tests

**Consumes:** `segment.*`, `schema.*`, `query.Node`.

**Produces:**
- **`Open(ctx, dir string, m *schema.Mapping, opts Options) (*Shard, error)`:** reopens the segments from the manifest.
- **Writes:**
  - `Apply(changes []Change)`, with `Change{Seq int64; Kind; DocID string; Doc *schema.Doc; QueryID string; Query query.Node; Meta []byte}`.
  - The upsert and delete semantics hold across the buffer and all segments.
- **Refresh:** `Refresh(ctx) error`, plus a background refresh every `refresh_interval`. `WaitRefreshed(ctx, seq)`.
- **Reads:**
  - `Acquire() *Generation` and `(*Generation).Release()`.
  - A `Generation` exposes its `Segments []SegmentView` (reader, deletes, base ordinal) plus `QuerySegments`, `Seq()` and `MaxSeq()`.
- **Durability:** `CommittedSeq() int64`, which is the manifest `seq` that is durable on disk.
- **Merges:** a tiered policy in the background under a budget, with an atomic manifest swap.
- **Filter cache:** `FilterCache.Get(segmentID, key)`.

**Tests:**
- the Review Focus 2 cases;
- concurrent readers during refresh and merge (`-race`);
- reopen after a simulated crash: segments plus `CommittedSeq` match.

## Task 6: Search (planner, execution, sort, paging, aggregations, reduce)

**Files:**
- `internal/search/{request,plan,exec,residual,sort,aggs,reduce}.go` + tests + benchmarks

**Consumes:** `shard.Generation`, `query.*`, `segment.*`.

**Produces:**
- **`Request`:** `Query query.Node; Sort []SortField; Size int; SearchAfter []any; TrackTotal int; Aggs map[string]Agg; Fields []string; Timeout`.
- **`ExecuteShard(ctx, g *shard.Generation, r *Request) (*ShardResult, error)`.** It plans with cost-based leaf ordering, uses bitmaps where it can and residual filters otherwise, runs segments in parallel, keeps top-k with early termination, and computes partial aggregations.
- **`Reduce(rs []*ShardResult, r *Request) *Response`**, where `Response` holds `Hits []Hit{ID, Sort []any, Body}`, `Total`, `TotalRelation`, `Aggs`, `Next []any` and `TimedOut`.
- **Aggregations:** `terms`, `range`, `histogram`, `date_histogram`, `stats` and `cardinality` (HLL++), with one level of metric sub-aggregations.

**Tests:**
- a property test where search equals brute force with `query.Match`, over random documents, queries, updates, deletes and merges;
- sort, `search_after` and totals;
- every aggregation compared with brute force;
- benchmarks at 1M documents.

## Task 7: Percolator

**Files:**
- `internal/percolate/{anchors,index,atoms,percolate}.go` + tests + benchmarks

**Consumes:** `query.*`, `schema.Doc`, `shard.Generation.QuerySegments`, and segment term statistics.

**Produces:**
- **`Extract(n query.Node, stats TermStats) Anchors`:** follows the rules in spec §7.
- **Query segments:** `BuildQuerySegment(dir, queries []StoredQuery, stats) (segment.Meta, error)`. The anchor dictionary maps to postings of query ordinals, plus interval trees, compiled ASTs and the always-check list. Shard refresh and merge call it through the `shard.Options` hook.
- **`Percolate(ctx, g *shard.Generation, docs []schema.Doc) ([][]string, error)`:** returns the matching query ids per document, processing documents in parallel.

**Tests:**
- the completeness property: across 100k random query and document pairs, the candidate set always contains every true match, and the results equal brute force;
- benchmarks at 1k, 10k and 100k queries; the per-document p99 at 100k must be under 1 ms.

## Task 8: SQL store (interface, dialects, migrations, group commit, registry)

**Files:**
- `internal/store/{store,types,groupcommit,conformance_test}.go`
- `internal/store/{sqlite,postgres,mysql}/{store.go,migrations/*.sql}` + tests

**Produces:**
- **The `Store` interface:** exactly as in spec §8, plus `Registry() RegistryStore` and `Blobs() BlobStore`.
- **Types:**
  - `Change{Seq, Index, Shard, Kind, ID, Payload []byte, At}`, and `Record`.
  - `RegistryStore` with `Heartbeat`, `Nodes`, `ClaimCopy`, `RenewLeases`, `Copies`, `SetCopyState` and `ReportApplied`.
- **`Open(ctx, url string) (Store, error)`:** selects the dialect by URL scheme.
- **`GroupCommitter`:** coalesces concurrent `Apply` calls on a node into one transaction, flushed every 2 ms or at 1,000 changes.

**Tests:**
- the conformance suite on all dialects: contiguous, in-order sequence numbers under 32 concurrent writers; `ChangesAfter` never skips a late commit; registry lease semantics; pruning;
- Review Focus 3.

## Task 9: Replica (changelog tailer and apply loop per shard copy)

**Files:**
- `internal/replica/{tailer,apply,recover}.go` + tests

**Consumes:** `store.Store`, `shard.Shard`, `schema.Analyze`, `query.Parse`.

**Produces:**
- **`NewTailer(st store.Store, sh *shard.Shard, id ShardID, opts) *Tailer`**, with `Run(ctx) error`, `Applied() int64` and `Wake()`, which handles the push hint and `LISTEN/NOTIFY`.
- **`Recover(ctx, sh, st, id)`:** resumes from `sh.CommittedSeq()`, or scans the shard via `ScanShard` when the shard is empty.

**Tests:**
- two shard copies tailing one store converge under random interleavings and restarts;
- an apply error halts the copy and marks it for recovery, and never skips a change;
- lag metrics.

## Task 10: HTTP API (all endpoints, auth, limits, problems, consistency)

**Files:**
- `internal/api/{server,indexes,docs,bulk,search,queries,percolate,fields,health,auth,limits,problems}.go` + tests
- `api/openapi.yaml`

**Consumes:** a `Coordinator` interface: `Write`, `Search`, `Percolate`, `Fields`, `Health` and similar. In Task 10 it is implemented by a single-node coordinator over the local shards, the store and the tailers. Task 11 swaps in the cluster coordinator.

**Produces:**
- **`NewServer(c Coordinator, t *telemetry.T, cfg config.Config) http.Handler`**, serving every endpoint in spec §5, including `_bulk?percolate=true`, `refresh=wait_for`, `wait_for_seq`, `if_seq`, 429 backpressure, request deadlines and `timed_out`.
- **`internal/node/single.go`:** the single-node coordinator.

**Tests:**
- `httptest` coverage of every endpoint;
- Review Focus 5;
- read-your-writes;
- `openapi.yaml` validated against the routes.

## Task 11: Cluster (membership, leases, allocation, peer recovery, routing, sharding, rolling restarts)

**Files:**
- `internal/cluster/{membership,allocator,recovery,peer_api,router,ars,coordinator,health}.go` + tests

**Consumes:** `store.RegistryStore`, `shard`, `replica`, `api.Coordinator`.

**Produces:**
- **`cluster.Node`:** `Start(ctx)` and `Stop(ctx)`. It registers, sends heartbeats, runs the allocator, streams peer recovery (checksummed and resumable), serves the internal peer API (`cluster_token` auth), and implements `api.Coordinator`:
  - routing, scatter-gather and reduce;
  - adaptive replica selection and retries;
  - push hints;
  - `/_cluster/*`;
  - the rolling `retiring` shutdown.

**Tests:**
- an in-process 3-node cluster over localhost: join, allocation to target, node kill and reallocation, peer recovery of a 100k-document shard, writes on every node converging, reads retried around a dead node with no client error, a rolling restart, and cluster health colours;
- Review Focus 3 and 4.

## Task 12: Binary, packaging, docs

**Files:**
- `cmd/searchlight/main.go`: full wiring and graceful shutdown.
- `deploy/Dockerfile` (distroless), `deploy/compose.yml` (3 nodes plus Postgres), `deploy/k8s/*.yaml`.
- `docs/{api.md,operations.md,architecture.md}`.

**Tests:**
- a smoke test that starts the binary on SQLite, then creates an index, bulk-loads, searches, percolates and shuts down cleanly with a final manifest.

## Task 13: Benchmark harness against Elasticsearch, plus the chaos suite

**Files:**
- `bench/{cmd/slbench,datasets,workloads,es,report}` and `bench/docker-compose.es.yml` (Elasticsearch 8.x)
- `.github/workflows/bench.yml`
- `test/chaos/*_test.go`, behind the `chaos` build tag

**Produces:**
- **`slbench`:** generates datasets (1M and 10M products, scrape-bot-shaped) and runs the spec §14 workloads against both Searchlight and Elasticsearch, translating the DSL to Elasticsearch queries and percolator documents.
- **Report:** throughput and p50/p99/p999, disk, RSS, recovery and restart times, written as markdown in `docs/benchmarks.md`.
- **Chaos suite:** kill a node mid-bulk, mid-merge and mid-recovery; restart the database; corrupt a segment; a rolling upgrade. The invariants from spec §14 must hold.

## Task 14: Tune to targets

Run `slbench` in CI, or on a Linux host with Docker, against Elasticsearch. Profile with pprof and fix the bottlenecks until every spec §1 target is met. Record the results in `docs/benchmarks.md`.

## Task 15: Timers (epic [#9](https://github.com/Imposter/go-searchlight/issues/9))

Added 2026-10-04 after the T7 write-to-visible diagnosis and the slow, flaky test runs in Tasks 9–11. Each part is its own GitHub issue, with full scope and done-when criteria there.

| Part | Issue | Depends on | Summary |
|---|---|---|---|
| 15a | [#10](https://github.com/Imposter/go-searchlight/issues/10) | — | `internal/clock` (Real and Fake). Route every engine timer through it: shard refresh, replica polling and backoff, cluster leases, heartbeats and maintenance, group-commit window, readiness and staleness. Convert the slow timing tests to the fake clock. Forbid direct `time.*` calls with a lint rule. |
| 15b | [#11](https://github.com/Imposter/go-searchlight/issues/11) | 15a | Fixed-grid refresh scheduling. Today the period is the interval plus the refresh time, because the timer re-arms only after each refresh ends. |
| 15c | [#12](https://github.com/Imposter/go-searchlight/issues/12) | — | Split refresh from flush: publish without fsync, and flush (fsync, manifest, `CommittedSeq`) on its own cadence. Promoted from Task 14. |
| 15d | [#13](https://github.com/Imposter/go-searchlight/issues/13) | 15a helps | Fast-by-default tests: `-short` tier under about 2 minutes, a heavy tier, SQLite `synchronous=OFF` in non-durability tests, separate CI jobs. |

**Target** (with 15b and 15c): write-to-visible p50 about 0.5s and p99 ≤ 1.07s at a 1s interval, quiet or under disk load. A refresh drops from 66–108ms to 6–10ms.

---

## Execution order

| Order | Tasks |
|---|---|
| 1 | Task 1 |
| 2 | Tasks 2 and 8, in parallel (independent) |
| 3 | Tasks 3 and 4, in parallel (both depend on 2) |
| 4 | Task 5 |
| 5 | Tasks 6 and 7, in parallel |
| 6 | Task 9 |
| 7 | Task 10 |
| 8 | Task 11 |
| 9 | Tasks 12 and 13; Task 15a and 15c may start in parallel once Task 11 merges (they touch the same timers) |
| 10 | Task 15b and 15d (after 15a) |
| 11 | Task 14 |

Parallel tasks run in separate git worktrees of this repository, and the controller merges them. Use `go mod tidy` on `go.mod`/`go.sum` conflicts.
