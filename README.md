# Searchlight

Searchlight is a search engine written in Go. It runs forward search with aggregations and
percolation of saved queries. Its source of truth is a SQL database: Postgres, MySQL 8.0.19
or later (tested on 8.4 LTS, with `max_allowed_packet` of at least 64 MB, the default) or
SQLite (one node only: a cluster needs Postgres or MySQL; a second node refuses to join a
SQLite store). The design is in
[docs/superpowers/specs/2026-10-02-searchlight-design.md](docs/superpowers/specs/2026-10-02-searchlight-design.md)
and the build plan is in [docs/superpowers/plans/2026-10-02-searchlight.md](docs/superpowers/plans/2026-10-02-searchlight.md).

> **Status:** foundation only. The binary loads its configuration, sets up telemetry and
> serves the admin endpoints. The engine and the public API are still to come.

## Building

You need Go 1.25 or newer. The code is pure Go with no cgo, and it builds on Linux and on
Windows.

```sh
make build       # go build ./... and bin/searchlight (bin/searchlight.exe on Windows)
make test        # the short tier: go test -short ./..., under two minutes
make test-heavy  # every test: the short tier and the heavy one (go test ./...)
make test-all    # test-heavy with the property tests' long modes
make test-race   # test-heavy under -race, as CI runs it
make lint        # go vet ./... and golangci-lint run (golangci-lint v2)
make bench       # all benchmarks; narrow them with BENCH=<regexp>
make parity      # regenerate testdata/parity/*.json from scrape-bot (see below)
```

- **Two test tiers.** `make test` runs the short tier: every package, with the tests that
  wait on real time (cluster failover, rolling restarts, lease and latency bounds), the
  100,000-document recovery, the full byte-identity matrices and the end-to-end benchmark
  run left to the heavy tier, which runs only without `-short`. CI runs the short tier and
  the full suite as two jobs, both under `-race` on Linux with SQLite, Postgres and MySQL,
  and the short tier and the end-to-end run on Windows. [CONTRIBUTING.md](CONTRIBUTING.md)
  says which tier a new test belongs to.
- **`-race` needs cgo.** `TESTFLAGS` adds flags to every test target (`make test
  TESTFLAGS=-race`); without a C compiler (common on Windows), leave `-race` to CI.
- **Postgres and MySQL tests** run only when `SEARCHLIGHT_TEST_PG_URL` or
  `SEARCHLIGHT_TEST_MYSQL_URL` is set, for example:
  - `postgres://searchlight:searchlight@127.0.0.1:5432/searchlight?sslmode=disable`
  - `mysql://searchlight:searchlight@127.0.0.1:3306/searchlight`

  The SQLite tests always run and need no services.
- **`make parity`** runs `uv run --project $(SCRAPE_BOT) python tools/parity/gen.py`.
  `SCRAPE_BOT` defaults to `E:/code/scrape_bot`; point it at your scrape-bot checkout.

## Running

Only `store_url` is required:

```sh
SEARCHLIGHT_STORE_URL=sqlite:///var/lib/searchlight/searchlight.db ./bin/searchlight
# or
./bin/searchlight --store_url=postgres://searchlight:secret@db:5432/searchlight
```

`searchlight -h` lists every setting with its environment variable and default.

- **Setting names:** each setting has one snake_case name. It is the flag `--<name>` and
  the environment variable `SEARCHLIGHT_<NAME>`.
- **Precedence:** a flag wins over the environment, and the environment wins over the
  default.
- **Secrets:** `store_url` and `cluster_token` can also be read from a file named by
  `SEARCHLIGHT_<NAME>_FILE`.
- **Errors:** the binary checks the whole configuration at start and names every bad
  setting.

| Setting | Default | Meaning |
|---|---|---|
| `store_url` | (required) | SQL database URL: `postgres://`, `mysql://` or `sqlite://` |
| `listen` | `:8780` | public API address |
| `admin_listen` | `:8781` | `/healthz`, `/metrics` and pprof |
| `advertise_address` | from `listen` and the host name | the address peers use to reach this node |
| `node_id` | host name | the node's name in the registry, logs and telemetry |
| `data_dir` | `data` | local segments |
| `tokens_file` | (none) | API bearer tokens, one per line: `<token>` (read-write) or `<token> read` (read-only); required unless `insecure_no_auth` |
| `insecure_no_auth` | `false` | serve the API with no auth when `tokens_file` is empty (logged as a warning; never on a reachable node) |
| `cluster_token` | (none) | auth for the internal peer API |
| `peer_ca_file` | (system roots) | PEM bundle of the CAs that sign peers' TLS certificates (the peer API runs over TLS when `tls_cert` is set) |
| `lease_ttl` | `0` (`30s` on SQLite, `10s` elsewhere) | how long a shard copy's lease lasts unrenewed (renewed every 2 s) |
| `prune_stall_timeout` | `15m` | how long a copy behind the others may make no progress and still hold the changelog's prune floor |
| `retiring_retention` | `15m` | how long the changelog is kept for a cleanly stopped node's copies to replay on restart |
| `changelog_retention` | `24h` | the oldest a change may grow before it is pruned whatever copy still needs it |
| `refresh_interval` | `1s` | how often writes become searchable (a refresh is visibility only: it fsyncs nothing) |
| `flush_interval` | `10s` | how often a shard copy makes what refreshes published durable: fsyncs its new segments and deletes, writes its manifest, and only then reports that seq as applied (a crash replays at most this much of the changelog) |
| `max_lag` | `2s` | how far a copy may trail the changelog and still serve |
| `changelog_poll_interval` | `500ms` | how often a shard copy polls the changelog when no notification or local write wakes it |
| `remap_debounce` | `2s` | how long a copy that a mapping change must rebuild waits for more mapping changes (`0s` = rebuild at once) |
| `halt_retry_base`, `halt_retry_cap` | `30s`, `10m` | backoff between retries of a halted shard copy |
| `rebuild_retry_cap` | `2m` | longest backoff between retries of a copy rebuild that keeps failing |
| `merge_budget` | `64MiB` | bytes per second merges may write (`0` = unlimited) |
| `merge_threads` | GOMAXPROCS/4, at least 1 | concurrent background merges |
| `search_threads` | GOMAXPROCS | search worker pool size |
| `log_level` | `info` | `debug`, `info`, `warn` or `error` |
| `pprof` | `false` | serve `/debug/pprof/*` on the admin listener |
| `shutdown_timeout` | `30s` | how long a graceful shutdown may take |
| `max_body_bytes` | `16MiB` | largest API request body (at most 32MiB less 1KiB, below a segment's largest document) |
| `max_doc_bytes` | `4MiB` | largest document JSON, in a PUT or a `_bulk` line |
| `max_bulk_ops` | `10000` | most operations in one `_bulk` request |
| `request_timeout` | `30s` | deadline of every API request |
| `read_timeout` | `1m` | how long reading a request's headers and body may take |
| `search_queue` | `1000` | reads in progress at once before more get a 429 |
| `max_inflight_write_bytes` | `512MiB` | heap the writes in progress may take (body bytes times `inflight_amplification`) before more get a 429 |
| `max_inflight_read_bytes` | `256MiB` | the same for reads |
| `inflight_amplification` | `10` | heap a request takes per byte of body at its peak: measured about 4.5 for a bulk, 8 for a bulk with `percolate=true` or a percolation (`BenchmarkBulkPeakHeap`); a node's heap is about 1.5 times the two budgets, plus each shard copy's write buffers (64 MiB times 4 by default) |
| `max_index_fields` | `1000` | most fields one index's mapping holds (Elasticsearch's `index.mapping.total_fields.limit`) |
| `drop_timeout` | `10m` | how long dropping an index from the store may take |
| `shutdown_grace` | `2s` | how long the API keeps serving after readiness turns false at shutdown |
| `tls_cert`, `tls_key` | (none) | serve the API over TLS; without them, put a TLS-terminating proxy in front |

The binary stops gracefully on SIGINT or SIGTERM.

## Telemetry

- **Logs** are JSON on standard error. Every line carries `node_id`. Lines logged inside a
  span also carry `trace_id` and `span_id`.
- **Metrics** are OpenTelemetry instruments. The admin listener serves them at
  `GET /metrics` in the Prometheus format, along with the Go runtime and process metrics.
  The metric catalogue is in `internal/telemetry/metrics.go`.
- **OTLP export** of traces and metrics is off by default. The standard `OTEL_*`
  variables turn it on:
  - Set `OTEL_EXPORTER_OTLP_ENDPOINT`, or the per-signal `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`
    or `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`. Or set `OTEL_TRACES_EXPORTER=otlp` or
    `OTEL_METRICS_EXPORTER=otlp`.
  - `OTEL_EXPORTER_OTLP_PROTOCOL` picks `http/protobuf` (the default) or `grpc`.
  - `OTEL_SDK_DISABLED=true` turns all export off.
  - `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` override the resource.
