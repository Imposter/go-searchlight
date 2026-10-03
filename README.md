# Searchlight

Searchlight is a search engine written in Go. It runs forward search with aggregations and
percolation of saved queries. Its source of truth is a SQL database (Postgres, MySQL or
SQLite). The design is in
[docs/superpowers/specs/2026-10-02-searchlight-design.md](docs/superpowers/specs/2026-10-02-searchlight-design.md)
and the build plan is in [docs/superpowers/plans/2026-10-02-searchlight.md](docs/superpowers/plans/2026-10-02-searchlight.md).

> **Status:** foundation only. The binary loads its configuration, sets up telemetry and
> serves the admin endpoints. The engine and the public API are still to come.

## Building

You need Go 1.25 or newer. The code is pure Go with no cgo, and it builds on Linux and on
Windows.

```sh
make build    # go build ./... and bin/searchlight (bin/searchlight.exe on Windows)
make test     # go test -race ./...
make lint     # go vet ./... and golangci-lint run (golangci-lint v2)
make bench    # all benchmarks; narrow them with BENCH=<regexp>
make parity   # regenerate testdata/parity/*.json from scrape-bot (see below)
```

- **`-race` needs cgo.** If no C compiler is installed (common on Windows), run
  `make test RACE=` instead. CI runs `-race` on Linux.
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
| `tokens_file` | (none: API auth off) | API bearer tokens |
| `cluster_token` | (none) | auth for the internal peer API |
| `refresh_interval` | `1s` | how often writes become searchable |
| `seq_persist_interval` | `30s` | how often a shard persists a changelog position that moved without new segments |
| `max_lag` | `2s` | how far a copy may trail the changelog and still serve |
| `changelog_poll_interval` | `500ms` | how often a shard copy polls the changelog when no notification or local write wakes it |
| `merge_budget` | `64MiB` | bytes per second merges may write (`0` = unlimited) |
| `merge_threads` | GOMAXPROCS/4, at least 1 | concurrent background merges |
| `search_threads` | GOMAXPROCS | search worker pool size |
| `log_level` | `info` | `debug`, `info`, `warn` or `error` |
| `pprof` | `false` | serve `/debug/pprof/*` on the admin listener |
| `shutdown_timeout` | `30s` | how long a graceful shutdown may take |

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
