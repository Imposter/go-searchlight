# Searchlight

Searchlight is a search engine written in Go. It runs forward search with aggregations and
percolation of saved queries. Its source of truth is a SQL database: Postgres, MySQL 8.0.19
or later (tested on 8.4 LTS, with `max_allowed_packet` of at least 64 MB, the default) or
SQLite (one node only: a cluster needs Postgres or MySQL; a second node refuses to join a
SQLite store). The design is in
[docs/superpowers/specs/2026-10-02-searchlight-design.md](docs/superpowers/specs/2026-10-02-searchlight-design.md)
and the build plan is in [docs/superpowers/plans/2026-10-02-searchlight.md](docs/superpowers/plans/2026-10-02-searchlight.md).

> **Status:** the engine, the HTTP API and the cluster are built, and the binary runs
> them. The benchmark harness against Elasticsearch (`bench/`) and the chaos suite
> (`test/chaos`) are in place; the tuning to the spec's targets is still to come.
> [docs/architecture.md](docs/architecture.md#planned-not-built) lists what is planned but
> not built.

## Documentation

- [docs/api.md](docs/api.md): the HTTP API, with examples (the contract is
  [api/openapi.yaml](api/openapi.yaml)).
- [docs/operations.md](docs/operations.md): deploying (binary, Docker, Compose,
  Kubernetes), every setting, sizing, backups, upgrades, monitoring and troubleshooting.
- [docs/architecture.md](docs/architecture.md): the components and how data flows
  through them.

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
make docker      # the container image (deploy/Dockerfile), searchlight:dev
make parity      # regenerate testdata/parity/*.json from scrape-bot (see below)
```

- **Two test tiers.** `make test` runs the short tier: every package, with the tests that
  wait on real time (cluster failover, rolling restarts, lease and latency bounds), the
  100,000-document recovery, the full byte-identity matrices, the binary's smoke test and
  the end-to-end benchmark run left to the heavy tier, which runs only without `-short`. CI
  runs the short tier and the full suite as two jobs, both under `-race` on Linux with
  SQLite, Postgres and MySQL, and on Windows the short tier, the two smoke tests and the
  heavy tier of `store`, `shard` and `segment`. [CONTRIBUTING.md](CONTRIBUTING.md)
  says which tier a new test belongs to.
- **`-race` needs cgo.** `TESTFLAGS` adds flags to every test target (`make test
  TESTFLAGS=-race`); without a C compiler (common on Windows), leave `-race` to CI.
- **Postgres and MySQL tests** run only when `SEARCHLIGHT_TEST_PG_URL` or
  `SEARCHLIGHT_TEST_MYSQL_URL` is set, for example:
  - `postgres://searchlight:searchlight@127.0.0.1:5432/searchlight?sslmode=disable`
  - `mysql://searchlight:searchlight@127.0.0.1:3306/searchlight`

  The SQLite tests always run and need no services.
- **The chaos suite** (`test/chaos`, behind the `chaos` build tag) runs the binary as a
  three-node cluster on Postgres and kills, restarts and corrupts it under load:
  `SEARCHLIGHT_TEST_PG_URL=... go test -tags chaos ./test/chaos`. Its database restart
  scenario also needs `SEARCHLIGHT_CHAOS_DB_RESTART`, a shell command that restarts that
  Postgres. CI runs it in `.github/workflows/chaos.yml`.
- **`make parity`** runs `uv run --project $(SCRAPE_BOT) python tools/parity/gen.py`.
  `SCRAPE_BOT` defaults to `E:/code/scrape_bot`; point it at your scrape-bot checkout.

## Running

`store_url` is required, and so is `tokens_file` unless `insecure_no_auth` is set:

```sh
printf '%s write\n' "$(openssl rand -hex 24)" > tokens
SEARCHLIGHT_STORE_URL=sqlite:///var/lib/searchlight/searchlight.db SEARCHLIGHT_TOKENS_FILE=tokens ./bin/searchlight
# or, on a laptop only
./bin/searchlight --store_url=postgres://searchlight:secret@db:5432/searchlight --insecure_no_auth
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

The settings, with their defaults, are listed in
[docs/operations.md](docs/operations.md#every-setting). The binary stops gracefully on
SIGINT or SIGTERM: readiness turns false, the listener drains, and the node writes its
shards' final manifests and leaves the cluster.

To run a three-node cluster on Postgres:

```sh
docker compose -f deploy/compose.yml up --build -d
```

`deploy/k8s/` has Kubernetes manifests (a StatefulSet of three). Both are described in
[docs/operations.md](docs/operations.md#deploy).

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
