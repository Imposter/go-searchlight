# Searchlight

Searchlight is a search engine written in Go. It runs forward search with aggregations and
percolation of saved queries. Its source of truth is a SQL database: Postgres, MySQL 8.0.19
or later (tested on 8.4 LTS, with `max_allowed_packet` of at least 64 MB, the default) or
SQLite (one node only: a cluster needs Postgres or MySQL; a second node refuses to join a
SQLite store). The design is in
[docs/superpowers/specs/2026-10-02-searchlight-design.md](docs/superpowers/specs/2026-10-02-searchlight-design.md)
and the build plan is in [docs/superpowers/plans/2026-10-02-searchlight.md](docs/superpowers/plans/2026-10-02-searchlight.md).

> **Status:** v1 is built and tuned. Every graded target in the spec's §1 passes against
> Elasticsearch 8 on the same host, 1M documents
> ([docs/benchmarks.md](docs/benchmarks.md)).
> [docs/architecture.md](docs/architecture.md#known-gaps) lists what is known to be missing.

| Target | Searchlight | Elasticsearch 8 |
| --- | --- | --- |
| Bulk indexing, acknowledged = durable (T1) | 13.5k docs/s | 7.3k docs/s |
| Filter, sort and aggregation latency (T2–T4) | p50 0.5–2.9 ms, p99 1.1–15 ms | p50 1.9–140 ms, p99 4.7 ms–2.2 s |
| Percolation, 100k saved queries (T5) | 3,187 docs/s, server p99 783 µs per doc | 12.6 docs/s |
| Resident memory per million docs (T6) | 2.42 GiB | 4.15 GiB |
| Write to visible, p99 (T7) | 999 ms | 998 ms |
| New replica to serving (T8) | 6.0 s | 28.5 s |
| Node restart to serving (T9) | 1.1 s | — |

Disk per million documents is 2.33 GiB against Elasticsearch's 1.5 GiB: 3-grams on every
keyword and text field, a deliberate trade-off for substring search. The bench runs on
SQLite; write throughput on Postgres and MySQL is not measured yet
([#69](https://github.com/Imposter/go-searchlight/issues/69)).

## What it is, and what it is not

- **The database is the write path.** A write is acknowledged once its SQL transaction
  commits; that transaction is the write-ahead log, so there is no queue or second copy to
  keep in sync. Every node also tails that changelog, so write throughput is bounded by
  the database: scale the database for writes.
- **Search scales with nodes.** Matching, sorting, aggregations and percolation run in the
  nodes, on memory-mapped segments on local disk; the database does no text work. Add
  nodes for read and percolation load; each node opens up to 64 pooled database
  connections (plus one for `LISTEN` on Postgres).
- **Near-real-time, not instant.** A write is searchable within the refresh interval (1 s
  by default). `refresh=wait_for` or `wait_for_seq` gives read-your-writes.
- **Boolean matching, no relevance scoring.** Queries match or do not; results sort by
  field. Text matching folds case and Unicode (NFKC), and offers words and phrases,
  substrings (`contains`, backed by 3-grams), prefixes and trigram similarity (`similar`)
  for typo-tolerant lookups. BM25 ranking, stemming, synonyms and phonetic matching are
  not in v1.
- **SQLite is one node.** A cluster needs Postgres or MySQL, and there is no
  SQLite-to-Postgres migration tool yet: moving means re-indexing from the source
  ([#95](https://github.com/Imposter/go-searchlight/issues/95)).
- **One pure-Go binary.** No cgo, no JVM; it builds and runs on Linux, macOS and Windows.
  Memory is bounded by request and buffer budgets ([Sizing](docs/operations.md#sizing)).

## Install

Each [release](https://github.com/Imposter/go-searchlight/releases) publishes binaries, a
container image and deployment samples. [CHANGELOG.md](CHANGELOG.md) has its notes.

- **Binaries** for linux/amd64, linux/arm64, darwin/amd64, darwin/arm64 and
  windows/amd64, each archived with this README,
  [docs/operations.md](docs/operations.md), the licence and the third-party notices, and
  checked by `SHA256SUMS`:

  ```sh
  base=https://github.com/Imposter/go-searchlight/releases/download/v1.6.0
  curl -fsSLO "$base/searchlight-1.6.0-linux-amd64.tar.gz" -O "$base/SHA256SUMS"
  sha256sum --ignore-missing -c SHA256SUMS
  tar -xzf searchlight-1.6.0-linux-amd64.tar.gz
  ./searchlight-1.6.0-linux-amd64/searchlight version
  ```

  The macOS binaries are not signed: one downloaded with a browser needs
  `xattr -d com.apple.quarantine searchlight` (or **Open Anyway** in System Settings ›
  Privacy & Security) before it
  runs ([docs/operations.md](docs/operations.md#the-binary)).
  `go install github.com/Imposter/go-searchlight/cmd/searchlight@v1.6.0` builds it too.

- **The image**, for linux/amd64 and linux/arm64, tagged `1.6.0`, `1.6`, `1` and
  `latest`:

  ```sh
  docker pull ghcr.io/imposter/searchlight:1.6.0
  ```

- **Docker Compose samples** in `searchlight-deploy-1.6.0.tar.gz`, which needs no
  checkout: a three-node cluster on Postgres 17 behind nginx (`compose.postgres.yml`), a
  single node on SQLite (`compose.sqlite.yml`), their `.env.example`, and the Kubernetes
  manifests (`k8s/`):

  ```sh
  curl -fsSL https://github.com/Imposter/go-searchlight/releases/download/v1.6.0/searchlight-deploy-1.6.0.tar.gz | tar -xz
  cd searchlight-deploy-1.6.0
  docker compose -f compose.postgres.yml up -d                              # nginx on 127.0.0.1:8080
  docker compose -f compose.postgres.yml run --rm secrets cat /secrets/tokens  # the API tokens
  ```

  The same files are in [deploy/](deploy). Upgrade by setting `SEARCHLIGHT_VERSION` in
  `.env` and running `up -d` again ([docs/operations.md](docs/operations.md#docker-compose)).

## Documentation

- [docs/api.md](docs/api.md): the HTTP API, with examples (the contract is
  [api/openapi.yaml](api/openapi.yaml)).
- [docs/operations.md](docs/operations.md): deploying (binary, Docker, Compose,
  Kubernetes), every setting, sizing, backups, upgrades, monitoring and troubleshooting.
- [docs/architecture.md](docs/architecture.md): the components and how data flows
  through them.
- [docs/clustering.md](docs/clustering.md): how clustering on Postgres (or MySQL)
  works — joining, writes, reads, shard allocation, recovery and failure handling.
- [docs/nginx.md](docs/nginx.md): putting nginx in front of the cluster as the load
  balancer for queries and writes, with a working config at
  [deploy/nginx/searchlight.conf](deploy/nginx/searchlight.conf).
- [CHANGELOG.md](CHANGELOG.md): what each release changed, with its upgrade notes;
  [RELEASING.md](RELEASING.md): how to cut a release.

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

`store_url` is required; so is `tokens_file`, and on Postgres or MySQL `cluster_token`, unless
`insecure_no_auth` is set:

```sh
printf '%s write\n' "$(openssl rand -hex 24)" > tokens
SEARCHLIGHT_STORE_URL=sqlite:///var/lib/searchlight/searchlight.db SEARCHLIGHT_TOKENS_FILE=tokens ./bin/searchlight
# or, on a laptop only
./bin/searchlight --store_url=postgres://searchlight:secret@db:5432/searchlight --insecure_no_auth
```

`searchlight -h` lists every setting with its environment variable and default, and
`searchlight version` prints the version.

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

To run a three-node cluster on Postgres built from this checkout:

```sh
docker compose -f deploy/compose.yml up --build -d
```

`deploy/compose.postgres.yml` and `deploy/compose.sqlite.yml` run the published image
instead ([Install](#install)), and `deploy/k8s/` has Kubernetes manifests (a StatefulSet
of three). All are described in [docs/operations.md](docs/operations.md#deploy).

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

## License

Searchlight is released under the [MIT License](LICENSE).
