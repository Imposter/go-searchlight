# Operating Searchlight

How to deploy, configure, size, back up, upgrade, monitor and troubleshoot Searchlight.
For what the components do, see [architecture.md](architecture.md). For the API, see
[api.md](api.md).

- [Deploy](#deploy)
- [Configuration](#configuration)
- [Databases](#databases)
- [Sizing](#sizing)
- [Backups and disaster recovery](#backups-and-disaster-recovery)
- [Upgrades and rolling restarts](#upgrades-and-rolling-restarts)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)

## Deploy

A node is one static binary, `searchlight`. It needs a SQL database and a directory for
its segments, and nothing else: no coordination service, no shared disk. Every node is
the same; any node takes any request.

| Port | Listener | Serves |
|---|---|---|
| 8780 (`listen`) | public | the API, and the internal peer API under `/_internal/` (cluster_token auth) |
| 8781 (`admin_listen`) | admin | `GET /healthz`, `GET /readyz`, `GET /metrics` (no auth), `/debug/pprof/*` when `pprof` is on |

- **Admin listener.** It binds to loopback, `127.0.0.1:8781`, by default. The image and
  the Kubernetes ConfigMap set `:8781` explicitly, so probes and Prometheus can reach it.
  Wherever it is reachable, keep the port private: Prometheus and probes only. A node
  with `pprof=true` on a non-loopback `admin_listen` logs a warning at start.
- **Peers** reach each other on the public port, at `advertise_address`.

### The binary

```sh
make build                       # bin/searchlight, version from git describe
printf '%s write\n' "$(openssl rand -hex 24)" > tokens
SEARCHLIGHT_STORE_URL=sqlite:///var/lib/searchlight/searchlight.db \
SEARCHLIGHT_TOKENS_FILE=./tokens \
SEARCHLIGHT_DATA_DIR=/var/lib/searchlight/data \
  ./bin/searchlight
```

- **Start.** It logs `searchlight starting`, with its configuration and secrets redacted.
  - `/healthz` answers as soon as the admin listener is up.
  - Opening and migrating the store is retried with backoff (1 s, doubling, up to 30 s),
    and each failure is logged. What happens next depends on the error:
    - **Unreachable** (keeps retrying until it opens or the node is signalled): a network
      error (refused, timed out, DNS), a server not taking connections yet (Postgres
      class 08, 57P03 or 53300; MySQL 2002, 2003, 2013, 1040 or 1053; a broken
      connection), or a SQLite file another process holds locked (BUSY, LOCKED).
    - **Misconfigured** (exits 1 at once, without retrying): credentials refused
      (Postgres 28P01 or 28000; MySQL 1045 or 1044), a database that does not exist
      (Postgres 3D000; MySQL 1049), a SQLite file that cannot be opened or written
      (CANTOPEN, PERM, READONLY, NOTADB, or a missing directory), an invalid
      `store_url`, or a schema newer than the binary.
    - **Anything else** is retried for 2 minutes, then the node exits 1.

    Fix the cause and start the node again: `*_FILE` secrets are read only at start.
  - `/readyz` turns 200 once the node has joined the cluster and every shard copy has
    finished its startup recovery.
- **Stop.** SIGINT or SIGTERM stops it gracefully, within `shutdown_grace` plus
  `shutdown_timeout` (see [rolling restarts](#upgrades-and-rolling-restarts)). A signal
  during startup stops it cleanly too. A second signal kills it at once.
- **Exit codes.**
  - 0 after a clean stop.
  - 1, with one line on standard error, when it cannot start (a bad setting, a
    misconfigured store, a SQLite store another node uses) or when its shutdown failed.
- **Help.** `searchlight -h` lists every setting.
- **`searchlight healthcheck [--live]`** probes the node on this host, for container
  healthchecks. It GETs `/readyz` (`/healthz` with `--live`) on `SEARCHLIGHT_ADMIN_LISTEN`,
  where an empty or unspecified host means `127.0.0.1`. It waits at most 2 s, and exits 0
  on a 200, else 1.

### Docker

[`deploy/Dockerfile`](../deploy/Dockerfile) builds a static (`CGO_ENABLED=0`,
`-trimpath`) binary onto `gcr.io/distroless/static-debian12:nonroot`. The base images are
pinned by digest. Dependabot (`.github/dependabot.yml`) proposes updates for them, for
the compose images, for the Go modules and for the CI actions. The one exception is the
kubeconform image that CI runs inside a `run:` step: Dependabot cannot see it, so it is
bumped by hand, as the comment beside it says.

```sh
make docker                                  # searchlight:dev; IMAGE=... to rename
docker run -d --name searchlight -p 8780:8780 -p 127.0.0.1:8781:8781 \
  -v searchlight-data:/var/lib/searchlight \
  -v "$PWD/secrets:/run/secrets/searchlight:ro" \
  -e SEARCHLIGHT_STORE_URL_FILE=/run/secrets/searchlight/store_url \
  -e SEARCHLIGHT_TOKENS_FILE=/run/secrets/searchlight/tokens \
  searchlight:dev
```

- **User.** It runs as uid/gid 65532.
- **Defaults.** `SEARCHLIGHT_LISTEN=:8780`, `SEARCHLIGHT_ADMIN_LISTEN=:8781` and
  `SEARCHLIGHT_DATA_DIR=/var/lib/searchlight`, which is a volume.
- **Labels.** It carries the OCI labels (version, revision, created, source).
- **`HEALTHCHECK`.** It runs `searchlight healthcheck` (readiness) every 5 s, with a
  2-minute start period. The image has no shell, so the binary probes itself.

### Docker Compose: a three-node cluster

[`deploy/compose.yml`](../deploy/compose.yml) runs three nodes on Postgres 17:

```sh
docker compose -f deploy/compose.yml up --build -d
docker compose -f deploy/compose.yml run --rm secrets cat /secrets/tokens    # the API tokens
curl -H "Authorization: Bearer <write token>" localhost:8780/_cluster/health
```

- **Secrets.** A one-shot `secrets` service generates them on the first start, into a
  volume: the Postgres password, the `store_url` built from it, the `cluster_token`, and
  a tokens file with one read-write and one read-only token. The nodes read them through
  `SEARCHLIGHT_*_FILE` and `tokens_file`. The compose file holds no secret.
- **Ports.** The nodes serve the API on `127.0.0.1:8780`, `:8782` and `:8784`, and
  their admin listeners on `:8781`, `:8783` and `:8785`.
- **Addresses.** Each node's `advertise_address` is its service name.
- **Health.** Postgres is checked with `pg_isready`, and each node with
  `searchlight healthcheck`.
- **Stopping.** `stop_grace_period` is 75 s: `shutdown_grace` (5 s) plus
  `shutdown_timeout` (60 s), plus 10 s of slack.
- **TLS.** The peer API runs in plain HTTP on the compose network, and the nodes log a
  warning that `cluster_token` crosses it in the clear.

### Kubernetes

[`deploy/k8s/`](../deploy/k8s) holds a StatefulSet of three, a headless Service for peer
addresses, a client Service, an admin Service for Prometheus, a PodDisruptionBudget and a
ConfigMap. It has been validated with `kubeconform -strict` against Kubernetes 1.27 and
master.

```sh
kubectl create secret generic searchlight \
  --from-file=store_url=./store_url --from-file=cluster_token=./cluster_token --from-file=tokens=./tokens
kubectl apply -f deploy/k8s/
```

- **Identity.** Each pod's `node_id` is its pod name. Its `advertise_address` is
  `<pod>.searchlight-peers.<namespace>.svc:8780`.
- **Peer DNS.** The headless Service publishes not-ready pods: a node registers, and
  peers may call it, before it is ready.
- **Probes.** Both run on the admin port.
  - **Liveness** checks `/healthz`. It answers from the moment the admin listener is up,
    before the store is opened, so a node that is retrying the database or migrating is
    not restarted. No startup probe is needed.
  - **Readiness** checks `/readyz` every 5 s, and takes the pod out after 6 failures.
- **Termination.** No `preStop` hook is needed: a terminating pod leaves the Services'
  endpoints at once, and `shutdown_grace` (10 s) keeps it serving while that reaches
  every proxy. `terminationGracePeriodSeconds` is 80: `shutdown_grace` plus
  `shutdown_timeout` (60 s), plus 10 s.
- **Disruptions.** The PodDisruptionBudget allows one node down at a time.
- **Placement.** The example prefers spreading the nodes over hosts. For production,
  require it: make the pod anti-affinity `requiredDuringSchedulingIgnoredDuringExecution`,
  or add `topologySpreadConstraints` over `topology.kubernetes.io/zone`. Then one host or
  zone failing never takes every copy of a shard.
- **Security.** The pods run as non-root with a read-only root filesystem, no
  capabilities and the RuntimeDefault seccomp profile. They mount no service-account
  token, and the volume's ownership is fixed only when its root does not match
  (`fsGroupChangePolicy: OnRootMismatch`).
- **Resources** are sized in [Sizing](#sizing). The memory request equals the limit.

## Configuration

`store_url` is required, and so is `tokens_file` unless `insecure_no_auth` is set. Every
setting has one snake_case name, which is both the flag `--<name>` and the environment
variable `SEARCHLIGHT_<NAME>`.

- **Precedence.** A flag wins over the environment, which wins over the default.
- **Secrets from files.** `store_url` and `cluster_token` can also come from
  `SEARCHLIGHT_STORE_URL_FILE` and `SEARCHLIGHT_CLUSTER_TOKEN_FILE`. The file is read
  and its content trimmed. Setting both the variable and its `_FILE` is an error.
- **Validation.** The whole configuration is checked at start, and every bad setting
  is named. There is no configuration file: use flags, the environment, and the
  `_FILE` secrets.
- **Units.** Durations are Go durations (`500ms`, `30s`, `15m`, `24h`). Byte sizes take
  binary (`KiB`, `MiB`, `GiB`, `TiB`) or decimal (`KB`, `MB`, `GB`, `TB`) units, or plain
  bytes.

### Store URLs

```
postgres://user:pass@host:5432/db?sslmode=require     (or postgresql://)
mysql://user:pass@host:3306/db
sqlite:///var/lib/searchlight/searchlight.db
```

### Tokens file

The tokens file has one token per line, optionally followed by its scope: `write` (the
default) or `read`. Blank lines and lines starting with `#` are skipped. A token is at
least 16 bytes, with no whitespace. Without a tokens file, the node refuses to start
unless `insecure_no_auth` is set.

```
# ci-writer
2f6c0e9b5f8d4a7c9e1b3d5f7a9c1e3b5d7f9a1c write
# dashboards
8b1d3f5a7c9e1b3d5f7a9c1e3b5d7f9a1c3e5b7d read
```

### Every setting

| Setting | Default | Meaning |
|---|---|---|
| `store_url` | (required) | SQL database URL; also `SEARCHLIGHT_STORE_URL_FILE` |
| `listen` | `:8780` | public API address (`host:port`); port 0 picks a free port, which is then advertised |
| `admin_listen` | `127.0.0.1:8781` | admin listener for `/healthz`, `/readyz`, `/metrics` and pprof; must differ from `listen`. Loopback by default: set `:8781` (as the image does) for probes and Prometheus from elsewhere |
| `advertise_address` | `listen`, with this host's name when `listen` has no host | the `host:port` peers dial to reach this node |
| `node_id` | host name | this node's name in the registry, logs and telemetry; keep it stable across restarts so the node reclaims its own copies |
| `data_dir` | `data` | local segments, and recovery staging |
| `tokens_file` | (none) | the API bearer tokens; required unless `insecure_no_auth` |
| `insecure_no_auth` | `false` | serve the API unauthenticated when `tokens_file` is empty (logged as a warning; never on a reachable node) |
| `cluster_token` | (none) | auth for the internal peer API; also `SEARCHLIGHT_CLUSTER_TOKEN_FILE`. Required for a cluster: without it the peer API is closed |
| `peer_ca_file` | system roots | PEM CAs that sign peers' TLS certificates (the peer API runs over TLS when `tls_cert` is set) |
| `tls_cert`, `tls_key` | (none) | serve the API, and the peer API, over TLS; set together. Without them, put a TLS-terminating proxy in front |
| `lease_ttl` | `0`: 30 s on SQLite, 10 s elsewhere | how long a shard copy's lease lasts unrenewed (renewed every 2 s); at least 3 s |
| `prune_stall_timeout` | `15m` | how long a copy behind the others may make no progress and still hold the changelog's prune floor |
| `retiring_retention` | `15m` | how long the changelog is kept for a cleanly stopped node's copies, so a node restarted within it replays the tail rather than rebuilding |
| `changelog_retention` | `24h` | the oldest a change may grow before it is pruned, whatever copy still needs it (that copy rebuilds) |
| `bundle_interval` | `0s` (off) | how often each shard's durable state is uploaded to the database as a recovery bundle (`sl_blobs`), for a copy to recover from when no peer can serve it; at least `1s` when set (see [Recovery bundles](#recovery-bundles)) |
| `bundle_retention` | `2` | how many recovery bundles of each shard are kept, the newest |
| `refresh_interval` | `1s` | how often writes become searchable, on a fixed grid (an index's own `refresh_interval` overrides it). A refresh is visibility only: it fsyncs nothing |
| `flush_interval` | `10s` | how often a shard copy makes what refreshes published durable: it fsyncs the new segments and deletes, writes its manifest, and only then reports that seq as applied (the changelog is pruned by it). A crash replays at most this much of the changelog. A merge, a peer snapshot and a shutdown also flush at once |
| `max_lag` | `2s` | how far a copy may trail and still serve reads; also how long the database may go unanswered before readiness turns false |
| `changelog_poll_interval` | `500ms` | how often a copy polls the changelog when no local write, peer hint or Postgres notification wakes it |
| `remap_debounce` | `2s` | how long a copy that a mapping change must rebuild waits for more mapping changes (`0s` rebuilds at once) |
| `halt_retry_base`, `halt_retry_cap` | `30s`, `10m` | backoff between retries of a halted shard copy |
| `rebuild_retry_cap` | `2m` | longest backoff between retries of a copy rebuild that keeps failing |
| `merge_budget` | `64MiB` | bytes per second merges may write on this node (`0` = unlimited) |
| `merge_threads` | GOMAXPROCS/4, at least 1 | concurrent merge goroutines on this node |
| `search_threads` | GOMAXPROCS | the search worker pool |
| `search_queue` | `1000` | reads (searches, counts, percolations, field catalogues) in progress before more get 429 |
| `max_body_bytes` | `16MiB` | largest request body (at most 32 MiB less 1 KiB) |
| `max_doc_bytes` | `4MiB` | largest document or saved query JSON, in a PUT or a `_bulk` line |
| `max_bulk_ops` | `10000` | most operations in one `_bulk` |
| `max_index_fields` | `1000` | most fields in one index's mapping |
| `request_timeout` | `30s` | every API request's deadline |
| `read_timeout` | `1m` | how long reading a request's headers and body may take (slow clients are cut off) |
| `max_inflight_write_bytes` | `512MiB` | heap the writes in progress may take before more get 429 |
| `max_inflight_read_bytes` | `256MiB` | the same for reads |
| `inflight_amplification` | `10` | heap a request is charged per byte of its body (see [Sizing](#sizing)) |
| `drop_timeout` | `10m` | how long dropping an index from the database may take |
| `shutdown_grace` | `2s` | how long the API keeps serving after readiness turns false, so load balancers stop sending traffic first |
| `shutdown_timeout` | `60s` | the whole budget of a graceful shutdown after `shutdown_grace`, split as in [rolling restarts](#upgrades-and-rolling-restarts) |
| `log_level` | `info` | `debug`, `info`, `warn` or `error` |
| `pprof` | `false` | serve `/debug/pprof/*` on the admin listener |

### Refresh and flush

- **A refresh is visibility.** Every `refresh_interval`, on a fixed grid, each shard
  copy turns its write buffer into a segment and publishes it to searches. It writes the
  segment without fsync, so it costs CPU and page-cache writes only.
- **A flush is durability.** It runs every `flush_interval`, at shutdown, at every merge
  and before a peer snapshot. It fsyncs what the refreshes wrote, swaps the copy's
  manifest atomically, and only then advances `CommittedSeq`, the seq the copy reports
  as applied.
- **What a crash costs.** Acknowledged writes are never at risk: they are in the
  database. A crashed node reopens each copy at its last flushed manifest, and replays
  the changelog from there, which is at most `flush_interval` of changes.
- **Failed fsyncs.** A failed fsync is never retried. The copy fails and reopens from its
  last flush.
- **Tuning.** Raise `flush_interval` to fsync less often, at the price of a longer
  replay after a crash. A clean shutdown always flushes.

### Recovery bundles

A copy that must be rebuilt (a new node, a lost or corrupt disk, a copy pruned past)
fetches a serving peer's segments. With **no peer** to serve it (a single node, or every
node of the shard rebuilt at once), it rebuilds from the database's documents
(`ScanShard`), re-analyzing every one. Recovery bundles put a faster step in between:

- **Upload.** With `bundle_interval` set, every interval one serving copy of each shard
  (on the live node with the lowest `node_id`; set `bundle_interval` alike on every node,
  since a lowest-id node without it uploads none) flushes and uploads its durable segments,
  as one checksummed blob in `sl_blobs`, written in 1 MiB chunks. A shard with no write
  since its newest bundle is skipped. Once a new bundle is surely stored, the oldest are
  deleted down to `bundle_retention`.
- **Restore.** A copy with no peer to fetch from restores the newest bundle of its
  shard's current incarnation that the changelog still reaches, then replays the
  changelog after it. Each file, and the whole bundle, is checked against its SHA-256
  while it is staged under `data_dir/recovery`; a bundle that fails any check is never
  installed, and the next older one, then `ScanShard`, is used instead.
- **Cost.** Bundles take database space: about the size of each shard's segments, times
  `bundle_retention`. Each retained bundle also keeps the changelog after its seq, so
  `sl_changes` holds up to `bundle_interval × bundle_retention` of writes (never more
  than `changelog_retention`; a bundle that pruning by age passes is deleted).
- **Off.** `bundle_interval=0s`, the default, uploads none. Bundles already stored are
  still restored and still kept until pruning by age passes them, or their index is
  dropped.

Turn bundles on where rebuilding a shard from its documents is slow and no peer may be
there to copy from: a single node with a large index, or a cluster whose nodes may all
lose their disks together. A cluster that always has a serving peer gains nothing from
them.

### Removed settings

A removed setting is refused by name at start, whether it is given as a flag or as a
`SEARCHLIGHT_*` variable, and the error names its replacement:

| Setting | Removed | Instead |
|---|---|---|
| `seq_persist_interval` | with the refresh/flush split (#12) | `flush_interval` (default 10s) now persists the seq |

### Telemetry settings

These use the standard OpenTelemetry variables, not `SEARCHLIGHT_*`:

- **OTLP export.** Off by default. `OTEL_EXPORTER_OTLP_ENDPOINT`, or a per-signal
  `OTEL_EXPORTER_OTLP_{TRACES,METRICS}_ENDPOINT`, turns it on, as does
  `OTEL_{TRACES,METRICS}_EXPORTER=otlp`.
- **Protocol.** `OTEL_EXPORTER_OTLP_PROTOCOL` is `http/protobuf` (the default) or
  `grpc`.
- **Off switch.** `OTEL_SDK_DISABLED=true` turns all export off.
- **Resource.** `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` override it.
- **Prometheus.** `/metrics` on the admin listener is always on.

Logs are JSON on standard error, with `node_id` on every line, and `trace_id` and
`span_id` inside a span.

## Databases

The database is the write-ahead log, the system of record, and the cluster's only
coordination point. Its schema is created and migrated at start, under a lock, by
whichever node gets there first.

| Database | Supported | Notes |
|---|---|---|
| Postgres | tested on 17 | `LISTEN/NOTIFY` wakes the tailers between polls. Each node opens up to 64 connections plus one for `LISTEN`, so set `max_connections` to at least 65 per node plus headroom |
| MySQL | **8.0.19 or later**, tested on 8.4 LTS | 8.0.19 is the floor because the upserts use the row-alias form (`INSERT ... AS new ON DUPLICATE KEY UPDATE`). `max_allowed_packet` must be at least 64 MB (the 8.x default). Up to 64 connections per node |
| SQLite | 3.35+, bundled (pure Go) | **one node only.** A second live node on the same file refuses to start (`cluster: a SQLite store serves a single node`). The lease TTL defaults to 30 s, because the single writer can delay a renewal |

SQLite suits a laptop, a test, or a small single-node deployment. Its file must sit on a
local disk, not a network filesystem. To grow past one node, move to Postgres or MySQL by
re-indexing from the source. There is no SQLite-to-Postgres migration tool.

## Sizing

### Memory

A node's memory is three budgets, plus the page cache:

1. **In-flight requests.**
   - Each request is charged its body bytes times `inflight_amplification` (10) against
     `max_inflight_write_bytes` (512 MiB) or `max_inflight_read_bytes` (256 MiB).
     Requests past the budget get 429.
   - The amplification was measured at about 4.5 for a bulk, and about 8 for a bulk with
     `percolate=true` or a percolation (`BenchmarkBulkPeakHeap`). 10 leaves headroom.
   - The heap this takes at peak is about **1.5 × (write budget + read budget)**:
     1.1 GiB with the defaults.
   - Each budget must be at least `max_body_bytes × inflight_amplification`, so any one
     request fits.
2. **Shard write buffers.**
   - Each hosted shard copy buffers applied changes until its next refresh.
   - An early refresh starts at 64 MiB of buffer, and the buffer is capped at 4 × 64 MiB.
     A refresh in progress holds one more, frozen.
   - So a copy taking writes faster than it refreshes holds up to **about 320 MiB**, and
     an idle copy almost nothing. Count the copies that ingest at once, not every copy.
3. **Merges.** A merge streams its inputs into the merged file and holds only
   per-document state, under 1 KiB a document, plus up to 8 MiB for each field it
   writes at once (one per thread it runs on). A merge of a million of the benchmark's
   products peaks at about 0.6 GiB of heap on 4 threads and 0.7 GiB on 16.
   `merge_threads` merges can run at once.
4. **Caches and the runtime.** The filter cache, the search rank cache (up to
   256 MiB), the generations readers hold, and Go's own overhead.
5. **Segments are memory-mapped.** Their resident pages are page cache, not heap. In a
   container they count against the memory limit but are reclaimed under pressure.
   Searches stay fast while the hot segments fit in memory. The
   `searchlight_shard_mmap_resident_bytes` and `searchlight_shard_disk_size_bytes`
   metrics show how much of each shard is resident.

Set `GOMEMLIMIT` below the container limit, so the gap is left for the page cache. The
Kubernetes example sets `GOMEMLIMIT=3GiB` under a 4 GiB limit, with the default budgets.
That fits the in-flight peak, a few busy copies, and about 1 GiB of page cache. To run
smaller, lower the in-flight budgets (and `max_body_bytes` with them) first.

### CPU

- **Pools.** `search_threads` defaults to GOMAXPROCS, and `merge_threads` to a quarter of
  it.
- **Containers.** Go 1.25 sets GOMAXPROCS from the container's CPU limit, so both pools
  follow it.
- **Merges** are further capped by `merge_budget` bytes per second, so a merge storm
  cannot starve searches of disk.

### Disk

- **Segments.** A copy's segments are compressed: stored fields in small s2 blocks
  against a per-segment dictionary, bit-packed doc values and point blocks, roaring
  postings and prefix-compressed term dictionaries. On the benchmark's product listings
  (about 870 bytes of JSON each) a fully merged copy takes about 1.2 GiB per million
  documents; 3-grams of every keyword and text field, which make contains and similar
  fast, are about a quarter of it.
- **Headroom.** Leave at least the size of the largest shard copy free. A peer recovery
  stages a whole copy under `data_dir/recovery`, and an aside rebuild holds the old copy
  and the new one at once. A merge needs room for its result while its inputs are
  still on disk, plus, briefly, up to its result's term dictionaries again: fields
  written while an earlier one is still being written wait in temp files beside the
  segment (`<segment>.seg.spill*`, removed when the merge ends, or at the next start
  after a crash). Those writes count against `merge_budget`.
- **Speed.** Use local SSDs. Refreshes write segments without fsync, into the page
  cache. Each flush, every `flush_interval`, fsyncs what the refreshes since the last one
  wrote.

### The database

- **Records.** It holds every document and saved query (`sl_documents`, `sl_queries`).
- **Changelog.** `sl_changes` holds the changes not yet pruned: at most
  `changelog_retention` (24 h) of writes, usually far less. Pruning follows the slowest
  live copy, so a stalled copy holds the floor for at most `prune_stall_timeout`. Each
  retained recovery bundle holds it too, at its seq.
- **Recovery bundles** (with `bundle_interval` set) take about each shard's segment size
  times `bundle_retention` in `sl_blobs`.
- **Write rate.** Group commit coalesces each node's concurrent writes into one
  transaction every few milliseconds, so the database sees few, larger transactions.
  Use `_bulk` for throughput.

## Backups and disaster recovery

**The SQL database is the source of truth. `data_dir` is a cache that can always be
rebuilt.**

- **Back up the database** with its own tools: `pg_dump`, or base backups with WAL
  archiving for point-in-time recovery on Postgres; `mysqldump --single-transaction`,
  or a physical backup, on MySQL; for SQLite, `sqlite3 searchlight.db ".backup
  backup.db"` or a filesystem snapshot (never a plain copy of a live file). Every
  `sl_*` table belongs to Searchlight.
- **Do not back up `data_dir`.** A node that loses it rebuilds every copy: from a
  serving peer when one exists, else from a recovery bundle (when `bundle_interval` is
  set), else from the database. Losing a node's disk loses nothing.
- **Restoring the database to an earlier point.** Stop every node, and wipe every
  node's `data_dir` (or replace the volumes) before you start them again. Local
  segments may hold changes the restored database does not, and must not be reused.
  The nodes rebuild their copies from the restored database.
- **Losing the database** loses acknowledged writes back to your last backup. Plan its
  high availability as you would for any primary datastore. On a Postgres or MySQL
  failover, the nodes reconnect with backoff, since the sequence numbers live in the
  database. While it is unreachable, writes get 503 and reads serve from the segments,
  marked `stale`, and readiness turns false after `max_lag`.

## Upgrades and rolling restarts

Restart or upgrade **one node at a time**:

1. **Signal.** Send SIGTERM: `kubectl rollout restart`, `docker compose stop`, or
   `systemctl stop`.
2. **The node stops.** The signal fixes one deadline: `shutdown_grace` plus
   `shutdown_timeout` (T, 60 s by default) from then. Within it:
   1. Readiness turns false, and the node retires each shard copy that another node can
      serve, within T/4. The database checks atomically that another copy still serves,
      so nodes stopping together never retire a shard's last serving copy.
   2. The API keeps serving for `shutdown_grace`, while load balancers take the node
      out.
   3. The listener stops accepting, and requests in flight finish, within T/4.
   4. The node stops its remaining copies, writes their final manifests, marks them
      `retiring` in the registry and deregisters. It has until T/10 before the deadline,
      which is at least 0.4 T.
   5. In the last T/10, it closes the database and the admin listener, and flushes
      telemetry.
3. **Start the new binary** with the same `node_id` and `data_dir`. It reclaims its
   copies, reopens their segments, and replays only the changelog since it stopped. The
   changelog is kept for it for `retiring_retention` (15 min). Past that, it rebuilds its
   copies from a peer.
4. **Wait** until the node's `/readyz` is 200 and `GET /_cluster/health` is `green`
   again, then move on to the next node.

Notes:

- **Grace period.** Give the process `shutdown_grace` plus `shutdown_timeout`, plus a
  few seconds, before a hard kill. That is `terminationGracePeriodSeconds` in Kubernetes
  (80 in the example), and `stop_grace_period` or `docker stop -t` in Docker (75 s in
  the compose file). A node killed mid-shutdown loses nothing acknowledged: it replays
  more of the changelog when it comes back.
- **Schema migrations** run at start, on the first upgraded node. A binary refuses to
  start against a database a newer one has migrated (`store: database schema is newer
  than this binary: it has migration N, this binary knows up to M`). So an upgrade that
  brings a migration cannot be rolled back by downgrading the binary: restore the
  database instead, as above. Whether nodes still on the old binary keep working against
  the migrated schema is up to each migration, and its release notes say so. So far there
  is one migration, the initial schema.
- **Segment format.** It is versioned by a major number, and each binary reads its own
  major and the one before it (N−1). An upgrade across one major therefore reopens its
  segments, as a restart does; merges rewrite them into the new major as they go, and
  every segment the node writes from then on is in it.
  - A copy whose segments are older than N−1 (an upgrade that skips a major) is rebuilt:
    from a peer, a recovery bundle or the database. Upgrade one major at a time to
    avoid it, or budget for the rebuild.
  - A recovering copy fetches from a peer already in the current major when one serves
    it, and from an older-major peer only when none does (its segments still open).
  - A copy in a **newer** format is refused and left as it is (`shard copy is in a newer
    format than this binary reads`): a binary rolled back never destroys what its
    successor wrote. Roll forward again, or wipe that node's `data_dir` to have it
    rebuild from its peers or the database.
  - A node still on the old binary that must recover a copy during the upgrade cannot
    use an upgraded peer's segments: the peer refuses the snapshot (409
    `segments_newer_format`, before taking it), and the old node's bundle reader
    refuses a bundle an upgraded node wrote. It tries its other peers, then older
    bundles, and otherwise rebuilds the copy from the database. Finish a rolling upgrade
    before replacing nodes.
- **Upgrading to segment format 4** (from 3). Nothing to do: the first start reopens the
  format-3 segments and serves at once. Merges rewrite them as they go (about 2%
  smaller on the benchmark's documents, with hit fetches several times faster). A
  segment's deletes sidecars are stamped with the segment's own format, so deletes
  alone never make a format-3 copy unreadable to a node not yet upgraded.
  Untyped-value marks (which decide whether a mapping change that maps a new field
  needs a rebuild) carry over from the copy's manifest. A rollback to a format-3
  binary refuses every copy that has written a segment since the upgrade, as above.
- **Readiness gates traffic.** A restarted node reports ready only after its copies have
  finished their startup recovery. Until then, the other nodes serve its shards.

## Monitoring

Prometheus scrapes `http://<node>:8781/metrics`, which needs no auth. The metric names
are OpenTelemetry names with dots turned into underscores, plus a unit suffix
(`_seconds`, `_bytes`, `_ratio`) and `_total` on counters. Labels include `index`,
`shard`, `http_route` and `http_response_status_code`. The full catalogue, with every
label, is `Catalog` in [`internal/telemetry/metrics.go`](../internal/telemetry/metrics.go).

| Area | Metrics |
|---|---|
| Requests | `searchlight_http_request_duration_seconds` (by route, method, status), `searchlight_http_request_errors_total`, `searchlight_http_requests_inflight` |
| Search | `searchlight_search_phase_duration_seconds` (plan, execute, reduce, fetch), `searchlight_search_segments_touched`, `searchlight_search_filter_cache_lookups_total` (hit, miss), `searchlight_search_documents_scanned_total`, `searchlight_search_documents_matched_total` |
| Percolation | `searchlight_percolate_duration_seconds` (probe, verify), `searchlight_percolate_candidates`, `searchlight_percolate_verifications_total`, `searchlight_percolate_always_check` |
| Indexing | `searchlight_index_changes_total`, `searchlight_store_group_commit_batch_size`, `searchlight_replica_apply_batch_size`, `searchlight_replica_apply_duration_seconds`, `searchlight_replica_backpressure_total` |
| Refresh, flush and merge | `searchlight_shard_open_failures_total`, `searchlight_shard_refresh_duration_seconds`, `searchlight_shard_refresh_failures_total`, `searchlight_shard_flush_duration_seconds`, `searchlight_shard_flush_failures_total`, `searchlight_shard_merge_duration_seconds`, `searchlight_shard_merge_bytes_total`, `searchlight_shard_merge_backlog`, `searchlight_shard_merge_failures_total`, `searchlight_shard_segments`, `searchlight_shard_buffer_documents` |
| Size | `searchlight_shard_documents`, `searchlight_shard_terms`, `searchlight_shard_disk_size_bytes`, `searchlight_shard_mmap_resident_bytes` |
| Replication | `searchlight_replica_lag_seq`, `searchlight_replica_lag_time_seconds`, `searchlight_replica_halted_ratio`, `searchlight_replica_halts_total`, `searchlight_replica_poll_failing_ratio`, `searchlight_replica_recovery_progress_ratio`, `searchlight_replica_recoveries_total`, `searchlight_replica_recovery_duration_seconds`, `searchlight_replica_recovery_bytes_total`, `searchlight_replica_watch_reconnects_total` |
| Cluster | `searchlight_cluster_nodes`, `searchlight_cluster_lease_changes_total` (by kind: `claim`, `renew`, `renew_failed`, `reclaim`, `lapse`, `resume`, `lost`, `release`), `searchlight_cluster_allocation_changes_total`, `searchlight_cluster_peer_request_duration_seconds`, `searchlight_cluster_read_retries_total` |
| Database | `searchlight_store_operation_duration_seconds` (by operation, dialect), `searchlight_store_errors_total`, `searchlight_store_wal_size_bytes` (SQLite's write-ahead log, and the part no checkpoint has copied yet, `pending="true"`) |
| Runtime | `go_*` and `process_*` |

Counters and histograms with no observations yet do not appear until their first one.

### Suggested alerts

| Alert | Expression (PromQL sketch) | Why |
|---|---|---|
| Cluster red | `GET /_cluster/health` reports `red` (probe it, or alert on the next row) | a shard has no serving copy: its reads fail |
| Copy halted | `max by (index, shard) (searchlight_replica_halted_ratio) > 0` for 5 m | a copy stopped at a change it cannot apply (see below) |
| Replica lag | `max by (index, shard) (searchlight_replica_lag_time_seconds) > 30` for 5 m | a copy trails the changelog; it serves no reads past `max_lag` |
| Changelog polls failing | `max(searchlight_replica_poll_failing_ratio) > 0` for 2 m | the database is unreachable from a node |
| Nodes missing | `max(searchlight_cluster_nodes) < 3` (your node count) for 5 m | a node died or cannot heartbeat |
| Lease renewals failing | `rate(searchlight_cluster_lease_changes_total{kind="renew_failed"}[5m]) > 0` | a node is about to lose its copies |
| Leases lost | `rate(searchlight_cluster_lease_changes_total{kind="lost"}[5m]) > 0` | a node lost copies to another node (its renewals failed past `lease_ttl`); they recover elsewhere |
| Database errors | `rate(searchlight_store_errors_total[5m]) > 0` for 5 m | SQL errors |
| Server errors | `sum(rate(searchlight_http_request_errors_total{http_response_status_code=~"5.."}[5m])) / sum(rate(searchlight_http_request_duration_seconds_count[5m])) > 0.01` | more than 1% of requests fail |
| Backpressure | `rate(searchlight_http_request_errors_total{http_response_status_code="429"}[5m]) > 0` for 10 m | clients are being refused; see Sizing |
| Refresh, flush or merge failing | `rate(searchlight_shard_refresh_failures_total[5m]) > 0`, `rate(searchlight_shard_flush_failures_total[5m]) > 0`, `rate(searchlight_shard_merge_failures_total[5m]) > 0` | usually a full or failing disk |
| Merge backlog | `max(searchlight_shard_merge_backlog) > 50` for 30 m | merges cannot keep up: raise `merge_budget` or `merge_threads` |
| Not ready | the readiness probe failing for 10 m | stuck recovery, or the database unreachable |

## Troubleshooting

Start with `GET /_cluster/shards` and `GET /_cluster/nodes`, on any node, with a read
token. Each copy shows:

- its node and `state`;
- `applied_seq`, `refreshed_seq` and `committed_seq`;
- its `lag` in changes;
- `docs`;
- `stale` and `rebuilding`;
- `error`, for a halted copy.

Logs carry `index`, `shard` and `node_id`.

### A copy is `halted`

A halted copy met a change that the database accepted but the copy cannot apply: a body
that does not analyze, a query that does not parse, a change the shard refuses. That is a
bug. Copies never skip a change, so the copy stops just before it:

1. It is marked `recovering` in the registry, and serves no reads. The other copies
   serve the shard.
2. After a backoff (`halt_retry_base`, doubling up to `halt_retry_cap`), it tails again.
3. If it halts at the same change again, it rebuilds itself from a snapshot. A snapshot
   holds only current rows, so a change that has since been superseded is not replayed.
4. If the offending row is still current, the rebuild halts too, and the copy backs off
   again.

What to do:

- **Find the change.** `error` on `/_cluster/shards`, and the log line `shard copy halted:
  the store accepted a change this copy cannot apply`, name the seq, the id and the
  reason. `searchlight_replica_halts_total` has a
  `reason` label.
- **Unblock the copy.** If the document or saved query can go, overwrite or delete it
  through the API. The next rebuild then gets past it.
- **Report it**, with the log line and the offending body. A halt means the API accepted
  a write that analysis refuses.

When every copy of a shard halts, the shard has no serving copy and the cluster is
`red`.

### A copy stays `recovering`

A recovering copy is fetching a snapshot from a serving peer, or rebuilding from the
database, and then replaying the changelog.

- **Is it making progress?** `searchlight_replica_recovery_progress_ratio` and
  `searchlight_replica_recovery_bytes_total` (by `source`: `peer`, `blob` for a recovery
  bundle, or `sql`) should climb.
- **Can it reach its peers?** Check that `advertise_address` resolves and is reachable
  from the other nodes, and that every node has the **same** `cluster_token` (a missing
  one closes the peer API, and a wrong one gets 401s). With TLS, check `peer_ca_file`.
  Failing peer requests show in `searchlight_cluster_peer_request_duration_seconds`
  with status 0, 401 or 5xx.
- **Is there room?** Staging needs the copy's whole size under `data_dir/recovery`.
- **Is it retrying?** A recovery that fails falls back to a recovery bundle when there
  is one (`a recovery bundle failed its checks` names one refused), then to the database
  (`ScanShard`), then retries with backoff up to `rebuild_retry_cap`. A copy whose changelog was pruned
  past (`ErrPruned` in the log) rebuilds rather than replays: that is expected after a
  node was down for longer than `retiring_retention` or `prune_stall_timeout`.
- **Is it a remap?** A mapping change that newly maps a field already held by documents
  rebuilds the affected copies after `remap_debounce`. Reads of such a copy answer 503
  meanwhile.

### Readiness stays false

`/readyz` says why, in its problem `detail`:

- `the database has not answered for …`: the node cannot reach the database. Check the
  network and credentials, and `searchlight_store_errors_total`.
- `the node has not joined the cluster yet`: it is starting, or its registration in the
  database failed (see the log).
- `N shard copies have not finished their startup recovery`: see above.
- `the node is shutting down`: it received SIGTERM.

### A second node will not start on SQLite

`cluster: a SQLite store serves a single node; use Postgres or MySQL for a cluster` is
expected: SQLite serves one node. If no other node is in fact running, the registration
of the earlier one ages out after 10 s.

### Other messages

- **`cluster_token is sent in the clear`.** `advertise_address` is not loopback, and
  `tls_cert` is unset. Set `tls_cert`/`tls_key` (and `peer_ca_file`), or accept the risk
  on a private network.
- **A lease lost.** It shows as `kind="lost"` on the lease metric, `ErrLeaseLost` in a
  log line, or `a lease expired and its slot could not be claimed back`. The node could
  not renew a lease within `lease_ttl`, and another node may have claimed the slot. The copy stops serving and is dropped or
  re-recovered. Look for database latency spikes, or a node paused by its host (GC,
  CPU starvation).
- **429 `too_many_requests`.** The cause is in `detail`:
  - a full `search_queue`;
  - an in-flight byte budget;
  - a copy's write buffer full (backpressure while refreshes catch up).

  Retry after `Retry-After`. If it persists, see [Sizing](#sizing).
- **`stale: true` on reads.** The database cannot be reached, and the node answers from
  its segments.
- **`shard copy does not open; wiping it to rebuild`.** A copy's files failed their
  checks when the node opened them: a segment failing its checksum, a damaged manifest,
  a file the manifest lists gone missing, or a format older than this binary reads. The
  copy is never served; it is wiped and rebuilt like a new one (from a peer, a recovery
  bundle or the database). Repeated on a node, suspect its disk.
  `searchlight_shard_open_failures_total` counts these by `reason`: `corrupt` (wiped),
  `newer_format` (refused, see [upgrades](#upgrades-and-rolling-restarts)) and `error`
  (an I/O, permission or resource failure: nothing is wiped, and the allocator retries).
- **Profiling.** Set `pprof=true`, then
  `go tool pprof http://<node>:8781/debug/pprof/profile?seconds=30`.
