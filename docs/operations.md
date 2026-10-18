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
the same; any node takes any request. For how that cluster behaves (joining, writes,
reads, recovery, failures) see [clustering.md](clustering.md); for putting nginx in
front of it as the load balancer, see [nginx.md](nginx.md).

| Port | Listener | Serves |
|---|---|---|
| 8780 (`listen`) | public | the API, and the internal peer API under `/_internal/` (cluster_token auth) |
| 8781 (`admin_listen`) | admin | `GET /healthz`, `GET /readyz`, `GET /metrics` (no auth), `/debug/pprof/*` when `pprof` is on (not `cmdline`, which could show secrets passed as flags) |

- **Admin listener.** It binds to loopback, `127.0.0.1:8781`, by default. The image and
  the Kubernetes ConfigMap set `:8781` explicitly, so probes and Prometheus can reach it.
  Wherever it is reachable, keep the port private: Prometheus and probes only. A node
  with `pprof=true` on a non-loopback `admin_listen` logs a warning at start.
- **Peers** reach each other on the public port, at `advertise_address`.

Each [release](https://github.com/Imposter/go-searchlight/releases) publishes what a
deployment needs, none of which needs a checkout of the repository:

- binaries for linux/amd64, linux/arm64, darwin/amd64, darwin/arm64 and windows/amd64
  (`searchlight-<version>-<os>-<arch>.tar.gz`, `.zip` on Windows), with `SHA256SUMS`;
- the image `ghcr.io/imposter/searchlight:<version>`, for linux/amd64 and linux/arm64
  ([Docker](#docker));
- `searchlight-deploy-<version>.tar.gz`: the Compose samples with their `.env.example`,
  nginx's config and the Kubernetes manifests ([Docker Compose](#docker-compose),
  [Kubernetes](#kubernetes)).

[CHANGELOG.md](../CHANGELOG.md) has each release's notes; read them before an upgrade.

### The binary

From a release:

```sh
base=https://github.com/Imposter/go-searchlight/releases/download/v1.2.0
curl -fsSLO "$base/searchlight-1.2.0-linux-amd64.tar.gz" -O "$base/SHA256SUMS"
sha256sum --ignore-missing -c SHA256SUMS
tar -xzf searchlight-1.2.0-linux-amd64.tar.gz
install searchlight-1.2.0-linux-amd64/searchlight /usr/local/bin/
```

Each archive also holds `LICENSE` and `THIRD_PARTY_NOTICES`, the licences of Go and of
every dependency linked into the binary.

**macOS.** The darwin binaries are not signed or notarised. Downloaded with `curl` they
run as they are; downloaded with a browser, macOS quarantines them and Gatekeeper refuses
to open "searchlight". Clear the quarantine flag:

```sh
xattr -d com.apple.quarantine searchlight
```

**Windows.** Run the node with `GODEBUG=asyncpreemptoff=1` in its environment. The Go
runtime can corrupt a thread's registers when it preempts that thread while Windows is
handling an exception on it, such as a page fault in a mapped segment
([golang/go#79249](https://github.com/golang/go/issues/79249)). The node then crashes
with an access violation at a small address in code that is otherwise correct. The fix
is in Go 1.28, and no release carries it yet. The setting costs nothing beyond a
goroutine in a long, call-free loop sometimes waiting for its next function call to
yield. Drop it once a release is built with the fixed toolchain.

or, after macOS refuses to run it once, choose **Open Anyway** in System Settings ›
Privacy & Security (on macOS 14 and earlier, Finder's right-click **Open** also works);
macOS remembers the choice.

Or build it from a checkout (`go install github.com/Imposter/go-searchlight/cmd/searchlight@v1.2.0`
also works, and reports its version), then run it:

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
- **`searchlight version`** (or `--version`) prints the version it was built as, with its
  Go version and platform: `searchlight v1.2.0 (go1.25.14, linux/amd64)`.
- **`searchlight healthcheck [--live]`** probes the node on this host, for container
  healthchecks. It GETs `/readyz` (`/healthz` with `--live`) on `SEARCHLIGHT_ADMIN_LISTEN`,
  where an empty or unspecified host means `127.0.0.1`. It waits at most 2 s, and exits 0
  on a 200, else 1.

### Docker

Each release publishes `ghcr.io/imposter/searchlight`, for linux/amd64 and linux/arm64,
tagged with its version (`1.2.0`). Its minor (`1.2`) and major (`1`) lines, and `latest`,
point at the newest stable release of each: they only move forward, so a patch to an
older line moves its own `<major>.<minor>` tag and nothing else. Pin the full version in
production. The `sha-<commit>` tags are where the release workflow stages an image
before it is tested; do not deploy them.

```sh
docker pull ghcr.io/imposter/searchlight:1.2.0
docker run -d --name searchlight -p 8780:8780 -p 127.0.0.1:8781:8781 \
  -v searchlight-data:/var/lib/searchlight \
  -v "$PWD/secrets:/run/secrets/searchlight:ro" \
  -e SEARCHLIGHT_STORE_URL_FILE=/run/secrets/searchlight/store_url \
  -e SEARCHLIGHT_TOKENS_FILE=/run/secrets/searchlight/tokens \
  ghcr.io/imposter/searchlight:1.2.0
```

- **User.** It runs as uid/gid 65532.
- **Defaults.** `SEARCHLIGHT_LISTEN=:8780`, `SEARCHLIGHT_ADMIN_LISTEN=:8781` and
  `SEARCHLIGHT_DATA_DIR=/var/lib/searchlight`, which is a volume.
- **Labels.** It carries the OCI labels (version, revision, created, source).
- **Licences.** `/usr/share/doc/searchlight` holds `LICENSE` and `THIRD_PARTY_NOTICES`.
- **Provenance.** The published image carries a SLSA provenance attestation and an SBOM:
  `docker buildx imagetools inspect ghcr.io/imposter/searchlight:1.2.0 --format '{{json .Provenance}}'`
  (or `.SBOM`).
- **`HEALTHCHECK`.** It runs `searchlight healthcheck` (readiness) every 5 s, with a
  2-minute start period. The image has no shell, so the binary probes itself.

To build the image from a checkout instead,
[`deploy/Dockerfile`](../deploy/Dockerfile) builds a static (`CGO_ENABLED=0`,
`-trimpath`) binary onto `gcr.io/distroless/static-debian12:nonroot`:

```sh
make docker                                  # searchlight:dev; IMAGE=... to rename
```

The base images are pinned by digest. Dependabot (`.github/dependabot.yml`) proposes
updates for them, for the compose images, for the Go modules and for the CI actions. The
one exception is the kubeconform image that CI runs inside a `run:` step: Dependabot
cannot see it, so it is bumped by hand, as the comment beside it says.

### Docker Compose

Two samples run the published image, from a release's
`searchlight-deploy-<version>.tar.gz` (or [`deploy/`](../deploy) in a checkout), with
every path relative to the compose file:

- [`compose.postgres.yml`](../deploy/compose.postgres.yml): three nodes on Postgres 17,
  behind nginx;
- [`compose.sqlite.yml`](../deploy/compose.sqlite.yml): one node on SQLite.

```sh
curl -fsSL https://github.com/Imposter/go-searchlight/releases/download/v1.2.0/searchlight-deploy-1.2.0.tar.gz | tar -xz
cd searchlight-deploy-1.2.0
cp .env.example .env                                                       # optional
docker compose -f compose.postgres.yml up -d
docker compose -f compose.postgres.yml run --rm secrets cat /secrets/tokens   # the API tokens
curl -H "Authorization: Bearer <write token>" 127.0.0.1:8080/_cluster/health
```

The SQLite sample reads its API tokens from a `tokens` file beside it, mounted as a
Compose secret:

```sh
printf '%s write\n' "$(openssl rand -hex 24)" > tokens
docker compose -f compose.sqlite.yml up -d
curl -H "Authorization: Bearer $(cut -d' ' -f1 tokens)" 127.0.0.1:8780/indexes
```

- **Settings.** [`.env.example`](../deploy/.env.example) lists what the samples read:
  the image tag (`SEARCHLIGHT_VERSION`), the published address and ports, Postgres's user,
  database, `max_connections` and `shared_buffers`, the log level, and the changelog and
  recovery-bundle retention. Copy it to `.env` beside the compose file; its values are
  the defaults. Any other setting is a `SEARCHLIGHT_*` variable in the compose file.
- **Secrets.** In the Postgres sample, a one-shot `secrets` service generates them on the
  first start, into a volume: the Postgres password, the `store_url` built from it, the
  `cluster_token`, and a tokens file with one read-write and one read-only token. The
  nodes read them through `SEARCHLIGHT_*_FILE` and `tokens_file`. Neither the compose
  file nor `.env` holds a secret. The `store_url` is built once: changing
  `POSTGRES_USER` or `POSTGRES_DB` later needs fresh `secrets` and `pgdata` volumes.
- **Ports.** nginx serves the cluster's API on `SEARCHLIGHT_BIND:SEARCHLIGHT_HTTP_PORT`,
  `127.0.0.1:8080` by default; the SQLite node on `SEARCHLIGHT_BIND:SEARCHLIGHT_API_PORT`,
  `127.0.0.1:8780`. Set `SEARCHLIGHT_BIND=0.0.0.0` to serve other hosts, behind TLS
  ([nginx.md](nginx.md#tls)). The cluster's nodes keep their own API on `127.0.0.1:8780`,
  `:8782` and `:8784`, and every admin listener stays on `127.0.0.1` (`:8781`, `:8783`
  and `:8785`).
- **Addresses.** Each node's `node_id` and `advertise_address` is its service name, so it
  reclaims its own copies when it is recreated.
- **Health.** Postgres is checked with `pg_isready`, and each node with
  `searchlight healthcheck`.
- **Stopping.** `stop_grace_period` is 75 s: `shutdown_grace` (5 s) plus
  `shutdown_timeout` (60 s), plus 10 s of slack (72 s for the SQLite node, whose
  `shutdown_grace` is 2 s).
- **TLS.** The peer API runs in plain HTTP on the compose network, and the nodes log a
  warning that `cluster_token` crosses it in the clear.
- **Load balancer.** An `nginx` service fronts the three nodes, using
  [`nginx/searchlight.conf`](../deploy/nginx/searchlight.conf); see [nginx.md](nginx.md)
  for what it does and why.

**Upgrading.** Read the new release's notes in [CHANGELOG.md](../CHANGELOG.md), set
`SEARCHLIGHT_VERSION` in `.env` to it, and run `docker compose -f <file> up -d` again.
Compose pulls the new image and recreates the nodes; the first to start migrates the
schema. That restarts every node of the cluster at once. To keep it serving, recreate
one node at a time, as in [rolling restarts](#upgrades-and-rolling-restarts):

```sh
docker compose -f compose.postgres.yml pull
docker compose -f compose.postgres.yml up -d --no-deps searchlight-1    # then wait until it is healthy
docker compose -f compose.postgres.yml up -d --no-deps searchlight-2    # and so on
```

A newer release's deploy bundle may change the compose files too: compare them with
yours before you switch to them.

**From a checkout**, [`deploy/compose.yml`](../deploy/compose.yml) runs the same
three-node cluster on an image built from the working tree, for trying a change out:

```sh
docker compose -f deploy/compose.yml up --build -d
docker compose -f deploy/compose.yml run --rm secrets cat /secrets/tokens
```

It repeats `compose.postgres.yml`'s services, with `build:` in place of the published
image; change the two together.

### Kubernetes

[`deploy/k8s/`](../deploy/k8s) holds a StatefulSet of three, a headless Service for peer
addresses, a client Service, an admin Service for Prometheus, a PodDisruptionBudget and a
ConfigMap. It has been validated with `kubeconform -strict` against Kubernetes 1.27 and
master. The same files are `k8s/` in a release's deploy bundle, and run that release's
image, `ghcr.io/imposter/searchlight:<version>`; upgrade by changing the StatefulSet's
image, which rolls the nodes one at a time.

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

`store_url` is required; so is `tokens_file`, and on Postgres or MySQL `cluster_token`,
unless `insecure_no_auth` is set. Every setting has one snake_case name, which is both
the flag `--<name>` and the environment variable `SEARCHLIGHT_<NAME>`.

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
| `cluster_token` | (none) | auth for the internal peer API; also `SEARCHLIGHT_CLUSTER_TOKEN_FILE`. Required on Postgres and MySQL (refused at start without it unless `insecure_no_auth` is set): without it the peer API is closed. A peer that refuses this node's token counts as a failed copy, and is skipped |
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
| `gc_heap_floor` | `64MiB` | heap size below which the garbage collector does not start a cycle (`0` = off). A `GOGC` environment variable overrides it, and it is at most a quarter of `GOMEMLIMIT`. The effective value is logged at start. See [Memory](#memory) |
| `search_queue` | `1000` | reads (searches, counts, percolations, field catalogues) in progress before more get 429 |
| `max_body_bytes` | `16MiB` | largest request body (at most 32 MiB less 1 KiB) |
| `max_doc_bytes` | `4MiB` | largest document or saved query JSON, in a PUT or a `_bulk` line |
| `max_bulk_ops` | `10000` | most operations in one `_bulk` |
| `max_result_window` | `10000` | most a search's `from + size` reaches (Elasticsearch's `index.max_result_window`, here per node), at most 1,000,000; past it the search is a 400, and deeper pages use `search_after`. Each shard holds `from + size` hits of such a search, and the coordinator merges them all. The node that takes the search applies its own, so set it alike on every node. With `from` 0 it bounds `size` too, as in Elasticsearch: below 10,000 it refuses a `size` above it, which was answered before. A search with a `from` reserves about 1 KiB per hit of its window from `max_inflight_read_bytes` (at most half of it) while it runs |
| `max_index_fields` | `1000` | most fields in one index's mapping |
| `request_timeout` | `30s` | every API request's deadline |
| `read_timeout` | `1m` | how long reading a request's headers and body may take (slow clients are cut off) |
| `max_inflight_write_bytes` | `512MiB` | heap the writes in progress may take before more get 429 |
| `max_inflight_read_bytes` | `256MiB` | the same for reads |
| `inflight_amplification` | `10` | heap a request is charged per byte of its body (see [Sizing](#sizing)) |
| `drop_timeout` | `10m` | how long dropping an index from the database may take |
| `shutdown_grace` | `2s` | how long the API keeps serving after readiness turns false, so load balancers stop sending traffic first |
| `shutdown_timeout` | `60s` | the whole budget of a graceful shutdown after `shutdown_grace`, split as in [rolling restarts](#upgrades-and-rolling-restarts) |
| `request_cache_size` | `128MiB` | memory for the [request cache](#request-cache) of size-0 searches' shard results (`0` = none in memory) |
| `request_cache_redis_url` | (none) | a Redis behind the request cache, shared by every node that reaches it (`redis://[user:password@]host:port[/db]`, `rediss://` for TLS); also `SEARCHLIGHT_REQUEST_CACHE_REDIS_URL_FILE` |
| `request_cache_redis_password` | (none) | the Redis password, in place of the URL's; also `SEARCHLIGHT_REQUEST_CACHE_REDIS_PASSWORD_FILE` |
| `request_cache_redis_prefix` | `searchlight:rc:` | prefix of every key the cache writes to Redis |
| `request_cache_redis_ttl` | `24h` | how long a Redis entry lives |
| `request_cache_redis_timeout` | `50ms` | bound of each Redis call, its dial included: a slow or unreachable Redis delays a search by at most about this much (and is skipped for a while after 5 failures in a row) |
| `request_cache_redis_max_entry_bytes` | `8MiB` | largest encoded shard result written to or read from Redis |
| `log_level` | `info` | `debug`, `info`, `warn` or `error` |
| `pprof` | `false` | serve `/debug/pprof/*` on the admin listener |

### Request cache

Each node keeps the shard results of searches with `size` 0 (counts and
aggregation-only searches, such as a facet panel's) and answers an identical search of
the same shard contents from them, as Elasticsearch's shard request cache does
([`request_cache`](api.md#search-and-count)).

- **Never stale.** An entry is keyed by the request and by what the shard copy holds:
  a refresh that writes, a delete, a merge or a mapping change makes new keys, and the
  copy's older entries are dropped from memory at the next store. Nothing is
  invalidated by hand. A hit has the counts and buckets the search would compute; a
  float sum may differ in its last bits (it was summed in another copy's segment
  order), and the result's segment and scan counts are the computing copy's.
- **Memory.** `request_cache_size` (128 MiB) bounds the entries' estimated heap,
  least recently used out first; one entry takes at most an eighth of it. Concurrent
  identical searches of a copy compute once; a search waiting for another's result
  waits within its own `timeout`.
- **Redis.** With `request_cache_redis_url` set, the memory cache sits in front of a
  Redis that every node, and every copy of a shard, shares, and that outlives
  restarts. Its keys name the index incarnation, the shard, the mapping version and
  the shard's newest change the copy holds (its seq, and its mark: id, commit time and
  nonce, so a database failover that reissued seqs never reads another history's
  results), this binary's version, and a hash of the request. Every copy that holds the
  shard's changes up to that change holds the same documents, whatever its segments
  and however far writes to other shards and indexes have moved its seq. Values are
  the shard result as peers send it, compressed with zstd.
- **What Redis shares, and what it does not.** A copy names its contents for Redis
  only once a changelog poll (every `changelog_poll_interval`, 500 ms, or sooner when
  woken) has checked the shard's newest change; until then it uses its memory cache
  alone. A shard written to often therefore shares little: with `w` writes a second to
  it and a poll every `P` seconds, a key exists about `e^(−w·P)` of the time. The win
  is the quiet shards, which keep their keys whatever other shards and indexes write,
  across nodes, copies and restarts. Two more cases have no key: a copy rebuilt from
  the database after pruning removed its shard's newest change from the changelog (a
  shard quiet for longer than the copies' prune floor), until the shard's next write;
  and a shard whose newest change predates change nonces (written by a release before
  them), until its next write.
- **A Redis that fails costs nothing but its hits.** Each call, its dial included, is
  bounded by `request_cache_redis_timeout` (50 ms) and by a tenth of what is left of
  the search's own time (`timeout`, or `request_timeout`), with no retries; a search
  with under 20 ms left does not ask Redis. A read that runs out of its time while
  the search has time left is Redis's failure. A failed read or write is logged (once per 10 s per operation),
  counted in `searchlight_search_request_cache_errors_total`, and the shard computes.
  After 5 failures in a row Redis is skipped for 1 s, then one call probes it; each
  failed probe doubles the wait, up to 30 s
  (`searchlight_search_request_cache_store_open_ratio` is 1 meanwhile). Writes are made in
  the background, at most 16 at once; those dropped are counted in
  `searchlight_search_request_cache_writes_dropped_total`.
- **Redis is trusted, as the database is.** Anyone who can write to it can make
  searches return forged counts: give it a password (`request_cache_redis_password`),
  TLS (`rediss://`) on an untrusted network, and no other writers under the prefix.
  What is read back is bounded (`request_cache_redis_max_entry_bytes`, and at most 8
  times that decoded) and checked against the request, so a corrupt value is a miss,
  never a failure.
- **Expiry.** Redis entries expire after `request_cache_redis_ttl` (24 h); results
  whose encoding is larger than `request_cache_redis_max_entry_bytes` stay in memory
  only. Point several clusters at one Redis with different
  `request_cache_redis_prefix`es, and give the Redis a `maxmemory` with an eviction
  policy such as `allkeys-lru`.

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
- **Refresh on demand.** An index whose `refresh_interval` is `-1` never refreshes on a
  timer. A request that waits for a seq refreshes the copy itself once the copy has
  applied it: a read's `wait_for_seq`, a write's `refresh=wait_for` or `true`, and a
  percolation, which waits for the saved queries written through its node. Without such
  a wait, writes stay invisible until one comes.

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
   A [point field](api.md#point-fields) adds 24 bytes per point the merged segment
   keeps, held whole while its k-d tree is built: about 240 MB for 10M points (1M
   documents of 10). No setting bounds it (`merge_budget` limits bytes written per
   second, not memory). The merge policy caps a merged segment at 1 GiB, and a point
   takes about 14 bytes there, so a merge holds at most about 1.8 GiB of points (75M),
   and that only for a segment of little but points; `merge_threads` merges can run
   at once.
4. **Caches and the runtime.**
   - The filter cache.
   - The search rank cache: up to 256 MiB, at most 64 MiB of it for one segment (a
     segment past 16M documents rebuilds its rank array per search instead).
   - The global-ordinals cache: up to 256 MiB. A terms aggregation or cardinality on a
     keyword or list field over many of its terms merges the field's dictionaries
     across a shard's segments once, and keeps that for later searches until a
     refresh or merge changes the segment list. Its size and build time grow with the
     field's distinct terms (about 24 bytes each, plus 8 per term per segment): a few
     milliseconds for thousands of values, tens of milliseconds and megabytes for
     hundreds of thousands. A search whose hits hold few of the terms builds nothing.
     After a refresh the new list's ordinals are derived from the old list's (a
     fraction of a full build: the new segment's terms are looked up, the rest is
     renumbered), in the background for the fields searched in the last 10 minutes.
     Every build, a search's or a warm's, takes a slot of one pool of a quarter of the
     CPUs (at least one), so builds never use more; each shard's field has at most one
     in flight, a newer generation's superseding a queued older one (a running one
     finishes, and the newer derives from it), and a search's build is served before
     warm builds queued earlier. A search never waits on a build still queued, and on
     a running one at most 250 ms, then merges by term, exactly; the build finishes in
     the background and serves the next search. `searchlight_search_ordinals_builds_total` (by
     `kind`: full, incremental, unstarted; `trigger`: search, warm; `result`: done,
     cancelled, superseded, shutdown, panic) and `searchlight_search_ordinals_lookups_total`
     (`hit`, `shared`, `miss`, `queued`: left to the background, `stale`, and `absent` at a
     search's start) show how
     often searches wait on a build.
   - The request cache: up to `request_cache_size` (128 MiB) of size-0 searches'
     shard results, by estimate; a terms aggregation's partial takes about 100 bytes
     a bucket.
   - The generations readers hold, and Go's own overhead.
   - **The heap floor.** Segments are mapped files, so a serving node's live heap is
     only a few megabytes, and at Go's default `GOGC` the garbage collector would run
     every few searches and lengthen their tails. `gc_heap_floor` (64 MiB) lets the
     heap grow to that size before a cycle starts; past twice the live heap, `GOGC` is
     the default. Expect the heap to sit near the floor even when idle.
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
  Document bodies are stored compressed (zstd) once the cluster has turned compression on
  (see [upgrading to compressed bodies](#upgrades-and-rolling-restarts)): on the benchmark's product listings
  (about 870 bytes of JSON each) `sl_documents` takes about 0.8 GB per million documents
  on Postgres, against 1 GB uncompressed.
- **Changelog.** `sl_changes` holds the changes not yet pruned: at most
  `changelog_retention` (24 h) of writes, usually far less. The leader prunes every 30 s,
  below what every live copy has flushed, so under a steady stream of writes it holds
  three to four `flush_interval`s of them. Pruning follows the slowest live copy, so a
  stalled copy holds the floor for at most `prune_stall_timeout`. Each retained recovery
  bundle holds it too, at its seq. Its payloads are compressed like the bodies. A pruned
  row's space is reused by later writes (SQLite's free pages, Postgres's vacuum), so the
  database grows to the documents plus the changelog's high-water mark.
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
  failover, the nodes reconnect with backoff. While it is unreachable, writes get 503
  and reads serve from the segments, marked `stale`, and readiness turns false after
  `max_lag`.
- **A failover is transparent only without lost commits.** Use synchronous replication
  (Postgres `synchronous_commit` with `synchronous_standby_names`; MySQL
  semi-synchronous replication waiting `AFTER_SYNC`). A failover that may have lost
  commits, as async replication can, is a restore to an earlier point: the lost writes
  are gone, and new writes reuse their seqs. Running copies detect it at their next
  poll and rebuild from the database by themselves (the log line `the changelog no
  longer holds what this copy applied`, and `searchlight_changelog_rewinds_total`), so
  their shards serve again once the rebuilds finish. Nodes stopped across the failover
  need nothing either while their copies still hold the changelog's prune floor: each
  copy's manifest keeps what it applied of its own shard and the marks of the changes it
  applied last, which the copy checks when it starts. A stopped node's copy holds the
  floor until the node is found dead or the copy's lease runs out (seconds), or, when a
  clean stop retired it, for `retiring_retention` (15 min) after its lease ran out; and
  changes older than `changelog_retention` are pruned whatever copy needs them. A node
  back later may find its shards pruned without it: a copy pruned past its seq rebuilds,
  but in one pruned short of it, a rewind whose reissued changes of the copy's shard
  were all pruned may go unseen. (A copy whose last flush
  predates these marks has none until it flushes again, and trusts what it reads at its
  first poll: only once, at the upgrade.)

## Upgrades and rolling restarts

Read the new release's notes in [CHANGELOG.md](../CHANGELOG.md) first: they say whether
it migrates the schema and what else it asks of an upgrade. Then restart or upgrade
**one node at a time** (for the Compose samples, see [Upgrading](#docker-compose)):

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
   4. The node stops its remaining copies' tailers, all together, each writing its
      final manifest and applied seq; then it marks the copies `retiring` in the
      registry, closes them and deregisters. It has until T/10 before the deadline,
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
  are three: the initial schema, compressed bodies, and change nonces (both below). Each
  migration's DDL waits at most 1 s for its table's lock (Postgres `lock_timeout`, MySQL
  `lock_wait_timeout`), so a long query on the table holds the table's other queries
  behind the migration for at most that second; one that times out is retried, up to 40
  times over about three minutes, half a second to five seconds apart.
- **Upgrading to a release that requires `cluster_token`.** A node on Postgres or MySQL
  now refuses to start without `cluster_token` (`cluster_token: set cluster_token ...`),
  unless `insecure_no_auth` is set. A cluster of several nodes has one already: without
  it the peer API was closed, and reads of another node's shards failed. **A single node
  on Postgres or MySQL may not**: set `cluster_token` (any shared secret, through
  `SEARCHLIGHT_CLUSTER_TOKEN` or `SEARCHLIGHT_CLUSTER_TOKEN_FILE`) before you upgrade
  it. Otherwise the upgraded pod crash-loops: an outage under a Recreate strategy or a
  single replica, and a stalled rollout under RollingUpdate.
- **Upgrading to compressed bodies** (schema migration 2) is a rolling upgrade. The
  migration only adds columns and a table, which takes milliseconds whatever the
  database holds: `body_z` beside `sl_documents.body` and `payload_z` beside
  `sl_changes.payload` (bytes, NULL until used), `sl_nodes.body_codecs` (0 by default),
  and `sl_features`. Nothing is rewritten, and nodes still on the old binary keep reading
  and writing as before: they never touch the new columns.
  - Upgraded nodes heartbeat `body_codecs = 1` and keep writing bodies as JSON text until
    the cluster turns compression on. The leader does that once every node in `sl_nodes`
    reads compressed bodies, or has been silent for longer than `dead_after` plus
    `lease_ttl` (its leases have run out, so it serves nothing): it writes the row
    `zstd_bodies` into `sl_features`, for good. Each node sees it at its next registry
    read and from then on stores document bodies and changelog payloads compressed (zstd,
    behind a codec byte, in `body_z` and `payload_z` with the text column empty). Rows
    written before keep their JSON text and read as they always did. On a single node,
    compression comes on within a second of the upgraded node starting.
  - `SELECT * FROM sl_features` shows whether it is on, and since when.
  - Once the first node has migrated, an old binary refuses to start (the schema is newer
    than it knows), so **an old pod that crashes or restarts mid-rollout crash-loops** on
    `store: database schema is newer than this binary` until it runs the new image:
    finish the rollout rather than waiting on it. For the same reason a rollback means
    restoring the database, as above.
  - **An old pod must be gone, not merely silent**, by the time `dead_after` plus
    `lease_ttl` has passed since it last heartbeated. The fence counts a silent node as
    gone; one that was only cut off or frozen (a network partition, a paused VM) and
    comes back without restarting still runs the old binary, claims copies again, and
    reads compressed rows as empty bodies (its realtime `GET`s answer empty documents,
    and its copies halt). Terminate old pods during the rollout rather than isolating
    them; once restarted, an old binary refuses the schema, as above.
- **Upgrading to change nonces** (schema migration 3) is a rolling upgrade. The migration
  only adds a column, which takes milliseconds whatever the database holds:
  `sl_changes.nonce` (a BIGINT, NULL until used). Nothing is rewritten, and nodes still
  on the old binary keep reading and writing as before: they never touch the new column,
  and the changes they write have no nonce.
  - Upgraded nodes give every change they write a random nonce. A copy checking that the
    changelog still holds what it applied compares the change's seq, index, shard, kind,
    id and commit time, as before, and, when both sides have one, the nonce too. So a
    change a failover lost, reissued to the same document in the same millisecond, is
    told apart from the one that took its seq.
  - Once the first node has migrated, an old binary refuses to start (the schema is newer
    than it knows), so **an old pod that crashes or restarts mid-rollout crash-loops** on
    `store: database schema is newer than this binary` until it runs the new image:
    finish the rollout rather than waiting on it. For the same reason a rollback means
    restoring the database, as above.
- **Upgrading to point fields** (schema migration 4, segment format 4.1) is a rolling
  upgrade. The migration only adds a column, `sl_nodes.mapping_types` (an INTEGER,
  0 for a node that has not heartbeated since), in which each node records the
  mapping field types it reads; nothing is rewritten. As with migrations 2 and 3, an
  old binary refuses to start once the first node has migrated, so finish the rollout.
  - Creating an index with a [point field](api.md#point-fields), or patching one into a
    mapping, is refused (409, naming the nodes) while any node that reads no point type
    heartbeats within the fence (`dead_after` plus `lease_ttl`) or holds a copy under a
    live lease: such a node could not decode the mapping, so its copies would halt at
    the change and its recoveries would fail. Retry once every node runs the new
    binary.
  - **Once a point field exists, no node can be downgraded below this release**: an
    older binary cannot decode the mapping (its copies halt, and it cannot open a
    manifest or recovery bundle that carries it), and a point field cannot be removed.
    Drop the index (or restore the database) before a rollback.
  - Segments without a point field are still written as format 4.0, byte for byte.
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
| Search | `searchlight_search_phase_duration_seconds` (plan, execute, reduce, fetch), `searchlight_search_segments_touched`, `searchlight_search_filter_cache_lookups_total` (hit, miss), `searchlight_search_documents_scanned_total`, `searchlight_search_documents_matched_total`, `searchlight_search_ordinals_builds_total` (by kind: full, incremental, unstarted; trigger: search, warm; result: done, cancelled, superseded, shutdown, panic), `searchlight_search_ordinals_build_duration_seconds`, `searchlight_search_ordinals_lookups_total` (hit, shared, miss, queued, stale, absent) |
| Request cache | `searchlight_search_request_cache_lookups_total` (by `store`, memory or redis, and `result`: hit, miss, or skipped while Redis is skipped), `searchlight_search_request_cache_errors_total` (by store and operation: get, set, encode, decode), `searchlight_search_request_cache_evictions_total`, `searchlight_search_request_cache_stored_bytes_total` (written to Redis), `searchlight_search_request_cache_writes_dropped_total` (by reason: busy, store_failing, closed), `searchlight_search_request_cache_store_open_ratio` (1 while Redis is skipped), `searchlight_search_request_cache_size_bytes`, `searchlight_search_request_cache_entries` |
| Percolation | `searchlight_percolate_duration_seconds` (probe, verify), `searchlight_percolate_candidates`, `searchlight_percolate_verifications_total`, `searchlight_percolate_always_check` |
| Indexing | `searchlight_index_changes_total`, `searchlight_store_group_commit_batch_size`, `searchlight_replica_apply_batch_size`, `searchlight_replica_apply_duration_seconds`, `searchlight_replica_backpressure_total` |
| Refresh, flush and merge | `searchlight_shard_open_failures_total`, `searchlight_shard_refresh_duration_seconds`, `searchlight_shard_refresh_failures_total`, `searchlight_shard_flush_duration_seconds`, `searchlight_shard_flush_failures_total`, `searchlight_shard_merge_duration_seconds`, `searchlight_shard_merge_bytes_total`, `searchlight_shard_merge_backlog`, `searchlight_shard_merge_failures_total`, `searchlight_shard_segments`, `searchlight_shard_buffer_documents` |
| Size | `searchlight_shard_documents`, `searchlight_shard_terms`, `searchlight_shard_disk_size_bytes`, `searchlight_shard_mmap_resident_bytes` |
| Replication | `searchlight_replica_lag_seq`, `searchlight_replica_lag_time_seconds`, `searchlight_replica_halted_ratio`, `searchlight_replica_halts_total`, `searchlight_replica_poll_failing_ratio`, `searchlight_replica_recovery_progress_ratio`, `searchlight_replica_recoveries_total`, `searchlight_replica_recovery_duration_seconds`, `searchlight_replica_recovery_bytes_total`, `searchlight_replica_watch_reconnects_total`, `searchlight_changelog_rewinds_total` |
| Cluster | `searchlight_cluster_nodes`, `searchlight_cluster_lease_changes_total` (by kind: `claim`, `renew`, `renew_failed`, `reclaim`, `lapse`, `resume`, `lost`, `release`), `searchlight_cluster_allocation_changes_total`, `searchlight_cluster_peer_request_duration_seconds`, `searchlight_cluster_read_retries_total`, `searchlight_cluster_read_rereads_total` (by op and result: `found`, `none`, `timeout`) |
| Database | `searchlight_store_operation_duration_seconds` (by operation, dialect), `searchlight_store_errors_total`, `searchlight_store_wal_size_bytes` (SQLite's write-ahead log, and the part no checkpoint has copied yet, `pending="true"`) |
| Runtime | `go_*` and `process_*` |

Counters and histograms with no observations yet do not appear until their first one.

### Suggested alerts

| Alert | Expression (PromQL sketch) | Why |
|---|---|---|
| Cluster red | `GET /_cluster/health` reports `red` (probe it, or alert on the next row) | a shard has no serving copy: its reads fail |
| Copy halted | `max by (index, shard) (searchlight_replica_halted_ratio) > 0` for 5 m | a copy stopped at a change it cannot apply (see below) |
| Changelog rewound | `increase(searchlight_changelog_rewinds_total[10m]) > 0` | the database lost commits (a failover without synchronous replication): copies rebuild from it, and the lost writes are gone |
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

### Heap above the live set at idle

A node's heap (`go_memstats_heap_alloc_bytes`) holding tens of megabytes while its live
set (the in-use space of a heap profile, `/debug/pprof/heap`) is a few megabytes is the
[heap floor](#memory): the collector waits until the heap reaches `gc_heap_floor`
(64 MiB by default) before it starts a cycle. It is not a leak. Lower the floor, set it
to `0`, or set `GOGC`, to trade that memory back for more frequent collections and
slower search tails. Under a tight `GOMEMLIMIT` the floor is capped at a quarter of it.

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
