# nginx in front of a cluster

The spec's bar for "plug and play" is that "a dumb load balancer in front is enough;
clients never need to know the topology" (spec §1) — because every node is a
coordinator (spec §2), there is no separate role to route requests to. This is how to
make nginx that load balancer, with a working configuration at
[`deploy/nginx/searchlight.conf`](../deploy/nginx/searchlight.conf) and an nginx
service added to [`deploy/compose.yml`](../deploy/compose.yml). For how the cluster
itself behaves, see [clustering.md](clustering.md); for every node setting, see
[operations.md](operations.md).

- [Upstream and keepalive](#upstream-and-keepalive)
- [Health](#health)
- [Retries](#retries)
- [Limits and timeouts](#limits-and-timeouts)
- [TLS](#tls)
- [Internal peer traffic](#internal-peer-traffic)
- [Compose](#compose)
- [Kubernetes](#kubernetes)

## Upstream and keepalive

```nginx
upstream searchlight_nodes {
    least_conn;
    server searchlight-1:8780 max_fails=3 fail_timeout=10s;
    server searchlight-2:8780 max_fails=3 fail_timeout=10s;
    server searchlight-3:8780 max_fails=3 fail_timeout=10s;
    keepalive 32;
}
```

- **`least_conn`** sends each request to whichever node has the fewest open
  connections right now. There's no reason to hash on anything (no session affinity,
  no shard-aware routing): any node answers any request, so the only thing worth
  balancing on is current load.
- **`keepalive 32`** keeps up to 32 idle connections per worker open to the upstreams,
  so repeated requests don't pay a new TCP (and TLS, if you terminate it twice)
  handshake each time. It needs `proxy_http_version 1.1;` and
  `proxy_set_header Connection "";` in the `location` block, both in the shipped conf.
- List every node's `advertise_address` host and its public `listen` port (8780 by
  default) — not the admin port, see [below](#the-admin-listener-stays-off-the-proxy).

## Health

**Open-source nginx only has passive health checks**: `max_fails`/`fail_timeout` on
each `server` line mark a node temporarily down after that many failed proxy attempts,
for `fail_timeout`, with no active probing of `/readyz` in between. Combined with
Searchlight's own shutdown sequence, that is still enough for clean rolling restarts,
without nginx ever polling anything itself:

1. On SIGTERM, a node turns its own readiness false immediately, then keeps serving
   for `shutdown_grace` (operations.md's
   [Upgrades and rolling restarts](operations.md#upgrades-and-rolling-restarts)).
   `/readyz` going false doesn't, by itself, stop nginx from sending that node
   requests — nginx never looked at `/readyz`. What stops it is that **the node keeps
   answering normally during `shutdown_grace`**: it just happens to be in the process
   of retiring the shard copies another node can now serve, so a request landing
   there during the grace period still gets a correct answer (routed to a shard this
   node still serves, or passed to a peer).
2. Only once the listener actually stops accepting (after `shutdown_grace`) does an
   in-flight or new connection to that node start failing — and that's what
   `max_fails`/`fail_timeout` reacts to: a handful of failed proxy attempts within
   `fail_timeout` take the node out of rotation for the next `fail_timeout`, by which
   point the node has usually finished stopping (or come back up, on a restart).
3. Set `fail_timeout` to a few seconds, not a few hundred milliseconds: it needs to
   outlast the gap between "nginx's first failed attempt" and "the node's listener is
   fully closed", but not so long that a node that comes back quickly (a restart, not
   a removal) is kept out of rotation after it's ready again.

**Active health checks** (actually polling `/readyz` on a schedule, independent of
real traffic) need either a commercial proxy or a different open-source one:
**nginx Plus** has `health_check` directives for this out of the box; **HAProxy** and
**Envoy** are open-source alternatives whose active health-check support is
considerably more capable than open-source nginx's passive-only model, if you'd
rather not pair nginx with Searchlight's own readiness probe the way Kubernetes does
(see [Kubernetes](#kubernetes)).

## Retries

```nginx
proxy_next_upstream error timeout http_502 http_503 non_idempotent;
proxy_next_upstream_tries 3;
proxy_next_upstream_timeout 10s;
```

(All of `proxy_next_upstream`'s conditions have to be on the one directive: nginx
takes the last occurrence of a directive in a context, not a merge of several, so
writing `non_idempotent` on a line of its own would silently drop `error timeout
http_502 http_503` rather than add to them.)

- **What `error timeout http_502 http_503` catches.** `error` and `timeout` are
  connection-level: nginx couldn't reach the node, or it didn't answer in time. `502`
  is nginx's own code for a failed connection to the upstream — Searchlight never
  answers 502 itself (there is no code path that constructs one;
  `internal/api/problems.go`'s `ProblemFor` maps every internal error to 500, 503 or
  504, never 502). `503` is Searchlight's own `unavailable`: the database can't be
  reached, or the node is draining for shutdown — both of which another node can
  probably answer right now. `504` (the node's own `request_timeout` passed) and `500`
  are deliberately **not** in the list: a 504 means the *request* was slow or stuck
  (a new node would just spend its own `request_timeout` on it too), and a 500 is a
  server-side bug, not a reason to think another node will do better.
- **Is retrying a write safe?** This is the one place "a dumb load balancer in front
  is enough" needs checking against the code, because nginx's default is to never
  retry a POST on its own (PUT and DELETE, the single-document and single-query
  write methods, are already classed as idempotent and retried without
  `non_idempotent` at all). The answer here is yes, enable it, for one reason the code
  confirms directly: **every id in Searchlight is client-supplied.** There is no
  endpoint that writes a document under a server-generated id — `PUT
  /indexes/{i}/docs/{id}` takes the id in the path, and every `_bulk` action line
  requires an `id` or `_id` (`internal/api/bulk.go`'s `parseAction` returns
  `InvalidAt(loc+".id", "an action needs an id")` when neither is given). That rules
  out the failure mode that makes retrying Elasticsearch's auto-id `POST
  /index/_doc` unsafe: a retry can never produce a second document under a different
  id, because there is no "different id" a retry could get.
  - **Plain upserts** (`upsert`/`index`, no `if_seq`) are last-write-wins, retried or
    not: applying the same body to the same id twice leaves the same document at a
    newer `seq`, and nothing is duplicated. The one hazard is a retry of a write that
    did commit on the first attempt landing *after* another client's newer write to the
    same id, which it then silently replaces. That race exists without nginx too (a
    client's own retry, or nginx retrying a `PUT`, which it does by default), and
    `non_idempotent` only extends it to `_bulk` items. A client that must not lose such
    an update writes with `if_seq`, which turns the race into a 409.
  - **`op_type=create` and `if_seq`-conditioned writes** are the ones worth
    understanding, not avoiding: if the first attempt actually committed before the
    connection died, a retry of the same op fails its condition and comes back with
    Searchlight's own 409 `conflict` (`current_seq` included) — either as that item's
    own entry in the `_bulk` response, or as the single write's answer — instead of
    silently reapplying. That is the condition doing exactly its job: the retry is
    indistinguishable, from Searchlight's side, from any other concurrent write
    racing the condition. Nothing is corrupted; a client that treats "409 after a
    retry" as "maybe this already went through — check the seq" rather than as a
    plain failure gets the right answer.
  - **`_search`, `_count` and `_percolate`** are POST but read-only, so retrying them
    has no write to worry about at all.
  - **`PATCH` mapping/settings** fall under nginx's own "non-idempotent" class too
    (alongside POST), and are also safe to retry: mapping changes are additive only
    (adding a field that's already there the same way is a no-op), and a settings
    change applied twice at the same value is also a no-op.
- **`proxy_next_upstream_tries 3` and `proxy_next_upstream_timeout 10s`** bound how
  much a client's request can cost in retries: at most 3 attempts, and the whole
  sequence gives up after 10 s even if individual attempts haven't each timed out yet.
  Without a cap here, a client's own timeout, not nginx's, would be the only thing
  stopping an unbounded retry loop against a cluster where every node happens to be
  struggling at once.

## Limits and timeouts

- **`client_max_body_size`** should match the node's `max_body_bytes` (16 MiB by
  default, operations.md's [every setting](operations.md#every-setting) table).
  Smaller, and nginx itself 413s large requests before Searchlight gets to; larger
  just wastes buffer space nginx will never need, since Searchlight refuses the same
  request anyway. There's no need to additionally size for `max_doc_bytes` or
  `max_bulk_ops` at the proxy — those are Searchlight's own, more specific limits
  inside a body that already fits under `max_body_bytes`.
- **`proxy_read_timeout`** (and `proxy_send_timeout`) need to sit above
  `request_timeout` (30 s by default), the node's own per-request deadline — not above
  any `timeout` a client puts *inside* a search body, which is Searchlight's own
  concern and answered with `timed_out: true` rather than by hanging, and not
  specially raised for `refresh=wait_for` or `?wait_for_seq=N` either: both run under
  the same request deadline, they don't get their own budget. 35 s (`request_timeout`
  plus 5 s of margin) is the shipped default; raise both together if you raise
  `request_timeout`.
- **Buffering.** Every Searchlight response, success or problem JSON, is written in
  one call (`internal/api/server.go`'s `writeJSON`) — there is nothing streamed or
  chunked to turn `proxy_buffering off` for. Leave it on (the nginx default).

## TLS

Terminate TLS at nginx (`listen 8443 ssl;` plus `ssl_certificate`/
`ssl_certificate_key` in the shipped conf, commented out since this repo ships no
certificate) and run `deploy/compose.yml`'s nodes in plain HTTP behind it, the same
way the compose file already runs the peer API in plain HTTP on its private network
(and logs a warning that `cluster_token` crosses it in the clear — see
[clustering.md](clustering.md#postgres-sizing-and-settings) and
operations.md's [Compose](operations.md#docker-compose-a-three-node-cluster) section
for that trade-off). Client `Authorization` headers pass through nginx unchanged —
nothing in the shipped conf hides, rewrites or strips them, and nothing should: don't
add `proxy_hide_header Authorization` or a `proxy_set_header Authorization ...` of
your own.

If you'd rather have Searchlight terminate TLS itself node-to-node as well (for
example, so the peer API is encrypted even on a network you don't fully trust), set
`tls_cert`/`tls_key` (and `peer_ca_file`) on the nodes per operations.md's
[configuration table](operations.md#every-setting); nginx's own TLS termination for
client traffic is independent of that and needs no coordination with it.

## Internal peer traffic

Node-to-node traffic — recovery snapshots, push hints, peer reads under ARS — never
goes through nginx. It runs over the internal API under `/_internal/` on each node's
own public listener, dialed directly at that node's `advertise_address`
(architecture.md's [Routing](architecture.md#routing)). This is why
`advertise_address` has to be **each node's own address**, never nginx's: if it
pointed at the load balancer, a peer's "ask node B for its snapshot" call would land
on whichever node nginx picked that moment, not necessarily B, and recovery and
routing would both break. `deploy/compose.yml` already sets this correctly — each
node's `SEARCHLIGHT_ADVERTISE_ADDRESS` is its own Compose service name
(`searchlight-1:8780`, and so on), not the nginx service's.

### The admin listener stays off the proxy

Each node serves `/healthz` and `/readyz` on both listeners, without auth, and
`/metrics` on both (the public one requires a read token). pprof is only on the admin
listener (`127.0.0.1:8781` by default, loopback-only), and nothing outside the host or
pod should reach that listener: the shipped conf has no `location` or `upstream` entry
that points at 8781. Through nginx, `/healthz` and `/readyz` answer for whichever node
`least_conn` picked, which is what the Compose nginx healthcheck relies on; per-node
health and scraping go to each node directly, as `deploy/compose.yml`'s node
healthchecks and the Kubernetes probes do (operations.md's
[port table](operations.md#deploy)).

## Compose

`deploy/compose.yml` adds an `nginx` service as the front door: it publishes one port
(`8080`) and mounts `deploy/nginx/searchlight.conf` read-only, following the file's
existing conventions (an image pinned by digest, a healthcheck, `depends_on` on the
nodes' own healthchecks). Bring the cluster up exactly as before and go through nginx
instead of a single node's published port:

```sh
docker compose -f deploy/compose.yml up --build -d
curl -H "Authorization: Bearer <write token>" http://127.0.0.1:8080/_cluster/health
```

The three nodes' own ports (`8780`/`8782`/`8784` and their admin ports) stay published
too, for the same local debugging operations.md already describes — nginx is an
additional way in, not a replacement for them.

Validate the compose file with `docker compose -f deploy/compose.yml config --quiet`
and the nginx conf itself with:

```sh
docker run --rm -v "$PWD/deploy/nginx/searchlight.conf:/etc/nginx/conf.d/searchlight.conf:ro" \
  nginx:1.27-alpine nginx -t
```

CI's "image, compose and Kubernetes manifests" job (`.github/workflows/ci.yml`)
already runs `docker compose ... config --quiet` on every PR and brings the whole
compose cluster up, so it validates both the compose file and (by starting the nginx
container from it) the conf syntax on every change; run the two commands above
locally first if you have Docker running.

## Kubernetes

The Kubernetes example doesn't need nginx at all: the `searchlight` Service
(`deploy/k8s/services.yaml`) already load-balances across every ready pod the same
way `least_conn` does here, using whatever Kubernetes' own `kube-proxy` or CNI
provides, and pod readiness (backed by the node's own `/readyz`) is already wired
into it (operations.md's [Kubernetes](operations.md#kubernetes) section). Put
ingress-nginx (or any ingress controller) in front only if you need a single
public hostname, TLS termination at the edge, or to share one load balancer across
several services. Size it the same way as the compose config above:

```yaml
metadata:
  annotations:
    nginx.ingress.kubernetes.io/proxy-body-size: "16m"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "35"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "35"
```
