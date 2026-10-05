# Searchlight HTTP API

This is a guide to Searchlight's public API, with examples. The contract is
[`api/openapi.yaml`](../api/openapi.yaml) (OpenAPI 3.1). The API tests check every route
and response against it, so where this guide and the contract differ, the contract wins.
Each section below names the `operationId`s it covers.

- [Conventions](#conventions)
- [Indexes](#indexes)
- [Documents](#documents)
- [Bulk](#bulk)
- [Search and count](#search-and-count)
- [Saved queries](#saved-queries)
- [Percolation](#percolation)
- [Field catalogue](#field-catalogue)
- [Cluster and probes](#cluster-and-probes)
- [Errors](#errors)

## Conventions

- **Base URL.** Any node's public listener (`listen`, `:8780` by default) serves the
  whole API. Any node takes any request, reads and writes alike, so a plain load
  balancer in front is enough.
- **Format.** Requests and responses are JSON (`Content-Type: application/json`). `_bulk`
  is the exception: its request is NDJSON. Errors are problem JSON (see [Errors](#errors)).
- **Auth.** Send `Authorization: Bearer <token>`, with a token from `tokens_file`:
  - a `read` token may call every `GET` route, plus `_search`, `_count` and `_percolate`;
  - a `write` token may call everything.

  `/healthz` and `/readyz` need no token. A missing or unknown token gets a 401, and a
  read-only token on a write route gets a 403.
- **Names.** An index name is 1 to 255 bytes: lowercase letters, digits, `-`, `_` and
  `.`, starting with a letter or a digit. A document or saved-query id is 1 to 512
  bytes of UTF-8 without NUL. Percent-encode a `/` in an id.
- **Sequence numbers.** Every write answers with the `seq` it committed. A write is
  acknowledged only after its SQL transaction commits, so an acknowledged write is
  durable. Seqs are global, contiguous and increase in commit order.

### Consistency

| You want | Do this |
|---|---|
| The write to be searchable before the answer | `?refresh=wait_for` on the write. It returns once the write is searchable on the node that took it. |
| The written shards refreshed now | `?refresh=true`. It costs a small segment per call, so don't use it per document under load. |
| Read-your-writes on any node | Pass the write's `seq` as `?wait_for_seq=N` on the read. The read waits, under its deadline, until every change up to N is searchable on the copies it reads. |
| Nothing special | Writes become searchable everywhere within the index's `refresh_interval` (1 s by default). |

`GET` of one document or one saved query is realtime: it reads the database, not the
segments.

### Limits

| Limit | Setting | Answer |
|---|---|---|
| Request body | `max_body_bytes` (16 MiB) | 413 `too_large` |
| One document, or one saved query's body | `max_doc_bytes` (4 MiB) | 413 |
| Operations in one `_bulk` | `max_bulk_ops` (10,000) | 413 |
| Reads in progress | `search_queue` (1,000) | 429 with `Retry-After` |
| Heap of in-flight requests | `max_inflight_write_bytes`, `max_inflight_read_bytes` | 429 with `Retry-After` |
| A shard copy's write buffer full | (backpressure) | 429 with `Retry-After` |
| Request deadline | `request_timeout` (30 s) | A search answers what it found with `timed_out: true`; a wait gives 504 |
| Query size | 4 levels of `all`/`any`, 50 conditions, 100 nodes, strings of 500 characters, lists of 200 | 400 with `loc` |

## Indexes

`listIndexes`, `createIndex`, `getIndex`, `deleteIndex`, `patchMapping`, `patchSettings`.

```sh
curl -sS -X PUT localhost:8780/indexes/products -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{
    "mapping": {
      "dynamic": "strict",
      "fields": {"title": "text", "brand": "keyword", "tags": "keyword_list",
                 "price": "number", "in_stock": "bool", "seen_at": "date"}
    },
    "settings": {"shards": 2, "refresh_interval": "1s"}
  }'
```

- **Answer.** 201 with the `IndexInfo`: `name`, `uid` (this incarnation of the index),
  `version`, `created_at`, `mapping`, `settings`, and `docs` and `queries` (the live
  counts searchable on this node). An existing name is a 409 `index_exists`.
- **Field types.** `keyword`, `text`, `keyword_list`, `number`, `bool` and `date` (Unix
  milliseconds). The spec's §3 has the operators and aggregations each type supports.
- **`dynamic`.**
  - `true`, the default, types a new field from its first value.
  - `false` stores a new field but does not index it.
  - `"strict"` refuses a document with an unmapped field.
- **`settings`.**
  - `shards` is fixed at creation (1 by default, at most 1,024).
  - `replicas_per_shard` is the target number of copies of each shard. 0, the
    default, means every node holds every shard.
  - `refresh_interval` is a duration, or `-1` to refresh only on demand.

`GET /indexes` lists every index, and `GET /indexes/{index}` describes one.
`DELETE /indexes/{index}` drops the index with its documents and saved queries and
answers `{"acknowledged": true}`.

Mappings are additive. `PATCH /indexes/{index}/mapping` with `{"fields": {...}}` adds
fields. Retyping a field is a 400, as is passing `max_index_fields`.

`PATCH /indexes/{index}/settings` changes `refresh_interval`, which applies at once, or
`replicas_per_shard`. The allocator then places or releases copies to meet the new
target.

## Documents

`putDocument`, `getDocument`, `deleteDocument`.

```sh
curl -sS -X PUT 'localhost:8780/indexes/products/docs/sku-1?refresh=wait_for' \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"title": "Oak chair", "brand": "Acme", "tags": ["wood", "chair"], "price": 30}'
# {"id":"sku-1","seq":42}

curl -sS localhost:8780/indexes/products/docs/sku-1 -H "Authorization: Bearer $TOKEN"
# {"id":"sku-1","seq":42,"body":{"title":"Oak chair",...}}
```

- **`if_seq=N`.** Applies only if the document's current seq is N. Otherwise the answer
  is a 409 `conflict` carrying `current_seq`, which gives optimistic concurrency.
- **`op_type=create`.** Applies only if the document does not exist yet (409
  otherwise).
- **`refresh`.** `true`, `wait_for` or `false` (see [Consistency](#consistency)).
- **Deletes.** A delete answers `{"id", "seq"}`. Deleting a document that does not exist
  is a 404 `document_not_found` with `result: "not_found"`, and writes nothing.
- **Stale reads.** While the database cannot be reached, `GET` answers from this node's
  copy with `stale: true` and no `seq`.

## Bulk

`bulk`: `POST /indexes/{index}/_bulk`. Every operation in one request commits in one
transaction.

```sh
cat > ops.ndjson <<'EOF'
{"upsert": {"id": "sku-2"}}
{"title": "Pine table", "brand": "Initech", "price": 120}
{"create": {"id": "sku-3"}}
{"title": "Desk lamp", "brand": "Acme", "price": 8}
{"delete": {"id": "sku-9", "if_seq": 12}}
EOF
curl -sS -X POST 'localhost:8780/indexes/products/_bulk?percolate=true' \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/x-ndjson' \
  --data-binary @ops.ndjson
```

```json
{"took_ms": 4, "seq": 57, "errors": true, "timed_out": false, "percolated": true,
 "items": [
   {"op": "upsert", "id": "sku-2", "status": 200, "seq": 55, "queries": []},
   {"op": "upsert", "id": "sku-3", "status": 200, "seq": 56, "queries": ["cheap"]},
   {"op": "delete", "id": "sku-9", "status": 409, "error": {"code": "conflict", "current_seq": 30, "...": "..."}}
 ]}
```

- **Actions.** Each operation is an action line, `upsert`, `create` or `delete`,
  followed by the document for an upsert or a create. `index` is an alias of `upsert`,
  and `_id` of `id`.
- **Partial failure.** An operation refused on its own (an invalid document, a failed
  `if_seq`) is reported in its item, with `errors` set, and the rest commit.
- **Whole-request refusal.** A malformed action line, a line over `max_doc_bytes`, or
  more than `max_bulk_ops` operations refuse the whole request before anything is
  written.
- **`seq`.** The newest seq the request committed. Pass it as `wait_for_seq` to read
  your writes.
- **`percolate=true`.** Each upserted document's item lists the saved queries it matches.
  This is how scrape-bot's diff engine works out its match edges (spec §16).
- **`refresh`.** As on single-document writes.

## Search and count

`search`: `POST /indexes/{index}/_search`, and `count`: `POST /indexes/{index}/_count`.

```sh
curl -sS -X POST 'localhost:8780/indexes/products/_search?wait_for_seq=57' \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' -d '{
    "query": {"all": [
      {"field": "brand", "op": "in", "value": ["acme", "initech"]},
      {"field": "price", "op": "between", "value": [5, 100]},
      {"not": {"field": "tags", "op": "has", "value": "discontinued"}}
    ]},
    "sort": [{"price": "desc"}],
    "size": 20,
    "track_total": true,
    "fields": ["title", "price"],
    "aggs": {
      "brands": {"terms": {"field": "brand", "size": 10}},
      "price": {"stats": {"field": "price"}},
      "bands": {"range": {"field": "price", "ranges": [{"to": 10}, {"from": 10, "to": 100}, {"from": 100}]}}
    }
  }'
```

```json
{"took_ms": 3, "timed_out": false,
 "total": {"value": 2, "relation": "eq"},
 "hits": [{"id": "sku-1", "sort": [30, "sku-1"], "body": {"title": "Oak chair", "price": 30}},
          {"id": "sku-3", "sort": [8, "sku-3"], "body": {"title": "Desk lamp", "price": 8}}],
 "next": null,
 "aggs": {"brands": {"buckets": [{"key": "acme", "doc_count": 2}], "...": "..."},
          "price": {"count": 2, "min": 8, "max": 30, "avg": 19, "sum": 38},
          "bands": {"buckets": ["..."]}}}
```

- **The query language** is a tree of `all`, `any`, `not` and conditions
  `{"field", "op", "value"}`. `{"all": []}`, the default, matches every document, and
  `_id` refers to the document id. The spec's §4 lists every operator.
  - Matching is boolean. There is no relevance scoring.
  - Text compares normalized, so `"acme"` matches `"ACME"`. This is byte-for-byte
    compatible with scrape-bot.
- **`sort`** takes field names, `{"field": "asc"|"desc"}` or
  `{"field": {"order": "desc"}}`. `_id` breaks ties.
- **Paging.** `size` is 10 by default, at most 10,000. To page, pass the previous
  response's `next` as `search_after`. `next` is null on a short page. There is no
  `from`.
- **`track_total`.**
  - `true` counts exactly.
  - `false` counts no further than the hits.
  - A number counts exactly up to it (10,000 by default). Past it, `relation` is
    `gte`.
- **Aggregations** (`aggs`, or `aggregations`):
  - `terms`, with `size`, `shard_size` and `min_doc_count`;
  - `range`, with `ranges` of `from`/`to`/`key`;
  - `histogram`, with `interval`, `offset` and `min_doc_count`;
  - `date_histogram`, with `fixed_interval`, `calendar_interval`, `offset` and
    `min_doc_count`;
  - `stats`;
  - `cardinality` (HyperLogLog++).

  A bucket aggregation may carry metric sub-aggregations one level deep. A `terms`
  aggregation over several shards reports its error bound, as Elasticsearch does.
- **`fields`** limits each hit's `body` to those members.
- **`timeout`** (`"250ms"`, or milliseconds) caps this search below `request_timeout`.
  Past it, the answer holds what was found, with `timed_out: true`.

`_count` takes `{"query": ...}` and answers `{"count", "relation", "timed_out"}`, counting
exactly.

## Saved queries

`putQuery`, `getQuery`, `deleteQuery`, `listQueries`.

```sh
curl -sS -X PUT localhost:8780/indexes/products/queries/cheap \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"query": {"field": "price", "op": "lt", "value": 10}, "meta": {"search_id": 7}}'
# {"id":"cheap","seq":58}

curl -sS 'localhost:8780/indexes/products/queries?size=100' -H "Authorization: Bearer $TOKEN"
# {"queries":[{"id":"cheap","seq":58,"query":{...},"meta":{"search_id":7}}],"next":null}
```

- **Validation.** A saved query is validated against the index's mapping, so an
  unmapped field or an operator its type does not take is a 400 with a `loc` such as
  `query.field`.
- **`meta`** is an opaque object of at most 16 KiB, returned as given. scrape-bot keeps
  its search id there.
- **Writes** take the same `if_seq`, `op_type` and `refresh` parameters as documents.
- **Listing.** `GET .../queries` pages by id. Pass `next` as `after`.

## Percolation

`percolate`: `POST /indexes/{index}/_percolate`. It answers which saved queries each
document matches, for given documents (`docs`), stored ones (`ids`), or both.

```sh
curl -sS -X POST 'localhost:8780/indexes/products/_percolate?wait_for_seq=58' \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"docs": [{"brand": "Acme", "price": 5}], "ids": ["sku-1", "missing"]}'
```

```json
{"took_ms": 1, "results": [
  {"found": true, "queries": ["cheap"]},
  {"id": "sku-1", "found": true, "queries": []},
  {"id": "missing", "found": false, "queries": []}]}
```

- **Order.** Results list the given documents first, then the ids, each in request
  order.
- **Analysis.** Given documents are analyzed under the index's mapping.
- **Stored documents** are read from this node's copies, as a search reads them, so
  pass `wait_for_seq` to see a recent write.
- **Limits.** Up to 10,000 of each per request.

## Field catalogue

`fields`: `GET /indexes/{index}/_fields?entries=N`. It describes the index's fields from
its mapping, and with `entries` > 0, each `keyword_list` field's N most frequent entries
with their counts (at most 1,000). scrape-bot's catalogue page calls it.

```json
{"dynamic": "strict", "fields": [
  {"name": "brand", "type": "keyword"},
  {"name": "tags", "type": "keyword_list", "entries": [{"value": "wood", "count": 41}]}]}
```

## Cluster and probes

`clusterHealth`, `clusterNodes`, `clusterShards`, `healthz`, `readyz`, `metrics`.

- **`GET /_cluster/health`**:
  `{"status", "nodes", "indexes", "shards", "serving_shards", "unassigned"}`. The status
  has Elasticsearch's meaning:
  - `green`: every copy serves;
  - `yellow`: a shard is below its copy target;
  - `red`: a shard has no serving copy.
- **`GET /_cluster/nodes`**: the live nodes, with `id`, `address`, `version` and `self`.
- **`GET /_cluster/shards`**: every shard copy, with:
  - `node` and `state`: `serving`, `recovering`, `halted` or `retiring`;
  - `applied_seq`, `refreshed_seq`, `committed_seq` and `lag`;
  - `docs`;
  - `error` for a halted copy;
  - `stale` and `rebuilding`.

  It is the operator's view (see [operations](operations.md#troubleshooting)).
- **`GET /healthz`** is liveness: 200 while the process serves.
- **`GET /readyz`** is readiness. It answers 503, with the reason in `detail`:
  - while the node has not joined the cluster yet;
  - while a shard copy has not finished its startup recovery;
  - while the database has not answered for longer than `max_lag`;
  - while the node drains for shutdown.
- **`GET /metrics`** is the Prometheus exposition. It needs a token here.
- **The admin listener** (`admin_listen`: `127.0.0.1:8781` by default, `:8781` in the
  container image) serves the same metrics without a token, along with `/healthz`,
  `/readyz` and pprof. Keep it private.

## Errors

Every error is RFC 9457 problem JSON (`application/problem+json`):

```json
{"type": "urn:searchlight:problem:invalid_request", "title": "Bad Request", "status": 400,
 "code": "invalid_request", "detail": "the query is invalid", "request_id": "5f0c2d9e1a7b3c44",
 "problems": [{"loc": "query.all.2.any.0.value", "message": "..."}]}
```

- **`code`** is stable and machine-readable:
  - `invalid_request`, `unauthorized` and `forbidden`;
  - `not_found`, `index_not_found`, `document_not_found` and `query_not_found`;
  - `method_not_allowed`, `index_exists` and `conflict`;
  - `too_large` and `too_many_requests`;
  - `timeout`, `request_timeout` and `client_closed_request`;
  - `unavailable` and `internal`.
- **`problems[].loc`** is a dotted path into the request, for example
  `query.all.2.any.0.value`, `params.wait_for_seq` or `line.3.upsert.id`.
- **`request_id`** echoes `X-Request-Id`, or a fresh id when the client sent none. Logs
  carry it too.
- **Retries.** 429 and 503 carry `Retry-After`. Both are safe to retry. A write that got
  a 5xx may or may not have committed, so retry it with `if_seq` or `op_type=create`
  when that matters.
