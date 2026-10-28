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
| The write to be searchable before the answer | `?refresh=wait_for` on the write. It returns once the write is searchable on the node that took it, or, on a node that hosts no copy, the first copy that has it. |
| The written shards refreshed now | `?refresh=true`. It costs a small segment per call, so don't use it per document under load. |
| Read-your-writes on any node | Pass the write's `seq` as `?wait_for_seq=N` on the read. The read waits, under its deadline, until every change up to N is searchable on the copies it reads. An N past the newest committed seq is a 400 `invalid_request`. While the database cannot be reached, an N past the newest seq the node knows cannot be confirmed: it is a 503 `unavailable` with `Retry-After`. |
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
| Heap of in-flight requests (a search with `from` also reserves about 1 KiB per hit of `from + size`) | `max_inflight_write_bytes`, `max_inflight_read_bytes` | 429 with `Retry-After` |
| A shard copy's write buffer full | (backpressure) | 429 with `Retry-After` |
| Request deadline | `request_timeout` (30 s) | A search answers what it found with `timed_out: true`; a wait gives 504 |
| Query size | 4 levels of `all`/`any`/`nested`, 50 conditions, 100 nodes, strings of 500 characters, lists of 200 | 400 with `loc` |
| Members of a [nested field](#nested-fields) in one document | 10,000 | 400 |
| A nested node's `inner_hits` | `size` 1,000, `fields` 100; 10,000 inner hit objects per response (`size` × the `inner_hits` sizes) | 400 with `loc` |
| A search's `from + size` | `max_result_window` (10,000) | 400 with `loc` `from` (`size` when `from` is 0): page deeper with `search_after` |
| Sort keys in a search | 16, each field once, none after `_id` | 400 with `loc` |
| Aggregations in a search | 100, sub-aggregations included | 400 with `loc` `aggs` |
| Ranges in a `range` aggregation | 100 | 400 with `loc` |
| A `terms` `order` | 8 keys, each once, none after `_key` | 400 with `loc` |
| A `top_metrics` | `size` 10, 10 `metrics` | 400 with `loc` |
| A `composite` page | `size` 10,000, 10 `sources` | 400 with `loc`: page on with `after` |
| A `terms` `include` or `exclude` | 1,000 values; a pattern of 1,000 characters, 10,000 compiled instructions | 400 with `loc` |
| `fields` in a search | 100 | 400 with `loc` `fields` |
| Buckets in a search response, every aggregation's together | 65,536 | 400 with `loc` when one shard passes it; `"truncated": true` on the aggregations cut when only the merged shards do |
| Documents in one `_percolate`, `docs` and `ids` together | 10,000 | 413 `too_large` |
| Matches in one percolation, every document's together | 1,000,000 | `_percolate`: 422 `too_many_matches`; `_bulk?percolate=true`: `percolated: false`, `percolate_reason: "too_many_matches"` |

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
  milliseconds; a document may give an RFC 3339 string or a bare `YYYY-MM-DD`, midnight
  UTC), `point` (see [Point fields](#point-fields)) and `nested` (see
  [Nested fields](#nested-fields)). The spec's §3 has the operators and aggregations
  each type supports.
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

### Point fields

A point field holds points of two coordinates, as Elasticsearch's `point` field and
Lucene's `XYPoint` do, for [box queries](#box-queries). Its type is an object naming its
axes, each `number` or `date`:

```json
{"mapping": {"fields": {
  "seen": {"type": "point", "x": "date", "y": "number"},
  "spot": {"type": "point", "x": "number", "y": "number"}}}}
```

- **Values.** A list of `[x, y]` pairs, or one pair: `{"seen": [["2026-10-16", 4.5],
  [1760000000000, 9]], "spot": [3, 4]}`. A `number` axis takes what a `number` field
  takes; a `date` axis what a `date` field takes (Unix milliseconds, an RFC 3339 string
  or a bare `YYYY-MM-DD`). A pair that is not two finite coordinates of its axes' types
  (`[1e400, 1]`, `["x", 1]`, `[1, 2, 3]`) is no point, as a non-finite number is no
  number: the field is present still (`exists` matches it), with the pairs that are.
  A list whose members are not all arrays (`[1, [2, 3]]`) is neither one pair nor a
  list of pairs, and holds no point. Repeated points count once. A value holds at most
  10,000 points: its first 10,000 valid pairs, in document order; the pairs past them
  are not read.
- **Upgrades.** An index cannot gain a point field (at creation or by a mapping patch)
  while a node of a release before point fields serves: the request is a 409 naming
  those nodes. Once one exists, no node can be downgraded below the release that added
  them ([upgrade notes](operations.md#upgrades-and-rolling-restarts)).
- **Typing.** Dynamic mapping never types a point (a list of pairs is a
  `keyword_list`): map the field first, at creation or with `PATCH .../mapping`. A
  point's axes are part of its type, so `point(number, date)` cannot become
  `point(date, number)` (400).
- **Operators.** `box` and `exists`. No aggregation takes a point field (400 at the
  aggregation's `field`), and no sort (400 at `sort.N`).
- **The field catalogue** gives a point field's `type` as the mapping writes it, the
  object.

### Nested fields

A nested field holds an array of objects, each matched apart, as Elasticsearch's
`nested` type: "size L, in stock and under 150" holds of a product only when one of its
variants is all three. It is written with its sub-fields, each of any type but a
point:

```json
{"mapping": {"fields": {
  "title": "text",
  "_v": {"type": "nested", "fields": {
    "_key": "keyword", "color": "keyword", "size": "keyword",
    "price": "number", "regular_price": "number", "in_stock": "bool"}}}}}
```

- **Names.** A sub-field is named by its full name, the path, a dot and its own name:
  `_v.size` in queries, aggregations, sorts, `inner_hits` and the field catalogue (which
  lists `_v` as `nested`, then each sub-field). A nested field's name holds no dot, and
  no other field takes a name under it: a mapping giving `_v.size` beside a nested
  `_v` is a 400 at `mapping.fields._v.size`, one making a field nested over names
  mapped under it a 400 too.
- **Growing.** `PATCH .../mapping` with `{"fields": {"_v": {"type": "nested", "fields":
  {"sku": "keyword"}}}}` adds sub-fields (so does `{"fields": {"_v.sku": "keyword"}}`,
  naming one by its full name); retyping one is a 400, as retyping a field
  is. Dynamic typing never makes a field nested (an array of objects is a
  `keyword_list` of nothing): map it first. Under `dynamic: true` a new sub-field is
  typed from its first value, as a new field is; `false` keeps it unindexed, and
  `"strict"` refuses the document, naming `_v.<name>`.
- **Values.** An array of objects, or one object alone: `{"_v": [{"_key": "SKU-1",
  "size": "L", "price": 12999, "in_stock": true}, {"_key": "SKU-2", ...}]}`. Each
  object's members are read as a document's are (a member's value by its sub-field's
  type, `null` as absent). A member that is no object (`"x"`, `null`, `3`) keeps its
  place in the array, for `inner_hits` offsets, and matches nothing. At most 10,000
  members per document (a 400 past it). A top-level member named under a nested field
  (`"_v.size": "M"`) is kept in the body but never indexed: that name is the
  sub-field's.
- **Upgrades.** An index cannot gain a nested field, or a sub-field, while a node of a
  release before nested fields serves: a 409 naming those nodes, as for point fields.
  Once one exists, no node can be downgraded below 1.4
  ([upgrade notes](operations.md#upgrades-and-rolling-restarts)).

**Nested queries.** `{"nested": {"path": "_v", "query": node}}` holds when one object of
`_v` satisfies the whole `query`, whose conditions name `_v`'s sub-fields:

```json
{"nested": {"path": "_v", "query": {"all": [
  {"field": "_v.size", "op": "eq", "value": "L"},
  {"field": "_v.in_stock", "op": "eq", "value": true},
  {"field": "_v.price", "op": "lt", "value": 15000}]},
  "inner_hits": {"size": 50, "fields": ["_v._key"]}}}
```

- A `not` inside applies per object: `{"nested": {"path": "_v", "query": {"not":
  {"field": "_v.size", "op": "eq", "value": "L"}}}}` is "a variant that is not L". A
  `not` of a nested node is "no variant that ...". A missing `query`, or
  `{"all": []}`, holds of any object, and `{"field": "_v", "op": "exists"}` of any
  document holding a value of `_v` at all.
- The node composes like any other: in `all`, `any` and `not`, in `post_filter`, in
  `filter` and `filters` aggregations, and in saved queries. It counts one level
  towards the 4 levels (a `not` counts none), and its conditions towards the 50.
- A sub-field outside a nested node, any other field inside one, a nested node inside
  another, and a `path` that is no nested field are each a 400 at their `loc`
  (`query.field`, `query.nested.query.all.0.field`, `query.nested.path`).

**Inner hits.** `inner_hits` gives each hit, under `inner_hits.<key>`, the members of the
path that satisfy the nested query: `{"total": n, "offsets": [...], "objects":
[...]}`, `n` of them, the first `size` (3 by default, at most 1,000) by their offsets in
the array, in array order, and as objects holding the sub-fields `fields` names, by
their own names (every member when `fields` is empty; at most 100):

```json
{"id": "p1", "sort": ["p1"], "body": {"title": "Tee"},
 "inner_hits": {"_v": {"total": 1, "offsets": [1], "objects": [{"_key": "SKU-2"}]}}}
```

The key is `name`, or the path. They are read from each hit's stored body once the
page is fetched (a `fields` limit applies after), and a hit matched by another branch
of an `any` has them too, maybe with a `total` of 0. `inner_hits` under a `not` (a
document a `not` matches has no object to show), or in an aggregation's filter, is a
400 at its `loc`.

**Merged by name.** Nested nodes of one path that give one key, in the query and the
`post_filter` alike, share one set of inner hits under it: the members satisfying any
of their queries, each once (`total` counts them exactly), the first of them by the
largest of their `size`s, with the sub-fields any of them names (every member when one
names none). "A variant in size L under 150, or in size M under 120" is one list:

```json
{"any": [
  {"nested": {"path": "_v", "query": {"all": [{"field": "_v.size", "op": "eq", "value": "L"},
    {"field": "_v.price", "op": "lt", "value": 15000}]}, "inner_hits": {"name": "picked", "size": 10}}},
  {"nested": {"path": "_v", "query": {"all": [{"field": "_v.size", "op": "eq", "value": "M"},
    {"field": "_v.price", "op": "lt", "value": 12000}]}, "inner_hits": {"name": "picked", "size": 10}}}]}
```

A key given to nodes of two paths is a 400 at the second one's `inner_hits`.

A response holds at most 10,000 inner hit objects: a search whose `size` times its
`inner_hits` sizes (added up, a merged key's once) passes that is a 400 at `size`, and
the objects it may hold count against `max_inflight_read_bytes` (1 KiB each) while it
runs.

**Aggregations** on a sub-field: `terms` (with its `size`, `shard_size`,
`min_doc_count`, `include` and `exclude`), `stats`, `min`, `max` and `cardinality`, at
the top level or under `filter`, `filters` and `global`. They collect the objects of
the hits; `"where"`, a query of the sub-fields, keeps the objects it matches:

```json
{"aggs": {
  "sizes": {"terms": {"field": "_v.size", "where": {"field": "_v.in_stock", "op": "eq", "value": true}}},
  "cheapest": {"min": {"field": "_v.price", "where": {"field": "_v.in_stock", "op": "eq", "value": true}}}}}
```

`terms` counts each hit once per value, however many of its objects hold it: a
product with two L variants is one L. `stats`, `min`, `max` and `cardinality` read
every chosen object's value.

**Sub-aggregations under a `terms` on a sub-field.** Such a `terms` holds `stats`,
`min`, `max`, `cardinality` and `terms` sub-aggregations (the `terms` holding none of
its own), each bucket collecting them over:

- **its documents**, for a sub-aggregation on a top-level field: each hit holding a
  chosen object with the bucket's value, once;
- **its objects**, for one on a sub-field of the same nested field: the chosen objects
  (by the outer `where`) holding the bucket's value, narrowed by the sub-aggregation's
  own `where` when it has one. A `terms` there counts documents, each once per value
  among those objects.

```json
{"aggs": {"sizes": {"terms": {"field": "_v.size", "where": {"field": "_v.in_stock", "op": "eq", "value": true}},
  "aggs": {
    "sample": {"min": {"field": "item_no"}},
    "price": {"stats": {"field": "_v.price"}},
    "colors": {"terms": {"field": "_v.color", "size": 20}}}}}}
```

```json
{"sizes": {"buckets": [{"key": "l", "doc_count": 12,
  "sample": {"value": 1042},
  "price": {"count": 15, "min": 8999, "max": 15999, "avg": 12100, "sum": 181500},
  "colors": {"buckets": [{"key": "red", "doc_count": 7, "doc_count_error_upper_bound": 0}], "sum_other_doc_count": 0, "doc_count_error_upper_bound": 0}}]}}
```

No shard cuts a `terms` sub-aggregation's list (it takes no `shard_size`); the response
keeps its `size` buckets, as `min_doc_count`, `include` and `exclude` choose them, and
counts the rest in `sum_other_doc_count`. Its counts are exact where every shard
returned the outer bucket; where a shard cut the outer bucket out of its list, that
shard's documents are missing from it, and the sub-aggregation and each of its buckets
carry the outer bucket's `doc_count_error_upper_bound`. Its buckets count towards the
65,536 as each shard collects them, every one it holds (a shard past them refuses the
search, as for a histogram), and in the response those under the buckets it holds,
right after their own aggregation's.

Any other aggregation on a sub-field, a sub-aggregation of a `stats`, `min`, `max` or
`cardinality` on one, one on a sub-field under any other bucket aggregation or of
another nested field, any other sub-aggregation type under a `terms` on a sub-field,
and a `where` on a field that is no sub-field are each a 400 at their `loc`. A search
holding sub-aggregations under a `terms` on a sub-field reaches only nodes of 1.5 or
later (search wire level 7), a 503 while only older copies serve a shard.

**Sorts** on a sub-field (a number, date, keyword or text) order the hits by the least
(`"mode": "min"`, ascending's default) or greatest (`"max"`, descending's) value among
their objects, of those `"where"` matches when given; a hit with none sorts last, as a
missing value does:

```json
{"sort": [{"_v.price": {"order": "asc", "mode": "min",
                        "where": {"field": "_v.in_stock", "op": "eq", "value": true}}}]}
```

`search_after` pages on it as on any key. A `mode` other than `min` or `max` is a 400 at
`sort.N.<field>.mode`; `mode` or `where` on a field that is no sub-field, and a sort on
a `bool` or `keyword_list` sub-field or on the nested field itself, are 400s at
`sort.N`.

`PATCH /indexes/{index}/settings` changes `refresh_interval`, which applies at once,
`replicas_per_shard`, after which the allocator places or releases copies to meet the
new target, or [`gc_deletes`](#documents), how long deletes keep their versions.

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
- **`version=N`**, Elasticsearch's external versioning (`version_type=external`): the
  client's own version of the document, 1 to 2^62. The write applies only when N is
  higher than the stored version, and answers `{"id", "seq", "version"}`; otherwise it is
  a noop, `{"id", "version", "result": "noop"}` (200, no `seq`), and writes nothing. It
  excludes `if_seq` and `op_type=create` (400 at `params.version`).
  - The stored version is the greater of the document's and its tombstone's. Versions
    only rise: a write without `version` keeps the stored version (so a stale replay
    of a lower one after it is still a noop), and a delete keeps the document's
    version as a tombstone, so a replay of an older write after it is a noop as well.
    A versioned write above it applies and replaces the tombstone.
  - **A versioned delete of a document that does not exist**: when N is higher than
    its tombstone (or there is none), a 404 `document_not_found` (`result:
    "not_found"`) that records N as its tombstone; at or below it, a noop (200,
    `result: "noop"`).
  - **`gc_deletes`**, an index setting as Elasticsearch's `index.gc_deletes` (24 h by
    default, 1 s to 365 days, set at creation or by `PATCH .../settings`): how long a
    tombstone is kept. The cluster's leader prunes older ones at each changelog prune,
    after which a write of any version applies to the deleted id, as in
    Elasticsearch. Replay within it.
  - `GET` returns the document's `version` (absent for 0), as `_bulk` items and
    percolation matches do.
  - Replays and reconciles are harmless: send the same versioned writes again, and each
    one already applied is a noop.
  - **During a rolling upgrade** from 1.4, until every node runs 1.5, versions may
    disagree: a 1.4 node writes as before, leaving the stored version untouched, and
    its copies report no versions or `_seq` (a request with `percolate_matches` or
    `seq_no` skips them, 503 with no other copy). Send versioned writes to upgraded
    nodes, and trust `version` in `GET` and percolation matches once the rollout is
    done.
- **Unchanged documents.** An upsert without `if_seq`, `op_type=create` or `version`
  whose body is byte for byte the stored document's is a noop: `{"id", "seq",
  "result": "noop"}`, where `seq` is the seq the stored document was written at, and
  nothing is analyzed, committed, indexed or logged for it. Each document row keeps a
  hash of its body (the first 8 bytes of its SHA-256) for this, read by id before the
  write is analyzed. The bytes must match exactly: the same document with its keys
  reordered or its whitespace changed is written. `seq` still reads it back:
  `wait_for_seq` with it waits until the document is searchable, as after a write.
  `detect_noop=false` writes it anyway, as releases before 1.6 did (a new seq, the
  document indexed again). A document written by a release before 1.6 has no hash
  until it is next written, and during a rolling upgrade a 1.5 node's write leaves the
  hash behind, which is then not trusted: such a document is written, never answered
  as a noop from a stale hash. **When it takes effect:** the hashes are read once, by
  id, before the request analyzes or commits anything, outside its transaction, so a
  noop takes effect at that read, not at the request's commit. A write of the same id
  that another request commits after that read and before this request answers is
  not undone: the document then holds the other request's body, as if this request's
  upsert had committed first. Within one request the noops and the writes are not one
  atomic step (the writes still commit together); send `detect_noop=false` for a bulk
  whose upserts must all commit in its transaction. A noop writes nothing, so it is neither checked against
  the mapping again (a document stored before the index turned `dynamic: "strict"` is
  a noop where a write of it would be a 400) nor held back by write backpressure (a
  request of noops alone answers while its shards refuse writes with 429 or 503).
- **`refresh`.** `true`, `wait_for` or `false` (see [Consistency](#consistency)).
- **Deletes.** A delete answers `{"id", "seq"}`. Deleting a document that does not exist
  is a 404 `document_not_found` with `result: "not_found"`, and writes nothing (but the
  version of a versioned one).
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
- **Saved queries.** `query_upsert` is followed by the saved query, as `PUT
  .../queries/{id}` takes it (`{"query": ..., "meta": {...}}`; a line of another shape
  refuses the whole request), and `query_delete` takes none. Documents and saved queries
  mix in one request, in one transaction. With `percolate=true` every document of the
  request percolates after the whole request commits, so against the saved queries as
  they stand at its end: those its `query_upsert`s write, wherever they are in the
  request, included, and those its `query_delete`s remove left out:

  ```
  {"query_upsert": {"id": "s7", "version": 3}}
  {"query": {"field": "brand", "op": "eq", "value": "acme"}, "meta": {"search_id": 7}}
  {"upsert": {"id": "sku-1", "version": 12}}
  {"brand": "Acme", "price": 30}
  {"query_delete": {"id": "s8", "version": 5}}
  ```

- **`version`** in an action's metadata is the [`version=N`](#documents) of its op, on
  documents and saved queries alike: an item at or below the stored version answers
  `{"status": 200, "version": N, "result": "noop"}`, with no `seq` and, under
  `percolate=true`, no `queries`. It excludes `if_seq` and `create` (400 at
  `line.N.<action>.version`).
- **Unchanged documents** are [noops](#documents) item by item: an `upsert` (no
  `if_seq`, `create` or `version`) of the stored document's bytes answers `{"status":
  200, "seq": <the stored document's>, "result": "noop"}`, and under `percolate=true`
  still lists its `queries` (and `matches`): it is percolated as a write of the same
  body would be, against the saved queries as they stand. An id that more than one
  operation of the request names is always written. `detect_noop=false` writes every
  upsert.
- **Partial failure.** An operation refused on its own (an invalid document, a failed
  `if_seq`) is reported in its item, with `errors` set, and the rest commit.
- **Whole-request refusal.** A malformed action line, a line over `max_doc_bytes`, or
  more than `max_bulk_ops` operations refuse the whole request before anything is
  written.
- **`seq`.** The newest seq the request committed, or an unchanged upsert found its
  document at. Pass it as `wait_for_seq` to read your writes.
- **A client that gives up.** When the client disconnects before the answer (its
  timeout, say), the request stops at its next step that has not committed: before the
  next document it would analyze, before the transaction of its group commit begins, or,
  once committed, before its percolation or refresh wait. Once that transaction is
  under way, it may still commit after the client has gone: it is cancelled only while
  it has not reached its commit, and only when every request sharing it has gone too. The
  request commits in one transaction, so it is committed whole or not at all, never in
  part, but the client cannot tell which. Sending the same bulk again is then safe and
  cheap: an attempt that committed makes the retry's upserts noops at their stored
  seqs (still percolated, with their matches), and one that did not is written now. A
  retry sent while the abandoned attempt is still committing may write some documents
  again (the same bodies, at new seqs) rather than find them unchanged; the stored
  state is the same either way. Deletes and versioned or conditional operations retried after a commit answer
  as they would to any later request (a 404 for a delete already done, a noop for a
  version already applied, a 409 for an `if_seq` already used).
- **`percolate=true`.** Each upserted document's item lists the saved queries it matches.
  This is how scrape-bot's diff engine works out its match edges (spec §16).
  - `percolated` says whether the items carry their matches. It is false when the
    percolation could not finish after the writes committed. The writes stay
    committed either way.
  - Past 1,000,000 matches, every document's together, `percolated` is false and
    `percolate_reason` is `too_many_matches`, with no item's `queries`. Percolate
    those documents in smaller requests.
  - `percolated_seq`, with `percolated` true, is a seq every saved query committed at
    or below which the documents were percolated against: the least, over the shards,
    of the seq the copy percolating it held every change up to. A saved query written
    after it may or may not have been seen. It is not `seq`: an unchanged document's
    noop puts its stored seq, which can be far older, in `seq`, and a written
    document's seq can be newer than the copies' last refresh. On a cluster, a shard
    percolated by a copy on another node counts up to the seq the percolation asked of
    it (`wait_for_seq`, or the newest saved query this node wrote to the shard), so pass
    `wait_for_seq` there to raise it.
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

- **The query language** is a tree of `all`, `any`, `not`, [`nested`](#nested-fields)
  and conditions `{"field", "op", "value"}`. `{"all": []}`, the default, matches every
  document, and `_id` refers to the document id. The spec's §4 lists every operator.
  - Matching is boolean. There is no relevance scoring.
  - Text compares normalized, so `"acme"` matches `"ACME"`. This is byte-for-byte
    compatible with scrape-bot.
  - [Date math](#date-math) gives a `date` field's ranges relative bounds, such as
    `"now-7d/d"`.
- **`sort`** takes field names, `{"field": "asc"|"desc"}` or
  `{"field": {"order": "desc"}}`. `_id` breaks ties.
  - At most 16 keys, each field once.
  - Nothing after `_id`: ids are unique, so no later key could break a tie.
- **Paging.** `size` is 10 by default, at most 10,000. There are two ways to page,
  as in Elasticsearch:
  - **`from`** skips that many hits in sort order (0 by default): `"from": 40,
    "size": 20` is the third page of 20. `from + size` is at most
    `max_result_window` (10,000 by default); past it the search is a 400 at `loc`
    `from`, and deeper pages use `search_after`. Each shard finds its best `from +
    size`, so a deep `from` costs more the deeper it goes; only the page's bodies are
    read. The `total` and the aggregations are the whole search's, whatever `from` is.
  - **`search_after`**: pass the previous response's `next`. A cursor costs the same
    at any depth, and has no window. `from` must then be 0 (a 400 at `from`
    otherwise).

  `next` is the page's last hit's `sort` when the page is full, and null on a short
  page, with `from` too: a client can switch from `from` to `search_after` at any
  page by passing it on.
- **`track_total`.**
  - `true` counts exactly.
  - `false` counts no further than the hits.
  - A number counts exactly up to it (10,000 by default). Past it, `relation` is
    `gte`.
- **Aggregations** (`aggs`, or `aggregations`):
  - `terms`, with `size`, `shard_size`, `min_doc_count`, `include`, `exclude`,
    `collect_mode` and [`order`](#terms-order);
  - `range`, with `ranges` of `from`/`to`/`key`;
  - `histogram`, with `interval`, `offset` and `min_doc_count`;
  - `date_histogram`, with `fixed_interval`, `calendar_interval`, `offset` and
    `min_doc_count`;
  - [`composite`](#composite), with `sources`, `size` and `after`: every combination
    of its sources' values, a page at a time;
  - `stats`;
  - `min` and `max`, `{"value": x}`, or `{"value": null}` when no document has one.
    On a `date` field the value is Unix milliseconds, as `stats` gives it. `-0` is
    `0` to them and to `stats`' `min` and `max`, as it is to every bucket key;
  - `cardinality` (HyperLogLog++), with `precision`, or `threshold` (1 to 65,536) for
    an exact answer to "are there at least n values?": `{"value": v}` is the number of
    distinct values when there are fewer than `threshold`, else `threshold`. Each
    segment stops counting once it has seen `threshold` values, and so does the rest of
    its shard; over a dense hit set the dictionary's document frequencies answer
    without reading the hits. It is not Elasticsearch's `precision_threshold`, and
    takes no `precision`;
  - `filter`, a query: one bucket of the documents both the search's `query` and it
    match, `{"doc_count": n, ...}`;
  - `filters`, `{"filters": {"key": query, ...}, "other_bucket_key": "other"}`: a
    `filter` bucket per key, and with `other_bucket_key` one more of the documents no
    filter matches, `{"buckets": {"key": {"doc_count": n, ...}, ...}}`. Keyed only:
    the anonymous list form is refused;
  - `global`, `{}`: one bucket of every document of the index, whatever the `query`
    and `post_filter`, `{"doc_count": n, ...}`. Top level only;
  - [`top_metrics`](#top_metrics), with `metrics`, `sort` and `size`: fields of the
    documents first by a sort, `{"top": [{"sort": [v], "metrics": {"field": v}}]}`.

  A bucket aggregation may carry metric sub-aggregations (`stats`, `cardinality`,
  `min`, `max`, `top_metrics`) one level deep. `filter`,
  `filters` and `global` may carry any other aggregation, written inline in their
  bucket as Elasticsearch writes it, each with its own metric sub-aggregations: at
  most `filter` → `terms` → `stats`. A sub-aggregation is never named after a member
  of the bucket it is written in (`doc_count` under `filter`, `filters` and `global`;
  `key`, `key_as_string`, `from`, `to`, `doc_count` and `doc_count_error_upper_bound`
  under a bucket aggregation). A filter is the query language, checked against the
  mapping as `query` is, each problem at its own `loc` (`aggs.brands.filter.field`,
  `aggs.picked.filters.filters.acme.op`), and each filter on its own within the
  query's bounds (4 levels, 50 conditions, 100 nodes). A `terms` aggregation over
  several shards reports its error bound, as Elasticsearch does, under a `filter`
  too. On one shard it is exact, and its `doc_count_error_upper_bound` is 0.

  **`collect_mode`** is how a `terms` aggregation collects its sub-aggregations, as
  Elasticsearch's: `depth_first` collects them for every value as the hits are read;
  `breadth_first` counts the values first, makes each shard's `shard_size` cut, then
  reads the hits again for the values kept alone, so a field of many values does not
  pay for buckets the shard drops. Unset, each segment chooses: breadth first when its
  field holds more distinct values than `shard_size`, depth first otherwise. Both
  answer the same; only keyword, text and `keyword_list` fields defer (number and bool
  fields collect depth first). Breadth first reads each hit's values twice, but counts
  a dense hit set from the dictionary as a `terms` aggregation without
  sub-aggregations does.

  **`include` and `exclude`** choose the values a `terms` aggregation buckets, as
  Elasticsearch's do. Each is an array of exact values or a pattern, and the two
  combine, `exclude` winning. Values left out are never counted: the top `size` are the
  top of the chosen values, on every shard before its `shard_size` cut, and
  `sum_other_doc_count` and the error bounds count only chosen values. `min_doc_count`
  applies as before.
  - **Exact values** compare as the field's values do: keyword, text,
    `keyword_list` and `_id` values normalized (`"NIKE"` is `nike`; a number or bool as
    its text), numbers exactly, dates as Unix milliseconds or a date string the field
    takes, bools as `true` or `false`. A value no document holds makes no bucket. An
    empty array includes nothing (Elasticsearch's numeric fields read it as no
    include).
  - **A pattern**, a string, matches the whole normalized key, and is folded as a value
    is: `"Nike.*"` is `nike.*`, every key starting with `nike`, and `"tea  cup"` is
    `tea cup`. Each literal character is casefolded (`ß` reads `ss`), whitespace runs
    are one space and none at either end, and a class takes its members' folded forms
    too (`[A-C]` matches `a` to `c`). What cannot fold faithfully is refused: a class
    member that folds to more than one character (`[ß]`), and a negated class of
    characters that fold (`[^A-Z]`: write `[^a-z]`). `.` is one character of the
    folded key. It takes this subset of Lucene's regular
    expressions: characters as themselves, `\` before a punctuation character for it
    literally, `"..."` for a literal run, `.` for any character, classes `[abc]`,
    `[a-z]` and `[^a-z]`, repetition `*`, `+`, `?`, `{n}`, `{n,}` and `{n,m}` (n and m at
    most 1,000), alternation `|` and groups `( )`. Lucene's `~`, `&`, `<n-m>`, `@` and
    `#`, `\d`-style classes, and `^` and `$` (a pattern is anchored already) are
    refused at the `loc`, with the character's position. Patterns run as RE2, in
    time linear in the key. Only text keys take one: on a number, date or bool field
    a pattern is refused.
  - Elasticsearch's partitions (`{"partition": 0, "num_partitions": 10}`) are refused.

  Each segment decides the filter once per distinct value: exact values are looked up
  in its dictionary; a pattern, or an `exclude` alone, reads the dictionary (only the
  terms after a pattern's literal prefix) when the hits are many, and with few hits
  for its dictionary decides each value as it is collected. Its cost follows the
  field's distinct values or the hits, whichever is less.

  Bounds, each refused with a 400 whose `loc` names the place:
  - At most 100 aggregations per search (`loc` `aggs`, or `aggregations` as the body
    spells it), sub-aggregations included,
    and those under a `filters` aggregation counted once per bucket, as each bucket
    collects them apart: a `filters` of 10 keys and an `other_bucket_key` holding 2
    aggregations counts 1 + 11 × 2 = 23.
  - At most 100 `ranges` per `range` aggregation, and 100 `filters` per `filters`
    aggregation.
  - A `terms` `size` of at most 65,536. `shard_size` is capped at 65,536.
  - A `terms` `order` of at most 8 keys; a `top_metrics` `size` of at most 10 and at most
    10 `metrics`; a `composite` `size` of at most 10,000 and at most 10 `sources`. A
    `composite` page's buckets count towards the 65,536 below, as any aggregation's do.
  - At most 1,000 values in an `include` or `exclude` array, and 1,000 characters in
    a pattern, which compiles to at most 10,000 instructions: one whose counted
    repetitions multiply past it is refused as too complex (`loc`
    `aggs.<name>.terms.include`, or `.include.<i>` for one value).
  - At most 65,536 buckets per response, every aggregation's together (Elasticsearch's
    `search.max_buckets`), nested ones included: a `filter` or `global` counts one,
    a `filters` one per key. A shard whose own buckets pass it refuses the search, at
    the aggregation that passed it, asking for a wider interval, a lower size or a
    narrower query. A `terms` aggregation counts its `size` there, not its
    `shard_size` over-fetch. Shards that pass it only once merged are cut, in aggregation name
    order (those under a `filter`, `filters` or `global` right after it, whose own
    buckets are never cut), and each aggregation cut says `"truncated": true`. A cut `terms`
    aggregation counts the documents of its cut buckets in `sum_other_doc_count`.
  - A histogram's empty buckets (`min_doc_count` 0) are filled in only while the
    response stays within 65,536 buckets, and while each bucket's index (intervals from
    `offset`) is within 2^53 of 0, where a float64 still tells neighbouring buckets apart.
    A histogram left without them says `"truncated": true`.
  - A value a histogram cannot place in a bucket (its bucket's key overflows a
    float64, or a calendar interval cannot date it) refuses the search as too many
    buckets, rather than drop a counted document from every bucket.
- **`post_filter`**, a query, narrows the hits once the aggregations are counted:
  `hits`, `total`, the sort, `from`, `search_after` and `next` are those of the documents
  `query` and `post_filter` both match, while every aggregation but `global` counts
  `query`'s. It is checked as `query` is, at `post_filter...`.
- **`fields`** limits each hit's `body` to those members, at most 100 of them.
- **`timeout`** (`"250ms"`, or milliseconds) caps this search below `request_timeout`.
  Past it, the answer holds what was found, with `timed_out: true`. The deadline (the
  earlier of `timeout` and `request_timeout`) bounds each shard's whole search, not
  only its scan, and keeps a tenth of the budget (1 ms to 1 s) for the merge after
  the scan:
  - each shard scans its segments until the deadline less that reserve; a segment
    unfinished then is left out, its hits, total and aggregations with it;
  - the shard then merges its segments' aggregations until the deadline. A `terms`
    or `cardinality` whose field's global ordinals are still building waits only
    for a build already running, at most 250 ms, then merges by term, exactly (the
    build goes on in the background, for the next search). Only an aggregation the deadline cuts
    off even so is answered empty from that shard;
  - the coordinating node merges the shards' answers once it has them all, which is
    never cut: it is bounded by what the shards sent.

  Any cut sets `timed_out: true`. The hits found are fetched past the deadline, and
  the response written after it.
- **`seq_no`** (`false` by default), as Elasticsearch's `seq_no_primary_term`: each hit
  carries `_seq`, the seq of the change that wrote the version its copy holds (absent
  for a document indexed by a release before 1.5, until it is written again or its
  copy rebuilt). It is the same seq a write answered, and a percolation's `_seq`, so a
  client can tell which of two answers saw the newer document. A search with hits and
  `seq_no` reaches only nodes of 1.5 or later (search wire level 8).
- **`request_cache`** (`true` by default), Elasticsearch's shard request cache. A search
  with `size` 0 (a count, or aggregations only; no `from` or `search_after`) is
  answered, shard by shard, from what an identical search of the same shard contents
  computed, and is computed and stored otherwise. It is keyed by the request (its
  queries with [date math](#date-math) resolved, its aggregations, `track_total`; not
  its `timeout` or `sort`) and by what each shard holds, so a refresh that writes, a
  delete, a merge or a mapping change misses: a hit has the counts and buckets the
  search would compute (a float sum may differ in its last bits, as between two
  copies). Date math that reads `now` with no rounding (`now-7d`) resolves to another
  instant every millisecond, so such a search is never cached; a rounded one
  (`now-7d/d`) hits until the rounding moves on. `false` keeps the search out of the
  cache. A result that timed out is never stored. `_count` is cached as a size-0 search
  is. See [the request cache](operations.md#request-cache) for its memory and Redis.
- **`max_staleness`** (none by default; a duration such as `"5s"`, or milliseconds, at
  most `"1h"`), for a search with `size` 0 or a `_count` (in its body too): how old a
  cached result may be. Under continuous writes every refresh changes what a shard
  holds, so without it a count or a facet search almost never hits the request cache.
  With it, each shard may answer with its copy's newest result for the same search
  under the same mapping computed within `max_staleness`, although the copy has
  refreshed writes since: each such search then computes about once per
  `max_staleness` per shard copy, however often it is asked. The answer may miss the
  writes of the last `max_staleness`, on top of the refresh interval every search
  misses. A mapping change is never served across; `request_cache: false` and
  `wait_for_seq` (read-your-writes) ignore it. It is answered from this node's memory
  cache, not the shared Redis. Peers before 1.6 ignore it and answer with a current
  result.

**A facet panel in one search.** Each facet counts under every pick but its own, and
the hits obey every pick: put the picks in `post_filter`, and make each facet a
`filter` aggregation of the other facets' picks. Here brand `acme` and colour `red`
are picked, under a price range:

```json
{"query": {"field": "price", "op": "lt", "value": 100},
 "post_filter": {"all": [
   {"field": "brand", "op": "in", "value": ["acme"]},
   {"field": "color", "op": "in", "value": ["red"]}]},
 "sort": [{"price": "desc"}], "size": 20,
 "aggs": {
   "brand": {"filter": {"field": "color", "op": "in", "value": ["red"]},
             "aggs": {"brand": {"terms": {"field": "brand"}}}},
   "color": {"filter": {"field": "brand", "op": "in", "value": ["acme"]},
             "aggs": {"color": {"terms": {"field": "color"}}}},
   "everything": {"global": {}, "aggs": {"price": {"stats": {"field": "price"}}}}}}
```

```json
{"total": {"value": 3, "relation": "eq"}, "hits": ["..."],
 "aggs": {"brand": {"doc_count": 9, "brand": {"buckets": [{"key": "acme", "doc_count": 3}, "..."], "...": "..."}},
          "color": {"doc_count": 12, "color": {"buckets": [{"key": "red", "doc_count": 3}, "..."], "...": "..."}},
          "everything": {"doc_count": 120, "price": {"count": 120, "...": "..."}}}}
```

A facet with no pick of its own is a `filter` of every pick, or a plain aggregation
while nothing is picked.

**Facet type-ahead, and the picks outside the top buckets.** A facet's search box is a
pattern `include` of what was typed; the picks are counted in the same
search by an exact `include`, even when they fall outside the top 10:

```json
{"size": 0,
 "aggs": {
   "brand_typed": {"terms": {"field": "brand", "size": 10, "include": "ni.*"}},
   "brand_picked": {"terms": {"field": "brand", "include": ["Nike", "Puma"]}},
   "brand_rest": {"terms": {"field": "brand", "size": 10, "exclude": ["Nike", "Puma"]}}}}
```

`_count` takes `{"query": ...}` and answers `{"count", "relation", "timed_out"}`, counting
exactly. It refuses any other member, `post_filter` included (400 at its name).

### Terms order

`order` orders a `terms` aggregation's buckets, as Elasticsearch's does:
`{"_count": "desc"}` (the default), `{"_count": "asc"}`, `{"_key": "asc"|"desc"}`, or a
metric sub-aggregation of the `terms` by name: a `min`, `max` or `cardinality` by its
value (`"low"`, or `"low.value"`), a `stats` by a member (`"prices.avg"`: `count`,
`min`, `max`, `avg` or `sum`), a `top_metrics` of `size` 1 by one of its metrics
(`"latest.price"`). A list orders by its keys in turn, each breaking the ties of the
ones before: `[{"prices.avg": "desc"}, {"_count": "desc"}]`. `_key` ascending breaks
whatever ties are left, and nothing orders after `_key` (keys are unique). A bucket
whose metric has no value (a `min` over no price) sorts after every bucket with one,
ascending or descending.

```json
{"size": 0,
 "aggs": {"brands": {"terms": {"field": "brand", "size": 5, "order": {"price.avg": "desc"}},
                     "aggs": {"price": {"stats": {"field": "price"}}}}}}
```

How exact the answer is over several shards follows the first key, as in
Elasticsearch:

- **`_count` descending**: each shard returns its `shard_size` best terms, and
  `doc_count_error_upper_bound` bounds what the shards left out, as without `order`.
- **`_key`**: each shard returns its first `shard_size` keys in that order, so a term
  a shard left out sorts after the last one it returned, and the top `size` are exact
  (`doc_count_error_upper_bound` 0). With a `min_doc_count` above 1 a bucket can miss a
  shard's documents; such a bucket says `-1`.
- **`_count` ascending, or a sub-aggregation**: no shard can bound what it leaves out.
  Each shard collects every term's sub-aggregations (depth first; `collect_mode`
  `breadth_first` is refused), orders its terms exactly and returns `shard_size` of
  them, 10,000 by default rather than `size × 1.5 + 10`. The answer is exact while no
  shard holds more distinct values, and then `doc_count_error_upper_bound` is 0. When
  a shard does cut its list, `doc_count_error_upper_bound` is `-1` (unknown), on the
  aggregation and on each bucket some shard that cut did not return: a bucket that
  says 0 is still exact. Raise `shard_size` (up to 65,536) for more distinct values;
  its cost is that many buckets, with their sub-aggregations, per shard.

On one shard every order is exact. Searches with an `order` reach only nodes of 1.7 or
later (search wire level 10), as do `top_metrics` and `composite`.

### top_metrics

`top_metrics` reads `metrics`, fields of the documents first by `sort`, as
Elasticsearch's does: the latest price, the cheapest listing's store. It is a metric:
top level, under `filter`, `filters` and `global`, or a bucket aggregation's
sub-aggregation, per bucket.

```json
{"size": 0,
 "aggs": {"stores": {"terms": {"field": "store"},
                     "aggs": {"latest": {"top_metrics": {"metrics": [{"field": "price"}, {"field": "sku"}],
                                                         "sort": {"changed_at": "desc"},
                                                         "size": 1}}}}}}
```

```json
{"latest": {"top": [{"sort": [1761500000000], "metrics": {"price": 12.5, "sku": "a-1"}}]}}
```

- `metrics` is `{"field": name}` or a list of them, at most 10: a number or date field
  (a number; a date as Unix milliseconds), a keyword or text field (its value
  normalized, as aggregations read it), a bool, or `_id` (the document's exact id). A
  document without a value has `null` for it. A `keyword_list`, a point field and a
  nested field's sub-fields are refused.
- `sort` is one key, as a search's `sort` takes it (`"price"`, `{"price": "desc"}`,
  `{"price": {"order": "desc"}}`), on a number or date field. A document without a
  value sorts after every document with one; equal values go to the least `_id` (exact,
  byte order), so the answer does not depend on how documents spread over shards.
- `size` is how many documents, best first: 1 by default, at most 10. `top` holds fewer
  when fewer documents match.

It is exact: each segment keeps its `size` best documents, and each shard and the
coordinating node keep the `size` best of those.

### Composite

`composite` pages through every combination of its `sources`' values, as
Elasticsearch's does: per-group statistics over every group, however many groups there
are, `size` at a time.

```json
{"size": 0,
 "aggs": {"groups": {"composite": {"size": 100,
                                   "sources": [{"store": {"terms": {"field": "store"}}},
                                               {"day": {"date_histogram": {"field": "changed_at", "calendar_interval": "day"}}}]},
                     "aggs": {"price": {"stats": {"field": "price"}}}}}}
```

```json
{"groups": {"after_key": {"store": "acme", "day": 1761436800000},
            "buckets": [{"key": {"store": "acme", "day": 1761350400000}, "doc_count": 12, "price": {"...": "..."}},
                        "..."]}}
```

- `sources` is a list of `{"name": {type: {...}}}`, at most 10, each name once: `terms`
  (`field`: a keyword, text, `keyword_list`, number, date or bool field, or `_id`),
  `histogram` (`field`, `interval`, `offset`) or `date_histogram` (`field`,
  `calendar_interval` or `fixed_interval`, `offset`), each with `"order": "asc"`
  (the default) or `"desc"`. A bucket's `key` holds each source's value by name: a
  histogram's bucket start, a date as Unix milliseconds.
- The buckets are ordered by their keys, source by source, and a page is the first
  `size` (10 by default, at most 10,000) of them, or with `after` the first `size` past
  it. `after_key` is the page's last bucket's key: pass it as `after`, as it is, for
  the next page, until a page comes back without buckets (and without `after_key`).
- A document without a value for some source is in no bucket (Elasticsearch's
  `missing_bucket` is not taken). A `keyword_list` document is in one bucket per
  combination of its values. Metric sub-aggregations (`stats`, `cardinality`, `min`,
  `max`, `top_metrics`) are computed per bucket.
- It is exact, counts and metrics alike: each segment keeps the `size` least keys past
  `after` it has seen (with every document of each), and each shard and the
  coordinating node the `size` least of those. A walk over a changing index sees each
  page as of its own search, as `search_after` does.
- Top level only. Its page counts towards the response's 65,536 buckets; a page cut to
  fit them says `"truncated": true`, and its `after_key` is the last bucket kept.

### Box queries

`box` matches a document with at least one point inside a box: its x within the `x`
bounds and its y within the `y` bounds, both of the same point. Each axis takes at most
one lower bound (`gt` or `gte`) and one upper bound (`lt` or `lte`); an axis left out is
unbounded, and `{}` matches every document with a point.

```json
{"all": [
  {"field": "seen", "op": "box", "value": {"x": {"gte": "now-7d/d"}, "y": {"gte": 4}}},
  {"not": {"field": "spot", "op": "box", "value": {"x": {"gt": 0, "lt": 10}, "y": {"lte": 0}}}}
]}
```

- **The same point.** A document with points `(1, 10)` and `(10, 1)` does not match
  `{"x": {"gte": 5}, "y": {"gte": 5}}`: neither point is inside on both axes.
- **Bounds.** A `number` axis takes numbers. A `date` axis takes Unix milliseconds or a
  string, a date or [date math](#date-math), each bound rounding as the range op of its
  key does (`gte` and `lt` down, `gt` and `lte` up).
- **Composition.** `box` is a condition like any other: in `all`, `any` and `not`, in
  `post_filter`, in `filter` and `filters` aggregations, and in saved queries, whose
  date math each percolation resolves at its own `now`.
- **Validation.** Each problem is a 400 at its `loc`, such as `query.value.x.gte`: an
  unknown key, both `gt` and `gte`, a bound that is no finite number, date math on a
  `number` axis, or an axis whose bounds leave nothing between them (`{"gt": 5, "lt":
  5}`), which is checked when neither bound reads `now`, as `between` is checked.

### Date math

On a `date` field, `lt`, `lte`, `gt`, `gte` and each bound of `between` take a number
(Unix milliseconds) or a string: a date, or Elasticsearch's date math. So does each
bound of a [box](#box-queries) on a point field's `date` axis.

```json
{"all": [
  {"field": "seen_at", "op": "gte", "value": "now-7d/d"},
  {"field": "seen_at", "op": "between", "value": ["2026-10-01||+1M", "now"]}
]}
```

- **Anchor.** `now`, or a date (RFC 3339, or `YYYY-MM-DD` for midnight UTC) followed by
  `||`. A date with no `||` is a plain date.
- **Steps**, any number, applied in order: `+N<unit>` and `-N<unit>` (N defaults to
  1), and `/<unit>`, which rounds to the unit's start. Units: `y` years, `M` months,
  `w` weeks (from Monday), `d` days, `h` or `H` hours, `m` minutes, `s` seconds.
  Months and years keep the day, clamped to the month's last: `2026-01-31||+1M` is
  2026-02-28.
- **Rounding by op**, as Elasticsearch's range query rounds:

  | Op | Rounds | `now/d` on 2026-10-16 | Matches |
  | --- | --- | --- | --- |
  | `gte`, `between` lo | down | 2026-10-16T00:00:00.000 | from the day's start |
  | `lt` | down | 2026-10-16T00:00:00.000 | before the day |
  | `lte`, `between` hi | up | 2026-10-16T23:59:59.999 | through the day's end |
  | `gt` | up | 2026-10-16T23:59:59.999 | after the day |

  Rounding up also fills what a plain date leaves out: `lte "2026-10-16"` takes in the
  whole day, and a time with no fraction stands for its second's last millisecond.
- **Time zone.** UTC. There is no `time_zone` parameter.
- **`now`** is read once per request, to the millisecond, by the node that takes it.
  Every shard, on this node or a peer, answers at that instant: the query's, the
  `post_filter`'s and every `filter` and `filters` aggregation's date math alike, the
  bounds of boxes included.
- **Saved queries** keep the expression: `GET` returns it as written. Each percolation
  (`_percolate`, `_bulk?percolate=true`) resolves it at its own `now`, so a saved
  `gte now-7d` stops matching a document once the document is a week old.
- **Not for `eq`, `ne` or `in`.** These take Unix milliseconds on a `date` field, and
  refuse any string, a plain date as well as date math: the ranges read date strings,
  the equalities do not. Elasticsearch turns a date in a `term` query into a range;
  write the range instead: `between ["now/d", "now/d"]` is today, and
  `between ["2026-10-01", "2026-10-01"]` is that day.
- **Validation.** Each of these is a 400 at the value's `loc` (`query.value`, or
  `query.value.1` for `between`'s hi): a malformed expression; a string on a `number`
  field; a bound outside the dates a `date` field holds (±8.64e15 ms); a string to
  `eq`, `ne` or `in` on a `date` field. A range on a field whose type takes none
  (`lt` on a `keyword`) is refused at `query.op`, as any op its type does not take.
  A `between` whose `lo` is past its `hi` at every instant is a 400: when neither
  reads `now`, or when both are `now` plus or minus fixed-length units (`d`, `w`, `h`,
  `m`, `s`) with no rounding, such as `["now", "now-1d"]`. Any other `between` that
  reads `now` matches nothing while it is reversed.

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
- **Writes** take the same `if_seq`, `op_type`, `refresh` and [`version`](#documents)
  parameters as documents, and so do `_bulk`'s `query_upsert` and `query_delete`
  ([Bulk](#bulk)). `GET` returns the query's `version` (absent for 0), and every
  percolation copy indexes it with the query.
- **Listing.** `GET .../queries` pages by id. Pass `next` as `after`. A page takes two
  statements in one snapshot: one picks its ids, the first `size` of every shard's
  primary key range after `after` (each range cut at `size`), then one reads the trees
  and meta of those ids alone, so a page costs what it returns whatever the index holds.
  - **`fields`**, a comma-separated list of `id`, `seq`, `version`, `query` and `meta`,
    keeps those members of each query (`id` always). Without `query` and `meta`, their
    trees and meta are never read: `?fields=id,version&size=1000` pages a reconcile of
    100,000 queries in 100 requests of ids and versions (`version` 0 for none):

    ```json
    {"queries": [{"id": "s7", "version": 3}, {"id": "s8", "version": 0}], "next": "s8"}
    ```

## Percolation

`percolate`: `POST /indexes/{index}/_percolate`. It answers which saved queries each
document matches, for given documents (`docs`), stored ones (`ids`), or both.

```sh
curl -sS -X POST 'localhost:8780/indexes/products/_percolate?wait_for_seq=58' \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"docs": [{"brand": "Acme", "price": 5}], "ids": ["sku-1", "missing"]}'
```

```json
{"results": [
  {"found": true, "queries": ["cheap"]},
  {"id": "sku-1", "found": true, "queries": []},
  {"id": "missing", "found": false, "queries": []}],
 "took_ms": 1, "took_us": 1288}
```

- **Order.** Results list the given documents first, then the ids, each in request
  order.
- **Analysis.** Given documents are analyzed under the index's mapping.
- **Stored documents** are read from this node's copies, as a search reads them, so
  pass `wait_for_seq` to see a recent write. Each found one's result carries `_seq`,
  the seq of the change that wrote the version the copy percolated (absent for a
  document indexed by a release before 1.5), so a client can order its decisions by
  document version.
- **Shards at once.** The node sends the documents to every peer's copy at once (its own
  copies too for 64 documents or more; fewer, it runs them in turn while the peers
  answer) and merges their matches (a saved query lives on the shard its id hashes to); a shard
  that fails cancels the others.
- **Matches with versions: `percolate_matches=true`.** Each result also lists
  `matches`, its matched saved queries in `queries`' order, each with its client
  [version](#saved-queries) (absent for none). `queries` stays as it was, so a client
  that does not ask sees no change.
- **Inner hits: `percolate_inner_hits=true`** (implies `percolate_matches`). Each match
  of a saved query whose nested nodes declare `inner_hits` gives them by key, merged by
  name as a [search](#nested-fields) merges them, as `{"total", "offsets"}`: the
  document's members those nodes' queries match, the first `size` (the saved query's)
  by their offsets, exactly what a search of that saved query reports for the document,
  without the objects. The copy that holds the saved query reads them off the
  document as it percolated it.

  ```json
  {"results": [{"found": true, "queries": ["s7"], "matches": [
    {"id": "s7", "version": 3, "inner_hits": {"picked": {"total": 2, "offsets": [0, 3]}}}]}]}
  ```

  Both parameters work on `_bulk?percolate=true` too, each upserted document's item
  carrying `matches` beside `queries` (and `seq`, the version it was percolated at).
  With them, a percolation reaches only nodes of 1.5 or later (percolate wire level 4).
- **Limits.**
  - Up to 10,000 documents per request, given and stored together. More is a 413.
  - At most 1,000,000 matches per request, every document's together. Past it, the
    request is refused with a 422 whose code is `too_many_matches`: percolate fewer
    documents per request. An answer is never truncated, since a missing match is
    a wrong answer.
  - With `percolate_inner_hits`, at most 100,000 inner hit offsets per request, every
    match's together: past them, the same 422 `too_many_matches` (`_bulk`:
    `percolated: false`, `percolate_reason: "too_many_matches"`). The request reserves
    what they may take (16 bytes each, at most half the budget) from
    `max_inflight_read_bytes` while it runs.
- **Server time.** `took_ms` and `took_us` are the server's time for the request, from
  its admission to the last byte of its answer encoded, in whole milliseconds and
  microseconds. The `Server-Timing` header carries the same time
  (`total;dur=1.288`, in milliseconds). Like Elasticsearch's `took`, it leaves out the
  network and the client's decoding.

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
  - `too_many_matches` (422: a percolation matching more than 1,000,000 saved queries);
  - `timeout`, `request_timeout` and `client_closed_request`;
  - `unavailable` and `internal`.
- **`problems[].loc`** is a dotted path into the request, for example
  `query.all.2.any.0.value`, `params.wait_for_seq` or `line.3.upsert.id`.
- **`request_id`** echoes `X-Request-Id`, or a fresh id when the client sent none. Logs
  carry it too.
- **Retries.** 429 and 503 carry `Retry-After`. Both are safe to retry. A write that got
  a 5xx may or may not have committed, so retry it with `if_seq` or `op_type=create`
  when that matters.
