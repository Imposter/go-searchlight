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
the top level or under `filter`, `filters` and `global`, with no sub-aggregations. They
collect the objects of the hits; `"where"`, a query of the sub-fields, keeps the objects
it matches:

```json
{"aggs": {
  "sizes": {"terms": {"field": "_v.size", "where": {"field": "_v.in_stock", "op": "eq", "value": true}}},
  "cheapest": {"min": {"field": "_v.price", "where": {"field": "_v.in_stock", "op": "eq", "value": true}}}}}
```

`terms` counts each hit once per value, however many of its objects hold it: a
product with two L variants is one L. `stats`, `min`, `max` and `cardinality` read
every chosen object's value. Any other aggregation on a sub-field, one under a bucket
aggregation, a sub-aggregation of one, and a `where` on a field that is no sub-field
are each a 400 at their `loc`.

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
  - `percolated` says whether the items carry their matches. It is false when the
    percolation could not finish after the writes committed. The writes stay
    committed either way.
  - Past 1,000,000 matches, every document's together, `percolated` is false and
    `percolate_reason` is `too_many_matches`, with no item's `queries`. Percolate
    those documents in smaller requests.
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
  - `terms`, with `size`, `shard_size`, `min_doc_count`, `include`, `exclude` and
    `collect_mode`;
  - `range`, with `ranges` of `from`/`to`/`key`;
  - `histogram`, with `interval`, `offset` and `min_doc_count`;
  - `date_histogram`, with `fixed_interval`, `calendar_interval`, `offset` and
    `min_doc_count`;
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
    and `post_filter`, `{"doc_count": n, ...}`. Top level only.

  A bucket aggregation may carry metric sub-aggregations (`stats`, `cardinality`,
  `min`, `max`) one level deep. `filter`,
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
  pass `wait_for_seq` to see a recent write.
- **Limits.**
  - Up to 10,000 documents per request, given and stored together. More is a 413.
  - At most 1,000,000 matches per request, every document's together. Past it, the
    request is refused with a 422 whose code is `too_many_matches`: percolate fewer
    documents per request. An answer is never truncated, since a missing match is
    a wrong answer.
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
