package api_test

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

func TestBulk(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("b", `{"mapping": {"dynamic": "strict", "fields": {"title": "text", "price": "number"}}, "settings": {"shards": 2}}`)
	r := e.must(http.StatusOK, "POST", "/indexes/b/_bulk?refresh=wait_for", ndjson(
		`{"upsert": {"id": "1"}}`,
		`{"title": "one", "price": 1}`,
		``,
		`{"index": {"_id": "2"}}`,
		`{"title": "two", "price": 2}`,
		`{"create": {"id": "3"}}`,
		`{"title": "three", "price": 3}`,
		`{"upsert": {"id": "bad"}}`,
		`{"title": "x", "unmapped": 1}`,
		`{"upsert": {"id": "notjson"}}`,
		`{"title": `,
		`{"delete": {"id": "gone"}}`,
	))
	if r["errors"] != true || r["timed_out"] != false {
		t.Errorf("bulk = %v", r)
	}
	items := r["items"].([]any) //nolint:forcetypeassert,errcheck // the shape
	statuses := make([]int, len(items))
	for i, it := range items {
		statuses[i] = int(it.(map[string]any)["status"].(float64)) //nolint:forcetypeassert,errcheck // the shape
	}
	// A delete of nothing is a 404 (result not_found) and writes no change.
	if fmt.Sprint(statuses) != "[200 200 200 400 400 404]" {
		t.Errorf("item statuses = %v: %v", statuses, items)
	}
	last := seqOf(t, r)
	if seq := int64(items[2].(map[string]any)["seq"].(float64)); seq != last { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("the last committed item's seq %d, the bulk's %d", seq, last)
	}
	if gone := items[5].(map[string]any)["error"].(map[string]any); gone["result"] != "not_found" || items[5].(map[string]any)["seq"] != nil { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("a delete of nothing = %v", items[5])
	}
	bad := items[3].(map[string]any)["error"].(map[string]any) //nolint:forcetypeassert,errcheck // the shape
	if bad["code"] != "invalid_request" || !hasLoc(bad, "body.unmapped") {
		t.Errorf("an invalid document's error = %v", bad)
	}
	if c := e.must(http.StatusOK, "POST", "/indexes/b/_count", ""); c["count"] != 3.0 {
		t.Errorf("count = %v", c)
	}

	// if_seq conflicts fail their item alone; create of an existing id too.
	r = e.must(http.StatusOK, "POST", "/indexes/b/_bulk", ndjson(
		`{"upsert": {"id": "1", "if_seq": 99999}}`,
		`{"title": "one!"}`,
		`{"create": {"id": "2"}}`,
		`{"title": "two!"}`,
		`{"delete": {"id": "3"}}`,
	))
	items = r["items"].([]any)                                                                                                                                       //nolint:forcetypeassert,errcheck // the shape
	if s := []any{items[0].(map[string]any)["status"], items[1].(map[string]any)["status"], items[2].(map[string]any)["status"]}; fmt.Sprint(s) != "[409 409 200]" { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("conditional items = %v", items)
	}

	// Request-level refusals write nothing.
	for _, tc := range []struct {
		body, loc string
		status    int
	}{
		{ndjson(`{"upsert": {"id": "x"}}`, `{"title": "x"}`, `not json`), "line.3", 400},
		{ndjson(`{"frobnicate": {"id": "x"}}`), "line.1.frobnicate", 400},
		{ndjson(`{"upsert": {}}`, `{}`), "line.1.upsert.id", 400},
		{ndjson(`{"upsert": {"id": "x", "if_seq": 0}}`, `{}`), "line.1.upsert.if_seq", 400},
		{ndjson(`{"upsert": {"id": "x"}}`), "line.1", 400},
		{"", "body", 400},
		{"\n\n", "body", 400},
	} {
		got := e.do("POST", "/indexes/b/_bulk", tc.body)
		p := e.problem(got, tc.status, "invalid_request")
		if !hasLoc(p, tc.loc) {
			t.Errorf("bulk %q: locs %v, want %s", tc.body, locs(p), tc.loc)
		}
	}
	e.problem(e.do("POST", "/indexes/b/_bulk?percolate=maybe", ndjson(`{"delete": {"id": "1"}}`)), 400, "invalid_request")
	e.problem(e.do("POST", "/indexes/missing/_bulk", ndjson(`{"delete": {"id": "1"}}`)), 404, "index_not_found")
}

func TestSearch(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("s", `{"mapping": {"fields": {"brand": "keyword", "price": "number", "tags": "keyword_list", "title": "text"}}, "settings": {"shards": 3}}`)
	var lines []string
	brands := []string{"acme", "globex", "initech"}
	for i := range 60 {
		lines = append(lines, fmt.Sprintf(`{"upsert": {"id": "d%02d"}}`, i),
			fmt.Sprintf(`{"brand": %q, "price": %d, "tags": ["t%d", "all"], "title": "item %d"}`, brands[i%3], i, i%5, i))
	}
	w := e.must(http.StatusOK, "POST", "/indexes/s/_bulk", ndjson(lines...))
	seq := seqOf(t, w)
	path := fmt.Sprintf("/indexes/s/_search?wait_for_seq=%d", seq)

	// Sorted paging over three shards: every document once, in order.
	var got []string
	body := `{"query": {"field": "brand", "op": "in", "value": ["acme", "initech"]}, "sort": [{"price": "desc"}], "size": 7}`
	for page := 0; ; page++ {
		r := e.must(http.StatusOK, "POST", path, body)
		if totalOf(r) != 40 {
			t.Fatalf("total = %v", r["total"])
		}
		for _, h := range r["hits"].([]any) { //nolint:forcetypeassert,errcheck // the shape
			hm := h.(map[string]any) //nolint:forcetypeassert,errcheck // the shape
			if _, ok := hm["ref"]; ok {
				t.Fatalf("a hit exposes its internal ref: %v", hm)
			}
			if hm["body"].(map[string]any)["brand"] == "globex" { //nolint:forcetypeassert,errcheck // the shape
				t.Fatalf("a filtered-out hit: %v", hm)
			}
		}
		got = append(got, ids(r)...)
		next, _ := r["next"].([]any)
		if next == nil {
			break
		}
		after, _ := json.Marshal(next)
		body = fmt.Sprintf(`{"query": {"field": "brand", "op": "in", "value": ["acme", "initech"]}, "sort": [{"price": "desc"}], "size": 7, "search_after": %s}`, after)
		if page > 10 {
			t.Fatal("paging does not end")
		}
	}
	var want []string
	for i := 59; i >= 0; i-- {
		if i%3 != 1 {
			want = append(want, fmt.Sprintf("d%02d", i))
		}
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("paged ids:\n got %v\nwant %v", got, want)
	}

	// Default size, fields, aggregations.
	r := e.must(http.StatusOK, "POST", path, `{"fields": ["price"], "aggs": {"brands": {"terms": {"field": "brand"}}, "p": {"stats": {"field": "price"}}}}`)
	if len(ids(r)) != 10 {
		t.Errorf("default size: %d hits", len(ids(r)))
	}
	for _, h := range r["hits"].([]any) { //nolint:forcetypeassert,errcheck // the shape
		if b := h.(map[string]any)["body"].(map[string]any); len(b) != 1 || b["price"] == nil { //nolint:forcetypeassert,errcheck // the shape
			t.Errorf("fields did not limit the body: %v", b)
		}
	}
	aggs := r["aggs"].(map[string]any)                                         //nolint:forcetypeassert,errcheck // the shape
	buckets := aggs["brands"].(map[string]any)["buckets"].([]any)              //nolint:forcetypeassert,errcheck // the shape
	if len(buckets) != 3 || buckets[0].(map[string]any)["doc_count"] != 20.0 { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("terms = %v", buckets)
	}
	if st := aggs["p"].(map[string]any); st["count"] != 60.0 || st["max"] != 59.0 { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("stats = %v", st)
	}

	// Validation: query problems carry their loc; unknown keys are refused.
	p := e.problem(e.do("POST", path, `{"query": {"all": [{"field": "price", "op": "contains", "value": "x"}]}}`), 400, "invalid_request")
	if !hasLoc(p, "query.all.0") {
		t.Errorf("an op the field does not take: %v", p)
	}
	p = e.problem(e.do("POST", path, `{"query": {"field": "nope", "op": "eq", "value": 1}}`), 400, "invalid_request")
	if !hasLoc(p, "query.field") {
		t.Errorf("an unmapped field: %v", p)
	}
	p = e.problem(e.do("POST", path, `{"from": 10}`), 400, "invalid_request")
	if !hasLoc(p, "from") {
		t.Errorf("from: %v", p)
	}
	p = e.problem(e.do("POST", path, `{"sort": ["tags"]}`), 400, "invalid_request")
	if !hasLoc(p, "sort.0") {
		t.Errorf("a sort on a list: %v", p)
	}
	e.problem(e.do("POST", "/indexes/s/_search?wait_for_seq=-1", `{}`), 400, "invalid_request")
	e.problem(e.do("POST", "/indexes/s/_search?wait_for=1", `{}`), 400, "invalid_request")

	c := e.must(http.StatusOK, "POST", "/indexes/s/_count", `{"query": {"field": "tags", "op": "has", "value": "t0"}}`)
	if c["count"] != 12.0 || c["relation"] != "eq" {
		t.Errorf("count = %v", c)
	}
	e.problem(e.do("POST", "/indexes/s/_count", `{"size": 1}`), 400, "invalid_request")
}

func TestSavedQueriesAndPercolate(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("p", `{"mapping": {"fields": {"brand": "keyword", "price": "number", "title": "text"}}, "settings": {"shards": 2}}`)
	put := func(id, q string) int64 {
		return seqOf(t, e.must(http.StatusOK, "PUT", "/indexes/p/queries/"+id, q))
	}
	put("cheap", `{"query": {"field": "price", "op": "lt", "value": 10}, "meta": {"search_id": 7}}`)
	put("acme", `{"query": {"field": "brand", "op": "eq", "value": "Acme"}}`)
	seq := put("everything", `{"query": {"all": []}}`)

	q := e.must(http.StatusOK, "GET", "/indexes/p/queries/cheap", "")
	if q["meta"].(map[string]any)["search_id"] != 7.0 || q["query"].(map[string]any)["op"] != "lt" { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("GET query = %v", q)
	}
	l := e.must(http.StatusOK, "GET", "/indexes/p/queries?size=2", "")
	if len(l["queries"].([]any)) != 2 || l["next"] != "cheap" { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("first page = %v", l)
	}
	l = e.must(http.StatusOK, "GET", "/indexes/p/queries?size=2&after=cheap", "")
	if len(l["queries"].([]any)) != 1 || l["next"] != nil { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("last page = %v", l)
	}

	p := e.problem(e.do("PUT", "/indexes/p/queries/bad", `{"query": {"all": [{"field": "brand", "op": "frob", "value": 1}]}}`), 400, "invalid_request")
	if !hasLoc(p, "query.all.0.op") {
		t.Errorf("a bad op: %v", p)
	}
	p = e.problem(e.do("PUT", "/indexes/p/queries/bad", `{"query": {"field": "nope", "op": "eq", "value": 1}}`), 400, "invalid_request")
	if !hasLoc(p, "query.field") {
		t.Errorf("an unmapped field: %v", p)
	}
	e.problem(e.do("PUT", "/indexes/p/queries/bad", `{"query": {"all": []}, "meta": [1]}`), 400, "invalid_request")
	e.problem(e.do("PUT", "/indexes/p/queries/bad", `{"meta": {}}`), 400, "invalid_request")
	e.problem(e.do("PUT", "/indexes/p/queries/bad", `{"query": {"all": []}, "meta": {"x": "`+strings.Repeat("m", 17<<10)+`"}}`), 413, "too_large")
	e.problem(e.do("GET", "/indexes/p/queries/bad", ""), 404, "query_not_found")
	e.problem(e.do("GET", "/indexes/p/queries?size=0", ""), 400, "invalid_request")

	r := e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/p/_bulk?percolate=true&wait_for_seq=%d", seq), ndjson(
		`{"upsert": {"id": "x"}}`, `{"brand": "ACME", "price": 5}`,
		`{"upsert": {"id": "y"}}`, `{"brand": "other", "price": 50}`,
		`{"delete": {"id": "z"}}`,
	))
	items := r["items"].([]any)                                                                                                                                                                   //nolint:forcetypeassert,errcheck // the shape
	if got := fmt.Sprint(items[0].(map[string]any)["queries"], items[1].(map[string]any)["queries"], items[2].(map[string]any)["queries"]); got != "[acme cheap everything] [everything] <nil>" { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("bulk percolation = %s", got)
	}

	r = e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/p/_percolate?wait_for_seq=%d", seqOf(t, r)), `{"docs": [{"brand": "acme", "price": 50}], "ids": ["x", "missing"]}`)
	res := r["results"].([]any) //nolint:forcetypeassert,errcheck // the shape
	if got := fmt.Sprint(res); got != "[map[found:true queries:[acme everything]] map[found:true id:x queries:[acme cheap everything]] map[found:false id:missing queries:[]]]" {
		t.Errorf("percolate = %s", got)
	}
	p = e.problem(e.do("POST", "/indexes/p/_percolate", `{"docs": [{"brand": "a"}, "nope"]}`), 400, "invalid_request")
	if !hasLoc(p, "docs.1") {
		t.Errorf("a doc that is no object: %v", p)
	}
	e.problem(e.do("POST", "/indexes/p/_percolate", `{}`), 400, "invalid_request")

	e.must(http.StatusOK, "DELETE", "/indexes/p/queries/everything?refresh=wait_for", "")
	r = e.must(http.StatusOK, "POST", "/indexes/p/_percolate", `{"docs": [{"brand": "zzz", "price": 50}]}`)
	if got := fmt.Sprint(r["results"]); got != "[map[found:true queries:[]]]" {
		t.Errorf("after a delete: %s", got)
	}
}

// TestBulkPercolateMatchesBruteForce checks _bulk?percolate=true against query.Match
// over every saved query, for random queries and documents on several shards.
func TestBulkPercolateMatchesBruteForce(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("bf", `{"mapping": {"fields": {"brand": "keyword", "price": "number", "tags": "keyword_list", "title": "text"}}, "settings": {"shards": 3}}`)
	rng := rand.New(rand.NewPCG(1, 2))
	brands := []string{"Acme", "Globex", "Initech", "Umbrella"}
	words := []string{"red", "chair", "table", "lamp", "oak", "steel"}
	leaf := func() string {
		switch rng.IntN(6) {
		case 0:
			return fmt.Sprintf(`{"field": "brand", "op": "eq", "value": %q}`, brands[rng.IntN(4)])
		case 1:
			return fmt.Sprintf(`{"field": "price", "op": "between", "value": [%d, %d]}`, rng.IntN(50), 50+rng.IntN(50))
		case 2:
			return fmt.Sprintf(`{"field": "tags", "op": "has", "value": %q}`, words[rng.IntN(6)])
		case 3:
			return fmt.Sprintf(`{"field": "title", "op": "contains", "value": %q}`, words[rng.IntN(6)][:3])
		case 4:
			return fmt.Sprintf(`{"field": "title", "op": "words_any", "value": [%q, %q]}`, words[rng.IntN(6)], words[rng.IntN(6)])
		}
		return `{"not": {"field": "brand", "op": "eq", "value": "Acme"}}`
	}
	queries := map[string]query.Node{}
	var seq int64
	for i := range 40 {
		var q string
		switch rng.IntN(3) {
		case 0:
			q = leaf()
		case 1:
			q = fmt.Sprintf(`{"all": [%s, %s]}`, leaf(), leaf())
		default:
			q = fmt.Sprintf(`{"any": [%s, %s]}`, leaf(), leaf())
		}
		id := fmt.Sprintf("q%02d", i)
		seq = seqOf(t, e.must(http.StatusOK, "PUT", "/indexes/bf/queries/"+id, `{"query": `+q+`}`))
		n, problems := query.Parse([]byte(q))
		if len(problems) > 0 {
			t.Fatal(problems)
		}
		queries[id] = n
	}
	var lines []string
	var bodies []string
	for i := range 80 {
		body := fmt.Sprintf(`{"brand": %q, "price": %d, "tags": [%q, %q], "title": "%s %s %s"}`,
			brands[rng.IntN(4)], rng.IntN(100), words[rng.IntN(6)], words[rng.IntN(6)], words[rng.IntN(6)], words[rng.IntN(6)], words[rng.IntN(6)])
		lines = append(lines, fmt.Sprintf(`{"upsert": {"id": "d%d"}}`, i), body)
		bodies = append(bodies, body)
	}
	r := e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/bf/_bulk?percolate=true&wait_for_seq=%d", seq), ndjson(lines...))
	mapping := &schema.Mapping{Fields: map[string]schema.FieldType{"brand": schema.Keyword, "price": schema.Number, "tags": schema.KeywordList, "title": schema.Text}}
	items := r["items"].([]any) //nolint:forcetypeassert,errcheck // the shape
	matched := 0
	for i, body := range bodies {
		doc, _, err := schema.Analyze(mapping, fmt.Sprintf("d%d", i), []byte(body))
		if err != nil {
			t.Fatal(err)
		}
		var want []string
		for id, n := range queries {
			if query.Match(n, &doc) {
				want = append(want, id)
			}
		}
		slices.Sort(want)
		var got []string
		for _, q := range items[i].(map[string]any)["queries"].([]any) { //nolint:forcetypeassert,errcheck // the shape
			got = append(got, q.(string)) //nolint:forcetypeassert,errcheck // the shape
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("doc %d %s:\n got %v\nwant %v", i, body, got, want)
		}
		matched += len(want)
	}
	if matched == 0 {
		t.Fatal("no document matched any query: the test proves nothing")
	}
}

func TestFieldsAndCluster(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("f", `{"mapping": {"fields": {"tags": "keyword_list", "brand": "keyword"}}, "settings": {"shards": 2}}`)
	w := e.must(http.StatusOK, "POST", "/indexes/f/_bulk", ndjson(
		`{"upsert": {"id": "1"}}`, `{"tags": ["a", "b"], "brand": "x"}`,
		`{"upsert": {"id": "2"}}`, `{"tags": ["a"], "brand": "y"}`,
		`{"upsert": {"id": "3"}}`, `{"tags": ["a", "c"]}`,
	))
	r := e.must(http.StatusOK, "GET", fmt.Sprintf("/indexes/f/_fields?entries=2&wait_for_seq=%d", seqOf(t, w)), "")
	if got := fmt.Sprint(r["fields"]); got != "[map[name:brand type:keyword] map[entries:[map[count:3 value:a] map[count:1 value:b]] name:tags type:keyword_list]]" {
		t.Errorf("fields = %s", got)
	}
	if r = e.must(http.StatusOK, "GET", "/indexes/f/_fields", ""); fmt.Sprint(r["fields"]) != "[map[name:brand type:keyword] map[name:tags type:keyword_list]]" {
		t.Errorf("fields without entries = %v", r["fields"])
	}
	e.problem(e.do("GET", "/indexes/f/_fields?entries=1001", ""), 400, "invalid_request")

	h := e.must(http.StatusOK, "GET", "/_cluster/health", "")
	if h["status"] != "green" || h["shards"] != 2.0 || h["indexes"] != 1.0 {
		t.Errorf("health = %v", h)
	}
	n := e.must(http.StatusOK, "GET", "/_cluster/nodes", "")
	if fmt.Sprint(n["nodes"]) != "[map[address:127.0.0.1:8780 id:test-node self:true version:test]]" {
		t.Errorf("nodes = %v", n)
	}
	s := e.must(http.StatusOK, "GET", "/_cluster/shards", "")
	shards := s["shards"].([]any)                                             //nolint:forcetypeassert,errcheck // the shape
	if len(shards) != 2 || shards[0].(map[string]any)["state"] != "serving" { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("shards = %v", shards)
	}
	if r := e.must(http.StatusOK, "GET", "/healthz", ""); r["status"] != "ok" {
		t.Errorf("healthz = %v", r)
	}
	if r := e.must(http.StatusOK, "GET", "/readyz", ""); r["status"] != "ready" {
		t.Errorf("readyz = %v", r)
	}
	e.problem(e.do("GET", "/metrics", ""), 404, "not_found") // no telemetry in this server

	e.problem(e.do("GET", "/nothing/here", ""), 404, "not_found")
	got := e.do("POST", "/indexes/f", "")
	e.problem(got, 405, "method_not_allowed")
	if got.header.Get("Allow") != "DELETE, GET, PUT" {
		t.Errorf("Allow = %q", got.header.Get("Allow"))
	}
	if id := got.header.Get("X-Request-Id"); id == "" || got.json(t)["request_id"] != id {
		t.Errorf("request id %q not in the problem %s", id, got.body)
	}
	if got := e.do("GET", "/healthz", "", "X-Request-Id", "abc-123"); got.header.Get("X-Request-Id") != "abc-123" {
		t.Errorf("the client's request id was not kept: %q", got.header.Get("X-Request-Id"))
	}
	if got := e.do("GET", "/healthz", "", "X-Request-Id", "bad id;drop"); got.header.Get("X-Request-Id") == "bad id;drop" {
		t.Error("an unsafe request id was echoed")
	}
}
