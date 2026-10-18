package api_test

import (
	"fmt"
	"net/http"
	"testing"
)

// Client versions on single writes: a write applies above the stored version and is a
// noop (no seq) otherwise, a versioned delete keeps its version, GET returns it, and
// version excludes if_seq and op_type=create.
func TestVersionedWrites(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("v", `{"mapping": {"fields": {"brand": "keyword"}}}`)
	r := e.must(http.StatusOK, "PUT", "/indexes/v/docs/a?version=3", `{"brand": "x"}`)
	if r["version"] != 3.0 || r["seq"] == nil || r["result"] != nil {
		t.Fatalf("a versioned write = %v", r)
	}
	for _, v := range []string{"3", "2"} {
		r = e.must(http.StatusOK, "PUT", "/indexes/v/docs/a?version="+v, `{"brand": "y"}`)
		if r["result"] != "noop" || r["seq"] != nil || r["version"] != 3.0-map[string]float64{"3": 0, "2": 1}[v] {
			t.Fatalf("version %s over 3 = %v", v, r)
		}
	}
	if d := e.must(http.StatusOK, "GET", "/indexes/v/docs/a", ""); d["version"] != 3.0 || d["body"].(map[string]any)["brand"] != "x" { //nolint:forcetypeassert,errcheck // the shape
		t.Fatalf("GET = %v", d)
	}
	e.problem(e.do("PUT", "/indexes/v/docs/a?version=4&if_seq=1", `{}`), 400, "invalid_request")
	e.problem(e.do("PUT", "/indexes/v/docs/a?version=4&op_type=create", `{}`), 400, "invalid_request")
	e.problem(e.do("PUT", "/indexes/v/docs/a?version=0", `{}`), 400, "invalid_request")
	e.must(http.StatusOK, "DELETE", "/indexes/v/docs/a?version=5", "")
	if r = e.must(http.StatusOK, "PUT", "/indexes/v/docs/a?version=4", `{}`); r["result"] != "noop" {
		t.Fatalf("a write below the delete's version = %v", r)
	}
	e.problem(e.do("GET", "/indexes/v/docs/a", ""), 404, "document_not_found")
	if r = e.must(http.StatusOK, "PUT", "/indexes/v/docs/a?version=6", `{}`); r["seq"] == nil {
		t.Fatalf("a write above the delete's version = %v", r)
	}
	r = e.must(http.StatusOK, "PUT", "/indexes/v/queries/q?version=2", `{"query": {"all": []}}`)
	if r["version"] != 2.0 {
		t.Fatalf("a versioned query = %v", r)
	}
	if q := e.must(http.StatusOK, "GET", "/indexes/v/queries/q", ""); q["version"] != 2.0 {
		t.Fatalf("GET query = %v", q)
	}
	if r = e.must(http.StatusOK, "DELETE", "/indexes/v/queries/q?version=1", ""); r["result"] != "noop" {
		t.Fatalf("a delete below the query's version = %v", r)
	}
}

// _bulk takes query_upsert and query_delete beside documents, each with its version:
// a replay of the same bulk is a noop item by item, documents percolate against the
// queries written before them, and the lean listing pages ids and versions.
func TestBulkQueryActions(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("bq", `{"mapping": {"fields": {"brand": "keyword"}}, "settings": {"shards": 3}}`)
	bulk := ndjson(
		`{"query_upsert": {"id": "acme", "version": 1}}`,
		`{"query": {"field": "brand", "op": "eq", "value": "acme"}, "meta": {"search": 1}}`,
		`{"query_upsert": {"id": "all", "version": 1}}`,
		`{"query": {"all": []}}`,
		`{"upsert": {"id": "d1", "version": 1}}`,
		`{"brand": "ACME"}`,
		`{"query_delete": {"id": "nothing", "version": 1}}`,
	)
	items := func(r map[string]any) []map[string]any {
		var out []map[string]any
		for _, it := range r["items"].([]any) { //nolint:forcetypeassert,errcheck // the shape
			out = append(out, it.(map[string]any)) //nolint:forcetypeassert,errcheck // the shape
		}
		return out
	}
	r := e.must(http.StatusOK, "POST", "/indexes/bq/_bulk?percolate=true", bulk)
	got := items(r)
	if got[0]["op"] != "query_upsert" || got[0]["version"] != 1.0 || got[0]["seq"] == nil || got[3]["status"] != 404.0 {
		t.Fatalf("first bulk = %v", got)
	}
	if fmt.Sprint(got[2]["queries"]) != "[acme all]" {
		t.Fatalf("d1 percolated against the queries before it: %v", got[2])
	}
	r = e.must(http.StatusOK, "POST", "/indexes/bq/_bulk?percolate=true", bulk)
	got = items(r)
	for i := range 3 {
		if got[i]["result"] != "noop" || got[i]["seq"] != nil || got[i]["queries"] != nil {
			t.Fatalf("replayed item %d = %v", i, got[i])
		}
	}
	r = e.must(http.StatusOK, "POST", "/indexes/bq/_bulk", ndjson(
		`{"query_upsert": {"id": "acme", "version": 2}}`, `{"query": {"field": "brand", "op": "eq", "value": "other"}}`,
		`{"query_delete": {"id": "all", "version": 2}}`,
		`{"query_upsert": {"id": "all", "version": 2}}`, `{"query": {"all": []}}`,
	))
	got = items(r)
	if got[0]["seq"] == nil || got[1]["seq"] == nil || got[2]["result"] != "noop" {
		t.Fatalf("versions 2 = %v", got)
	}
	l := e.must(http.StatusOK, "GET", fmt.Sprintf("/indexes/bq/queries?fields=id,version&wait_for_seq=%d", seqOf(t, r)), "")
	if fmt.Sprint(l["queries"]) != "[map[id:acme version:2]]" {
		t.Fatalf("lean listing = %v", l)
	}
	e.problem(e.do("GET", "/indexes/bq/queries?fields=id,body", ""), 400, "invalid_request")
	for _, tc := range []struct{ body, loc string }{
		{ndjson(`{"query_upsert": {"id": "q"}}`, `{"meta": {}}`), "line.2"},
		{ndjson(`{"query_upsert": {"id": "q"}}`), "line.1"},
		{ndjson(`{"upsert": {"id": "x", "version": 1, "if_seq": 2}}`, `{}`), "line.1.upsert.version"},
		{ndjson(`{"create": {"id": "x", "version": 1}}`, `{}`), "line.1.create.version"},
		{ndjson(`{"query_delete": {"id": "x", "version": 0}}`), "line.1.query_delete.version"},
	} {
		p := e.problem(e.do("POST", "/indexes/bq/_bulk", tc.body), 400, "invalid_request")
		if !hasLoc(p, tc.loc) {
			t.Errorf("bulk %q: locs %v, want %s", tc.body, locs(p), tc.loc)
		}
	}
}
