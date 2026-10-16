package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
)

// TestFacetPanelInOneSearch: the facet panel of docs/api.md over three shards, end to
// end. The hits obey every pick (post_filter), each facet counts under every pick but
// its own (a filter aggregation of the others), and global counts the whole index: each
// answers exactly what a search of its own would.
func TestFacetPanelInOneSearch(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("f", `{"mapping": {"dynamic": "strict", "fields": {"brand": "keyword", "color": "keyword", "price": "number"}}, "settings": {"shards": 3}}`)
	brands, colors := []string{"acme", "initech", "hooli"}, []string{"red", "blue", "green", "black"}
	lines := make([]string, 0, 240)
	for i := range 120 {
		lines = append(lines, fmt.Sprintf(`{"upsert": {"id": "d%03d"}}`, i),
			fmt.Sprintf(`{"brand": %q, "color": %q, "price": %d}`, brands[i%3], colors[(i/3)%4], i))
	}
	e.must(http.StatusOK, "POST", "/indexes/f/_bulk?refresh=wait_for", ndjson(lines...))
	brandPick := `{"field": "brand", "op": "in", "value": ["acme", "hooli"]}`
	colorPick := `{"field": "color", "op": "in", "value": ["red"]}`
	query := `{"field": "price", "op": "lt", "value": 100}`
	r := e.must(http.StatusOK, "POST", "/indexes/f/_search", `{
		"query": `+query+`,
		"post_filter": {"all": [`+brandPick+`, `+colorPick+`]},
		"sort": [{"price": "desc"}], "size": 4, "track_total": true,
		"aggs": {
			"brand": {"filter": `+colorPick+`, "aggs": {"brand": {"terms": {"field": "brand"}}}},
			"color": {"filter": `+brandPick+`, "aggs": {"color": {"terms": {"field": "color"}}}},
			"picked": {"filters": {"filters": {"acme": {"field": "brand", "op": "eq", "value": "acme"}}, "other_bucket_key": "rest"}},
			"everything": {"global": {}, "aggs": {"price": {"stats": {"field": "price"}}}}
		}}`)
	alone := func(q, aggs string) map[string]any {
		return e.must(http.StatusOK, "POST", "/indexes/f/_search", `{"query": `+q+`, "size": 0, "track_total": true, "aggs": `+aggs+`}`)
	}
	jsonOf := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	hitsOnly := e.must(http.StatusOK, "POST", "/indexes/f/_search", `{"query": {"all": [`+query+`, `+brandPick+`, `+colorPick+`]}, "sort": [{"price": "desc"}], "size": 4, "track_total": true}`)
	if g, w := jsonOf([]any{r["total"], r["hits"], r["next"]}), jsonOf([]any{hitsOnly["total"], hitsOnly["hits"], hitsOnly["next"]}); g != w {
		t.Fatalf("hits under the post_filter %s, want %s", g, w)
	}
	aggs := r["aggs"].(map[string]any) //nolint:forcetypeassert,errcheck // the shape
	for _, tc := range []struct{ facet, q, inner string }{
		{"brand", `{"all": [` + query + `, ` + colorPick + `]}`, `{"terms": {"field": "brand"}}`},
		{"color", `{"all": [` + query + `, ` + brandPick + `]}`, `{"terms": {"field": "color"}}`},
	} {
		want := alone(tc.q, `{"x": `+tc.inner+`}`)
		got := aggs[tc.facet].(map[string]any)                                                                                           //nolint:forcetypeassert,errcheck // the shape
		wantAggs := want["aggs"].(map[string]any)                                                                                        //nolint:forcetypeassert,errcheck // the shape
		if g, w := jsonOf(got[tc.facet]), jsonOf(wantAggs["x"]); g != w || got["doc_count"] != want["total"].(map[string]any)["value"] { //nolint:forcetypeassert,errcheck // the shape
			t.Fatalf("facet %s: %v documents, %s; want %v, %s", tc.facet, got["doc_count"], g, want["total"], w)
		}
	}
	picked := aggs["picked"].(map[string]any)["buckets"].(map[string]any)                                                                    //nolint:forcetypeassert,errcheck // the shape
	acme := alone(`{"all": [`+query+`, {"field": "brand", "op": "eq", "value": "acme"}]}`, `{}`)["total"].(map[string]any)["value"]          //nolint:forcetypeassert,errcheck // the shape
	rest := alone(`{"all": [`+query+`, {"not": {"field": "brand", "op": "eq", "value": "acme"}}]}`, `{}`)["total"].(map[string]any)["value"] //nolint:forcetypeassert,errcheck // the shape
	if g := jsonOf(picked); g != jsonOf(map[string]any{"acme": map[string]any{"doc_count": acme}, "rest": map[string]any{"doc_count": rest}}) {
		t.Fatalf("filters %s, want acme %v and rest %v", g, acme, rest)
	}
	everything := aggs["everything"].(map[string]any)                                                                               //nolint:forcetypeassert,errcheck // the shape
	if st := everything["price"].(map[string]any); everything["doc_count"] != 120.0 || st["count"] != 120.0 || st["max"] != 119.0 { //nolint:forcetypeassert,errcheck // the shape
		t.Fatalf("global %v: want every document", everything)
	}

	// Each query is checked against the mapping at its own loc; _count takes no
	// post_filter.
	for _, tc := range []struct{ body, loc string }{
		{`{"post_filter": {"field": "nope", "op": "eq", "value": 1}}`, "post_filter.field"},
		{`{"aggs": {"b": {"filter": {"field": "price", "op": "contains", "value": "x"}}}}`, "aggs.b.filter.op"},
		{`{"aggs": {"b": {"filters": {"filters": {"k": {"field": "nope", "op": "exists"}}}}}}`, "aggs.b.filters.filters.k.field"},
		{`{"aggs": {"g": {"global": {}, "aggs": {"f": {"filter": {"field": "nope", "op": "exists"}}}}}}`, "aggs.g.aggs.f.filter.field"},
		{`{"aggs": {"t": {"terms": {"field": "brand"}, "aggs": {"g": {"global": {}}}}}}`, "aggs.t.aggs.g"},
	} {
		p := e.problem(e.do("POST", "/indexes/f/_search", tc.body), 400, "invalid_request")
		if !slices.Contains(locs(p), tc.loc) {
			t.Errorf("%s: %v, want a problem at %s", tc.body, p, tc.loc)
		}
	}
	p := e.problem(e.do("POST", "/indexes/f/_count", `{"query": {"all": []}, "post_filter": {"all": []}}`), 400, "invalid_request")
	if !slices.ContainsFunc(locs(p), func(l string) bool { return strings.HasPrefix(l, "post_filter") }) {
		t.Errorf("_count with a post_filter: %v, want a problem at post_filter", p)
	}
}
