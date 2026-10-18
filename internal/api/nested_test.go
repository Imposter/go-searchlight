package api_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// Nested fields through the API, every answer checked against the contract: a mapping
// naming one with its sub-fields, read back grouped, refused when malformed, grown by
// a PATCH; products with variants; correlated nested queries with inner hits, counts,
// aggregations with a where, sorts by a variant's price; refusals at their locs;
// saved nested queries percolating; and the field catalogue.
func TestNestedFields(t *testing.T) {
	e := newEnv(t, envOpts{})
	for body, loc := range map[string]string{
		`{"mapping": {"fields": {"_v": {"type": "nested", "fields": {"p": {"type": "point", "x": "number", "y": "number"}}}}}}`: "mapping.fields._v.fields.p",
		`{"mapping": {"fields": {"_v": {"type": "nested", "fields": {"size": "keyword"}}, "_v.size": "keyword"}}}`:              "mapping.fields._v.size",
		`{"mapping": {"fields": {"a.b": {"type": "nested", "fields": {}}}}}`:                                                    "mapping.fields.a.b",
	} {
		p := e.problem(e.do("PUT", "/indexes/bad", body), http.StatusBadRequest, "invalid_request")
		if got := locs(p); len(got) != 1 || got[0] != loc {
			t.Errorf("%s: %v, want a problem at %s", body, p, loc)
		}
	}
	e.createIndex("shop", `{"mapping": {"dynamic": "strict", "fields": {"title": "text", "_v": {"type": "nested", "fields": {"_key": "keyword", "size": "keyword", "price": "number"}}}}, "settings": {"shards": 2}}`)
	e.must(http.StatusOK, "PATCH", "/indexes/shop/mapping", `{"fields": {"_v": {"type": "nested", "fields": {"in_stock": "bool"}}}}`)
	info := e.must(http.StatusOK, "GET", "/indexes/shop", "")
	if got := fmt.Sprint(info["mapping"]); !strings.Contains(got, "_v:map[fields:map[_key:keyword in_stock:bool price:number size:keyword] type:nested]") {
		t.Fatalf("the mapping reads back as %s", got)
	}
	p := e.problem(e.do("PATCH", "/indexes/shop/mapping", `{"fields": {"_v": {"type": "nested", "fields": {"size": "number"}}}}`), http.StatusBadRequest, "invalid_request")
	if !strings.Contains(fmt.Sprint(p), "cannot become") {
		t.Fatalf("retyping a sub-field: %v", p)
	}
	p = e.problem(e.do("PUT", "/indexes/shop/docs/x", `{"_v": [{"size": "L", "colour": "red"}]}`), http.StatusBadRequest, "invalid_request")
	if !strings.Contains(fmt.Sprint(p), "_v.colour") {
		t.Fatalf("a strict mapping's unmapped sub-field: %v", p)
	}

	seq := seqOf(t, e.must(http.StatusOK, "POST", "/indexes/shop/_bulk?refresh=wait_for", ndjson(
		`{"upsert": {"id": "a"}}`, `{"title": "tee", "_v": [{"_key": "a1", "size": "L", "price": 12999, "in_stock": false}, {"_key": "a2", "size": "M", "price": 9999, "in_stock": true}]}`,
		`{"upsert": {"id": "b"}}`, `{"title": "hoodie", "_v": [{"_key": "b1", "size": "S", "price": 20000, "in_stock": true}, {"_key": "b2", "size": "L", "price": 14999, "in_stock": true}]}`,
		`{"upsert": {"id": "c"}}`, `{"title": "cap", "_v": {"_key": "c1", "size": "L", "price": 1500, "in_stock": true}}`,
		`{"upsert": {"id": "d"}}`, `{"title": "sock", "_v": []}`,
	)))
	inStockL := `{"all": [{"field": "_v.size", "op": "eq", "value": "L"}, {"field": "_v.in_stock", "op": "eq", "value": true}, {"field": "_v.price", "op": "lt", "value": 15000}]}`
	search := func(body string) map[string]any {
		t.Helper()
		return e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/shop/_search?wait_for_seq=%d", seq), body)
	}
	ids := func(r map[string]any) string {
		var out []string
		for _, h := range r["hits"].([]any) { //nolint:forcetypeassert,errcheck // the shape
			out = append(out, fmt.Sprint(h.(map[string]any)["id"])) //nolint:forcetypeassert,errcheck // the shape
		}
		return strings.Join(out, " ")
	}
	r := search(`{"query": {"nested": {"path": "_v", "query": ` + inStockL + `, "inner_hits": {"size": 5, "fields": ["_v._key"]}}}, "sort": ["_id"], "fields": ["title"]}`)
	if got := ids(r); got != "b c" {
		t.Fatalf("size L, in stock and under 150 on one variant: %s, want b c (a's L is out of stock)", got)
	}
	hit := r["hits"].([]any)[0].(map[string]any) //nolint:forcetypeassert,errcheck // the shape
	raw, err := json.Marshal(hit["inner_hits"])
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"_v":{"objects":[{"_key":"b2"}],"offsets":[1],"total":1}}` || fmt.Sprint(hit["body"]) != "map[title:hoodie]" {
		t.Fatalf("b's inner hits %s, body %v", raw, hit["body"])
	}
	for q, want := range map[string]string{
		`{"nested": {"path": "_v"}}`: "a b c",
		`{"not": {"nested": {"path": "_v", "query": {"field": "_v.in_stock", "op": "eq", "value": true}}}}`:                                              "d",
		`{"nested": {"path": "_v", "query": {"not": {"field": "_v.size", "op": "eq", "value": "L"}}}}`:                                                   "a b",
		`{"all": [{"field": "title", "op": "eq", "value": "tee"}, {"nested": {"path": "_v", "query": {"field": "_v.size", "op": "eq", "value": "m"}}}]}`: "a",
	} {
		if got := ids(search(`{"query": ` + q + `, "sort": ["_id"]}`)); got != want {
			t.Errorf("%s: %s, want %s", q, got, want)
		}
	}
	c := e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/shop/_count?wait_for_seq=%d", seq), `{"query": {"nested": {"path": "_v", "query": `+inStockL+`}}}`)
	if fmt.Sprint(c["count"]) != "2" {
		t.Errorf("count %v", c["count"])
	}

	r = search(`{"size": 0, "aggs": {"sizes": {"terms": {"field": "_v.size"}}, "in_stock": {"terms": {"field": "_v.size", "where": {"field": "_v.in_stock", "op": "eq", "value": true}}}, "cheapest": {"min": {"field": "_v.price", "where": {"field": "_v.in_stock", "op": "eq", "value": true}}}}}`)
	aggs, err := json.Marshal(r["aggs"])
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"sizes":{"buckets":[{"doc_count":3,"key":"l"},{"doc_count":1,"key":"m"},{"doc_count":1,"key":"s"}]`,
		`"in_stock":{"buckets":[{"doc_count":2,"key":"l"},{"doc_count":1,"key":"m"},{"doc_count":1,"key":"s"}]`,
		`"cheapest":{"value":1500}`,
	} {
		if !strings.Contains(string(aggs), want) {
			t.Errorf("aggregations %s, want %s", aggs, want)
		}
	}
	r = search(`{"query": {"nested": {"path": "_v"}}, "sort": [{"_v.price": {"order": "asc", "mode": "max", "where": {"field": "_v.size", "op": "eq", "value": "L"}}}]}`)
	if got := ids(r); got != "c a b" {
		t.Errorf("by the L variant's price: %s, want c a b", got)
	}
	if got := ids(search(`{"sort": [{"_v.price": "desc"}]}`)); got != "b a c d" {
		t.Errorf("by the dearest variant, descending: %s, want b a c d (d has none)", got)
	}

	for _, tc := range []struct{ body, loc, says string }{
		{`{"query": {"field": "_v.size", "op": "eq", "value": "L"}}`, "query.field", "nested"},
		{`{"query": {"nested": {"path": "title"}}}`, "query.nested.path", "not a nested field"},
		{`{"query": {"not": {"nested": {"path": "_v", "inner_hits": {}}}}}`, "query.not.nested.inner_hits", "under a not"},
		{`{"aggs": {"h": {"histogram": {"field": "_v.price", "interval": 10}}}}`, "aggs.h.histogram.field", "sub-field"},
		{`{"aggs": {"t": {"terms": {"field": "title", "where": {"all": []}}}}}`, "aggs.t.terms.where", "nested"},
		{`{"aggs": {"t": {"terms": {"field": "_v.size", "where": {"field": "title", "op": "exists"}}}}}`, "aggs.t.terms.where.field", "sub-fields"},
		{`{"sort": [{"_v.price": {"mode": "avg"}}]}`, "sort.0._v.price.mode", "min"},
		{`{"sort": [{"title": {"mode": "min"}}]}`, "sort.0", "sub-field"},
	} {
		p := e.problem(e.do("POST", "/indexes/shop/_search", tc.body), http.StatusBadRequest, "invalid_request")
		if got := locs(p); len(got) != 1 || got[0] != tc.loc || !strings.Contains(fmt.Sprint(p["problems"]), tc.says) {
			t.Errorf("%s: %v, want %s saying %q", tc.body, p["problems"], tc.loc, tc.says)
		}
	}

	seq = seqOf(t, e.must(http.StatusOK, "PUT", "/indexes/shop/queries/l", `{"query": {"nested": {"path": "_v", "query": `+inStockL+`}}}`))
	pr := e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/shop/_percolate?wait_for_seq=%d", seq), `{"ids": ["a", "b"]}`)
	var out []string
	for _, res := range pr["results"].([]any) { //nolint:forcetypeassert,errcheck // the shape
		m := res.(map[string]any) //nolint:forcetypeassert,errcheck // the shape
		out = append(out, fmt.Sprint(m["id"], m["queries"]))
	}
	if got := strings.Join(out, " "); got != "a[] b[l]" {
		t.Errorf("percolate: %s", got)
	}

	cat := e.must(http.StatusOK, "GET", "/indexes/shop/_fields", "")
	if got := fmt.Sprint(cat["fields"]); !strings.Contains(got, "map[name:_v type:nested]") || !strings.Contains(got, "map[name:_v.size type:keyword]") {
		t.Errorf("the catalogue: %s", got)
	}
}
