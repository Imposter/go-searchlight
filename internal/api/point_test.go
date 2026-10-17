package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/node"
)

// Point fields through the API, every answer checked against the contract: a mapping
// naming them, refused when malformed; documents of pairs; box searches, counts and
// filters with date math on a date axis; refusals of aggregations and sorts on them;
// saved boxes percolating at their own now; and the field catalogue.
func TestPointFields(t *testing.T) {
	t0 := time.Date(2031, 3, 10, 13, 45, 30, 0, time.UTC)
	wall := &settableWall{at: t0}
	e := newEnv(t, envOpts{node: func(o *node.Options) { o.Clock = wall }})
	for body, says := range map[string]string{
		`{"mapping": {"fields": {"p": "point"}}}`:                                             "names its axes",
		`{"mapping": {"fields": {"p": {"type": "point", "x": "number"}}}}`:                    "needs its y axis",
		`{"mapping": {"fields": {"p": {"type": "point", "x": "text", "y": "number"}}}}`:       `"number" or "date"`,
		`{"mapping": {"fields": {"p": {"type": "point", "x": "date", "y": "date", "z": 1}}}}`: "unknown field",
	} {
		p := e.problem(e.do("PUT", "/indexes/bad", body), http.StatusBadRequest, "invalid_request")
		if !strings.Contains(fmt.Sprint(p), says) {
			t.Errorf("%s: %v, want it to say %q", body, p, says)
		}
	}
	e.createIndex("pt", `{"mapping": {"dynamic": "strict", "fields": {"seen": {"type": "point", "x": "date", "y": "number"}, "brand": "keyword"}}, "settings": {"shards": 2}}`)
	info := e.must(http.StatusOK, "GET", "/indexes/pt", "")
	if got := fmt.Sprint(info["mapping"]); !strings.Contains(got, "seen:map[type:point x:date y:number]") {
		t.Fatalf("the mapping reads back as %s", got)
	}
	e.must(http.StatusOK, "PATCH", "/indexes/pt/mapping", `{"fields": {"spot": {"type": "point", "x": "number", "y": "number"}}}`)
	p := e.problem(e.do("PATCH", "/indexes/pt/mapping", `{"fields": {"spot": {"type": "point", "x": "date", "y": "number"}}}`), http.StatusBadRequest, "invalid_request")
	if !strings.Contains(fmt.Sprint(p), "cannot become") {
		t.Fatalf("retyping a point: %v", p)
	}
	p = e.problem(e.do("PATCH", "/indexes/pt/mapping", `{"fields": {"other": {"type": "point"}}}`), http.StatusBadRequest, "invalid_request")
	if got := locs(p); len(got) != 1 || got[0] != "fields.other" {
		t.Fatalf("a malformed point type: %v", p)
	}

	ago := func(d int) string { return t0.Add(-time.Duration(d) * 24 * time.Hour).Format(time.RFC3339) }
	seq := seqOf(t, e.must(http.StatusOK, "POST", "/indexes/pt/_bulk?refresh=wait_for", ndjson(
		`{"upsert": {"id": "a"}}`, fmt.Sprintf(`{"brand": "acme", "seen": [[%q, 5], [%q, 1]], "spot": [[1, 10], [10, 1]]}`, ago(2), ago(40)),
		`{"upsert": {"id": "b"}}`, fmt.Sprintf(`{"brand": "acme", "seen": [%q, 9], "spot": [6, 6]}`, ago(20)),
		`{"upsert": {"id": "c"}}`, `{"brand": "initech", "seen": []}`,
	)))
	week := `{"field": "seen", "op": "box", "value": {"x": {"gte": "now-7d/d"}, "y": {"gte": 5}}}`
	same := `{"field": "spot", "op": "box", "value": {"x": {"gte": 5}, "y": {"gte": 5}}}`
	for q, want := range map[string]string{
		week:                                "[a]",
		same:                                "[b]",
		`{"not": ` + same + `}`:             "[a c]",
		`{"field": "seen", "op": "exists"}`: "[a b c]",
		`{"field": "spot", "op": "box", "value": {}}`: "[a b]",
		`{"all": [{"field": "brand", "op": "eq", "value": "acme"}, {"field": "seen", "op": "box", "value": {"x": {"lt": "now-30d"}}}]}`: "[a]",
	} {
		r := e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/pt/_search?wait_for_seq=%d", seq), `{"query": `+q+`, "sort": ["_id"]}`)
		var ids []string
		for _, h := range r["hits"].([]any) { //nolint:forcetypeassert,errcheck // the shape
			ids = append(ids, fmt.Sprint(h.(map[string]any)["id"])) //nolint:forcetypeassert,errcheck // the shape
		}
		if got := fmt.Sprint(ids); got != want {
			t.Errorf("%s: %s, want %s", q, got, want)
		}
		c := e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/pt/_count?wait_for_seq=%d", seq), `{"query": `+q+`}`)
		if got := fmt.Sprint(c["count"]); got != fmt.Sprint(strings.Count(want, " ")+1) {
			t.Errorf("%s: count %s, want %s", q, got, want)
		}
	}
	for _, tc := range []struct{ body, loc, says string }{
		{`{"query": {"field": "seen", "op": "box", "value": {"y": {"gte": "now"}}}}`, "query.value.y.gte", "number axis"},
		{`{"query": {"field": "seen", "op": "box", "value": {"x": {"gte": "now-1x"}}}}`, "query.value.x.gte", "unknown unit"},
		{`{"query": {"field": "spot", "op": "box", "value": {"x": {"gt": 1, "gte": 2}}}}`, "query.value.x.gte", "not both"},
		{`{"query": {"field": "spot", "op": "lt", "value": 3}}`, "query.op", "exists/box"},
		{`{"aggs": {"s": {"terms": {"field": "spot"}}}}`, "aggs.s.terms.field", "point field"},
		{`{"sort": ["seen"]}`, "sort.0", "point field"},
	} {
		p := e.problem(e.do("POST", "/indexes/pt/_search", tc.body), http.StatusBadRequest, "invalid_request")
		if got := locs(p); len(got) != 1 || got[0] != tc.loc || !strings.Contains(fmt.Sprint(p["problems"]), tc.says) {
			t.Errorf("%s: %v, want %s saying %q", tc.body, p["problems"], tc.loc, tc.says)
		}
	}

	seq = seqOf(t, e.must(http.StatusOK, "PUT", "/indexes/pt/queries/week", `{"query": `+week+`}`))
	got := e.must(http.StatusOK, "GET", "/indexes/pt/queries/week", "")
	if q := fmt.Sprint(got["query"]); !strings.Contains(q, "gte:now-7d/d") {
		t.Errorf("GET returns %s", q)
	}
	percolate := func() string {
		r := e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/pt/_percolate?wait_for_seq=%d", seq), `{"ids": ["a", "b"]}`)
		var out []string
		for _, res := range r["results"].([]any) { //nolint:forcetypeassert,errcheck // the shape
			m := res.(map[string]any) //nolint:forcetypeassert,errcheck // the shape
			out = append(out, fmt.Sprint(m["id"], m["queries"]))
		}
		return strings.Join(out, " ")
	}
	if got := percolate(); got != "a[week] b[]" {
		t.Errorf("percolate now: %s", got)
	}
	wall.set(t0.Add(6 * 24 * time.Hour))
	if got := percolate(); got != "a[] b[]" {
		t.Errorf("percolate six days on: %s", got)
	}

	cat := e.must(http.StatusOK, "GET", "/indexes/pt/_fields", "")
	if got := fmt.Sprint(cat["fields"]); !strings.Contains(got, "map[name:spot type:map[type:point x:number y:number]]") {
		t.Errorf("the catalogue: %s", got)
	}
}
