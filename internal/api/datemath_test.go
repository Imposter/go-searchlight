package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/node"
)

// settableWall is the real clock with a wall time the test sets; the timers stay real.
type settableWall struct {
	clock.Real
	mu sync.Mutex
	at time.Time
}

func (c *settableWall) Wall() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at
}

func (c *settableWall) set(at time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.at = at
}

// Date math through the API: refused with a precise loc on search, count and saved
// queries; a saved query read back as it was written; and a percolation, given and
// bulk, resolving now at its own time, the query never saved again.
func TestDateMath(t *testing.T) {
	t0 := time.Date(2031, 3, 10, 13, 45, 30, 0, time.UTC)
	wall := &settableWall{at: t0}
	e := newEnv(t, envOpts{node: func(o *node.Options) { o.Clock = wall }})
	e.createIndex("dm", `{"mapping": {"fields": {"seen": "date", "brand": "keyword", "price": "number"}}, "settings": {"shards": 2}}`)
	ago := func(d int) string { return t0.Add(-time.Duration(d) * 24 * time.Hour).Format(time.RFC3339) }

	for _, tc := range []struct{ query, loc, says string }{
		{`{"field": "seen", "op": "gte", "value": "now-7x"}`, "query.value", "unknown unit"},
		{`{"all": [{"field": "brand", "op": "eq", "value": "a"}, {"field": "seen", "op": "between", "value": ["now-1d", "now+"]}]}`, "query.all.1.value.1", "ends early"},
		{`{"field": "seen", "op": "eq", "value": "now/d"}`, "query.value", "date math is for lt, lte, gt, gte and between"},
		{`{"field": "price", "op": "lt", "value": "now"}`, "query.value", "date math is for date fields"},
		{`{"field": "seen", "op": "lt", "value": "now+300000y"}`, "query.value", "falls outside"},
	} {
		for _, req := range []struct{ method, path, body string }{
			{"POST", "/indexes/dm/_search", `{"query": ` + tc.query + `}`},
			{"POST", "/indexes/dm/_count", `{"query": ` + tc.query + `}`},
			{"PUT", "/indexes/dm/queries/bad", `{"query": ` + tc.query + `}`},
		} {
			p := e.problem(e.do(req.method, req.path, req.body), http.StatusBadRequest, "invalid_request")
			if got := locs(p); len(got) != 1 || got[0] != tc.loc || !strings.Contains(fmt.Sprint(p["problems"]), tc.says) {
				t.Errorf("%s %s %s: %v, want %s saying %q", req.method, req.path, tc.query, p["problems"], tc.loc, tc.says)
			}
		}
	}

	const week = `{"field":"seen","op":"gte","value":"now-7d"}`
	seq := seqOf(t, e.must(http.StatusOK, "PUT", "/indexes/dm/queries/week", `{"query": `+week+`}`))
	e.must(http.StatusOK, "PUT", "/indexes/dm/queries/this-month", `{"query": {"all": [{"field": "brand", "op": "eq", "value": "acme"}, {"field": "seen", "op": "between", "value": ["now/M", "now/M"]}]}}`)
	got := e.must(http.StatusOK, "GET", "/indexes/dm/queries/week", "")
	if q := fmt.Sprint(got["query"]); q != "map[field:seen op:gte value:now-7d]" {
		t.Errorf("GET returns %s", q)
	}

	seq = seqOf(t, e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/dm/_bulk?percolate=true&refresh=wait_for&wait_for_seq=%d", seq), ndjson(
		`{"upsert": {"id": "six"}}`, `{"brand": "acme", "seen": "`+ago(6)+`"}`,
		`{"upsert": {"id": "eight"}}`, `{"brand": "acme", "seen": "`+ago(8)+`"}`,
	)))
	percolate := func() string {
		r := e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/dm/_percolate?wait_for_seq=%d", seq), `{"ids": ["six", "eight"]}`)
		var out []string
		for _, res := range r["results"].([]any) { //nolint:forcetypeassert,errcheck // the shape
			m := res.(map[string]any) //nolint:forcetypeassert,errcheck // the shape
			out = append(out, fmt.Sprint(m["id"], m["queries"]))
		}
		return strings.Join(out, " ")
	}
	if got := percolate(); got != "six[this-month week] eight[this-month]" {
		t.Errorf("at t0: %s", got)
	}
	search := func(q string) int {
		return totalOf(e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/dm/_search?wait_for_seq=%d", seq), `{"query": `+q+`}`))
	}
	if n := search(week); n != 1 {
		t.Errorf("at t0 the search finds %d", n)
	}

	wall.set(t0.Add(2 * 24 * time.Hour))
	if got := percolate(); got != "six[this-month] eight[this-month]" {
		t.Errorf("two days on: %s", got)
	}
	// Before five is written: wait_for_seq is a floor, and a refresh may show five to a
	// search at the older seq.
	if n := search(week); n != 0 {
		t.Errorf("two days on the search finds %d", n)
	}
	r := e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/dm/_bulk?percolate=true&wait_for_seq=%d", seq), ndjson(
		`{"upsert": {"id": "five"}}`, `{"brand": "acme", "seen": "`+ago(5)+`"}`,
	))
	if items := fmt.Sprint(r["items"].([]any)[0].(map[string]any)["queries"]); items != "[this-month week]" { //nolint:forcetypeassert,errcheck // the shape
		t.Errorf("two days on, a bulk percolation: %s", items)
	}
	if n := totalOf(e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/dm/_search?wait_for_seq=%d", seqOf(t, r)), `{"query": `+week+`}`)); n != 1 {
		t.Errorf("two days on, with five: the search finds %d", n)
	}

	wall.set(t0.Add(30 * 24 * time.Hour))
	if got := percolate(); got != "six[] eight[]" {
		t.Errorf("a month on: %s", got)
	}
}
