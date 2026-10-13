package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/percolate"
)

// A search past one of its bounds is refused with problem JSON naming where.
func TestSearchBounds(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("s", `{"mapping": {"fields": {"brand": "keyword", "price": "number"}}, "settings": {"shards": 2}}`)
	keys := make([]string, 17)
	for i := range keys {
		keys[i] = fmt.Sprintf("%q", fmt.Sprintf("f%d", i))
	}
	aggs := make([]string, 101)
	ranges := make([]string, 101)
	fields := make([]string, 101)
	for i := range aggs {
		aggs[i] = fmt.Sprintf(`"a%d": {"stats": {"field": "price"}}`, i)
		ranges[i] = fmt.Sprintf(`{"from": %d}`, i)
		fields[i] = fmt.Sprintf(`"f%d"`, i)
	}
	for _, tc := range []struct{ body, loc string }{
		{`{"sort": [` + strings.Join(keys, ",") + `]}`, "sort.16"},
		{`{"sort": ["price", {"price": "desc"}]}`, "sort.1"},
		{`{"sort": ["_id", "price"]}`, "sort.1"},
		{`{"aggs": {` + strings.Join(aggs, ",") + `}}`, "aggs"},
		{`{"aggs": {"r": {"range": {"field": "price", "ranges": [` + strings.Join(ranges, ",") + `]}}}}`, "aggs.r.range.ranges"},
		{`{"fields": [` + strings.Join(fields, ",") + `]}`, "fields"},
	} {
		p := e.problem(e.do("POST", "/indexes/s/_search", tc.body), http.StatusBadRequest, "invalid_request")
		if !hasLoc(p, tc.loc) {
			t.Errorf("%s: locs %v, want %s", tc.body, locs(p), tc.loc)
		}
	}
	e.must(http.StatusOK, "POST", "/indexes/s/_search", `{"sort": [`+strings.Join(keys[:16], ",")+`], "aggs": {`+strings.Join(aggs[:100], ",")+`}, "fields": [`+strings.Join(fields[:100], ",")+`]}`)
}

// One percolation answers at most percolate.MaxMatches matches, every document's
// together, whether one copy or the merge of several passes it: _percolate past it is
// a 422 too_many_matches, never a truncated answer; _bulk?percolate=true keeps its
// writes and says percolated false, with the reason.
func TestPercolateMatchesAreBounded(t *testing.T) {
	if testing.Short() {
		t.Skip("percolates a million matches")
	}
	e := newEnv(t, envOpts{})
	queries := percolate.MaxMatches/api.MaxPercolateDocs + 1
	docs := make([]string, api.MaxPercolateDocs)
	for i := range docs {
		docs[i] = `{"n": 1}`
	}
	for _, shards := range []int{1, 2} {
		index := fmt.Sprintf("p%d", shards)
		e.createIndex(index, fmt.Sprintf(`{"mapping": {"fields": {"n": "number"}}, "settings": {"shards": %d}}`, shards))
		var seq int64
		for i := range queries {
			seq = seqOf(t, e.must(http.StatusOK, "PUT", fmt.Sprintf("/indexes/%s/queries/q%03d", index, i), `{"query": {"all": []}}`))
		}
		path := fmt.Sprintf("/indexes/%s/_percolate?wait_for_seq=%d", index, seq)
		e.problem(e.do("POST", path, `{"docs": [`+strings.Join(docs, ",")+`]}`), http.StatusUnprocessableEntity, api.CodeTooManyMatches)
		r := e.must(http.StatusOK, "POST", path, `{"docs": [`+strings.Join(docs[:percolate.MaxMatches/queries], ",")+`]}`)
		if n := len(r["results"].([]any)); n != percolate.MaxMatches/queries { //nolint:forcetypeassert,errcheck // the shape
			t.Fatalf("%d shards: %d results under the bound", shards, n)
		}
	}
	lines := make([]string, 0, 2*api.MaxPercolateDocs)
	for i := range api.MaxPercolateDocs {
		lines = append(lines, fmt.Sprintf(`{"upsert": {"id": "d%05d"}}`, i), `{"n": 1}`)
	}
	r := e.must(http.StatusOK, "POST", "/indexes/p2/_bulk?percolate=true", ndjson(lines...))
	if r["percolated"] != false || r["percolate_reason"] != api.CodeTooManyMatches || seqOf(t, r) == 0 || r["errors"] != false {
		t.Fatalf("bulk past the bound: percolated %v, reason %v, seq %v, errors %v", r["percolated"], r["percolate_reason"], r["seq"], r["errors"])
	}
	for _, it := range r["items"].([]any) { //nolint:forcetypeassert,errcheck // the shape
		if q, ok := it.(map[string]any)["queries"]; ok { //nolint:forcetypeassert,errcheck // the shape
			t.Fatalf("an item carries queries %v past the bound", q)
		}
	}
}
