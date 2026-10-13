package api_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
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
