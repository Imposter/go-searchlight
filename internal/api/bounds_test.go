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
	for _, tc := range []struct{ body, loc string }{
		{`{"sort": [` + strings.Join(keys, ",") + `]}`, "sort.16"},
		{`{"sort": ["price", {"price": "desc"}]}`, "sort.1"},
		{`{"sort": ["_id", "price"]}`, "sort.1"},
	} {
		p := e.problem(e.do("POST", "/indexes/s/_search", tc.body), http.StatusBadRequest, "invalid_request")
		if !hasLoc(p, tc.loc) {
			t.Errorf("%s: locs %v, want %s", tc.body, locs(p), tc.loc)
		}
	}
	e.must(http.StatusOK, "POST", "/indexes/s/_search", `{"sort": [`+strings.Join(keys[:16], ",")+`]}`)
}
