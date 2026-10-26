package api_test

import (
	"fmt"
	"net/http"
	"testing"
)

// A count or a size-0 search with max_staleness may be answered with a result each
// shard computed within that long, the writes refreshed since unseen; wait_for_seq
// overrides it (read-your-writes), and one past 1h or not a duration is refused.
func TestMaxStalenessServesCachedResults(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("s", `{"mapping": {"fields": {"brand": "keyword"}}, "settings": {"shards": 2}}`)
	var lines []string
	for i := range 20 {
		lines = append(lines, fmt.Sprintf(`{"upsert": {"id": "a%d"}}`, i), `{"brand": "acme"}`)
	}
	seq := seqOf(t, e.must(http.StatusOK, "POST", "/indexes/s/_bulk?refresh=wait_for", ndjson(lines...)))
	count := func(path, body string) float64 {
		t.Helper()
		n, _ := e.must(http.StatusOK, "POST", path, body)["count"].(float64)
		return n
	}
	const stale = `{"query": {"field": "brand", "op": "eq", "value": "acme"}, "max_staleness": "1h"}`
	if n := count(fmt.Sprintf("/indexes/s/_count?wait_for_seq=%d", seq), stale); n != 20 {
		t.Fatalf("count %v, want 20", n)
	}
	if n := count("/indexes/s/_count", stale); n != 20 {
		t.Fatalf("count %v, want 20", n)
	}
	seq = seqOf(t, e.must(http.StatusOK, "PUT", "/indexes/s/docs/b?refresh=wait_for", `{"brand": "acme"}`))
	if n := count("/indexes/s/_count", stale); n != 20 {
		t.Fatalf("a count within max_staleness = %v, want the cached 20", n)
	}
	agg := `{"size": 0, "max_staleness": 3600000, "aggs": {"b": {"terms": {"field": "brand"}}}}`
	e.must(http.StatusOK, "POST", "/indexes/s/_search", agg)
	if n := count(fmt.Sprintf("/indexes/s/_count?wait_for_seq=%d", seq), stale); n != 21 {
		t.Fatalf("with wait_for_seq a stale count = %v, want 21", n)
	}
	if n := count("/indexes/s/_count", `{"query": {"field": "brand", "op": "eq", "value": "acme"}}`); n != 21 {
		t.Fatalf("a current count = %v, want 21", n)
	}
	e.problem(e.do("POST", "/indexes/s/_count", `{"max_staleness": "2h"}`), 400, "invalid_request")
	e.problem(e.do("POST", "/indexes/s/_search", `{"size": 0, "max_staleness": "soon"}`), 400, "invalid_request")
}
