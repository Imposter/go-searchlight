package api_test

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/config"
)

// benchDoc is a scrape-bot-shaped listing.
func benchDoc(i int) string {
	brands := []string{"Acme", "Globex", "Initech", "Umbrella", "Hooli"}
	return fmt.Sprintf(`{"title": "Listing %d oak chair with steel legs", "brand": %q, "price": %d, "tags": ["tag%d", "all"], "url": "https://example.com/item/%d"}`,
		i, brands[i%len(brands)], i%997, i%31, i)
}

func benchBulk(start, n int) string {
	var b strings.Builder
	for i := start; i < start+n; i++ {
		fmt.Fprintf(&b, "{\"upsert\":{\"id\":\"d%d\"}}\n%s\n", i, benchDoc(i))
	}
	return b.String()
}

func benchEnv(b *testing.B) *env {
	b.Helper()
	e := newEnv(b, envOpts{cfg: func(c *config.Config) { c.RefreshInterval = time.Second }})
	e.must(http.StatusCreated, "PUT", "/indexes/bench", `{"mapping": {"fields": {"title": "text", "brand": "keyword", "price": "number", "tags": "keyword_list", "url": "keyword"}}, "settings": {"shards": 2}}`)
	return e
}

// BenchmarkHTTPBulkIndex measures bulk indexing over HTTP: 1,000-document _bulk
// requests, each acknowledged once committed to SQLite (acknowledged = durable).
func BenchmarkHTTPBulkIndex(b *testing.B) {
	e := benchEnv(b)
	const batch = 1000
	bodies := make([]string, 0, 8)
	for i := range cap(bodies) {
		bodies = append(bodies, benchBulk(i*batch, batch))
	}
	b.ResetTimer()
	start := time.Now()
	for i := 0; b.Loop(); i++ {
		r := e.do("POST", "/indexes/bench/_bulk", bodies[i%len(bodies)])
		if r.status != http.StatusOK {
			b.Fatalf("bulk: %d %s", r.status, r.body)
		}
	}
	b.ReportMetric(float64(b.N*batch)/time.Since(start).Seconds(), "docs/s")
}

// BenchmarkHTTPSearch measures search latency over HTTP on 20,000 documents in two
// shards: a filtered, sorted page of 10 (query then fetch), with p50 and p99.
func BenchmarkHTTPSearch(b *testing.B) {
	e := benchEnv(b)
	var seq int64
	for i := range 20 {
		seq = seqOf(b, e.must(http.StatusOK, "POST", "/indexes/bench/_bulk", benchBulk(i*1000, 1000)))
	}
	e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/bench/_count?wait_for_seq=%d", seq), "")
	queries := []string{
		`{"query": {"all": [{"field": "brand", "op": "eq", "value": "Acme"}, {"field": "price", "op": "between", "value": [100, 400]}]}, "sort": [{"price": "desc"}], "size": 10}`,
		`{"query": {"field": "tags", "op": "has_any", "value": ["tag3", "tag7"]}, "size": 10}`,
		`{"query": {"field": "title", "op": "words_all", "value": ["oak", "chair"]}, "sort": ["price"], "size": 10, "aggs": {"b": {"terms": {"field": "brand"}}}}`,
	}
	var lat []time.Duration
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		t0 := time.Now()
		r := e.do("POST", "/indexes/bench/_search", queries[i%len(queries)])
		lat = append(lat, time.Since(t0))
		if r.status != http.StatusOK {
			b.Fatalf("search: %d %s", r.status, r.body)
		}
	}
	slices.Sort(lat)
	b.ReportMetric(float64(lat[len(lat)/2].Microseconds()), "p50-us")
	b.ReportMetric(float64(lat[len(lat)*99/100].Microseconds()), "p99-us")
}

// BenchmarkHTTPBulkPercolate measures _bulk?percolate=true: 500-document bulks
// against 1,000 saved queries.
func BenchmarkHTTPBulkPercolate(b *testing.B) {
	e := benchEnv(b)
	var ops []string
	for i := range 1000 {
		var q string
		switch i % 3 {
		case 0:
			q = fmt.Sprintf(`{"all": [{"field": "brand", "op": "eq", "value": "Acme"}, {"field": "price", "op": "lt", "value": %d}]}`, i)
		case 1:
			q = fmt.Sprintf(`{"field": "tags", "op": "has", "value": "tag%d"}`, i%31)
		default:
			q = fmt.Sprintf(`{"field": "title", "op": "contains", "value": "listing %d"}`, i)
		}
		ops = append(ops, q)
	}
	var seq int64
	for i, q := range ops {
		seq = seqOf(b, e.must(http.StatusOK, "PUT", fmt.Sprintf("/indexes/bench/queries/q%d", i), `{"query": `+q+`}`))
	}
	const batch = 500
	bodies := []string{benchBulk(0, batch), benchBulk(batch, batch)}
	path := fmt.Sprintf("/indexes/bench/_bulk?percolate=true&wait_for_seq=%d", seq)
	b.ResetTimer()
	start := time.Now()
	for i := 0; b.Loop(); i++ {
		r := e.do("POST", path, bodies[i%2])
		if r.status != http.StatusOK {
			b.Fatalf("bulk: %d %s", r.status, r.body)
		}
	}
	b.ReportMetric(float64(b.N*batch)/time.Since(start).Seconds(), "docs/s")
}
