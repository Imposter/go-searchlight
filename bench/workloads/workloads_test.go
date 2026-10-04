package workloads

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/bench/datasets"
	"github.com/Imposter/go-searchlight/internal/search"
)

func TestRunClosedLoopCountsOnlyMeasured(t *testing.T) {
	var calls atomic.Int64
	seen := make([]atomic.Bool, 150)
	m := Run(context.Background(), RunOptions{Warmup: 50, Iterations: 100, Concurrency: 4}, func(_ context.Context, i int) (int, error) {
		calls.Add(1)
		seen[i].Store(true)
		return 2, nil
	})
	if calls.Load() != 150 || m.Ops != 100 || m.Docs != 200 || m.Hist.Count() != 100 || m.Errors != 0 {
		t.Fatalf("calls %d ops %d docs %d recorded %d errors %d", calls.Load(), m.Ops, m.Docs, m.Hist.Count(), m.Errors)
	}
	for i := range seen {
		if !seen[i].Load() {
			t.Fatalf("iteration %d never ran: warmup and measured iterations must be numbered 0..149", i)
		}
	}
	if m.Throughput() <= 0 || m.DocsPerSec() != 2*m.Throughput() {
		t.Fatalf("throughput %v docs/s %v", m.Throughput(), m.DocsPerSec())
	}
}

func TestRunErrorsAreCountedNotRecorded(t *testing.T) {
	boom := errors.New("boom")
	m := Run(context.Background(), RunOptions{Iterations: 20, Concurrency: 2}, func(_ context.Context, i int) (int, error) {
		if i%4 == 0 {
			return 0, boom
		}
		return 1, nil
	})
	if m.Errors != 5 || m.Ops != 15 || !errors.Is(m.FirstErr, boom) {
		t.Fatalf("errors %d ops %d first %v", m.Errors, m.Ops, m.FirstErr)
	}
}

func TestRunOpenLoopChargesQueueing(t *testing.T) {
	// 200 requests/s, each taking 20 ms, at most 2 in flight: the engine cannot keep up,
	// so later requests wait and their latency (from the scheduled start) grows.
	m := Run(context.Background(), RunOptions{Iterations: 20, Concurrency: 2, Rate: 200}, func(context.Context, int) (int, error) {
		time.Sleep(20 * time.Millisecond)
		return 1, nil
	})
	if m.Ops != 20 {
		t.Fatalf("ops %d", m.Ops)
	}
	s := m.Hist.Summary()
	if s.Max < 50_000 { // µs: the last requests queue for well over 50 ms
		t.Fatalf("max latency %v µs: queueing behind a full window is not charged", s.Max)
	}
	if s.Min < 19_000 {
		t.Fatalf("min latency %v µs is below the request's own 20 ms", s.Min)
	}
}

func TestRunDurationBound(t *testing.T) {
	start := time.Now()
	m := Run(context.Background(), RunOptions{Duration: 100 * time.Millisecond, Concurrency: 2}, func(context.Context, int) (int, error) {
		time.Sleep(time.Millisecond)
		return 1, nil
	})
	if el := time.Since(start); el > 2*time.Second || m.Ops == 0 {
		t.Fatalf("ran %v, %d ops", el, m.Ops)
	}
}

func TestWithID(t *testing.T) {
	for in, want := range map[string]string{
		`{"a": 1}`:    `{"sl_id":"p1","a": 1}`,
		` { } `:       `{"sl_id":"p1"}`,
		"{\n\"b\":2}": "{\"sl_id\":\"p1\",\"b\":2}",
	} {
		if got := string(withID(json.RawMessage(in), "p1")); got != want {
			t.Errorf("withID(%q) = %q, want %q", in, got, want)
		}
	}
}

func parseReq(t *testing.T, body string) *search.Request {
	t.Helper()
	r, ps := search.ParseRequest([]byte(body))
	if len(ps) > 0 {
		t.Fatal(ps)
	}
	return r
}

// TestDiffSearchCanonicalAggs checks that the engines' aggregation shapes compare
// equal where they mean the same: bool terms keys, range bucket order, stats sums
// within tolerance, cardinality within 3%.
func TestDiffSearchCanonicalAggs(t *testing.T) {
	req := parseReq(t, `{"size": 0, "aggs": {
		"b": {"terms": {"field": "in_stock"}},
		"r": {"range": {"field": "price", "ranges": [{"to": 10}, {"from": 10}]}},
		"s": {"stats": {"field": "price"}},
		"c": {"cardinality": {"field": "brand"}},
		"t": {"terms": {"field": "brand"}, "aggs": {"p": {"stats": {"field": "price"}}}}}}`)
	sl := SearchResult{Total: 10, Relation: "eq", Aggs: json.RawMessage(`{
		"b": {"doc_count_error_upper_bound": 0, "sum_other_doc_count": 0, "buckets": [{"key": true, "doc_count": 7}, {"key": false, "doc_count": 3}]},
		"r": {"buckets": [{"key": "*-10", "to": 10, "doc_count": 4}, {"key": "10-*", "from": 10, "doc_count": 6}]},
		"s": {"count": 10, "min": 1, "max": 50, "avg": 12.3, "sum": 123.00000000000001},
		"c": {"value": 1000},
		"t": {"doc_count_error_upper_bound": 0, "sum_other_doc_count": 0, "buckets": [{"key": "acme", "doc_count": 6, "p": {"count": 6, "min": 1, "max": 9, "avg": 5, "sum": 30}}]}}`)}
	es := SearchResult{Total: 10, Relation: "eq", Aggs: json.RawMessage(`{
		"b": {"doc_count_error_upper_bound": 0, "sum_other_doc_count": 0, "buckets": [{"key": 1, "key_as_string": "true", "doc_count": 7}, {"key": 0, "key_as_string": "false", "doc_count": 3}]},
		"r": {"buckets": [{"key": "10-*", "from": 10.0, "doc_count": 6}, {"key": "*-10", "to": 10.0, "doc_count": 4}]},
		"s": {"count": 10, "min": 1.0, "max": 50.0, "avg": 12.3, "sum": 123.0},
		"c": {"value": 1020},
		"t": {"doc_count_error_upper_bound": 0, "sum_other_doc_count": 0, "buckets": [{"key": "acme", "doc_count": 6, "p": {"count": 6, "min": 1.0, "max": 9.0, "avg": 5.0, "sum": 30.0}}]}}`)}
	if ps, _ := diffSearch(req, sl, es); len(ps) > 0 {
		t.Fatalf("equal answers reported different: %v", ps)
	}

	es.Aggs = json.RawMessage(strings.Replace(string(es.Aggs), `"value": 1020`, `"value": 1100`, 1))
	es.Aggs = json.RawMessage(strings.Replace(string(es.Aggs), `"doc_count": 7}`, `"doc_count": 8}`, 1))
	ps, tol := diffSearch(req, sl, es)
	if len(ps) != 2 || tol {
		t.Fatalf("problems %v tolerated %v; want a cardinality and a bucket difference, not tolerated", ps, tol)
	}
}

func TestDiffSearchHitsAndTolerance(t *testing.T) {
	req := parseReq(t, `{"size": 3, "aggs": {"t": {"terms": {"field": "brand", "size": 1}}}}`)
	sl := SearchResult{IDs: []string{"a", "b", "c"}, Total: 3, Relation: "eq", Aggs: json.RawMessage(`{"t": {"doc_count_error_upper_bound": 2, "buckets": [{"key": "x", "doc_count": 5}]}}`)}
	es := SearchResult{IDs: []string{"a", "b", "c"}, Total: 3, Relation: "eq", Aggs: json.RawMessage(`{"t": {"doc_count_error_upper_bound": 0, "buckets": [{"key": "x", "doc_count": 6}]}}`)}
	ps, tol := diffSearch(req, sl, es)
	if len(ps) != 1 || !tol {
		t.Fatalf("a terms difference under a nonzero error bound: problems %v tolerated %v", ps, tol)
	}
	es.IDs = []string{"a", "c", "b"}
	ps, tol = diffSearch(req, sl, es)
	if tol || !strings.Contains(strings.Join(ps, ";"), "first difference at 1: b vs c") {
		t.Fatalf("problems %v tolerated %v", ps, tol)
	}
}

func TestDiffPercolate(t *testing.T) {
	if ps := diffPercolate([][]string{{"q2", "q1"}, nil}, [][]string{{"q1", "q2"}, {}}); len(ps) != 0 {
		t.Fatalf("same sets in another order: %v", ps)
	}
	ps := diffPercolate([][]string{{"q1"}}, [][]string{{"q1", "q9"}})
	if len(ps) != 1 || !strings.Contains(ps[0], "only elasticsearch [q9]") {
		t.Fatalf("%v", ps)
	}
}

// fakeES answers a percolate with more matches than one page, so the engine must page
// by qid and collect every slot.
func TestElasticsearchPercolatePaging(t *testing.T) {
	var requests atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		type hit struct {
			ID     string           `json:"_id"`
			Sort   []any            `json:"sort,omitempty"`
			Fields map[string][]int `json:"fields"`
		}
		total := percolatePage + 2
		var hits []hit
		after, paging := body["search_after"].([]any)
		switch {
		case !paging: // the first page: not all of them
			for i := range percolatePage {
				hits = append(hits, hit{ID: qid(i), Fields: map[string][]int{"_percolator_document_slot": {i % 2}}})
			}
		case after[0] == "":
			for i := range percolatePage {
				hits = append(hits, hit{ID: qid(i), Sort: []any{qid(i)}, Fields: map[string][]int{"_percolator_document_slot": {i % 2}}})
			}
		default:
			for i := percolatePage; i < total; i++ {
				hits = append(hits, hit{ID: qid(i), Sort: []any{qid(i)}, Fields: map[string][]int{"_percolator_document_slot": {0, 1}}})
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"hits": map[string]any{"total": map[string]any{"value": total}, "hits": hits}})
	}))
	defer srv.Close()
	e := NewElasticsearch(ElasticsearchOptions{URL: srv.URL})
	out, err := e.Percolate(context.Background(), "q", []json.RawMessage{json.RawMessage(`{}`), json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 3 || len(out[0]) != percolatePage/2+2 || len(out[1]) != percolatePage/2+2 {
		t.Fatalf("%d requests, %d and %d matches", requests.Load(), len(out[0]), len(out[1]))
	}
}

func qid(i int) string { return datasets.SearchID(int64(i)) }

func TestSearchSpecsTranslateAndCover(t *testing.T) {
	specs := SearchSpecs(1, 8, 1000)
	groups := map[string]int{}
	e := NewElasticsearch(ElasticsearchOptions{URL: "http://unused"})
	s := NewSearchlight(SearchlightOptions{URL: "http://unused"})
	for _, spec := range specs {
		groups[spec.Group]++
		if len(spec.Bodies) != 8 {
			t.Fatalf("%s: %d variants", spec.Name, len(spec.Bodies))
		}
		for _, b := range spec.Bodies {
			if _, err := e.Prepare(b); err != nil {
				t.Fatalf("%s: %v: %s", spec.Name, err, b)
			}
			if _, err := s.Prepare(b); err != nil {
				t.Fatalf("%s: %v", spec.Name, err)
			}
		}
	}
	for _, g := range []string{"filter", "sorted", "aggs"} {
		if groups[g] == 0 {
			t.Errorf("no %s workload", g)
		}
	}
	var doc map[string]any
	if err := json.Unmarshal(datasets.AppendProduct(nil, 1, 5), &doc); err != nil {
		t.Fatal(err)
	}
	for _, c := range CoverageChecks(doc) {
		if _, err := e.Prepare(c.Bodies[0]); err != nil {
			t.Fatalf("%s: %v: %s", c.Name, err, c.Bodies[0])
		}
	}
	// Same seed, same bodies.
	again := SearchSpecs(1, 8, 1000)
	for i := range specs {
		for j := range specs[i].Bodies {
			if !bytes.Equal(specs[i].Bodies[j], again[i].Bodies[j]) {
				t.Fatalf("%s variant %d differs between two calls", specs[i].Name, j)
			}
		}
	}
}
