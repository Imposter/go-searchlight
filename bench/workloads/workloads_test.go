package workloads

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/bench/datasets"
	"github.com/Imposter/go-searchlight/bench/report"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/testtier"
)

// fakeFootprintEngine answers Resources with a fixed sequence (repeating its last
// entry); every other Engine method is unused here and left to panic on a nil
// embedded Engine if ever called by mistake.
type fakeFootprintEngine struct {
	Engine
	name string
	mu   sync.Mutex
	seq  []report.Resources
	i    int
}

func (f *fakeFootprintEngine) Name() string { return f.name }

func (f *fakeFootprintEngine) Resources(context.Context, string) (report.Resources, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.seq[min(f.i, len(f.seq)-1)]
	f.i++
	return r, nil
}

func TestRunClosedLoopCountsOnlyMeasured(t *testing.T) {
	var calls atomic.Int64
	seen := make([]atomic.Bool, 150)
	m := Run(context.Background(), RunOptions{Warmup: 50, Iterations: 100, Concurrency: 4}, func(_ context.Context, i int) (int, time.Duration, error) {
		calls.Add(1)
		seen[i].Store(true)
		return 2, 0, nil
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

// TestFloorElapsedGuaranteesPositiveWhenOpsRan is the regression test for a flake in
// TestRunClosedLoopCountsOnlyMeasured: a fast closed loop's two time.Now() readings
// (both from Go's monotonic clock) landed close enough together that time.Since
// reported 0 on a coarse Windows timer, even though 100 real ops ran — and
// Throughput/DocsPerSec treat Elapsed<=0 as "no data", reporting a false 0 rate for
// work that demonstrably happened. 0 is not a legitimate answer whenever at least
// one op ran (it is legitimate when none did: nothing happened, instantaneously), so
// floorElapsed guarantees Elapsed is positive in the former case and leaves it alone
// in the latter.
func TestFloorElapsedGuaranteesPositiveWhenOpsRan(t *testing.T) {
	opsRan := &Measurement{Ops: 5, Elapsed: 0}
	floorElapsed(opsRan)
	if opsRan.Elapsed <= 0 {
		t.Fatalf("Elapsed = %v, want > 0 once ops ran", opsRan.Elapsed)
	}

	errorsOnly := &Measurement{Errors: 1, Elapsed: 0}
	floorElapsed(errorsOnly)
	if errorsOnly.Elapsed <= 0 {
		t.Fatalf("Elapsed = %v, want > 0 once an op ran, even if it only ever errored", errorsOnly.Elapsed)
	}

	nothingRan := &Measurement{Elapsed: 0}
	floorElapsed(nothingRan)
	if nothingRan.Elapsed != 0 {
		t.Fatalf("Elapsed = %v, want left at 0: nothing ran, so instantaneous is legitimate", nothingRan.Elapsed)
	}

	realElapsed := &Measurement{Ops: 3, Elapsed: 5 * time.Millisecond}
	floorElapsed(realElapsed)
	if realElapsed.Elapsed != 5*time.Millisecond {
		t.Fatalf("Elapsed = %v, want the real measured value left untouched", realElapsed.Elapsed)
	}
}

func TestRunErrorsAreCountedNotRecorded(t *testing.T) {
	boom := errors.New("boom")
	m := Run(context.Background(), RunOptions{Iterations: 20, Concurrency: 2}, func(_ context.Context, i int) (int, time.Duration, error) {
		if i%4 == 0 {
			return 0, 0, boom
		}
		return 1, 0, nil
	})
	if m.Errors != 5 || m.Ops != 15 || !errors.Is(m.FirstErr, boom) {
		t.Fatalf("errors %d ops %d first %v", m.Errors, m.Ops, m.FirstErr)
	}
}

func TestRunOpenLoopChargesQueueing(t *testing.T) {
	// 200 requests/s, each taking 20 ms, at most 2 in flight: the engine cannot keep up,
	// so later requests wait and their latency (from the scheduled start) grows.
	m := Run(context.Background(), RunOptions{Iterations: 20, Concurrency: 2, Rate: 200}, func(context.Context, int) (int, time.Duration, error) {
		time.Sleep(20 * time.Millisecond)
		return 1, 0, nil
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
	m := Run(context.Background(), RunOptions{Duration: 100 * time.Millisecond, Concurrency: 2}, func(context.Context, int) (int, time.Duration, error) {
		time.Sleep(time.Millisecond)
		return 1, 0, nil
	})
	if el := time.Since(start); el > 2*time.Second || m.Ops == 0 {
		t.Fatalf("ran %v, %d ops", el, m.Ops)
	}
}

// TestRunSelfTimedOverridesWallClock checks that an op's own reported latency
// replaces the call's wall time when it is nonzero (refresh=wait_for's untimed
// confirming search relies on this: the sleep below stands in for it).
func TestRunSelfTimedOverridesWallClock(t *testing.T) {
	m := Run(context.Background(), RunOptions{Iterations: 30, Concurrency: 1}, func(context.Context, int) (int, time.Duration, error) {
		time.Sleep(5 * time.Millisecond) // unmeasured tail work
		return 1, 2 * time.Millisecond, nil
	})
	s := m.Hist.Summary()
	if s.Max >= 4000 || s.Min < 1900 { // µs: ~2 ms, not the ~5 ms wall time
		t.Fatalf("min %v max %v µs: self-timed latency was not used", s.Min, s.Max)
	}
}

// TestFootprintSamplerRunningMax checks that the sampler keeps the largest disk and
// RSS it has seen across several samples, even when a later sample is smaller on one
// axis (a RSS spike followed by a disk spike must not erase either peak).
func TestFootprintSamplerRunningMax(t *testing.T) {
	eng := &fakeFootprintEngine{name: "x", seq: []report.Resources{
		{DiskBytes: 100, DiskSource: "a", RSSBytes: 200, RSSSource: "b"},
		{DiskBytes: 50, DiskSource: "a", RSSBytes: 500, RSSSource: "b"},  // RSS spikes
		{DiskBytes: 300, DiskSource: "a", RSSBytes: 100, RSSSource: "b"}, // disk spikes
	}}
	sampler := newFootprintSampler()
	for range 3 {
		sampler.sample(context.Background(), []Engine{eng}, "idx")
	}
	if p := sampler.peakOf("x"); p.DiskBytes != 300 || p.RSSBytes != 500 {
		t.Fatalf("peak = %+v, want disk 300 rss 500", p)
	}
	if p := sampler.peakOf("missing"); p.DiskBytes != 0 || p.RSSBytes != 0 {
		t.Fatalf("an engine never sampled: %+v, want zero", p)
	}
}

// TestStartFootprintSamplerStopWaitsForLastSample checks that stop only returns
// once the background goroutine has actually exited, so its peak is final and safe
// to read the moment stop returns (RunSuite relies on exactly this).
func TestStartFootprintSamplerStopWaitsForLastSample(t *testing.T) {
	old := footprintSampleInterval
	footprintSampleInterval = 2 * time.Millisecond
	defer func() { footprintSampleInterval = old }()

	eng := &fakeFootprintEngine{name: "x", seq: []report.Resources{{DiskBytes: 1, RSSBytes: 1}, {DiskBytes: 9, RSSBytes: 9}}}
	s := &suite{engines: []Engine{eng}, cfg: Config{Index: "idx"}}
	sampler, stop := s.startFootprintSampler(context.Background())
	time.Sleep(30 * time.Millisecond) // several ticks at 2 ms
	stop()
	if p := sampler.peakOf("x"); p.DiskBytes == 0 {
		t.Fatal("stop returned before any sample was taken")
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

// TestRetryDelayClampsRetryAfter checks that a server-given Retry-After is honored
// up to maxRetryAfter but never beyond it: a misbehaving (or hostile) server must
// not be able to stretch a single retry's wait, and so the whole request's total
// retry time across maxRetries of them, without bound.
func TestRetryDelayClampsRetryAfter(t *testing.T) {
	if got := retryDelay(0, time.Hour); got != maxRetryAfter {
		t.Fatalf("retryDelay with a 1h Retry-After = %v, want capped at maxRetryAfter (%v)", got, maxRetryAfter)
	}
	if got := retryDelay(0, maxRetryAfter); got != maxRetryAfter {
		t.Fatalf("retryDelay at exactly maxRetryAfter = %v, want it unchanged (%v)", got, maxRetryAfter)
	}
	const under = 10 * time.Second
	if got := retryDelay(0, under); got != under {
		t.Fatalf("retryDelay with a %v Retry-After (under the cap) = %v, want it unchanged", under, got)
	}
	// No Retry-After: the exponential backoff, itself bounded by retryMaxDelay (well
	// under maxRetryAfter), applies instead.
	if got := retryDelay(0, 0); got != retryBaseDelay {
		t.Fatalf("retryDelay(0, 0) = %v, want the base backoff %v", got, retryBaseDelay)
	}
}

// TestClientRetriesBackpressureThenSucceeds checks that a 429 (e.g. the shard write
// buffer backpressure a bulk load can hit) is retried, honoring Retry-After, instead
// of failing the request on the first refusal.
func TestClientRetriesBackpressureThenSucceeds(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) <= 2 {
			w.Header().Set("Retry-After", "0") // keep the test fast
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"detail":"the write buffer is full while refreshes catch up; retry"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	var log bytes.Buffer
	c := newClient(srv.URL, "", &log)
	b, err := c.do(context.Background(), http.MethodPost, "/x", "", nil)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	_ = b
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3 (2 retried 429s then a 200)", calls.Load())
	}
	if !strings.Contains(log.String(), "HTTP 429") {
		t.Fatalf("log = %q, want it to mention the 429s it retried", log.String())
	}
}

// TestClientRetryExhaustedReturnsRealStatusError checks that once retries run out,
// the caller gets the actual *StatusError (its status and body), never a bare
// context error that would hide why the request failed. The short tier retries
// twice, the heavy one maxRetries times through the whole backoff.
func TestClientRetryExhaustedReturnsRealStatusError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "0")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("draining"))
	}))
	defer srv.Close()
	var log bytes.Buffer
	c := newClient(srv.URL, "", &log)
	c.retries = testtier.Pick(2, maxRetries)
	_, err := c.do(context.Background(), http.MethodPost, "/x", "", nil)
	var st *StatusError
	if !errors.As(err, &st) || st.Status != http.StatusServiceUnavailable {
		t.Fatalf("err = %v (%T), want a 503 *StatusError", err, err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v: a context error masking the real 503", err)
	}
}

// fakeBulkEngine is an Engine whose Bulk fails once, on a chosen call, with a
// caller-given error, and otherwise succeeds after a short delay (so a failure can
// race ahead of its concurrent siblings, as in the real incident).
type fakeBulkEngine struct {
	Engine
	mu      sync.Mutex
	calls   int
	failAt  int
	failErr error
}

func (f *fakeBulkEngine) Name() string { return "fake" }

func (f *fakeBulkEngine) Bulk(ctx context.Context, _ string, _ []Doc, _ string) error {
	f.mu.Lock()
	i := f.calls
	f.calls++
	f.mu.Unlock()
	if i == f.failAt {
		return f.failErr
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(20 * time.Millisecond):
	}
	return nil
}

// TestBulkLoadSurfacesRealErrorNotContextCanceled is the regression test for the
// incident this fixes: one worker's bulk request fails (here with the 429 a
// backpressured shard answers), bulkLoad cancels the others so they stop early, and
// the error that reaches the caller must be the real cause, not the "context
// canceled" their own canceled-context sends turn into.
func TestBulkLoadSurfacesRealErrorNotContextCanceled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "products.ndjson")
	var b strings.Builder
	for i := range 40 {
		fmt.Fprintf(&b, `{"id":"p%d","doc":{"x":1}}`+"\n", i)
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}

	boom := &StatusError{
		Method: "POST", URL: "http://x/_bulk", Status: http.StatusTooManyRequests,
		Body: "the write buffer is full while refreshes catch up; retry",
	}
	eng := &fakeBulkEngine{failAt: 1, failErr: boom}
	s := &suite{
		engines: []Engine{eng},
		cfg:     Config{Index: "idx", DataFile: path, BulkBatch: 2, BulkConcurrency: 4, Log: io.Discard},
		run:     &report.Run{},
	}

	_, _, err := s.bulkLoad(context.Background(), eng)
	if err == nil {
		t.Fatal("want an error")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("bulkLoad returned %v: context.Canceled is hiding the real cause", err)
	}
	var st *StatusError
	if !errors.As(err, &st) || st.Status != http.StatusTooManyRequests {
		t.Fatalf("err = %v (%T), want the *StatusError (429) that actually failed the load", err, err)
	}
}

// TestWriteJitterDeterministicAndBounded checks writeJitter's contract: uniform on
// [0, interval), the same (seed, workload, i) always gives the same delay (so a run
// is reproducible and, at a given i, identical across engines), and different i
// mostly gives different delays (it is not a constant in disguise).
func TestWriteJitterDeterministicAndBounded(t *testing.T) {
	const interval = 150 * time.Millisecond
	seen := map[time.Duration]bool{}
	for i := range 50 {
		d := writeJitter(7, "refresh_visible", i, interval)
		if d < 0 || d >= interval {
			t.Fatalf("writeJitter(i=%d) = %v, want [0, %v)", i, d, interval)
		}
		if again := writeJitter(7, "refresh_visible", i, interval); again != d {
			t.Fatalf("writeJitter(i=%d) = %v then %v: not deterministic", i, d, again)
		}
		seen[d] = true
	}
	if len(seen) < 25 {
		t.Fatalf("only %d distinct delays across 50 i's, want a spread, not a near-constant", len(seen))
	}
	// A different workload name must not collapse to the same sequence (refresh_visible
	// and refresh_wait_for, run back to back for the same engine, would otherwise apply
	// identical delays at identical i's for no reason).
	if writeJitter(7, "refresh_wait_for", 3, interval) == writeJitter(7, "refresh_visible", 3, interval) {
		t.Fatalf("refresh_visible and refresh_wait_for got the same delay at i=3: want them independent")
	}
	// interval 0 (an engine with refresh disabled) never sleeps.
	if d := writeJitter(7, "refresh_visible", 0, 0); d != 0 {
		t.Fatalf("writeJitter with interval 0 = %v, want 0", d)
	}
}

// fakeVisEngine is an Engine whose Bulk and Search each advance a fake clock by a
// fixed, known amount and then succeed (Prepare needs no real translation): standing
// in for a real engine's write-then-poll in
// TestVisibilityExcludesJitterFromMeasuredLatency. It never really sleeps, so that
// test is exact and deterministic instead of a wall-clock comparison against a
// possibly busy, shared CI runner's scheduling noise.
type fakeVisEngine struct {
	Engine
	name    string
	advance func(time.Duration)
	work    time.Duration
}

func (f *fakeVisEngine) Name() string { return f.name }

type fakePrepared struct{}

func (fakePrepared) isPrepared() {}

func (f *fakeVisEngine) Prepare([]byte) (Prepared, error) { return fakePrepared{}, nil }

func (f *fakeVisEngine) Bulk(context.Context, string, []Doc, string) error {
	f.advance(f.work)
	return nil
}

func (f *fakeVisEngine) Search(context.Context, string, Prepared, []any) (SearchResult, error) {
	f.advance(f.work)
	return SearchResult{Total: 1, Relation: "eq"}, nil
}

// TestVisibilityExcludesJitterFromMeasuredLatency is the regression test for the
// coordinator's requested change: the visibility workloads must sleep a random,
// seeded, per-iteration delay before a measured write (so writes do not land in
// lockstep right after the previous refresh), and that delay must not count toward
// the recorded latency.
//
// It injects the fake clock and sleeper suite.now/suite.sleep stand in for, instead
// of asserting on a wall-clock margin (which flaked under package-wide load: a
// coarse Windows timer plus scheduling noise from other tests running at once can
// stretch even a few milliseconds of real work well past a tight threshold). The
// fake clock only advances when told to: the injected sleeper advances it by exactly
// the jitter delay without really blocking, and fakeVisEngine's Bulk/Search each
// advance it by a known, fixed work duration. So the recorded latency is exactly
// computable: 2*fakeWork if the jitter is excluded, or that plus the jitter delay if
// it leaked in, nothing in between and nothing left to timing chance.
func TestVisibilityExcludesJitterFromMeasuredLatency(t *testing.T) {
	const (
		interval   = 500 * time.Millisecond
		fakeWork   = 2 * time.Millisecond
		iterations = 5
	)
	var clock atomic.Int64 // fake nanoseconds since an arbitrary epoch
	now := func() time.Time { return time.Unix(0, clock.Load()) }
	advance := func(d time.Duration) { clock.Add(int64(d)) }

	eng := &fakeVisEngine{name: "fake", advance: advance, work: fakeWork}
	s := &suite{
		engines: []Engine{eng},
		cfg: Config{
			Index: "idx", Seed: 1, VisibleIterations: iterations,
			Only: []string{"refresh_visible"}, Log: io.Discard,
		},
		run: &report.Run{Engines: []report.EngineInfo{
			{Name: "fake", Config: map[string]string{"refresh_interval": interval.String()}},
		}},
		now:   now,
		sleep: func(_ context.Context, d time.Duration) error { advance(d); return nil },
	}

	if err := s.visibility(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Every iteration (warmup, min(2, iterations), plus the measured ones) sleeps a
	// jitter delay and does the fake work: the fake clock's final value must equal
	// that exactly, proving the delay really was applied at every iteration, not
	// just the measured ones.
	total := min(2, iterations) + iterations
	var wantClock int64
	for i := range total {
		wantClock += int64(writeJitter(1, "refresh_visible", i, interval)) + 2*int64(fakeWork)
	}
	if got := clock.Load(); got != wantClock {
		t.Fatalf("fake clock ended at %v, want exactly %v: the jitter delay was not applied at every iteration", time.Duration(got), time.Duration(wantClock))
	}

	r := findResult(s.run.Results, "refresh_visible", "fake")
	if r == nil || r.Latency == nil {
		t.Fatal("no refresh_visible/fake result with latency")
	}
	wantMicros := float64(2*fakeWork) / float64(time.Microsecond)
	// The histogram is HDR-style (0.1% precision), not exact storage, so compare
	// with a tolerance well above that but minuscule next to the 500 ms interval a
	// leaked jitter would add.
	if tol := wantMicros * 0.05; r.Latency.Min < wantMicros-tol || r.Latency.Max > wantMicros+tol {
		t.Fatalf("recorded latency min %g µs max %g µs, want %g µs ± %g%%: the jitter leaked into the measurement",
			r.Latency.Min, r.Latency.Max, wantMicros, 5.0)
	}
}

// findResult returns the result for workload/engine, or nil.
func findResult(rs []report.Result, workload, engine string) *report.Result {
	for i := range rs {
		if rs[i].Workload == workload && rs[i].Engine == engine {
			return &rs[i]
		}
	}
	return nil
}
