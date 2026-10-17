package search

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/Imposter/go-searchlight/internal/reqcache"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// rcShard is one shard copy for the request cache's tests: an index incarnation,
// a mapping version and, at each refresh, a changelog mark, so its generations are
// shareable.
type rcShard struct {
	t   testing.TB
	s   *shard.Shard
	seq int64
	own int64 // the seq of the shard's newest change
}

// testContent stands in for replica.OwnContentMark: rcShard's own marks name the
// shard's newest change as they are.
func testContent(own json.RawMessage, _ int64) json.RawMessage { return own }

func newRCShard(t testing.TB) *rcShard {
	t.Helper()
	s, err := shard.Open(context.Background(), t.TempDir(), testMapping, shard.Options{
		RefreshInterval: -1, DisableMerges: true, Logger: quiet, Index: "products", Shard: 0,
	})
	if err != nil {
		t.Fatalf("shard.Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	r := &rcShard{t: t, s: s}
	r.apply(shard.Change{Kind: shard.Remap, Mapping: testMapping, MappingVersion: 1})
	return r
}

func (r *rcShard) apply(ch shard.Change) {
	r.t.Helper()
	r.seq++
	r.own = r.seq
	ch.Seq, ch.IndexUID = r.seq, "uid-products"
	if err := r.s.Apply(context.Background(), []shard.Change{ch}); err != nil {
		r.t.Fatalf("Apply: %v", err)
	}
}

func (r *rcShard) put(id, body string) {
	r.t.Helper()
	d, _, err := schema.Analyze(testMapping, id, []byte(body))
	if err != nil {
		r.t.Fatalf("Analyze: %v", err)
	}
	r.apply(shard.Change{Kind: shard.Upsert, Doc: &d})
}

func (r *rcShard) del(id string) {
	r.t.Helper()
	r.apply(shard.Change{Kind: shard.Delete, DocID: id})
}

// refresh publishes what was applied, with the own mark every copy records of the
// shard's newest change.
func (r *rcShard) refresh() {
	r.t.Helper()
	r.s.SetOwnMark(json.RawMessage(fmt.Sprintf(`{"seq":%d,"nonce":7}`, r.own)))
	if err := r.s.Refresh(context.Background()); err != nil {
		r.t.Fatalf("Refresh: %v", err)
	}
}

func (r *rcShard) search(c *RequestCache, req *Request) *ShardResult {
	r.t.Helper()
	res, err := r.searchCtx(context.Background(), c, req)
	if err != nil {
		r.t.Fatalf("Search: %v", err)
	}
	return res
}

func (r *rcShard) searchCtx(ctx context.Context, c *RequestCache, req *Request) (*ShardResult, error) {
	g := r.s.Acquire()
	defer g.Release()
	return c.Search(ctx, g, req)
}

func (r *rcShard) fill(n int) {
	r.t.Helper()
	brands := []string{"acme", "initech", "globex", "umbrella"}
	for i := range n {
		r.put(fmt.Sprintf("d%d", i), fmt.Sprintf(`{"brand":%q,"tags":["t%d","t%d"],"price":%d,"active":%t,"created":%d}`,
			brands[i%len(brands)], i%7, i%11, i%100, i%2 == 0, int64(1_700_000_000_000)+int64(i)*3_600_000))
	}
	r.refresh()
}

const rcAggs = `{"size":0,"aggs":{
	"brands":{"terms":{"field":"brand"},"aggs":{"p":{"stats":{"field":"price"}}}},
	"tags":{"cardinality":{"field":"tags"}},
	"active":{"terms":{"field":"active"}},
	"bands":{"range":{"field":"price","ranges":[{"to":50},{"from":50}]}},
	"hist":{"histogram":{"field":"price","interval":25}},
	"f":{"filters":{"filters":{"cheap":{"field":"price","op":"lt","value":20}},"other_bucket_key":"rest"},
	     "aggs":{"b":{"terms":{"field":"brand"}}}}}}`

func parseReq(t testing.TB, raw string) *Request {
	t.Helper()
	r, ps := ParseRequest([]byte(raw))
	if len(ps) > 0 {
		t.Fatalf("ParseRequest(%s): %v", raw, ps)
	}
	return r
}

// sameResult reports whether a and b are the same result, as peers would send them.
func sameResult(t testing.TB, a, b *ShardResult) bool {
	t.Helper()
	ja, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	jb, err := json.Marshal(b)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Equal(ja, jb)
}

func reduceOne(r *Request, res *ShardResult) *Response {
	return Reduce(context.Background(), []*ShardResult{res}, r)
}

func wantStats(t *testing.T, c *RequestCache, hits, misses int64) {
	t.Helper()
	if s := c.Stats(); s.Hits != hits || s.Misses != misses {
		t.Fatalf("stats %v, want hits=%d misses=%d", s, hits, misses)
	}
}

func TestRequestCacheHitIsIdentical(t *testing.T) {
	s := newRCShard(t)
	s.fill(200)
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20})
	req := parseReq(t, rcAggs)
	first := s.search(c, req)
	second := s.search(c, parseReq(t, rcAggs))
	wantStats(t, c, 1, 1)
	if !sameResult(t, first, second) {
		t.Fatalf("the hit differs from the miss:\n%+v\n%+v", first, second)
	}
	uncached := s.search(nil, req)
	if !reflect.DeepEqual(reduceOne(req, second), reduceOne(req, uncached)) {
		t.Fatal("the cached result reduces differently from a fresh one")
	}
	if st := c.Stats(); st.Entries != 1 || st.Bytes <= 0 {
		t.Fatalf("stats %v: want one entry with bytes", st)
	}
}

func TestRequestCacheKeyIgnoresTimeoutAndIndexLabel(t *testing.T) {
	a := parseReq(t, `{"size":0,"timeout":"1s","aggs":{"b":{"terms":{"field":"brand"}}}}`)
	b := parseReq(t, `{"size":0,"timeout":"5s","sort":[{"price":"desc"}],"aggregations":{"b":{"terms":{"field":"brand"}}}}`)
	b.Index = "other"
	ka, _ := requestCacheKey(a)
	kb, _ := requestCacheKey(b)
	if !bytes.Equal(ka, kb) {
		t.Fatal("the timeout, the sort, the aggs spelling or the index label changed the key")
	}
	for _, raw := range []string{
		`{"size":0,"aggs":{"b":{"terms":{"field":"brand","size":5}}}}`,
		`{"size":0,"aggs":{"c":{"terms":{"field":"brand"}}}}`,
		`{"size":0,"track_total":true,"aggs":{"b":{"terms":{"field":"brand"}}}}`,
		`{"size":0,"query":{"field":"price","op":"lt","value":5},"aggs":{"b":{"terms":{"field":"brand"}}}}`,
		`{"size":0,"post_filter":{"field":"price","op":"lt","value":5},"aggs":{"b":{"terms":{"field":"brand"}}}}`,
		`{"size":0,"aggs":{"b":{"filter":{"field":"price","op":"lt","value":5}}}}`,
		`{"size":0,"aggs":{"b":{"filter":{"field":"price","op":"lt","value":6}}}}`,
		`{"size":0,"aggs":{"b":{"terms":{"field":"brand","collect_mode":"breadth_first"}}}}`,
		`{"size":0,"aggs":{"b":{"min":{"field":"price"}}}}`,
		`{"size":0,"aggs":{"b":{"max":{"field":"price"}}}}`,
		`{"size":0,"aggs":{"b":{"cardinality":{"field":"brand"}}}}`,
		`{"size":0,"aggs":{"b":{"cardinality":{"field":"brand","threshold":2}}}}`,
		`{"size":0,"aggs":{"b":{"cardinality":{"field":"brand","threshold":3}}}}`,
	} {
		k, ok := requestCacheKey(parseReq(t, raw))
		if !ok || bytes.Equal(k, ka) {
			t.Errorf("%s: key %v the same as the base's (cacheable %v)", raw, k, ok)
		}
	}
}

func TestRequestCacheRefreshWriteMisses(t *testing.T) {
	s := newRCShard(t)
	s.fill(50)
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20})
	req := parseReq(t, `{"size":0,"aggs":{"brands":{"terms":{"field":"brand"}}}}`)
	before := s.search(c, req)
	s.put("new", `{"brand":"zeta","price":1}`)
	if got := s.search(c, req); !sameResult(t, got, before) {
		t.Fatal("an unrefreshed write changed the result")
	}
	wantStats(t, c, 1, 1)
	s.refresh()
	after := s.search(c, req)
	wantStats(t, c, 1, 2)
	if after.Total != before.Total+1 {
		t.Fatalf("total %d after the write, want %d", after.Total, before.Total+1)
	}
	if st := c.Stats(); st.Entries != 1 {
		t.Fatalf("stats %v: the older generation's entry was not dropped", st)
	}
	s.search(c, req)
	wantStats(t, c, 2, 2)
}

func TestRequestCacheDeleteMisses(t *testing.T) {
	s := newRCShard(t)
	s.fill(50)
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20})
	req := parseReq(t, `{"size":0}`)
	before := s.search(c, req)
	s.del("d3")
	s.refresh()
	after := s.search(c, req)
	wantStats(t, c, 0, 2)
	if after.Total != before.Total-1 {
		t.Fatalf("total %d after the delete, want %d", after.Total, before.Total-1)
	}
}

func TestRequestCacheMergeMissesWithTheSameResult(t *testing.T) {
	s := newRCShard(t)
	for i := range 4 {
		s.put(fmt.Sprintf("a%d", i), fmt.Sprintf(`{"brand":"b%d","price":%d}`, i%2, i))
		s.refresh()
	}
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20})
	req := parseReq(t, `{"size":0,"aggs":{"brands":{"terms":{"field":"brand"}},"p":{"stats":{"field":"price"}}}}`)
	before := s.search(c, req)
	if err := s.s.ForceMerge(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	after := s.search(c, req)
	wantStats(t, c, 0, 2)
	if !reflect.DeepEqual(reduceOne(req, before), reduceOne(req, after)) {
		t.Fatal("the merge changed the result")
	}
}

func TestRequestCacheSeqOnlyRefreshStillHits(t *testing.T) {
	s := newRCShard(t)
	s.fill(20)
	st := newMemStore()
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20, Store: st, ContentMark: testContent})
	req := parseReq(t, `{"size":0}`)
	s.search(c, req)
	if err := s.s.Advance(s.seq + 100); err != nil {
		t.Fatal(err)
	}
	s.seq += 100
	s.refresh()
	s.search(c, req)
	s.search(c, req)
	wantStats(t, c, 2, 1)
	c.writes.Wait()
	// Writes elsewhere moved the seq, not what the shard holds: its store key stands,
	// and a copy on another node at the new seq reads it.
	if n := st.writes.Load(); n != 1 {
		t.Fatalf("%d store writes, want 1", n)
	}
	other := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20, Store: st, ContentMark: testContent})
	s.search(other, req)
	if got := other.Stats(); got.StoreHits != 1 {
		t.Fatalf("another node's stats %v: want a store hit at the new seq", got)
	}
}

func TestRequestCacheNeverCaches(t *testing.T) {
	s := newRCShard(t)
	s.fill(20)
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20})
	for _, raw := range []string{
		`{"size":0,"request_cache":false,"aggs":{"b":{"terms":{"field":"brand"}}}}`,
		`{"size":5,"aggs":{"b":{"terms":{"field":"brand"}}}}`,
		`{"size":5,"request_cache":true}`,
		`{"size":0,"from":5}`,
		`{"size":0,"search_after":["x"]}`,
	} {
		for range 2 {
			s.search(c, parseReq(t, raw))
		}
	}
	if st := c.Stats(); st.Hits+st.Misses != 0 || st.Entries != 0 {
		t.Fatalf("stats %v: an uncacheable search was cached", st)
	}
	if _, ps := ParseRequest([]byte(`{"request_cache":"no"}`)); len(ps) != 1 || ps[0].Loc != "request_cache" {
		t.Fatalf("problems %v, want one at request_cache", ps)
	}
}

func TestRequestCacheSkipsTimedOutResults(t *testing.T) {
	s := newRCShard(t)
	s.fill(20)
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20})
	req := parseReq(t, `{"size":0,"aggs":{"b":{"terms":{"field":"brand"}}}}`)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	res, err := s.searchCtx(ctx, c, req)
	if err != nil {
		t.Fatal(err)
	}
	if !res.TimedOut {
		t.Fatal("the search past its deadline did not time out")
	}
	if st := c.Stats(); st.Entries != 0 {
		t.Fatalf("stats %v: a timed-out result was cached", st)
	}
	if got := s.search(c, req); got.TimedOut || got.Total != 20 {
		t.Fatalf("the next search answered %+v", got)
	}
	wantStats(t, c, 0, 2)
}

func TestRequestCacheDateMath(t *testing.T) {
	s := newRCShard(t)
	s.fill(50)
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20})
	at := func(raw string, now time.Time) *Request {
		r := parseReq(t, raw)
		if ps := ResolveQueries(r, now); len(ps) > 0 {
			t.Fatal(ps)
		}
		return r
	}
	day := time.Date(2026, 10, 17, 9, 0, 0, 0, time.UTC)
	rounded := `{"size":0,"query":{"field":"created","op":"gte","value":"now-7d/d"}}`
	s.search(c, at(rounded, day))
	s.search(c, at(rounded, day.Add(3*time.Hour)))
	wantStats(t, c, 1, 1)
	exact := `{"size":0,"query":{"field":"created","op":"gte","value":"now-7d"}}`
	if r := at(exact, day); !r.NoRequestCache {
		t.Fatal("unrounded now left the request cache on")
	}
	if r := at(rounded, day); r.NoRequestCache {
		t.Fatal("rounded now turned the request cache off")
	}
	for _, raw := range []string{
		`{"size":0,"post_filter":{"field":"created","op":"between","value":["2026-01-01","now"]}}`,
		`{"size":0,"aggs":{"recent":{"filter":{"field":"created","op":"gt","value":"now-1h"}}}}`,
		`{"size":0,"aggs":{"f":{"filters":{"filters":{"a":{"field":"created","op":"lt","value":"now+1d"}}}}}}`,
	} {
		if !at(raw, day).NoRequestCache {
			t.Errorf("%s: unrounded now left the request cache on", raw)
		}
	}
	s.search(c, at(exact, day))
	s.search(c, at(exact, day))
	wantStats(t, c, 1, 1)
}

// TestRequestCacheComputesOnce: concurrent identical searches compute once.
func TestRequestCacheComputesOnce(t *testing.T) {
	s := newRCShard(t)
	s.fill(500)
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20})
	req := parseReq(t, rcAggs)
	const n = 16
	results := make([]*ShardResult, n)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range n {
		wg.Go(func() {
			<-start
			results[i] = s.search(c, req)
		})
	}
	close(start)
	wg.Wait()
	wantStats(t, c, n-1, 1)
	for i := 1; i < n; i++ {
		if !sameResult(t, results[i], results[0]) {
			t.Fatalf("result %d differs", i)
		}
	}
}

// TestRequestCacheSharedResultsReduceConcurrently: one cached result reduced by
// many searches at once is never changed (run with -race).
func TestRequestCacheSharedResultsReduceConcurrently(t *testing.T) {
	s := newRCShard(t)
	s.fill(300)
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20})
	req := parseReq(t, rcAggs)
	want := reduceOne(req, s.search(c, req))
	var wg sync.WaitGroup
	var bad atomic.Int32
	for range 8 {
		wg.Go(func() {
			r := parseReq(t, rcAggs)
			got := Reduce(context.Background(), []*ShardResult{s.search(c, r), s.search(c, r)}, r)
			if got.Total != 2*want.Total {
				bad.Add(1)
			}
			if !reflect.DeepEqual(reduceOne(r, s.search(c, r)), want) {
				bad.Add(1)
			}
		})
	}
	wg.Wait()
	if bad.Load() > 0 {
		t.Fatal("reducing a shared result changed it")
	}
}

func TestRequestCacheEvictsWithinItsBound(t *testing.T) {
	s := newRCShard(t)
	s.fill(100)
	const bound = 64 << 10
	c := NewRequestCache(RequestCacheOptions{MaxBytes: bound})
	for i := range 200 {
		s.search(c, parseReq(t, fmt.Sprintf(`{"size":0,"query":{"field":"price","op":"lt","value":%d},"aggs":{"b":{"terms":{"field":"brand"}}}}`, i)))
	}
	st := c.Stats()
	if st.Evictions == 0 || st.Bytes > bound || st.Entries == 0 || st.Entries >= 200 {
		t.Fatalf("stats %v: want evictions, within %d bytes", st, bound)
	}
	// The newest is still there.
	s.search(c, parseReq(t, `{"size":0,"query":{"field":"price","op":"lt","value":199},"aggs":{"b":{"terms":{"field":"brand"}}}}`))
	if got := c.Stats().Hits; got != 1 {
		t.Fatalf("hits %d, want the newest entry kept", got)
	}
}

func TestNilRequestCacheExecutes(t *testing.T) {
	if c := NewRequestCache(RequestCacheOptions{}); c != nil {
		t.Fatal("a cache with no memory and no store was made")
	}
	s := newRCShard(t)
	s.fill(5)
	var c *RequestCache
	if got := s.search(c, parseReq(t, `{"size":0}`)); got.Total != 5 {
		t.Fatalf("total %d", got.Total)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCachedResultEncodingRoundTrips(t *testing.T) {
	s := newRCShard(t)
	s.fill(300)
	for _, raw := range []string{
		rcAggs, `{"size":0,"track_total":true}`, `{"size":0,"aggs":{"g":{"global":{},"aggs":{"n":{"cardinality":{"field":"brand"}}}}}}`,
		`{"size":0,"aggs":{"lo":{"min":{"field":"price"}},"hi":{"max":{"field":"created"}},"none":{"min":{"field":"desc_n"}},
			"few":{"cardinality":{"field":"tags","threshold":3}},"all":{"cardinality":{"field":"tags","threshold":1000}},
			"b":{"terms":{"field":"brand","collect_mode":"breadth_first"},"aggs":{"lo":{"min":{"field":"price"}},"n":{"cardinality":{"field":"tags","threshold":2}}}}}}`,
	} {
		req := parseReq(t, raw)
		res := s.search(nil, req)
		enc, err := encodeCachedResult(res, 8<<20)
		if err != nil {
			t.Fatal(err)
		}
		dec, err := decodeCachedResult(enc, 8<<20, req)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(reduceOne(req, dec), reduceOne(req, res)) {
			t.Fatalf("%s: the decoded result reduces differently", raw)
		}
		mixed := Reduce(context.Background(), []*ShardResult{res, dec}, req)
		if mixed.Total != 2*res.Total {
			t.Fatalf("%s: a fresh and a decoded result reduce to %d", raw, mixed.Total)
		}
		for name, a := range mixed.Aggs {
			want := reduceOne(req, res).Aggs[name]
			if len(a.Buckets) != len(want.Buckets) {
				t.Fatalf("%s: aggregation %s has %d buckets mixed, %d alone: the keys decode to other types", raw, name, len(a.Buckets), len(want.Buckets))
			}
		}
		enc[0]++
		if _, err := decodeCachedResult(enc, 8<<20, req); err == nil {
			t.Fatal("a value of another format decoded")
		}
	}
}

// memStore is a [reqcache.Store] in a map, which can be made to fail.
type memStore struct {
	mu     sync.Mutex
	m      map[string][]byte
	fail   bool
	gets   atomic.Int64
	writes atomic.Int64
}

func newMemStore() *memStore { return &memStore{m: map[string][]byte{}} }

func (s *memStore) Name() string         { return "test" }
func (s *memStore) MaxEntryBytes() int64 { return 1 << 30 }
func (s *memStore) Close() error         { return nil }

func (s *memStore) Get(_ context.Context, key string) ([]byte, error) {
	s.gets.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return nil, errors.New("down")
	}
	return s.m[key], nil
}

func (s *memStore) Set(_ context.Context, key string, v []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("down")
	}
	s.m[key] = v
	s.writes.Add(1)
	return nil
}

var _ reqcache.Store = (*memStore)(nil)

// TestRequestCacheSharedAcrossCopies: two copies of a shard at the same seq, with
// different segments, on two nodes sharing a store: the second reads what the first
// computed, and so does a node restarted.
func TestRequestCacheSharedAcrossCopies(t *testing.T) {
	st := newMemStore()
	a, b := newRCShard(t), newRCShard(t)
	for i := range 40 {
		body := fmt.Sprintf(`{"brand":"b%d","price":%d,"tags":["x%d"]}`, i%3, i, i%5)
		a.put(fmt.Sprintf("d%d", i), body)
		a.refresh()
		b.put(fmt.Sprintf("d%d", i), body)
	}
	b.refresh()
	nodeA := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20, Store: st, ContentMark: testContent, Version: "v1"})
	nodeB := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20, Store: st, ContentMark: testContent, Version: "v1"})
	req := parseReq(t, rcAggs)
	fromA := a.search(nodeA, req)
	nodeA.writes.Wait()
	if st.writes.Load() != 1 {
		t.Fatalf("%d store writes, want 1", st.writes.Load())
	}
	fromB := b.search(nodeB, req)
	if s := nodeB.Stats(); s.StoreHits != 1 || s.Misses != 1 {
		t.Fatalf("node B stats %v: want a memory miss and a store hit", s)
	}
	if !reflect.DeepEqual(reduceOne(req, fromA), reduceOne(req, fromB)) {
		t.Fatal("the shared result reduces differently")
	}
	b.search(nodeB, req)
	if s := nodeB.Stats(); s.Hits != 1 || st.gets.Load() != 2 {
		t.Fatalf("node B stats %v, %d store reads: the store hit was not kept in memory", s, st.gets.Load())
	}
	restarted := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20, Store: st, ContentMark: testContent, Version: "v1"})
	b.search(restarted, req)
	if s := restarted.Stats(); s.StoreHits != 1 {
		t.Fatalf("restarted node stats %v: want a store hit", s)
	}
	upgraded := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20, Store: st, ContentMark: testContent, Version: "v2"})
	b.search(upgraded, req)
	if s := upgraded.Stats(); s.StoreHits != 0 || s.StoreMisses != 1 {
		t.Fatalf("upgraded node stats %v: another release read the entry", s)
	}
}

// TestRequestCacheStoreNeedsAMark: a copy whose own mark names no change of its
// shard is not shareable: the store is never asked.
func TestRequestCacheStoreNeedsAMark(t *testing.T) {
	st := newMemStore()
	s := newRCShard(t)
	s.put("a", `{"brand":"x"}`)
	if err := s.s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20, Store: st, ContentMark: testContent})
	s.search(c, parseReq(t, `{"size":0}`))
	c.writes.Wait()
	if st.gets.Load() != 0 || st.writes.Load() != 0 {
		t.Fatal("an unmarked generation used the shared store")
	}
}

// TestRequestCacheStoreDown: a failing store fails no search.
func TestRequestCacheStoreDown(t *testing.T) {
	st := newMemStore()
	st.fail = true
	s := newRCShard(t)
	s.fill(30)
	c := NewRequestCache(RequestCacheOptions{Store: st, ContentMark: testContent, Logger: quiet})
	req := parseReq(t, `{"size":0,"aggs":{"b":{"terms":{"field":"brand"}}}}`)
	for range 3 {
		if got := s.search(c, req); got.Total != 30 {
			t.Fatalf("total %d", got.Total)
		}
	}
	c.writes.Wait()
	if s := c.Stats(); s.StoreErrors != 6 || s.Entries != 0 || s.Hits+s.Misses != 0 {
		t.Fatalf("stats %v: want 3 failed reads and 3 failed writes, and no memory cache", s)
	}
}

// BenchmarkRequestCache: an aggregation-only search's shard phase uncached, and
// answered from the memory cache.
func BenchmarkRequestCache(b *testing.B) {
	s := newRCShard(b)
	s.fill(20_000)
	req := parseReq(b, rcAggs)
	b.Run("miss", func(b *testing.B) {
		for b.Loop() {
			if _, err := s.searchCtx(context.Background(), nil, req); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("hit", func(b *testing.B) {
		c := NewRequestCache(RequestCacheOptions{MaxBytes: 64 << 20})
		s.search(c, req)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := s.searchCtx(context.Background(), c, req); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("store_hit", func(b *testing.B) {
		st := newMemStore()
		warm := NewRequestCache(RequestCacheOptions{Store: st, ContentMark: testContent})
		s.search(warm, req)
		warm.writes.Wait()
		c := NewRequestCache(RequestCacheOptions{Store: st, ContentMark: testContent})
		b.ReportAllocs()
		for b.Loop() {
			if _, err := s.searchCtx(context.Background(), c, req); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// TestRequestCacheOnRedis: the shared store on Redis, then Redis gone: searches go
// on uncached, each delayed by at most about the store's timeout.
func TestRequestCacheOnRedis(t *testing.T) {
	mr := miniredis.RunT(t)
	redis := func() reqcache.Store {
		st, err := reqcache.NewRedis(reqcache.RedisOptions{URL: "redis://" + mr.Addr(), Timeout: 50 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		return st
	}
	a, b := newRCShard(t), newRCShard(t)
	a.fill(100)
	b.fill(100)
	nodeA := NewRequestCache(RequestCacheOptions{MaxBytes: 1 << 20, Store: redis(), ContentMark: testContent, Version: "v1", Logger: quiet})
	nodeB := NewRequestCache(RequestCacheOptions{MaxBytes: 1 << 20, Store: redis(), ContentMark: testContent, Version: "v1", Logger: quiet})
	defer nodeB.Close()
	req := parseReq(t, rcAggs)
	want := reduceOne(req, a.search(nodeA, req))
	if err := nodeA.Close(); err != nil { // waits for the write
		t.Fatal(err)
	}
	if keys := mr.Keys(); len(keys) != 1 || !strings.HasPrefix(keys[0], reqcache.DefaultRedisPrefix+strconv.Itoa(requestCacheFormat)+":products:0:") {
		t.Fatalf("Redis keys %v", keys)
	}
	if got := reduceOne(req, b.search(nodeB, req)); !reflect.DeepEqual(got, want) {
		t.Fatal("the result read from Redis reduces differently")
	}
	if s := nodeB.Stats(); s.StoreHits != 1 {
		t.Fatalf("stats %v: want a Redis hit", s)
	}

	mr.Close()
	b.put("more", `{"brand":"x"}`)
	b.refresh()
	start := time.Now()
	for range 3 {
		if got := b.search(nodeB, parseReq(t, `{"size":0,"track_total":true}`)); got.Total != 101 {
			t.Fatalf("total %d with Redis down", got.Total)
		}
	}
	nodeB.writes.Wait()
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("three searches with Redis down took %v", d)
	}
	if s := nodeB.Stats(); s.StoreErrors < 2 || s.Hits != 2 {
		t.Fatalf("stats %v: want Redis errors, and the memory cache still answering", s)
	}
}

func encodeJSONValue(t *testing.T, js string) []byte {
	t.Helper()
	return rcEncoder().EncodeAll([]byte(js), []byte{requestCacheFormat})
}

// TestCachedResultDecodeRefusesForgeries: a value that is too large, of another
// format, or not a shard result of its request decodes to an error, never a panic.
func TestCachedResultDecodeRefusesForgeries(t *testing.T) {
	req := parseReq(t, `{"size":0,"aggs":{"n":{"cardinality":{"field":"brand"}},"b":{"terms":{"field":"brand"},"aggs":{"p":{"stats":{"field":"price"}}}}}}`)
	ok := `{"hits":null,"total":3,"total_relation":"eq","aggs":{"n":{"type":"cardinality","sketch":{"p":14,"hashes":[1,2,3]}},"b":{"type":"terms","buckets":[{"key":"x","doc_count":3,"aggs":{"p":{"type":"stats","stats":{"count":3,"sum":6,"min":1,"max":3}}}}]}},"timed_out":false,"segments":1,"scanned":0}`
	const maxEntry = 4096
	if _, err := decodeCachedResult(encodeJSONValue(t, ok), maxEntry, req); err != nil {
		t.Fatalf("a valid value: %v", err)
	}
	big := `{"total":1,"total_relation":"eq","pad":"` + strings.Repeat("x", 100_000) + `"}`
	for name, raw := range map[string][]byte{
		"empty":                        nil,
		"other format":                 append([]byte{requestCacheFormat + 1}, encodeJSONValue(t, ok)[1:]...),
		"not zstd":                     append([]byte{requestCacheFormat}, "garbage"...),
		"not json":                     encodeJSONValue(t, `{"total":`),
		"past max entry":               append(encodeJSONValue(t, ok), make([]byte, maxEntry)...),
		"decodes too big":              encodeJSONValue(t, big),
		"timed out":                    encodeJSONValue(t, strings.Replace(ok, `"timed_out":false`, `"timed_out":true`, 1)),
		"hits":                         encodeJSONValue(t, strings.Replace(ok, `"hits":null`, `"hits":[{"id":"a","sort":null}]`, 1)),
		"bad relation":                 encodeJSONValue(t, strings.Replace(ok, `"eq"`, `"lt"`, 1)),
		"unknown agg":                  encodeJSONValue(t, strings.Replace(ok, `"n":{`, `"zz":{`, 1)),
		"wrong type":                   encodeJSONValue(t, strings.Replace(ok, `"type":"cardinality"`, `"type":"terms"`, 1)),
		"precision":                    encodeJSONValue(t, strings.Replace(ok, `"p":14`, `"p":12`, 1)),
		"registers":                    encodeJSONValue(t, strings.Replace(ok, `"hashes":[1,2,3]`, `"registers":"AAAA"`, 1)),
		"unsorted hashes":              encodeJSONValue(t, strings.Replace(ok, `[1,2,3]`, `[3,2,1]`, 1)),
		"register range":               encodeJSONValue(t, strings.Replace(ok, `"hashes":[1,2,3]`, `"registers":"`+base64.StdEncoding.EncodeToString(append(make([]byte, 1<<14-1), 60))+`"`, 1)),
		"nil bucket":                   encodeJSONValue(t, strings.Replace(ok, `"buckets":[{`, `"buckets":[null,{`, 1)),
		"object key":                   encodeJSONValue(t, strings.Replace(ok, `"key":"x"`, `"key":{"a":1}`, 1)),
		"negative count":               encodeJSONValue(t, strings.Replace(ok, `"doc_count":3`, `"doc_count":-3`, 1)),
		"unknown sub-agg":              encodeJSONValue(t, strings.Replace(ok, `"aggs":{"p":`, `"aggs":{"q":`, 1)),
		"nil partial":                  encodeJSONValue(t, strings.Replace(ok, `"n":{"type":"cardinality","sketch":{"p":14,"hashes":[1,2,3]}}`, `"n":null`, 1)),
		"negative total":               encodeJSONValue(t, strings.Replace(ok, `"total":3`, `"total":-1`, 1)),
		"value on cardinality":         encodeJSONValue(t, strings.Replace(ok, `"type":"cardinality",`, `"type":"cardinality","value":3,`, 1)),
		"distinct without a threshold": encodeJSONValue(t, strings.Replace(ok, `"type":"cardinality",`, `"type":"cardinality","distinct":[1],`, 1)),
	} {
		if res, err := decodeCachedResult(raw, maxEntry, req); err == nil {
			t.Errorf("%s: decoded %+v", name, res)
		}
	}
}

// TestRequestCacheForgedStoreValueIsAMiss: a corrupt value under the right key is a
// store error, and the shard computes.
func TestRequestCacheForgedStoreValueIsAMiss(t *testing.T) {
	s := newRCShard(t)
	s.fill(30)
	st := newMemStore()
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 1 << 20, Store: st, ContentMark: testContent, Logger: quiet})
	req := parseReq(t, `{"size":0,"aggs":{"n":{"cardinality":{"field":"brand"}}}}`)
	rk, _ := requestCacheKey(req)
	g := s.s.Acquire()
	key := c.sharedKey(g.CacheIdentity(), rk)
	g.Release()
	if key == "" {
		t.Fatal("no shared key")
	}
	st.m[key] = encodeJSONValue(t, `{"total":30,"total_relation":"eq","aggs":{"n":{"type":"cardinality","sketch":{"p":4,"registers":"AAAAAAAAAAAAAAAAAAAAAA=="}}}}`)
	got := s.search(c, req)
	if want := s.search(nil, req); !reflect.DeepEqual(reduceOne(req, got), reduceOne(req, want)) {
		t.Fatal("a forged value was served")
	}
	if s := c.Stats(); s.StoreErrors != 1 || s.StoreHits != 0 {
		t.Fatalf("stats %v: want one decode error", s)
	}
}

// gatedStore is a memStore whose calls wait for their context (a write at most
// 20 ms, as a store's own timeout bounds it), or release.
type gatedStore struct {
	*memStore
	release chan struct{}
}

func (s *gatedStore) Get(ctx context.Context, key string) ([]byte, error) {
	select {
	case <-s.release:
		return s.memStore.Get(ctx, key)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *gatedStore) Set(ctx context.Context, key string, v []byte) error {
	select {
	case <-s.release:
		return s.memStore.Set(ctx, key, v)
	case <-time.After(20 * time.Millisecond):
		return context.DeadlineExceeded
	}
}

// TestRequestCacheTimeoutsCoverWaitsAndTheStore: a search's timeout bounds its store
// read and its wait for another identical search, and it then runs with what is left
// of it (none), as an uncached search out of time does.
func TestRequestCacheTimeoutsCoverWaitsAndTheStore(t *testing.T) {
	s := newRCShard(t)
	s.fill(30)
	st := &gatedStore{memStore: newMemStore(), release: make(chan struct{})}
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 1 << 20, Store: st, ContentMark: testContent, Logger: quiet})
	slow := parseReq(t, `{"size":0,"aggs":{"b":{"terms":{"field":"brand"}}}}`)
	leader := make(chan *ShardResult, 1)
	go func() { leader <- s.search(c, slow) }()
	// The leader waits on the store, with no timeout; a waiter with one gives up.
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		n := len(c.flights)
		c.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the leader never started")
		}
		time.Sleep(time.Millisecond)
	}
	waiter := parseReq(t, `{"size":0,"timeout":"30ms","aggs":{"b":{"terms":{"field":"brand"}}}}`)
	start := time.Now()
	res := s.search(c, waiter)
	close(st.release)
	if d := time.Since(start); (!res.TimedOut && res.Total != 30) || d > 2*time.Second {
		t.Fatalf("the waiter answered %+v after %v", res, d)
	}
	if got := <-leader; got.TimedOut || got.Total != 30 {
		t.Fatalf("the leader answered %+v", got)
	}
}

// TestRequestCacheHungStoreTakesAShareOfTheTimeout: a store that never answers
// takes a tenth of a search's time, not all of it: the search computes its result in
// time, the store's failure is counted, and its breaker opens. A search with too
// little time left does not ask the store at all.
func TestRequestCacheHungStoreTakesAShareOfTheTimeout(t *testing.T) {
	s := newRCShard(t)
	s.fill(30)
	hung := &gatedStore{memStore: newMemStore(), release: make(chan struct{})}
	br := reqcache.NewBreaker(hung, 3, time.Hour, time.Hour)
	c := NewRequestCache(RequestCacheOptions{Store: br, ContentMark: testContent, Logger: quiet})
	for i := range 3 {
		// A distinct query each time: no result in flight or stored to reuse.
		res := s.search(c, parseReq(t, fmt.Sprintf(`{"size":0,"timeout":"200ms","query":{"field":"price","op":"gte","value":%d}}`, -i)))
		if res.TimedOut || res.Total != 30 {
			t.Fatalf("search %d answered %+v with the store hung", i, res)
		}
	}
	c.writes.Wait()
	if got := c.Stats(); got.StoreErrors < 3 || !br.Open() {
		t.Fatalf("stats %v, breaker open %v: want the store's failures counted, and the breaker open", got, br.Open())
	}
	skips := c.Stats().StoreSkips
	res := s.search(c, parseReq(t, `{"size":0,"timeout":"200ms","query":{"field":"price","op":"gte","value":-9}}`))
	if got := c.Stats(); res.TimedOut || got.StoreSkips != skips+1 {
		t.Fatalf("stats %v: want the open breaker to skip the store", got)
	}

	idle := newMemStore()
	c2 := NewRequestCache(RequestCacheOptions{Store: idle, ContentMark: testContent, Logger: quiet})
	s.search(c2, parseReq(t, `{"size":0,"timeout":"10ms"}`))
	if got := c2.Stats(); idle.gets.Load() != 0 || got.StoreSkips != 1 {
		t.Fatalf("stats %v, %d reads: a search with 10 ms asked the store", got, idle.gets.Load())
	}
}

// TestRequestCacheCloseDropsLateWrites: a closed cache neither reads nor writes its
// store, and still answers.
func TestRequestCacheCloseDropsLateWrites(t *testing.T) {
	s := newRCShard(t)
	s.fill(5)
	st := newMemStore()
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 1 << 20, Store: st, ContentMark: testContent})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if got := s.search(c, parseReq(t, `{"size":0}`)); got.Total != 5 {
		t.Fatalf("total %d", got.Total)
	}
	if got := c.Stats(); got.DroppedWrites != 0 || st.writes.Load() != 0 || st.gets.Load() != 0 {
		t.Fatalf("stats %v, %d reads, %d writes: a closed cache used its store", got, st.gets.Load(), st.writes.Load())
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestCachedResultDecodeChecksNewPartials: a min or max partial holds a value and
// nothing else; a thresholded cardinality's hashes are sorted, unique and at most
// the threshold, with no sketch.
func TestCachedResultDecodeChecksNewPartials(t *testing.T) {
	req := parseReq(t, `{"size":0,"aggs":{"lo":{"min":{"field":"price"}},"n":{"cardinality":{"field":"brand","threshold":3}}}}`)
	ok := `{"total":3,"total_relation":"eq","aggs":{"lo":{"type":"min","value":1.5},"n":{"type":"cardinality","distinct":[1,5,9]}}}`
	if _, err := decodeCachedResult(encodeJSONValue(t, ok), 4096, req); err != nil {
		t.Fatalf("a valid value: %v", err)
	}
	for name, js := range map[string]string{
		"past the threshold": strings.Replace(ok, `[1,5,9]`, `[1,5,9,11]`, 1),
		"unsorted":           strings.Replace(ok, `[1,5,9]`, `[5,1,9]`, 1),
		"duplicate":          strings.Replace(ok, `[1,5,9]`, `[1,5,5]`, 1),
		"sketch too":         strings.Replace(ok, `"distinct":[1,5,9]`, `"distinct":[1],"sketch":{"p":14}`, 1),
		"min with buckets":   strings.Replace(ok, `"value":1.5`, `"value":1.5,"buckets":[{"key":"x","doc_count":1}]`, 1),
		"min with stats":     strings.Replace(ok, `"value":1.5`, `"stats":{"count":1,"sum":1,"min":1,"max":1}`, 1),
		"min with distinct":  strings.Replace(ok, `"value":1.5`, `"distinct":[1]`, 1),
	} {
		if res, err := decodeCachedResult(encodeJSONValue(t, js), 4096, req); err == nil {
			t.Errorf("%s: decoded %+v", name, res)
		}
	}
	terms := parseReq(t, `{"size":0,"aggs":{"b":{"terms":{"field":"brand"}}}}`)
	if _, err := decodeCachedResult(encodeJSONValue(t, `{"total":1,"total_relation":"eq","aggs":{"b":{"type":"terms","value":2}}}`), 4096, terms); err == nil {
		t.Error("a terms partial with a value decoded")
	}
}

// TestRequestCacheStoresNoCutOrFailedResult: a shard result the merge cut short
// (a breadth-first replay past the deadline: TimedOut) or that failed (a replay that
// panics: an error) is stored neither in memory nor in the store, and the next
// identical search computes again.
func TestRequestCacheStoresNoCutOrFailedResult(t *testing.T) {
	s := newRCShard(t)
	s.fill(300)
	st := newMemStore()
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20, Store: st, ContentMark: testContent, Logger: quiet})
	const body = `{"size":0,%s"aggs":{"t":{"terms":{"field":"tags","size":3,"collect_mode":"breadth_first"},"aggs":{"s":{"stats":{"field":"price"}}}}}}`
	req := func(timeout string) *Request { return parseReq(t, fmt.Sprintf(body, timeout)) }
	var replays atomic.Int32
	defer func() { replayHook = nil }()
	nothingStored := func(what string) {
		t.Helper()
		c.writes.Wait()
		if got := c.Stats(); got.Entries != 0 || st.writes.Load() != 0 {
			t.Fatalf("%s: stats %v, %d store writes: a result was stored", what, got, st.writes.Load())
		}
	}

	replayHook = func(ctx context.Context, _ []*ordPartial) {
		replays.Add(1)
		<-ctx.Done()
	}
	if res := s.search(c, req(`"timeout":"300ms",`)); !res.TimedOut {
		t.Fatal("the cut replay did not time the shard out")
	}
	nothingStored("a cut merge")

	replayHook = func(_ context.Context, parts []*ordPartial) {
		replays.Add(1)
		for _, p := range parts {
			if p.deferred != nil {
				p.deferred.s = &segExec{ctx: t.Context()} // no reader: the replay panics
			}
		}
	}
	g := s.s.Acquire()
	_, err := c.Search(context.Background(), g, req(""))
	g.Release()
	if err == nil || isCtxErr(err) {
		t.Fatalf("a failed replay answered %v; want the shard's failure", err)
	}
	nothingStored("a failed replay")
	if replays.Load() != 2 {
		t.Fatalf("%d replays: the searches did not take the breadth-first path", replays.Load())
	}

	replayHook = nil
	misses := c.Stats().Misses
	res := s.search(c, req(""))
	c.writes.Wait()
	if got := c.Stats(); res.TimedOut || len(res.Aggs["t"].Buckets) == 0 || got.Misses != misses+1 || got.Entries != 1 || st.writes.Load() != 1 {
		t.Fatalf("the next search: timed out %v, stats %v, %d store writes; want it computed and stored", res.TimedOut, got, st.writes.Load())
	}
}

// TestCachedResultDecodeRefusesImpossibleBuckets: a value no shard computes (a
// bucket twice, stats out of order, a range partial of another length, a histogram
// key that is no number) decodes to an error.
func TestCachedResultDecodeRefusesImpossibleBuckets(t *testing.T) {
	req := parseReq(t, `{"size":0,"aggs":{"b":{"terms":{"field":"brand"}},"p":{"stats":{"field":"price"}},
		"r":{"range":{"field":"price","ranges":[{"to":50},{"from":50}]}},"h":{"histogram":{"field":"price","interval":10}}}}`)
	ok := `{"total":3,"total_relation":"eq","aggs":{` +
		`"b":{"type":"terms","buckets":[{"key":"x","doc_count":2},{"key":"y","doc_count":1}]},` +
		`"p":{"type":"stats","stats":{"count":3,"sum":6,"min":1,"max":3}},` +
		`"r":{"type":"range","buckets":[{"key":0,"doc_count":1},{"key":1,"doc_count":2}]},` +
		`"h":{"type":"histogram","buckets":[{"key":0,"doc_count":3}]}}}`
	if _, err := decodeCachedResult(encodeJSONValue(t, ok), 4096, req); err != nil {
		t.Fatalf("a valid value: %v", err)
	}
	for name, js := range map[string]string{
		"duplicate term":     strings.Replace(ok, `{"key":"y","doc_count":1}`, `{"key":"x","doc_count":1}`, 1),
		"negative count":     strings.Replace(ok, `"count":3`, `"count":-3`, 1),
		"min past max":       strings.Replace(ok, `"min":1,"max":3`, `"min":4,"max":3`, 1),
		"a range short":      strings.Replace(ok, `[{"key":0,"doc_count":1},{"key":1,"doc_count":2}]`, `[{"key":0,"doc_count":1}]`, 1),
		"a range more":       strings.Replace(ok, `{"key":1,"doc_count":2}]`, `{"key":1,"doc_count":2},{"key":2,"doc_count":0}]`, 1),
		"duplicate range":    strings.Replace(ok, `{"key":1,"doc_count":2}`, `{"key":0,"doc_count":2}`, 1),
		"histogram text key": strings.Replace(ok, `{"key":0,"doc_count":3}`, `{"key":"zero","doc_count":3}`, 1),
	} {
		if res, err := decodeCachedResult(encodeJSONValue(t, js), 4096, req); err == nil {
			t.Errorf("%s: decoded %+v", name, res)
		}
	}
	empty := `{"total":0,"total_relation":"eq","aggs":{"p":{"type":"stats","stats":{"count":0,"sum":0,"min":9,"max":1}}}}`
	if _, err := decodeCachedResult(encodeJSONValue(t, empty), 4096, req); err != nil {
		t.Fatalf("empty stats, whose min and max mean nothing: %v", err)
	}
}
