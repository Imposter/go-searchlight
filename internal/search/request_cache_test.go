package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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
}

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

// refresh publishes what was applied, with the mark every copy records of the
// change at its seq.
func (r *rcShard) refresh() {
	r.t.Helper()
	r.s.SetMark(r.seq, json.RawMessage(fmt.Sprintf(`{"seq":%d,"nonce":7}`, r.seq)))
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
	if !reflect.DeepEqual(first, second) {
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
	b := parseReq(t, `{"size":0,"timeout":"5s","aggregations":{"b":{"terms":{"field":"brand"}}}}`)
	b.Index = "other"
	ka, _ := requestCacheKey(a)
	kb, _ := requestCacheKey(b)
	if !bytes.Equal(ka, kb) {
		t.Fatal("the timeout, the aggs spelling or the index label changed the key")
	}
	for _, raw := range []string{
		`{"size":0,"aggs":{"b":{"terms":{"field":"brand","size":5}}}}`,
		`{"size":0,"aggs":{"c":{"terms":{"field":"brand"}}}}`,
		`{"size":0,"track_total":true,"aggs":{"b":{"terms":{"field":"brand"}}}}`,
		`{"size":0,"query":{"field":"price","op":"lt","value":5},"aggs":{"b":{"terms":{"field":"brand"}}}}`,
		`{"size":0,"post_filter":{"field":"price","op":"lt","value":5},"aggs":{"b":{"terms":{"field":"brand"}}}}`,
		`{"size":0,"aggs":{"b":{"filter":{"field":"price","op":"lt","value":5}}}}`,
		`{"size":0,"aggs":{"b":{"filter":{"field":"price","op":"lt","value":6}}}}`,
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
	if got := s.search(c, req); !reflect.DeepEqual(got, before) {
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
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20, Store: st})
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
	// The memory hit at the new seq wrote the result under the new seq's key, once,
	// for the other copies there.
	if n := st.writes.Load(); n != 2 {
		t.Fatalf("%d store writes, want 2", n)
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
	s.search(c, at(exact, day))
	s.search(c, at(exact, day.Add(time.Millisecond)))
	wantStats(t, c, 1, 3)
	s.search(c, at(exact, day))
	wantStats(t, c, 2, 3)
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
		if !reflect.DeepEqual(results[i], results[0]) {
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
	for _, raw := range []string{rcAggs, `{"size":0,"track_total":true}`, `{"size":0,"aggs":{"g":{"global":{},"aggs":{"n":{"cardinality":{"field":"brand"}}}}}}`} {
		req := parseReq(t, raw)
		res := s.search(nil, req)
		enc, err := encodeCachedResult(res)
		if err != nil {
			t.Fatal(err)
		}
		dec, err := decodeCachedResult(enc)
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
		if _, err := decodeCachedResult(enc); err == nil {
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
	nodeA := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20, Store: st, Version: "v1"})
	nodeB := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20, Store: st, Version: "v1"})
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
	restarted := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20, Store: st, Version: "v1"})
	b.search(restarted, req)
	if s := restarted.Stats(); s.StoreHits != 1 {
		t.Fatalf("restarted node stats %v: want a store hit", s)
	}
	upgraded := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20, Store: st, Version: "v2"})
	b.search(upgraded, req)
	if s := upgraded.Stats(); s.StoreHits != 0 || s.StoreMisses != 1 {
		t.Fatalf("upgraded node stats %v: another release read the entry", s)
	}
}

// TestRequestCacheStoreNeedsAMark: a copy whose seq has no mark is not shareable: the
// store is never asked.
func TestRequestCacheStoreNeedsAMark(t *testing.T) {
	st := newMemStore()
	s := newRCShard(t)
	s.put("a", `{"brand":"x"}`)
	if err := s.s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20, Store: st})
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
	c := NewRequestCache(RequestCacheOptions{Store: st, Logger: quiet})
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
		warm := NewRequestCache(RequestCacheOptions{Store: st})
		s.search(warm, req)
		warm.writes.Wait()
		c := NewRequestCache(RequestCacheOptions{Store: st})
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
	nodeA := NewRequestCache(RequestCacheOptions{MaxBytes: 1 << 20, Store: redis(), Version: "v1", Logger: quiet})
	nodeB := NewRequestCache(RequestCacheOptions{MaxBytes: 1 << 20, Store: redis(), Version: "v1", Logger: quiet})
	defer nodeB.Close()
	req := parseReq(t, rcAggs)
	want := reduceOne(req, a.search(nodeA, req))
	if err := nodeA.Close(); err != nil { // waits for the write
		t.Fatal(err)
	}
	if keys := mr.Keys(); len(keys) != 1 || !strings.HasPrefix(keys[0], reqcache.DefaultRedisPrefix+"1:products:0:") {
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
