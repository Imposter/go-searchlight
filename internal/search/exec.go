package search

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// Hit is one matching document.
type Hit struct {
	ID string `json:"id"`
	// Sort is the hit's value for each sort key, then its _id (the tie-breaker; a
	// sort that names _id ends there): nil for a missing value. Pass the last hit's
	// as SearchAfter to page on.
	Sort []any `json:"sort"`
	// Body is the stored document, limited to Request.Fields when set; nil when the
	// request asked for no bodies (Request.NoBodies), for a later [FetchShard].
	Body json.RawMessage `json:"body,omitempty"`
	// Ref locates the hit in the generation that found it, for [FetchShard].
	Ref *HitRef `json:"ref,omitempty"`
	// InnerHits are the hit's inner hits, by key ([query.InnerHits.Key]): set by
	// [FillInnerHits] once the hit's body is fetched, for each nested node that asks.
	InnerHits map[string]*InnerHits `json:"inner_hits,omitempty"`
}

// HitRef is where a hit lives: a segment of the generation that found it, and the
// document's ordinal there.
type HitRef struct {
	Segment string `json:"segment"`
	Ord     uint32 `json:"ord"`
}

// ShardResult is one shard's answer: its own top hits (bodies fetched), its exact
// total, and partial aggregations for [Reduce].
type ShardResult struct {
	// Hits are the shard's best Request.From+Size, in order: the reduce needs every
	// one's place to know which make the page (a query phase, NoBodies, reads none of
	// their bodies).
	Hits []Hit `json:"hits"`
	// Total is how many live documents matched: exact when TotalRelation is
	// RelationEq; with RelationGte a lower bound, because the shard stopped verifying
	// candidates once it had confirmed more than TrackTotal matches (and had settled
	// its top hits), or timed out. Reduce applies TrackTotal.
	Total         int64                  `json:"total"`
	TotalRelation string                 `json:"total_relation"`
	Aggs          map[string]*AggPartial `json:"aggs,omitempty"`
	// TimedOut is set when the search ran out of time (Request.Timeout or the
	// context's deadline) before every segment finished, or before the shard merged
	// every aggregation: the hits, total and aggregations are those of the segments
	// that did, and an aggregation left unmerged is empty.
	TimedOut bool `json:"timed_out"`
	// Segments is how many segments the shard searched; Scanned how many candidates
	// it verified one at a time (residual checks): the per-query work the index could
	// not do alone.
	Segments int   `json:"segments"`
	Scanned  int64 `json:"scanned"`
}

// Response is the reduced answer to a request.
type Response struct {
	Hits []Hit
	// Total is how many documents matched: exact when TotalRelation is RelationEq, a
	// lower bound when RelationGte (past TrackTotal, or a timed-out search).
	Total         int64
	TotalRelation string
	Aggs          map[string]*AggResult
	// Next is the last hit's Sort when the page is full (more may follow): the next
	// page's SearchAfter. Nil on a short page.
	Next     []any
	TimedOut bool
}

// The search thread pool: the goroutines, beyond each caller's own, that segments of
// any search may run on. Bounded process-wide, like Elasticsearch's search pool.
var slots atomic.Pointer[chan struct{}]

func init() { SetThreads(runtime.GOMAXPROCS(0)) }

// SetThreads sets how many goroutines, beyond each search's own, run segments
// (search_threads); at least 1. Searches in flight keep the pool they started with.
func SetThreads(n int) {
	ch := make(chan struct{}, max(1, n))
	slots.Store(&ch)
}

// poolSize is how many calls runParallel makes at once at most: the search pool's
// goroutines and the caller's.
func poolSize() int { return cap(*slots.Load()) + 1 }

// runParallel calls fn(0..n-1), on the calling goroutine and on as many pool
// goroutines as are free, and returns when every call has. A panic in fn is recovered,
// logged with its stack, and returned as an error: it never takes the process down.
func runParallel(n int, fn func(i int)) error {
	if n == 0 {
		return nil
	}
	pool := *slots.Load()
	var next atomic.Int64
	var panicked atomic.Pointer[error]
	call := func(i int) {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("search: panic: %v", r)
				slog.Default().Error("search: recovered a panic", slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
				panicked.CompareAndSwap(nil, &err)
			}
		}()
		fn(i)
	}
	work := func() {
		for {
			i := int(next.Add(1) - 1)
			if i >= n {
				return
			}
			call(i)
		}
	}
	var wg sync.WaitGroup
helpers:
	for range n - 1 {
		select {
		case pool <- struct{}{}:
			wg.Add(1)
			go func() {
				defer func() { <-pool; wg.Done() }()
				work()
			}()
		default:
			break helpers
		}
	}
	work()
	wg.Wait()
	if err := panicked.Load(); err != nil {
		return *err
	}
	return nil
}

// segExec is one segment's search.
type segExec struct {
	ctx     context.Context
	p       *prepared
	sv      *shard.SegmentView
	seg     int
	r       *segment.Reader
	n       uint32
	allDocs *roaring.Bitmap
	cache   *shard.FilterCache

	cacheMemo map[*leafPlan]*roaring.Bitmap // nil value: a miss
	estimates map[*leafPlan]estimate
	sources   map[string]*fieldSrc

	approxes   map[*pnode]approx
	orders     map[*pnode][]*pnode
	ordMatches map[ordKey]bool
	// lazy is set when the segment verifies candidates only as needed.
	lazy *lazyState

	scanned int64
	// buckets counts the buckets the segment's histograms hold by key, together at
	// most MaxBuckets: shared by the forks that collect parts of the segment apart.
	buckets *atomic.Int64
	// masks are the terms aggregations' filters over this segment's ordinals, shared
	// by the forks as buckets is.
	masks *termMasks
	// collecting is how many documents the aggregations being made collect, which
	// chooses how a terms filter is decided (lazyTerms).
	collecting uint64
	// inners are the executions over the segment's nested fields' objects, by path,
	// made on first use (inner); nil for a field the segment holds no member of.
	inners map[string]*segExec
	// parent, in an execution over a nested field's objects, is its segment's: nv and
	// path are that field's, r its object segment, and allDocs the members that are
	// objects.
	parent *segExec
	nv     *segment.Nested
	path   string
	err    error
}

// lazyState is a segment's lazy verification: the root's maybe documents, those
// checked so far, and how many of them matched.
type lazyState struct {
	maybe   *roaring.Bitmap
	checked *roaring.Bitmap
	found   int64
	// batched holds the matches among every maybe document once one-at-a-time checks
	// stopped paying (see lazyBatchAfter): from then on accept is a lookup.
	batched *roaring.Bitmap
}

// lazyBatchAfter is how many one-at-a-time checks a segment makes before it verifies
// all its remaining candidates in one batch: when most candidates fail (a needle
// whose grams are common but which is itself rare) the top-k would otherwise check
// nearly all of them singly, which batching does several times faster. A variable so
// tests can make it small.
var lazyBatchAfter uint64 = 2048

// accept reports whether document d, a candidate about to enter the top-k, matches:
// at once for a sure document, by a check of the query for a maybe one.
func (s *segExec) accept(d uint32) bool {
	l := s.lazy
	if l == nil || !l.maybe.Contains(d) {
		return true
	}
	if l.batched == nil && l.checked.GetCardinality() >= lazyBatchAfter {
		rest := roaring.AndNot(l.maybe, l.checked)
		l.batched = s.resolve(s.p.root, rest)
		l.checked.Or(rest)
		got := card(l.batched)
		l.found += got
		s.p.confirmed.Add(got)
	}
	if l.batched != nil {
		return l.batched.Contains(d)
	}
	l.checked.Add(d)
	if !s.check(s.p.root, d) {
		return false
	}
	l.found++
	s.p.confirmed.Add(1)
	return true
}

func newSegExec(ctx context.Context, p *prepared, g *shard.Generation, i int) *segExec {
	sv := &g.Segments[i]
	all := roaring.New()
	all.AddRange(0, uint64(sv.NumDocs))
	return &segExec{
		ctx: ctx, p: p, sv: sv, seg: i, r: sv.Reader, n: sv.NumDocs, allDocs: all,
		cache:      g.FilterCache(),
		cacheMemo:  map[*leafPlan]*roaring.Bitmap{},
		approxes:   map[*pnode]approx{},
		ordMatches: map[ordKey]bool{},
		orders:     map[*pnode][]*pnode{},
		estimates:  map[*leafPlan]estimate{},
		sources:    map[string]*fieldSrc{},
		buckets:    new(atomic.Int64),
		masks:      &termMasks{},
	}
}

// checkCtx reports whether the search must stop: its context ended, or it failed.
func (s *segExec) checkCtx() bool {
	if s.err != nil {
		return true
	}
	if err := s.ctx.Err(); err != nil {
		s.err = err
		return true
	}
	return false
}

func (s *segExec) fail(err error) {
	if s.err == nil {
		s.err = fmt.Errorf("search: segment %s: %w", s.sv.ID, err)
	}
}

func (s *segExec) cacheGet(lp *leafPlan) (*roaring.Bitmap, bool) {
	if bm, ok := s.cacheMemo[lp]; ok {
		return bm, bm != nil
	}
	if s.cache == nil {
		return nil, false
	}
	bm, ok := s.cache.Get(s.sv.ID, lp.key)
	if !ok {
		bm = nil
	}
	s.cacheMemo[lp] = bm
	return bm, ok
}

// cachePut caches lp's exact bitmap over the whole segment, returning the cached copy.
func (s *segExec) cachePut(lp *leafPlan, bm *roaring.Bitmap) *roaring.Bitmap {
	if s.cache == nil {
		return bm
	}
	owned := s.cache.Put(s.sv.ID, lp.key, bm)
	s.cacheMemo[lp] = owned
	return owned
}

// segResult is one segment's part of a shard search.
type segResult struct {
	done bool
	err  error
	hits int64
	// complete: hits is exact; else a lower bound (lazy verification stopped).
	complete bool
	top      []segHit
	aggs     map[string]*AggPartial
	scanned  int64
}

// countChunk is how many unverified candidates the counting phase verifies at once (a
// variable so tests can make it small).
var countChunk = 4096

// runSegment searches segment i of g. Aggregations and exact totals need every match,
// so every candidate is verified, in batches. Otherwise verification runs in two
// phases: only the candidates that could enter the top hits, then as many more as the
// total needs, counted in batches sized from the match rate so far to what is still
// needed (twice over), at least countChunk: few round trips, little overshoot.
func runSegment(ctx context.Context, p *prepared, g *shard.Generation, i int) (out segResult) {
	s := newSegExec(ctx, p, g, i)
	ctx, span := otel.Tracer(telemetry.ScopeName).Start(ctx, "search.segment", trace.WithAttributes(
		attribute.String("segment", s.sv.ID), attribute.Int64("documents", int64(s.sv.Live))))
	defer func() {
		span.SetAttributes(attribute.Int64("hits", out.hits), attribute.Int64("verified", out.scanned),
			attribute.Bool("complete", out.complete))
		if out.err != nil {
			span.RecordError(out.err)
		}
		span.End()
	}()
	s.ctx = ctx
	if s.checkCtx() {
		out.err = s.err
		return out
	}
	live := s.allDocs
	if !s.sv.Deletes.IsEmpty() {
		live = roaring.AndNot(s.allDocs, s.sv.Deletes)
	}
	a := s.approximate(p.root, live) //nolint:contextcheck // only the filter cache's hit counter takes no context
	hits := a.sure
	out.complete = true
	switch {
	case s.err != nil:
	case a.maybe.IsEmpty():
	case len(p.aggs) > 0 || p.need < 0:
		hits = roaring.Or(a.sure, s.resolve(p.root, a.maybe))
	default:
		s.lazy = &lazyState{maybe: a.maybe, checked: roaring.New()}
		p.confirmed.Add(card(a.sure))
		out.top = s.topK(roaring.Or(a.sure, a.maybe), p.size) //nolint:contextcheck // only the filter cache's hit counter takes no context
		rest := roaring.AndNot(a.maybe, s.lazy.checked)
		var tried, matched int64
		for !rest.IsEmpty() && p.confirmed.Load() < p.need && !s.checkCtx() {
			size := int64(countChunk)
			if tried > 0 {
				missing := p.need - p.confirmed.Load()
				size = max(size, 2*missing*tried/max(matched, 1))
			}
			buf := make([]uint32, min(size, card(rest)))
			n := rest.ManyIterator().NextMany(buf)
			chunk := roaring.BitmapOf(buf[:n]...)
			got := card(s.resolve(p.root, chunk))
			tried += int64(n)
			matched += got
			s.lazy.found += got
			p.confirmed.Add(got)
			rest.AndNot(chunk)
		}
		out.hits = card(a.sure) + s.lazy.found
		out.complete = rest.IsEmpty()
	}
	if s.lazy == nil && s.err == nil {
		matched := hits
		if p.post != nil {
			matched = s.matches(p.post, hits) //nolint:contextcheck // only the filter cache's hit counter takes no context
		}
		out.hits = card(matched)
		p.confirmed.Add(out.hits)
		out.top = s.topK(matched, p.size) //nolint:contextcheck // only the filter cache's hit counter takes no context
		if s.err == nil {
			out.aggs = s.aggregate(hits, live) //nolint:contextcheck // only the filter cache's hit counter takes no context
		}
	}
	out.scanned = s.scanned
	if s.err != nil {
		out.err = s.err
		return out
	}
	out.done = true
	return out
}

// card is a bitmap's cardinality as an int64 (at most 2^32).
func card(bm *roaring.Bitmap) int64 {
	return int64(bm.GetCardinality()) //nolint:gosec // a segment holds at most 2^32 documents
}

func isCtxErr(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// ExecuteShard searches one shard's generation. The caller holds g (Acquire) for the
// whole call, and may Release it once it returns: the result owns all it holds.
//
// The plan and execution are described in plan.go: a cost-ordered tree of bitmap
// operations per segment, in two phases: the index's candidates first, then residual
// checks where the index cannot decide alone, made only as the top hits and the total
// need them (all of them when there are aggregations, or TrackTotal is
// TrackTotalAll). Segments run in parallel on the search pool ([SetThreads]).
//
// The shard's deadline is the earlier of ctx's and r.Timeout from the call; the
// segments get it less deadlineReserve, and the merge of their partials the rest.
// When the segments' deadline passes, they stop where they are and the result holds
// what the finished ones found, with TimedOut set; an aggregation whose shard merge
// the deadline cuts off is left empty, also with TimedOut set (one whose global
// ordinals are still building merges by term instead: see finishOrds). The hits found
// are still fetched. A ctx cancelled before the merge is done returns its error. A request the mapping refuses (an unknown aggregation type, a sort on a list,
// ...) is a *RequestError.
func ExecuteShard(ctx context.Context, g *shard.Generation, r *Request) (*ShardResult, error) {
	if g == nil || r == nil {
		return nil, errNoGeneration
	}
	start := time.Now()
	tr := otel.Tracer(telemetry.ScopeName)
	ctx, span := tr.Start(ctx, "search.shard", trace.WithAttributes(
		attribute.String(telemetry.KeyIndex, r.Index),
		attribute.Int64("generation", int64(g.Gen())), //nolint:gosec // a commit counter
		attribute.Int64("seq", g.Seq()),
		attribute.Int("segments", len(g.Segments)),
		attribute.Int("size", r.Size),
		attribute.Int("aggs", len(r.Aggs)),
	))
	defer span.End()
	fail := func(err error, why string) (*ShardResult, error) {
		span.RecordError(err)
		span.SetStatus(codes.Error, why)
		return nil, err
	}
	_, planSpan := tr.Start(ctx, "search.plan")
	p, err := prepare(r, g.Mapping())
	planSpan.End()
	if err != nil {
		return fail(err, "invalid request")
	}
	planned := time.Now()
	parent := ctx
	segCtx := ctx
	if deadline, ok := shardDeadline(ctx, start, r.Timeout); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
		segCtx, cancel = context.WithDeadline(ctx, deadline.Add(-deadlineReserve(deadline.Sub(start))))
		defer cancel()
	}
	n := len(g.Segments)
	p.segments = n
	loadOrds(ctx, g, p)
	results := make([]segResult, n)
	if err := runParallel(n, func(i int) { results[i] = runSegment(segCtx, p, g, i) }); err != nil {
		return fail(err, "segment panicked")
	}
	if err := parent.Err(); errors.Is(err, context.Canceled) {
		return fail(err, "cancelled")
	}
	res := &ShardResult{TotalRelation: RelationEq, Segments: n}
	tops := make([][]segHit, 0, n)
	segAggs := make([]map[string]*AggPartial, 0, n)
	complete := true
	for i := range results {
		sr := &results[i]
		res.Scanned += sr.scanned
		if sr.err != nil && !isCtxErr(sr.err) {
			return fail(sr.err, "segment failed")
		}
		if !sr.done {
			res.TimedOut = true
			continue
		}
		res.Total += sr.hits
		complete = complete && sr.complete
		if len(sr.top) > 0 {
			tops = append(tops, sr.top)
		}
		segAggs = append(segAggs, sr.aggs)
	}
	aggs := mergeShard(ctx, p.aggs, segAggs, g, p)
	if err := parent.Err(); errors.Is(err, context.Canceled) {
		return fail(err, "cancelled")
	}
	if err := p.failed.Load(); err != nil {
		return fail(*err, "aggregation failed")
	}
	buckets := 0
	if spec := cutShardAggs(p.aggs, aggs, &buckets); spec != nil {
		return fail(tooManyBuckets(spec), "too many buckets")
	}
	res.Aggs = aggs
	res.TimedOut = res.TimedOut || p.cut.Load()
	if res.TimedOut || !complete {
		res.TotalRelation = RelationGte
	}
	executed := time.Now()
	_, fetchSpan := tr.Start(ctx, "search.fetch")
	hits, err := fetchTops(parent, g, p, tops)
	fetchSpan.SetAttributes(attribute.Int("hits", len(hits)))
	fetchSpan.End()
	if err != nil {
		return fail(err, "fetch failed")
	}
	res.Hits = hits
	in := instruments()
	attrs := metric.WithAttributeSet(attribute.NewSet(attribute.String(telemetry.KeyIndex, r.Index)))
	in.recordPhase(ctx, r.Index, "plan", planned.Sub(start))
	in.recordPhase(ctx, r.Index, "execute", executed.Sub(planned))
	in.recordPhase(ctx, r.Index, "fetch", time.Since(executed))
	in.segments.Record(ctx, float64(n), attrs)
	in.scanned.Add(ctx, res.Scanned, attrs)
	in.matched.Add(ctx, res.Total, attrs)
	span.SetAttributes(
		attribute.Int64("total", res.Total),
		attribute.String("total_relation", res.TotalRelation),
		attribute.Int("hits", len(res.Hits)),
		attribute.Int64("scanned", res.Scanned),
		attribute.Bool("timed_out", res.TimedOut),
	)
	log := telemetry.WithRequest(ctx)
	if res.TimedOut {
		if suppressed, ok := timeoutWarnings.allow(r.Index); ok {
			log.WarnContext(ctx, "search: shard timed out; partial results",
				slog.String(telemetry.KeyIndex, r.Index), slog.Int64("total", res.Total),
				slog.Int("suppressed", suppressed),
				slog.Float64(telemetry.KeyDuration, float64(time.Since(start).Microseconds())/1000))
		}
	} else {
		log.DebugContext(ctx, "search: shard searched",
			slog.String(telemetry.KeyIndex, r.Index), slog.Int("segments", n), slog.Int64("total", res.Total),
			slog.Int64("scanned", res.Scanned), slog.Float64(telemetry.KeyDuration, float64(time.Since(start).Microseconds())/1000))
	}
	return res, nil
}

// shardDeadline is when a shard search started at start must be done: the earlier of
// ctx's deadline and start+timeout (a timeout of 0 adds none).
func shardDeadline(ctx context.Context, start time.Time, timeout time.Duration) (time.Time, bool) {
	deadline, ok := ctx.Deadline()
	if timeout > 0 {
		if d := start.Add(timeout); !ok || d.Before(deadline) {
			deadline, ok = d, true
		}
	}
	return deadline, ok
}

// deadlineReserve is how much of a shard search's time budget its segments leave for
// the merge of their partials: a tenth of the budget, between 1 ms and 1 s.
func deadlineReserve(budget time.Duration) time.Duration {
	return min(max(budget/10, time.Millisecond), time.Second)
}

// warnLimiter lets one warning per key through per interval, counting the rest.
type warnLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	last     map[string]time.Time
	dropped  map[string]int
}

// timeoutWarnings rate-limits the timed-out shard warning: one per index per 10 s.
var timeoutWarnings = &warnLimiter{interval: 10 * time.Second, last: map[string]time.Time{}, dropped: map[string]int{}}

// allow reports whether a warning for key may be logged now, and how many were
// dropped since the last one. Past 1024 keys it forgets them all: memory stays
// bounded, and many indexes just log a little more often.
func (w *warnLimiter) allow(key string) (int, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	if t, ok := w.last[key]; ok && now.Sub(t) < w.interval {
		w.dropped[key]++
		return 0, false
	}
	if len(w.last) > 1024 {
		clear(w.last)
		clear(w.dropped)
	}
	w.last[key] = now
	n := w.dropped[key]
	delete(w.dropped, key)
	return n, true
}

// topMerge merges the segments' sorted tops by global sort values.
type topMerge struct {
	g     *shard.Generation
	p     *prepared
	lists [][]segHit
	pos   []int
	order []int // a heap of list indexes, by their current head
	err   error
}

func (m *topMerge) id(h *segHit) string {
	if err := resolveID(&m.g.Segments[h.seg], h); err != nil && m.err == nil {
		m.err = err
	}
	return h.id
}

// resolveID reads h's id, once: from the id dictionary when h has its rank, else (or
// when the dictionary has no such rank) from its stored record.
func resolveID(sv *shard.SegmentView, h *segHit) error {
	if h.hasID {
		return nil
	}
	if h.hasRank {
		if id, ok := sv.Reader.IDAt(h.rank); ok {
			h.id, h.hasID = id, true
			return nil
		}
	}
	id, err := sv.Reader.ID(h.ord)
	if err != nil {
		return fmt.Errorf("search: segment %s: %w", sv.ID, err)
	}
	h.id, h.hasID = id, true
	return nil
}

func (m *topMerge) compare(a, b *segHit) int {
	for j, spec := range m.p.sorts {
		if spec.kind == sortID {
			c := cmpValue(m.id(a), m.id(b), spec.desc)
			if c == 0 {
				c = a.seg - b.seg
			}
			return c
		}
		if c := cmpValue(a.vals[j], b.vals[j], spec.desc); c != 0 {
			return c
		}
	}
	return a.seg - b.seg
}

func (m *topMerge) head(i int) *segHit { return &m.lists[i][m.pos[i]] }
func (m *topMerge) Len() int           { return len(m.order) }
func (m *topMerge) Less(i, j int) bool {
	return m.compare(m.head(m.order[i]), m.head(m.order[j])) < 0
}
func (m *topMerge) Swap(i, j int) { m.order[i], m.order[j] = m.order[j], m.order[i] }
func (m *topMerge) Push(x any)    { m.order = append(m.order, x.(int)) } //nolint:forcetypeassert,errcheck // only ints
func (m *topMerge) Pop() any {
	last := m.order[len(m.order)-1]
	m.order = m.order[:len(m.order)-1]
	return last
}

// fetchTops merges the segments' tops into the shard's best p.size hits and fetches
// their bodies. A canceled ctx stops the fetch; a passed deadline does not, so a search
// that timed out still returns the hits it found. The hits are chosen first, reading
// ids only where they break a tie; then the chosen records are fetched in parallel: a
// stored block's decompression is most of a small search.
func fetchTops(ctx context.Context, g *shard.Generation, p *prepared, tops [][]segHit) ([]Hit, error) {
	if p.size == 0 || len(tops) == 0 {
		return nil, nil
	}
	m := &topMerge{g: g, p: p, lists: tops, pos: make([]int, len(tops))}
	for i := range tops {
		m.order = append(m.order, i)
	}
	heap.Init(m)
	var chosen []*segHit
	for len(chosen) < p.size && m.Len() > 0 && m.err == nil {
		i := m.order[0]
		chosen = append(chosen, m.head(i))
		m.pos[i]++
		if m.pos[i] == len(m.lists[i]) {
			heap.Pop(m)
		} else {
			heap.Fix(m, 0)
		}
	}
	if m.err != nil {
		return nil, m.err
	}
	out := make([]Hit, len(chosen))
	errs := make([]error, len(chosen))
	const perTask = 2
	if err := runParallel((len(chosen)+perTask-1)/perTask, func(task int) {
		for k := task * perTask; k < min(len(chosen), (task+1)*perTask); k++ {
			if errs[k] = canceled(ctx); errs[k] != nil {
				return
			}
			out[k], errs[k] = fetchHit(g, p, chosen[k])
		}
	}); err != nil {
		return nil, err
	}
	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// fetchHit reads one chosen hit's id and body.
func fetchHit(g *shard.Generation, p *prepared, h *segHit) (Hit, error) {
	sv := &g.Segments[h.seg]
	if err := resolveID(sv, h); err != nil {
		return Hit{}, err
	}
	hit := Hit{ID: h.id, Ref: &HitRef{Segment: sv.ID, Ord: h.ord}}
	if !p.req.NoBodies {
		body, err := readBody(sv.Reader, h.ord, p.fields)
		if err != nil {
			return Hit{}, fmt.Errorf("search: segment %s, document %q: %w", sv.ID, h.id, err)
		}
		hit.Body = body
	}
	sortVals := make([]any, len(p.sorts))
	copy(sortVals, h.vals)
	for j, spec := range p.sorts {
		if spec.kind == sortID {
			sortVals[j] = h.id
		}
	}
	hit.Sort = sortVals
	return hit, nil
}

// readBody reads document ord's stored body, limited to fields when set.
func readBody(r *segment.Reader, ord uint32, fields []string) (json.RawMessage, error) {
	body, err := r.Stored(ord)
	if err != nil {
		return nil, err
	}
	if len(fields) > 0 {
		return filterBody(body, fields)
	}
	return body, nil
}

// ErrStaleHit is a hit FetchShard cannot find: its segment is not in the generation
// (fetch with the generation the query ran on).
var ErrStaleHit = errors.New("search: the hit's segment is not in this generation")

// FetchShard fills the bodies of hits found with Request.NoBodies (each with its Ref),
// from g, the generation the query ran on: the fetch phase of a query-then-fetch.
// fields limits each body as Request.Fields does. A canceled ctx stops the fetch; a
// passed deadline does not, so a search that timed out still gets the bodies of the
// hits it found.
func FetchShard(ctx context.Context, g *shard.Generation, hits []Hit, fields []string) error {
	_, span := otel.Tracer(telemetry.ScopeName).Start(ctx, "search.fetch", trace.WithAttributes(attribute.Int("hits", len(hits))))
	defer span.End()
	segs := make(map[string]*segment.Reader, len(g.Segments))
	for i := range g.Segments {
		segs[g.Segments[i].ID] = g.Segments[i].Reader
	}
	errs := make([]error, len(hits))
	if err := runParallel(len(hits), func(k int) {
		if errs[k] = canceled(ctx); errs[k] != nil {
			return
		}
		h := &hits[k]
		if h.Ref == nil {
			errs[k] = fmt.Errorf("%w: hit %q has no ref", ErrStaleHit, h.ID)
			return
		}
		r := segs[h.Ref.Segment]
		if r == nil {
			errs[k] = fmt.Errorf("%w: hit %q, segment %s", ErrStaleHit, h.ID, h.Ref.Segment)
			return
		}
		h.Body, errs[k] = readBody(r, h.Ref.Ord, fields)
	}); err != nil {
		return err
	}
	if err := errors.Join(errs...); err != nil {
		span.RecordError(err)
		return err
	}
	return nil
}

// canceled is ctx's error when ctx was canceled, nil while it is live or only past its
// deadline.
func canceled(ctx context.Context) error {
	if err := ctx.Err(); errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// filterBody keeps the members of a stored JSON object named in fields, in fields'
// order (a repeated member: its last value, as everywhere else). A field listed twice
// is written once.
func filterBody(body []byte, fields []string) (json.RawMessage, error) {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(body, &members); err != nil {
		return nil, err
	}
	out := []byte{'{'}
	first := true
	for _, f := range fields {
		v, ok := members[f]
		if !ok {
			continue
		}
		delete(members, f)
		if !first {
			out = append(out, ',')
		}
		first = false
		name, err := json.Marshal(f)
		if err != nil {
			return nil, err
		}
		out = append(out, name...)
		out = append(out, ':')
		out = append(out, v...)
	}
	return append(out, '}'), nil
}

// searchInstruments are the search metrics (spec section 11).
type searchInstruments struct {
	phase    metric.Float64Histogram
	segments metric.Float64Histogram
	scanned  metric.Int64Counter
	matched  metric.Int64Counter
}

// recordPhase records one phase's time (the catalogue's plan, execute, fetch or
// reduce).
func (in *searchInstruments) recordPhase(ctx context.Context, index, phase string, d time.Duration) {
	in.phase.Record(ctx, d.Seconds(), metric.WithAttributeSet(attribute.NewSet(
		attribute.String(telemetry.KeyIndex, index), attribute.String("phase", phase))))
}

// instruments come from the global meter provider, which telemetry.Setup installs
// (instruments made before it delegate to it once it is).
var instruments = sync.OnceValue(func() *searchInstruments {
	in := telemetry.NewInstruments(otel.Meter(telemetry.ScopeName))
	si := &searchInstruments{
		phase:    in.Histogram(telemetry.MetricSearchPhaseDuration),
		segments: in.Histogram(telemetry.MetricSearchSegmentsTouched),
		scanned:  in.Counter(telemetry.MetricSearchDocsScanned),
		matched:  in.Counter(telemetry.MetricSearchDocsMatched),
	}
	if err := in.Err(); err != nil {
		slog.Error("search metrics unavailable", slog.Any("error", err))
	}
	return si
})
