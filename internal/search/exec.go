package search

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/schema"
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
	// Body is the stored document, limited to Request.Fields when set.
	Body json.RawMessage `json:"body"`
}

// ShardResult is one shard's answer: its own top hits (bodies fetched), its exact
// total, and partial aggregations for [Reduce].
type ShardResult struct {
	Hits []Hit `json:"hits"`
	// Total is how many live documents matched: exact (bitmaps make counting free),
	// unless the shard timed out, when it is what the finished segments matched and
	// TotalRelation is RelationGte. Reduce applies TrackTotal.
	Total         int64                  `json:"total"`
	TotalRelation string                 `json:"total_relation"`
	Aggs          map[string]*AggPartial `json:"aggs,omitempty"`
	// TimedOut is set when the search ran out of time (Request.Timeout or the
	// context's deadline) before every segment finished: the hits, total and
	// aggregations are those of the segments that did.
	TimedOut bool `json:"timed_out"`
	// Segments is how many segments the shard searched; Scanned how many documents it
	// checked one at a time (residual checks, aggregations, sorting aside).
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

// runParallel calls fn(0..n-1), on the calling goroutine and on as many pool
// goroutines as are free, and returns when every call has.
func runParallel(n int, fn func(i int)) {
	if n == 0 {
		return
	}
	pool := *slots.Load()
	var next atomic.Int64
	work := func() {
		for {
			i := int(next.Add(1) - 1)
			if i >= n {
				return
			}
			fn(i)
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
	mapping *schema.Mapping
	cache   *shard.FilterCache

	cacheMemo map[*leafPlan]*roaring.Bitmap // nil value: a miss
	estimates map[*leafPlan]estimate
	sources   map[string]*fieldSrc

	scanned int64
	err     error
	warned  atomic.Bool // residual checks run in parallel
}

func newSegExec(ctx context.Context, p *prepared, g *shard.Generation, i int) *segExec {
	sv := &g.Segments[i]
	all := roaring.New()
	all.AddRange(0, uint64(sv.NumDocs))
	return &segExec{
		ctx: ctx, p: p, sv: sv, seg: i, r: sv.Reader, n: sv.NumDocs, allDocs: all,
		mapping: g.Mapping(), cache: g.FilterCache(),
		cacheMemo: map[*leafPlan]*roaring.Bitmap{},
		estimates: map[*leafPlan]estimate{},
		sources:   map[string]*fieldSrc{},
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

func (s *segExec) warnResidual(id string, err error) {
	if !s.warned.CompareAndSwap(false, true) {
		return
	}
	telemetry.WithRequest(s.ctx).WarnContext(s.ctx, "search: a stored document does not analyze; it matches no residual check",
		slog.String("segment", s.sv.ID), slog.String("id", id), slog.Any("error", err))
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
	done    bool
	err     error
	hits    int64
	top     []segHit
	aggs    map[string]*AggPartial
	scanned int64
}

func runSegment(ctx context.Context, p *prepared, g *shard.Generation, i int) (out segResult) {
	s := newSegExec(ctx, p, g, i)
	if s.checkCtx() {
		out.err = s.err
		return out
	}
	live := s.allDocs
	if !s.sv.Deletes.IsEmpty() {
		live = roaring.AndNot(s.allDocs, s.sv.Deletes)
	}
	hits := s.eval(p.root, live) //nolint:contextcheck // only the filter cache's hit counter takes no context
	if s.err == nil {
		out.hits = card(hits)
		out.top = s.topK(hits, p.size)
	}
	if s.err == nil {
		out.aggs = s.aggregate(hits)
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
// operations per segment, with residual checks where the index cannot decide alone.
// Segments run in parallel on the search pool ([SetThreads]). When the request's
// Timeout or ctx's deadline passes, segments stop where they are and the result holds
// what the finished ones found, with TimedOut set; a cancelled ctx returns its error.
// A request the mapping refuses (an unknown aggregation type, a sort on a list, ...)
// is a *RequestError.
func ExecuteShard(ctx context.Context, g *shard.Generation, r *Request) (*ShardResult, error) {
	if g == nil || r == nil {
		return nil, errNoGeneration
	}
	start := time.Now()
	ctx, span := otel.Tracer(telemetry.ScopeName).Start(ctx, "search.shard", trace.WithAttributes(
		attribute.String(telemetry.KeyIndex, r.Index),
		attribute.Int64("generation", int64(g.Gen())), //nolint:gosec // a commit counter
		attribute.Int64("seq", g.Seq()),
		attribute.Int("segments", len(g.Segments)),
		attribute.Int("size", r.Size),
		attribute.Int("aggs", len(r.Aggs)),
	))
	defer span.End()
	p, err := prepare(r, g.Mapping())
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "invalid request")
		return nil, err
	}
	parent := ctx
	if r.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.Timeout)
		defer cancel()
	}
	n := len(g.Segments)
	results := make([]segResult, n)
	runParallel(n, func(i int) { results[i] = runSegment(ctx, p, g, i) })
	if err := parent.Err(); errors.Is(err, context.Canceled) {
		span.RecordError(err)
		span.SetStatus(codes.Error, "cancelled")
		return nil, err
	}
	res := &ShardResult{TotalRelation: RelationEq, Segments: n}
	tops := make([][]segHit, 0, n)
	var aggs map[string]*AggPartial
	if len(p.aggs) > 0 {
		aggs = make(map[string]*AggPartial, len(p.aggs))
		for _, spec := range p.aggs {
			aggs[spec.name] = &AggPartial{Type: spec.typ}
		}
	}
	for i := range results {
		sr := &results[i]
		res.Scanned += sr.scanned
		if sr.err != nil && !isCtxErr(sr.err) {
			span.RecordError(sr.err)
			span.SetStatus(codes.Error, "segment failed")
			return nil, sr.err
		}
		if !sr.done {
			res.TimedOut = true
			continue
		}
		res.Total += sr.hits
		if len(sr.top) > 0 {
			tops = append(tops, sr.top)
		}
		for _, spec := range p.aggs {
			mergePartial(aggs[spec.name], sr.aggs[spec.name], spec)
		}
	}
	for _, spec := range p.aggs {
		cutShard(aggs[spec.name], spec)
	}
	res.Aggs = aggs
	if res.TimedOut {
		res.TotalRelation = RelationGte
	}
	fetchStart := time.Now()
	hits, err := fetchTops(g, p, tops)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "fetch failed")
		return nil, err
	}
	res.Hits = hits
	in := instruments()
	attrs := metric.WithAttributeSet(attribute.NewSet(attribute.String(telemetry.KeyIndex, r.Index)))
	in.phase.Record(ctx, time.Since(start).Seconds(), metric.WithAttributeSet(attribute.NewSet(
		attribute.String(telemetry.KeyIndex, r.Index), attribute.String("phase", "query"))))
	in.phase.Record(ctx, time.Since(fetchStart).Seconds(), metric.WithAttributeSet(attribute.NewSet(
		attribute.String(telemetry.KeyIndex, r.Index), attribute.String("phase", "fetch"))))
	in.segments.Record(ctx, float64(n), attrs)
	in.scanned.Add(ctx, res.Scanned, attrs)
	in.matched.Add(ctx, res.Total, attrs)
	span.SetAttributes(
		attribute.Int64("total", res.Total),
		attribute.Int("hits", len(res.Hits)),
		attribute.Int64("scanned", res.Scanned),
		attribute.Bool("timed_out", res.TimedOut),
	)
	log := telemetry.WithRequest(ctx)
	if res.TimedOut {
		log.WarnContext(ctx, "search: shard timed out; partial results",
			slog.String(telemetry.KeyIndex, r.Index), slog.Int64("total", res.Total),
			slog.Float64(telemetry.KeyDuration, float64(time.Since(start).Microseconds())/1000))
	} else {
		log.DebugContext(ctx, "search: shard searched",
			slog.String(telemetry.KeyIndex, r.Index), slog.Int("segments", n), slog.Int64("total", res.Total),
			slog.Int64("scanned", res.Scanned), slog.Float64(telemetry.KeyDuration, float64(time.Since(start).Microseconds())/1000))
	}
	return res, nil
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
	if !h.hasID {
		id, err := m.g.Segments[h.seg].Reader.ID(h.ord)
		if err != nil && m.err == nil {
			m.err = fmt.Errorf("search: segment %s: %w", m.g.Segments[h.seg].ID, err)
		}
		h.id, h.hasID = id, true
	}
	return h.id
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
// their bodies.
func fetchTops(g *shard.Generation, p *prepared, tops [][]segHit) ([]Hit, error) {
	if p.size == 0 || len(tops) == 0 {
		return nil, nil
	}
	m := &topMerge{g: g, p: p, lists: tops, pos: make([]int, len(tops))}
	for i := range tops {
		m.order = append(m.order, i)
	}
	heap.Init(m)
	// Choose first, reading ids only where they break a tie; then fetch the chosen
	// records in parallel: a stored block's decompression is most of a small search.
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
	runParallel((len(chosen)+perTask-1)/perTask, func(task int) {
		for k := task * perTask; k < min(len(chosen), (task+1)*perTask); k++ {
			out[k], errs[k] = fetchHit(g, p, chosen[k])
		}
	})
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
	if !h.hasID {
		id, err := sv.Reader.ID(h.ord)
		if err != nil {
			return Hit{}, fmt.Errorf("search: segment %s: %w", sv.ID, err)
		}
		h.id, h.hasID = id, true
	}
	body, err := sv.Reader.Stored(h.ord)
	if err != nil {
		return Hit{}, fmt.Errorf("search: segment %s: %w", sv.ID, err)
	}
	if len(p.fields) > 0 {
		if body, err = filterBody(body, p.fields); err != nil {
			return Hit{}, fmt.Errorf("search: document %q: %w", h.id, err)
		}
	}
	sortVals := make([]any, len(p.sorts))
	copy(sortVals, h.vals)
	for j, spec := range p.sorts {
		if spec.kind == sortID {
			sortVals[j] = h.id
		}
	}
	return Hit{ID: h.id, Sort: sortVals, Body: body}, nil
}

// filterBody keeps the members of a stored JSON object named in fields, in fields'
// order (a repeated member: its last value, as everywhere else).
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
		delete(members, f) // a field listed twice is written once
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
