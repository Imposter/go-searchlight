package percolate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// Options configures a [Percolator]. The zero value is usable.
type Options struct {
	// Index and Shard label spans and metrics.
	Index string
	Shard int
	// Threads bounds the goroutines one call verifies documents with; 0 means
	// GOMAXPROCS.
	Threads int
	// Logger, Tracer and Meter are the telemetry; nil means slog.Default() and the
	// global OpenTelemetry providers (which telemetry.Setup installs).
	Logger *slog.Logger
	Tracer trace.Tracer
	Meter  metric.Meter
}

// Percolator percolates documents against a shard's saved queries. It is safe for
// concurrent use.
type Percolator struct {
	threads int
	log     *slog.Logger
	tr      trace.Tracer
	attrs   []attribute.KeyValue

	duration     metric.Float64Histogram
	candidates   metric.Float64Histogram
	verified     metric.Int64Counter
	alwaysCheck  metric.Float64Gauge
	set          metric.MeasurementOption
	probeSet     metric.RecordOption
	verifySet    metric.RecordOption
	matchSet     metric.AddOption
	missSet      metric.AddOption
	scratchCache sync.Pool
	plan         atomic.Pointer[splitPlan]
}

// New returns a Percolator.
func New(opts Options) *Percolator {
	if opts.Threads <= 0 {
		opts.Threads = runtime.GOMAXPROCS(0)
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Tracer == nil {
		opts.Tracer = otel.GetTracerProvider().Tracer(telemetry.ScopeName)
	}
	if opts.Meter == nil {
		opts.Meter = otel.GetMeterProvider().Meter(telemetry.ScopeName)
	}
	base := []attribute.KeyValue{attribute.String(telemetry.KeyIndex, opts.Index), attribute.Int(telemetry.KeyShard, opts.Shard)}
	in := telemetry.NewInstruments(opts.Meter)
	p := &Percolator{
		threads:     opts.Threads,
		log:         opts.Logger.With(telemetry.KeyIndex, opts.Index, telemetry.KeyShard, opts.Shard),
		tr:          opts.Tracer,
		attrs:       base,
		duration:    in.Histogram(telemetry.MetricPercolateDuration),
		candidates:  in.Histogram(telemetry.MetricPercolateCandidates),
		verified:    in.Counter(telemetry.MetricPercolateVerifications),
		alwaysCheck: in.Gauge(telemetry.MetricPercolateAlwaysCheck),
		set:         metric.WithAttributeSet(attribute.NewSet(base...)),
		probeSet:    metric.WithAttributeSet(attribute.NewSet(append(base[:2:2], attribute.String("phase", "probe"))...)),
		verifySet:   metric.WithAttributeSet(attribute.NewSet(append(base[:2:2], attribute.String("phase", "verify"))...)),
		matchSet:    metric.WithAttributeSet(attribute.NewSet(append(base[:2:2], attribute.String("result", "match"))...)),
		missSet:     metric.WithAttributeSet(attribute.NewSet(append(base[:2:2], attribute.String("result", "miss"))...)),
	}
	p.scratchCache.New = func() any { return new(scratch) }
	if err := in.Err(); err != nil {
		p.log.Error("percolator metrics unavailable", slog.Any("error", err))
	}
	return p
}

var defaultPercolator = sync.OnceValue(func() *Percolator { return New(Options{}) })

// Percolate is [Percolator.Percolate] on a process-wide Percolator with default
// options.
func Percolate(ctx context.Context, g *shard.Generation, docs []schema.Doc) ([]IDs, error) {
	return defaultPercolator().Percolate(ctx, g, docs)
}

// view is one query segment of a generation, as percolation reads it.
type view struct {
	seg     *Segment           // nil: not the percolator's format (checked by brute force)
	other   shard.QuerySegment // when seg is nil
	deletes *roaring.Bitmap    // nil when none
	n       uint32
}

// docStats are one document's counts, for metrics: candidates gathered, programs run
// by verdict (a memoized verdict is not a run), ids returned, and the time spent
// probing and verifying.
type docStats struct {
	candidates, verifiedMatch, verifiedMiss, matched int
	probe, verify                                    time.Duration
}

// count counts one program evaluation by its verdict.
func (s *docStats) count(hit bool) {
	if hit {
		s.verifiedMatch++
	} else {
		s.verifiedMiss++
	}
}

// add sums o into s.
func (s *docStats) add(o *docStats) {
	s.candidates += o.candidates
	s.verifiedMatch += o.verifiedMatch
	s.verifiedMiss += o.verifiedMiss
	s.matched += o.matched
	s.probe += o.probe
	s.verify += o.verify
}

// Percolate returns, for each document, the ids of the live saved queries in g that
// match it, sorted, as a JSON array (nil when none). A document whose Fields is nil is
// analyzed from its ID and Body with g's mapping (an unmapped field under a strict
// mapping is an error); one whose Fields is set is taken as already analyzed under the
// index's mapping, as a bulk write or [schema.AnalyzeForMatch] analyzes it. Documents
// are percolated in parallel, at most Options.Threads at a time. g must stay acquired
// for the call; the arrays returned are copies, valid after it.
//
// The span records the call's candidates, programs run and matches, and probe_ms and
// verify_ms: per-document probe and verify time summed over every worker (busy time,
// which exceeds the wall time when documents run in parallel).
//
// The result is exactly the brute force one, every live query checked with
// [query.Match]: the query index only skips queries that cannot match, and each
// candidate's program decides as the matcher does.
func (p *Percolator) Percolate(ctx context.Context, g *shard.Generation, docs []schema.Doc) ([]IDs, error) {
	if g == nil {
		return nil, errors.New("percolate: no generation")
	}
	ctx, span := p.tr.Start(ctx, "percolate", trace.WithAttributes(append(p.attrs,
		attribute.Int("documents", len(docs)), attribute.Int64("queries", int64(g.NumQueries())))...)) //nolint:gosec // a query count
	defer span.End()
	start := time.Now()

	views := make([]view, len(g.QuerySegments))
	var maxN, maxEntries uint32
	maxFields, always := 0, 0
	for i := range g.QuerySegments {
		qs := &g.QuerySegments[i]
		v := view{n: qs.NumQueries}
		if seg, ok := qs.Segment.(*Segment); ok {
			v.seg = seg
			maxEntries = max(maxEntries, seg.NumEntries())
			maxFields = max(maxFields, len(seg.fields))
			always += seg.NumAlways()
		} else {
			v.other = qs.Segment
		}
		if qs.Deletes != nil && !qs.Deletes.IsEmpty() {
			v.deletes = qs.Deletes
		}
		views[i] = v
		maxN = max(maxN, v.n)
	}
	p.alwaysCheck.Record(ctx, float64(always), p.set)
	size := scratchSize{n: maxN, entries: maxEntries, fields: maxFields}
	win := p.planFor(views, len(docs))

	out := make([]IDs, len(docs))
	var total docStats
	var mu sync.Mutex
	var firstErr error
	var next atomic.Int64
	work := func() {
		sc, _ := p.scratchCache.Get().(*scratch)
		if sc == nil {
			sc = new(scratch)
		}
		sc.fit(size.n, size.entries, size.fields)
		var local docStats
		for {
			i := int(next.Add(1) - 1)
			if i >= len(docs) {
				break
			}
			if err := ctx.Err(); err != nil {
				mu.Lock()
				firstErr = cmpErr(firstErr, err)
				mu.Unlock()
				break
			}
			var ids IDs
			var st docStats
			var err error
			if win != nil {
				ids, st, err = p.split(ctx, g, views, &docs[i], win, sc, size)
			} else {
				ids, st, err = p.one(ctx, g, views, &docs[i], sc)
			}
			if err != nil {
				mu.Lock()
				firstErr = cmpErr(firstErr, fmt.Errorf("percolate: document %d (%q): %w", i, docs[i].ID, err))
				mu.Unlock()
				next.Store(int64(len(docs))) // stop the others
				break
			}
			out[i] = ids
			local.add(&st)
		}
		p.scratchCache.Put(sc)
		mu.Lock()
		total.add(&local)
		mu.Unlock()
	}
	if workers := min(p.threads, len(docs)); workers <= 1 {
		work()
	} else {
		var wg sync.WaitGroup
		for range workers {
			wg.Go(work)
		}
		wg.Wait()
	}

	if total.verifiedMatch > 0 {
		p.verified.Add(ctx, int64(total.verifiedMatch), p.matchSet)
	}
	if total.verifiedMiss > 0 {
		p.verified.Add(ctx, int64(total.verifiedMiss), p.missSet)
	}
	// probe_ms and verify_ms are per-document time summed over every worker: with
	// several workers they exceed the call's wall time.
	span.SetAttributes(attribute.Int("candidates", total.candidates), attribute.Int("verified", total.verifiedMatch+total.verifiedMiss),
		attribute.Int("matches", total.matched), attribute.Int("always_check", always),
		attribute.Float64("probe_ms", float64(total.probe.Microseconds())/1000), attribute.Float64("verify_ms", float64(total.verify.Microseconds())/1000))
	if firstErr != nil {
		span.RecordError(firstErr)
		span.SetStatus(codes.Error, "percolate failed")
		return nil, firstErr
	}
	p.log.DebugContext(ctx, "percolated", slog.Int("documents", len(docs)), slog.Int("candidates", total.candidates),
		slog.Int("matches", total.matched), slog.Float64(telemetry.KeyDuration, float64(time.Since(start).Microseconds())/1000))
	return out, nil
}

// cmpErr keeps the first error, preferring a real one to a cancellation.
func cmpErr(have, got error) error {
	if have == nil || errors.Is(have, context.Canceled) || errors.Is(have, context.DeadlineExceeded) {
		if got != nil {
			return got
		}
	}
	return have
}

// one percolates one document across every query segment.
func (p *Percolator) one(ctx context.Context, g *shard.Generation, views []view, d *schema.Doc, sc *scratch) (IDs, docStats, error) {
	var st docStats
	if d.Fields == nil {
		analyzed, _, err := schema.AnalyzeForMatch(g.Mapping(), d.ID, d.Body)
		if err != nil {
			return nil, st, err
		}
		d = &analyzed
	}
	for i := range views {
		v := &views[i]
		if v.seg == nil {
			if err := bruteForce(v, d, sc, &st); err != nil {
				sc.clearHits()
				return nil, st, err
			}
			continue
		}
		t0 := time.Now()
		v.seg.collect(d, sc)
		t1 := time.Now()
		st.candidates += len(sc.cands)
		verify(v, sc, &st)
		sc.reset()
		st.probe += t1.Sub(t0)
		st.verify += time.Since(t1)
	}
	ids := sc.results()
	p.duration.Record(ctx, st.probe.Seconds(), p.probeSet)
	p.duration.Record(ctx, st.verify.Seconds(), p.verifySet)
	p.candidates.Record(ctx, float64(st.candidates), p.set)
	return ids, st, nil
}

// scratchSize is what a scratch is fit to: the most queries, dictionary entries and
// fields of a generation's query segments.
type scratchSize struct {
	n, entries uint32
	fields     int
}

// Splitting one document: minWindowQueries is the fewest saved queries per worker
// (a variable so tests can force splits), windowsPerWorker how many windows each
// worker's share is cut into, and pivotSamples how many ids per window the pivots
// are chosen from.
var minWindowQueries uint32 = 16 << 10

const (
	windowsPerWorker = 2
	pivotSamples     = 32
)

// splitPlan splits a generation's query segments into k windows by id, so that one
// document's percolation runs on several cores: window w holds, in every segment, the
// ranks whose ids lie in [pivot w, pivot w+1). The pivots are quantiles of ids sampled
// from every segment in proportion to its size, so windows hold about as many queries
// each. Windows hold disjoint id ranges in order, so their matches, each window's
// merged, concatenate into the sorted answer.
type splitPlan struct {
	segs    []*Segment
	workers int
	k       int
	bounds  []uint32 // per segment, its k+1 window boundaries (ranks)
}

// bound returns the first rank of window w in segment i (w == k: its rank count).
func (win *splitPlan) bound(i, w int) uint32 { return win.bounds[i*(win.k+1)+w] }

// planFor returns how one document's percolation splits across views, nil when it
// does not: a document gets an equal share of the threads as workers, each with at
// least minWindowQueries queries, and windowsPerWorker windows per worker. Only
// segments the percolator built split. The plan of the last generation seen is kept
// for the next call.
func (p *Percolator) planFor(views []view, docs int) *splitPlan {
	if docs == 0 {
		return nil
	}
	var total uint64
	for i := range views {
		if views[i].seg == nil {
			return nil
		}
		total += uint64(views[i].n)
	}
	workers := int(min(uint64(p.threads/docs), total/uint64(minWindowQueries))) //nolint:gosec // at most the thread count
	if workers <= 1 {
		return nil
	}
	if win := p.plan.Load(); win != nil && win.workers == workers && slices.EqualFunc(win.segs, views, func(s *Segment, v view) bool { return s == v.seg }) {
		return win
	}
	win := newSplitPlan(views, workers, workers*windowsPerWorker)
	p.plan.Store(win)
	return win
}

// newSplitPlan splits views (each a segment the percolator built) into k windows for
// workers workers.
func newSplitPlan(views []view, workers, k int) *splitPlan {
	var total uint64
	for i := range views {
		total += uint64(views[i].n)
	}
	samples := uint64(k) * pivotSamples //nolint:gosec // k is a small positive count
	var sample [][]byte
	for i := range views {
		seg := views[i].seg
		m := (uint64(seg.n)*samples + total - 1) / max(total, 1)
		for j := range m {
			sample = append(sample, seg.rawIDAt(uint32(uint64(seg.n)*j/m))) //nolint:gosec // below seg.n
		}
	}
	slices.SortFunc(sample, bytes.Compare)
	win := &splitPlan{workers: workers, k: k, segs: make([]*Segment, len(views)), bounds: make([]uint32, len(views)*(k+1))}
	for i := range views {
		seg := views[i].seg
		win.segs[i] = seg
		b := win.bounds[i*(k+1) : (i+1)*(k+1)]
		b[k] = seg.n
		for w := 1; w < k; w++ {
			if len(sample) == 0 {
				continue
			}
			pivot := sample[len(sample)*w/k]
			b[w] = uint32(sort.Search(int(seg.n), func(r int) bool { //nolint:gosec // a rank
				return bytes.Compare(seg.rawIDAt(uint32(r)), pivot) >= 0 //nolint:gosec // a rank
			}))
		}
	}
	return win
}

// split percolates one document across every query segment, window by window, on
// win.workers goroutines (this one and helpers, sc serving this one) that take the
// windows in turn, and joins the windows' matches in order.
func (p *Percolator) split(ctx context.Context, g *shard.Generation, views []view, d *schema.Doc, win *splitPlan, sc *scratch, size scratchSize) (IDs, docStats, error) {
	if d.Fields == nil {
		analyzed, _, err := schema.AnalyzeForMatch(g.Mapping(), d.ID, d.Body)
		if err != nil {
			return nil, docStats{}, err
		}
		d = &analyzed
	}
	// A window's matches are buf[lo:hi] of the scratch of the worker that took it.
	type part struct {
		buf    *[]byte
		lo, hi int
	}
	parts := make([]part, win.k)
	scs := make([]*scratch, win.workers)
	stats := make([]docStats, win.workers)
	var next atomic.Int32
	work := func(id int) {
		c := scs[id]
		if c == nil {
			c, _ = p.scratchCache.Get().(*scratch)
			if c == nil {
				c = new(scratch)
			}
			c.fit(size.n, size.entries, size.fields)
			scs[id] = c
		}
		st := &stats[id]
		c.buf = c.buf[:0]
		for {
			w := int(next.Add(1) - 1)
			if w >= win.k {
				return
			}
			for i := range views {
				v := &views[i]
				lo, hi := win.bound(i, w), win.bound(i, w+1)
				if lo == hi {
					continue
				}
				t0 := time.Now()
				v.seg.collectIn(d, c, lo, hi)
				t1 := time.Now()
				st.candidates += len(c.cands)
				verify(v, c, st)
				c.reset()
				st.probe += t1.Sub(t0)
				st.verify += time.Since(t1)
			}
			lo := len(c.buf)
			c.buf = c.merger.appendRanks(c.buf, c.ranks, c.runs)
			parts[w] = part{buf: &c.buf, lo: lo, hi: len(c.buf)}
			c.clearHits()
		}
	}
	scs[0] = sc
	var wg sync.WaitGroup
	for id := 1; id < win.workers; id++ {
		wg.Go(func() { work(id) })
	}
	work(0)
	wg.Wait()
	var st docStats
	for i := range stats {
		st.add(&stats[i])
	}
	n := 1
	for _, pt := range parts {
		n += pt.hi - pt.lo
	}
	var ids IDs
	if n > 1 {
		ids = make(IDs, 1, n)
		ids[0] = '['
		for _, pt := range parts {
			ids = append(ids, (*pt.buf)[pt.lo:pt.hi]...)
		}
		ids[n-1] = ']'
	}
	for _, c := range scs[1:] {
		if c != nil {
			p.scratchCache.Put(c)
		}
	}
	p.duration.Record(ctx, st.probe.Seconds(), p.probeSet)
	p.duration.Record(ctx, st.verify.Seconds(), p.verifySet)
	p.candidates.Record(ctx, float64(st.candidates), p.set)
	return ids, st, nil
}

// verify evaluates every candidate's program (see prog.go) on the document whose values
// collect left in sc, and records the ranks of the live ones that match as one run of
// sc's matches (the candidates are ranks: taken in order, they are in id order).
func verify(v *view, sc *scratch, st *docStats) {
	seg := v.seg
	sc.inOrder()
	start := len(sc.ranks)
	classes, memo := seg.classes, sc.memo
	for _, r := range sc.cands {
		if v.deletes != nil && v.deletes.Contains(seg.ordAt(r)) {
			continue
		}
		var hit bool
		switch rep := classes[r]; {
		case rep == trivialClass:
			hit = true
		case rep == noClass:
			hit = sc.evalProg(seg.program(r))
			st.count(hit)
		case memo[rep] == memoMatch:
			hit = true
		case memo[rep] == memoMiss:
		default:
			hit = sc.evalProg(seg.program(r))
			st.count(hit)
			memo[rep] = pick[uint8](hit, memoMatch, memoMiss)
			sc.memoSet = append(sc.memoSet, rep)
		}
		if hit {
			sc.ranks = append(sc.ranks, r)
		}
	}
	st.matched += len(sc.ranks) - start
	sc.endRun(seg, start)
}

// bruteForce checks every live query of a segment the percolator did not build.
func bruteForce(v *view, d *schema.Doc, sc *scratch, st *docStats) error {
	for ord := range v.n {
		if v.deletes != nil && v.deletes.Contains(ord) {
			continue
		}
		q, err := v.other.Query(ord)
		if err != nil {
			return err
		}
		st.candidates++
		if !query.Match(q.Query, d) {
			st.verifiedMiss++
			continue
		}
		st.verifiedMatch++
		st.matched++
		sc.lits = append(sc.lits, idLiteral(q.ID))
	}
	return nil
}
