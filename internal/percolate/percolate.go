package percolate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
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
func Percolate(ctx context.Context, g *shard.Generation, docs []schema.Doc) ([][]string, error) {
	return defaultPercolator().Percolate(ctx, g, docs)
}

// view is one query segment of a generation, as percolation reads it.
type view struct {
	seg     *Segment           // nil: not the percolator's format (checked by brute force)
	other   shard.QuerySegment // when seg is nil
	deletes *roaring.Bitmap    // nil when none
	n       uint32
}

// docStats are one document's counts, for metrics.
type docStats struct {
	candidates, verified, matched int
	probe, verify                 time.Duration
}

// Percolate returns, for each document, the ids of the live saved queries in g that
// match it, sorted (nil when none). A document whose Fields is nil is analyzed from
// its ID and Body with g's mapping (an unmapped field under a strict mapping is an
// error); one whose Fields is set is taken as already analyzed under the index's
// mapping, as a bulk write analyzes it. Documents are percolated in parallel, at most
// Options.Threads at a time. g must stay acquired for the call; the ids returned are
// copies, valid after it.
//
// The result is exactly the brute force one, every live query checked with
// [query.Match]: the query index only skips queries that cannot match.
func (p *Percolator) Percolate(ctx context.Context, g *shard.Generation, docs []schema.Doc) ([][]string, error) {
	if g == nil {
		return nil, errors.New("percolate: no generation")
	}
	ctx, span := p.tr.Start(ctx, "percolate", trace.WithAttributes(append(p.attrs,
		attribute.Int("documents", len(docs)), attribute.Int64("queries", int64(g.NumQueries())))...)) //nolint:gosec // a query count
	defer span.End()
	start := time.Now()

	views := make([]view, len(g.QuerySegments))
	var maxN, maxEntries uint32
	always := 0
	for i := range g.QuerySegments {
		qs := &g.QuerySegments[i]
		v := view{n: qs.NumQueries}
		if seg, ok := qs.Segment.(*Segment); ok {
			v.seg = seg
			maxEntries = max(maxEntries, seg.NumEntries())
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

	out := make([][]string, len(docs))
	var total docStats
	var mu sync.Mutex
	var firstErr error
	var next atomic.Int64
	work := func() {
		sc, _ := p.scratchCache.Get().(*scratch)
		if sc == nil {
			sc = new(scratch)
		}
		sc.fit(maxN, maxEntries)
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
			ids, st, err := p.one(ctx, g, views, &docs[i], sc)
			if err != nil {
				mu.Lock()
				firstErr = cmpErr(firstErr, fmt.Errorf("percolate: document %d (%q): %w", i, docs[i].ID, err))
				mu.Unlock()
				next.Store(int64(len(docs))) // stop the others
				break
			}
			out[i] = ids
			local.candidates += st.candidates
			local.verified += st.verified
			local.matched += st.matched
			local.probe += st.probe
			local.verify += st.verify
		}
		p.scratchCache.Put(sc)
		mu.Lock()
		total.candidates += local.candidates
		total.verified += local.verified
		total.matched += local.matched
		total.probe += local.probe
		total.verify += local.verify
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

	if total.matched > 0 {
		p.verified.Add(ctx, int64(total.matched), p.matchSet)
	}
	if miss := total.verified - total.matched; miss > 0 {
		p.verified.Add(ctx, int64(miss), p.missSet)
	}
	span.SetAttributes(attribute.Int("candidates", total.candidates), attribute.Int("verified", total.verified),
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
func (p *Percolator) one(ctx context.Context, g *shard.Generation, views []view, d *schema.Doc, sc *scratch) ([]string, docStats, error) {
	var st docStats
	if d.Fields == nil {
		analyzed, _, err := schema.Analyze(g.Mapping(), d.ID, d.Body)
		if err != nil {
			return nil, st, err
		}
		d = &analyzed
	}
	runs := 0 // segments that matched: each appends a run sorted by id
	for i := range views {
		v := &views[i]
		before := len(sc.hits)
		if v.seg == nil {
			if err := bruteForce(v, d, sc, &st); err != nil {
				sc.clearHits()
				return nil, st, err
			}
			runs += 2 // not sorted
			continue
		}
		t0 := time.Now()
		v.seg.collect(d, sc)
		t1 := time.Now()
		st.candidates += len(sc.cands)
		err := verify(v, d, sc, &st)
		sc.reset()
		st.probe += t1.Sub(t0)
		st.verify += time.Since(t1)
		if err != nil {
			sc.clearHits()
			return nil, st, err
		}
		if len(sc.hits) > before {
			runs++
		}
	}
	ids := sc.results(runs <= 1)
	p.duration.Record(ctx, st.probe.Seconds(), p.probeSet)
	p.duration.Record(ctx, st.verify.Seconds(), p.verifySet)
	p.candidates.Record(ctx, float64(st.candidates), p.set)
	return ids, st, nil
}

// verify checks every candidate in sc with the exact matcher, appending the ids of the
// live ones that match to sc.hits, sorted by id.
func verify(v *view, d *schema.Doc, sc *scratch, st *docStats) error {
	seg := v.seg
	sc.matched = sc.matched[:0]
	for _, ord := range sc.cands {
		if v.deletes != nil && v.deletes.Contains(ord) {
			continue
		}
		rep := seg.class(ord)
		var hit bool
		switch sc.memo[rep] {
		case memoMatch:
			hit = true
		case memoMiss:
		default:
			c, err := seg.compiledQuery(rep)
			if err != nil {
				return err
			}
			st.verified++
			hit = c.Match(d)
			if hit {
				sc.memo[rep] = memoMatch
			} else {
				sc.memo[rep] = memoMiss
			}
			sc.memoSet = append(sc.memoSet, rep)
		}
		if hit {
			sc.matched = append(sc.matched, uint64(seg.rank(ord))<<32|uint64(ord))
		}
	}
	st.matched += len(sc.matched)
	slices.Sort(sc.matched) // by rank: by id
	for _, m := range sc.matched {
		sc.hits = append(sc.hits, seg.id(lo32(m))) // the low half is the ordinal
	}
	return nil
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
		st.verified++
		if query.Match(q.Query, d) {
			st.matched++
			sc.hits = append(sc.hits, []byte(q.ID))
		}
	}
	return nil
}
