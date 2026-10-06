package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// Group commit defaults: flush every 2 ms or at 4,096 changes, so a few concurrent
// _bulk requests of a thousand documents share one transaction (and its fsync).
const (
	DefaultGroupCommitDelay   = 2 * time.Millisecond
	DefaultGroupCommitChanges = 4096
)

// GroupCommitOptions tune a GroupCommitter. Zero values take the defaults.
type GroupCommitOptions struct {
	// MaxDelay is the longest a request waits for company before its batch
	// is flushed, counted from its arrival.
	MaxDelay time.Duration
	// MaxChanges flushes a batch as soon as it holds this many changes. A
	// single request larger than this is committed on its own.
	MaxChanges int
	// Tracer and Meter default to the global providers'.
	Tracer trace.Tracer
	Meter  metric.Meter
	// Clock times the batching window; nil means clock.Real.
	Clock clock.Clock

	// received, when set (tests only), runs in the committer's goroutine as
	// each request is taken into a batch.
	received func()
}

// GroupCommitter coalesces concurrent Apply calls on a node into one
// transaction, the way Elasticsearch amortizes translog fsyncs (spec §8).
// Each caller gets its own contiguous seq range within the batch, in arrival
// order, and a failed transaction fails every caller in it.
//
// One caller's bad request never fails another's. Each request is validated
// on its own before it joins a batch, so an invalid change is reported to
// its caller alone (as a *ChangeError positioned in that request). A request
// naming a missing index gets its own *IndexNotFoundError and the rest is
// retried without it. When IfSeq conditions fail, only the first conflicting
// request is rejected and everything after it is re-evaluated in the next
// attempt, since its conditions may have failed only because of the rejected
// request; a rejection is final only once every request before it has
// committed or itself been answered.
//
// A caller whose context ends before its batch is flushed is dropped from it.
// A caller whose context ends while its batch is committing gets the
// context's error and its changes may or may not have committed; the
// transaction is cancelled only when every caller in it has given up.
type GroupCommitter struct {
	st         Applier
	maxDelay   time.Duration
	maxChanges int
	tracer     trace.Tracer
	clock      clock.Clock
	batchSize  metric.Float64Histogram
	received   func()

	reqs    chan *gcRequest
	closing chan struct{}
	done    chan struct{}
	once    sync.Once

	flushes atomic.Int64
}

type gcRequest struct {
	ctx     context.Context
	changes []Change
	prep    *prepared
	arrived time.Time
	result  chan gcResult // buffered, 1
}

type gcResult struct {
	first, last int64
	err         error
}

// NewGroupCommitter starts a committer over st. Close it to stop.
func NewGroupCommitter(st Applier, o GroupCommitOptions) *GroupCommitter {
	if o.MaxDelay < 0 {
		o.MaxDelay = 0
	} else if o.MaxDelay == 0 {
		o.MaxDelay = DefaultGroupCommitDelay
	}
	if o.MaxChanges <= 0 {
		o.MaxChanges = DefaultGroupCommitChanges
	}
	if o.Tracer == nil {
		o.Tracer = otel.Tracer(telemetry.ScopeName)
	}
	if o.Meter == nil {
		o.Meter = otel.Meter(telemetry.ScopeName)
	}
	if o.Clock == nil {
		o.Clock = clock.Real{}
	}
	in := telemetry.NewInstruments(o.Meter)
	g := &GroupCommitter{
		st:         st,
		maxDelay:   o.MaxDelay,
		maxChanges: o.MaxChanges,
		tracer:     o.Tracer,
		clock:      o.Clock,
		batchSize:  in.Histogram(telemetry.MetricGroupCommitBatchSize),
		reqs:       make(chan *gcRequest),
		closing:    make(chan struct{}),
		done:       make(chan struct{}),
		received:   o.received,
	}
	go g.run()
	return g
}

// Apply commits batch as part of a group-commit transaction and returns its
// seq range, setting each applied change's Seq, like Store.Apply (a request
// whose every change was a skipped IfExists gets 0, 0).
func (g *GroupCommitter) Apply(ctx context.Context, batch []Change) (first, last int64, err error) {
	if len(batch) == 0 {
		return 0, 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	// Validate here, so a bad change fails only this caller.
	p, err := prepare(batch)
	if err != nil {
		return 0, 0, err
	}
	r := &gcRequest{ctx: ctx, changes: batch, prep: p, arrived: g.clock.Now(), result: make(chan gcResult, 1)}
	select {
	case g.reqs <- r:
	case <-g.closing:
		return 0, 0, ErrClosed
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
	select {
	case res := <-r.result:
		return res.first, res.last, res.err
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}

// Close flushes the requests already accepted, waits for them, and stops.
// Later Apply calls return ErrClosed.
func (g *GroupCommitter) Close() error {
	g.once.Do(func() { close(g.closing) })
	<-g.done
	return nil
}

// Flushes is the number of transactions committed or attempted so far.
func (g *GroupCommitter) Flushes() int64 { return g.flushes.Load() }

func (g *GroupCommitter) run() {
	defer close(g.done)
	var carry *gcRequest
	for {
		first := carry
		carry = nil
		if first == nil {
			select {
			case first = <-g.reqs:
				g.noteReceived()
			case <-g.closing:
				return
			}
		}
		batch := []*gcRequest{first}
		n := len(first.changes)
		deadline := first.arrived.Add(g.maxDelay)
		var timer clock.Timer
	collect:
		for n < g.maxChanges {
			// Take whoever is already waiting before looking at the clock,
			// so a backlog is never flushed one request at a time.
			var r *gcRequest
			select {
			case r = <-g.reqs:
			default:
				wait := g.clock.Until(deadline)
				if wait <= 0 {
					break collect
				}
				if timer == nil {
					timer = g.clock.NewTimer(wait)
				}
				select {
				case r = <-g.reqs:
				case <-timer.C():
					break collect
				case <-g.closing:
					break collect
				}
			}
			g.noteReceived()
			if n+len(r.changes) > g.maxChanges {
				carry = r
				break
			}
			batch = append(batch, r)
			n += len(r.changes)
		}
		if timer != nil {
			timer.Stop()
		}
		g.flush(batch)
	}
}

func (g *GroupCommitter) noteReceived() {
	if g.received != nil {
		g.received()
	}
}

// Request states during a flush.
const (
	gcActive = iota // in the next attempt
	gcHeld          // provisionally rejected for a conflict
	gcDone          // answered
)

// flush commits batch's requests in as few transactions as it takes to
// answer each one on its own merits.
func (g *GroupCommitter) flush(batch []*gcRequest) {
	state := make([]int, len(batch))
	held := make([]*ConflictError, len(batch))
	// answer finishes request i; requests held after it are re-evaluated,
	// since their conflicts may have depended on it.
	answer := func(i int, res gcResult) {
		state[i] = gcDone
		batch[i].result <- res
		for j := i + 1; j < len(batch); j++ {
			if state[j] == gcHeld {
				state[j] = gcActive
				held[j] = nil
			}
		}
	}
	// finish answers everyone left: held requests with their conflicts when
	// err is nil (everything before them is settled), all others with err.
	finish := func(err error) {
		for i := range batch {
			switch {
			case state[i] == gcHeld && err == nil:
				batch[i].result <- gcResult{err: held[i]}
			case state[i] != gcDone:
				batch[i].result <- gcResult{err: err}
			}
			state[i] = gcDone
		}
	}
	for {
		// Callers that have gone are dropped before every attempt.
		for i, r := range batch {
			if state[i] != gcDone && r.ctx.Err() != nil {
				answer(i, gcResult{err: r.ctx.Err()})
			}
		}
		var live []int
		for i := range batch {
			if state[i] == gcActive {
				live = append(live, i)
			}
		}
		if len(live) == 0 {
			finish(nil)
			return
		}
		reqs := make([]*gcRequest, len(live))
		for k, i := range live {
			reqs[k] = batch[i]
		}
		changes, err := g.commit(reqs)

		var ce *ConflictError
		var nf *IndexNotFoundError
		switch {
		case err == nil:
			// Each request's changes get their seqs back (a skipped IfExists
			// change keeps 0), and its range spans the ones applied.
			off := 0
			for _, i := range live {
				var res gcResult
				for j := range batch[i].changes {
					seq := changes[off+j].Seq
					batch[i].changes[j].Seq = seq
					if seq == 0 {
						continue
					}
					if res.first == 0 {
						res.first = seq
					}
					res.last = seq
				}
				off += len(batch[i].changes)
				state[i] = gcDone
				batch[i].result <- res
			}
			finish(nil)
			return
		case errors.As(err, &nf):
			// A missing index does not depend on the other requests: every
			// request naming one is answered for good, each naming only the
			// indexes its own changes refer to (not the whole batch's).
			for k, e := range splitPositions(reqs, nf.Positions, nil) {
				answer(live[k], gcResult{err: &IndexNotFoundError{Indexes: ownIndexes(reqs[k], e.Positions), Positions: e.Positions}})
			}
		case errors.As(err, &ce):
			// Only the first conflict is certain; hold it until everything
			// before it is settled and retry the rest.
			parts := splitPositions(reqs, ce.Positions, ce.Current)
			k := minKey(parts)
			state[live[k]] = gcHeld
			held[live[k]] = parts[k]
		default:
			finish(err)
			return
		}
	}
}

// commit runs one transaction for reqs and returns the combined batch, each applied
// change's Seq set.
func (g *GroupCommitter) commit(reqs []*gcRequest) ([]Change, error) {
	total := 0
	links := make([]trace.Link, 0, len(reqs))
	for _, r := range reqs {
		total += len(r.changes)
		if sc := trace.SpanContextFromContext(r.ctx); sc.IsValid() {
			links = append(links, trace.Link{SpanContext: sc})
		}
	}
	changes := make([]Change, 0, total)
	prep := &prepared{}
	for _, r := range reqs {
		changes = append(changes, r.changes...)
		prep.append(r.prep)
	}

	// The transaction runs until it finishes or every caller has gone.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var waiting atomic.Int64
	waiting.Store(int64(len(reqs)))
	stops := make([]func() bool, len(reqs))
	for i, r := range reqs {
		stops[i] = context.AfterFunc(r.ctx, func() {
			if waiting.Add(-1) == 0 {
				cancel()
			}
		})
	}
	defer func() {
		for _, stop := range stops {
			stop()
		}
	}()
	ctx, span := g.tracer.Start(ctx, "store.group_commit", trace.WithLinks(links...),
		trace.WithAttributes(attribute.Int("requests", len(reqs)), attribute.Int("changes", total)))
	defer span.End()

	g.flushes.Add(1)
	g.batchSize.Record(ctx, float64(total))
	var first, last int64
	var err error
	if pa, ok := g.st.(preparedApplier); ok {
		first, last, err = pa.applyPrepared(ctx, changes, prep)
	} else {
		first, last, err = g.st.Apply(ctx, changes)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return changes, err
	}
	// An Applier that does not report each change's Seq applied them all, in
	// order, as first..last.
	if last-first+1 == int64(len(changes)) && len(changes) > 0 && changes[0].Seq == 0 {
		for i := range changes {
			changes[i].Seq = first + int64(i)
		}
	}
	return changes, nil
}

// preparedApplier is the store's Apply without re-validation.
type preparedApplier interface {
	applyPrepared(ctx context.Context, batch []Change, p *prepared) (int64, int64, error)
}

// splitPositions maps positions (and their parallel values, if any) in a
// combined batch back to the requests that own them, relative to each
// request, keyed by request index.
func splitPositions(reqs []*gcRequest, positions []int, values []int64) map[int]*ConflictError {
	out := make(map[int]*ConflictError)
	start := 0
	ri := 0
	for k, pos := range positions {
		for pos >= start+len(reqs[ri].changes) {
			start += len(reqs[ri].changes)
			ri++
		}
		e := out[ri]
		if e == nil {
			e = &ConflictError{}
			out[ri] = e
		}
		e.Positions = append(e.Positions, pos-start)
		if values != nil {
			e.Current = append(e.Current, values[k])
		}
	}
	return out
}

// ownIndexes reads the index names at r's own changes at positions
// (relative to r, as splitPositions returns them) and returns them deduped
// in first-use order, so a request that named several missing indexes is
// told about only its own, not every missing index in the whole batch.
func ownIndexes(r *gcRequest, positions []int) []string {
	seen := make(map[string]bool, 1)
	var out []string
	for _, pos := range positions {
		name := r.changes[pos].Index
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	return out
}

func minKey(m map[int]*ConflictError) int {
	first := -1
	for k := range m {
		if first < 0 || k < first {
			first = k
		}
	}
	return first
}
