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

	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// Group commit defaults (plan Task 8): flush every 2 ms or at 1,000 changes.
const (
	DefaultGroupCommitDelay   = 2 * time.Millisecond
	DefaultGroupCommitChanges = 1000
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
}

// GroupCommitter coalesces concurrent Apply calls on a node into one
// transaction, the way Elasticsearch amortizes translog fsyncs (spec §8).
// Each caller gets its own contiguous seq range within the batch, in arrival
// order, and a failed transaction fails every caller in it. Requests whose
// IfSeq conditions fail are answered with their own *ConflictError and the
// rest of the batch is committed without them, so one request's conflict
// never fails another's.
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
	batchSize  metric.Float64Histogram

	reqs    chan *gcRequest
	closing chan struct{}
	done    chan struct{}
	once    sync.Once

	flushes atomic.Int64
}

type gcRequest struct {
	ctx     context.Context
	changes []Change
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
	in := telemetry.NewInstruments(o.Meter)
	g := &GroupCommitter{
		st:         st,
		maxDelay:   o.MaxDelay,
		maxChanges: o.MaxChanges,
		tracer:     o.Tracer,
		batchSize:  in.Histogram(telemetry.MetricGroupCommitBatchSize),
		reqs:       make(chan *gcRequest),
		closing:    make(chan struct{}),
		done:       make(chan struct{}),
	}
	go g.run()
	return g
}

// Apply commits batch as part of a group-commit transaction and returns its
// seq range, like Store.Apply.
func (g *GroupCommitter) Apply(ctx context.Context, batch []Change) (first, last int64, err error) {
	if len(batch) == 0 {
		return 0, 0, nil
	}
	if err := ctx.Err(); err != nil {
		return 0, 0, err
	}
	r := &gcRequest{ctx: ctx, changes: batch, arrived: time.Now(), result: make(chan gcResult, 1)}
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
			case <-g.closing:
				return
			}
		}
		batch := []*gcRequest{first}
		n := len(first.changes)
		deadline := first.arrived.Add(g.maxDelay)
		var timer *time.Timer
	collect:
		for n < g.maxChanges {
			// Take whoever is already waiting before looking at the clock,
			// so a backlog is never flushed one request at a time.
			var r *gcRequest
			select {
			case r = <-g.reqs:
			default:
				wait := time.Until(deadline)
				if wait <= 0 {
					break collect
				}
				if timer == nil {
					timer = time.NewTimer(wait)
				}
				select {
				case r = <-g.reqs:
				case <-timer.C:
					break collect
				case <-g.closing:
					break collect
				}
			}
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

// flush commits the live requests of batch in one transaction, retrying
// without any whose seq conditions fail.
func (g *GroupCommitter) flush(batch []*gcRequest) {
	live := batch[:0]
	for _, r := range batch {
		if err := r.ctx.Err(); err != nil {
			r.result <- gcResult{err: err}
			continue
		}
		live = append(live, r)
	}
	for len(live) > 0 {
		var rejected map[int]*ConflictError
		live, rejected = g.commit(live)
		if rejected == nil {
			return
		}
		next := live[:0]
		for i, r := range live {
			if ce, ok := rejected[i]; ok {
				r.result <- gcResult{err: ce}
				continue
			}
			next = append(next, r)
		}
		live = next
	}
}

// commit runs one transaction for reqs. On success or a plain failure it
// answers every request and returns (nil, nil). When seq conditions fail it
// answers nobody and returns reqs with each conflicting request's own
// *ConflictError by position.
func (g *GroupCommitter) commit(reqs []*gcRequest) ([]*gcRequest, map[int]*ConflictError) {
	total := 0
	links := make([]trace.Link, 0, len(reqs))
	for _, r := range reqs {
		total += len(r.changes)
		if sc := trace.SpanContextFromContext(r.ctx); sc.IsValid() {
			links = append(links, trace.Link{SpanContext: sc})
		}
	}
	changes := make([]Change, 0, total)
	for _, r := range reqs {
		changes = append(changes, r.changes...)
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
	first, _, err := g.st.Apply(ctx, changes)

	var ce *ConflictError
	if errors.As(err, &ce) {
		return reqs, splitConflict(reqs, ce)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		for _, r := range reqs {
			r.result <- gcResult{err: err}
		}
		return nil, nil
	}
	off := int64(0)
	for _, r := range reqs {
		n := int64(len(r.changes))
		r.result <- gcResult{first: first + off, last: first + off + n - 1}
		off += n
	}
	return nil, nil
}

// splitConflict maps a combined batch's conflicts back to the requests that
// own them, with positions relative to each request.
func splitConflict(reqs []*gcRequest, ce *ConflictError) map[int]*ConflictError {
	out := make(map[int]*ConflictError)
	start := 0
	ri := 0
	for k, pos := range ce.Positions {
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
		e.Current = append(e.Current, ce.Current[k])
	}
	return out
}
