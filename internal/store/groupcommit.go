package store

import (
	"context"
	"errors"
	"slices"
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

// Group commit defaults: flush every 2 ms, at 4,096 changes or at 16 MiB of stored
// payload, so a few concurrent _bulk requests of a thousand documents share one
// transaction (and its fsync) while one of large documents does not hold the next
// requests behind a long write.
const (
	DefaultGroupCommitDelay   = 2 * time.Millisecond
	DefaultGroupCommitChanges = 4096
	DefaultGroupCommitBytes   = 16 << 20
)

// DefaultWriteTransactions is how many write transactions a node runs at once by
// default. Each holds a connection, so a cluster's nodes together need that many on
// top of their reads: three nodes at 16 each fit Postgres's default max_connections of
// 100 with room for their reads, where Phase 0's best, 32, would not.
const DefaultWriteTransactions = 16

// GroupCommitOptions tune a GroupCommitter. Zero values take the defaults.
type GroupCommitOptions struct {
	// MaxDelay is the longest a request waits for company before its batch
	// is flushed, counted from its arrival.
	MaxDelay time.Duration
	// MaxChanges flushes a batch as soon as it holds this many changes. A
	// single request larger than this is committed on its own.
	MaxChanges int
	// MaxBytes flushes a batch as soon as its stored payloads (compressed where
	// they are) reach this size. A single request larger than this is committed
	// on its own.
	MaxBytes int
	// MaxTransactions bounds the transactions the committer runs at once, so its
	// share of the store's connections. Above 1, every shard has a lane of its own:
	// a request is split by shard, and each shard's part commits in a transaction
	// of that shard's alone, beside other shards'. At 1 there is one lane, and a
	// request commits whole, its shards together. The default is DefaultWriteTransactions
	// where the Applier is a Store, at most half its write connections (1 on SQLite,
	// whose one writer gains nothing from the split, and would pay a commit per shard),
	// and 1 otherwise.
	MaxTransactions int
	// Tracer and Meter default to the global providers'.
	Tracer trace.Tracer
	Meter  metric.Meter
	// Clock times the batching window; nil means clock.Real.
	Clock clock.Clock

	// received, when set (tests only), runs as each request (each part of a split
	// one) joins its lane, before Apply waits for it.
	received func()
}

// GroupCommitter coalesces concurrent Apply calls on a node into one transaction per
// shard, the way Elasticsearch amortizes translog fsyncs (spec §8). Each shard has a
// lane, which collects the requests for its shard (its parts of requests that span
// several) for up to MaxDelay and commits them together. Lanes commit at the same
// time, up to MaxTransactions transactions; a lane waits out its batching window
// before it takes a transaction, so a lane collecting holds no connection. Each
// caller's changes on a shard take contiguous seqs of it, in arrival order, and a
// failed transaction fails every caller in it.
//
// One caller's bad request never fails another's. Each request is validated
// on its own before it joins a batch, so an invalid change is reported to
// its caller alone (as a *ChangeError positioned in that request). A request
// naming a missing index, or a shard its index does not have, gets its own error
// and the rest is retried without it. When IfSeq conditions fail, only the first
// conflicting request is rejected and everything after it is re-evaluated in the
// next attempt, since its conditions may have failed only because of the rejected
// request; a rejection is final only once every request before it has committed
// or itself been answered.
//
// A request split over several shards commits shard by shard: when some shards'
// transactions fail, Apply returns a *PartialError naming them, and the changes of
// every other shard have committed.
//
// A caller whose context ends before its batch is flushed is dropped from it.
// A caller whose context ends while its batch is committing gets the
// context's error and its changes may or may not have committed; the
// transaction is cancelled only when every caller in it has given up.
type GroupCommitter struct {
	st         Applier
	maxDelay   time.Duration
	maxChanges int
	maxBytes   int
	byShard    bool
	slots      chan struct{}
	tracer     trace.Tracer
	clock      clock.Clock
	batchSize  metric.Float64Histogram
	received   func()

	mu      sync.Mutex
	lanes   map[ShardID]*gcLane
	closed  bool
	closing chan struct{}
	running sync.WaitGroup

	flushes atomic.Int64
}

// gcLane is the requests waiting for one shard's transaction (every shard's, with one
// lane). A goroutine drains it while it holds any.
type gcLane struct {
	key    ShardID
	reqs   []*gcRequest
	active bool
	wake   chan struct{} // buffered, 1: a request joined
}

type gcRequest struct {
	ctx     context.Context
	changes []Change
	prep    *prepared
	arrived time.Time
	result  chan error // buffered, 1
}

// writeConns is implemented by a store, which says how many write connections it
// has.
type writeConns interface {
	writeConns() int
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
	if o.MaxBytes <= 0 {
		o.MaxBytes = DefaultGroupCommitBytes
	}
	if o.MaxTransactions <= 0 {
		o.MaxTransactions = 1
		if w, ok := st.(writeConns); ok {
			o.MaxTransactions = max(min(DefaultWriteTransactions, w.writeConns()/2), 1)
		}
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
	return &GroupCommitter{
		st:         st,
		maxDelay:   o.MaxDelay,
		maxChanges: o.MaxChanges,
		maxBytes:   o.MaxBytes,
		byShard:    o.MaxTransactions > 1,
		slots:      make(chan struct{}, o.MaxTransactions),
		tracer:     o.Tracer,
		clock:      o.Clock,
		batchSize:  in.Histogram(telemetry.MetricGroupCommitBatchSize),
		lanes:      map[ShardID]*gcLane{},
		closing:    make(chan struct{}),
		received:   o.received,
	}
}

// gcPart is the changes of one lane of a request, at their positions in it (at is
// nil when the part is the whole request). The changes are a copy of the request's,
// which the committer owns: it sets their seqs, possibly after the caller has given
// up and returned, so it never touches the caller's own slice.
type gcPart struct {
	key     ShardID
	at      []int
	changes []Change
	prep    *prepared
	req     *gcRequest
}

// Apply commits batch through the committer and sets each applied change's Seq,
// like Store.Apply; it returns a *PartialError when the request was split by shard
// and some shards' changes did not commit. The batch is validated and compressed in
// the caller's goroutine, so a bad change fails only this caller.
func (g *GroupCommitter) Apply(ctx context.Context, batch []Change) error {
	if len(batch) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var p *prepared
	var err error
	if pa, ok := g.st.(preparedApplier); ok {
		p, err = pa.prepare(batch)
	} else {
		p, err = prepare(batch, false)
	}
	if err != nil {
		return err
	}
	for i := range batch {
		batch[i].Seq, batch[i].Noop = 0, false
	}
	parts := g.split(batch, p)
	arrived := g.clock.Now()
	for _, part := range parts {
		part.req = &gcRequest{ctx: ctx, changes: part.changes, prep: part.prep, arrived: arrived, result: make(chan error, 1)}
	}
	if err := g.submit(parts); err != nil { //nolint:contextcheck // a lane outlives its requests; each transaction carries its callers' contexts
		return err
	}
	errs := make([]error, len(parts))
	for i, part := range parts {
		select {
		case errs[i] = <-part.req.result:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if len(parts) == 1 {
		if errs[0] == nil {
			parts[0].copyBack(batch)
		}
		return errs[0]
	}
	var pe *PartialError
	for i, part := range parts {
		if errs[i] == nil {
			part.copyBack(batch)
			continue
		}
		if pe == nil {
			pe = &PartialError{}
		}
		pe.Failed = append(pe.Failed, ShardFailure{Shard: part.key, Positions: part.at, Err: atPositions(errs[i], part.at)})
	}
	if pe != nil {
		return pe
	}
	return nil
}

// copyBack sets the request's changes' seqs and noops from the part's, once the part
// committed. The caller runs it, after the part's result came back.
func (part *gcPart) copyBack(batch []Change) {
	for k := range part.changes {
		at := k
		if part.at != nil {
			at = part.at[k]
		}
		batch[at].Seq, batch[at].Noop = part.changes[k].Seq, part.changes[k].Noop
	}
}

// split cuts a request into its lanes' parts: one, the whole request, unless the
// committer has a lane per shard and the request names several shards.
func (g *GroupCommitter) split(batch []Change, p *prepared) []*gcPart {
	if !g.byShard {
		return []*gcPart{{changes: slices.Clone(batch), prep: p}}
	}
	first := batch[0].ShardID()
	one := true
	for i := range batch {
		if batch[i].ShardID() != first {
			one = false
			break
		}
	}
	if one {
		return []*gcPart{{key: first, changes: slices.Clone(batch), prep: p}}
	}
	byShard := map[ShardID]*gcPart{}
	var parts []*gcPart
	for i := range batch {
		id := batch[i].ShardID()
		part := byShard[id]
		if part == nil {
			part = &gcPart{key: id, prep: &prepared{conditional: p.conditional}}
			byShard[id] = part
			parts = append(parts, part)
		}
		part.at = append(part.at, i)
		part.changes = append(part.changes, batch[i])
		part.prep.payload = append(part.prep.payload, p.payload[i])
		part.prep.query = append(part.prep.query, p.query[i])
		part.prep.bytes += p.payload[i].size()
	}
	return parts
}

// atPositions maps the positions an error names in a part to positions in its
// request.
func atPositions(err error, at []int) error {
	var ce *ConflictError
	var nf *IndexNotFoundError
	var che *ChangeError
	switch {
	case errors.As(err, &ce):
		out := &ConflictError{Current: ce.Current}
		for _, pos := range ce.Positions {
			out.Positions = append(out.Positions, at[pos])
		}
		return out
	case errors.As(err, &nf):
		out := &IndexNotFoundError{Indexes: nf.Indexes}
		for _, pos := range nf.Positions {
			out.Positions = append(out.Positions, at[pos])
		}
		return out
	case errors.As(err, &che):
		return &ChangeError{Position: at[che.Position], Err: che.Err}
	}
	return err
}

// submit queues every part in its lane, or none once the committer is closed.
func (g *GroupCommitter) submit(parts []*gcPart) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return ErrClosed
	}
	for _, part := range parts {
		l := g.lanes[part.key]
		if l == nil {
			l = &gcLane{key: part.key, wake: make(chan struct{}, 1)}
			g.lanes[part.key] = l
		}
		l.reqs = append(l.reqs, part.req)
		if g.received != nil {
			g.received()
		}
		if !l.active {
			l.active = true
			g.running.Add(1)
			go g.drain(l)
			continue
		}
		select {
		case l.wake <- struct{}{}:
		default:
		}
	}
	return nil
}

// Close flushes the requests already accepted, waits for them, and stops.
// Later Apply calls return ErrClosed.
func (g *GroupCommitter) Close() error {
	g.mu.Lock()
	if !g.closed {
		g.closed = true
		close(g.closing)
	}
	g.mu.Unlock()
	g.running.Wait()
	return nil
}

// Flushes is the number of transactions committed or attempted so far.
func (g *GroupCommitter) Flushes() int64 { return g.flushes.Load() }

// drain commits l's requests until it has none: it waits out the first request's
// batching window (or until the lane holds a full batch, or the committer closes),
// then takes a transaction slot and every request then waiting, up to a batch. A
// backlog's window has passed already, so it is never flushed one request at a time.
func (g *GroupCommitter) drain(l *gcLane) {
	defer g.running.Done()
	for {
		g.mu.Lock()
		if len(l.reqs) == 0 {
			l.active = false
			delete(g.lanes, l.key)
			g.mu.Unlock()
			return
		}
		deadline := l.reqs[0].arrived.Add(g.maxDelay)
		g.mu.Unlock()
		g.window(l, deadline)
		g.slots <- struct{}{}
		g.mu.Lock()
		batch := g.take(l)
		g.mu.Unlock()
		g.flush(batch)
		<-g.slots
	}
}

// window waits until deadline, until l holds a full batch, or until the committer
// closes.
func (g *GroupCommitter) window(l *gcLane, deadline time.Time) {
	var timer clock.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for !g.full(l) {
		wait := g.clock.Until(deadline)
		if wait <= 0 {
			return
		}
		if timer == nil {
			timer = g.clock.NewTimer(wait)
		}
		select {
		case <-l.wake:
		case <-timer.C():
			return
		case <-g.closing:
			return
		}
	}
}

// full reports whether l's requests reach MaxChanges or MaxBytes.
func (g *GroupCommitter) full(l *gcLane) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	n, size := 0, 0
	for _, r := range l.reqs {
		n += len(r.changes)
		size += r.prep.bytes
		if n >= g.maxChanges || size >= g.maxBytes {
			return true
		}
	}
	return false
}

// take removes l's first requests, as many as one batch holds: it closes before the
// request that would pass MaxChanges or MaxBytes, and as soon as it reaches either,
// and holds at least one. g.mu is held.
func (g *GroupCommitter) take(l *gcLane) []*gcRequest {
	n, size, k := 0, 0, 0
	for _, r := range l.reqs {
		if k > 0 && (n >= g.maxChanges || size >= g.maxBytes || n+len(r.changes) > g.maxChanges || size+r.prep.bytes > g.maxBytes) {
			break
		}
		n += len(r.changes)
		size += r.prep.bytes
		k++
	}
	batch := l.reqs[:k:k]
	l.reqs = l.reqs[k:]
	return batch
}

// Request states during a flush.
const (
	gcActive = iota // in the next attempt
	gcHeld          // provisionally rejected for a conflict
	gcDone          // answered
)

// flush commits batch's requests in as few transactions as it takes to
// answer each one on its own merits. Callers that have gone are dropped before every
// attempt.
//
// After a commit, each request's changes get their seqs back (a skipped IfExists
// change keeps 0). A missing index, or a shard its index does not have, does not
// depend on the other requests: every request naming one is answered for good, each
// naming only the indexes and positions its own changes refer to (not the whole
// batch's). Only the first conflict is certain: that request is held until everything
// before it is settled, and the rest retried. Answering a request makes the ones held
// after it active again, since their conflicts may have depended on it; once nothing
// is left to retry, held requests get their conflicts (everything before them is
// settled), and on any other error every request left gets that error.
func (g *GroupCommitter) flush(batch []*gcRequest) {
	state := make([]int, len(batch))
	held := make([]*ConflictError, len(batch))
	answer := func(i int, err error) {
		state[i] = gcDone
		batch[i].result <- err
		for j := i + 1; j < len(batch); j++ {
			if state[j] == gcHeld {
				state[j] = gcActive
				held[j] = nil
			}
		}
	}
	finish := func(err error) {
		for i := range batch {
			switch {
			case state[i] == gcHeld && err == nil:
				batch[i].result <- held[i]
			case state[i] != gcDone:
				batch[i].result <- err
			}
			state[i] = gcDone
		}
	}
	for {
		for i, r := range batch {
			if state[i] != gcDone && r.ctx.Err() != nil {
				answer(i, r.ctx.Err())
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
		var che *ChangeError
		switch {
		case err == nil:
			off := 0
			for _, i := range live {
				for j := range batch[i].changes {
					batch[i].changes[j].Seq, batch[i].changes[j].Noop = changes[off+j].Seq, changes[off+j].Noop
				}
				off += len(batch[i].changes)
				state[i] = gcDone
				batch[i].result <- nil
			}
			finish(nil)
			return
		case errors.As(err, &nf):
			for k, e := range splitPositions(reqs, nf.Positions, nil) {
				answer(live[k], &IndexNotFoundError{Indexes: ownIndexes(reqs[k], e.Positions), Positions: e.Positions})
			}
		case errors.As(err, &che):
			for k, e := range splitPositions(reqs, []int{che.Position}, nil) {
				answer(live[k], &ChangeError{Position: e.Positions[0], Err: che.Err})
			}
		case errors.As(err, &ce):
			parts := splitPositions(reqs, ce.Positions, ce.Current)
			k := minKey(parts)
			for j := live[k] + 1; j < len(batch); j++ {
				if state[j] == gcHeld {
					state[j] = gcActive
					held[j] = nil
				}
			}
			state[live[k]] = gcHeld
			held[live[k]] = parts[k]
		default:
			finish(err)
			return
		}
	}
}

// commit runs one transaction for reqs and returns the combined batch, each applied
// change's Seq set. The transaction runs until it finishes or every caller has gone.
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
	var err error
	if pa, ok := g.st.(preparedApplier); ok {
		err = pa.applyPrepared(ctx, changes, prep)
	} else {
		err = g.st.Apply(ctx, changes)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	return changes, err
}

// preparedApplier is the store's Apply split in two: prepare validates and encodes a
// batch, and applyPrepared commits it without doing that again.
type preparedApplier interface {
	prepare(batch []Change) (*prepared, error)
	applyPrepared(ctx context.Context, batch []Change, p *prepared) error
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
