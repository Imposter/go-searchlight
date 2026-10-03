// Package replica keeps one shard copy in step with the SQL changelog, which is the
// write-ahead log (spec sections 8 to 10): a [Tailer] reads the shard's changes in seq
// order, analyzes them, applies them to its [shard.Shard], and reports what is durable
// to the cluster registry. It is the copy's only writer: the node that commits a write
// wakes its own tailers ([Tailer.Wake]) rather than applying to the shard itself.
//
// # States
//
// A tailer is recovering, tailing or halted ([State]).
//
//   - Recovering: the copy is brought to a point the changelog can be replayed from.
//     A copy that has applied changes resumes from its seq; an empty copy, one whose
//     changelog was pruned past its seq (store.ErrPruned), one whose index was dropped
//     and recreated (its IndexUID changed), and one interrupted mid-rebuild are wiped
//     and rebuilt, from a [Fetcher] (a serving peer, Task 11) when there is one, else
//     from the store's ScanShard snapshot. A failed shard (shard.ErrFailed) is reopened
//     from its directory, and rebuilt if that fails.
//   - Tailing: HeadSeq, then ChangesAfter from the applied seq; each page is analyzed
//     and applied, and a page shorter than its limit proves no change of the shard up
//     to the head is missing, so the copy Advances there (seqs are global, so a shard's
//     own have gaps). Between polls it sleeps until woken by a local write, a Postgres
//     notification ([Hub]) or the poll interval.
//   - Halted: a change the store accepted but this copy cannot apply (a document the
//     mapping refuses, a query that does not parse, one the shard refuses) is a bug,
//     never skipped: the copy stops before it, is marked recovering in the registry,
//     and Run returns a *[HaltError].
//
// # Errors
//
// Transient failures (the database unreachable, a refresh failing) are retried with
// backoff and never halt; so is shard.ErrBackpressure, after a refresh drains the
// buffer. Losing the copy's lease (store.ErrLeaseLost), the index being dropped
// ([ErrIndexDropped]) and the shard being closed under the tailer stop Run.
package replica

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// ShardID names the shard a tailer follows.
type ShardID = store.ShardID

// Defaults.
const (
	DefaultPollInterval        = 500 * time.Millisecond
	DefaultWatchedPollInterval = 10 * time.Second
	DefaultBatchSize           = store.DefaultChangesLimit
	DefaultReportInterval      = time.Second
	DefaultCatalogInterval     = 10 * time.Second
	DefaultRetryBase           = 50 * time.Millisecond
	DefaultRetryCap            = 5 * time.Second
)

// Options configures a [Tailer]. The zero value is usable.
type Options struct {
	// Copy is the registry copy the tailer reports for: its CommittedSeq goes to
	// ReportApplied, it is marked recovering while the copy is rebuilt or halted and
	// serving once it has caught up. Nil means no registry (a single node, a test).
	Copy *store.Copy
	// PollInterval is how often the changelog is polled when nothing wakes the
	// tailer sooner (config changelog_poll_interval). 0 means DefaultPollInterval.
	PollInterval time.Duration
	// WatchedPollInterval is the poll interval while store notifications are
	// flowing (Postgres): the safety net under them. 0 means
	// DefaultWatchedPollInterval.
	WatchedPollInterval time.Duration
	// BatchSize is the changes read and applied at a time. 0 means
	// DefaultBatchSize.
	BatchSize int
	// ReportInterval is how often a moved CommittedSeq is reported. 0 means
	// DefaultReportInterval.
	ReportInterval time.Duration
	// CatalogInterval is how often the index's catalogue entry is re-read, to see
	// mapping additions other nodes made and an index dropped while no change
	// arrives. 0 means DefaultCatalogInterval.
	CatalogInterval time.Duration
	// RetryBase and RetryCap bound the backoff between retries of transient
	// failures. 0 means DefaultRetryBase and DefaultRetryCap.
	RetryBase, RetryCap time.Duration
	// Hub delivers store notifications. Nil means the tailer watches the store
	// itself when it is a store.Watcher; share one Hub across a node's tailers so
	// they share one connection.
	Hub *Hub
	// Fetcher, when set, is tried before ScanShard when the copy must be rebuilt.
	Fetcher Fetcher
	// OpenShard reopens the copy's directory: after a wipe, and after the shard
	// failed. Nil opens it with shard.Open and the first shard's options.
	OpenShard func(ctx context.Context, dir string) (*shard.Shard, error)
	// OnShard is called, from Run, each time the tailer replaces its shard with a
	// reopened or rebuilt one, so readers can move to it. [Tailer.Shard] returns
	// it too.
	OnShard func(*shard.Shard)
	// Logger, Tracer and Meter are the tailer's telemetry; nil means
	// slog.Default(), a no-op tracer and no metrics.
	Logger *slog.Logger
	Tracer trace.Tracer
	Meter  metric.Meter
}

func (o *Options) resolve() {
	if o.PollInterval <= 0 {
		o.PollInterval = DefaultPollInterval
	}
	if o.WatchedPollInterval <= 0 {
		o.WatchedPollInterval = DefaultWatchedPollInterval
	}
	if o.BatchSize <= 0 {
		o.BatchSize = DefaultBatchSize
	}
	if o.ReportInterval <= 0 {
		o.ReportInterval = DefaultReportInterval
	}
	if o.CatalogInterval <= 0 {
		o.CatalogInterval = DefaultCatalogInterval
	}
	if o.RetryBase <= 0 {
		o.RetryBase = DefaultRetryBase
	}
	if o.RetryCap <= 0 {
		o.RetryCap = DefaultRetryCap
	}
	o.RetryCap = max(o.RetryCap, o.RetryBase)
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Tracer == nil {
		o.Tracer = tracenoop.NewTracerProvider().Tracer(telemetry.ScopeName)
	}
}

// State is where a tailer is in its life.
type State int32

// The states.
const (
	// StateIdle: Run is not running.
	StateIdle State = iota
	// StateRecovering: the copy is being opened, reopened or rebuilt.
	StateRecovering
	// StateTailing: the copy applies the changelog.
	StateTailing
	// StateHalted: a change could not be applied; Run returned a *HaltError.
	StateHalted
)

func (s State) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StateRecovering:
		return "recovering"
	case StateTailing:
		return "tailing"
	case StateHalted:
		return "halted"
	}
	return fmt.Sprintf("State(%d)", int32(s))
}

// Errors.
var (
	// ErrIndexDropped is returned by Run when the index no longer exists.
	ErrIndexDropped = errors.New("replica: the index was dropped")
	// ErrRunning is returned by a Run while another Run of the tailer is running.
	ErrRunning = errors.New("replica: the tailer is already running")
	// ErrHalted matches every *HaltError.
	ErrHalted = errors.New("replica: shard copy halted")
)

// Tailer applies one shard's changelog to one shard copy. Run drives it; the other
// methods are safe for concurrent use.
type Tailer struct {
	st   store.Store
	id   ShardID
	opts Options
	log  *slog.Logger
	tr   trace.Tracer
	inst *instruments

	sh      atomic.Pointer[shard.Shard]
	applied atomic.Int64
	head    atomic.Int64
	lagSeq  atomic.Int64
	lagAge  atomic.Int64 // nanoseconds
	state   atomic.Int32
	running atomic.Bool
	wake    chan struct{}

	// Run's own state, touched by Run's goroutine only.
	shardOpts   shard.Options
	cat         catalog
	copy        *store.Copy
	reported    int64
	lastReport  time.Time
	lastCatalog time.Time
	retry       time.Duration
	warn        rateLimitedWarn
	lastSource  string // the source of the last rebuild
	recoveredAt int64  // the applied seq that rebuild ended at
	needRebuild string // a rebuild to (re)try before tailing, by reason
	needReopen  bool
}

// NewTailer returns a tailer that keeps sh, the copy of shard id, in step with st's
// changelog. Run starts it. The tailer may replace sh with a reopened or rebuilt
// shard (Options.OnShard); [Tailer.Shard] returns the current one.
func NewTailer(st store.Store, sh *shard.Shard, id ShardID, opts Options) *Tailer {
	opts.resolve()
	t := &Tailer{
		st:   st,
		id:   id,
		opts: opts,
		log: opts.Logger.With(slog.String("component", "replica"),
			slog.String(telemetry.KeyIndex, id.Index), slog.Int(telemetry.KeyShard, id.Shard)),
		tr:        opts.Tracer,
		wake:      make(chan struct{}, 1),
		shardOpts: sh.Options(),
		cat:       catalog{idx: st.Indexes(), name: id.Index},
		warn:      rateLimitedWarn{every: 30 * time.Second},
	}
	t.inst = newInstruments(opts.Meter, id, t.log)
	t.sh.Store(sh)
	t.applied.Store(sh.AppliedSeq())
	if opts.Copy != nil {
		c := *opts.Copy
		t.copy = &c
	}
	return t
}

// Shard returns the shard copy the tailer currently applies to. Once Run has
// returned, the caller owns it (and closes it).
func (t *Tailer) Shard() *shard.Shard { return t.sh.Load() }

// Applied returns the seq the copy has applied up to: every change of the shard at
// or below it is in the shard, searchable after its next refresh. It is in memory;
// CommittedSeq on the shard is what is durable.
func (t *Tailer) Applied() int64 { return t.applied.Load() }

// Lag returns how far the copy trailed the changelog at its last poll: in changes
// (the head seq minus the applied one) and in time (the age of the oldest change it
// had not applied; zero when caught up).
func (t *Tailer) Lag() (seq int64, age time.Duration) {
	return t.lagSeq.Load(), time.Duration(t.lagAge.Load())
}

// State returns the tailer's state.
func (t *Tailer) State() State { return State(t.state.Load()) }

// Wake asks the tailer to poll the changelog now: the node that committed a change of
// the shard calls it, and so do store notifications. It never blocks.
func (t *Tailer) Wake() {
	select {
	case t.wake <- struct{}{}:
	default:
	}
}

func (t *Tailer) setState(s State) { t.state.Store(int32(s)) }

// Run recovers the copy if it needs to, then applies the changelog until ctx ends
// (it returns nil), the copy's lease is lost (an error matching store.ErrLeaseLost),
// the index is dropped ([ErrIndexDropped]), the shard is closed under it
// (shard.ErrClosed), or a change cannot be applied (a *[HaltError]). Only one Run
// may run at a time.
func (t *Tailer) Run(ctx context.Context) (err error) {
	if !t.running.CompareAndSwap(false, true) {
		return ErrRunning
	}
	defer t.running.Store(false)
	defer func() {
		if t.State() != StateHalted {
			t.setState(StateIdle)
		}
	}()

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	hub := t.opts.Hub
	if hub == nil {
		if hub = NewHub(t.st, HubOptions{Logger: t.opts.Logger, Meter: t.opts.Meter, RetryBase: t.opts.RetryBase, RetryCap: t.opts.RetryCap}); hub != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = hub.Run(runCtx)
			}()
		}
	}
	if hub != nil {
		defer hub.subscribe(t)()
	}

	t.log.InfoContext(ctx, "tailer started", slog.Int64("seq", t.Applied()))
	err = t.loop(runCtx, hub)
	if ctx.Err() != nil && !errors.Is(err, ErrHalted) && !errors.Is(err, store.ErrLeaseLost) {
		t.finalReport(ctx)
		t.log.InfoContext(ctx, "tailer stopped", slog.Int64("seq", t.Applied()))
		return nil
	}
	if err != nil && !errors.Is(err, ErrHalted) {
		t.log.WarnContext(ctx, "tailer stopped", slog.Int64("seq", t.Applied()), slog.Any("error", err))
	}
	return err
}

// loop is Run's body.
func (t *Tailer) loop(ctx context.Context, hub *Hub) error {
	t.setState(StateRecovering)
	if err := t.start(ctx); err != nil {
		if stop := t.handle(ctx, err); stop != nil {
			return stop
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := t.recoverIfNeeded(ctx); err != nil {
			if stop := t.handle(ctx, err); stop != nil {
				return stop
			}
			continue
		}
		t.setState(StateTailing)
		caughtUp, err := t.step(ctx)
		if err == nil {
			err = t.housekeeping(ctx, caughtUp)
		}
		if err != nil {
			if stop := t.handle(ctx, err); stop != nil {
				return stop
			}
			continue
		}
		t.retry = 0
		if caughtUp {
			t.sleep(ctx, hub)
		}
	}
}

// handle classifies a failure: it returns the error Run must stop with, or nil after
// arranging the retry (a rebuild, a reopen, or a backoff).
func (t *Tailer) handle(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var rb *rebuildError
	var halt *HaltError
	switch {
	case errors.As(err, &halt):
		return t.halt(ctx, halt)
	case errors.As(err, &rb):
		again := t.needRebuild != ""
		t.needRebuild = rb.reason
		t.log.InfoContext(ctx, "shard copy must be rebuilt", slog.String("reason", rb.reason), slog.Any("error", rb.err))
		if again {
			t.backoff(ctx) // a rebuild asked for another: do not spin
		}
		return nil
	case errors.Is(err, shard.ErrFailed):
		t.needReopen = true
		t.log.WarnContext(ctx, "shard copy failed; reopening it", slog.Any("error", err))
		return nil
	case errors.Is(err, store.ErrLeaseLost), errors.Is(err, ErrIndexDropped), errors.Is(err, shard.ErrClosed),
		errors.Is(err, store.ErrClosed), errors.Is(err, store.ErrInvalid):
		return err
	}
	// Transient: the database, or a refresh.
	if suppressed, ok := t.warn.allow(); ok {
		t.log.WarnContext(ctx, "tailer retrying after a failure", slog.Any("error", err), slog.Int("suppressed", suppressed))
	}
	t.backoff(ctx)
	return nil
}

// backoff waits out the next retry delay, or until ctx ends.
func (t *Tailer) backoff(ctx context.Context) {
	if t.retry == 0 {
		t.retry = t.opts.RetryBase
	} else {
		t.retry = min(2*t.retry, t.opts.RetryCap)
	}
	sleepCtx(ctx, t.retry)
}

func sleepCtx(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// sleep waits for a wake-up, the poll interval, or the next report.
func (t *Tailer) sleep(ctx context.Context, hub *Hub) {
	d := t.opts.PollInterval
	if hub != nil && hub.Watching() {
		d = t.opts.WatchedPollInterval
	}
	if t.copy != nil && t.Shard().CommittedSeq() > t.reported {
		d = min(d, max(time.Millisecond, t.opts.ReportInterval-time.Since(t.lastReport)))
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-t.wake:
	case <-timer.C:
	}
}

// step polls once: it reads the head, then the shard's changes after the applied seq,
// applies them, and when the page proves nothing more is pending, advances to the
// head. caughtUp reports that.
func (t *Tailer) step(ctx context.Context) (caughtUp bool, err error) {
	sh := t.Shard()
	if err := sh.Err(); err != nil {
		return false, err
	}
	head, err := t.st.HeadSeq(ctx)
	if err != nil {
		return false, err
	}
	from := t.Applied()
	changes, err := t.st.ChangesAfter(ctx, t.id, from, t.opts.BatchSize)
	if errors.Is(err, store.ErrPruned) {
		return false, &rebuildError{reason: reasonPruned, err: err}
	}
	if err != nil {
		return false, err
	}
	if n := len(changes); n > 0 {
		head = max(head, changes[n-1].Seq)
		t.recordLag(ctx, head, from, time.Since(changes[0].At))
		if err := t.applyChanges(ctx, sh, changes); err != nil {
			return false, err
		}
	}
	if len(changes) >= t.opts.BatchSize {
		return false, nil // more to read now
	}
	// The head was read before the page: every change at or below it had
	// committed, and the page holds all of this shard's after from.
	if head > t.Applied() {
		if err := sh.Advance(head); err != nil {
			return false, err
		}
		t.applied.Store(head)
	}
	t.recordLag(ctx, head, t.Applied(), 0)
	return true, nil
}

// recordLag publishes the lag: seq behind head, and the age of the oldest change not
// applied (0 when caught up).
func (t *Tailer) recordLag(ctx context.Context, head, applied int64, age time.Duration) {
	t.head.Store(head)
	lag := max(0, head-applied)
	age = max(0, age)
	t.lagSeq.Store(lag)
	t.lagAge.Store(int64(age))
	t.inst.lagSeq.Record(ctx, float64(lag), t.inst.attrs)
	t.inst.lagTime.Record(ctx, age.Seconds(), t.inst.attrs)
}

// housekeeping reports the durable seq, promotes a caught-up copy to serving, and
// re-reads the index's catalogue entry now and then.
func (t *Tailer) housekeeping(ctx context.Context, caughtUp bool) error {
	if err := t.report(ctx, false); err != nil {
		return err
	}
	if caughtUp {
		if err := t.markServing(ctx); err != nil {
			return err
		}
	}
	if time.Since(t.lastCatalog) >= t.opts.CatalogInterval {
		if err := t.refreshCatalog(ctx); err != nil {
			return err
		}
	}
	return nil
}

// report sends CommittedSeq to the registry when it moved, at most every
// ReportInterval unless force.
func (t *Tailer) report(ctx context.Context, force bool) error {
	if t.copy == nil {
		return nil
	}
	seq := t.Shard().CommittedSeq()
	if seq <= t.reported || (!force && time.Since(t.lastReport) < t.opts.ReportInterval) {
		return nil
	}
	if err := t.st.Registry().ReportApplied(ctx, *t.copy, seq); err != nil {
		return fmt.Errorf("report applied seq %d: %w", seq, err)
	}
	t.reported, t.lastReport = seq, time.Now()
	return nil
}

// finalReport makes a last report when Run stops, best effort.
func (t *Tailer) finalReport(ctx context.Context) {
	if t.copy == nil {
		return
	}
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := t.report(rctx, true); err != nil {
		t.log.DebugContext(ctx, "final applied-seq report failed", slog.Any("error", err))
	}
}

// setCopyState moves the registry copy to state, when there is one and it is not
// there already.
func (t *Tailer) setCopyState(ctx context.Context, state store.CopyState) error {
	if t.copy == nil || t.copy.State == state {
		return nil
	}
	if err := t.st.Registry().SetCopyState(ctx, *t.copy, state); err != nil {
		return fmt.Errorf("mark the copy %s: %w", state, err)
	}
	t.copy.State = state
	t.log.InfoContext(ctx, "shard copy state changed", slog.String("state", string(state)))
	return nil
}

// markServing promotes a recovering copy that has caught up. A retiring copy stays
// retiring: that is the node's shutdown, not the tailer's to undo.
func (t *Tailer) markServing(ctx context.Context) error {
	if t.copy == nil || t.copy.State != store.CopyRecovering {
		return nil
	}
	return t.setCopyState(ctx, store.CopyServing)
}

// rateLimitedWarn lets a recurring failure be logged at most once per every.
type rateLimitedWarn struct {
	every      time.Duration
	last       time.Time
	suppressed int
}

func (w *rateLimitedWarn) allow() (suppressed int, ok bool) {
	if now := time.Now(); now.Sub(w.last) >= w.every {
		suppressed = w.suppressed
		w.last, w.suppressed = now, 0
		return suppressed, true
	}
	w.suppressed++
	return 0, false
}
