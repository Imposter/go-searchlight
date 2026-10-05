// Package replica keeps one shard copy in step with the SQL changelog, which is the
// write-ahead log (spec sections 8 to 10): a [Tailer] reads the shard's changes in seq
// order, analyzes them, applies them to its [shard.Shard], and reports what is durable
// to the cluster registry. It is the copy's only writer: the node that commits a write
// wakes its own tailers ([Tailer.Wake]) rather than applying to the shard itself, and
// only one tailer may run over a shard directory.
//
// # States
//
// A tailer is recovering, tailing or halted ([State]).
//
//   - Recovering: the copy is brought to a point the changelog can be replayed from.
//     A copy that has applied changes resumes from its seq; an empty copy, one whose
//     changelog was pruned past its seq (store.ErrPruned), one whose index was dropped
//     and recreated (its IndexUID changed), one a mapping change makes analyze
//     differently, and one interrupted mid-rebuild are wiped and rebuilt, from a
//     [Fetcher] (a serving peer, Task 11) when there is one, else from the store's
//     ScanShard snapshot. A failed shard (shard.ErrFailed) is reopened from its
//     directory, and rebuilt if that fails.
//   - Tailing: HeadSeq, then ChangesAfter from the applied seq; each page is analyzed
//     and applied, and a page shorter than its limit proves no change of the shard up
//     to the head is missing, so the copy Advances there (seqs are global, so a shard's
//     own have gaps). Between polls it sleeps until woken by a local write, a Postgres
//     notification ([Hub]) or the poll interval.
//   - Halted: a change the store accepted but this copy cannot apply (a body that does
//     not analyze, a query that does not parse, one the shard refuses) is a bug, never
//     skipped: the copy stops before it and is marked recovering in the registry.
//     Run keeps going: after a backoff (HaltRetryBase to HaltRetryCap) it tails
//     again, and when it halts at the same change again it rebuilds the copy (a
//     snapshot holds only current rows, so a change since superseded is not replayed;
//     one still current halts the load, and the copy backs off again).
//
// # Mappings
//
// An index's mapping changes through the changelog: the store logs a mapping change to
// every shard (store.KindMapping), and every change carries the mapping version it was
// written under. The copy adopts a mapping exactly at its seq (a shard.Remap), so it
// analyzes every document under the mapping it was written under, whatever the
// catalogue says now. A document field the mapping does not map is indexed for
// presence only, whatever the mapping's dynamic mode: analysis is a function of the
// mapped fields and the body alone, so a tailed and a rebuilt copy agree. When a
// mapping change makes a field mapped (or retyped, or unmapped) that a live document
// of the copy holds, the copy is rebuilt at that seq, so its documents are analyzed
// under the new mapping as a copy rebuilt later would be.
//
// # Errors
//
// Transient failures (the database unreachable, a refresh failing) are retried with
// backoff and never halt; so is shard.ErrBackpressure, after a refresh drains the
// buffer. Losing the copy's lease (store.ErrLeaseLost, seen at the next registry
// write), the index being dropped ([ErrIndexDropped]) and the shard being closed under
// the tailer stop Run.
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

	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// ShardID names the shard a tailer follows.
type ShardID = store.ShardID

// Defaults.
const (
	DefaultPollInterval    = 500 * time.Millisecond
	DefaultMaxLag          = 2 * time.Second
	DefaultBatchSize       = store.DefaultChangesLimit
	DefaultReportInterval  = time.Second
	DefaultCatalogInterval = 10 * time.Second
	DefaultRetryBase       = 50 * time.Millisecond
	DefaultRetryCap        = 5 * time.Second
	DefaultRebuildRetryCap = 2 * time.Minute
	DefaultHaltRetryBase   = 30 * time.Second
	DefaultHaltRetryCap    = 10 * time.Minute
	DefaultRemapDebounce   = 2 * time.Second
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
	// MaxLag is how far a copy may trail and still serve (config max_lag). 0 means
	// DefaultMaxLag.
	MaxLag time.Duration
	// WatchedPollInterval is the poll interval while the Hub's notifications flow:
	// the safety net under them. 0 means MaxLag/2.
	WatchedPollInterval time.Duration
	// BatchSize is the changes read and applied at a time. 0 means
	// DefaultBatchSize.
	BatchSize int
	// ReportInterval is how often a moved CommittedSeq is reported. 0 means
	// DefaultReportInterval.
	ReportInterval time.Duration
	// CatalogInterval is how often the index's catalogue entry is re-read, to see an
	// index dropped (or recreated, or its shard removed) while no change arrives. An
	// index missing for a whole interval stops Run. 0 means DefaultCatalogInterval.
	CatalogInterval time.Duration
	// RetryBase and RetryCap bound the backoff between retries of transient
	// failures; RebuildRetryCap bounds it for a rebuild that keeps failing. 0 means
	// the defaults.
	RetryBase, RetryCap, RebuildRetryCap time.Duration
	// HaltRetryBase and HaltRetryCap bound the backoff of a halted copy's retries.
	// 0 means DefaultHaltRetryBase and DefaultHaltRetryCap.
	HaltRetryBase, HaltRetryCap time.Duration
	// Clock runs the tailer's polls, backoffs and debounces and ages its lag. Nil
	// means clock.Real.
	Clock clock.Clock
	// RemapDebounce is how long a copy that a mapping change must rebuild waits,
	// applying nothing, for more mapping changes to come, so that a burst of them
	// costs one rebuild (at the latest seq) rather than one each. The wait restarts
	// while new mapping versions keep arriving, up to five times over. 0 means
	// DefaultRemapDebounce; negative means no wait.
	RemapDebounce time.Duration
	// Hub delivers store notifications (Postgres): share one per node, run by the
	// node. Nil means the tailer only polls, woken early by Wake.
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

	// hooks are test seams; nil outside tests.
	hooks *testHooks
}

// testHooks are test seams.
type testHooks struct {
	// afterDiscard runs in a wipe once the manifest is gone, before the files are.
	afterDiscard func(ctx context.Context, dir string)
	// beforeSwap runs in an aside rebuild once the new copy in dir has caught up,
	// before it is made current; an error abandons the rebuild there (a crash).
	beforeSwap func(ctx context.Context, dir string) error
	// inPlace rebuilds every copy in place, as one that cannot be built aside is.
	inPlace bool
}

func (o *Options) resolve() {
	if o.PollInterval <= 0 {
		o.PollInterval = DefaultPollInterval
	}
	if o.MaxLag <= 0 {
		o.MaxLag = DefaultMaxLag
	}
	if o.WatchedPollInterval <= 0 {
		o.WatchedPollInterval = max(o.MaxLag/2, time.Millisecond)
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
	if o.RebuildRetryCap <= 0 {
		o.RebuildRetryCap = DefaultRebuildRetryCap
	}
	o.RebuildRetryCap = max(o.RebuildRetryCap, o.RetryCap)
	if o.HaltRetryBase <= 0 {
		o.HaltRetryBase = DefaultHaltRetryBase
	}
	if o.HaltRetryCap <= 0 {
		o.HaltRetryCap = DefaultHaltRetryCap
	}
	o.HaltRetryCap = max(o.HaltRetryCap, o.HaltRetryBase)
	if o.RemapDebounce == 0 {
		o.RemapDebounce = DefaultRemapDebounce
	}
	if o.Clock == nil {
		o.Clock = clock.Real{}
	}
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
	// StateHalted: a change could not be applied; Run backs off and retries.
	StateHalted
	// StateRebuilding: the copy is being rebuilt aside (a mapping change, a pruned
	// changelog, a halt): the old copy keeps serving, stale, until the new one has
	// caught up and replaces it.
	StateRebuilding
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
	case StateRebuilding:
		return "rebuilding"
	}
	return fmt.Sprintf("State(%d)", int32(s))
}

// Errors.
var (
	// ErrIndexDropped is returned by Run when the index (or the tailer's shard of
	// it) no longer exists.
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
	state   atomic.Int32
	running atomic.Bool
	halt    atomic.Pointer[HaltError]
	wake    chan struct{}
	lag     lagState

	// Run's own state, touched by Run's goroutine only.
	shardOpts   shard.Options
	cat         catalog
	copy        *store.Copy
	reported    int64
	lastReport  time.Time
	lastCatalog time.Time
	retry       time.Duration
	rebuildWait time.Duration
	haltWait    time.Duration
	warn        rateLimitedWarn
	lastSource  string // the source of the last rebuild
	recoveredAt int64  // the applied seq that rebuild ended at
	needRebuild string // a rebuild to (re)try before tailing, by reason
	needReopen  bool
	// minMappingVersion is the mapping version a fetched copy must have: the
	// remap that asked for the rebuild.
	minMappingVersion int64
	// guardedSeq is the change the halt guard last rebuilt the copy for.
	guardedSeq int64
	// gate is the halt a snapshot load stopped at: the copy is half-loaded and
	// can only be rebuilt, which waits until the halted row is superseded.
	gate *HaltError
	// scanFor and scanFrom are where the search for a change superseding a halted
	// one got to.
	scanFor  *HaltError
	scanFrom int64
	// remapSince and remapVersion are when a remap's rebuild started waiting, and
	// the newest mapping version seen since.
	remapSince   time.Time
	remapVersion int64
}

// lagState is the lag as last observed, behind a mutex: the age keeps growing from
// the observation while nothing new is learned (the database down, the copy halted).
type lagState struct {
	mu     sync.Mutex
	seq    int64
	behind bool
	age    time.Duration // by the database clock, at obs
	obs    time.Time     // local, monotonic
	// failing is set while polls (or the recoveries that stand in for them) fail;
	// lastOK is when the last poll succeeded, by the local monotonic clock (the
	// database's cannot be read then).
	failing bool
	lastOK  time.Time
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
		cat:       catalog{idx: st.Indexes(), name: id.Index, clock: opts.Clock},
		warn:      rateLimitedWarn{clock: opts.Clock, every: 30 * time.Second},
	}
	t.inst = newInstruments(opts.Meter, id, t.log)
	t.lag.lastOK = t.opts.Clock.Now()
	t.sh.Store(sh)
	t.applied.Store(sh.AppliedSeq())
	if opts.Copy != nil {
		c := *opts.Copy
		t.copy = &c
	}
	return t
}

// Shard returns the shard copy the tailer currently applies to. If Run stops in the
// middle of a rebuild, it may be one the tailer abandoned (closed, its directory being
// replaced): reopen the directory. Once Run has returned, the caller owns it.
func (t *Tailer) Shard() *shard.Shard { return t.sh.Load() }

// Applied returns the seq the copy has applied up to: every change of the shard at
// or below it is in the shard, searchable after its next refresh. It is in memory;
// CommittedSeq on the shard is what is durable.
func (t *Tailer) Applied() int64 { return t.applied.Load() }

// Lag returns how far the copy trails the changelog: in changes (the head seq minus
// the applied one, at the last poll that succeeded) and in time (the age, by the
// database clock, of the oldest change it has not applied; zero when caught up). The
// age keeps growing while the copy is stuck: halted, or unable to reach the
// database. While polls fail the copy cannot know what it misses, so the age is at
// least the time since the last poll that succeeded (other nodes may be writing),
// and the seq lag is the last known one ([Tailer.PollFailing] says so).
func (t *Tailer) Lag() (seq int64, age time.Duration) {
	t.lag.mu.Lock()
	defer t.lag.mu.Unlock()
	if t.lag.behind {
		age = t.lag.age + t.opts.Clock.Since(t.lag.obs)
	}
	if t.lag.failing {
		age = max(age, t.opts.Clock.Since(t.lag.lastOK))
	}
	return t.lag.seq, age
}

// PollFailing reports whether the copy's polls of the changelog are failing now.
func (t *Tailer) PollFailing() bool {
	t.lag.mu.Lock()
	defer t.lag.mu.Unlock()
	return t.lag.failing
}

// pollFailed records a failed poll (or recovery step); pollOK a successful one.
func (t *Tailer) pollFailed() {
	t.lag.mu.Lock()
	defer t.lag.mu.Unlock()
	t.lag.failing = true
}

func (t *Tailer) pollOK() {
	t.lag.mu.Lock()
	defer t.lag.mu.Unlock()
	t.lag.failing, t.lag.lastOK = false, t.opts.Clock.Now()
}

// observeLag records a poll's view: head and applied seqs, and the age of the oldest
// unapplied change (behind) by the database clock.
func (t *Tailer) observeLag(head, applied int64, behind bool, age time.Duration) {
	t.lag.mu.Lock()
	defer t.lag.mu.Unlock()
	t.lag.seq = max(0, head-applied)
	t.lag.behind = behind
	t.lag.age = max(0, age)
	t.lag.obs = t.opts.Clock.Now()
}

// State returns the tailer's state.
func (t *Tailer) State() State { return State(t.state.Load()) }

// Halt returns the change the copy is halted at, or nil when it is not halted.
func (t *Tailer) Halt() *HaltError { return t.halt.Load() }

func (t *Tailer) halted() bool { return t.halt.Load() != nil }

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
// the index or the shard is dropped ([ErrIndexDropped]), the shard is closed under
// it (shard.ErrClosed), or the store is (store.ErrClosed). A halted copy does not
// stop Run: it is retried with backoff ([Tailer.Halt] says where it is stuck). Only
// one Run may run at a time.
func (t *Tailer) Run(ctx context.Context) (err error) {
	if !t.running.CompareAndSwap(false, true) {
		return ErrRunning
	}
	defer t.running.Store(false)
	defer t.setState(StateIdle)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	hub := t.opts.Hub
	if hub != nil {
		defer hub.subscribe(t)()
	}
	defer t.inst.observe(t)()
	t.pollOK() // the lag is judged from now

	t.log.InfoContext(ctx, "tailer started", slog.Int64("seq", t.Applied()))
	err = t.loop(runCtx, hub)
	if ctx.Err() != nil && !errors.Is(err, store.ErrLeaseLost) {
		t.finalReport(ctx)
		t.log.InfoContext(ctx, "tailer stopped", slog.Int64("seq", t.Applied()))
		return nil
	}
	if err != nil {
		t.log.WarnContext(ctx, "tailer stopped", slog.Int64("seq", t.Applied()), slog.Any("error", err))
	}
	return err
}

// loop is Run's body.
func (t *Tailer) loop(ctx context.Context, hub *Hub) error {
	t.setState(StateRecovering)
	// Nothing is known until the catalogue has been read: retry until the start
	// decides, never treating an unread catalogue as a changed incarnation.
	for {
		err := t.start(ctx)
		if err == nil {
			break
		}
		if stop := t.handle(ctx, err); stop != nil {
			return stop
		}
		if t.needRebuild != "" || t.needReopen {
			break
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
		if !t.halted() {
			t.setState(StateTailing)
		}
		caughtUp, err := t.step(ctx)
		if err == nil {
			t.clearHalt()
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
		return t.onHalt(ctx, halt)
	case errors.As(err, &rb):
		again := t.needRebuild != ""
		t.needRebuild = rb.reason
		t.log.InfoContext(ctx, "shard copy must be rebuilt", slog.String("reason", rb.reason), slog.Any("error", rb.err))
		if again {
			t.backoffRebuild(ctx) // a rebuild asked for another: do not spin
		}
		return nil
	case errors.Is(err, shard.ErrFailed):
		t.needReopen = true
		t.log.WarnContext(ctx, "shard copy failed; reopening it", slog.Any("error", err))
		return nil
	case errors.Is(err, ErrIndexDropped):
		// A drop followed at once by a create is a new incarnation, not the end:
		// only an index the catalogue lacks at every read for a whole
		// CatalogInterval stops the copy.
		// The catalogue is read again at every retry (RetryBase to RetryCap
		// apart), so any moment it holds the index ends the grace.
		if !t.cat.missingSince.IsZero() && t.opts.Clock.Since(t.cat.missingSince) >= t.opts.CatalogInterval {
			return err
		}
		t.log.DebugContext(ctx, "index missing; checking the catalogue again", slog.Any("error", err))
		t.lastCatalog = time.Time{}
		t.backoff(ctx)
		return nil
	case errors.Is(err, store.ErrLeaseLost), errors.Is(err, shard.ErrClosed),
		errors.Is(err, store.ErrClosed), errors.Is(err, store.ErrInvalid):
		return err
	}
	// Transient: the database, or a refresh.
	t.pollFailed()
	if suppressed, ok := t.warn.allow(); ok {
		t.log.WarnContext(ctx, "tailer retrying after a failure", slog.Any("error", err), slog.Int("suppressed", suppressed))
	}
	if t.needRebuild != "" {
		t.backoffRebuild(ctx)
	} else {
		t.backoff(ctx)
	}
	return nil
}

// backoff waits out the next retry delay, or until ctx ends.
func (t *Tailer) backoff(ctx context.Context) {
	t.retry = nextBackoff(t.retry, t.opts.RetryBase, t.opts.RetryCap)
	t.pause(ctx, t.retry)
}

// backoffRebuild waits out the next delay between rebuild attempts: longer than
// tailing retries, since each one wipes and reloads the copy.
func (t *Tailer) backoffRebuild(ctx context.Context) {
	t.rebuildWait = nextBackoff(t.rebuildWait, t.opts.RetryBase, t.opts.RebuildRetryCap)
	t.pause(ctx, t.rebuildWait)
}

func nextBackoff(cur, base, ceiling time.Duration) time.Duration {
	if cur == 0 {
		return base
	}
	return min(2*cur, ceiling)
}

// pause waits for d to pass or ctx to end.
func (t *Tailer) pause(ctx context.Context, d time.Duration) {
	_ = t.opts.Clock.Sleep(ctx, d)
}

// sleep waits for a wake-up, the poll interval, or the next report.
func (t *Tailer) sleep(ctx context.Context, hub *Hub) {
	d := t.opts.PollInterval
	if hub != nil && hub.Watching() {
		d = t.opts.WatchedPollInterval
	}
	if t.copy != nil && t.Shard().CommittedSeq() > t.reported {
		d = min(d, max(time.Millisecond, t.opts.ReportInterval-t.opts.Clock.Since(t.lastReport)))
	}
	timer := t.opts.Clock.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-t.wake:
	case <-timer.C():
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
	head, dbNow, err := t.st.HeadSeq(ctx)
	if err != nil {
		t.pollFailed()
		return false, err
	}
	from := t.Applied()
	changes, err := t.st.ChangesAfter(ctx, t.id, from, t.opts.BatchSize)
	if errors.Is(err, store.ErrPruned) {
		t.pollOK()
		return false, &rebuildError{reason: reasonPruned, err: err}
	}
	if err != nil {
		t.pollFailed()
		return false, err
	}
	t.pollOK()
	if n := len(changes); n > 0 {
		head = max(head, changes[n-1].Seq)
		t.observeLag(head, from, true, dbNow.Sub(changes[0].At))
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
	t.observeLag(head, t.Applied(), false, 0)
	return true, nil
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
	if t.opts.Clock.Since(t.lastCatalog) >= t.opts.CatalogInterval {
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
	if seq <= t.reported || (!force && t.opts.Clock.Since(t.lastReport) < t.opts.ReportInterval) {
		return nil
	}
	if err := t.st.Registry().ReportApplied(ctx, *t.copy, seq); err != nil {
		return fmt.Errorf("report applied seq %d: %w", seq, err)
	}
	t.reported, t.lastReport = seq, t.opts.Clock.Now()
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
	if t.copy == nil || t.copy.State != store.CopyRecovering || t.halted() {
		return nil
	}
	return t.setCopyState(ctx, store.CopyServing)
}

// rateLimitedWarn lets a recurring failure be logged at most once per every.
type rateLimitedWarn struct {
	clock      clock.Clock
	every      time.Duration
	last       time.Time
	suppressed int
}

func (w *rateLimitedWarn) allow() (suppressed int, ok bool) {
	if now := w.clock.Now(); now.Sub(w.last) >= w.every {
		suppressed = w.suppressed
		w.last, w.suppressed = now, 0
		return suppressed, true
	}
	w.suppressed++
	return 0, false
}
