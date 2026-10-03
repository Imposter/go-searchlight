// Package node is the single-node coordinator: the [api.Coordinator] that serves every
// shard of every index from copies on this node, over the SQL store (the write-ahead
// log and system of record) and one changelog tailer per shard copy.
//
// # Writes
//
// A write is analyzed and validated here, its new dynamic fields are added to the
// stored mapping first (the store's mapping is the one arbiter of a field's type, so
// every copy analyzes the document alike), then its changes are committed through the
// store's group committer: the write is acknowledged with its seq once the SQL
// transaction commits. The node then wakes the written shards' tailers, which apply
// the changes to the shard copies; the node never writes a shard itself.
//
// # Read-your-writes
//
// A write answers with the seq it committed. refresh=wait_for waits until the written
// copies have a searchable generation covering that seq (shard.WaitRefreshed);
// refresh=true waits until they have applied it, then refreshes them at once. Any read
// takes wait_for_seq=N and waits the same way on every copy it reads, after waking
// their tailers, so an idle copy advances to N without a change of its own. Every wait
// is bounded by the request's deadline.
//
// # Searches
//
// A search holds one generation of each shard from the query to the fetch: with one
// shard it runs once with bodies; with several it runs the query phase on each
// (NoBodies), reduces, then fetches the bodies of the winning hits alone from the
// generations that found them.
package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/percolate"
	"github.com/Imposter/go-searchlight/internal/replica"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// DefaultMaxApplyLag is how many seqs a written shard copy may trail what the node
// committed to it before writes to it are refused with a 429.
const DefaultMaxApplyLag = 100_000

// Options configures a [Single].
type Options struct {
	// Store is the SQL store, migrated. The node does not close it.
	Store store.Store
	// Config is the node's configuration: data_dir, refresh_interval,
	// seq_persist_interval, merge_threads, merge_budget, search_threads, max_doc_bytes,
	// node_id, advertise_address and max_lag are used.
	Config config.Config
	// NewTailer makes each shard copy's tailer. Nil means the replica tailer
	// (ReplicaTailers), sharing one replica Hub the node runs when the store
	// pushes notifications (Postgres).
	NewTailer NewTailerFunc
	// Background are node-wide loops the tailers share (the replica Hub's Run, which
	// delivers Postgres notifications), run from NewSingle until Close. An error one
	// returns before Close is logged.
	Background []func(ctx context.Context) error
	// Version is reported in the node list.
	Version string
	// ShardOptions, when set, adjusts each shard's options before it opens (tests:
	// small buffers, no merges).
	ShardOptions func(*shard.Options)
	// MaxApplyLag overrides DefaultMaxApplyLag.
	MaxApplyLag int64
	// GroupCommit tunes the group committer.
	GroupCommit store.GroupCommitOptions
	// Logger, Tracer and Meter are the node's telemetry; nil means slog.Default() and
	// the OpenTelemetry globals.
	Logger *slog.Logger
	Tracer trace.Tracer
	Meter  metric.Meter
}

// Single is the single-node coordinator.
type Single struct {
	st      store.Store
	records store.RecordReader
	cfg     config.Config
	opts    Options
	gc      *store.GroupCommitter
	log     *slog.Logger
	tr      trace.Tracer
	meter   metric.Meter
	budget  *shard.MergeBudget
	cache   *shard.FilterCache
	maxLag  int64

	// head is the newest seq committed or seen applied.
	head atomic.Int64

	// dbOK is when the database last answered (Unix nanoseconds); dbDown is set
	// while it does not. Reads served meanwhile are marked stale (spec section 10).
	dbOK   atomic.Int64
	dbDown atomic.Bool

	mu      sync.RWMutex
	indexes map[string]*index
	// reserved are names being created or dropped: no other create takes them.
	reserved map[string]bool
	closed   bool

	// bgCancel stops the Background loops; bg waits for them.
	bgCancel context.CancelFunc
	bg       sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

var _ api.Coordinator = (*Single)(nil)

// index is one open index: its catalogue entry and its shard copies.
type index struct {
	name string
	// catalog serializes changes to the catalogue entry (mapping and settings),
	// honouring deadlines; fields queues writes' new dynamic fields for it.
	catalog catalogLock
	fields  fieldsBatch
	meta    atomic.Pointer[indexState]
	// copies are the shard copies, by shard number.
	copies []*copyState
	dir    string
	// refresh is the resolved refresh interval in nanoseconds (<= 0: disabled);
	// refreshWake tells the refresher it changed.
	refresh     atomic.Int64
	refreshWake chan struct{}
	dropped     atomic.Bool
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// indexState is an index's catalogue entry, parsed.
type indexState struct {
	meta     store.IndexMeta
	mapping  *schema.Mapping
	settings api.IndexSettings
}

// copyState is one shard copy and its tailer.
type copyState struct {
	id     store.ShardID
	tailer Tailer
	perc   *percolate.Percolator
	// halted is why the tailer's Run stopped on its own, nil while it runs.
	halted atomic.Pointer[error]
	// written is the newest seq the node committed to this shard; querySeq the
	// newest that wrote one of its saved queries.
	written  atomic.Int64
	querySeq atomic.Int64
	// started is set once the copy has finished its startup recovery: at once for
	// a tailer that cannot say (no StateReporter), else once it is first seen
	// tailing.
	started atomic.Bool
}

// startedUp reports whether the copy has finished its startup recovery.
func (c *copyState) startedUp() bool {
	if c.started.Load() {
		return true
	}
	if sr, ok := c.tailer.(StateReporter); !ok || sr.StateName() == StateTailing {
		c.started.Store(true)
		return true
	}
	return false
}

// pingInterval is how often the database is checked: often enough to notice it is
// gone well within max_lag.
func (n *Single) pingInterval() time.Duration {
	return min(time.Second, max(50*time.Millisecond, n.cfg.MaxLag/4))
}

// pingLoop checks the database until ctx ends.
func (n *Single) pingLoop(ctx context.Context) {
	t := time.NewTicker(n.pingInterval())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		// A database slower than max_lag/2 to answer a ping is as good as gone
		// for reads that must not trail by more than max_lag.
		pctx, cancel := context.WithTimeout(ctx, n.pingTimeout())
		err := n.st.Ping(pctx)
		cancel()
		if ctx.Err() != nil {
			return
		}
		n.noteDB(err)
	}
}

// pingTimeout bounds one ping: max_lag/2.
func (n *Single) pingTimeout() time.Duration {
	if n.cfg.MaxLag > 0 {
		return n.cfg.MaxLag / 2
	}
	return time.Second
}

// noteDB records whether the database just answered.
func (n *Single) noteDB(err error) {
	if err == nil {
		n.dbOK.Store(time.Now().UnixNano())
		n.dbDown.Store(false)
		return
	}
	if store.IsTransient(err) {
		n.dbDown.Store(true)
	}
}

// stale reports whether reads should be marked stale: the database cannot be
// reached, so the copies may trail writes made through other nodes.
func (n *Single) stale() bool { return n.dbDown.Load() }

// indexStale reports whether a read of idx should be marked stale: the database is
// unreachable, or a copy trails the changelog by more than max_lag or cannot poll it.
func (n *Single) indexStale(idx *index) bool {
	if n.stale() {
		return true
	}
	for _, c := range idx.copies {
		if c.trailing(n.cfg.MaxLag) || c.rebuilding() {
			return true
		}
	}
	return false
}

// rebuilding reports whether the copy is being rebuilt aside: it serves, stale.
func (c *copyState) rebuilding() bool {
	sr, ok := c.tailer.(StateReporter)
	return ok && sr.StateName() == StateRebuilding
}

// trailing reports whether the copy trails the changelog by more than maxLag (by
// its tailer's measure), or cannot poll it now.
func (c *copyState) trailing(maxLag time.Duration) bool {
	if pr, ok := c.tailer.(PollReporter); ok && pr.PollFailing() {
		return true
	}
	if lr, ok := c.tailer.(LagReporter); ok && maxLag > 0 {
		if _, age := lr.Lag(); age > maxLag {
			return true
		}
	}
	return false
}

// notServing is why the copy cannot serve a read or take a write now (a 503), or nil:
// it has halted, or it is recovering (being opened, reopened or rebuilt: the shard a
// tailer exposes then may be empty or partly loaded, so it is never read).
func (c *copyState) notServing() error {
	if h := c.halted.Load(); h != nil {
		return api.Unavailable(*h, "shard %d of index %q has halted", c.id.Shard, c.id.Index)
	}
	if hr, ok := c.tailer.(HaltReporter); ok {
		if err := hr.HaltErr(); err != nil {
			return api.Unavailable(err, "shard %d of index %q has halted", c.id.Shard, c.id.Index)
		}
	}
	if sr, ok := c.tailer.(StateReporter); ok {
		switch sr.StateName() {
		case StateTailing, StateRebuilding:
			// Rebuilding aside: the current copy serves (stale) until its
			// replacement is swapped in.
		case StateHalted:
			return api.Unavailable(errors.New(sr.StateName()), "shard %d of index %q has halted", c.id.Shard, c.id.Index)
		default:
			return api.Unavailable(errors.New(sr.StateName()), "shard %d of index %q is recovering", c.id.Shard, c.id.Index)
		}
	}
	return nil
}

// LagReporter is implemented by a tailer that measures how far its copy trails the
// changelog (replica.Tailer.Lag): in changes of its own shard, and in time.
type LagReporter interface {
	Lag() (seq int64, age time.Duration)
}

// backlog is how far the copy trails its writes: the tailer's own measure when it has
// one, else the seqs between what the node committed to the shard and what the copy
// has applied (an over-estimate, since other shards' changes share the seq space).
func (c *copyState) backlog() int64 {
	if lr, ok := c.tailer.(LagReporter); ok {
		seq, _ := lr.Lag()
		return seq
	}
	sh := c.shard()
	if sh == nil {
		return 0
	}
	return max(0, c.written.Load()-sh.AppliedSeq())
}

func (c *copyState) shard() *shard.Shard { return c.tailer.Shard() }

// NewSingle opens every index in the store's catalogue, with a tailer per shard copy,
// and starts them. Copies of indexes no longer in the catalogue are removed from
// data_dir.
func NewSingle(ctx context.Context, o Options) (*Single, error) {
	if o.Store == nil {
		return nil, errors.New("node: Options.Store is required")
	}
	rr, ok := o.Store.(store.RecordReader)
	if !ok {
		return nil, fmt.Errorf("node: the store %T cannot read records", o.Store)
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Tracer == nil {
		o.Tracer = otel.Tracer(telemetry.ScopeName)
	}
	if o.Meter == nil {
		o.Meter = otel.Meter(telemetry.ScopeName)
	}
	if o.MaxApplyLag <= 0 {
		o.MaxApplyLag = DefaultMaxApplyLag
	}
	if o.GroupCommit.Tracer == nil {
		o.GroupCommit.Tracer = o.Tracer
	}
	if o.GroupCommit.Meter == nil {
		o.GroupCommit.Meter = o.Meter
	}
	if o.NewTailer == nil {
		// One Hub per node, shared by every tailer; nil (no notifications, as on
		// SQLite and MySQL) means the tailers poll and are woken by writes.
		hub := replica.NewHub(o.Store, replica.HubOptions{Logger: o.Logger, Meter: o.Meter})
		if hub != nil {
			o.Background = append(o.Background, hub.Run)
		}
		o.NewTailer = ReplicaTailers(o.Config, hub, o.Logger, o.Tracer, o.Meter)
	}
	n := &Single{
		st:       o.Store,
		records:  rr,
		cfg:      o.Config,
		opts:     o,
		log:      o.Logger,
		tr:       o.Tracer,
		meter:    o.Meter,
		budget:   shard.NewMergeBudget(max(1, o.Config.MergeThreads), o.Config.MergeBudget),
		cache:    shard.NewFilterCache(shard.DefaultFilterCacheBytes, o.Meter),
		maxLag:   o.MaxApplyLag,
		indexes:  map[string]*index{},
		reserved: map[string]bool{},
	}
	if o.Config.SearchThreads > 0 {
		search.SetThreads(o.Config.SearchThreads)
	}
	n.gc = store.NewGroupCommitter(o.Store, o.GroupCommit) //nolint:contextcheck // the committer outlives this call; each Apply carries its caller's context
	bgCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	n.bgCancel = cancel
	for _, run := range o.Background {
		n.bg.Go(func() {
			if err := run(bgCtx); err != nil && bgCtx.Err() == nil {
				n.log.ErrorContext(bgCtx, "a node background loop stopped", slog.Any("error", err))
			}
		})
	}
	metas, err := o.Store.Indexes().List(ctx)
	if err != nil {
		_ = n.Close(ctx)
		return nil, fmt.Errorf("node: list indexes: %w", err)
	}
	n.noteDB(nil)
	// The wait_for_seq bound and every copy's saved-query wait start at the
	// store's head, so nothing committed before a restart is missed.
	if head, _, err := o.Store.HeadSeq(ctx); err == nil {
		n.noteHead(head)
	} else {
		_ = n.Close(ctx)
		return nil, fmt.Errorf("node: read the head seq: %w", err)
	}
	n.bg.Go(func() { n.pingLoop(bgCtx) })
	keep := map[string]bool{}
	for _, m := range metas {
		idx, err := n.openIndex(ctx, m)
		if err != nil {
			_ = n.Close(ctx)
			return nil, err
		}
		n.indexes[m.Name] = idx
		keep[m.UID] = true
	}
	n.collectGarbage(ctx, keep)
	n.log.InfoContext(ctx, "single node ready", slog.Int("indexes", len(metas)))
	return n, nil
}

// indexesDir holds a directory per index incarnation (its uid), with one per shard.
func (n *Single) indexesDir() string { return filepath.Join(n.cfg.DataDir, "indexes") }

// collectGarbage removes the copies of incarnations no longer in the catalogue.
func (n *Single) collectGarbage(ctx context.Context, keep map[string]bool) {
	entries, err := os.ReadDir(n.indexesDir())
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() || keep[e.Name()] {
			continue
		}
		path := filepath.Join(n.indexesDir(), e.Name())
		if err := os.RemoveAll(path); err != nil {
			n.log.WarnContext(ctx, "removing a dropped index's copies failed", slog.String("dir", path), slog.Any("error", err))
		}
	}
}

// parseState reads a catalogue entry.
func parseState(m store.IndexMeta) (*indexState, error) {
	st := &indexState{meta: m, mapping: &schema.Mapping{Fields: map[string]schema.FieldType{}}, settings: api.IndexSettings{Shards: api.DefaultShards}}
	if len(m.Mapping) > 0 {
		if err := json.Unmarshal(m.Mapping, st.mapping); err != nil {
			return nil, fmt.Errorf("node: index %q: mapping: %w", m.Name, err)
		}
	}
	if len(m.Settings) > 0 {
		if err := json.Unmarshal(m.Settings, &st.settings); err != nil {
			return nil, fmt.Errorf("node: index %q: settings: %w", m.Name, err)
		}
	}
	return st, nil
}

// resolvedRefresh is the refresh interval an index's settings give on this node.
func (n *Single) resolvedRefresh(s api.IndexSettings) time.Duration {
	switch {
	case s.RefreshInterval == api.DisabledRefresh:
		return 0
	case s.RefreshInterval > 0:
		return s.RefreshInterval
	case n.cfg.RefreshInterval > 0:
		return n.cfg.RefreshInterval
	}
	return shard.DefaultRefreshInterval
}

// openIndex opens an index's shard copies and starts their tailers and its refresher.
func (n *Single) openIndex(ctx context.Context, m store.IndexMeta) (*index, error) {
	state, err := parseState(m)
	if err != nil {
		return nil, err
	}
	idx := &index{name: m.Name, dir: filepath.Join(n.indexesDir(), m.UID), refreshWake: make(chan struct{}, 1), catalog: make(catalogLock, 1)}
	idx.meta.Store(state)
	idx.refresh.Store(int64(n.resolvedRefresh(state.settings)))
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	idx.cancel = cancel
	for s := range state.settings.Shards {
		id := store.ShardID{Index: m.Name, Shard: s}
		opts := shard.Options{
			Index:              m.Name,
			Shard:              s,
			RefreshInterval:    -1, // the node's refresher drives refreshes, so the interval can change
			SeqPersistInterval: n.cfg.SeqPersistInterval,
			MergeBudget:        n.budget,
			FilterCache:        n.cache,
			QueryIndex:         percolate.Index{},
			Logger:             n.log,
			Tracer:             n.tr,
			Meter:              n.meter,
		}
		if n.opts.ShardOptions != nil {
			n.opts.ShardOptions(&opts)
		}
		// A copy rebuilt aside lives in a subdirectory its root names: open the
		// current one (and collect what a crash or a swap left behind).
		sh, err := replica.OpenCopy(ctx, filepath.Join(idx.dir, strconv.Itoa(s)), state.mapping, opts)
		if err != nil {
			_ = n.stopIndex(ctx, idx)
			return nil, fmt.Errorf("node: open %s: %w", id, err)
		}
		n.noteHead(sh.AppliedSeq())
		c := &copyState{
			id:     id,
			tailer: n.opts.NewTailer(n.st, sh, id, TailerEnv{Head: n.head.Load}),
			perc: percolate.New(percolate.Options{
				Index: m.Name, Shard: s, Threads: n.cfg.SearchThreads,
				Logger: n.log, Tracer: n.tr, Meter: n.meter,
			}),
		}
		// A percolation waits until the copy has every change up to the head as
		// of its opening (its saved queries among them), then only for queries
		// saved since.
		c.querySeq.Store(n.head.Load())
		idx.copies = append(idx.copies, c)
		idx.wg.Add(1)
		go n.runTailer(runCtx, idx, c)
	}
	idx.wg.Add(1)
	go n.refresher(runCtx, idx)
	return idx, nil
}

// runTailer runs a copy's tailer until the index stops, recording why it halted when
// it stops on its own.
func (n *Single) runTailer(ctx context.Context, idx *index, c *copyState) {
	defer idx.wg.Done()
	defer func() {
		if p := recover(); p != nil {
			err := fmt.Errorf("tailer panic: %v", p)
			c.halted.Store(&err)
			n.log.ErrorContext(ctx, "shard copy tailer panicked", slog.String(telemetry.KeyIndex, c.id.Index), slog.Int(telemetry.KeyShard, c.id.Shard), slog.Any("panic", p))
		}
	}()
	err := c.tailer.Run(ctx)
	if ctx.Err() != nil || idx.dropped.Load() {
		return
	}
	if err == nil {
		err = errors.New("the tailer stopped")
	}
	c.halted.Store(&err)
	n.log.ErrorContext(ctx, "shard copy halted", slog.String(telemetry.KeyIndex, c.id.Index), slog.Int(telemetry.KeyShard, c.id.Shard), slog.Any("error", err))
}

// refresher refreshes an index's copies every refresh interval (which settings may
// change at any time).
func (n *Single) refresher(ctx context.Context, idx *index) {
	defer idx.wg.Done()
	warn := time.Time{}
	for {
		d := time.Duration(idx.refresh.Load())
		var tick <-chan time.Time
		var timer *time.Timer
		if d > 0 {
			timer = time.NewTimer(d)
			tick = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case <-idx.refreshWake:
			if timer != nil {
				timer.Stop()
			}
			continue
		case <-tick:
		}
		for _, c := range idx.copies {
			sh := c.shard()
			if sh == nil || sh.Err() != nil {
				continue
			}
			if err := sh.Refresh(ctx); err != nil && ctx.Err() == nil && !errors.Is(err, shard.ErrClosed) && time.Since(warn) > time.Minute {
				warn = time.Now()
				n.log.WarnContext(ctx, "refresh failed", slog.String(telemetry.KeyIndex, c.id.Index), slog.Int(telemetry.KeyShard, c.id.Shard), slog.Any("error", err))
			}
		}
	}
}

// stopIndex stops an index's tailers and refresher and closes its copies.
func (n *Single) stopIndex(ctx context.Context, idx *index) error {
	if idx.cancel != nil {
		idx.cancel()
	}
	idx.wg.Wait()
	var errs []error
	for _, c := range idx.copies {
		if sh := c.shard(); sh != nil {
			if err := sh.Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("close %s: %w", c.id, err))
			}
		}
	}
	return errors.Join(errs...)
}

// noteHead raises the node's head to seq.
func (n *Single) noteHead(seq int64) {
	for {
		cur := n.head.Load()
		if seq <= cur || n.head.CompareAndSwap(cur, seq) {
			return
		}
	}
}

// lookup returns an open index, or a 404.
func (n *Single) lookup(name string) (*index, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.closed {
		return nil, api.Unavailable(store.ErrClosed, "the node is shutting down")
	}
	idx := n.indexes[name]
	if idx == nil {
		return nil, indexNotFound(name)
	}
	return idx, nil
}

func indexNotFound(name string) *api.Error {
	return api.NotFound(api.CodeIndexNotFound, "index %q does not exist", name)
}

// Close stops every index's tailers, closes its shard copies (a final refresh makes
// their manifests cover every change they applied) and stops the group committer. It
// does not close the store. It is idempotent.
func (n *Single) Close(ctx context.Context) error {
	n.closeOnce.Do(func() {
		n.mu.Lock()
		n.closed = true
		list := slices.Collect(maps.Values(n.indexes))
		n.mu.Unlock()
		var errs []error
		if n.gc != nil {
			errs = append(errs, n.gc.Close())
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		for _, idx := range list {
			wg.Go(func() {
				if err := n.stopIndex(ctx, idx); err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
				}
			})
		}
		wg.Wait()
		if n.bgCancel != nil {
			n.bgCancel()
		}
		n.bg.Wait()
		n.closeErr = errors.Join(errs...)
		n.log.InfoContext(ctx, "single node closed")
	})
	return n.closeErr
}

// storeError maps a store error to the API's: a missing index is a 404, a conflict a
// 409, a bad request a 400, a deadline or cancellation itself, and anything else (the
// database unreachable) a 503.
func storeError(err error, index string) error {
	var ae *api.Error
	switch {
	case err == nil:
		return nil
	case errors.As(err, &ae):
		return ae
	case errors.Is(err, store.ErrNotFound):
		return indexNotFound(index)
	case errors.Is(err, store.ErrConflict), errors.Is(err, store.ErrExists):
		return err
	case errors.Is(err, store.ErrInvalid):
		return api.InvalidAt("body", "%s", err.Error())
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return err
	}
	return api.Unavailable(err, "the database is unavailable")
}
