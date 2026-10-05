// Package node is the coordinator engine: the [api.Coordinator] that serves every shard
// of every index over the SQL store (the write-ahead log and system of record) and one
// changelog tailer per shard copy it hosts.
//
// On its own (a single node) it hosts every shard's one copy. With Options.Cluster set
// (package cluster) it is one node of a cluster: it hosts the copies the cluster gives
// it ([Single.HostCopy]), keeps its catalogue in step with the store
// ([Single.SyncCatalog]), and reads the shards it does not host, or whose copy here is
// not current, from a serving copy elsewhere ([Cluster.Remote]). Every read and write
// takes the same path either way, so a cluster of one behaves as a single node.
//
// # Writes
//
// A write is analyzed and validated here, its new dynamic fields are added to the
// stored mapping first (the store's mapping is the one arbiter of a field's type, so
// every copy analyzes the document alike), then its changes are committed through the
// store's group committer: the write is acknowledged with its seq once the SQL
// transaction commits. The node then wakes the written shards' tailers here, and asks
// the cluster to push hints to the peers holding copies; the node never writes a shard
// itself.
//
// # Read-your-writes
//
// A write answers with the seq it committed. refresh=wait_for waits until the written
// copies have a searchable generation covering that seq (shard.WaitRefreshed);
// refresh=true waits until they have applied it, then refreshes them at once. On a
// cluster node that is every serving copy of the written shards, here and elsewhere.
// Any read takes wait_for_seq=N and waits the same way on every copy it reads, after
// waking their tailers, so an idle copy advances to N without a change of its own.
// Every wait is bounded by the request's deadline.
//
// # Searches
//
// A search holds one generation of each shard's copy from the query to the fetch:
// with one shard it runs once with bodies; with several it runs the query phase on
// each (NoBodies), reduces, then fetches the bodies of the winning hits alone from the
// copies that found them (a peer pins its generation between the two). A copy lost
// between the phases makes the search run again.
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
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/percolate"
	"github.com/Imposter/go-searchlight/internal/replica"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/segment"
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
	// Config is the node's configuration: data_dir, refresh_interval, flush_interval,
	// merge_threads, merge_budget, search_threads, max_doc_bytes, node_id,
	// advertise_address and max_lag are used.
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
	// Cluster, when set, makes the node one of a cluster (package cluster sets it): it
	// hosts only the shard copies the cluster gives it (HostCopy), reads the other
	// shards from peers, and keeps its index catalogue in step with the store
	// (SyncCatalog). Nil: a single node, hosting every shard's one copy.
	Cluster Cluster
	// Clock runs the node's timers (refreshes, database pings, waits and their
	// bounds) and those of its shards, tailers, replica Hub and group committer, and
	// judges readiness and staleness against max_lag. Nil means clock.Real. On a
	// clock.Fake a write waits out the group commit window until the fake is advanced.
	Clock clock.Clock
	// Logger, Tracer and Meter are the node's telemetry; nil means slog.Default() and
	// the OpenTelemetry globals.
	Logger *slog.Logger
	Tracer trace.Tracer
	Meter  metric.Meter
}

// Single is the single-node coordinator, and the engine of a cluster node: with
// Options.Cluster set it hosts the copies the cluster gives it and reads the rest from
// peers.
type Single struct {
	st      store.Store
	records store.RecordReader
	cfg     config.Config
	opts    Options
	cl      Cluster // nil: a single node
	gc      *store.GroupCommitter
	log     *slog.Logger
	tr      trace.Tracer
	meter   metric.Meter
	clock   clock.Clock
	budget  *shard.MergeBudget
	cache   *shard.FilterCache
	maxLag  int64
	// openFailed counts copies whose files did not open, by reason.
	openFailed metric.Int64Counter

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

	absent absentIndexes

	// bgCancel stops the Background loops; bg waits for them.
	bgCancel context.CancelFunc
	bg       sync.WaitGroup

	closeOnce sync.Once
	closeErr  error
}

var _ api.Coordinator = (*Single)(nil)

// index is one open index: its catalogue entry and its shards.
type index struct {
	name string
	// catalog serializes changes to the catalogue entry (mapping and settings),
	// honouring deadlines; fields queues writes' new dynamic fields for it.
	catalog catalogLock
	fields  fieldsBatch
	meta    atomic.Pointer[indexState]
	// shards are the index's shards, by number: the count is fixed at creation.
	shards []*shardSlot
	dir    string
	// refresh is the resolved refresh interval in nanoseconds (<= 0: disabled);
	// refreshWake tells the refresher it changed.
	refresh     atomic.Int64
	refreshWake chan struct{}
	dropped     atomic.Bool
	// runCtx is the context the index's tailers and refresher run under; cancel
	// stops them all.
	runCtx context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// indexState is an index's catalogue entry, parsed.
type indexState struct {
	meta     store.IndexMeta
	mapping  *schema.Mapping
	settings api.IndexSettings
}

// shardSlot is one shard of an index, and this node's copy of it when it hosts one.
type shardSlot struct {
	id store.ShardID
	// written is the newest seq the node committed to this shard; querySeq the
	// newest that wrote one of its saved queries.
	written  atomic.Int64
	querySeq atomic.Int64
	// host serializes hosting and unhosting the copy: one tailer per copy directory.
	host           sync.Mutex
	local          atomic.Pointer[copyState]
	unhostedAtNano atomic.Int64
}

// copyState is one shard copy hosted on this node, and its tailer.
type copyState struct {
	id     store.ShardID
	slot   *shardSlot
	tailer Tailer
	perc   *percolate.Percolator
	// halted is why the tailer's Run stopped on its own, nil while it runs.
	halted atomic.Pointer[error]
	// started is set once the copy has finished its startup recovery: at once for
	// a tailer that cannot say (no StateReporter), else once it is first seen
	// tailing.
	started atomic.Bool
	// startup marks a copy readiness waits on: every copy of a single node; on a
	// cluster node, the copies it took when it started.
	startup bool
	// spec is what the cluster gave the copy (cluster nodes): Held reports whether
	// its lease surely holds (nil: no lease, a single node), Quarantined whether it
	// may not serve yet.
	spec HostSpec
	// paused is set once the copy's tailer was stopped because its lease lapsed by
	// the local clock without being confirmed lost: it writes nothing, serves no
	// peer, and stays open for this node's own last-resort reads, stale.
	paused atomic.Bool
	// warm, on a copy resumed from a pause, is the shard it resumed with: while its new
	// tailer starts up over that very shard (not a rebuild's), the copy serves on.
	warm *shard.Shard
	// copy is the registry copy (cluster nodes), nil on a single node.
	copy *store.Copy
	// cancel stops the copy's tailer; done is closed once its Run has returned.
	cancel context.CancelFunc
	done   chan struct{}
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

// copies returns the copies of idx this node hosts, by shard.
func (idx *index) copies() []*copyState {
	out := make([]*copyState, 0, len(idx.shards))
	for _, sl := range idx.shards {
		if c := sl.local.Load(); c != nil {
			out = append(out, c)
		}
	}
	return out
}

// copyRoot is the directory of this node's copy of shard s of idx.
func (idx *index) copyRoot(s int) string { return filepath.Join(idx.dir, strconv.Itoa(s)) }

// pingInterval is how often the database is checked: often enough to notice it is
// gone well within max_lag.
func (n *Single) pingInterval() time.Duration {
	return min(time.Second, max(50*time.Millisecond, n.cfg.MaxLag/4))
}

// pingLoop checks the database until ctx ends.
func (n *Single) pingLoop(ctx context.Context) {
	t := n.clock.NewTicker(n.pingInterval())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
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
		n.dbOK.Store(n.clock.Now().UnixNano())
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

// copyStale reports whether reads of the copy are stale: the database is unreachable,
// or the copy trails the changelog by more than max_lag, cannot poll it, or is being
// rebuilt aside.
func (n *Single) copyStale(c *copyState) bool {
	return n.stale() || c.lapsed() || c.trailing(n.cfg.MaxLag) || c.rebuilding()
}

// lapsed reports whether the copy's lease is not surely held (a cluster node): it is
// paused, or past its local deadline.
func (c *copyState) lapsed() bool {
	return c.paused.Load() || (c.spec.Held != nil && !c.spec.Held())
}

// peerServing is why the copy cannot serve another node's read (or a write's refresh
// wait) now, or nil: it cannot serve at all, or its lease is not surely held.
func (c *copyState) peerServing() error {
	if err := c.notServing(); err != nil {
		return err
	}
	if c.lapsed() {
		return api.Unavailable(store.ErrLeaseLost, "this node's lease on shard %d of index %q has lapsed", c.id.Shard, c.id.Index)
	}
	return nil
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
// its lease may be lost (a cluster node), it has halted, or it is recovering (being
// opened, reopened or rebuilt: the shard a tailer exposes then may be empty or partly
// loaded, so it is never read).
func (c *copyState) notServing() error {
	if c.spec.Quarantined != nil && c.spec.Quarantined() {
		return api.Unavailable(store.ErrLeaseLost, "shard %d of index %q was taken over from another node and serves nothing yet", c.id.Shard, c.id.Index)
	}
	if c.paused.Load() {
		return nil // it serves this node's last-resort reads, stale (lapsed)
	}
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
			if c.warm != nil && c.shard() == c.warm {
				return nil // resumed: its new tailer starts up over the shard that served
			}
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
	return max(0, c.slot.written.Load()-sh.AppliedSeq())
}

func (c *copyState) shard() *shard.Shard { return c.tailer.Shard() }

// NewSingle opens every index in the store's catalogue and, on a single node, a copy of
// every shard with its tailer, and starts them. Copies of indexes no longer in the
// catalogue are removed from data_dir.
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
	if o.Clock == nil {
		o.Clock = clock.Real{}
	}
	if o.GroupCommit.Clock == nil {
		o.GroupCommit.Clock = o.Clock
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
		hub := replica.NewHub(o.Store, replica.HubOptions{Logger: o.Logger, Meter: o.Meter, Clock: o.Clock})
		if hub != nil {
			o.Background = append(o.Background, hub.Run)
		}
		o.NewTailer = ReplicaTailers(o.Config, hub, o.Clock, o.Logger, o.Tracer, o.Meter)
	}
	n := &Single{
		st:       o.Store,
		records:  rr,
		cfg:      o.Config,
		opts:     o,
		cl:       o.Cluster,
		log:      o.Logger,
		tr:       o.Tracer,
		meter:    o.Meter,
		clock:    o.Clock,
		budget:   shard.NewMergeBudget(max(1, o.Config.MergeThreads), o.Config.MergeBudget, o.Clock),
		cache:    shard.NewFilterCache(shard.DefaultFilterCacheBytes, o.Meter),
		maxLag:   o.MaxApplyLag,
		indexes:  map[string]*index{},
		absent:   absentIndexes{clock: o.Clock},
		reserved: map[string]bool{},
	}
	in := telemetry.NewInstruments(o.Meter)
	n.openFailed = in.Counter(telemetry.MetricShardOpenFailed)
	if err := in.Err(); err != nil {
		n.log.ErrorContext(ctx, "shard open metrics unavailable", slog.Any("error", err))
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
	n.log.InfoContext(ctx, "node engine ready", slog.Int("indexes", len(metas)), slog.Bool("cluster", n.cl != nil))
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

// openIndex opens an index and starts its refresher. A single node hosts every shard's
// copy at once; a cluster node hosts the copies the cluster gives it (HostCopy).
func (n *Single) openIndex(ctx context.Context, m store.IndexMeta) (*index, error) {
	state, err := parseState(m)
	if err != nil {
		return nil, err
	}
	idx := &index{name: m.Name, dir: filepath.Join(n.indexesDir(), m.UID), refreshWake: make(chan struct{}, 1), catalog: make(catalogLock, 1)}
	idx.meta.Store(state)
	idx.refresh.Store(int64(n.resolvedRefresh(state.settings)))
	idx.runCtx, idx.cancel = context.WithCancel(context.WithoutCancel(ctx))
	for s := range state.settings.Shards {
		sl := &shardSlot{id: store.ShardID{Index: m.Name, Shard: s}}
		// A percolation waits until the copy has every change up to the head as of
		// its opening (its saved queries among them), then only for queries saved
		// since.
		sl.querySeq.Store(n.head.Load())
		idx.shards = append(idx.shards, sl)
	}
	if n.cl == nil {
		for s := range idx.shards {
			if err := n.hostCopy(ctx, idx, s, HostSpec{Startup: true}); err != nil {
				_ = n.stopIndex(ctx, idx)
				return nil, err
			}
		}
	}
	idx.wg.Add(1)
	go n.refresher(idx.runCtx, idx) //nolint:contextcheck // the refresher outlives the call that opens the index
	return idx, nil
}

// hostCopy opens this node's copy of shard s of idx and starts its tailer, unless it
// hosts one already.
func (n *Single) hostCopy(ctx context.Context, idx *index, s int, spec HostSpec) error {
	sl := idx.shards[s]
	sl.host.Lock()
	defer sl.host.Unlock()
	if sl.local.Load() != nil {
		return nil
	}
	if idx.runCtx.Err() != nil {
		return api.Unavailable(shard.ErrClosed, "index %q is closing", idx.name)
	}
	state := idx.meta.Load()
	opts := shard.Options{
		Index:           idx.name,
		Shard:           s,
		RefreshInterval: -1, // the node's refresher drives refreshes, so the interval can change
		FlushInterval:   n.cfg.FlushInterval,
		MergeBudget:     n.budget,
		FilterCache:     n.cache,
		QueryIndex:      percolate.Index{},
		Clock:           n.clock,
		Logger:          n.log,
		Tracer:          n.tr,
		Meter:           n.meter,
	}
	if n.opts.ShardOptions != nil {
		n.opts.ShardOptions(&opts)
	}
	// A copy rebuilt aside lives in a subdirectory its root names: open the
	// current one (and collect what a crash or a swap left behind). The directory is a
	// cache of the store: one whose data is damaged or of a format this build no longer
	// reads (segment.Rebuildable) is wiped, and the copy, opened empty, is rebuilt from a
	// peer or the store, never served (spec section 10). One written in a newer format is
	// refused and left as it is (spec section 9: a binary rolled back must not destroy
	// its successor's copy), and any other failure (I/O, permissions, resources) is
	// returned for the allocator to retry.
	root := idx.copyRoot(s)
	sh, err := replica.OpenCopy(ctx, root, state.mapping, opts)
	if err != nil && ctx.Err() == nil {
		attrs := []any{slog.String(telemetry.KeyIndex, idx.name), slog.Int(telemetry.KeyShard, s), slog.Any("error", err)}
		switch {
		case segment.Rebuildable(err):
			n.noteOpenFailed(ctx, idx.name, "corrupt")
			n.log.WarnContext(ctx, "shard copy does not open; wiping it to rebuild", attrs...)
			if werr := replica.WipeCopy(root); werr != nil {
				return fmt.Errorf("node: open %s: %w (wiping it failed: %w)", sl.id, err, werr)
			}
			sh, err = replica.OpenCopy(ctx, root, state.mapping, opts)
		case errors.Is(err, segment.ErrNewerFormat):
			n.noteOpenFailed(ctx, idx.name, "newer_format")
			n.log.ErrorContext(ctx, "shard copy is in a newer format than this binary reads; refusing it, and leaving its files as they are", attrs...)
		default:
			n.noteOpenFailed(ctx, idx.name, "error")
		}
	}
	if err != nil {
		return fmt.Errorf("node: open %s: %w", sl.id, err)
	}
	n.noteHead(sh.AppliedSeq())
	env := TailerEnv{Head: n.head.Load, Fetcher: spec.Fetcher}
	if n.cl != nil {
		cp := spec.Copy
		env.Copy = &cp
	}
	c := &copyState{
		id:      sl.id,
		slot:    sl,
		tailer:  n.opts.NewTailer(n.st, sh, sl.id, env),
		startup: spec.Startup,
		spec:    spec,
		copy:    env.Copy,
		perc: percolate.New(percolate.Options{
			Index: idx.name, Shard: s, Threads: n.cfg.SearchThreads,
			Logger: n.log, Tracer: n.tr, Meter: n.meter,
		}),
	}
	n.startTailer(idx, c) //nolint:contextcheck // the tailer outlives the call that hosts the copy
	return nil
}

// startTailer runs c's tailer and makes c the slot's copy. The slot's host lock is
// held.
func (n *Single) startTailer(idx *index, c *copyState) {
	var runCtx context.Context
	runCtx, c.cancel = context.WithCancel(idx.runCtx)
	c.done = make(chan struct{})
	c.slot.local.Store(c)
	idx.wg.Add(1)
	go n.runTailer(runCtx, idx, c)
}

// runTailer runs a copy's tailer until the index stops or the copy is unhosted,
// recording why it halted when it stops on its own (and telling the cluster).
func (n *Single) runTailer(ctx context.Context, idx *index, c *copyState) {
	defer idx.wg.Done()
	defer close(c.done)
	var stopped error
	defer func() {
		if p := recover(); p != nil {
			err := fmt.Errorf("tailer panic: %v", p)
			c.halted.Store(&err)
			stopped = err
			n.log.ErrorContext(ctx, "shard copy tailer panicked", slog.String(telemetry.KeyIndex, c.id.Index), slog.Int(telemetry.KeyShard, c.id.Shard), slog.Any("panic", p))
		}
		if stopped != nil && n.cl != nil && c.copy != nil {
			n.cl.CopyStopped(*c.copy, stopped)
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
	stopped = err
	n.log.ErrorContext(ctx, "shard copy halted", slog.String(telemetry.KeyIndex, c.id.Index), slog.Int(telemetry.KeyShard, c.id.Shard), slog.Any("error", err))
}

func (n *Single) refresher(ctx context.Context, idx *index) {
	defer idx.wg.Done()
	warn := time.Time{}
	clock.GridLoop{
		Clock:    n.clock,
		Period:   func() time.Duration { return time.Duration(idx.refresh.Load()) },
		Reanchor: idx.refreshWake,
		Task: func() {
			for _, c := range idx.copies() {
				sh := c.shard()
				if sh == nil || sh.Err() != nil {
					continue
				}
				if err := sh.Refresh(ctx); err != nil && ctx.Err() == nil && !errors.Is(err, shard.ErrClosed) && n.clock.Since(warn) > time.Minute {
					warn = n.clock.Now()
					n.log.WarnContext(ctx, "refresh failed", slog.String(telemetry.KeyIndex, c.id.Index), slog.Int(telemetry.KeyShard, c.id.Shard), slog.Any("error", err))
				}
			}
		},
	}.Run(ctx)
}

// stopIndex stops an index's tailers and refresher and closes its copies.
func (n *Single) stopIndex(ctx context.Context, idx *index) error {
	if idx.cancel != nil {
		idx.cancel()
	}
	idx.wg.Wait()
	var errs []error
	for _, sl := range idx.shards {
		sl.host.Lock()
		c := sl.local.Swap(nil)
		sl.host.Unlock()
		if c == nil {
			continue
		}
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

// lookup returns an open index, or a 404. A cluster node that does not know the index
// yet (another node created it since its last catalogue sync) looks it up in the store.
func (n *Single) lookup(ctx context.Context, name string) (*index, error) {
	return n.lookupIndex(ctx, name, false)
}

// lookupForRead is lookup for a read, which may answer 404 from a recent store lookup
// (absentTTL) unless it waits for a seq: a read that names one follows a write, and
// must find the index that write found.
func (n *Single) lookupForRead(ctx context.Context, name string, waitSeq int64) (*index, error) {
	return n.lookupIndex(ctx, name, waitSeq <= 0)
}

func (n *Single) lookupIndex(ctx context.Context, name string, cachedAbsence bool) (*index, error) {
	n.mu.RLock()
	closed, idx := n.closed, n.indexes[name]
	n.mu.RUnlock()
	if closed {
		return nil, api.Unavailable(store.ErrClosed, "the node is shutting down")
	}
	if idx != nil {
		return idx, nil
	}
	if n.cl != nil {
		if idx, err := n.adoptIndex(ctx, name, cachedAbsence); err != nil || idx != nil {
			return idx, err
		}
	}
	return nil, indexNotFound(name)
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
		n.log.InfoContext(ctx, "node engine closed")
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

// noteOpenFailed counts a copy of index whose files did not open, for reason.
func (n *Single) noteOpenFailed(ctx context.Context, index, reason string) {
	if n.openFailed != nil {
		n.openFailed.Add(ctx, 1, metric.WithAttributes(attribute.String(telemetry.KeyIndex, index), attribute.String("reason", reason)))
	}
}
