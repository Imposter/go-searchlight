// Package cluster makes Searchlight nodes a plug-and-play cluster over the SQL database
// (spec section 9). There is no coordination service: the database's registry is the
// only meeting point.
//
//   - Membership (membership.go): a node registers in sl_nodes and heartbeats every
//     HeartbeatInterval (2 s); one silent for DeadAfter (10 s) is dead. Peers are found
//     in the same table, so a node needs only store_url (and advertise_address behind
//     NAT) to join.
//   - Leases (membership.go): each shard copy is a slot of sl_shard_copies held under a
//     lease with a fencing epoch. The node renews its leases on the same tick (a
//     separate call from the heartbeat) and keeps a local deadline per copy, by its own
//     monotonic clock, from the moment before each renewal: past it (less a margin) the
//     copy stops serving and its tailer stops, before any other node can steal the
//     slot.
//   - Allocation (allocator.go): every node runs the allocator. A shard below its copy
//     target is claimed by an eligible node (one without a copy of it, with spare
//     capacity, least loaded first) with the store's conditional claim; extra slots
//     are released when a target is lowered.
//   - Peer recovery (recovery.go, peer_api.go): a new copy fetches a serving peer's
//     snapshot over the internal API, file by file, each checked against its SHA-256,
//     resumable within a file and across attempts, the manifest written last; it falls
//     back to the store's snapshot.
//   - Routing (router.go, ars.go): any node takes any request. Writes commit to SQL
//     and push hints to the peers holding the written shards. Reads use this node's
//     copy when it is current, else a serving copy elsewhere chosen by adaptive replica
//     selection, retrying another copy when one fails.
//   - Rolling restarts (coordinator.go): Drain stops this node's copies that others
//     can stand in for and marks them retiring; Stop then stops the rest, releases the
//     leases and deregisters.
//   - Maintenance (maintenance.go): the live node with the lowest id prunes the
//     changelog behind the copies and sweeps abandoned blobs.
//
// [Node] is the cluster node: an [api.Coordinator] over the node engine (node.Single)
// with this package as its node.Cluster.
package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// Defaults (spec section 9).
const (
	DefaultHeartbeatInterval = 2 * time.Second
	DefaultDeadAfter         = 10 * time.Second
	DefaultLeaseTTL          = 10 * time.Second
	DefaultCatalogInterval   = time.Second
	DefaultPruneInterval     = 30 * time.Second
	DefaultPruneHaltGrace    = 15 * time.Minute
	DefaultSweepInterval     = 10 * time.Minute
	DefaultSweepAge          = time.Hour
	DefaultPeerTimeout       = 10 * time.Second
	DefaultPinTTL            = 30 * time.Second
	DefaultSnapshotTTL       = 2 * time.Minute
)

// Options configures a [Node]. Only Store and Config are required.
type Options struct {
	// Store is the SQL store, migrated. The node does not close it.
	Store store.Store
	// Config is the node's configuration: node_id, advertise_address,
	// cluster_token, data_dir and the engine's settings are used.
	Config config.Config
	// Version is reported in the registry.
	Version string
	// Capacity caps the shard copies this node holds; 0 means no cap.
	Capacity int

	// HeartbeatInterval is how often the node heartbeats, renews its leases and
	// allocates (2 s). DeadAfter is how long a silent node stays live (10 s), and
	// LeaseTTL how long a lease lasts unrenewed (10 s). LeaseMargin is how long
	// before its local deadline a copy stops serving (LeaseTTL/10), covering the
	// clocks' rate difference.
	HeartbeatInterval, DeadAfter, LeaseTTL, LeaseMargin time.Duration
	// ViewInterval is how often the registry is read for routing (HeartbeatInterval/4).
	ViewInterval time.Duration
	// CatalogInterval is how often the index catalogue is synced (1 s).
	CatalogInterval time.Duration
	// PruneInterval is how often the leader prunes the changelog (30 s), and
	// PruneHaltGrace how long a copy whose applied seq does not move while another
	// copy's does may hold the prune floor back (15 min).
	PruneInterval, PruneHaltGrace time.Duration
	// SweepInterval is how often the leader sweeps abandoned blob uploads (10 min),
	// SweepAge how old they must be (1 h).
	SweepInterval, SweepAge time.Duration
	// PeerTimeout bounds one attempt of a read on a peer (10 s); past it the read is
	// retried on another copy.
	PeerTimeout time.Duration
	// PinTTL is how long a peer keeps a generation pinned for a search's fetch phase
	// (30 s); SnapshotTTL how long an idle recovery snapshot is kept (2 min).
	PinTTL, SnapshotTTL time.Duration

	// Transport carries the peer API's requests; nil means a pooled default.
	Transport http.RoundTripper
	// Clock is the monotonic clock lease deadlines are kept by; nil means the
	// process's.
	Clock Clock
	// Engine, when set, adjusts the engine's options (tests: fake tailers, small
	// shards).
	Engine func(*node.Options)

	// Logger, Tracer and Meter are the node's telemetry; nil means slog.Default() and
	// the OpenTelemetry globals.
	Logger *slog.Logger
	Tracer trace.Tracer
	Meter  metric.Meter

	// hooks are test seams; nil outside tests.
	hooks *testHooks
}

// testHooks are test seams.
type testHooks struct {
	// peerFile wraps the writer a snapshot file is streamed to.
	peerFile func(name string, w http.ResponseWriter) http.ResponseWriter
}

func (o *Options) resolve() error {
	if o.Store == nil {
		return errors.New("cluster: Options.Store is required")
	}
	if o.Config.NodeID == "" {
		return errors.New("cluster: node_id is required")
	}
	if o.HeartbeatInterval <= 0 {
		o.HeartbeatInterval = DefaultHeartbeatInterval
	}
	if o.DeadAfter <= 0 {
		o.DeadAfter = DefaultDeadAfter
	}
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = DefaultLeaseTTL
	}
	if o.LeaseTTL <= o.HeartbeatInterval {
		return fmt.Errorf("cluster: the lease ttl (%s) must exceed the heartbeat interval (%s)", o.LeaseTTL, o.HeartbeatInterval)
	}
	if o.LeaseMargin <= 0 {
		o.LeaseMargin = o.LeaseTTL / 10
	}
	if o.ViewInterval <= 0 {
		o.ViewInterval = max(10*time.Millisecond, o.HeartbeatInterval/4)
	}
	if o.CatalogInterval <= 0 {
		o.CatalogInterval = DefaultCatalogInterval
	}
	if o.PruneInterval <= 0 {
		o.PruneInterval = DefaultPruneInterval
	}
	if o.PruneHaltGrace <= 0 {
		o.PruneHaltGrace = DefaultPruneHaltGrace
	}
	if o.SweepInterval <= 0 {
		o.SweepInterval = DefaultSweepInterval
	}
	if o.SweepAge <= 0 {
		o.SweepAge = DefaultSweepAge
	}
	if o.PeerTimeout <= 0 {
		o.PeerTimeout = DefaultPeerTimeout
	}
	if o.PinTTL <= 0 {
		o.PinTTL = DefaultPinTTL
	}
	if o.SnapshotTTL <= 0 {
		o.SnapshotTTL = DefaultSnapshotTTL
	}
	if o.Clock == nil {
		o.Clock = NewClock()
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
	return nil
}

// Node is one node of a cluster: the [api.Coordinator] every API request goes to. Build
// it with [New], start it with [Node.Start] and serve [Node.Handler]; [Node.Stop] (or
// Close) shuts it down as a rolling restart wants.
type Node struct {
	*node.Single

	opts  Options
	cfg   config.Config
	id    string
	st    store.Store
	reg   store.RegistryStore
	log   *slog.Logger
	tr    trace.Tracer
	clock Clock
	inst  *instruments

	client *http.Client
	scheme string
	ars    *ars
	view   atomic.Pointer[view]

	// leases are the copies this node holds, by shard.
	leaseMu sync.Mutex
	leases  map[store.ShardID]*lease
	// allocMu serializes claims, renewals and the lease bookkeeping around them.
	allocMu sync.Mutex

	pins  *pinTable
	snaps *snapTable
	sums  *sumCache
	hints *hinter
	fetch *fetcher
	peer  *peerAPI
	alloc allocState
	prune pruneState

	started  atomic.Bool
	draining atomic.Bool
	stopped  atomic.Bool
	drainMu  sync.Mutex

	loopCtx    context.Context
	loopCancel context.CancelFunc
	loops      sync.WaitGroup
	// bg is background work (unpins, snapshot releases, unhosting a stopped copy).
	bgMu     sync.Mutex
	bgClosed bool
	bg       sync.WaitGroup
	stopOnce sync.Once
	stopErr  error
}

var (
	_ api.Coordinator = (*Node)(nil)
	_ node.Cluster    = (*clusterHooks)(nil)
)

// clusterHooks is the Node as the engine's node.Cluster: kept apart so its methods do
// not join the Node's own.
type clusterHooks struct{ n *Node }

// New builds a cluster node over the store: the engine opens every index in the
// catalogue, hosting no copy until [Node.Start] allocates. It does not touch the
// registry.
func New(ctx context.Context, o Options) (*Node, error) {
	if err := o.resolve(); err != nil {
		return nil, err
	}
	n := &Node{
		opts:   o,
		cfg:    o.Config,
		id:     o.Config.NodeID,
		st:     o.Store,
		reg:    o.Store.Registry(),
		log:    o.Logger.With(slog.String(telemetry.KeyNodeID, o.Config.NodeID)),
		tr:     o.Tracer,
		clock:  o.Clock,
		leases: map[store.ShardID]*lease{},
		scheme: "http",
	}
	if o.Config.TLSCert != "" {
		n.scheme = "https"
	}
	n.inst = newInstruments(o.Meter, n.log)
	transport := o.Transport
	if transport == nil {
		t := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert,errcheck // the default transport is an *http.Transport
		t.MaxIdleConnsPerHost = 64
		t.IdleConnTimeout = time.Minute
		transport = t
	}
	n.client = &http.Client{Transport: transport}
	n.ars = newARS()
	n.view.Store(&view{nodes: map[string]store.Node{}, live: map[string]bool{n.id: true}, copies: map[store.ShardID][]store.Copy{}})
	n.pins = newPinTable(o.PinTTL)
	n.snaps = newSnapTable(o.SnapshotTTL)
	n.sums = newSumCache()
	n.hints = newHinter(n)
	n.fetch = &fetcher{n: n}
	n.peer = &peerAPI{n: n}
	eo := node.Options{Store: o.Store, Config: o.Config, Version: o.Version, Cluster: &clusterHooks{n}, Logger: o.Logger, Tracer: o.Tracer, Meter: o.Meter}
	if o.Engine != nil {
		o.Engine(&eo)
	}
	eo.Cluster = &clusterHooks{n}
	single, err := node.NewSingle(ctx, eo)
	if err != nil {
		return nil, err
	}
	n.Single = single
	n.loopCtx, n.loopCancel = context.WithCancel(context.WithoutCancel(ctx))
	return n, nil
}

// ID is the node's id.
func (n *Node) ID() string { return n.id }

// Start joins the cluster: it registers the node, reads the registry and the
// catalogue, takes this node's share of the copies (the ones it held before a
// restart first) and starts the heartbeat, lease, allocation, routing and
// maintenance loops. The copies it takes recover in the background; Ready reports
// when they have. Serve [Node.Handler] on advertise_address before calling it: peers
// may call the node as soon as it registers.
func (n *Node) Start(ctx context.Context) error {
	if n.stopped.Load() {
		return errors.New("cluster: the node was stopped")
	}
	if err := n.heartbeat(ctx); err != nil {
		return fmt.Errorf("cluster: register node %q: %w", n.id, err)
	}
	if err := n.refreshView(ctx); err != nil {
		return fmt.Errorf("cluster: read the registry: %w", err)
	}
	if err := n.SyncCatalog(ctx); err != nil {
		return fmt.Errorf("cluster: read the catalogue: %w", err)
	}
	if err := n.allocate(ctx, "", true); err != nil {
		n.log.WarnContext(ctx, "the first allocation pass failed; the allocator retries", slog.Any("error", err))
	}
	n.started.Store(true)
	n.goLoop(n.leaseLoop)
	n.goLoop(n.heartbeatLoop)
	n.goLoop(n.allocLoop)
	n.goLoop(n.viewLoop)
	n.goLoop(n.catalogLoop)
	n.goLoop(n.leaseWatchdog)
	n.goLoop(n.maintenanceLoop)
	n.goLoop(n.janitorLoop)
	n.log.InfoContext(ctx, "cluster node started", slog.String("address", n.cfg.AdvertiseAddress), slog.Int("copies", n.leaseCount()))
	return nil
}

// goLoop runs fn until the node stops.
func (n *Node) goLoop(fn func(ctx context.Context)) {
	n.loops.Go(func() { fn(n.loopCtx) })
}

// Handler serves the internal peer API under /_internal/ and the public API (pub)
// everywhere else: mount it on the node's listener, which advertise_address names.
func (n *Node) Handler(pub http.Handler) http.Handler {
	peer := n.peer.handler()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, peerPrefix) {
			peer.ServeHTTP(w, r)
			return
		}
		pub.ServeHTTP(w, r)
	})
}

// Drain begins a rolling shutdown: the node takes no new copy, and each copy it holds
// that a serving copy elsewhere can stand in for has its tailer stopped and is marked
// retiring, so reads go to the other copies and the allocator may place a
// replacement. A copy that is the shard's only serving one keeps serving until Stop.
// The API calls it when it starts draining (readiness false); Stop calls it too. It is
// idempotent.
func (n *Node) Drain(ctx context.Context) {
	n.drainMu.Lock()
	defer n.drainMu.Unlock()
	if n.draining.Swap(true) {
		return
	}
	if err := n.refreshView(ctx); err != nil {
		n.log.WarnContext(ctx, "reading the registry before draining failed", slog.Any("error", err))
	}
	v := n.view.Load()
	retired := 0
	for _, l := range n.leaseList() {
		if v.servingElsewhere(l.copy.Shard, n.id) == 0 {
			continue // the only serving copy: it serves until Stop
		}
		n.retire(ctx, l)
		retired++
	}
	n.log.InfoContext(ctx, "cluster node draining", slog.Int("retired", retired), slog.Int("kept", n.leaseCount()))
}

// retire stops a copy's tailer and its serving, then marks it retiring in the registry
// (in that order: a running tailer could still promote it to serving).
func (n *Node) retire(ctx context.Context, l *lease) {
	if err := n.UnhostCopy(ctx, l.copy, false); err != nil {
		n.log.WarnContext(ctx, "closing a retiring copy failed", slog.String("shard", l.copy.Shard.String()), slog.Any("error", err))
	}
	if err := n.reg.SetCopyState(ctx, l.copy, store.CopyRetiring); err != nil && !errors.Is(err, store.ErrLeaseLost) {
		n.log.WarnContext(ctx, "marking a copy retiring failed", slog.String("shard", l.copy.Shard.String()), slog.Any("error", err))
	} else if err == nil {
		l.retired.Store(true)
		n.inst.allocation(ctx, l.copy.Shard, string(store.CopyRetiring))
	}
}

// Stop leaves the cluster as a rolling restart wants: it drains (if the API has not),
// stops the loops, stops every copy's tailer and closes its shard (a final manifest),
// releases the leases, deregisters the node and closes the engine. It does not close
// the store. It is idempotent; Close is the same.
func (n *Node) Stop(ctx context.Context) error {
	n.stopOnce.Do(func() {
		n.stopped.Store(true)
		if n.started.Load() {
			n.Drain(ctx)
		}
		n.loopCancel()
		n.loops.Wait()
		n.closeBackground()
		var errs []error
		for _, l := range n.leaseList() {
			if err := n.UnhostCopy(ctx, l.copy, false); err != nil {
				errs = append(errs, err)
			}
			if !l.retired.Load() {
				if err := n.reg.SetCopyState(ctx, l.copy, store.CopyRetiring); err == nil {
					n.inst.allocation(ctx, l.copy.Shard, string(store.CopyRetiring))
				}
			}
			if err := n.reg.ReleaseCopy(ctx, l.copy); err != nil && !errors.Is(err, store.ErrLeaseLost) {
				n.log.WarnContext(ctx, "releasing a copy failed; its lease runs out", slog.String("shard", l.copy.Shard.String()), slog.Any("error", err))
			} else {
				n.inst.lease(ctx, "release")
			}
			n.dropLease(l.copy.Shard)
		}
		if n.started.Load() {
			if err := n.reg.RemoveNode(ctx, n.id); err != nil {
				n.log.WarnContext(ctx, "deregistering failed; the node ages out", slog.Any("error", err))
			}
		}
		n.hints.close()
		n.pins.closeAll()
		n.snaps.closeAll()
		if err := n.Single.Close(ctx); err != nil {
			errs = append(errs, err)
		}
		n.stopErr = errors.Join(errs...)
		n.log.InfoContext(ctx, "cluster node stopped")
	})
	return n.stopErr
}

// Close implements [api.Coordinator]: [Node.Stop].
func (n *Node) Close(ctx context.Context) error { return n.Stop(ctx) }

// kill stops the node as a crash would (tests): no drain, no final commit, no lease
// released, no deregistration. The shards are abandoned with what they had not
// committed.
func (n *Node) kill() {
	n.stopOnce.Do(func() {
		n.stopped.Store(true)
		n.draining.Store(true)
		n.loopCancel()
		n.loops.Wait()
		n.closeBackground()
		ctx := context.Background()
		for _, l := range n.leaseList() {
			_ = n.AbandonCopy(ctx, l.copy)
			n.dropLease(l.copy.Shard)
		}
		n.hints.close()
		n.pins.closeAll()
		n.snaps.closeAll()
		_ = n.Single.Close(ctx)
	})
}
