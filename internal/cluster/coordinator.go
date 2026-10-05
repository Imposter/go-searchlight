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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/clock"
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
	// DefaultSQLiteLeaseTTL is the lease TTL on SQLite, whose single writer can hold a
	// renewal behind a long commit (and a checkpoint) for a while.
	DefaultSQLiteLeaseTTL  = 30 * time.Second
	DefaultCatalogInterval = time.Second
	DefaultPruneInterval   = 30 * time.Second
	DefaultPruneStall      = 15 * time.Minute
	DefaultRetiringKeep    = 15 * time.Minute
	DefaultChangelogKeep   = 24 * time.Hour
	DefaultSnapshotMaxAge  = time.Hour
	DefaultPeerIdleTimeout = 30 * time.Second
	DefaultGCInterval      = time.Minute
	DefaultCopyDirGrace    = 10 * time.Minute
	DefaultSweepInterval   = 10 * time.Minute
	DefaultSweepAge        = time.Hour
	DefaultPeerTimeout     = 10 * time.Second
	DefaultPinTTL          = 30 * time.Second
	DefaultSnapshotTTL     = 2 * time.Minute
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
	// LeaseTTL how long a lease lasts unrenewed (config lease_ttl, else 30 s on SQLite
	// and 10 s elsewhere). LeaseMargin is how long before its local deadline a copy
	// stops serving peers (LeaseTTL/10), covering the clocks' rate difference.
	HeartbeatInterval, DeadAfter, LeaseTTL, LeaseMargin time.Duration
	// ViewInterval is how often the registry is read for routing (HeartbeatInterval/4).
	ViewInterval time.Duration
	// CatalogInterval is how often the index catalogue is synced (1 s).
	CatalogInterval time.Duration
	// PruneInterval is how often the leader prunes the changelog (30 s).
	// PruneStallTimeout is how long a copy behind the others may make no progress and
	// still hold the prune floor (config prune_stall_timeout, 15 min);
	// RetiringRetention how long a cleanly stopped copy's row holds it
	// (retiring_retention, 15 min); ChangelogRetention the oldest a change may grow
	// whatever copy needs it (changelog_retention, 24 h).
	PruneInterval, PruneStallTimeout, RetiringRetention, ChangelogRetention time.Duration
	// GCInterval is how often unused copy directories and recovery staging are looked
	// for (1 min), and CopyDirGrace how long one must have lain unused (10 min).
	GCInterval, CopyDirGrace time.Duration
	// SweepInterval is how often the leader sweeps abandoned blob uploads (10 min),
	// SweepAge how old they must be (1 h).
	SweepInterval, SweepAge time.Duration
	// PeerTimeout bounds one attempt of a read on a peer (10 s); past it the read is
	// retried on another copy.
	PeerTimeout time.Duration
	// PinTTL is how long a peer keeps a generation pinned for a search's fetch phase
	// (30 s); SnapshotTTL how long an idle recovery snapshot is kept (2 min), and
	// SnapshotMaxAge how long any is (1 h: a recovery resumes on a fresh one).
	PinTTL, SnapshotTTL, SnapshotMaxAge time.Duration
	// PeerIdleTimeout cuts a snapshot stream that delivers nothing for that long
	// (30 s): the recovery resumes it.
	PeerIdleTimeout time.Duration

	// Transport carries the peer API's requests; nil means a pooled default.
	Transport http.RoundTripper
	// Clock runs the node's timers (heartbeats, lease renewals and the watchdog,
	// maintenance, routing, recovery) and those of its engine, and keeps its lease
	// deadlines: by the monotonic reading (Since) and, against a suspended machine,
	// the wall clock (Wall). Nil means clock.Real.
	Clock clock.Clock
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
	allowSQLiteCluster bool

	// peerFile wraps the writer a snapshot file is streamed to.
	peerFile func(name string, w http.ResponseWriter) http.ResponseWriter
	// served is told of every read target this node's copy of id gave a peer, with the
	// leaseClock readings taken before the copy was checked.
	served func(id store.ShardID, began time.Duration, wall time.Time)
	// servedLocal is told of every read of this node's copy of id held under l, local
	// reads included.
	servedLocal func(id store.ShardID, l *lease)
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
		o.LeaseTTL = o.Config.LeaseTTL
	}
	if o.LeaseTTL <= 0 {
		o.LeaseTTL = DefaultLeaseTTL
		if o.Store.Dialect() == "sqlite" {
			o.LeaseTTL = DefaultSQLiteLeaseTTL
		}
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
	o.PruneStallTimeout = firstPositive(o.PruneStallTimeout, o.Config.PruneStallTimeout, DefaultPruneStall)
	o.RetiringRetention = firstPositive(o.RetiringRetention, o.Config.RetiringRetention, DefaultRetiringKeep)
	o.ChangelogRetention = firstPositive(o.ChangelogRetention, o.Config.ChangelogRetention, DefaultChangelogKeep)
	o.GCInterval = firstPositive(o.GCInterval, DefaultGCInterval)
	o.CopyDirGrace = firstPositive(o.CopyDirGrace, DefaultCopyDirGrace)
	o.SnapshotMaxAge = firstPositive(o.SnapshotMaxAge, DefaultSnapshotMaxAge)
	o.PeerIdleTimeout = firstPositive(o.PeerIdleTimeout, DefaultPeerIdleTimeout)
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
		o.Clock = clock.Real{}
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

// firstPositive returns the first positive duration of ds.
func firstPositive(ds ...time.Duration) time.Duration {
	for _, d := range ds {
		if d > 0 {
			return d
		}
	}
	return 0
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
	clock clock.Clock
	lc    leaseClock
	inst  *instruments

	client *http.Client
	scheme string
	// clearWarned are the peers warned about getting the cluster token in the clear.
	clearWarned sync.Map
	ars         *ars
	view        atomic.Pointer[view]
	missMu      sync.Mutex
	missReadAt  time.Time

	// leases are the copies this node holds, by shard.
	leaseMu sync.Mutex
	leases  map[store.ShardID]*lease
	// quarantines are the quarantines of the slots this node took over, by shard: they
	// outlive the lease, so a claim of the same slot at the same epoch keeps them.
	quarantines map[store.ShardID]quarantine
	// allocMu serializes allocation passes (claims and releases) and forgetting the
	// leases of dropped indexes. Renewals do not take it: they wait on nothing.
	allocMu sync.Mutex

	progressMu sync.Mutex
	progressHW map[copyKey]int64

	startedAt time.Time

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
		opts:        o,
		cfg:         o.Config,
		id:          o.Config.NodeID,
		st:          o.Store,
		reg:         o.Store.Registry(),
		log:         o.Logger.With(slog.String(telemetry.KeyNodeID, o.Config.NodeID)),
		tr:          o.Tracer,
		clock:       o.Clock,
		lc:          leaseClock{c: o.Clock, epoch: o.Clock.Now()},
		leases:      map[store.ShardID]*lease{},
		quarantines: map[store.ShardID]quarantine{},
		scheme:      "http",
		startedAt:   o.Clock.Now(),
	}
	n.alloc.clock = o.Clock
	if o.Config.TLSCert != "" {
		n.scheme = "https"
	}
	if n.scheme == "http" && o.Config.ClusterToken != "" && !loopbackAddress(o.Config.AdvertiseAddress) {
		n.log.WarnContext(ctx, "cluster_token is sent in the clear: advertise_address is not loopback and tls_cert is unset",
			slog.String("address", o.Config.AdvertiseAddress))
	}
	if o.Config.ShutdownGrace < o.ViewInterval {
		n.log.WarnContext(ctx, "shutdown_grace is shorter than the routing view interval: while this node stops, peers still route reads to it after its listener closes, and those reads fail",
			slog.Duration("shutdown_grace", o.Config.ShutdownGrace), slog.Duration("view_interval", o.ViewInterval))
	}
	n.inst = newInstruments(o.Meter, n.log)
	transport := o.Transport
	if transport == nil {
		t := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert,errcheck // the default transport is an *http.Transport
		t.MaxIdleConnsPerHost = 64
		t.IdleConnTimeout = time.Minute
		if o.Config.PeerCAFile != "" {
			pem, err := os.ReadFile(o.Config.PeerCAFile)
			if err != nil {
				return nil, fmt.Errorf("cluster: peer_ca_file: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("cluster: peer_ca_file %s holds no PEM certificate", o.Config.PeerCAFile)
			}
			t.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
		}
		transport = t
	}
	n.client = &http.Client{Transport: transport}
	n.ars = newARS(o.Clock)
	n.view.Store(&view{nodes: map[string]store.Node{}, live: map[string]bool{n.id: true}, copies: map[store.ShardID][]store.Copy{}})
	n.pins = newPinTable(o.PinTTL, o.Clock)
	n.snaps = newSnapTable(o.SnapshotTTL, o.SnapshotMaxAge, o.Clock)
	n.sums = newSumCache()
	n.hints = newHinter(n)
	n.fetch = &fetcher{n: n}
	n.peer = &peerAPI{n: n}
	eo := node.Options{Store: o.Store, Config: o.Config, Version: o.Version, Cluster: &clusterHooks{n}, Clock: o.Clock, Logger: o.Logger, Tracer: o.Tracer, Meter: o.Meter}
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

// loopbackAddress reports whether addr (a host:port, or a bare host) resolves to the
// loopback interface: a plain-http cluster_token never leaves the machine then.
// A host that is not a literal IP (a hostname) is treated as non-loopback, since it may
// resolve to a reachable address depending on DNS.
func loopbackAddress(addr string) bool {
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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
	if err := n.singleSQLite(ctx); err != nil {
		return err
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

// ErrSQLiteCluster is returned by Start on a SQLite store that another live node
// already uses: SQLite serves one node (spec sections 9 and 12); a cluster needs
// Postgres or MySQL.
var ErrSQLiteCluster = errors.New("cluster: a SQLite store serves a single node; use Postgres or MySQL for a cluster")

// singleSQLite refuses to join when the store is SQLite and another live node is
// registered (in-process tests aside). The node checks
// after registering, so of two nodes starting at once at least one sees the other; it
// deregisters before it fails.
func (n *Node) singleSQLite(ctx context.Context) error {
	if n.st.Dialect() != "sqlite" || (n.opts.hooks != nil && n.opts.hooks.allowSQLiteCluster) {
		return nil
	}
	v := n.view.Load()
	var others []string
	for _, id := range v.liveNodes() {
		if id != n.id {
			others = append(others, id)
		}
	}
	if len(others) == 0 {
		return nil
	}
	if err := n.reg.RemoveNode(context.WithoutCancel(ctx), n.id); err != nil {
		n.log.WarnContext(ctx, "deregistering after refusing to join failed; the node ages out", slog.Any("error", err))
	}
	return fmt.Errorf("%w (node %q found live node(s) %s on it)", ErrSQLiteCluster, n.id, strings.Join(others, ", "))
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
// that a serving copy elsewhere can stand in for has its tailer stopped, is marked
// retiring (only while another copy still serves, checked atomically in the store, so
// nodes draining together never retire a shard's last serving copies) and is closed;
// reads go to the other copies and the allocator may place a replacement. A copy that
// is the shard's only serving one keeps serving until Stop. The API calls it when it
// starts draining (readiness false); Stop calls it too. It is idempotent.
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
		if n.retire(ctx, l) {
			retired++
		}
	}
	n.log.InfoContext(ctx, "cluster node draining", slog.Int("retired", retired), slog.Int("kept", n.leaseCount()-retired))
}

// retire stops a copy's tailer (a running tailer could still promote it to serving),
// then marks it retiring if another copy serves, and closes it; otherwise it resumes
// the copy, which keeps serving. It reports whether the copy retired.
func (n *Node) retire(ctx context.Context, l *lease) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost || l.retired.Load() {
		return false
	}
	if !l.paused {
		if err := n.PauseCopy(ctx, l.copy); err != nil {
			n.log.WarnContext(ctx, "stopping a draining copy's tailer failed", slog.String("shard", l.copy.Shard.String()), slog.Any("error", err))
			return false
		}
	}
	ok, err := n.reg.RetireCopy(ctx, l.copy)
	if err != nil || !ok {
		if err != nil && !errors.Is(err, store.ErrLeaseLost) {
			n.log.WarnContext(ctx, "marking a copy retiring failed; it keeps serving", slog.String("shard", l.copy.Shard.String()), slog.Any("error", err))
		}
		if !l.paused {
			if rerr := n.ResumeCopy(ctx, l.copy); rerr != nil {
				n.log.WarnContext(ctx, "resuming a copy that could not retire failed", slog.String("shard", l.copy.Shard.String()), slog.Any("error", rerr))
			}
		}
		return false
	}
	l.retired.Store(true)
	l.copy.State = store.CopyRetiring
	n.inst.allocation(ctx, l.copy.Shard, string(store.CopyRetiring))
	if err := n.unhostCopy(ctx, l.copy, false); err != nil {
		n.log.WarnContext(ctx, "closing a retiring copy failed", slog.String("shard", l.copy.Shard.String()), slog.Any("error", err))
	}
	return true
}

// Stop leaves the cluster as a rolling restart wants: it drains (if the API has not),
// stops the loops, stops every copy's tailer and closes its shard (a final manifest and
// a final applied seq), marks every copy retiring and deregisters the node, then closes
// the engine. The copies' leases are not released: their rows stay, retiring, at their
// final applied seqs, so the changelog the node needs to replay when it comes back is
// kept for retiring_retention, and the node claims its own slots back at the same
// epochs. Peers place replacements meanwhile (a retiring copy does not count towards
// its target). It does not close the store. It is idempotent; Close is the same.
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
			if err := n.unhostCopy(ctx, l.copy, false); err != nil {
				errs = append(errs, err)
			}
			if !l.retired.Load() {
				if err := n.reg.SetCopyState(ctx, l.copy, store.CopyRetiring); err == nil {
					n.inst.allocation(ctx, l.copy.Shard, string(store.CopyRetiring))
				}
			}
			n.dropLease(l)
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
			n.dropLease(l)
		}
		n.hints.close()
		n.pins.closeAll()
		n.snaps.closeAll()
		_ = n.Single.Close(ctx)
	})
}
