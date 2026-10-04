package cluster

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/Imposter/go-searchlight/internal/store"
)

// Clock is the monotonic clock lease deadlines are kept by. Now is the time elapsed
// since a fixed point of the clock's choosing; it never goes back.
type Clock interface {
	Now() time.Duration
}

// NewClock returns the process's monotonic clock.
func NewClock() Clock { return monotonic{start: time.Now()} }

type monotonic struct{ start time.Time }

func (c monotonic) Now() time.Duration { return time.Since(c.start) }

// lease is a copy this node holds, and the local deadline of its lease.
//
// The deadline is taken by the node's own monotonic clock: the clock's reading just
// before the call that granted or renewed the lease, plus the lease's TTL. The
// database starts the TTL later (when it runs the statement), so its lease_until is
// never earlier than this deadline (up to the clocks' rate difference, which
// LeaseMargin covers). Another node can steal the slot only once the database's
// lease_until has passed; the copy stops serving at the local deadline less the
// margin, which is earlier. So a node cut off from the database stops serving every
// copy it may have lost before another node can take any of them over.
type lease struct {
	copy     store.Copy
	deadline atomic.Int64 // Clock nanoseconds
	margin   time.Duration
	clock    Clock
	// retired is set once the copy is marked retiring.
	retired atomic.Bool
}

// valid reports whether the copy may still serve: its lease holds by the local
// deadline, less the margin.
func (l *lease) valid() bool {
	return l.clock.Now() < time.Duration(l.deadline.Load())-l.margin
}

// extend moves the deadline to from+ttl, never back.
func (l *lease) extend(from, ttl time.Duration) {
	next := int64(from + ttl)
	for {
		cur := l.deadline.Load()
		if next <= cur || l.deadline.CompareAndSwap(cur, next) {
			return
		}
	}
}

func (n *Node) newLease(c store.Copy, from time.Duration) *lease {
	l := &lease{copy: c, margin: n.opts.LeaseMargin, clock: n.clock}
	l.deadline.Store(int64(from + n.opts.LeaseTTL))
	return l
}

// leaseList returns the copies this node holds, by shard.
func (n *Node) leaseList() []*lease {
	n.leaseMu.Lock()
	defer n.leaseMu.Unlock()
	out := make([]*lease, 0, len(n.leases))
	for _, l := range n.leases {
		out = append(out, l)
	}
	slices.SortFunc(out, func(a, b *lease) int { return cmpShard(a.copy.Shard, b.copy.Shard) })
	return out
}

func (n *Node) leaseCount() int {
	n.leaseMu.Lock()
	defer n.leaseMu.Unlock()
	return len(n.leases)
}

func (n *Node) leaseFor(id store.ShardID) *lease {
	n.leaseMu.Lock()
	defer n.leaseMu.Unlock()
	return n.leases[id]
}

func (n *Node) dropLease(id store.ShardID) {
	n.leaseMu.Lock()
	defer n.leaseMu.Unlock()
	delete(n.leases, id)
}

// heartbeat registers the node or refreshes its registration.
func (n *Node) heartbeat(ctx context.Context) error {
	err := n.reg.Heartbeat(ctx, store.Node{ID: n.id, Address: n.cfg.AdvertiseAddress, Version: n.opts.Version, Capacity: n.opts.Capacity})
	n.NoteDB(err)
	return err
}

// leaseLoop renews the leases every HeartbeatInterval. It waits on nothing else (the
// heartbeat has a loop of its own): a lease's safety depends on it alone.
func (n *Node) leaseLoop(ctx context.Context) {
	t := time.NewTicker(n.opts.HeartbeatInterval)
	defer t.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if gap := time.Since(last); gap > 3*n.opts.HeartbeatInterval {
			n.log.WarnContext(ctx, "the lease loop fell behind", slog.Duration("gap", gap))
		}
		last = time.Now()
		n.renew(ctx)
	}
}

// heartbeatLoop heartbeats every HeartbeatInterval.
func (n *Node) heartbeatLoop(ctx context.Context) {
	t := time.NewTicker(n.opts.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := n.heartbeat(ctx); err != nil && ctx.Err() == nil {
			n.log.WarnContext(ctx, "heartbeat failed", slog.Any("error", err))
		}
	}
}

// allocLoop runs the allocator every HeartbeatInterval.
func (n *Node) allocLoop(ctx context.Context) {
	t := time.NewTicker(n.opts.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if n.draining.Load() {
			continue
		}
		if err := n.refreshView(ctx); err != nil {
			continue
		}
		if err := n.allocate(ctx, "", false); err != nil && ctx.Err() == nil {
			n.log.WarnContext(ctx, "allocation pass failed", slog.Any("error", err))
		}
	}
}

// renew extends every lease this node holds. The local deadlines move to the clock's
// reading before the call plus the TTL; a copy whose lease the store did not renew
// (it expired, and maybe another node took the slot) is stopped at once.
func (n *Node) renew(ctx context.Context) {
	// Only the copies held before the renewal ran are judged by its result: one
	// claimed meanwhile is not in this list.
	leases := n.leaseList()
	if len(leases) == 0 {
		return
	}
	before := n.clock.Now()
	renewed, err := n.reg.RenewLeases(ctx, n.id, n.opts.LeaseTTL)
	n.NoteDB(err)
	if err != nil {
		if ctx.Err() == nil {
			n.inst.lease(ctx, "renew_failed")
			n.log.WarnContext(ctx, "renewing the leases failed; the copies stop serving at their deadlines unless a renewal succeeds", slog.Any("error", err))
		}
		return
	}
	if took := n.clock.Now() - before; took > n.opts.LeaseTTL/4 {
		n.log.WarnContext(ctx, "renewing the leases was slow: the database is overloaded or far", slog.Duration("took", took),
			slog.Duration("lease_ttl", n.opts.LeaseTTL))
	}
	held := map[store.ShardID]bool{}
	for _, id := range renewed {
		held[id] = true
	}
	for _, l := range leases {
		if held[l.copy.Shard] {
			l.extend(before, n.opts.LeaseTTL)
			n.inst.lease(ctx, "renew")
			continue
		}
		n.log.WarnContext(ctx, "a lease was not renewed: it expired; stopping the copy", slog.String("shard", l.copy.Shard.String()))
		n.inst.lease(ctx, "expire")
		n.loseCopy(ctx, l)
	}
}

// loseCopy stops a copy whose lease is (or may be) lost: it stops serving, its tailer
// stops and its shard closes; the directory is kept, so a later claim resumes it.
func (n *Node) loseCopy(ctx context.Context, l *lease) {
	if cur := n.leaseFor(l.copy.Shard); cur != l {
		return
	}
	n.dropLease(l.copy.Shard)
	if err := n.UnhostCopy(context.WithoutCancel(ctx), l.copy, false); err != nil {
		n.log.WarnContext(ctx, "closing a copy whose lease was lost failed", slog.String("shard", l.copy.Shard.String()), slog.Any("error", err))
	}
}

// leaseWatchdog stops every copy whose lease has run out by its local deadline, at
// once: the copy has already stopped serving (lease.valid), and its tailer must not
// write on.
func (n *Node) leaseWatchdog(ctx context.Context) {
	period := min(50*time.Millisecond, max(time.Millisecond, n.opts.LeaseMargin/2))
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		for _, l := range n.leaseList() {
			if !l.valid() {
				n.log.WarnContext(ctx, "a lease ran out by the local clock; stopping the copy", slog.String("shard", l.copy.Shard.String()))
				n.inst.lease(ctx, "expire")
				n.loseCopy(ctx, l)
			}
		}
	}
}

// view is the registry as last read: the nodes and every shard's copies.
type view struct {
	at     time.Time
	nodes  map[string]store.Node
	live   map[string]bool
	copies map[store.ShardID][]store.Copy
}

// liveNodes lists the live nodes' ids, sorted.
func (v *view) liveNodes() []string {
	out := make([]string, 0, len(v.live))
	for id := range v.live {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// usable reports whether a copy is held under a live lease by a live node.
func (v *view) usable(c *store.Copy) bool {
	return v.live[c.NodeID] && c.LeaseLeft > 0
}

// servingElsewhere counts the serving copies of id on live nodes other than self.
func (v *view) servingElsewhere(id store.ShardID, self string) int {
	k := 0
	for i := range v.copies[id] {
		c := &v.copies[id][i]
		if c.NodeID != self && c.State == store.CopyServing && v.usable(c) {
			k++
		}
	}
	return k
}

// refreshView reads the registry.
func (n *Node) refreshView(ctx context.Context) error {
	nodes, err := n.reg.Nodes(ctx)
	if err != nil {
		n.NoteDB(err)
		return err
	}
	copies, err := n.reg.Copies(ctx, "")
	n.NoteDB(err)
	if err != nil {
		return err
	}
	v := &view{at: time.Now(), nodes: map[string]store.Node{}, live: map[string]bool{n.id: true}, copies: map[store.ShardID][]store.Copy{}}
	for _, nd := range nodes {
		v.nodes[nd.ID] = nd
		if nd.HeartbeatAge < n.opts.DeadAfter {
			v.live[nd.ID] = true
		}
	}
	for _, c := range copies {
		v.copies[c.Shard] = append(v.copies[c.Shard], c)
	}
	n.view.Store(v)
	n.inst.nodes.Store(int64(len(v.live)))
	return nil
}

// viewLoop re-reads the registry every ViewInterval, for routing.
func (n *Node) viewLoop(ctx context.Context) {
	t := time.NewTicker(n.opts.ViewInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := n.refreshView(ctx); err != nil && ctx.Err() == nil && !errors.Is(err, context.Canceled) {
			n.log.DebugContext(ctx, "reading the registry failed", slog.Any("error", err))
		}
	}
}

// catalogLoop syncs the index catalogue every CatalogInterval, and forgets the leases
// of indexes dropped meanwhile (the engine has stopped their copies).
func (n *Node) catalogLoop(ctx context.Context) {
	t := time.NewTicker(n.opts.CatalogInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if err := n.SyncCatalog(ctx); err != nil && ctx.Err() == nil {
			n.log.DebugContext(ctx, "syncing the catalogue failed", slog.Any("error", err))
		}
		n.forgetDropped()
	}
}

// forgetDropped drops the leases of copies whose index was dropped (or recreated as a
// new incarnation) meanwhile: the engine has stopped them, and the store deleted their
// rows. A copy merely unhosted (retiring, before Stop releases it) keeps its lease.
func (n *Node) forgetDropped() {
	n.allocMu.Lock()
	defer n.allocMu.Unlock()
	known := map[string]bool{}
	for _, iv := range n.Indexes() {
		known[iv.Name] = true
	}
	for _, l := range n.leaseList() {
		if !known[l.copy.Shard.Index] {
			n.dropLease(l.copy.Shard)
		}
	}
}

func cmpShard(a, b store.ShardID) int {
	switch {
	case a.Index < b.Index:
		return -1
	case a.Index > b.Index:
		return 1
	}
	return a.Shard - b.Shard
}
