package cluster

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/store"
)

// leaseClock is how lease deadlines are kept, on the node's clock.Clock. Now is the
// time elapsed since the node's epoch by the monotonic clock: it never goes back. Wall
// is the wall clock, which keeps running while the machine sleeps (a monotonic clock
// may not: Linux's stops during suspend).
type leaseClock struct {
	c     clock.Clock
	epoch time.Time
}

func (c leaseClock) Now() time.Duration { return c.c.Since(c.epoch) }
func (c leaseClock) Wall() time.Time    { return c.c.Wall() }

// lease is a copy this node holds, and what the node knows of its lease.
//
// Its local deadline is taken by the node's own clocks: the clocks' readings just
// before the call that granted or renewed the lease, plus the lease's TTL. The database
// starts the TTL later (when it runs the statement), so its lease_until is never
// earlier than this deadline, by the database's clock. The lease is held for sure
// while both the monotonic and the wall clock say less than TTL less the margin has
// passed since that reading (the wall clock covers a suspended machine, whose monotonic
// clock may stand still; the margin covers the clocks' rate difference over one TTL).
//
// A lease is in one of three states:
//
//   - held: renewals succeed; the copy serves everyone.
//   - lapsed: the local deadline passed without a renewal (the database is
//     unreachable, or slow): the copy's tailer is paused (it writes nothing), it
//     serves no peer, and this node reads it only as a last resort, stale. A renewal or
//     a re-claim of the same slot at the same epoch resumes it at once.
//   - lost: confirmed (the store renewed the other leases but not this one and a
//     re-claim did not get it back, a registry write was fenced off, or the registry
//     shows another holder): the copy is unhosted.
//
// A copy whose claim took the slot over from another node (whose lease expired by the
// database's clock) is also quarantined: it serves nothing until TTL plus margin have
// passed since the claim by this node's own clock, so even a database clock that
// stepped forward cannot make it serve while the previous holder's copy still does.
// The quarantine belongs to the slot at that epoch, not to the lease: a copy dropped
// locally and claimed back at the same epoch (the store then names no previous holder)
// stays quarantined until the takeover's quarantine ends.
type lease struct {
	copy       store.Copy
	deadline   atomic.Int64 // leaseClock.Now nanoseconds
	wallBefore atomic.Int64 // leaseClock.Wall Unix nanoseconds of the last grant's start
	ttl        time.Duration
	margin     time.Duration
	clock      leaseClock
	quarantine time.Duration // leaseClock.Now until which the copy serves nothing; 0: none
	// claimed is when the claim that granted the lease had returned, committed, by
	// leaseClock.Now: a view of the registry whose read began before then may predate the
	// claim, so it cannot judge it.
	claimed time.Duration
	// retired is set once the copy is marked retiring.
	retired atomic.Bool

	// mu serializes the copy's transitions (pause, resume, loss).
	mu     sync.Mutex
	paused bool
	lost   bool
}

// valid reports whether the lease surely holds: by both clocks, less than TTL less the
// margin has passed since the last grant began.
func (l *lease) valid() bool {
	return l.validAt(l.clock.Now(), l.clock.Wall())
}

// validAt is valid at the leaseClock readings now and wall.
func (l *lease) validAt(now time.Duration, wall time.Time) bool {
	if now >= time.Duration(l.deadline.Load())-l.margin {
		return false
	}
	return wall.Sub(time.Unix(0, l.wallBefore.Load())) < l.ttl-l.margin
}

// quarantined reports whether the copy may not serve yet: it took over another node's
// slot less than TTL plus margin ago, by this node's clock.
func (l *lease) quarantined() bool {
	return l.quarantinedAt(l.clock.Now())
}

// quarantinedAt is quarantined at the leaseClock reading now: the copy serves from the
// quarantine's end on, not before.
func (l *lease) quarantinedAt(now time.Duration) bool {
	return l.quarantine > 0 && now < l.quarantine
}

// extend moves the lease to a grant that began at from (monotonic) and wall, never
// back.
func (l *lease) extend(from time.Duration, wall time.Time) {
	next := int64(from + l.ttl)
	for {
		cur := l.deadline.Load()
		if next <= cur || l.deadline.CompareAndSwap(cur, next) {
			break
		}
	}
	w := wall.UnixNano()
	for {
		cur := l.wallBefore.Load()
		if w <= cur || l.wallBefore.CompareAndSwap(cur, w) {
			return
		}
	}
}

func (n *Node) newLease(c store.Copy, from time.Duration, wall time.Time, claimed time.Duration) *lease {
	l := &lease{copy: c, ttl: n.opts.LeaseTTL, margin: n.opts.LeaseMargin, clock: n.lc, claimed: claimed}
	l.deadline.Store(int64(from + l.ttl))
	l.wallBefore.Store(wall.UnixNano())
	l.quarantine = n.quarantineFor(c, from+l.ttl+l.margin)
	return l
}

// quarantine is a takeover's quarantine: the slot at epoch serves nothing before until
// (leaseClock.Now).
type quarantine struct {
	epoch int64
	until time.Duration
}

// quarantineFor is the leaseClock.Now until which a claim of c may not serve (0: none):
// stolenUntil for a slot taken over from another node, else what is left of the
// quarantine of the takeover that gave this node the slot at c's epoch.
func (n *Node) quarantineFor(c store.Copy, stolenUntil time.Duration) time.Duration {
	n.leaseMu.Lock()
	defer n.leaseMu.Unlock()
	now := n.lc.Now()
	for id, q := range n.quarantines {
		if now >= q.until {
			delete(n.quarantines, id)
		}
	}
	if c.TakenFrom != "" && c.TakenFrom != n.id {
		n.quarantines[c.Shard] = quarantine{epoch: c.Epoch, until: stolenUntil}
		return stolenUntil
	}
	q, ok := n.quarantines[c.Shard]
	if !ok {
		return 0
	}
	if q.epoch != c.Epoch {
		delete(n.quarantines, c.Shard)
		return 0
	}
	return q.until
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

func (n *Node) dropLease(l *lease) {
	n.leaseMu.Lock()
	defer n.leaseMu.Unlock()
	if n.leases[l.copy.Shard] == l {
		delete(n.leases, l.copy.Shard)
	}
}

// heartbeat registers the node or refreshes its registration.
func (n *Node) heartbeat(ctx context.Context) error {
	err := n.reg.Heartbeat(ctx, store.Node{
		ID: n.id, Address: n.cfg.AdvertiseAddress, Version: n.opts.Version, Capacity: n.opts.Capacity,
		BodyCodecs: store.BodyCodecs,
	})
	n.NoteDB(err)
	return err
}

// leaseLoop renews the leases every HeartbeatInterval. It waits on nothing else (the
// heartbeat has a loop of its own): a lease's safety depends on it alone.
func (n *Node) leaseLoop(ctx context.Context) {
	t := n.clock.NewTicker(n.opts.HeartbeatInterval)
	defer t.Stop()
	last := n.clock.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
		}
		if gap := n.clock.Since(last); gap > 3*n.opts.HeartbeatInterval {
			n.log.WarnContext(ctx, "the lease loop fell behind", slog.Duration("gap", gap))
		}
		last = n.clock.Now()
		n.renew(ctx)
	}
}

// heartbeatLoop heartbeats every HeartbeatInterval.
func (n *Node) heartbeatLoop(ctx context.Context) {
	t := n.clock.NewTicker(n.opts.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
		}
		if err := n.heartbeat(ctx); err != nil && ctx.Err() == nil {
			n.log.WarnContext(ctx, "heartbeat failed", slog.Any("error", err))
		}
	}
}

// allocLoop runs the allocator every HeartbeatInterval.
func (n *Node) allocLoop(ctx context.Context) {
	t := n.clock.NewTicker(n.opts.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
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

// renew extends every lease this node holds. The local deadlines move to the clocks'
// readings before the call plus the TTL. A lease the store did not renew (it expired
// by the database's clock) is claimed again: the same slot at the same epoch resumes
// the copy, anything else is a confirmed loss.
func (n *Node) renew(ctx context.Context) {
	// Only the copies held before the renewal ran are judged by its result: one
	// claimed meanwhile is not in this list.
	leases := n.leaseList()
	if len(leases) == 0 {
		return
	}
	before, wall := n.lc.Now(), n.lc.Wall()
	renewed, err := n.reg.RenewLeases(ctx, n.id, n.opts.LeaseTTL)
	n.NoteDB(err)
	if err != nil {
		if ctx.Err() == nil {
			n.inst.lease(ctx, "renew_failed")
			n.log.WarnContext(ctx, "renewing the leases failed; a copy past its deadline pauses (no peer reads it) until a renewal succeeds", slog.Any("error", err))
		}
		return
	}
	if took := n.lc.Now() - before; took > n.opts.LeaseTTL/4 {
		n.log.WarnContext(ctx, "renewing the leases was slow: the database is overloaded or far", slog.Duration("took", took),
			slog.Duration("lease_ttl", n.opts.LeaseTTL))
	}
	held := map[store.ShardID]bool{}
	for _, id := range renewed {
		held[id] = true
	}
	for _, l := range leases {
		if held[l.copy.Shard] {
			l.extend(before, wall)
			n.inst.lease(ctx, "renew")
			n.resumeIfPaused(ctx, l)
			continue
		}
		n.reclaim(ctx, l)
	}
}

// reclaim claims a lease the store did not renew. The same slot at the same epoch (no
// other node took it meanwhile) holds again, and a paused copy resumes; anything else
// is a confirmed loss, and a slot the claim took over at a new epoch keeps its
// quarantine for the allocator's claim back. A failure to reach the store leaves the
// lease as it is.
func (n *Node) reclaim(ctx context.Context, l *lease) {
	before, wall := n.lc.Now(), n.lc.Wall()
	c, ok, err := n.reg.ClaimCopy(ctx, l.copy.Shard, n.id, n.claimTarget(l.copy.Shard), n.opts.LeaseTTL)
	n.NoteDB(err)
	switch {
	case err != nil && !errors.Is(err, store.ErrNotFound):
		return
	case err == nil && ok && c.Slot == l.copy.Slot && c.Epoch == l.copy.Epoch:
		l.extend(before, wall)
		n.inst.lease(ctx, "reclaim")
		n.resumeIfPaused(ctx, l)
		return
	case err == nil && ok:
		n.quarantineFor(c, before+n.opts.LeaseTTL+n.opts.LeaseMargin)
	}
	n.log.WarnContext(ctx, "a lease expired and its slot could not be claimed back at the same epoch; dropping the copy",
		slog.String("shard", l.copy.Shard.String()), slog.Bool("claimed", ok), slog.Any("error", err))
	n.inst.lease(ctx, "lost")
	n.loseCopy(ctx, l)
	// A slot the claim did get (another one) is the allocator's to host: its next
	// pass claims it again and finds this node already holds it.
}

// claimTarget is the target a claim of id may fill: the index's copy target (every
// node: more slots than any cluster has nodes).
func (n *Node) claimTarget(id store.ShardID) int {
	for _, iv := range n.Indexes() {
		if iv.Name == id.Index && iv.ReplicasPerShard > 0 {
			return iv.ReplicasPerShard
		}
	}
	return everyNode
}

// pause stops a lapsed copy's tailer: it writes nothing and serves no peer, and stays
// open for this node's own last-resort reads.
func (n *Node) pause(ctx context.Context, l *lease) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.paused || l.lost {
		return
	}
	if err := n.PauseCopy(ctx, l.copy); err != nil {
		n.log.WarnContext(ctx, "pausing a lapsed copy failed", slog.String("shard", l.copy.Shard.String()), slog.Any("error", err))
		return
	}
	l.paused = true
	n.inst.lease(ctx, "lapse")
}

// resumeIfPaused restarts a paused copy whose lease holds again (renewed, or claimed
// back at the same epoch). It recovers from where it stopped, marked recovering until
// its tailer has caught up.
func (n *Node) resumeIfPaused(ctx context.Context, l *lease) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.paused || l.lost || !l.valid() {
		return
	}
	cp := l.copy
	if cp.State != store.CopyRecovering && !l.retired.Load() {
		if err := n.reg.SetCopyState(ctx, cp, store.CopyRecovering); err != nil {
			n.log.WarnContext(ctx, "marking a resumed copy recovering failed", slog.String("shard", cp.Shard.String()), slog.Any("error", err))
			return
		}
		cp.State = store.CopyRecovering
		l.copy.State = cp.State
	}
	if err := n.ResumeCopy(ctx, cp); err != nil {
		n.log.WarnContext(ctx, "resuming a copy failed; dropping it", slog.String("shard", cp.Shard.String()), slog.Any("error", err))
		l.lost = true
		n.dropLease(l)
		_ = n.unhostCopy(context.WithoutCancel(ctx), cp, false)
		return
	}
	l.paused = false
	n.inst.lease(ctx, "resume")
}

// loseCopy unhosts a copy whose lease is lost for sure; the directory is kept, so a
// later claim resumes it.
func (n *Node) loseCopy(ctx context.Context, l *lease) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost {
		return
	}
	l.lost = true
	n.dropLease(l)
	if err := n.unhostCopy(context.WithoutCancel(ctx), l.copy, false); err != nil {
		n.log.WarnContext(ctx, "closing a copy whose lease was lost failed", slog.String("shard", l.copy.Shard.String()), slog.Any("error", err))
	}
}

// unhostCopy unhosts a copy and forgets what the node cached of its files.
func (n *Node) unhostCopy(ctx context.Context, cp store.Copy, wipe bool) error {
	n.sums.drop(cp.Shard)
	return n.UnhostCopy(ctx, cp, wipe)
}

// leaseWatchdog pauses every copy whose lease lapses by the local clocks, at once (its
// peers already stopped reading it: lease.valid), and drops the copies the registry
// shows another holder of.
func (n *Node) leaseWatchdog(ctx context.Context) {
	period := min(50*time.Millisecond, max(time.Millisecond, n.opts.LeaseMargin/2))
	t := n.clock.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
		}
		v := n.view.Load()
		for _, l := range n.leaseList() {
			if v.takenOver(l) {
				n.log.WarnContext(ctx, "the registry shows another holder of a copy's slot; dropping the copy", slog.String("shard", l.copy.Shard.String()))
				n.inst.lease(ctx, "lost")
				n.loseCopy(ctx, l)
				continue
			}
			if !l.valid() {
				n.pause(ctx, l)
			}
		}
	}
}

// view is the registry as last read: the nodes and every shard's copies.
type view struct {
	at        time.Time
	readBegan time.Duration
	nodes     map[string]store.Node
	live      map[string]bool
	copies    map[store.ShardID][]store.Copy
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
	return v.servingBelow(id, self, -1)
}

// servingBelow counts the serving copies of id on live nodes other than self in slots
// below slot (any slot when it is negative).
func (v *view) servingBelow(id store.ShardID, self string, slot int) int {
	k := 0
	for i := range v.copies[id] {
		c := &v.copies[id][i]
		if c.NodeID != self && c.State == store.CopyServing && v.usable(c) && (slot < 0 || c.Slot < slot) {
			k++
		}
	}
	return k
}

// takenOver reports whether the registry, read after l's claim, shows its slot held by
// another node, or by another incarnation.
func (v *view) takenOver(l *lease) bool {
	if v.at.IsZero() || v.readBegan <= l.claimed {
		return false
	}
	c := &l.copy
	for i := range v.copies[c.Shard] {
		e := &v.copies[c.Shard][i]
		if e.Slot == c.Slot {
			return e.NodeID != c.NodeID || e.Epoch != c.Epoch
		}
	}
	return false
}

// refreshView reads the registry and publishes the view, unless a read that began
// later has published one already (reads run concurrently: the view loop, the
// allocator, routing misses).
func (n *Node) refreshView(ctx context.Context) error {
	readAt := n.lc.Now()
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
	v := &view{at: n.clock.Now(), readBegan: readAt, nodes: map[string]store.Node{}, live: map[string]bool{n.id: true}, copies: map[store.ShardID][]store.Copy{}}
	for _, nd := range nodes {
		v.nodes[nd.ID] = nd
		if nd.HeartbeatAge < n.opts.DeadAfter {
			v.live[nd.ID] = true
		}
	}
	for i := range copies {
		v.copies[copies[i].Shard] = append(v.copies[copies[i].Shard], copies[i])
	}
	// A copy the registry shows another holder of is dropped before the view is
	// published: no read routed by this view can fall back to it.
	for _, l := range n.leaseList() {
		if v.takenOver(l) {
			n.log.WarnContext(ctx, "the registry shows another holder of a copy's slot; dropping the copy", slog.String("shard", l.copy.Shard.String()))
			n.inst.lease(ctx, "lost")
			n.loseCopy(ctx, l)
		}
	}
	for {
		cur := n.view.Load()
		if cur != nil && cur.readBegan > v.readBegan {
			return nil
		}
		if n.view.CompareAndSwap(cur, v) {
			break
		}
	}
	n.inst.nodes.Store(int64(len(v.live)))
	return nil
}

// viewLoop re-reads the registry every ViewInterval, for routing.
func (n *Node) viewLoop(ctx context.Context) {
	t := n.clock.NewTicker(n.opts.ViewInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
		}
		err := n.refreshView(ctx)
		if err == nil {
			err = n.bodyCodecGate(ctx)
		}
		if err != nil && ctx.Err() == nil && !errors.Is(err, context.Canceled) {
			n.log.DebugContext(ctx, "reading the registry failed", slog.Any("error", err))
		}
	}
}

// catalogLoop syncs the index catalogue every CatalogInterval, and forgets the leases
// of indexes dropped meanwhile (the engine has stopped their copies).
func (n *Node) catalogLoop(ctx context.Context) {
	t := n.clock.NewTicker(n.opts.CatalogInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
		}
		if err := n.SyncCatalog(ctx); err != nil && ctx.Err() == nil {
			n.log.DebugContext(ctx, "syncing the catalogue failed", slog.Any("error", err))
		}
		n.forgetDropped()
	}
}

// forgetDropped drops the leases of copies whose index was dropped (or recreated as a
// new incarnation) meanwhile: the engine has stopped them, and the store deleted their
// rows. The allocator's waits on dropped incarnations go with them.
func (n *Node) forgetDropped() {
	n.allocMu.Lock()
	defer n.allocMu.Unlock()
	known, uids := map[string]bool{}, map[string]bool{}
	for _, iv := range n.Indexes() {
		known[iv.Name], uids[iv.UID] = true, true
	}
	for _, l := range n.leaseList() {
		if !known[l.copy.Shard.Index] {
			n.dropLease(l)
		}
	}
	n.alloc.forget(uids)
	n.leaseMu.Lock()
	defer n.leaseMu.Unlock()
	for id := range n.quarantines {
		if !known[id.Index] {
			delete(n.quarantines, id)
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
