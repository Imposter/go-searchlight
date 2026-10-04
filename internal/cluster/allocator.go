package cluster

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/store"
)

// The allocator. Every node runs it on every heartbeat tick, over every shard of
// every index:
//
//   - A shard this node holds a copy of keeps it, unless the index's copy target is
//     lower than the copy's slot and at least target other copies serve: the extra
//     slot is released (a lowered target).
//   - A shard below its target (live copies, not counting retiring ones) is claimed by
//     an eligible node: one without a copy of it, with spare capacity. The least
//     loaded eligible nodes (fewest copies) claim first; the others wait three ticks
//     before they try too, so a node that is gone or stuck does not leave the shard
//     under target. The claim is the store's conditional insert-or-steal of a free
//     slot below the target (or of one whose lease expired), so racing allocators
//     never place more copies than the target. While copies retire, the target the
//     claim may fill grows by their number, so replacements are placed before the
//     retiring copies are released.
//   - A target of 0 means every node holds every shard: every node claims a copy of
//     every shard, and none is released.
//
// A node that held a copy before it restarted finds its own slot in the registry and
// claims it back (the store keeps its epoch), resuming the copy from its directory.

// allocState is the allocator's memory between passes.
type allocState struct {
	mu sync.Mutex
	// waiting is when a shard was first seen below target while this node was not
	// among the least loaded eligible nodes.
	waiting map[store.ShardID]time.Time
}

// everyNode is the claim target for "every node holds every shard": more slots than
// any cluster has nodes.
const everyNode = api.MaxReplicas

// allocate runs one allocation pass over every index, or over only. startup marks the
// copies it takes as ones readiness waits for. eager claims without waiting for less
// loaded nodes (a new index, so it serves when CreateIndex answers).
func (n *Node) allocate(ctx context.Context, only string, startup bool) error {
	return n.allocatePass(ctx, only, startup, false)
}

func (n *Node) allocatePass(ctx context.Context, only string, startup, eager bool) error {
	if n.draining.Load() || n.stopped.Load() {
		return nil
	}
	n.allocMu.Lock()
	defer n.allocMu.Unlock()
	v := n.view.Load()
	var errs []error
	for _, iv := range n.Indexes() {
		if only != "" && iv.Name != only {
			continue
		}
		for s := range iv.Shards {
			if err := n.allocateShard(ctx, v, iv, store.ShardID{Index: iv.Name, Shard: s}, startup, eager); err != nil {
				errs = append(errs, err)
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}
	return errors.Join(errs...)
}

// allocateShard claims, keeps or releases this node's copy of id.
func (n *Node) allocateShard(ctx context.Context, v *view, iv node.IndexView, id store.ShardID, startup, eager bool) error {
	if l := n.leaseFor(id); l != nil {
		return n.maybeRelease(ctx, v, iv, l)
	}
	target := iv.ReplicasPerShard
	mine, live, retiring := false, 0, 0
	for i := range v.copies[id] {
		c := &v.copies[id][i]
		if c.NodeID == n.id {
			mine = true // a slot this node held before it restarted: claim it back
			continue
		}
		if v.usable(c) {
			live++
			if c.State == store.CopyRetiring {
				retiring++
			}
		}
	}
	if !mine {
		if target > 0 && live-retiring >= target {
			n.alloc.clear(id)
			return nil
		}
		if n.opts.Capacity > 0 && n.leaseCount() >= n.opts.Capacity {
			return nil
		}
		if target > 0 && !eager && !n.firstInLine(v, id) && !n.alloc.waited(id, 3*n.opts.HeartbeatInterval) {
			return nil
		}
	}
	claim := everyNode
	if target > 0 {
		claim = target + retiring
	}
	before := n.clock.Now()
	c, ok, err := n.reg.ClaimCopy(ctx, id, n.id, claim, n.opts.LeaseTTL)
	n.NoteDB(err)
	switch {
	case errors.Is(err, store.ErrNotFound):
		return nil // dropped meanwhile
	case err != nil:
		return err
	case !ok:
		return nil // the target was met meanwhile
	}
	n.alloc.clear(id)
	if c.State == store.CopyRetiring {
		// This node's own slot, left retiring by a shutdown that could not release
		// it: it recovers again (the tailer never promotes a retiring copy).
		if err := n.reg.SetCopyState(ctx, c, store.CopyRecovering); err != nil {
			return err
		}
		c.State = store.CopyRecovering
	}
	l := n.newLease(c, before)
	n.leaseMu.Lock()
	n.leases[id] = l
	n.leaseMu.Unlock()
	n.inst.lease(ctx, "claim")
	n.inst.allocation(ctx, id, string(c.State))
	if err := n.HostCopy(ctx, node.HostSpec{Copy: c, LeaseValid: l.valid, Fetcher: n.fetch, Startup: startup}); err != nil {
		n.dropLease(id)
		if rerr := n.reg.ReleaseCopy(ctx, c); rerr == nil {
			n.inst.lease(ctx, "release")
		}
		return err
	}
	n.log.InfoContext(ctx, "shard copy claimed", slog.String("shard", id.String()), slog.Int("slot", c.Slot),
		slog.Int64("epoch", c.Epoch), slog.String("state", string(c.State)))
	return nil
}

// maybeRelease releases this node's copy when the index's target fell below its slot
// and at least target other copies serve.
func (n *Node) maybeRelease(ctx context.Context, v *view, iv node.IndexView, l *lease) error {
	target := iv.ReplicasPerShard
	if target == 0 || l.copy.Slot < target || v.servingElsewhere(l.copy.Shard, n.id) < target {
		return nil
	}
	n.log.InfoContext(ctx, "releasing an extra shard copy: the copy target was lowered", slog.String("shard", l.copy.Shard.String()),
		slog.Int("slot", l.copy.Slot), slog.Int("target", target))
	n.dropLease(l.copy.Shard)
	if err := n.UnhostCopy(ctx, l.copy.Shard, true); err != nil {
		n.log.WarnContext(ctx, "closing a released copy failed", slog.Any("error", err))
	}
	if err := n.reg.ReleaseCopy(ctx, l.copy); err != nil && !errors.Is(err, store.ErrLeaseLost) {
		return err
	}
	n.inst.lease(ctx, "release")
	return nil
}

// firstInLine reports whether this node is among the least loaded eligible nodes for
// a copy of id: live, without a copy of id, with spare capacity.
func (n *Node) firstInLine(v *view, id store.ShardID) bool {
	load := map[string]int{}
	holds := map[string]bool{}
	for sid, list := range v.copies {
		for i := range list {
			c := &list[i]
			if !v.usable(c) {
				continue
			}
			load[c.NodeID]++
			if sid == id {
				holds[c.NodeID] = true
			}
		}
	}
	mine := n.leaseCount()
	for nodeID := range v.live {
		if nodeID == n.id || holds[nodeID] {
			continue
		}
		if capacity := v.nodes[nodeID].Capacity; capacity > 0 && load[nodeID] >= capacity {
			continue
		}
		if load[nodeID] < mine {
			return false
		}
	}
	return true
}

// waited reports whether id has been waiting for a claim for at least d, noting when
// it started waiting.
func (a *allocState) waited(id store.ShardID, d time.Duration) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.waiting == nil {
		a.waiting = map[store.ShardID]time.Time{}
	}
	since, ok := a.waiting[id]
	if !ok {
		a.waiting[id] = time.Now()
		return false
	}
	return time.Since(since) >= d
}

func (a *allocState) clear(id store.ShardID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.waiting, id)
}
