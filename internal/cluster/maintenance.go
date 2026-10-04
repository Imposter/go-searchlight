package cluster

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/store"
)

// Maintenance: the live node with the lowest id (the leader) prunes the changelog and
// sweeps abandoned blob uploads. Leadership needs no election: every node reads the
// same registry, and both jobs are idempotent and safe when two nodes briefly both
// believe they lead.
//
// # Pruning (spec section 9)
//
// A shard's changelog is pruned below its floor: the lowest applied seq (the durable
// CommittedSeq each copy reports) over its live copies, those held under an unexpired
// lease by a live node, whatever their state. Recovery points are covered by the same
// rule: a copy being recovered holds its slot from the claim on, at applied seq 0 (or,
// rebuilt aside, at the seq its old copy reached, below the snapshot it fetches) until
// its recovered copy reports its seq; and the snapshot a peer serves is at or past the
// seq that peer's copy reports. So neither loses a change it will replay. A shard with
// no live copy is not pruned: whichever copy comes back next needs its log.
//
// A halted copy must not hold the floor back forever. The bounded policy: before each
// pass the leader asks every live node for its copies' states (the peer API's copies
// listing; its own it reads directly), and a copy its node has reported halted at
// every pass for PruneHaltGrace (15 minutes by default) no longer counts towards the
// floor. A halted copy re-recovers on its own (after its halt backoff it rebuilds, from
// a peer or the store's snapshot, neither of which needs the pruned changes); if the
// log is pruned past it first, its next read says so (store.ErrPruned) and it
// rebuilds. A copy that stops being halted counts again at once. A copy whose node
// does not answer is counted as it is: pruning only waits for it.

// pruneState is the leader's memory of which copies have been halted since when.
type pruneState struct {
	mu     sync.Mutex
	halted map[haltKey]time.Time
}

type haltKey struct {
	shard store.ShardID
	node  string
}

// leader reports whether this node is the live node with the lowest id.
func (n *Node) leader() bool {
	live := n.view.Load().liveNodes()
	return len(live) > 0 && live[0] == n.id
}

// maintenanceLoop prunes every PruneInterval and sweeps every SweepInterval, while this
// node leads.
func (n *Node) maintenanceLoop(ctx context.Context) {
	prune := time.NewTicker(n.opts.PruneInterval)
	defer prune.Stop()
	sweep := time.NewTicker(n.opts.SweepInterval)
	defer sweep.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-prune.C:
			if n.leader() {
				if err := n.pruneAll(ctx); err != nil && ctx.Err() == nil {
					n.log.WarnContext(ctx, "pruning the changelog failed", slog.Any("error", err))
				}
			}
		case <-sweep.C:
			if n.leader() {
				removed, err := n.st.Blobs().Sweep(ctx, n.opts.SweepAge)
				switch {
				case err != nil && ctx.Err() == nil:
					n.log.WarnContext(ctx, "sweeping abandoned blob uploads failed", slog.Any("error", err))
				case removed > 0:
					n.log.InfoContext(ctx, "swept abandoned blob uploads", slog.Int("removed", removed))
				}
			}
		}
	}
}

// pruneAll prunes every shard's changelog below its floor.
func (n *Node) pruneAll(ctx context.Context) error {
	if err := n.refreshView(ctx); err != nil {
		return err
	}
	v := n.view.Load()
	n.prune.observe(n.haltedCopies(ctx, v), time.Now())
	for _, iv := range n.Indexes() {
		for s := range iv.Shards {
			id := store.ShardID{Index: iv.Name, Shard: s}
			floor, ok := n.pruneFloor(v.copies[id], v)
			if !ok || floor <= 0 {
				continue
			}
			if err := n.st.Prune(ctx, id, floor+1); err != nil {
				return err
			}
		}
	}
	return nil
}

// pruneFloor is the seq a shard's changelog may be pruned up to (inclusive): the
// lowest applied seq of its live copies, long-halted ones aside. ok is false when the
// shard has no live copy that counts.
func (n *Node) pruneFloor(copies []store.Copy, v *view) (int64, bool) {
	floor, ok := int64(0), false
	for i := range copies {
		c := &copies[i]
		if !v.usable(c) || n.prune.longHalted(haltKey{shard: c.Shard, node: c.NodeID}, n.opts.PruneHaltGrace) {
			continue
		}
		if !ok || c.AppliedSeq < floor {
			floor, ok = c.AppliedSeq, true
		}
	}
	return floor, ok
}

// haltedCopies asks every live node which of its copies are halted: this node's
// directly, the others over the peer API. A node that does not answer reports none.
func (n *Node) haltedCopies(ctx context.Context, v *view) map[haltKey]bool {
	out := map[haltKey]bool{}
	for _, lc := range n.LocalCopies() { //nolint:gocritic // a short list
		if lc.Info.State == api.ShardHalted {
			out[haltKey{shard: store.ShardID{Index: lc.Info.Index, Shard: lc.Info.Shard}, node: n.id}] = true
		}
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, id := range v.liveNodes() {
		nd, ok := v.nodes[id]
		if id == n.id || !ok || nd.Address == "" {
			continue
		}
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			var reply copiesReply
			if err := n.call(cctx, id, nd.Address, http.MethodGet, peerPrefix+"copies", nil, &reply); err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, c := range reply.Copies {
				if c.State == api.ShardHalted {
					out[haltKey{shard: store.ShardID{Index: c.Index, Shard: c.Shard}, node: id}] = true
				}
			}
		})
	}
	wg.Wait()
	return out
}

// observe records which copies are halted now: a copy halted at every pass since it
// was first seen halted keeps that time; one no longer halted is forgotten.
func (p *pruneState) observe(halted map[haltKey]bool, now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.halted == nil {
		p.halted = map[haltKey]time.Time{}
	}
	for k := range p.halted {
		if !halted[k] {
			delete(p.halted, k)
		}
	}
	for k := range halted {
		if _, ok := p.halted[k]; !ok {
			p.halted[k] = now
		}
	}
}

// longHalted reports whether copy k has been halted for at least grace.
func (p *pruneState) longHalted(k haltKey, grace time.Duration) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	since, ok := p.halted[k]
	return ok && time.Since(since) >= grace
}
