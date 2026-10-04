package cluster

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/store"
)

// Health (spec sections 10 and 11), as Elasticsearch defines it:
//
//   - a shard is green when all its copies serve current data and it has its target
//     of them (replicas_per_shard, or every live node when that is 0);
//   - yellow when it has fewer serving copies than its target, or one serves stale data
//     (it trails the changelog by more than max_lag, or is being rebuilt aside), but at
//     least one serves;
//   - red when no copy serves.
//
// The cluster is the worst of its shards. Readiness is the single node's: the database
// answered within max_lag, the node has joined (its first allocation is done) and the
// copies it took as it started have finished their startup recovery. Copies that lag,
// or that it took later (a new replica recovering), make reads stale and health
// yellow, never the node unready.

// shardHealth is one shard's state as health sees it.
type shardHealth struct {
	serving, target int
	stale           bool
}

// shardStates gathers every shard's copies: this node's from the engine (exactly),
// the others' from the registry.
func (n *Node) shardStates() (map[store.ShardID]*shardHealth, int) {
	v := n.view.Load()
	local := map[store.ShardID]bool{}
	states := map[store.ShardID]*shardHealth{}
	liveNodes := len(v.live)
	for _, iv := range n.Indexes() {
		target := iv.ReplicasPerShard
		if target == 0 {
			target = liveNodes
		}
		for s := range iv.Shards {
			states[store.ShardID{Index: iv.Name, Shard: s}] = &shardHealth{target: target}
		}
	}
	for _, lc := range n.LocalCopies() { //nolint:gocritic // a short list
		id := store.ShardID{Index: lc.Info.Index, Shard: lc.Info.Shard}
		local[id] = true
		h := states[id]
		if h == nil || !lc.Serving {
			continue
		}
		h.serving++
		if lc.Info.Stale {
			h.stale = true
		}
	}
	for id, list := range v.copies {
		h := states[id]
		if h == nil {
			continue
		}
		for i := range list {
			c := &list[i]
			if c.NodeID == n.id || c.State != store.CopyServing || !v.usable(c) {
				continue
			}
			h.serving++
		}
	}
	return states, liveNodes
}

// Health implements [api.Coordinator].
func (n *Node) Health(context.Context) (*api.ClusterHealth, error) {
	states, live := n.shardStates()
	h := &api.ClusterHealth{Status: api.StatusGreen, Nodes: live, Indexes: len(n.Indexes()), Shards: len(states)}
	for _, s := range states {
		switch {
		case s.serving == 0:
			h.Unassigned++
			h.Status = api.StatusRed
		case s.serving < s.target || s.stale:
			h.ServingShards++
			if h.Status == api.StatusGreen {
				h.Status = api.StatusYellow
			}
		default:
			h.ServingShards++
		}
	}
	return h, nil
}

// Nodes implements [api.Coordinator]: the live nodes, read from the registry.
func (n *Node) Nodes(ctx context.Context) ([]api.NodeInfo, error) {
	if err := n.refreshView(ctx); err != nil && n.view.Load().at.IsZero() {
		return nil, api.Unavailable(err, "the registry cannot be read")
	}
	v := n.view.Load()
	out := make([]api.NodeInfo, 0, len(v.live))
	for _, id := range v.liveNodes() {
		nd, ok := v.nodes[id]
		if !ok && id == n.id {
			nd = store.Node{ID: n.id, Address: n.cfg.AdvertiseAddress, Version: n.opts.Version}
		}
		out = append(out, api.NodeInfo{ID: id, Address: nd.Address, Version: nd.Version, Self: id == n.id})
	}
	return out, nil
}

// Shards implements [api.Coordinator]: every live copy in the cluster. This node's
// copies are described from the engine; the others' by their nodes (over the peer
// API), or from the registry when a node does not answer.
func (n *Node) Shards(ctx context.Context) ([]api.ShardInfo, error) {
	v := n.view.Load()
	var out []api.ShardInfo
	for _, lc := range n.LocalCopies() { //nolint:gocritic // a short list
		info := lc.Info
		if l := n.leaseFor(store.ShardID{Index: info.Index, Shard: info.Shard}); l != nil && l.retired.Load() {
			info.State = api.ShardRetiring
		}
		out = append(out, info)
	}
	remote := n.peerCopies(ctx, v)
	for id, list := range v.copies {
		for i := range list {
			c := &list[i]
			if c.NodeID == n.id || !v.usable(c) {
				continue
			}
			info, ok := remote[copyID{node: c.NodeID, shard: id}]
			if !ok {
				info = api.ShardInfo{Index: id.Index, Shard: id.Shard, Node: c.NodeID, AppliedSeq: c.AppliedSeq, CommittedSeq: c.AppliedSeq}
			}
			switch c.State {
			case store.CopyRetiring:
				info.State = api.ShardRetiring
			case store.CopyRecovering:
				if info.State == "" || info.State == api.ShardServing {
					info.State = api.ShardRecovering
				}
			default:
				if info.State == "" {
					info.State = api.ShardServing
				}
			}
			info.Node = c.NodeID
			out = append(out, info)
		}
	}
	slices.SortFunc(out, func(a, b api.ShardInfo) int {
		if c := cmpShard(store.ShardID{Index: a.Index, Shard: a.Shard}, store.ShardID{Index: b.Index, Shard: b.Shard}); c != 0 {
			return c
		}
		switch {
		case a.Node < b.Node:
			return -1
		case a.Node > b.Node:
			return 1
		}
		return 0
	})
	return out, nil
}

type copyID struct {
	node  string
	shard store.ShardID
}

// peerCopies asks every live peer for its copies, briefly.
func (n *Node) peerCopies(ctx context.Context, v *view) map[copyID]api.ShardInfo {
	out := map[copyID]api.ShardInfo{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, id := range v.liveNodes() {
		nd, ok := v.nodes[id]
		if id == n.id || !ok || nd.Address == "" {
			continue
		}
		wg.Go(func() {
			cctx, cancel := context.WithTimeout(ctx, time.Second)
			defer cancel()
			var reply copiesReply
			if err := n.call(cctx, id, nd.Address, http.MethodGet, peerPrefix+"copies", nil, &reply); err != nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			for _, c := range reply.Copies {
				out[copyID{node: id, shard: store.ShardID{Index: c.Index, Shard: c.Shard}}] = c
			}
		})
	}
	wg.Wait()
	return out
}

// Ready implements [api.Coordinator] (see Health above).
func (n *Node) Ready(ctx context.Context) error {
	if n.stopped.Load() {
		return api.Unavailable(store.ErrClosed, "the node is shutting down")
	}
	if !n.started.Load() {
		return api.Unavailable(errors.New("starting"), "the node has not joined the cluster yet")
	}
	return n.Single.Ready(ctx)
}
