package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/store"
)

// sortedIndexes returns the open indexes by name.
func (n *Single) sortedIndexes() []*index {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]*index, 0, len(n.indexes))
	for _, name := range slices.Sorted(maps.Keys(n.indexes)) {
		out = append(out, n.indexes[name])
	}
	return out
}

// copyInfo describes one copy.
func (n *Single) copyInfo(c *copyState) api.ShardInfo {
	info := api.ShardInfo{Index: c.id.Index, Shard: c.id.Shard, Node: n.cfg.NodeID, State: api.ShardServing}
	sh := c.shard()
	if sh != nil {
		info.AppliedSeq = sh.AppliedSeq()
		info.RefreshedSeq = sh.RefreshedSeq()
		info.CommittedSeq = sh.CommittedSeq()
		info.Lag = c.backlog()
		if g := sh.Acquire(); g != nil {
			info.Docs = g.NumDocs()
			g.Release()
		}
		if err := sh.Err(); err != nil {
			info.State, info.Error = api.ShardHalted, err.Error()
		}
	} else {
		info.State = api.ShardRecovering
	}
	if sr, ok := c.tailer.(StateReporter); ok && info.State == api.ShardServing {
		switch sr.StateName() {
		case StateRecovering, StateIdle:
			info.State = api.ShardRecovering
		case StateHalted:
			info.State = api.ShardHalted
		}
	}
	if h := c.halted.Load(); h != nil {
		info.State, info.Error = api.ShardHalted, (*h).Error()
	}
	return info
}

// Shards implements [api.Coordinator].
func (n *Single) Shards(context.Context) ([]api.ShardInfo, error) {
	var out []api.ShardInfo
	for _, idx := range n.sortedIndexes() {
		for _, c := range idx.copies {
			out = append(out, n.copyInfo(c))
		}
	}
	return out, nil
}

// Health implements [api.Coordinator]: on one node every shard has its one copy, so
// the cluster is green while every copy serves and red when one is recovering or
// halted (that shard has no serving copy). Yellow (a shard below its copy target with
// a serving copy) needs more nodes.
func (n *Single) Health(ctx context.Context) (*api.ClusterHealth, error) {
	shards, err := n.Shards(ctx)
	if err != nil {
		return nil, err
	}
	h := &api.ClusterHealth{Status: api.StatusGreen, Nodes: 1, Indexes: len(n.sortedIndexes()), Shards: len(shards)}
	for i := range shards {
		switch shards[i].State {
		case api.ShardServing:
			h.ServingShards++
		default:
			h.Unassigned++
			h.Status = api.StatusRed
		}
	}
	return h, nil
}

// Nodes implements [api.Coordinator].
func (n *Single) Nodes(context.Context) ([]api.NodeInfo, error) {
	return []api.NodeInfo{{ID: n.cfg.NodeID, Address: n.cfg.AdvertiseAddress, Version: n.opts.Version, Self: true}}, nil
}

// Ready implements [api.Coordinator] (spec section 10): the node is ready while it is
// open, every copy has finished its startup recovery, and the database answered
// within max_lag. A copy that halts or re-recovers later shows in health (red), not
// here: the node still serves every other shard.
func (n *Single) Ready(context.Context) error {
	n.mu.RLock()
	closed := n.closed
	n.mu.RUnlock()
	if closed {
		return api.Unavailable(store.ErrClosed, "the node is shutting down")
	}
	if since := time.Since(time.Unix(0, n.dbOK.Load())); since > n.cfg.MaxLag {
		return api.Unavailable(store.ErrClosed, "the database has not answered for %s (max_lag %s)", since.Round(time.Millisecond), n.cfg.MaxLag)
	}
	var waiting []error
	for _, idx := range n.sortedIndexes() {
		for _, c := range idx.copies {
			if !c.startedUp() {
				waiting = append(waiting, fmt.Errorf("%s is still recovering", c.id))
			}
		}
	}
	if len(waiting) > 0 {
		return api.Unavailable(errors.Join(waiting...), "%d shard copies have not finished their startup recovery", len(waiting))
	}
	return nil
}
