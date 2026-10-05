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
	if hr, ok := c.tailer.(HaltReporter); ok {
		if err := hr.HaltErr(); err != nil {
			info.State, info.Error = api.ShardHalted, err.Error()
		}
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
	if c.paused.Load() {
		info.State = api.ShardServing // this node's own last-resort reads only
	}
	if info.State == api.ShardServing {
		info.Rebuilding = c.rebuilding()
		info.Stale = info.Rebuilding || c.lapsed() || c.trailing(n.cfg.MaxLag)
	}
	return info
}

// Shards implements [api.Coordinator].
func (n *Single) Shards(context.Context) ([]api.ShardInfo, error) {
	var out []api.ShardInfo
	for _, idx := range n.sortedIndexes() {
		for _, c := range idx.copies() {
			out = append(out, n.copyInfo(c))
		}
	}
	return out, nil
}

// Health implements [api.Coordinator]: on one node every shard has its one copy, so
// the cluster is green while every copy serves current data, yellow while a copy
// serves stale data (it trails the changelog by more than max_lag, or is being rebuilt
// aside), and red when one is recovering or halted (that shard has no serving copy).
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
			if shards[i].Stale && h.Status == api.StatusGreen {
				h.Status = api.StatusYellow // serving, but behind or being rebuilt
			}
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
// within max_lag. A copy that trails the changelog by more than max_lag, or is being
// rebuilt aside, marks reads stale and health yellow, but leaves the node ready (a
// node under a heavy bulk must not flap); one that halts or re-recovers later shows
// in health (red): the node still serves every other shard.
func (n *Single) Ready(context.Context) error {
	n.mu.RLock()
	closed := n.closed
	n.mu.RUnlock()
	if closed {
		return api.Unavailable(store.ErrClosed, "the node is shutting down")
	}
	if since := n.clock.Since(time.Unix(0, n.dbOK.Load())); since > n.cfg.MaxLag {
		return api.Unavailable(store.ErrClosed, "the database has not answered for %s (max_lag %s)", since.Round(time.Millisecond), n.cfg.MaxLag)
	}
	var waiting []error
	for _, idx := range n.sortedIndexes() {
		for _, c := range idx.copies() {
			if c.startup && !c.startedUp() {
				waiting = append(waiting, fmt.Errorf("%s is still recovering", c.id))
			}
		}
	}
	if len(waiting) > 0 {
		return api.Unavailable(errors.Join(waiting...), "%d shard copies have not finished their startup recovery", len(waiting))
	}
	return nil
}

// haltErr is why the copy's tailer has halted it, nil when it has not.
func haltErr(c *copyState) error {
	if hr, ok := c.tailer.(HaltReporter); ok {
		return hr.HaltErr()
	}
	return nil
}
