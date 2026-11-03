package cluster

import (
	"context"
	"log/slog"
	"time"

	"github.com/Imposter/go-searchlight/internal/store"
)

// Shard sequences (schema v7) come on only once no node from before them is live:
// such a node takes its seqs from the global sequence and reads changes by seq alone,
// so until then every node commits through the global sequence, as it does
// (store/upgrade.go). Each node heartbeats how it takes seqs (store.SeqScheme; an old
// node's row stays at 0). The leader turns them on, for good, once every registered
// node heartbeats the scheme or has been silent past the fence time, and no node that
// does not holds a copy under a live lease: the same test the zstd_bodies gate makes
// (codecs.go). The switch makes an old node's writes fail rather than land outside
// the shard sequences, should one still be running. Nodes learn of it at their next
// write.

// seedEvery is how often the leader looks for indexes without sequence rows once shard
// sequences are on.
const seedEvery = 30 * time.Second

// shardSeqGate turns shard sequences on, on the leader, once every node commits by them.
// Once they are on, the leader gives an index a 1.x node created after the switch (with
// no sequence rows) its rows, every seedEvery, ahead of its first write.
func (n *Node) shardSeqGate(ctx context.Context) error {
	if n.shardSeqs.Load() {
		if !n.leader() || n.clock.Since(n.lastSeed) < seedEvery {
			return nil
		}
		n.lastSeed = n.clock.Now()
		return n.st.SeedMissingShards(ctx)
	}
	on, err := n.st.ShardSeqs(ctx)
	if err != nil {
		return err
	}
	if !on {
		v := n.view.Load()
		if !n.leader() || !n.everyNode(v, func(nd store.Node) bool { return nd.SeqScheme >= store.SeqScheme }) {
			return nil
		}
		if err := n.st.EnableShardSeqs(ctx); err != nil {
			return err
		}
		n.log.InfoContext(ctx, "every node commits by shard sequences: turned the cluster's shard_seqs feature on",
			slog.Int("nodes", len(v.nodes)))
	}
	n.shardSeqs.Store(true)
	return nil
}
