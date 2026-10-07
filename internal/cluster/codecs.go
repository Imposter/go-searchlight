package cluster

import (
	"context"
	"log/slog"

	"github.com/Imposter/go-searchlight/internal/store"
)

// Compressed bodies (schema v2) are written only once every node reads them, so a
// rolling upgrade from a binary without codecs never hands an old node a row it
// cannot read. Each node heartbeats the codecs it reads (store.BodyCodecs; an old
// node's row stays at 0). The leader turns store.FeatureZstdBodies on, for good, once
// every registered node reads them or has been silent past the fence time, DeadAfter
// plus LeaseTTL: its leases have run out by then, so it serves and tails nothing
// unless it comes back and claims a copy again. Nodes learn of the feature as they
// refresh their view of the registry, and from then on store document bodies
// compressed. A node that has not learned of it yet writes text, which every node
// reads, so the switch needs no coordination.

// bodyCodecGate turns compressed bodies on for this node once the cluster has turned
// them on, and on the leader turns them on for the cluster when every node reads
// them. Until the feature is on it reads sl_features each time it is called.
func (n *Node) bodyCodecGate(ctx context.Context) error {
	if n.zstdBodies.Load() {
		return nil
	}
	features, err := n.reg.Features(ctx)
	if err != nil {
		return err
	}
	if _, on := features[store.FeatureZstdBodies]; !on {
		v := n.view.Load()
		if !n.leader() || !n.everyNodeReadsCodecs(v) {
			return nil
		}
		if err := n.reg.EnableFeature(ctx, store.FeatureZstdBodies); err != nil {
			return err
		}
		n.log.InfoContext(ctx, "every node reads compressed bodies: turned the cluster's zstd_bodies feature on",
			slog.Int("nodes", len(v.nodes)))
	}
	n.st.CompressBodies(true)
	n.zstdBodies.Store(true)
	n.log.InfoContext(ctx, "storing document bodies compressed")
	return nil
}

// everyNodeReadsCodecs reports whether every node v registers reads compressed bodies
// or has been silent past the fence time.
func (n *Node) everyNodeReadsCodecs(v *view) bool {
	fence := n.opts.DeadAfter + n.opts.LeaseTTL
	for _, nd := range v.nodes {
		if nd.BodyCodecs < store.BodyCodecs && nd.HeartbeatAge <= fence {
			return false
		}
	}
	return true
}
