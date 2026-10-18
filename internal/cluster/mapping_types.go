package cluster

import (
	"context"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/store"
)

// A mapping is decoded by every node: its tailers apply it, its recoveries and bundles
// carry it in their manifests. A node of a release from before point fields cannot
// decode one holding a point type (store.MappingTypes 0): its copies would halt at the
// mapping change, and a rollback to it would find manifests it cannot open. So a
// mapping that adds a point field is refused while such a node is live, or holds a
// copy under a live lease: the same fence the compressed-body switch keeps (codecs.go).
// Each node heartbeats the types it reads; an old node's row stays at 0.

// AdmitFields implements node.Cluster: a point field among fields is refused (409)
// while a node that reads no point type is live or holds a copy, naming those nodes.
// The registry is read afresh, so a node upgraded a moment ago counts as upgraded.
func (h *clusterHooks) AdmitFields(ctx context.Context, fields map[string]schema.FieldType) error {
	var points []string
	for name, t := range fields {
		if t.IsPoint() {
			points = append(points, name)
		}
	}
	if len(points) == 0 {
		return nil
	}
	n := h.n
	nodes, err := n.reg.Nodes(ctx)
	if err != nil {
		return api.Unavailable(err, "the node registry cannot be read to check that every node reads point fields")
	}
	old := olderMappingReaders(nodes, n.view.Load(), n.opts.DeadAfter+n.opts.LeaseTTL)
	if len(old) == 0 {
		return nil
	}
	slices.Sort(points)
	return api.Conflict(api.CodeConflict,
		"field %s is a point field, which nodes of a release before point fields cannot read, and %s still serve: upgrade every node first (a point field cannot be removed, so once one exists no node can be downgraded below this release)",
		strings.Join(points, ", "), strings.Join(old, ", "))
}

// olderMappingReaders lists, sorted, the nodes that read no point type and are live
// (heard from within fence) or hold a copy under a live lease in v, each as
// "id (version)"; a copy's node the registry does not list counts as one.
func olderMappingReaders(nodes []store.Node, v *view, fence time.Duration) []string {
	byID := make(map[string]store.Node, len(nodes))
	for i := range nodes {
		byID[nodes[i].ID] = nodes[i]
	}
	old := map[string]string{}
	name := func(id string) string {
		nd, ok := byID[id]
		switch {
		case !ok:
			return id + " (unregistered)"
		case nd.Version == "":
			return id
		}
		return id + " (" + nd.Version + ")"
	}
	for i := range nodes {
		if nd := &nodes[i]; nd.MappingTypes < store.MappingTypes && nd.HeartbeatAge <= fence {
			old[nd.ID] = name(nd.ID)
		}
	}
	if v != nil {
		for _, list := range v.copies {
			for i := range list {
				c := &list[i]
				if nd, ok := byID[c.NodeID]; !c.Expired() && (!ok || nd.MappingTypes < store.MappingTypes) {
					old[c.NodeID] = name(c.NodeID)
				}
			}
		}
	}
	return slices.Sorted(maps.Values(old))
}
