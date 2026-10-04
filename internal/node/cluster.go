package node

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/replica"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/store"
)

// Cluster is how a node takes part in a cluster; package cluster implements it. With
// none (Options.Cluster nil) the node is a single node, hosting every shard's one copy.
type Cluster interface {
	// Remote returns a read target for shard id on a serving copy of another node,
	// which has every change up to waitSeq searchable before it answers. It returns
	// an error (a 503 *api.Error) when no other node has a copy to try; the target's
	// first call fails the same way once every copy it tried has failed.
	Remote(ctx context.Context, id store.ShardID, waitSeq int64) (ShardTarget, error)
	// Committed is called once a write committed changes to shards of index (each
	// written shard to the newest seq it took): it sends push hints to the peers
	// holding copies of them. It must not block.
	Committed(index string, shards map[int]int64)
	// WaitRefreshed waits, under ctx, until every serving copy of id on another node
	// has seq searchable; with api.RefreshTrue the copies refresh at once once they
	// have applied it. localDone says this node's copy has it: when it has not and no
	// other copy reached seq either, the wait fails.
	WaitRefreshed(ctx context.Context, id store.ShardID, seq int64, mode api.RefreshMode, localDone bool) error
	// Allocate claims and hosts this node's copies of index now, so a new index
	// serves when CreateIndex answers.
	Allocate(ctx context.Context, index string) error
	// CopyStopped reports that a hosted copy's tailer stopped on its own (its lease
	// was lost, it could not go on): the cluster unhosts it. c is the copy's registry
	// copy. It must not block.
	CopyStopped(c store.Copy, err error)
	// Counts returns the live documents and saved queries of a serving copy of id on
	// another node.
	Counts(ctx context.Context, id store.ShardID) (docs, queries uint64, err error)
}

// ShardTarget is the copy one read uses for one shard: this node's own, or a serving
// copy elsewhere. Every call honours ctx. Release it when the read is done.
type ShardTarget interface {
	// Search runs r on the copy's generation: the query phase only when r.NoBodies
	// (the generation is then held for Fetch), else hits with bodies.
	Search(ctx context.Context, r *search.Request) (*search.ShardResult, error)
	// Fetch fills hits' bodies (limited to fields when set) from the generation the
	// Search ran on. It fails with search.ErrStaleHit or ErrTargetLost when that
	// generation is gone: the read starts again.
	Fetch(ctx context.Context, hits []search.Hit, fields []string) error
	// Percolate matches docs, analyzed under mapping (its catalogue JSON), against the
	// copy's saved queries: the ids each document matches, in order.
	Percolate(ctx context.Context, mapping json.RawMessage, docs []schema.Doc) ([][]string, error)
	// Get reads a stored document from the copy.
	Get(ctx context.Context, id string) (body []byte, found bool, err error)
	// GetQuery reads a saved query from the copy.
	GetQuery(ctx context.Context, id string) (q *api.SavedQuery, found bool, err error)
	// Stale reports whether the copy's reads are stale: it trails the changelog by
	// more than max_lag, or its node cannot reach the database.
	Stale() bool
	// Release lets the copy drop the generation the read held.
	Release()
}

// ErrTargetLost is returned by a ShardTarget's Fetch when the copy its Search ran on
// can no longer be reached (its node went away between the phases): the read starts
// again on another copy.
var ErrTargetLost = errors.New("node: the copy a read was using went away")

// HostSpec is a shard copy the cluster gives this node.
type HostSpec struct {
	// Copy is the registry copy: its slot and fencing epoch, which the tailer names in
	// every registry write.
	Copy store.Copy
	// Held reports whether the copy's lease surely holds (by the node's own clocks):
	// while it does not, the copy serves no peer, takes part in no write's refresh
	// wait, and its reads on this node are stale (the node's last resort, when no
	// other copy answers).
	Held func() bool
	// Quarantined reports whether the copy may not serve at all yet: a slot taken
	// over from another node serves nothing until that node's lease has surely run
	// out by this node's clock.
	Quarantined func() bool
	// Fetcher brings the copy's files from a serving peer when it must be rebuilt.
	Fetcher replica.Fetcher
	// Startup marks a copy the node took as it started: readiness waits on it.
	Startup bool
}

// IndexView describes an index as the cluster allocates it.
type IndexView struct {
	Name string
	UID  string
	// Shards is the shard count, fixed at creation.
	Shards int
	// ReplicasPerShard is the copy target per shard; 0 means every node.
	ReplicasPerShard int
}
