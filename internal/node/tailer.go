package node

import (
	"context"
	"hash/fnv"

	"github.com/Imposter/go-searchlight/internal/replica"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// Tailer keeps one shard copy in step with the changelog: it is the copy's only
// writer. The node commits writes to the store, then wakes the tailers of the shards
// it wrote; reads wait on the shard for the seq they need.
//
// It is the part of the replica tailer (replica.Tailer) the node uses; ReplicaTailers
// is the production [NewTailerFunc], and nodetest has one tests can pause.
type Tailer interface {
	// Run applies the changelog until ctx ends (it then returns nil) or it must
	// stop (it returns why). A copy the replica tailer halts does not stop Run
	// (HaltReporter says so); a simpler tailer's Run may return on a halt.
	Run(ctx context.Context) error
	// Applied is the newest seq the copy has applied.
	Applied() int64
	// Wake asks the tailer to read the changelog now rather than at its next poll.
	Wake()
	// Shard is the copy's current shard: a tailer may replace it with a reopened or
	// rebuilt one while it recovers, so readers always go through Shard.
	Shard() *shard.Shard
}

// Copy states a [StateReporter] names; replica.State's String gives these.
const (
	StateIdle       = "idle"
	StateRecovering = "recovering"
	StateTailing    = "tailing"
	StateHalted     = "halted"
	StateRebuilding = "rebuilding"
)

// StateReporter is implemented by a tailer that can say where its copy is: one of
// the State* names. The node shows a recovering copy as yellow and a halted one as red,
// and is not ready while either exists. The replica tailer reports its state as
// State().String(); wire it with a one-method wrapper. A tailer without it is tailing
// while Run runs and halted once Run returns on its own.
type StateReporter interface {
	StateName() string
}

// TailerEnv is what the node offers a tailer beyond the store and the shard.
type TailerEnv struct {
	// Head is the newest seq this node has committed or seen applied. A tailer that
	// cannot ask the store for its head (store.HeadSeq) may advance an idle copy to
	// it: every change at or below it is committed. The replica tailer ignores it.
	Head func() int64
	// Copy is the registry copy the tailer reports for (a cluster node): its applied
	// seq and state, fenced by its epoch. Nil on a single node.
	Copy *store.Copy
	// Fetcher, when set, brings the copy's files from a serving peer when it must be
	// rebuilt, before the store's snapshot is tried.
	Fetcher replica.Fetcher
}

// NewTailerFunc makes the tailer of one shard copy: the node's one seam for its
// tailer: ReplicaTailers in production, nodetest.NewTailer in tests that pause a copy.
type NewTailerFunc func(st store.Store, sh *shard.Shard, id store.ShardID, env TailerEnv) Tailer

// ShardFor is the shard of an index with n shards that a document or saved query id
// belongs to: FNV-1a of the id, modulo n. Every node routes the same way, forever, so
// it never changes.
func ShardFor(id string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % uint32(n)) //nolint:gosec // n is a shard count, at most api.MaxShards
}
