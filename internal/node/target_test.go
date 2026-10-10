package node_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/node/nodetest"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// restartingPeer is a cluster of this node and one peer that is restarting: no copy
// elsewhere answers. It hosts the node's copies itself.
type restartingPeer struct {
	n       *node.Single
	remotes atomic.Int32
}

func (p *restartingPeer) Remote(context.Context, store.ShardID, int64) (node.ShardTarget, error) {
	p.remotes.Add(1)
	return nil, api.Unavailable(errors.New("connection refused"), "the peer is restarting")
}

func (*restartingPeer) Committed(string, map[int]int64) {}

func (*restartingPeer) WaitRefreshed(context.Context, store.ShardID, int64, api.RefreshMode, bool) error {
	return nil
}

func (p *restartingPeer) Allocate(ctx context.Context, index string) error {
	return p.n.HostCopy(ctx, node.HostSpec{Copy: store.Copy{Shard: store.ShardID{Index: index}, NodeID: "n1", Epoch: 1}})
}

func (*restartingPeer) CopyStopped(store.Copy, error) {}

func (*restartingPeer) Counts(context.Context, store.ShardID) (uint64, uint64, error) {
	return 0, 0, errors.New("no peer")
}

// wakeHook runs fn at the after-th Wake of any copy's tailer once armed.
type wakeHook struct {
	mu    sync.Mutex
	after int
	fn    func()
}

func (h *wakeHook) arm(after int, fn func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.after, h.fn = after, fn
}

func (h *wakeHook) woken() {
	h.mu.Lock()
	var fn func()
	if h.fn != nil {
		h.after--
		if h.after == 0 {
			fn, h.fn = h.fn, nil
		}
	}
	h.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func (h *wakeHook) fired() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.fn == nil
}

// wakeHookTailer is the fake tailer, telling its hook of every Wake.
type wakeHookTailer struct {
	*nodetest.Tailer
	hook *wakeHook
}

func (t wakeHookTailer) Wake() {
	t.hook.woken()
	t.Tailer.Wake()
}

// TestReadFallsBackToTheCopyHostedInPlace (L1): a wait_for_seq read parked on this
// node's copy, which is unhosted and hosted again at a new epoch while the read waits,
// as the only peer restarts. The peer answers nothing, so the read is answered by the
// copy hosted in place of the closed one, not failed with the peer's 503. The swap runs
// at the read's second Wake of the copy: the first wakes every copy before the read
// takes one, the second is its wait on the copy it took.
func TestReadFallsBackToTheCopyHostedInPlace(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	peer := &restartingPeer{}
	hook := &wakeHook{}
	var mu sync.Mutex
	tailers := map[int64]*nodetest.Tailer{}
	n := open(t, cfg, st, func(o *node.Options) {
		o.Cluster = peer
		o.NewTailer = func(st store.Store, sh *shard.Shard, id store.ShardID, env node.TailerEnv) node.Tailer {
			tl := nodetest.NewTailer(st, sh, id, env).(*nodetest.Tailer) //nolint:forcetypeassert,errcheck // nodetest's own type
			if env.Copy != nil {
				mu.Lock()
				tailers[env.Copy.Epoch] = tl
				mu.Unlock()
			}
			return wakeHookTailer{Tailer: tl, hook: hook}
		}
	})
	peer.n = n
	if _, err := n.CreateIndex(ctx(t), "hp", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Fatal(err)
	}
	waitServing(t, n)
	mustWrite(t, n, "hp", upsert("a", `{"n": 1}`))
	mu.Lock()
	old := tailers[1]
	mu.Unlock()
	if old == nil {
		t.Fatal("no tailer for the copy at epoch 1")
	}
	old.Pause()
	last := mustWrite(t, n, "hp", upsert("late", `{"n": 2}`)).Seq

	id := store.ShardID{Index: "hp"}
	swap := func() {
		if err := n.UnhostCopy(context.Background(), store.Copy{Shard: id, NodeID: "n1", Epoch: 1}, false); err != nil {
			t.Error(err)
		}
		if err := n.HostCopy(context.Background(), node.HostSpec{Copy: store.Copy{Shard: id, NodeID: "n1", Epoch: 2}}); err != nil {
			t.Error(err)
		}
	}
	hook.arm(2, swap)
	resp, err := n.Search(ctx(t), "hp", &search.Request{Query: &query.All{}, Size: 10, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitForSeq: last})
	if !hook.fired() {
		t.Fatal("the read did not wait on the copy at epoch 1")
	}
	if err != nil {
		t.Fatalf("the read whose copy was hosted again in place, its peer restarting: %v", err)
	}
	if resp.Total != 2 {
		t.Fatalf("%d hits, want both documents", resp.Total)
	}
	if peer.remotes.Load() == 0 {
		t.Fatal("the read never asked for a copy elsewhere")
	}
	if c, ok := n.Hosted(id); !ok || c.Epoch != 2 {
		t.Fatalf("hosted %+v %v, want the copy at epoch 2", c, ok)
	}
}
