package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/store/storetest"
)

// upgradedDB is d's database as a rolling upgrade from 1.x finds it once its first new
// node has migrated it: schema v6 first, then this binary's migrations, which leave
// shard sequences off.
func upgradedDB(t *testing.T, d *db) *db {
	t.Helper()
	if d.dialect == "sqlite" {
		d = &db{dialect: "sqlite", url: storetest.SQLiteURL(filepath.Join(t.TempDir(), "upgraded.db"))}
	}
	ctx := context.Background()
	st, err := store.Open(ctx, d.url, store.WithLogger(quietLogger), store.WithSchema(6))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate to v6: %v", err)
	}
	return d
}

// TestShardSeqGate: a rolling upgrade from a release before shard sequences. While a
// node that heartbeats no seq scheme (a 1.x binary) is live, the new nodes commit by
// the global sequence and the shard_seqs feature stays off; once it has been silent
// past the fence time the leader turns shard sequences on, and the writes go on. A
// token from before the switch still reads back on every node, as does one from after,
// and a node started afterwards learns of it as it starts.
func TestShardSeqGate(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		d = upgradedDB(t, d)
		old := d.open(t)
		t.Cleanup(func() { _ = old.Close() })
		beat := func() error {
			return old.Registry().Heartbeat(ctx, store.Node{ID: "node-old", Address: "127.0.0.1:1", Version: "v1.8.0", BodyCodecs: store.BodyCodecs, MappingTypes: store.MappingTypes})
		}
		if err := beat(); err != nil {
			t.Fatal(err)
		}
		stopOld := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			tick := time.NewTicker(200 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-stopOld:
					return
				case <-tick.C:
					_ = beat()
				}
			}
		})

		c := newCluster(t, d, nil)
		n0 := c.start(0).n
		createIndex(t, n0, "g", 3, 1)
		before := mustWrite(t, n0, "g", upsertOp("a", 1), upsertOp("b", 2), upsertOp("c", 3), upsertOp("d", 4))
		time.Sleep(time.Second)
		if n0.shardSeqs.Load() {
			t.Fatal("shard sequences on while a node from before them is live")
		}
		if on, err := old.ShardSeqs(ctx); err != nil || on {
			t.Fatalf("shard sequences while a node from before them is live: %v %v", on, err)
		}
		seen := map[int64]bool{}
		for _, seq := range before.Seqs {
			if seen[seq] {
				t.Fatalf("two shards hold seq %d while shard sequences are off: %v", seq, before.Seqs)
			}
			seen[seq] = true
		}

		close(stopOld)
		wg.Wait()
		silent := time.Now()
		eventually(t, 30*time.Second, "the gate opens", func() error {
			if !n0.shardSeqs.Load() {
				return errors.New("still on the global sequence")
			}
			return nil
		})
		fence := n0.opts.DeadAfter + n0.opts.LeaseTTL
		if since := time.Since(silent); since < fence-500*time.Millisecond {
			t.Fatalf("the gate opened %s after the old node went silent, before the fence time %s", since, fence)
		}
		if f, err := old.Registry().Features(ctx); err != nil || f[store.FeatureShardSeqs].IsZero() {
			t.Fatalf("features: %v %v", f, err)
		}
		after := mustWrite(t, n0, "g", upsertOp("e", 5), upsertOp("f", 6), upsertOp("a", 7))
		for sh, seq := range after.Seqs {
			if b := before.Seq(sh); seq <= b {
				t.Fatalf("shard %d took seq %d after the switch, at or below %d before it", sh, seq, b)
			}
		}

		n1 := c.start(1).n
		if !n1.shardSeqs.Load() {
			t.Fatal("a node started after the switch has not seen it")
		}
		c.waitHealth(t, "green", 30*time.Second)
		for _, n := range []*Node{n0, n1} {
			if got, err := count(ctx, n, "g", before); err != nil || got < 4 {
				t.Errorf("%s counts %d waiting for the token from before the switch: %v", n.id, got, err)
			}
			if got, err := count(ctx, n, "g", merged(before, after)); err != nil || got != 6 {
				t.Errorf("%s counts %d waiting for both tokens: %v", n.id, got, err)
			}
		}
	})
}

// TestOldNodeAfterTheSwitchIsNotLive: once shard sequences are on, a node that
// heartbeats no seq scheme (a 1.x node revived after the switch) is not live, even
// with the lowest id: it does not lead (so 2.0 nodes keep pruning), and reads are not
// routed to it.
func TestOldNodeAfterTheSwitchIsNotLive(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		c := newCluster(t, d, nil)
		n0 := c.start(0).n
		if !n0.shardSeqs.Load() {
			t.Fatal("shard sequences are off on a new database")
		}
		old := d.open(t)
		t.Cleanup(func() { _ = old.Close() })
		if err := old.Registry().Heartbeat(ctx, store.Node{ID: "0-old", Address: "127.0.0.1:1", Version: "v1.8.0", BodyCodecs: store.BodyCodecs, MappingTypes: store.MappingTypes}); err != nil {
			t.Fatal(err)
		}
		if err := n0.refreshView(ctx); err != nil {
			t.Fatal(err)
		}
		v := n0.view.Load()
		if _, ok := v.nodes["0-old"]; !ok {
			t.Fatal("the old node is not in the view")
		}
		if v.live["0-old"] {
			t.Error("a node with no seq scheme is live after the switch")
		}
		if !n0.leader() {
			t.Errorf("%s does not lead beside an old node with a lower id", n0.id)
		}
	})
}

// TestPercolatedAtCoversAPeersCopy: a saved query written through the node holding its
// shard's only copy, and a percolation through another node, which reads that copy: the
// percolation's percolated_at names the generation the peer percolated at, so it covers
// the saved query it matched, though the percolating node wrote no saved query there.
func TestPercolatedAtCoversAPeersCopy(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		c := newCluster(t, d, nil)
		a := c.start(0)
		b := c.start(1)
		if _, err := a.n.CreateIndex(tctx(t), "pa", api.IndexSpec{Settings: api.IndexSettings{Shards: 1, ReplicasPerShard: 1}}); err != nil {
			t.Fatal(err)
		}
		waitCopies(t, a.st, "pa", 1, 1, time.Minute)
		_, holders := serving(t, a.st, store.ShardID{Index: "pa"})
		holder, other := a, b
		if holders[0] == b.cfg.NodeID {
			holder, other = b, a
		}
		mustWrite(t, holder.n, "pa", upsertOp("first", 1))
		if _, err := count(tctx(t), other.n, "pa", nil); err != nil { // the other node opens the index before the saved query
			t.Fatal(err)
		}
		q := mustWrite(t, holder.n, "pa", api.WriteOp{Kind: api.OpQueryUpsert, ID: "q", Query: json.RawMessage(`{"field": "price", "op": "exists"}`)})
		req := &api.PercolateRequest{Docs: []json.RawMessage{json.RawMessage(`{"price": 1}`)}}
		if _, err := holder.n.Percolate(tctx(t), "pa", req, api.ReadOptions{WaitFor: q}); err != nil {
			t.Fatal(err)
		}
		res, err := other.n.Write(tctx(t), "pa", []api.WriteOp{upsertOp("d", 1)}, api.WriteOptions{Percolate: true})
		if err != nil {
			t.Fatal(err)
		}
		if !res.Percolated || string(res.Items[0].Queries) != `["q"]` {
			t.Fatalf("the percolation through the other node: percolated %v, queries %s", res.Percolated, res.Items[0].Queries)
		}
		if got := res.PercolatedAt.Seq(0); got < q.Seq(0) {
			t.Errorf("percolated_at names shard 0 at %d, below the saved query it matched at %d", got, q.Seq(0))
		}
	})
}
