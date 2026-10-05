package cluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// TestRecreatedIndexWaitsForItsCreator: a node that saw an index's shard unclaimed
// and began leaving it to the index's creator, then saw the index dropped and created
// again under the same name, waits the full creator wait for the new incarnation: the
// wait it began on the old one neither carries over nor survives the drop.
func TestRecreatedIndexWaitsForItsCreator(t *testing.T) {
	d := sqliteDB(t)
	st := d.open(t)
	t.Cleanup(func() { _ = st.Close() })
	clk := clock.NewFake(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC))
	cfg := config.Default()
	cfg.StoreURL, cfg.NodeID, cfg.DataDir = d.url, "waiter", t.TempDir()
	cfg.InsecureNoAuth, cfg.ClusterToken = true, testToken
	o := Options{Store: st, Config: cfg, Version: "test", Clock: clk, Logger: quietLogger, hooks: &testHooks{allowSQLiteCluster: true}}
	fastOptions(&o)
	o.Engine = func(eo *node.Options) {
		eo.Logger = quietLogger
		eo.ShardOptions = func(so *shard.Options) { so.Logger = quietLogger }
	}
	ctx := tctx(t)
	n, err := New(ctx, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Stop(context.Background()) })
	creatorWait := 3 * o.HeartbeatInterval

	settings, err := json.Marshal(api.IndexSettings{Shards: 1})
	if err != nil {
		t.Fatal(err)
	}
	create := func() string {
		t.Helper()
		m, err := st.Indexes().Create(ctx, store.IndexMeta{Name: "again", Mapping: []byte(`{}`), Settings: settings})
		if err != nil {
			t.Fatal(err)
		}
		if err := n.SyncCatalog(ctx); err != nil {
			t.Fatal(err)
		}
		return m.UID
	}
	claimed := func() bool {
		t.Helper()
		copies, err := st.Registry().Copies(ctx, "again")
		if err != nil {
			t.Fatal(err)
		}
		return len(copies) > 0
	}
	allocate := func() {
		t.Helper()
		if err := n.allocate(ctx, "", false); err != nil {
			t.Fatal(err)
		}
	}

	first := create()
	allocate()
	if claimed() {
		t.Fatal("an unclaimed shard was claimed before its creator's wait")
	}
	clk.Advance(2 * creatorWait)

	if err := st.Indexes().Drop(ctx, "again"); err != nil {
		t.Fatal(err)
	}
	if err := n.SyncCatalog(ctx); err != nil {
		t.Fatal(err)
	}
	n.forgetDropped()
	if waits := n.alloc.waiting; len(waits) != 0 {
		t.Fatalf("the dropped incarnation's waits survived the drop: %v", waits)
	}
	second := create()
	if second == first {
		t.Fatalf("the recreated index kept its incarnation %s", first)
	}
	allocate()
	if claimed() {
		t.Fatal("the recreated index's shard was claimed at once: the old incarnation's wait bypassed the creator's")
	}
	clk.Advance(creatorWait / 2)
	allocate()
	if claimed() {
		t.Fatal("the recreated index's shard was claimed before its own creator wait ran out")
	}
	clk.Advance(creatorWait)
	allocate()
	if !claimed() {
		t.Fatal("the shard was not claimed once the creator wait ran out")
	}
}
