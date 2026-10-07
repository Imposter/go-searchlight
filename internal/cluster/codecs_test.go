package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/store"
)

// TestBodyCodecGate: a rolling upgrade from binaries without codecs. While a node
// that heartbeats no codecs (an old binary) is live, the new nodes write bodies as
// text and the zstd_bodies feature stays off; once it has been silent past the fence
// time (DeadAfter plus LeaseTTL) the leader turns the feature on and the nodes write
// compressed bodies. A node started afterwards learns of it as it starts. Documents
// written either way read back from the store and from a copy recovered from a peer.
func TestBodyCodecGate(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		old := d.open(t)
		t.Cleanup(func() { _ = old.Close() })
		beat := func() error {
			return old.Registry().Heartbeat(ctx, store.Node{ID: "node-old", Address: "127.0.0.1:1", Version: "v1"})
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
		createIndex(t, n0, "g", 1, 2)
		mustWrite(t, n0, "g", bigOp("a", 1), bigOp("b", 2))
		time.Sleep(time.Second)
		if n0.zstdBodies.Load() {
			t.Fatal("compressing bodies while a node without codecs is live")
		}
		if f, err := old.Registry().Features(ctx); err != nil || len(f) != 0 {
			t.Fatalf("features while a node without codecs is live: %v %v", f, err)
		}

		close(stopOld)
		wg.Wait()
		silent := time.Now()
		eventually(t, 30*time.Second, "the gate opens", func() error {
			if !n0.zstdBodies.Load() {
				return errors.New("still writing text")
			}
			return nil
		})
		fence := n0.opts.DeadAfter + n0.opts.LeaseTTL
		if since := time.Since(silent); since < fence-500*time.Millisecond {
			t.Fatalf("the gate opened %s after the old node went silent, before the fence time %s", since, fence)
		}
		if f, err := old.Registry().Features(ctx); err != nil || f[store.FeatureZstdBodies].IsZero() {
			t.Fatalf("features: %v %v", f, err)
		}
		seq := mustWrite(t, n0, "g", bigOp("c", 3), bigOp("a", 4))

		n1 := c.start(1).n
		if !n1.zstdBodies.Load() {
			t.Fatal("a node started after the gate opened writes text")
		}
		c.waitHealth(t, "green", 30*time.Second)
		seq = max(seq, mustWrite(t, n1, "g", bigOp("d", 5)))
		want := map[string]int{"a": 4, "b": 2, "c": 3, "d": 5}
		for _, n := range []*Node{n0, n1} {
			if got, err := count(ctx, n, "g", seq); err != nil || got != int64(len(want)) {
				t.Fatalf("%s counts %d, %v", n.id, got, err)
			}
			for id, v := range want {
				doc, err := n.GetDocument(ctx, "g", id)
				if err != nil {
					t.Fatalf("%s get %s: %v", n.id, id, err)
				}
				var body struct{ Price int }
				if err := json.Unmarshal(doc.Body, &body); err != nil || body.Price != v {
					t.Fatalf("%s get %s: %s, %v", n.id, id, doc.Body, err)
				}
			}
		}
	})
}

// bigOp is an upsert of a document long and repetitive enough that it is always
// stored compressed once compression is on.
func bigOp(id string, v int) api.WriteOp {
	desc := strings.Repeat(fmt.Sprintf("item %s in stock and ready to ship ", id), 20)
	return api.WriteOp{Kind: api.OpUpsert, ID: id, Body: json.RawMessage(fmt.Sprintf(`{"title":"item %s","description":%q,"price":%d}`, id, desc, v))}
}
