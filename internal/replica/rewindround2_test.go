package replica

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/store"
)

// TestHeadBehindOnceKeepsAHaltedCopyHalted: one poll that reads a head behind a halted
// copy's seq neither clears the halt nor promotes the copy: its registry row stays
// recovering.
func TestHeadBehindOnceKeepsAHaltedCopyHalted(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		const damaged = `{"title":"damaged"}`
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "c", testMapping)
		id := ShardID{Index: "c", Shard: 0}
		cp, ok, err := st.Registry().ClaimCopy(ctx, id, "node-a", 1, time.Minute)
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		fs := newFaultStore(d.open(t))
		fs.corrupt = func(c *store.Change) bool { return string(c.Payload) == damaged }
		opts := testOptions()
		opts.Copy = &cp
		opts.HaltRetryBase, opts.HaltRetryCap = 20*time.Millisecond, 50*time.Millisecond
		c := newCopy(t, fs, id, opts)
		c.start()
		c.waitApplied(mustApply(t, st, upsert("c", 0, "ok1", docBody("ok1", 1))))
		mustApply(t, st, upsert("c", 0, "bad", damaged))
		for deadline := time.Now().Add(30 * time.Second); c.current().Halt() == nil; {
			if time.Now().After(deadline) {
				t.Fatal("the copy never halted")
			}
			c.current().Wake()
			time.Sleep(5 * time.Millisecond)
		}
		waitCopyState(t, st, id, store.CopyRecovering)
		applied := c.current().Applied()
		var polls atomic.Int32
		fs.mu.Lock()
		fs.onTip = func(tip *store.Tip) {
			if polls.Add(1) == 1 {
				tip.Head = applied - 1
			}
		}
		fs.mu.Unlock()
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
			if c.current().Halt() == nil {
				t.Fatal("one lagging read cleared the halt")
			}
			copies, err := st.Registry().Copies(ctx, "c")
			if err == nil && len(copies) == 1 && copies[0].State == store.CopyServing {
				t.Fatal("one lagging read promoted the halted copy to serving")
			}
			time.Sleep(2 * time.Millisecond)
		}
		if polls.Load() < 2 {
			t.Fatalf("only %d polls in two seconds", polls.Load())
		}
	})
}
