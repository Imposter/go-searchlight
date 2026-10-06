package replica

import (
	"context"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// TestLoadHaltPrunedPast: a copy whose snapshot load halts at a bad row the changelog
// has been pruned past reloads once for the prune, then waits on the row itself rather
// than reloading at every halt backoff, and gets past it once the row changes.
func TestLoadHaltPrunedPast(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "h", `{"dynamic":"strict","fields":{"title":"text"}}`)
		id := ShardID{Index: "h", Shard: 0}
		mustApply(t, st, upsert("h", 0, "ok1", `{"title":"one"}`))
		mustApply(t, st, upsert("h", 0, "bad", `{"title":"x","_id":"y"}`))
		head := mustApply(t, st, upsert("h", 0, "ok2", `{"title":"two"}`))
		if err := st.Prune(ctx, id, head+1); err != nil {
			t.Fatal(err)
		}

		opts, reader := meteredOptions()
		opts.HaltRetryBase, opts.HaltRetryCap = 20*time.Millisecond, 50*time.Millisecond
		c := newCopy(t, d.open(t), id, opts)
		c.withFakeClock(opts.HaltRetryCap)
		c.start()
		c.until("the load halts", func() bool { return c.current().Halt() != nil })
		if h := c.current().Halt(); h.ID != "bad" || h.Reason != ReasonDocument {
			t.Fatalf("load halt %+v", h)
		}
		c.until("the halted load is retried", func() bool {
			return counterSum(t, reader, telemetry.MetricReplicaHalts, "", "") >= 8
		})
		if n := counterSum(t, reader, telemetry.MetricReplicaRecoveries, "", ""); n > 2 {
			t.Fatalf("%d snapshot loads while the bad row is current; want the first and one for the prune", n)
		}

		head = mustApply(t, st, store.Change{Index: "h", Shard: 0, Kind: store.KindDelete, ID: "bad"})
		sh := c.waitApplied(head)
		if c.tailer.Halt() != nil {
			t.Fatalf("still halted: %+v", c.tailer.Halt())
		}
		if dd := diff(viewOf(t, sh), truthOf(t, st, id), false); dd != "" {
			t.Fatalf("recovered copy differs:\n%s", dd)
		}
	})
}
