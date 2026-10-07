package replica

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// TestLoadHaltPrunedPast: a copy whose snapshot load halts at a bad row the changelog
// has been pruned past loads once, then waits on the row itself rather than reloading
// at every halt backoff, and gets past it once the row is deleted or written again.
func TestLoadHaltPrunedPast(t *testing.T) {
	for _, heal := range []string{"deleted", "rewritten"} {
		t.Run(heal, func(t *testing.T) {
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

				c, reader := haltedCopy(t, d, id)
				if h := c.current().Halt(); h.ID != "bad" || h.Reason != ReasonDocument {
					t.Fatalf("load halt %+v", h)
				}
				c.until("the halted load is retried", func() bool {
					return counterSum(t, reader, telemetry.MetricReplicaHalts, "", "") >= 8
				})
				if n := counterSum(t, reader, telemetry.MetricReplicaRecoveries, "", ""); n != 1 {
					t.Fatalf("%d snapshot loads while the bad row is current; want the first only", n)
				}

				fix := store.Change{Index: "h", Shard: 0, Kind: store.KindDelete, ID: "bad"}
				if heal == "rewritten" {
					fix = upsert("h", 0, "bad", `{"title":"fixed"}`)
				}
				head = mustApply(t, st, fix)
				sh := c.waitApplied(head)
				if c.tailer.Halt() != nil {
					t.Fatalf("still halted: %+v", c.tailer.Halt())
				}
				if dd := diff(viewOf(t, sh), truthOf(t, st, id), false); dd != "" {
					t.Fatalf("recovered copy differs:\n%s", dd)
				}
			})
		})
	}
}

// TestMappingHaltPrunedPast: a copy whose load halts at a mapping it cannot parse,
// with the changelog pruned past it, loads once and then waits on the index's mapping
// version, and gets past it once the mapping is replaced.
func TestMappingHaltPrunedPast(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "m", testMapping)
		id := ShardID{Index: "m", Shard: 0}
		mustApply(t, st, upsert("m", 0, "a", docBody("a", 1)))
		setMapping(t, st, "m", `{"fields":5}`)
		head, _, err := st.HeadSeq(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := st.Prune(ctx, id, head+1); err != nil {
			t.Fatal(err)
		}

		c, reader := haltedCopy(t, d, id)
		if h := c.current().Halt(); h.Reason != ReasonMapping {
			t.Fatalf("load halt %+v", h)
		}
		c.until("the halted load is retried", func() bool {
			return counterSum(t, reader, telemetry.MetricReplicaHalts, "", "") >= 8
		})
		if n := counterSum(t, reader, telemetry.MetricReplicaRecoveries, "", ""); n != 1 {
			t.Fatalf("%d snapshot loads while the bad mapping is current; want the first only", n)
		}

		setMapping(t, st, "m", testMapping)
		head = mustApply(t, st, upsert("m", 0, "b", docBody("b", 2)))
		sh := c.waitApplied(head)
		if c.tailer.Halt() != nil {
			t.Fatalf("still halted: %+v", c.tailer.Halt())
		}
		if dd := diff(viewOf(t, sh), truthOf(t, st, id), false); dd != "" {
			t.Fatalf("recovered copy differs:\n%s", dd)
		}
	})
}

// haltedCopy starts a copy of id on its own store connection, with a fast halt
// backoff on a fake clock, and waits for it to halt.
func haltedCopy(t *testing.T, d *db, id ShardID) (*copyRunner, *sdkmetric.ManualReader) {
	t.Helper()
	opts, reader := meteredOptions()
	opts.HaltRetryBase, opts.HaltRetryCap = 20*time.Millisecond, 50*time.Millisecond
	c := newCopy(t, d.open(t), id, opts)
	c.withFakeClock(opts.HaltRetryCap)
	c.start()
	c.until("the copy halts", func() bool { return c.current().Halt() != nil })
	return c, reader
}

// setMapping stores raw as index's mapping, as it is: the store takes any JSON.
func setMapping(t testing.TB, st store.Store, index, raw string) {
	t.Helper()
	ctx := context.Background()
	meta, err := st.Indexes().Get(ctx, index)
	if err != nil {
		t.Fatal(err)
	}
	meta.Mapping = []byte(raw)
	if _, err := st.Indexes().Update(ctx, meta); err != nil {
		t.Fatalf("update %s: %v", index, err)
	}
}
