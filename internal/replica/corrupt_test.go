package replica

import (
	"context"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// TestCorruptHalts: a stored body that does not decode (store.ErrCorrupt) halts the
// copy at its record, reason "corrupt", instead of failing the same read forever:
// tailing, after the changes before it are applied; loading a snapshot, at the
// document. The copy waits there, and gets past it once the document is written
// again.
func TestCorruptHalts(t *testing.T) {
	const damaged = `{"title":"damaged"}`
	for _, at := range []string{"tail", "load"} {
		t.Run(at, func(t *testing.T) {
			forEachDialect(t, func(t *testing.T, d *db) {
				st := d.open(t)
				createIndex(t, st, "c", testMapping)
				id := ShardID{Index: "c", Shard: 0}
				fs := newFaultStore(d.open(t))
				fs.corrupt = func(c *store.Change) bool { return string(c.Payload) == damaged }
				var bad int64
				fs.onRecord = func(_ context.Context, _ int, r store.Record) error {
					if string(r.Body) == damaged {
						return &store.CorruptError{Shard: id, Seq: r.Seq, ID: r.ID, Err: errInjected}
					}
					return nil
				}
				first := mustApply(t, st, upsert("c", 0, "ok1", docBody("ok1", 1)))
				if at == "load" {
					bad = mustApply(t, st, upsert("c", 0, "bad", damaged))
					mustApply(t, st, upsert("c", 0, "ok2", docBody("ok2", 2)))
				}
				opts, reader := meteredOptions()
				opts.HaltRetryBase, opts.HaltRetryCap = 20*time.Millisecond, 50*time.Millisecond
				c := newCopy(t, fs, id, opts)
				c.withFakeClock(opts.HaltRetryCap)
				c.start()
				if at == "tail" {
					c.waitApplied(first)
					bad = mustApply(t, st, upsert("c", 0, "bad", damaged), upsert("c", 0, "ok2", docBody("ok2", 2))) - 1
				}
				c.until("the copy halts", func() bool { return c.current().Halt() != nil })
				h := c.current().Halt()
				if h.Reason != ReasonCorrupt || h.ID != "bad" || h.Seq != bad {
					t.Fatalf("halt %+v, want corrupt at bad seq %d", h, bad)
				}
				c.until("the halt is retried", func() bool {
					return counterSum(t, reader, telemetry.MetricReplicaHalts, "", "") >= 6
				})
				if got := c.current().Applied(); got >= bad {
					t.Fatalf("applied %d, past the damaged change at %d", got, bad)
				}
				if n := counterSum(t, reader, telemetry.MetricReplicaRecoveries, "", ""); n != 1 {
					t.Fatalf("%d snapshot loads while the damaged row is current", n)
				}

				head := mustApply(t, st, upsert("c", 0, "bad", docBody("bad", 3)))
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
