package replica

import (
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// TestRemapSkipsUntypeableValues: a mapping change that maps fields live documents
// hold only as [] or {} (values dynamic inference does not type, which analyze as
// presence alone typed or not) rebuilds nothing, and the copy, adopting the mapping in
// place, equals one rebuilt after the change. A holder with a typeable value still
// rebuilds the copy.
func TestRemapSkipsUntypeableValues(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		st := d.open(t)
		createIndex(t, st, "mk", `{"dynamic":false,"fields":{"title":"text"}}`)
		id := ShardID{Index: "mk", Shard: 0}
		opts, reader := meteredOptions()
		opts.RemapDebounce = -1
		a := newCopy(t, d.open(t), id, opts)
		a.start()
		a.waitApplied(mustApply(t, st,
			upsert("mk", 0, "d1", `{"title":"x","images":[],"meta":{}}`),
			upsert("mk", 0, "d2", `{"title":"y","images":[],"tags":"red"}`)))
		before := a.shard()
		rebuilds := func() int64 { return counterSum(t, reader, telemetry.MetricReplicaRecoveries, "reason", reasonRemap) }

		updateMapping(t, st, "mk", func(m *schema.Mapping) {
			m.Fields["images"] = schema.KeywordList
			m.Fields["meta"] = schema.Keyword
		})
		head := mustApply(t, st, upsert("mk", 0, "d3", `{"title":"z","images":["a.jpg"]}`))
		va := viewOf(t, a.waitApplied(head))
		if n := rebuilds(); n != 0 || a.shard() != before {
			t.Fatalf("%d rebuilds (shard replaced: %v) for mapping fields only [] and {} held", n, a.shard() != before)
		}
		fresh := newCopy(t, d.open(t), id, testOptions())
		fresh.start()
		if dd := diff(va, viewOf(t, fresh.waitApplied(head)), true); dd != "" {
			t.Fatalf("the copy that adopted the mapping in place differs from a rebuilt one:\n%s", dd)
		}

		// tags holds a typeable value ("red"): mapping it re-analyzes d2.
		updateMapping(t, st, "mk", func(m *schema.Mapping) { m.Fields["tags"] = schema.Keyword })
		head = mustApply(t, st, upsert("mk", 0, "d4", `{"title":"w"}`))
		va = viewOf(t, a.waitApplied(head))
		if n := rebuilds(); n != 1 {
			t.Fatalf("%d rebuilds for mapping a field a live document holds a value of", n)
		}
		if !postingsHold(va, "tags", "d2") {
			t.Fatalf("d2 is not indexed under tags: %v", va.terms)
		}
	})
}
