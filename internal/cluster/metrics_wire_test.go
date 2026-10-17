package cluster

import (
	"encoding/json"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
)

// TestWireMetrics: a min or max aggregation, or a cardinality's threshold, at any
// level, needs search wire level 4, which a peer of an earlier release would refuse as
// unknown or ignore; a terms aggregation's
// collect_mode crosses the wire and needs nothing new, as either mode answers the
// same.
func TestWireMetrics(t *testing.T) {
	terms := func(sub search.Agg) map[string]search.Agg {
		return map[string]search.Agg{"t": {Type: search.AggTerms, Field: "brand", CollectMode: search.CollectBreadthFirst, Aggs: map[string]search.Agg{"m": sub}}}
	}
	for name, tc := range map[string]struct {
		aggs  map[string]search.Agg
		needs int
	}{
		"min":          {map[string]search.Agg{"m": {Type: search.AggMin, Field: "price"}}, levelMetrics},
		"max under":    {terms(search.Agg{Type: search.AggMax, Field: "price"}), levelMetrics},
		"collect_mode": {terms(search.Agg{Type: search.AggStats, Field: "price"}), 0},
		"threshold":    {terms(search.Agg{Type: search.AggCardinality, Field: "tags", Threshold: 2}), levelMetrics},
	} {
		wr, err := encodeRequest(&search.Request{Query: &query.All{}, Aggs: tc.aggs})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(wr)
		if err != nil {
			t.Fatal(err)
		}
		var back wireRequest
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatal(err)
		}
		r, err := back.decode()
		if err != nil {
			t.Fatal(err)
		}
		if wr.Needs != tc.needs || back.Needs != tc.needs {
			t.Errorf("%s: needs %d, %d after the wire; want %d", name, wr.Needs, back.Needs, tc.needs)
		}
		if got, want := r.Aggs["t"].Aggs["m"].Threshold, tc.aggs["t"].Aggs["m"].Threshold; got != want {
			t.Errorf("%s: threshold %d after the wire, want %d", name, got, want)
		}
		if got, want := r.Aggs["t"].CollectMode, tc.aggs["t"].CollectMode; got != want {
			t.Errorf("%s: collect_mode %q after the wire, want %q", name, got, want)
		}
	}
	if levelNeeds(levelMetrics) == levelNeeds(levelFrom) {
		t.Errorf("level %d's errors name level %d's needs", levelMetrics, levelFrom)
	}
}
