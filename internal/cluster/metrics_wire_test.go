package cluster

import (
	"encoding/json"
	"slices"
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

// TestWirePercentiles: a percentiles or percentile_ranks aggregation, at any level,
// needs search wire level 9, the newest, which a peer of an earlier release would refuse
// as unknown; its settings cross the wire as they were.
func TestWirePercentiles(t *testing.T) {
	pct := search.Agg{Type: search.AggPercentiles, Field: "price", Percents: []float64{50, 99.9}, Compression: 200, Unkeyed: true}
	ranks := search.Agg{Type: search.AggPercentileRanks, Field: "price", Values: []float64{10, 20}}
	for name, tc := range map[string]struct {
		aggs  map[string]search.Agg
		needs int
	}{
		"percentiles": {map[string]search.Agg{"m": pct}, levelPercentiles},
		"ranks under a filter": {map[string]search.Agg{"f": {Type: search.AggFilter, Filter: &query.All{}, Aggs: map[string]search.Agg{
			"t": {Type: search.AggTerms, Field: "brand", Aggs: map[string]search.Agg{"m": ranks}},
		}}}, levelPercentiles},
		"stats": {map[string]search.Agg{"m": {Type: search.AggStats, Field: "price"}}, 0},
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
		if got := r.Aggs["m"]; name == "percentiles" && (!slices.Equal(got.Percents, pct.Percents) || got.Compression != 200 || !got.Unkeyed) {
			t.Errorf("%s: %+v after the wire, want %+v", name, got, pct)
		}
		if got := r.Aggs["f"].Aggs["t"].Aggs["m"]; name == "ranks under a filter" && !slices.Equal(got.Values, ranks.Values) {
			t.Errorf("%s: values %v after the wire, want %v", name, got.Values, ranks.Values)
		}
	}
	if searchLevel != levelPercentiles || levelNeeds(levelPercentiles) == levelNeeds(levelSeqNo) {
		t.Errorf("level %d is not the newest, or its errors name level %d's needs", levelPercentiles, levelSeqNo)
	}
	aggs := map[string]search.Agg{"p": pct, "t": {Type: search.AggTerms, Field: "brand", Aggs: map[string]search.Agg{"r": ranks, "s": {Type: search.AggStats, Field: "price"}}}}
	dropPercentiles(aggs)
	if _, ok := aggs["p"]; ok || len(aggs["t"].Aggs) != 1 {
		t.Errorf("dropPercentiles left %v", aggs)
	}
}
