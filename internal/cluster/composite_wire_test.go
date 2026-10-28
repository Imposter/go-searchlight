package cluster

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
)

// TestWireCompositeAggs: a top_metrics or composite aggregation, or a terms
// aggregation's order, at any level, needs search wire level 10, which a peer of an
// earlier release would refuse as unknown or ignore (answering in count order); each
// crosses the wire whole.
func TestWireCompositeAggs(t *testing.T) {
	top := search.Agg{Type: search.AggTopMetrics, Metrics: []string{"price", "brand"}, TopSort: &search.SortField{Field: "created", Desc: true}, Size: 2}
	for name, tc := range map[string]map[string]search.Agg{
		"top_metrics":       {"m": top},
		"top_metrics under": {"t": {Type: search.AggTerms, Field: "brand", Aggs: map[string]search.Agg{"m": top}}},
		"order": {"t": {
			Type: search.AggTerms, Field: "brand", Order: []search.TermsOrder{{Key: "m.avg", Desc: true}, {Key: search.OrderCount}},
			Aggs: map[string]search.Agg{"m": {Type: search.AggStats, Field: "price"}},
		}},
		"order under filter": {"f": {Type: search.AggFilter, Filter: &query.All{}, Aggs: map[string]search.Agg{
			"t": {Type: search.AggTerms, Field: "brand", Order: []search.TermsOrder{{Key: search.OrderKey, Desc: true}}},
		}}},
		"composite": {"c": {
			Type: search.AggComposite, Size: 3, After: map[string]any{"b": "acme", "p": 10.0, "a": true},
			Sources: []search.CompositeSource{
				{Name: "b", Type: search.AggTerms, Field: "brand", Desc: true},
				{Name: "p", Type: search.AggHistogram, Field: "price", Interval: 5, Offset: 1},
				{Name: "a", Type: search.AggTerms, Field: "active"},
				{Name: "d", Type: search.AggDateHistogram, Field: "created", Calendar: "month"},
			},
			Aggs: map[string]search.Agg{"m": {Type: search.AggMax, Field: "price"}},
		}},
	} {
		wr, err := encodeRequest(&search.Request{Query: &query.All{}, Aggs: tc})
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
		if wr.Needs != levelCompositeAggs || back.Needs != levelCompositeAggs {
			t.Errorf("%s: needs %d, %d after the wire; want %d", name, wr.Needs, back.Needs, levelCompositeAggs)
		}
		for aggName, a := range tc {
			got := r.Aggs[aggName]
			if a.Filter != nil {
				a, got = a.Aggs["t"], got.Aggs["t"]
			}
			if !reflect.DeepEqual(got.Order, a.Order) || !reflect.DeepEqual(got.Sources, a.Sources) || !reflect.DeepEqual(got.After, a.After) ||
				!reflect.DeepEqual(got.Metrics, a.Metrics) || !reflect.DeepEqual(got.TopSort, a.TopSort) {
				t.Errorf("%s: %+v after the wire, want %+v", name, got, a)
			}
			if sub, ok := a.Aggs["m"]; ok && (!reflect.DeepEqual(got.Aggs["m"].Metrics, sub.Metrics) || !reflect.DeepEqual(got.Aggs["m"].TopSort, sub.TopSort)) {
				t.Errorf("%s: sub-aggregation %+v after the wire, want %+v", name, got.Aggs["m"], sub)
			}
		}
	}
	if levelNeeds(levelCompositeAggs) == levelNeeds(levelSeqNo) {
		t.Errorf("level %d's errors name level %d's needs", levelCompositeAggs, levelSeqNo)
	}
	if searchLevel < levelCompositeAggs {
		t.Errorf("search level %d is below %d", searchLevel, levelCompositeAggs)
	}
}
