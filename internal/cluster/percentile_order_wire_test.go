package cluster

import (
	"encoding/json"
	"reflect"
	"slices"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
)

// TestWirePercentileOrder: a terms order by a percentiles or percentile_ranks
// sub-aggregation, or by a member in brackets, at any level, needs search wire level 11,
// which a 1.7.0 peer would refuse; any other order still needs 10. Each crosses the wire
// whole.
func TestWirePercentileOrder(t *testing.T) {
	pct := map[string]search.Agg{
		"p":  {Type: search.AggPercentiles, Field: "price", Percents: []float64{50, 99.9}, Compression: 50},
		"r":  {Type: search.AggPercentileRanks, Field: "price", Values: []float64{100}, Unkeyed: true},
		"st": {Type: search.AggStats, Field: "price"},
	}
	terms := func(keys ...string) search.Agg {
		a := search.Agg{Type: search.AggTerms, Field: "brand", Aggs: pct}
		for _, k := range keys {
			a.Order = append(a.Order, search.TermsOrder{Key: k, Desc: true})
		}
		return a
	}
	for name, tc := range map[string]struct {
		aggs  map[string]search.Agg
		needs int
	}{
		"percentile":         {map[string]search.Agg{"t": terms("p.99.9")}, levelPercentileOrder},
		"bracketed percent":  {map[string]search.Agg{"t": terms("p[50]")}, levelPercentileOrder},
		"rank, then count":   {map[string]search.Agg{"t": terms("r.100", search.OrderCount)}, levelPercentileOrder},
		"count, then rank":   {map[string]search.Agg{"t": terms(search.OrderCount, "r.100")}, levelPercentileOrder},
		"bracketed stats":    {map[string]search.Agg{"t": terms("st[avg]")}, levelPercentileOrder},
		"under filter":       {map[string]search.Agg{"f": {Type: search.AggFilter, Filter: &query.All{}, Aggs: map[string]search.Agg{"t": terms("p.50")}}}, levelPercentileOrder},
		"stats":              {map[string]search.Agg{"t": terms("st.avg")}, levelCompositeAggs},
		"percentiles alone":  {map[string]search.Agg{"t": terms()}, levelPercentiles},
		"no such sub-aggreg": {map[string]search.Agg{"t": terms("q.50")}, levelCompositeAggs},
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
		for aggName, a := range tc.aggs {
			got := r.Aggs[aggName]
			if a.Filter != nil {
				a, got = a.Aggs["t"], got.Aggs["t"]
			}
			if !reflect.DeepEqual(got.Order, a.Order) {
				t.Errorf("%s: order %+v after the wire, want %+v", name, got.Order, a.Order)
			}
			for _, sub := range []string{"p", "r"} {
				g, w := got.Aggs[sub], a.Aggs[sub]
				if g.Type != w.Type || !slices.Equal(g.Percents, w.Percents) || !slices.Equal(g.Values, w.Values) || g.Compression != w.Compression || g.Unkeyed != w.Unkeyed {
					t.Errorf("%s: sub-aggregation %s is %+v after the wire, want %+v", name, sub, g, w)
				}
			}
		}
	}
	if levelNeeds(levelPercentileOrder) == levelNeeds(levelCompositeAggs) || searchLevel < levelPercentileOrder {
		t.Errorf("search level %d is below %d, or its errors name level %d's needs", searchLevel, levelPercentileOrder, levelCompositeAggs)
	}
}
