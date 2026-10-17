package search

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
)

// TestBreadthFirstEqualsDepthFirst: a terms aggregation with metric sub-aggregations
// answers the same in either collect mode, and with none given, over random data on
// several shards of several segments, collected whole or in parts, merged by global
// ordinal or by term, with small shard sizes (so the shards cut their lists), include
// and exclude, min_doc_count, list fields, and under a filter aggregation.
func TestBreadthFirstEqualsDepthFirst(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0xb4ead))
	c := newCluster(t, 3)
	rounds, queries := 4, 50
	if longMode() {
		rounds, queries = 10, 250
	}
	metric := func() Agg {
		switch rng.IntN(4) {
		case 0:
			return Agg{Type: AggStats, Field: pick(rng, []string{"price", "created", "nope"})}
		case 1:
			return Agg{Type: AggCardinality, Field: pick(rng, []string{"brand", "tags", "price"})}
		}
		return Agg{Type: pick(rng, []string{AggMin, AggMax}), Field: pick(rng, []string{"price", "created"})}
	}
	for round := range rounds {
		for range 80 {
			c.upsert(pick(rng, ids)+pick(rng, []string{"", "-1", "-2", "-3", "-4"}), randomDoc(rng))
		}
		for range 15 {
			c.remove(pick(rng, ids))
		}
		c.refreshAll()
		if round%2 == 1 {
			c.merge(rng.IntN(3), 1)
		}
		for range queries {
			q, raw := randomQuery(t, rng)
			if rng.IntN(2) == 0 {
				q, raw = &query.All{}, "all"
			}
			terms := Agg{
				Type: AggTerms, Field: pick(rng, []string{"brand", "tags", "title", "_id"}),
				Size: 1 + rng.IntN(4), ShardSize: 1 + rng.IntN(8),
				Aggs: map[string]Agg{"m": metric()},
			}
			if rng.IntN(2) == 0 {
				terms.Aggs["n"] = metric()
			}
			if rng.IntN(3) == 0 {
				terms.MinDocCount = ptr(1 + rng.IntN(3))
			}
			if rng.IntN(3) == 0 {
				seed := func() string { return pick(rng, vocab) }
				if terms.Field == IDField {
					seed = func() string { return pick(rng, ids) }
				}
				terms.Include, terms.Exclude = randomTermsFilters(rng, terms.Field, seed)
			}
			wrap := rng.IntN(3) == 0
			chunked := rng.IntN(2) == 0
			chunk := 1 + rng.Uint32N(40)
			density := pick(rng, []uint64{0, 8, 1 << 40})
			run := func(mode string) *AggResult {
				a := terms
				a.CollectMode = mode
				aggs := map[string]Agg{"t": a}
				if wrap {
					aggs = map[string]Agg{"f": {Type: AggFilter, Filter: &query.All{}, Aggs: aggs}}
				}
				if chunked {
					aggChunkMin, aggChunk = 1, chunk
				}
				globalOrdsDensity = density
				got, err := c.search(&Request{Query: q, Aggs: aggs})
				aggChunkMin, aggChunk, globalOrdsDensity = 1<<15, 1<<16, 8
				if err != nil {
					t.Fatalf("%s %+v: %v", raw, aggs, err)
				}
				if wrap {
					return got.Aggs["f"].Aggs["t"]
				}
				return got.Aggs["t"]
			}
			want := run(CollectDepthFirst)
			for _, mode := range []string{CollectBreadthFirst, ""} {
				got := run(mode)
				if d := diffAgg(got, want, "t"); d != "" {
					t.Fatalf("round %d, query %s, %s against depth_first, agg %+v (subs %+v):\n%s", round, raw, mode, terms, terms.Aggs, d)
				}
				if d := diffBounds(got, want); d != "" {
					t.Fatalf("round %d, query %s, %s against depth_first, agg %+v:\n%s", round, raw, mode, terms, d)
				}
			}
		}
	}
}

// diffBounds describes how two terms results' error bounds differ, or "".
func diffBounds(got, want *AggResult) string {
	if got.DocCountErrorUpperBound != want.DocCountErrorUpperBound {
		return fmt.Sprintf("doc_count_error_upper_bound %d, want %d", got.DocCountErrorUpperBound, want.DocCountErrorUpperBound)
	}
	for i, b := range got.Buckets {
		if b.DocCountErrorUpperBound != want.Buckets[i].DocCountErrorUpperBound {
			return fmt.Sprintf("bucket %d (%v): error bound %d, want %d", i, b.Key, b.DocCountErrorUpperBound, want.Buckets[i].DocCountErrorUpperBound)
		}
	}
	return ""
}

// TestMinMax: min and max at the top level, as sub-aggregations and under filter and
// global, in Elasticsearch's shape: {"value": x}, or null with no value.
func TestMinMax(t *testing.T) {
	c := newCluster(t, 2)
	for i, body := range []string{`{"brand":"a","price":3}`, `{"brand":"a","price":-0}`, `{"brand":"b","price":7.5}`, `{"brand":"c"}`} {
		c.upsert(fmt.Sprintf("d%d", i), body)
	}
	c.refreshAll()
	got, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{
		"lo":  {Type: AggMin, Field: "price"},
		"hi":  {Type: AggMax, Field: "price"},
		"no":  {Type: AggMin, Field: "created"},
		"by":  {Type: AggTerms, Field: "brand", Aggs: map[string]Agg{"lo": {Type: AggMin, Field: "price"}, "hi": {Type: AggMax, Field: "price"}}},
		"all": {Type: AggGlobal, Aggs: map[string]Agg{"hi": {Type: AggMax, Field: "price"}}},
		"f":   {Type: AggFilter, Filter: leaf("brand", query.OpEq, "a"), Aggs: map[string]Agg{"hi": {Type: AggMax, Field: "price"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"lo": `{"value":0}`, "hi": `{"value":7.5}`, "no": `{"value":null}`,
		"by":  `{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"doc_count":2,"hi":{"value":3},"key":"a","lo":{"value":0}},{"doc_count":1,"hi":{"value":7.5},"key":"b","lo":{"value":7.5}},{"doc_count":1,"hi":{"value":null},"key":"c","lo":{"value":null}}]}`,
		"all": `{"doc_count":4,"hi":{"value":7.5}}`, "f": `{"doc_count":2,"hi":{"value":3}}`,
	} {
		if s := string(mustJSON(got.Aggs[name])); s != want {
			t.Errorf("%s: %s, want %s", name, s, want)
		}
	}
}

// TestParseMinMaxCollectMode: min and max take a field alone, terms a collect_mode of
// depth_first or breadth_first; anything else is refused at its loc.
func TestParseMinMaxCollectMode(t *testing.T) {
	r, ps := ParseRequest([]byte(`{"aggs": {"lo": {"min": {"field": "price"}},
		"t": {"terms": {"field": "brand", "collect_mode": "breadth_first"}, "aggs": {"hi": {"max": {"field": "price"}}}}}}`))
	if len(ps) > 0 {
		t.Fatal(ps)
	}
	if r.Aggs["lo"].Type != AggMin || r.Aggs["t"].CollectMode != CollectBreadthFirst || r.Aggs["t"].Aggs["hi"].Type != AggMax {
		t.Fatalf("parsed %+v", r.Aggs)
	}
	for body, loc := range map[string]string{
		`{"aggs": {"t": {"terms": {"field": "brand", "collect_mode": "sideways"}}}}`: "aggs.t.terms.collect_mode",
		`{"aggs": {"m": {"min": {"field": "price", "size": 3}}}}`:                    "aggs.m.min.size",
	} {
		if _, ps := ParseRequest([]byte(body)); len(ps) != 1 || ps[0].Loc != loc {
			t.Errorf("%s: %v, want one problem at %s", body, ps, loc)
		}
	}
	for _, tc := range []struct {
		a   Agg
		loc string
	}{
		{Agg{Type: AggMax, Field: "brand"}, "aggs.x.max.field"},
		{Agg{Type: AggStats, Field: "price", CollectMode: CollectDepthFirst}, "aggs.x.stats.collect_mode"},
		{Agg{Type: AggTerms, Field: "brand", CollectMode: "sideways"}, "aggs.x.terms.collect_mode"},
	} {
		var ps problems
		prepareAgg("x", tc.a, testMapping, "aggs.x", "", &ps)
		if len(ps) != 1 || ps[0].Loc != tc.loc {
			t.Errorf("%+v: %v, want one problem at %s", tc.a, ps, tc.loc)
		}
	}
}
