package search

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

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
		prepareAgg("x", tc.a, testMapping, "aggs.x", &ps)
		if len(ps) != 1 || ps[0].Loc != tc.loc {
			t.Errorf("%+v: %v, want one problem at %s", tc.a, ps, tc.loc)
		}
	}
}

// TestCardinalityThreshold: a cardinality with a threshold is exact below it and the
// threshold at or past it, however its values spread over segments and shards (one
// value in several counts once), as a top-level aggregation (by ordinal, dense or
// not), under a filters aggregation's buckets and under a terms bucket (by hash).
func TestCardinalityThreshold(t *testing.T) {
	c := newCluster(t, 2)
	brands := []string{"a", "b", "c", "a", "b", "a", "d"}
	for i, b := range brands {
		c.upsert(fmt.Sprintf("d%d", i), fmt.Sprintf(`{"brand":%q,"tags":[%q,"x"],"price":%d,"active":true}`, b, b, i%3))
		c.refreshAll() // a segment per document: every value in several segments
	}
	cheap := leaf("price", query.OpLt, 1)
	for threshold, want := range map[int]int64{1: 1, 2: 2, 3: 3, 4: 4, 5: 4, 100: 4} {
		card := func(field string) Agg { return Agg{Type: AggCardinality, Field: field, Threshold: threshold} }
		for _, q := range []query.Node{&query.All{}, leaf("brand", query.OpIn, brands)} {
			got, err := c.search(&Request{Query: q, Aggs: map[string]Agg{
				"brand": card("brand"), "tags": card("tags"),
				"price": card("price"),
				"by":    {Type: AggTerms, Field: "active", Aggs: map[string]Agg{"brand": card("brand")}},
				"f":     {Type: AggFilters, Filters: map[string]query.Node{"all": &query.All{}, "cheap": cheap}, Aggs: map[string]Agg{"brand": card("brand")}},
			}})
			if err != nil {
				t.Fatal(err)
			}
			tags := min(want+1, int64(threshold)) // x too
			for name, w := range map[string]int64{"brand": want, "tags": tags, "price": min(3, int64(threshold))} {
				if v := got.Aggs[name].Value; v != w {
					t.Errorf("threshold %d: %s %d, want %d", threshold, name, v, w)
				}
			}
			if v := got.Aggs["by"].Buckets[0].Aggs["brand"].Value; v != want {
				t.Errorf("threshold %d: under terms %d, want %d", threshold, v, want)
			}
			// The cheap documents (price 0) are d0, d3 and d6: brands a, a, d.
			for i, w := range []int64{want, min(2, int64(threshold))} {
				if v := got.Aggs["f"].Buckets[i].Aggs["brand"].Value; v != w {
					t.Errorf("threshold %d: filters bucket %v %d, want %d", threshold, got.Aggs["f"].Buckets[i].Key, v, w)
				}
			}
			if s := string(mustJSON(got.Aggs["brand"])); s != fmt.Sprintf(`{"value":%d}`, want) {
				t.Errorf("threshold %d: %s", threshold, s)
			}
		}
	}
	for _, a := range []Agg{
		{Type: AggCardinality, Field: "brand", Threshold: MaxThreshold + 1},
		{Type: AggCardinality, Field: "brand", Threshold: 2, Precision: 10},
	} {
		var ps problems
		prepareAgg("x", a, testMapping, "aggs.x", &ps)
		if len(ps) != 1 || ps[0].Loc != "aggs.x.cardinality.threshold" {
			t.Errorf("%+v: %v", a, ps)
		}
	}
}

// TestReduceKeepsShardResults: the reduce reads the shards' results without changing
// them, so reducing them again (a cached shard result, say) answers the same.
func TestReduceKeepsShardResults(t *testing.T) {
	c := newCluster(t, 3)
	for i := range 200 {
		c.upsert(fmt.Sprintf("d%03d", i), fmt.Sprintf(`{"brand":"b%d","tags":["t%d","t%d"],"price":%d}`, i%7, i%5, i%11, i))
	}
	c.refreshAll()
	r := &Request{Query: &query.All{}, Aggs: map[string]Agg{
		"t": {Type: AggTerms, Field: "tags", Size: 4, Aggs: map[string]Agg{"s": {Type: AggStats, Field: "price"}, "n": {Type: AggCardinality, Field: "brand"}}},
		"f": {Type: AggFilter, Filter: &query.All{}, Aggs: map[string]Agg{"b": {Type: AggTerms, Field: "brand", Aggs: map[string]Agg{"lo": {Type: AggMin, Field: "price"}}}}},
	}}
	var parts []*ShardResult
	for _, s := range c.shards {
		g := s.Acquire()
		res, err := ExecuteShard(t.Context(), g, r)
		g.Release()
		if err != nil {
			t.Fatal(err)
		}
		parts = append(parts, res)
	}
	first := string(mustJSON(Reduce(t.Context(), parts, r).Aggs))
	if again := string(mustJSON(Reduce(t.Context(), parts, r).Aggs)); again != first {
		t.Fatalf("reduced again:\n%s\nfirst:\n%s", again, first)
	}
}

// TestReplayCutShort: a deadline that passes while a shard reads its hits again for
// breadth-first sub-aggregations leaves that terms aggregation empty, TimedOut set,
// rather than counts its sub-aggregations do not match; the shard's other
// aggregations are merged still. A cancel then is the search's error, not a timeout.
func TestReplayCutShort(t *testing.T) {
	c := newCluster(t, 1)
	for i := range 300 {
		c.upsert(fmt.Sprintf("d%03d", i), fmt.Sprintf(`{"tags":["t%d","t%d"],"price":%d}`, i%13, i%7, i))
		if i%100 == 99 {
			c.refreshAll()
		}
	}
	r := func(timeout time.Duration) *Request {
		return &Request{Query: &query.All{}, Timeout: timeout, Aggs: map[string]Agg{
			"t": {Type: AggTerms, Field: "tags", Size: 3, CollectMode: CollectBreadthFirst, Aggs: map[string]Agg{"s": {Type: AggStats, Field: "price"}}},
			"s": {Type: AggStats, Field: "price"},
		}}
	}
	defer func() { replayHook = nil }()
	g := c.shards[0].Acquire()
	defer g.Release()

	replayHook = func(ctx context.Context, _ []*ordPartial) { <-ctx.Done() }
	res, err := ExecuteShard(t.Context(), g, r(300*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if !res.TimedOut || len(res.Aggs["t"].Buckets) != 0 || res.Aggs["s"].Stats.Count != 300 {
		t.Fatalf("cut short: timed out %v, %d terms buckets, stats %+v", res.TimedOut, len(res.Aggs["t"].Buckets), res.Aggs["s"].Stats)
	}
	for _, b := range Reduce(t.Context(), []*ShardResult{res}, r(0)).Aggs["t"].Buckets {
		if b.Aggs["s"].Count != b.DocCount {
			t.Fatalf("bucket %v: %d documents, stats of %d", b.Key, b.DocCount, b.Aggs["s"].Count)
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	replayHook = func(context.Context, []*ordPartial) { cancel() }
	if res, err := ExecuteShard(ctx, g, r(0)); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled during the replay: %+v, %v; want context.Canceled", res, err)
	}

	replayHook = nil
	res, err = ExecuteShard(t.Context(), g, r(0))
	if err != nil || res.TimedOut || len(res.Aggs["t"].Buckets) != 13 {
		t.Fatalf("uncut: %+v, %v", res, err)
	}
}

// TestReplayOutlivesSegments: the replay runs within the shard merge's context, which
// lasts the shard's whole deadline: once the segments' own has ended (a reserve
// before it), the replay still finishes, and every bucket's sub-aggregations count
// its documents. A replay that fails outright (here, a panic) fails the shard, rather
// than time it out.
func TestReplayOutlivesSegments(t *testing.T) {
	c := newCluster(t, 1)
	for i := range 300 {
		c.upsert(fmt.Sprintf("d%03d", i), fmt.Sprintf(`{"tags":["t%d","t%d"],"price":%d}`, i%13, i%7, i))
	}
	c.refreshAll()
	r := &Request{Query: &query.All{}, Timeout: 3 * time.Second, Aggs: map[string]Agg{
		"t": {Type: AggTerms, Field: "tags", Size: 3, CollectMode: CollectBreadthFirst, Aggs: map[string]Agg{"s": {Type: AggStats, Field: "price"}}},
	}}
	defer func() { replayHook = nil }()
	g := c.shards[0].Acquire()
	defer g.Release()

	replayHook = func(ctx context.Context, parts []*ordPartial) {
		for _, p := range parts {
			if p.deferred != nil {
				<-p.deferred.s.ctx.Done() // the segments' deadline
			}
		}
		if ctx.Err() != nil {
			t.Error("the merge's context ended with the segments'")
		}
	}
	res, err := ExecuteShard(t.Context(), g, r)
	if err != nil {
		t.Fatal(err)
	}
	got := Reduce(t.Context(), []*ShardResult{res}, r).Aggs["t"]
	if res.TimedOut || len(got.Buckets) == 0 {
		t.Fatalf("after the segments' deadline: timed out %v, %d buckets", res.TimedOut, len(got.Buckets))
	}
	for _, b := range got.Buckets {
		if b.Aggs["s"].Count != b.DocCount {
			t.Fatalf("bucket %v: %d documents, stats of %d", b.Key, b.DocCount, b.Aggs["s"].Count)
		}
	}

	replayHook = func(_ context.Context, parts []*ordPartial) {
		for _, p := range parts {
			if p.deferred != nil {
				p.deferred.s = &segExec{ctx: t.Context()} // no reader: the replay panics
			}
		}
	}
	r.Timeout = 0
	if res, err := ExecuteShard(t.Context(), g, r); err == nil || isCtxErr(err) {
		t.Fatalf("a failed replay: %+v, %v; want the shard to fail", res, err)
	}
	if err := firstFailure([]error{context.DeadlineExceeded, nil, errors.New("boom")}); err == nil || err.Error() != "boom" {
		t.Fatalf("firstFailure: %v, want boom", err)
	}
}
