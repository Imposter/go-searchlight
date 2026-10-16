package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// randomWrapped returns a random filter, filters or global aggregation holding one or
// two of randomAgg's aggregations (now and then none).
func randomWrapped(t testing.TB, rng *rand.Rand) Agg {
	a := Agg{Aggs: map[string]Agg{"a": randomAgg(rng)}}
	if rng.IntN(2) == 0 {
		a.Aggs["b"] = randomAgg(rng)
	}
	if rng.IntN(6) == 0 {
		a.Aggs = nil
	}
	switch rng.IntN(3) {
	case 0:
		a.Type = AggFilter
		a.Filter, _ = randomQuery(t, rng)
	case 1:
		a.Type = AggFilters
		a.Filters = map[string]query.Node{}
		for i := range 1 + rng.IntN(4) {
			a.Filters[fmt.Sprintf("k%d", i)], _ = randomQuery(t, rng)
		}
		if rng.IntN(2) == 0 {
			a.OtherBucketKey = "other"
		}
	default:
		a.Type = AggGlobal
	}
	return a
}

// bruteWrapped computes a over matched, the query's documents, with every visible
// document (all) for a global aggregation, as the reduce should answer with exact
// shard sizes.
func bruteWrapped(t *testing.T, a Agg, matched, all []*schema.Doc) *AggResult {
	t.Helper()
	if !singleBucket(a.Type) {
		return bruteAgg(t, a, matched)
	}
	spec := prepareAgg("x", a, testMapping, "aggs.x", "", new(problems))
	if spec == nil {
		t.Fatalf("agg %+v refused", a)
	}
	bucket := func(key any, docs []*schema.Doc) *Bucket {
		b := &Bucket{Key: key, DocCount: int64(len(docs))}
		if len(spec.subs) > 0 {
			b.Aggs = map[string]*AggResult{}
			for _, sub := range spec.subs {
				b.Aggs[sub.name] = bruteSpec(sub, docs)
			}
		}
		return b
	}
	res := &AggResult{Type: a.Type}
	single := func(b *Bucket) *AggResult {
		res.DocCount, res.Aggs = b.DocCount, b.Aggs
		return res
	}
	switch a.Type {
	case AggGlobal:
		return single(bucket(nil, all))
	case AggFilter:
		return single(bucket(nil, brute(matched, a.Filter, nil)))
	}
	inSome := map[string]bool{}
	for _, key := range slices.Sorted(maps.Keys(a.Filters)) {
		docs := brute(matched, a.Filters[key], nil)
		for _, d := range docs {
			inSome[d.ID] = true
		}
		res.Buckets = append(res.Buckets, bucket(key, docs))
	}
	if a.OtherBucketKey != "" {
		var rest []*schema.Doc
		for _, d := range matched {
			if !inSome[d.ID] {
				rest = append(rest, d)
			}
		}
		res.Buckets = append(res.Buckets, bucket(a.OtherBucketKey, rest))
	}
	return res
}

// both is q and post together: the documents the hits are drawn from.
func both(q, post query.Node) query.Node {
	if post == nil {
		return q
	}
	return &query.All{Children: []query.Node{q, post}}
}

// TestFilterAggregationsEqualBruteForce: filter, filters and global aggregations, with
// any aggregation under them, and a post_filter, over random queries on several
// shards (with deletes, merges and writes not yet refreshed), equal brute force: each
// bucket counts the query's documents its filter matches (global: every visible
// document, the unrefreshed ones not), and the hits, total and search_after pages are
// those of the query and the post_filter together, under every track_total and through
// search_after when the post_filter is folded into a query without aggregations. Each
// search runs three times as three requests, a leaf being cached from its second
// sighting, so the third is served from the filter cache.
func TestFilterAggregationsEqualBruteForce(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0xf117))
	c := newCluster(t, 3)
	rounds, queries := 4, 30
	if longMode() {
		rounds, queries = 12, 200
	}
	defer leafUsage.reset()
	for round := range rounds {
		for range 70 {
			c.upsert(pick(rng, ids)+pick(rng, []string{"", "-1", "-2", "-3"}), randomDoc(rng))
		}
		for range 15 {
			c.remove(pick(rng, ids))
		}
		c.refreshAll()
		if round%2 == 1 {
			c.merge(rng.IntN(3), 1)
		}
		for range 10 {
			c.upsert(pick(rng, ids)+"-new", randomDoc(rng))
		}
		c.remove(pick(rng, ids))
		docs := c.docs()
		for range queries {
			q, raw := randomQuery(t, rng)
			if rng.IntN(3) == 0 {
				q, raw = &query.All{}, "all"
			}
			aggs := map[string]Agg{"w": randomWrapped(t, rng), "x": randomAgg(rng)}
			if rng.IntN(2) == 0 {
				aggs["y"] = randomWrapped(t, rng)
			}
			var post query.Node
			if rng.IntN(2) == 0 {
				post, _ = randomQuery(t, rng)
			}
			r := &Request{Query: q, PostFilter: post, Aggs: aggs, Size: pick(rng, []int{0, 3, 1000}), Sort: pick(rng, sortChoices), TrackTotal: TrackTotalAll}
			if rng.IntN(2) == 0 {
				aggChunkMin, aggChunk = 1, 1+rng.Uint32N(40)
			}
			globalOrdsDensity = pick(rng, []uint64{0, 8, 1 << 40})
			var got [3]*Response
			for pass := range got {
				again := *r
				resp, err := c.search(&again)
				if err != nil {
					t.Fatalf("%s %+v: %v", raw, aggs, err)
				}
				got[pass] = resp
			}
			aggChunkMin, aggChunk, globalOrdsDensity = 1<<15, 1<<16, 8
			fail := func(format string, args ...any) {
				t.Helper()
				t.Fatalf("round %d, query %s, post_filter %v, aggs %+v: "+format, append([]any{round, raw, post, aggs}, args...)...)
			}
			matched := brute(docs, q, nil)
			want := brute(docs, both(q, post), r.Sort)
			for pass, resp := range got {
				for name, a := range aggs {
					if d := diffAgg(resp.Aggs[name], unfilled(resp.Aggs[name], bruteWrapped(t, a, matched, docs)), name); d != "" {
						fail("pass %d: %s", pass, d)
					}
				}
				if resp.Total != int64(len(want)) || resp.TotalRelation != RelationEq {
					fail("pass %d: total %d %s, want %d eq", pass, resp.Total, resp.TotalRelation, len(want))
				}
				if ids, w := hitIDs(resp.Hits), docIDs(want[:min(len(want), r.Size)]); !slices.Equal(ids, w) {
					fail("pass %d: hits %v, want %v", pass, ids, w)
				}
			}
			if rng.IntN(4) == 0 && len(want) > 0 {
				pageWithAggs(t, c, r, 1+rng.IntN(7), want, fail)
			}
			checkFoldedPostFilter(t, c, rng, q, post, docs, fail)
		}
	}
	if st := c.cache.Stats(); st.Hits == 0 {
		t.Fatalf("no search was served from the filter cache: %+v", st)
	}
}

// checkFoldedPostFilter checks a post_filter without aggregations, folded into the
// query: under every track_total (lazy verification included) the hits and total are
// the query and post_filter's together, and search_after pages through them.
func checkFoldedPostFilter(t *testing.T, c *cluster, rng *rand.Rand, q, post query.Node, docs []*schema.Doc, fail func(string, ...any)) {
	t.Helper()
	if post == nil {
		post, _ = randomQuery(t, rng)
	}
	r := &Request{
		Query: q, PostFilter: post, Size: pick(rng, []int{0, 1, 3, 10, 1000}), Sort: pick(rng, sortChoices),
		TrackTotal: pick(rng, []int{0, 1, 5, TrackTotalAll, TrackTotalNone}),
	}
	want := brute(docs, both(q, post), r.Sort)
	resp, err := c.search(r)
	if err != nil {
		fail("post_filter without aggregations: %v", err)
	}
	checkTotal(t, resp, int64(len(want)), r.TrackTotal, fail)
	if ids, w := hitIDs(resp.Hits), docIDs(want[:min(len(want), r.Size)]); !slices.Equal(ids, w) {
		fail("post_filter %v without aggregations, track_total %d: hits %v, want %v", post, r.TrackTotal, ids, w)
	}
	if rng.IntN(3) == 0 && len(want) > 0 {
		pageWithAggs(t, c, r, 1+rng.IntN(7), want, fail)
	}
}

// pageWithAggs walks every page of size n of r (its aggregations and post_filter
// kept) and checks the walk is want, exactly.
func pageWithAggs(t *testing.T, c *cluster, r *Request, n int, want []*schema.Doc, fail func(string, ...any)) {
	t.Helper()
	var seen []string
	page := *r
	page.Size, page.SearchAfter = n, nil
	for pages := 0; ; pages++ {
		if pages > len(want)+2 {
			fail("paging does not end")
		}
		got, err := c.search(&page)
		if err != nil {
			fail("page %d after %#v: %v", pages, page.SearchAfter, err)
		}
		for _, h := range got.Hits {
			seen = append(seen, h.ID)
		}
		if got.Next == nil {
			break
		}
		page.SearchAfter = got.Next
	}
	if w := docIDs(want); !slices.Equal(seen, w) {
		fail("pages of %d walk %v, want %v", n, seen, w)
	}
}

// TestFilterAggregationEqualsFilteredSearch: an aggregation under a filter answers
// exactly what it answers at the top level of a search whose query is the query and
// the filter together, across shards: a terms aggregation's buckets with their error
// bounds and sum_other_doc_count under a small shard_size included. So does each
// bucket of a filters aggregation, and global, a search of every document.
func TestFilterAggregationEqualsFilteredSearch(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0xf11e))
	c := newCluster(t, 4)
	brands := []string{"b00", "b01", "b02", "b03", "b04", "b05", "b06", "b07", "b08", "b09", "b10", "b11"}
	for i := range 900 {
		b := brands[min(len(brands)-1, int(rng.ExpFloat64()*3))]
		c.upsert(fmt.Sprintf("d%04d", i), fmt.Sprintf(`{"brand":%q,"price":%d,"tags":[%q,%q],"active":%v}`, b, rng.IntN(60), pick(rng, brands), pick(rng, brands), rng.IntN(2) == 0))
	}
	c.refreshAll()
	c.merge(1, 1)
	jsonOf := func(a *AggResult) string {
		out, err := json.Marshal(a)
		if err != nil {
			t.Fatal(err)
		}
		return string(out)
	}
	for i := range 40 {
		inner := randomAgg(rng)
		if inner.Type == AggTerms {
			inner.Size, inner.ShardSize = pick(rng, []int{1, 3, 5}), pick(rng, []int{1, 2, 4, 0})
		}
		q, raw := randomQuery(t, rng)
		if i%3 == 0 {
			q, raw = &query.All{}, "all"
		}
		f, fraw := randomQuery(t, rng)
		got, err := c.search(&Request{Query: q, Aggs: map[string]Agg{
			"f": {Type: AggFilter, Filter: f, Aggs: map[string]Agg{"in": inner}},
			"s": {Type: AggFilters, Filters: map[string]query.Node{"k": f, "q": &query.All{}}, Aggs: map[string]Agg{"in": inner}},
			"g": {Type: AggGlobal, Aggs: map[string]Agg{"in": inner}},
		}})
		if err != nil {
			t.Fatalf("query %s, filter %s, %+v: %v", raw, fraw, inner, err)
		}
		for _, tc := range []struct {
			name     string
			q        query.Node
			docCount int64
			in       *AggResult
		}{
			{"filter", both(q, f), got.Aggs["f"].DocCount, got.Aggs["f"].Aggs["in"]},
			{"filters k", both(q, f), got.Aggs["s"].Buckets[0].DocCount, got.Aggs["s"].Buckets[0].Aggs["in"]},
			{"filters q", q, got.Aggs["s"].Buckets[1].DocCount, got.Aggs["s"].Buckets[1].Aggs["in"]},
			{"global", &query.All{}, got.Aggs["g"].DocCount, got.Aggs["g"].Aggs["in"]},
		} {
			want, err := c.search(&Request{Query: tc.q, TrackTotal: TrackTotalAll, Aggs: map[string]Agg{"in": inner}})
			if err != nil {
				t.Fatal(err)
			}
			if g, w := jsonOf(tc.in), jsonOf(want.Aggs["in"]); g != w || tc.docCount != want.Total {
				t.Fatalf("query %s, filter %s, %s holding %+v: %d documents, %s\nwant %d, %s", raw, fraw, tc.name, inner, tc.docCount, g, want.Total, w)
			}
		}
	}
}

// A post_filter narrows the hits, total and pages but not the aggregations; without
// aggregations it is folded into the query, which keeps the lazy top-k.
func TestPostFilterNarrowsHitsNotAggregations(t *testing.T) {
	c := newCluster(t, 2)
	for i := range 40 {
		c.upsert(fmt.Sprintf("d%02d", i), fmt.Sprintf(`{"brand":%q,"price":%d}`, []string{"acme", "initech"}[i%2], i))
	}
	c.refreshAll()
	post := &query.Leaf{Field: "brand", Op: query.OpEq, Value: json.RawMessage(`"acme"`)}
	brands := map[string]Agg{"brands": {Type: AggTerms, Field: "brand"}}
	resp, err := c.search(&Request{Query: &query.All{}, PostFilter: post, Aggs: brands, Size: 5, Sort: []SortField{{Field: "price", Desc: true}}, TrackTotal: TrackTotalAll})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Total != 20 || !slices.Equal(hitIDs(resp.Hits), []string{"d38", "d36", "d34", "d32", "d30"}) {
		t.Fatalf("total %d, hits %v: want 20 and the five dearest acme", resp.Total, hitIDs(resp.Hits))
	}
	if bs := resp.Aggs["brands"].Buckets; len(bs) != 2 || bs[0].DocCount != 20 || bs[1].DocCount != 20 {
		t.Fatalf("brands %v: want both brands, 20 each", bucketKeys(bs))
	}
	g := c.shards[0].Acquire()
	defer g.Release()
	p, err := prepare(&Request{Query: &query.All{}, PostFilter: post, Size: 5}, g.Mapping())
	if err != nil {
		t.Fatal(err)
	}
	if p.post != nil || p.root.kind != nAll || len(p.root.children) != 2 {
		t.Fatalf("without aggregations the post_filter is not folded into the query: post %v, root %+v", p.post, p.root)
	}
}

// Every new key's problem is reported at its loc: a filter's, post_filter's and each
// filters entry's under their own, the nesting rules, and the bounds.
func TestFilterAggregationProblems(t *testing.T) {
	filters := func(n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = fmt.Sprintf(`"k%03d":{"all":[]}`, i)
		}
		return `{"filters":{"filters":{` + strings.Join(parts, ",") + `}}}`
	}
	for _, tc := range []struct{ body, loc string }{
		{`{"aggs":{"f":{"filter":{"field":"brand","op":"nope","value":1}}}}`, "aggs.f.filter.op"},
		{`{"aggs":{"f":{"filter":{"any":[]}}}}`, "aggs.f.filter.any"},
		{`{"aggs":{"f":{"filter":[]}}}`, "aggs.f.filter"},
		{`{"aggs":{"f":{"filters":{"filters":[{"all":[]}]}}}}`, "aggs.f.filters.filters"},
		{`{"aggs":{"f":{"filters":{"filters":{"a":{"field":"brand"}}}}}}`, "aggs.f.filters.filters.a.op"},
		{`{"aggs":{"f":{"filters":{"filters":{"":{"all":[]}}}}}}`, "aggs.f.filters.filters"},
		{`{"aggs":{"f":` + filters(MaxFilters+1) + `}}`, "aggs.f.filters.filters"},
		{`{"aggs":{"f":{"filters":{"filters":{"a":{"all":[]}},"other_bucket_key":7}}}}`, "aggs.f.filters.other_bucket_key"},
		{`{"aggs":{"f":{"filters":{"filters":{"a":{"all":[]}},"other_bucket":true}}}}`, "aggs.f.filters.other_bucket"},
		{`{"aggs":{"f":{"filters":[]}}}`, "aggs.f.filters"},
		{`{"aggs":{"g":{"global":{"x":1}}}}`, "aggs.g.global.x"},
		{`{"aggs":{"g":{"global":[]}}}`, "aggs.g.global"},
		{`{"aggs":{"f":{"filter":{"all":[]},"global":{}}}}`, "aggs.f"},
		{`{"aggs":{"f":{"filter":{"all":[]},"aggs":{"t":{"terms":{"field":"brand"},"aggs":{"s":{"stats":{"field":"price"},"aggs":{}}}}}}}}`, "aggs.f.aggs.t.aggs.s.aggs"},
		{`{"aggs":{"t":{"terms":{"field":"brand"},"aggs":{"u":{"filter":{"all":[]},"aggs":{}}}}}}`, "aggs.t.aggs.u.aggs"},
		{`{"post_filter":{"field":"price"}}`, "post_filter.op"},
		{`{"post_filter":{"not":{"field":"price"}}}`, "post_filter.not.op"},
	} {
		_, ps := ParseRequest([]byte(tc.body))
		if !slices.ContainsFunc(ps, func(p query.Problem) bool { return p.Loc == tc.loc }) {
			t.Errorf("%s: problems %v, want one at %s", tc.body, ps, tc.loc)
		}
	}
	if _, ps := ParseRequest([]byte(`{"aggs":{"f":` + filters(MaxFilters) + `}}`)); len(ps) > 0 {
		t.Errorf("%d filters: %v", MaxFilters, ps)
	}

	c := newCluster(t, 1)
	c.upsert("a", `{"price":1,"brand":"x"}`)
	c.refreshAll()
	all := &query.All{}
	terms := Agg{Type: AggTerms, Field: "brand"}
	for _, tc := range []struct {
		name string
		aggs map[string]Agg
		loc  string
	}{
		{"global under filter", map[string]Agg{"f": {Type: AggFilter, Filter: all, Aggs: map[string]Agg{"g": {Type: AggGlobal}}}}, "aggs.f.aggs.g"},
		{"global under terms", map[string]Agg{"t": {Type: AggTerms, Field: "brand", Aggs: map[string]Agg{"g": {Type: AggGlobal}}}}, "aggs.t.aggs.g"},
		{"filter under global", map[string]Agg{"g": {Type: AggGlobal, Aggs: map[string]Agg{"f": {Type: AggFilter, Filter: all}}}}, "aggs.g.aggs.f"},
		{"filters under filters", map[string]Agg{"s": {Type: AggFilters, Filters: map[string]query.Node{"a": all}, Aggs: map[string]Agg{"t": {Type: AggFilters, Filters: map[string]query.Node{"a": all}}}}}, "aggs.s.aggs.t"},
		{"filter under terms", map[string]Agg{"t": {Type: AggTerms, Field: "brand", Aggs: map[string]Agg{"f": {Type: AggFilter, Filter: all}}}}, "aggs.t.aggs.f"},
		{"bucket under a filter's terms", map[string]Agg{"f": {Type: AggFilter, Filter: all, Aggs: map[string]Agg{"t": {Type: AggTerms, Field: "brand", Aggs: map[string]Agg{"u": terms}}}}}, "aggs.f.aggs.t.aggs.u"},
		{"metric under a filter's stats", map[string]Agg{"f": {Type: AggFilter, Filter: all, Aggs: map[string]Agg{"s": {Type: AggStats, Field: "price", Aggs: map[string]Agg{"u": {Type: AggStats, Field: "price"}}}}}}, "aggs.f.aggs.s.aggs"},
		{"no filters", map[string]Agg{"s": {Type: AggFilters}}, "aggs.s.filters.filters"},
		{"other key taken", map[string]Agg{"s": {Type: AggFilters, Filters: map[string]query.Node{"a": all}, OtherBucketKey: "a"}}, "aggs.s.filters.other_bucket_key"},
		{"a child's problem", map[string]Agg{"f": {Type: AggFilter, Filter: all, Aggs: map[string]Agg{"r": {Type: AggRange, Field: "brand", Ranges: []Range{{}}}}}}, "aggs.f.aggs.r.range.field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			refusedAt(t, c, &Request{Query: all, Aggs: tc.aggs}, tc.loc)
		})
	}
	many := Agg{Type: AggFilters, Filters: map[string]query.Node{}}
	for i := range MaxFilters + 1 {
		many.Filters[fmt.Sprintf("k%03d", i)] = all
	}
	refusedAt(t, c, &Request{Query: all, Aggs: map[string]Agg{"s": many}}, "aggs.s.filters.filters")
	children := map[string]Agg{}
	for i := range MaxAggs - 1 {
		children[fmt.Sprintf("s%03d", i)] = Agg{Type: AggStats, Field: "price"}
	}
	if _, err := c.search(&Request{Query: all, Aggs: map[string]Agg{"g": {Type: AggGlobal, Aggs: children}}}); err != nil {
		t.Fatalf("a global holding %d aggregations: %v", MaxAggs-1, err)
	}
	children["s999"] = Agg{Type: AggStats, Field: "price"}
	refusedAt(t, c, &Request{Query: all, Aggs: map[string]Agg{"g": {Type: AggGlobal, Aggs: children}}}, "aggs")

	perBucket := func(keys int) Agg {
		a := Agg{Type: AggFilters, Filters: map[string]query.Node{}, OtherBucketKey: "rest", Aggs: map[string]Agg{
			"c": {Type: AggCardinality, Field: "brand"}, "s": {Type: AggStats, Field: "price"},
		}}
		for i := range keys {
			a.Filters[fmt.Sprintf("k%02d", i)] = all
		}
		return a
	}
	if _, err := c.search(&Request{Query: all, Aggs: map[string]Agg{"s": perBucket((MaxAggs-1)/2 - 1)}}); err != nil {
		t.Fatalf("filters of %d buckets holding 2 aggregations: %v", (MaxAggs-1)/2, err)
	}
	refusedAt(t, c, &Request{Query: all, Aggs: map[string]Agg{"s": perBucket((MaxAggs - 1) / 2)}}, "aggs")

	for _, tc := range []struct{ body, loc string }{
		{`{"aggs":{"f":{"filter":{"all":[]},"aggs":{"doc_count":{"stats":{"field":"price"}}}}}}`, "aggs.f.aggs.doc_count"},
		{`{"aggs":{"t":{"terms":{"field":"brand"},"aggs":{"key":{"stats":{"field":"price"}}}}}}`, "aggs.t.aggs.key"},
		{`{"aggregations":{"t":{"terms":{"field":"brand"},"aggregations":{"doc_count":{"stats":{"field":"price"}}}}}}`, "aggregations.t.aggregations.doc_count"},
		{`{"aggregations":{"g":{"global":{},"aggs":{"r":{"range":{"field":"brand","ranges":[{}]}}}}}}`, "aggregations.g.aggs.r.range.field"},
	} {
		r, ps := ParseRequest([]byte(tc.body))
		if len(ps) > 0 {
			t.Fatalf("%s: %v", tc.body, ps)
		}
		refusedAt(t, c, r, tc.loc)
	}
}

// ValidateQueries checks every query of a request against the mapping, each problem
// at its loc: the query's, post_filter's, and each aggregation filter's, nested ones
// included.
func TestValidateQueriesLocatesEveryQuery(t *testing.T) {
	r, ps := ParseRequest([]byte(`{
		"query": {"field": "nope", "op": "eq", "value": 1},
		"post_filter": {"all": [{"field": "brand", "op": "eq", "value": "x"}, {"field": "nope", "op": "eq", "value": 1}]},
		"aggs": {
			"f": {"filter": {"field": "price", "op": "contains", "value": "x"}},
			"s": {"filters": {"filters": {"ok": {"all": []}, "bad": {"field": "nope", "op": "exists"}}}},
			"g": {"global": {}, "aggs": {"in": {"filter": {"field": "nope", "op": "exists"}}}}
		}}`))
	if len(ps) > 0 {
		t.Fatal(ps)
	}
	got := ValidateQueries(r, testMapping)
	var locs []string
	for _, p := range got {
		locs = append(locs, p.Loc)
	}
	want := []string{"query.field", "post_filter.all.1.field", "aggs.f.filter.op", "aggs.g.aggs.in.filter.field", "aggs.s.filters.filters.bad.field"}
	if !slices.Equal(locs, want) {
		t.Fatalf("locs %v, want %v", locs, want)
	}
	r, _ = ParseRequest([]byte(`{"aggregations": {"g": {"global": {}, "aggregations": {"f": {"filter": {"field": "nope", "op": "exists"}}}}}}`))
	if ps := ValidateQueries(r, testMapping); len(ps) != 1 || ps[0].Loc != "aggregations.g.aggregations.f.filter.field" {
		t.Fatalf("aggregations spelled out: %v", ps)
	}
	r, _ = ParseRequest([]byte(`{"post_filter": {"all": []}, "aggs": {"f": {"filter": {"field": "brand", "op": "eq", "value": "x"}}}}`))
	if ps := ValidateQueries(r, testMapping); len(ps) > 0 {
		t.Fatalf("a valid request: %v", ps)
	}
}

// The buckets under filter, filters and global count toward MaxBuckets with theirs: a
// shard past it refuses the search at the nested aggregation, and merged shards past
// it are cut in name order inside the bucket, each cut aggregation marked truncated.
func TestNestedBucketsBoundTheResponse(t *testing.T) {
	t.Run("shard", func(t *testing.T) {
		c := newCluster(t, 1)
		for i := range 700 {
			c.upsert(fmt.Sprintf("d%03d", i), fmt.Sprintf(`{"price":%d}`, i))
		}
		c.refreshAll()
		fits := MaxBuckets / 700
		if _, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"g": {Type: AggGlobal, Aggs: histograms(fits)}}}); err != nil {
			t.Fatalf("1+%d buckets: %v", fits*700, err)
		}
		refusedAt(t, c, &Request{Query: &query.All{}, Aggs: map[string]Agg{"g": {Type: AggGlobal, Aggs: histograms(fits + 1)}}}, fmt.Sprintf("aggs.g.aggs.h%03d.histogram", fits))
	})
	t.Run("reduce", func(t *testing.T) {
		c := newCluster(t, 2)
		for i := range 800 {
			d, _, err := schema.Analyze(testMapping, fmt.Sprintf("d%03d", i), fmt.Appendf(nil, `{"price":%d}`, i))
			if err != nil {
				t.Fatal(err)
			}
			c.seq++
			c.apply(i/400, shard.Change{Seq: c.seq, Kind: shard.Upsert, Doc: &d})
		}
		c.refreshAll()
		resp, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"f": {Type: AggFilter, Filter: &query.All{}, Aggs: histograms(90)}}})
		if err != nil {
			t.Fatal(err)
		}
		f := resp.Aggs["f"]
		buckets, truncated := bucketsOf(&Response{Aggs: f.Aggs})
		if f.DocCount != 800 || buckets != MaxBuckets-1 {
			t.Fatalf("filter of %d documents holding %d buckets, want 800 and %d", f.DocCount, buckets, MaxBuckets-1)
		}
		if first := (MaxBuckets - 1) / 800; len(truncated) != 90-first || truncated[0] != fmt.Sprintf("h%03d", first) {
			t.Fatalf("truncated %v, want h%03d on", truncated, first)
		}
	})
}

// A filter, filters or global result is written as Elasticsearch writes it.
func TestFilterAggregationJSON(t *testing.T) {
	c := newCluster(t, 2)
	for i, brand := range []string{"acme", "acme", "initech", "hooli"} {
		c.upsert(fmt.Sprintf("d%d", i), fmt.Sprintf(`{"brand":%q,"price":%d}`, brand, i))
	}
	c.refreshAll()
	r, ps := ParseRequest([]byte(`{"size": 0, "query": {"field": "price", "op": "gte", "value": 1}, "aggs": {
		"f": {"filter": {"field": "brand", "op": "eq", "value": "acme"}, "aggs": {"p": {"stats": {"field": "price"}}}},
		"s": {"filters": {"filters": {"a": {"field": "brand", "op": "eq", "value": "acme"}, "i": {"field": "brand", "op": "eq", "value": "initech"}}, "other_bucket_key": "rest"}},
		"g": {"global": {}, "aggs": {"n": {"terms": {"field": "brand", "size": 1}}}}}}`))
	if len(ps) > 0 {
		t.Fatal(ps)
	}
	resp, err := c.search(r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(resp.Aggs)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"f":{"doc_count":1,"p":{"count":1,"min":1,"max":1,"avg":1,"sum":1}},` +
		`"g":{"doc_count":4,"n":{"doc_count_error_upper_bound":0,"sum_other_doc_count":2,"buckets":[{"doc_count":2,"key":"acme"}]}},` +
		`"s":{"buckets":{"a":{"doc_count":1},"i":{"doc_count":1},"rest":{"doc_count":1}}}}`
	if string(got) != want {
		t.Fatalf("aggs %s\nwant %s", got, want)
	}
}

// The shards' partials of filter, filters and global aggregations survive JSON, as a
// peer's do: the reduce of the decoded partials is the reduce of the originals.
func TestFilterPartialsSurviveJSON(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x75e1))
	c := newCluster(t, 3)
	for range 200 {
		c.upsert(pick(rng, ids)+pick(rng, []string{"", "-1", "-2"}), randomDoc(rng))
	}
	c.refreshAll()
	for range 20 {
		r := &Request{Query: &query.All{}, Aggs: map[string]Agg{"w": randomWrapped(t, rng), "v": randomWrapped(t, rng)}}
		var direct, decoded []*ShardResult
		for _, s := range c.shards {
			g := s.Acquire()
			res, err := ExecuteShard(context.Background(), g, r)
			g.Release()
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(res)
			if err != nil {
				t.Fatal(err)
			}
			var back ShardResult
			if err := json.Unmarshal(raw, &back); err != nil {
				t.Fatal(err)
			}
			direct, decoded = append(direct, res), append(decoded, &back)
		}
		want, err := json.Marshal(Reduce(context.Background(), direct, r).Aggs)
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.Marshal(Reduce(context.Background(), decoded, r).Aggs)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%+v: decoded %s\nwant %s", r.Aggs, got, want)
		}
	}
}

// unfilled is want with the empty buckets taken out of each histogram that got left
// unfilled (marked truncated) to keep the response within MaxBuckets: the same
// histogram under several filters buckets can pass it where brute force does not
// count.
func unfilled(got, want *AggResult) *AggResult {
	if got == nil || want == nil {
		return want
	}
	if got.Truncated && (want.Type == AggHistogram || want.Type == AggDateHistogram) {
		var kept []*Bucket
		for _, b := range want.Buckets {
			if b.DocCount > 0 {
				kept = append(kept, b)
			}
		}
		want.Buckets = kept
	}
	for name, w := range want.Aggs {
		want.Aggs[name] = unfilled(got.Aggs[name], w)
	}
	for i, wb := range want.Buckets {
		if i < len(got.Buckets) {
			for name, w := range wb.Aggs {
				wb.Aggs[name] = unfilled(got.Buckets[i].Aggs[name], w)
			}
		}
	}
	return want
}

// A filter aggregation's leaves are cached as the query's are: from the second request
// that holds one, the third served from the filter cache.
func TestFilterAggregationUsesTheFilterCache(t *testing.T) {
	c := newCluster(t, 1)
	for i := range 50 {
		c.upsert(fmt.Sprintf("d%02d", i), fmt.Sprintf(`{"price":%d}`, i))
	}
	c.refreshAll()
	defer leafUsage.reset()
	leafUsage.reset()
	cheap := &query.Leaf{Field: "price", Op: query.OpBetween, Value: json.RawMessage(`[3, 17]`)}
	var hits [3]int64
	for i := range hits {
		resp, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"f": {Type: AggFilter, Filter: cheap}}})
		if err != nil {
			t.Fatal(err)
		}
		if n := resp.Aggs["f"].DocCount; n != 15 {
			t.Fatalf("request %d: %d documents, want 15", i, n)
		}
		hits[i] = c.cache.Stats().Hits
	}
	if hits[0] != 0 || hits[1] != 0 || hits[2] == 0 {
		t.Fatalf("filter cache hits after each request %v: want none, none, then some", hits)
	}
}
