package search

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// refusedAt runs r on c's first shard and fails unless it is refused with a problem
// at loc.
func refusedAt(t *testing.T, c *cluster, r *Request, loc string) {
	t.Helper()
	g := c.shards[0].Acquire()
	defer g.Release()
	_, err := ExecuteShard(context.Background(), g, r)
	var re *RequestError
	if !errors.As(err, &re) {
		t.Fatalf("%v, want a RequestError at %s", err, loc)
	}
	for _, p := range re.Problems {
		if p.Loc == loc {
			return
		}
	}
	t.Fatalf("problems %v, want one at %s", re.Problems, loc)
}

// A search sorts by at most MaxSortKeys keys, each field once, and nothing after _id
// (unique, so nothing after it can tie): the sort keys a hit carries stay few.
func TestSortKeysAreBounded(t *testing.T) {
	c := newCluster(t, 1)
	c.upsert("a", `{"price":1,"brand":"x"}`)
	c.refreshAll()
	keys := func(n int) []SortField {
		out := make([]SortField, n)
		for i := range out {
			out[i] = SortField{Field: fmt.Sprintf("f%d", i)}
		}
		return out
	}
	if _, err := c.search(&Request{Query: &query.All{}, Size: 1, Sort: keys(MaxSortKeys)}); err != nil {
		t.Fatalf("%d sort keys: %v", MaxSortKeys, err)
	}
	refusedAt(t, c, &Request{Query: &query.All{}, Size: 1, Sort: keys(MaxSortKeys + 1)}, fmt.Sprintf("sort.%d", MaxSortKeys))
	refusedAt(t, c, &Request{Query: &query.All{}, Size: 1, Sort: []SortField{{Field: "price"}, {Field: "brand"}, {Field: "price", Desc: true}}}, "sort.2")
	refusedAt(t, c, &Request{Query: &query.All{}, Size: 1, Sort: []SortField{{Field: IDField}, {Field: "price"}}}, "sort.1")
	resp, err := c.search(&Request{Query: &query.All{}, Size: 1, Sort: []SortField{{Field: "price"}, {Field: IDField, Desc: true}}})
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Hits[0].Sort; !slices.Equal(got, []any{1.0, "a"}) {
		t.Fatalf("sort values %v, want [1 a]", got)
	}
}

// A search holds at most MaxAggs aggregations (sub-aggregations count), MaxRanges
// ranges per range aggregation and MaxFields fields.
func TestAggregationsAndFieldsAreBounded(t *testing.T) {
	c := newCluster(t, 1)
	c.upsert("a", `{"price":1,"brand":"x"}`)
	c.refreshAll()
	stats := func(n int) map[string]Agg {
		out := make(map[string]Agg, n)
		for i := range n {
			out[fmt.Sprintf("s%03d", i)] = Agg{Type: AggStats, Field: "price"}
		}
		return out
	}
	if _, err := c.search(&Request{Query: &query.All{}, Aggs: stats(MaxAggs)}); err != nil {
		t.Fatalf("%d aggregations: %v", MaxAggs, err)
	}
	refusedAt(t, c, &Request{Query: &query.All{}, Aggs: stats(MaxAggs + 1)}, "aggs")
	withSubs := make(map[string]Agg)
	for i := range MaxAggs/3 + 1 {
		withSubs[fmt.Sprintf("t%03d", i)] = Agg{Type: AggTerms, Field: "brand", Aggs: stats(2)}
	}
	refusedAt(t, c, &Request{Query: &query.All{}, Aggs: withSubs}, "aggs")

	ranges := make([]Range, MaxRanges+1)
	for i := range ranges {
		ranges[i] = Range{From: ptr(float64(i))}
	}
	if _, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"r": {Type: AggRange, Field: "price", Ranges: ranges[:MaxRanges]}}}); err != nil {
		t.Fatalf("%d ranges: %v", MaxRanges, err)
	}
	refusedAt(t, c, &Request{Query: &query.All{}, Aggs: map[string]Agg{"r": {Type: AggRange, Field: "price", Ranges: ranges}}}, "aggs.r.range.ranges")

	fields := make([]string, MaxFields+1)
	for i := range fields {
		fields[i] = fmt.Sprintf("f%d", i)
	}
	if _, err := c.search(&Request{Query: &query.All{}, Size: 1, Fields: fields[:MaxFields]}); err != nil {
		t.Fatalf("%d fields: %v", MaxFields, err)
	}
	refusedAt(t, c, &Request{Query: &query.All{}, Size: 1, Fields: fields}, "fields")
}

// histograms returns n interval-1 histograms of price, named h000 on.
func histograms(n int) map[string]Agg {
	out := make(map[string]Agg, n)
	for i := range n {
		out[fmt.Sprintf("h%03d", i)] = Agg{Type: AggHistogram, Field: "price", Interval: 1}
	}
	return out
}

// bucketsOf counts a response's buckets over every aggregation, and its truncated
// aggregations.
func bucketsOf(resp *Response) (buckets int, truncated []string) {
	for name, a := range resp.Aggs {
		buckets += len(a.Buckets)
		if a.Truncated {
			truncated = append(truncated, name)
		}
	}
	slices.Sort(truncated)
	return buckets, truncated
}

// MaxBuckets bounds the buckets of the whole response, every aggregation together: a
// shard whose partial passes it refuses the search; merged shards past it are cut in
// aggregation name order, each cut aggregation marked truncated; and a histogram's
// empty buckets are filled in only while the response stays within it.
func TestBucketsBoundTheWholeResponse(t *testing.T) {
	t.Run("shard", func(t *testing.T) {
		c := newCluster(t, 1)
		for i := range 700 {
			c.upsert(fmt.Sprintf("d%03d", i), fmt.Sprintf(`{"price":%d}`, i))
		}
		c.refreshAll()
		if _, err := c.search(&Request{Query: &query.All{}, Aggs: histograms(MaxBuckets / 700)}); err != nil {
			t.Fatalf("%d buckets: %v", MaxBuckets/700*700, err)
		}
		refusedAt(t, c, &Request{Query: &query.All{}, Aggs: histograms(MaxBuckets/700 + 1)}, fmt.Sprintf("aggs.h%03d.histogram", MaxBuckets/700))
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
		resp, err := c.search(&Request{Query: &query.All{}, Aggs: histograms(100)})
		if err != nil {
			t.Fatal(err)
		}
		buckets, truncated := bucketsOf(resp)
		if buckets != MaxBuckets {
			t.Fatalf("%d buckets, want %d", buckets, MaxBuckets)
		}
		if first := MaxBuckets / 800; len(truncated) != 100-first || truncated[0] != fmt.Sprintf("h%03d", first) {
			t.Fatalf("truncated %v, want h%03d on", truncated, first)
		}
		if n := len(resp.Aggs["h000"].Buckets); n != 800 {
			t.Fatalf("h000: %d buckets, want 800", n)
		}
	})
	t.Run("fill", func(t *testing.T) {
		c := newCluster(t, 1)
		c.upsert("lo", `{"price":0}`)
		c.upsert("hi", fmt.Sprintf(`{"price":%d}`, MaxBuckets-1))
		c.refreshAll()
		resp, err := c.search(&Request{Query: &query.All{}, Aggs: histograms(1)})
		if err != nil {
			t.Fatal(err)
		}
		if n := len(resp.Aggs["h000"].Buckets); n != MaxBuckets {
			t.Fatalf("one histogram: %d buckets, want %d filled", n, MaxBuckets)
		}
		resp, err = c.search(&Request{Query: &query.All{}, Aggs: histograms(MaxAggs)})
		if err != nil {
			t.Fatal(err)
		}
		buckets, truncated := bucketsOf(resp)
		if buckets > MaxBuckets || len(truncated) > 0 {
			t.Fatalf("%d histograms: %d buckets, truncated %v; want at most %d, none truncated", MaxAggs, buckets, truncated, MaxBuckets)
		}
		for name, a := range resp.Aggs {
			var docs int64
			for _, b := range a.Buckets {
				docs += b.DocCount
			}
			if docs != 2 {
				t.Fatalf("%s: %d documents in its buckets, want 2", name, docs)
			}
		}
	})
}

// A fetch whose context is done stops and returns the context's error.
func TestFetchStopsWhenCanceled(t *testing.T) {
	c := newCluster(t, 1)
	for i := range 20 {
		c.upsert(fmt.Sprintf("d%02d", i), fmt.Sprintf(`{"price":%d}`, i))
	}
	c.refreshAll()
	g := c.shards[0].Acquire()
	defer g.Release()
	res, err := ExecuteShard(context.Background(), g, &Request{Query: &query.All{}, Size: 20, NoBodies: true})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := FetchShard(ctx, g, res.Hits, []string{"price"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("fetch under a canceled context: %v", err)
	}
}

// A histogram value whose bucket key overflows (or a date a calendar interval cannot
// place) is refused as too many buckets, not left out of every bucket.
func TestHistogramRefusesValuesItCannotBucket(t *testing.T) {
	c := newCluster(t, 1)
	c.upsert("small", `{"price":1,"created":0}`)
	c.upsert("huge", `{"price":1e308,"created":9e15}`)
	c.refreshAll()
	refusedAt(t, c, &Request{Query: &query.All{}, Aggs: map[string]Agg{"h": {Type: AggHistogram, Field: "price", Interval: 0.5}}}, "aggs.h.histogram")
	refusedAt(t, c, &Request{Query: &query.All{}, Aggs: map[string]Agg{"d": {Type: AggDateHistogram, Field: "created", Calendar: "year"}}}, "aggs.d.date_histogram")
}

// One shard's terms are exact: its doc_count_error_upper_bound is 0 even when it cut
// its list, as Elasticsearch reports.
func TestSingleShardTermsHaveNoErrorBound(t *testing.T) {
	c := newCluster(t, 1)
	for i, brand := range []string{"a", "a", "a", "b", "b", "c"} {
		c.upsert(fmt.Sprintf("d%d", i), fmt.Sprintf(`{"brand":%q}`, brand))
	}
	c.refreshAll()
	resp, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"t": {Type: AggTerms, Field: "brand", Size: 1, ShardSize: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	terms := resp.Aggs["t"]
	if terms.DocCountErrorUpperBound != 0 || terms.SumOtherDocCount != 3 || len(terms.Buckets) != 1 || terms.Buckets[0].DocCount != 3 {
		t.Fatalf("terms %+v, buckets %+v: want one exact bucket of 3, error bound 0, 3 others", terms, terms.Buckets)
	}
}
