package search

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
)

// TestRangeScanInParts: ranges checked on doc values in parallel parts of a segment, and
// collected from the point index, equal brute force, with deletes.
func TestRangeScanInParts(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x5ca7))
	c := newCluster(t, 1)
	for round := range 2 {
		for i := range 600 {
			c.upsert(fmt.Sprintf("r%04d", rng.IntN(900)+round*300+i%2), fmt.Sprintf(`{"price":%d,"brand":%q}`, rng.IntN(1000), pick(rng, []string{"a", "b"})))
		}
		for range 40 {
			c.remove(fmt.Sprintf("r%04d", rng.IntN(1200)))
		}
		c.refreshAll()
	}
	docs := c.docs()
	scanChunk = 64
	defer func() { scanChunk, docValuesFactor = 1<<16, defaultDocValuesFactor }()
	for range 60 {
		lo := rng.IntN(1000)
		raw := fmt.Sprintf(`{"all":[{"field":"brand","op":"eq","value":%q},{"field":"price","op":"between","value":[%d,%d]}]}`, pick(rng, []string{"a", "b"}), lo, lo+rng.IntN(600))
		q := mustParse(t, raw)
		want := docIDs(brute(docs, q, nil))
		for _, factor := range []float64{16, 0} {
			docValuesFactor = factor
			got, err := c.search(&Request{Query: q, Size: MaxSize, TrackTotal: TrackTotalAll})
			if err != nil {
				t.Fatal(err)
			}
			if ids := hitIDs(got.Hits); !slices.Equal(ids, want) {
				t.Fatalf("factor %v, %s:\n got %q\nwant %q", factor, raw, ids, want)
			}
		}
	}
}

// TestCardinalityPastExactByOrdinals: a keyword cardinality over more distinct values
// than a sketch keeps exactly, counted by global ordinal over several segments with
// deletes, estimates as a sketch of the same values does.
func TestCardinalityPastExactByOrdinals(t *testing.T) {
	c := newCluster(t, 1)
	for round := range 3 {
		for i := range 2500 {
			c.upsert(fmt.Sprintf("c%d-%d", round, i), fmt.Sprintf(`{"brand":"b%d","tags":["t%d","u%d"]}`, round*1700+i, i%4000, i))
		}
		for i := range 100 {
			c.remove(fmt.Sprintf("c%d-%d", round, i*7))
		}
		c.refreshAll()
	}
	for _, field := range []string{"brand", "tags"} {
		want := NewSketch(DefaultPrecision)
		seen := map[string]bool{}
		for _, d := range c.docs() {
			for _, v := range aggValues(d, field) {
				if s, _ := v.(string); !seen[s] {
					seen[s] = true
					want.Add(hashString(s) | 1)
				}
			}
		}
		got, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"n": {Type: AggCardinality, Field: field}}})
		if err != nil {
			t.Fatal(err)
		}
		if got.Aggs["n"].Value != want.Estimate() {
			t.Fatalf("%s: cardinality %d, want %d (of %d distinct)", field, got.Aggs["n"].Value, want.Estimate(), len(seen))
		}
	}
}

// TestGlobalOrdinalsCacheBounded: past its byte budget the cache drops the least
// recently used segment lists' ordinals, and searches still answer from new ones.
func TestGlobalOrdinalsCacheBounded(t *testing.T) {
	c := newCluster(t, 1)
	globalOrdsCacheBytes = 1
	defer func() { globalOrdsCacheBytes = 256 << 20 }()
	for i := range 5 {
		c.upsert(fmt.Sprintf("g%d", i), fmt.Sprintf(`{"brand":"b%d"}`, i))
		c.refreshAll()
		got, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"t": {Type: AggTerms, Field: "brand", Size: 10}}})
		if err != nil {
			t.Fatal(err)
		}
		if n := len(got.Aggs["t"].Buckets); n != i+1 {
			t.Fatalf("after %d documents: %d buckets", i+1, n)
		}
	}
	globalOrdsCache.mu.Lock()
	defer globalOrdsCache.mu.Unlock()
	if n := globalOrdsCache.lru.Len(); n != 1 {
		t.Fatalf("%d entries kept past the budget, want 1", n)
	}
}

// TestParsedRequestsCountedApart: the filter cache's admission counts every parsed
// request, even one that reuses an earlier request's memory.
func TestParsedRequestsCountedApart(t *testing.T) {
	leafUsage.reset()
	defer leafUsage.reset()
	body := []byte(`{"query":{"field":"price","op":"gt","value":123.25}}`)
	r1, ps := ParseRequest(body)
	if len(ps) > 0 {
		t.Fatal(ps)
	}
	if p := compileNode(r1.Query, requestID(r1)); p.leaf.useCache {
		t.Fatal("a first sighting is admitted")
	}
	r2, _ := ParseRequest(body)
	*r1 = *r2 // the next request, at the first one's address
	if p := compileNode(r1.Query, requestID(r1)); !p.leaf.useCache {
		t.Fatal("a second request reusing the first's memory is not counted")
	}
	shared := &Request{Query: r1.Query}
	first := compileNode(shared.Query, requestID(shared)).leaf.useCache
	if again := compileNode(shared.Query, requestID(shared)).leaf.useCache; again != first {
		t.Fatal("the shards of one request count it twice")
	}
}
