package search

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
)

// TestParseFrom: from is a whole number from 0, from + size at most MaxResultWindow,
// and 0 with search_after; each refusal is at loc from.
func TestParseFrom(t *testing.T) {
	r, ps := ParseRequest([]byte(`{"from": 30, "size": 20}`))
	if len(ps) > 0 || r.From != 30 || r.Size != 20 {
		t.Fatalf("from 30: %+v, %v", r, ps)
	}
	if r, ps := ParseRequest([]byte(`{}`)); len(ps) > 0 || r.From != 0 {
		t.Fatalf("no from: %+v, %v", r, ps)
	}
	if r, ps := ParseRequest([]byte(`{"from": 0, "search_after": ["a"]}`)); len(ps) > 0 || r.From != 0 || len(r.SearchAfter) != 1 {
		t.Fatalf("from 0 with search_after: %+v, %v", r, ps)
	}
	if r, ps := ParseRequest([]byte(fmt.Sprintf(`{"from": %d, "size": 10}`, MaxResultWindow-10))); len(ps) > 0 || r.From != MaxResultWindow-10 {
		t.Fatalf("from + size at the window: %+v, %v", r, ps)
	}
	for _, body := range []string{
		`{"from": -1}`,
		`{"from": 1.5}`,
		`{"from": null}`,
		`{"from": 1e12}`,
		fmt.Sprintf(`{"from": %d}`, MaxResultWindow+1),
		fmt.Sprintf(`{"from": %d, "size": 11}`, MaxResultWindow-10),
		`{"from": 10, "search_after": ["a"]}`,
	} {
		_, ps := ParseRequest([]byte(body))
		if len(ps) != 1 || ps[0].Loc != "from" {
			t.Errorf("%s: problems %v, want one at from", body, ps)
		}
	}
	_, ps = ParseRequest([]byte(`{"from": 10, "search_after": ["a"]}`))
	if !strings.Contains(ps[0].Message, "search_after") {
		t.Errorf("from with search_after: %q does not name search_after", ps[0].Message)
	}
	_, ps = ParseRequest([]byte(`{"from": 1000000}`))
	if !strings.Contains(ps[0].Message, "search_after") {
		t.Errorf("past the window: %q does not point to search_after", ps[0].Message)
	}
}

// TestFromRequestErrors: ExecuteShard refuses a from below 0, past the window, or with
// search_after, at loc from.
func TestFromRequestErrors(t *testing.T) {
	c := newCluster(t, 1)
	c.upsert("a", `{"price":1}`)
	c.refreshAll()
	g := c.shards[0].Acquire()
	defer g.Release()
	for _, r := range []*Request{
		{Query: &query.All{}, Size: 1, From: -1},
		{Query: &query.All{}, Size: 10, From: MaxResultWindow - 9},
		{Query: &query.All{}, Size: 1, From: 1, SearchAfter: []any{"a"}},
	} {
		var re *RequestError
		if _, err := ExecuteShard(context.Background(), g, r); !errors.As(err, &re) || re.Problems[0].Loc != "from" {
			t.Errorf("from %d size %d after %v: %v, want a RequestError at from", r.From, r.Size, r.SearchAfter, err)
		}
	}
}

// TestFromQueryThenFetch: the query-then-fetch the node runs (each shard's query phase
// with NoBodies, the reduce, then the bodies of the page alone) pages by from as brute
// force does, over shards with deletes, merges and unrefreshed writes: each shard
// returns its best from+size without a body, the reduce drops the first from, the page
// is fetched by its refs, and next is the page's last sort, from which search_after
// carries on where the next from would.
func TestFromQueryThenFetch(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0xf0f0))
	c := newCluster(t, 3)
	for round := range 3 {
		for i := range 120 {
			c.upsert(fmt.Sprintf("d%03d", rng.IntN(300)), fmt.Sprintf(`{"price":%d,"brand":"b%d","title":"t%d"}`, rng.IntN(30), rng.IntN(5), round*1000+i))
		}
		for range 20 {
			c.remove(fmt.Sprintf("d%03d", rng.IntN(300)))
		}
		c.refreshAll()
		if round == 1 {
			c.merge(rng.IntN(3), 1)
		}
	}
	for range 30 { // unrefreshed: not visible yet
		c.upsert(fmt.Sprintf("d%03d", rng.IntN(300)), `{"price":-1,"brand":"new"}`)
	}
	docs := c.docs()
	for range 60 {
		sorts := pick(rng, [][]SortField{nil, {{Field: "price"}}, {{Field: "price", Desc: true}, {Field: "brand"}}, {{Field: "brand", Desc: true}}})
		want := brute(docs, &query.All{}, sorts)
		size := 1 + rng.IntN(15)
		from := rng.IntN(len(want) + 5)
		r := &Request{Query: &query.All{}, Sort: sorts, Size: size, From: from, Fields: []string{"price"}, NoBodies: true}
		var parts []*ShardResult
		var gens []func()
		for _, s := range c.shards {
			g := s.Acquire()
			gens = append(gens, g.Release)
			res, err := ExecuteShard(context.Background(), g, r)
			if err != nil {
				t.Fatal(err)
			}
			if len(res.Hits) > from+size {
				t.Fatalf("a shard returned %d hits, past from+size %d", len(res.Hits), from+size)
			}
			for _, h := range res.Hits {
				if h.Body != nil || h.Ref == nil {
					t.Fatalf("query phase hit %+v", h)
				}
			}
			parts = append(parts, res)
		}
		resp := Reduce(context.Background(), parts, r)
		wantPage := want[min(from, len(want)):min(from+size, len(want))]
		if got, w := hitIDs(resp.Hits), docIDs(wantPage); !slices.Equal(got, w) {
			t.Fatalf("sort %+v from %d size %d: page %v, want %v", sorts, from, size, got, w)
		}
		for i := range resp.Hits {
			h := &resp.Hits[i]
			g := c.shards[c.route(h.ID)].Acquire()
			err := FetchShard(context.Background(), g, resp.Hits[i:i+1], r.Fields)
			g.Release()
			if err != nil {
				t.Fatal(err)
			}
			var body, doc map[string]any
			if err := json.Unmarshal(h.Body, &body); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(wantPage[i].Body, &doc); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(body, map[string]any{"price": doc["price"]}) {
				t.Fatalf("hit %q body %s, want the price of %s", h.ID, h.Body, wantPage[i].Body)
			}
		}
		for _, release := range gens {
			release()
		}
		if len(resp.Hits) < size {
			if resp.Next != nil {
				t.Fatalf("next %v on a short page", resp.Next)
			}
			continue
		}
		if !reflect.DeepEqual(resp.Next, resp.Hits[len(resp.Hits)-1].Sort) {
			t.Fatalf("next %v, want the last hit's sort %v", resp.Next, resp.Hits[len(resp.Hits)-1].Sort)
		}
		after, err := c.search(&Request{Query: &query.All{}, Sort: sorts, Size: size, SearchAfter: resp.Next})
		if err != nil {
			t.Fatal(err)
		}
		byFrom, err := c.search(&Request{Query: &query.All{}, Sort: sorts, Size: size, From: from + size})
		if err != nil {
			t.Fatal(err)
		}
		if a, f := hitIDs(after.Hits), hitIDs(byFrom.Hits); !slices.Equal(a, f) {
			t.Fatalf("search_after the from page's next gives %v, the next from %v", a, f)
		}
	}
}

// TestFromWithPostFilterAndAggs: from pages the post_filtered hits, ties broken by _id,
// while the total stays the post_filtered count and every aggregation counts all of
// the query's matches, whatever from is.
func TestFromWithPostFilterAndAggs(t *testing.T) {
	c := newCluster(t, 2)
	for i := range 60 {
		c.upsert(fmt.Sprintf("d%02d", i), fmt.Sprintf(`{"brand":%q,"price":%d}`, []string{"acme", "initech", "globex"}[i%3], i%7))
	}
	c.refreshAll()
	post := &query.Leaf{Field: "brand", Op: query.OpIn, Value: json.RawMessage(`["acme","globex"]`)}
	aggs := map[string]Agg{
		"brands": {Type: AggTerms, Field: "brand"},
		"price":  {Type: AggStats, Field: "price"},
		"all":    {Type: AggGlobal, Aggs: map[string]Agg{"n": {Type: AggCardinality, Field: "brand"}}},
	}
	sorts := []SortField{{Field: "price", Desc: true}}
	want := brute(c.docs(), &query.All{Children: []query.Node{&query.All{}, post}}, sorts)
	base, err := c.search(&Request{Query: &query.All{}, PostFilter: post, Aggs: aggs, Sort: sorts, Size: 0, TrackTotal: TrackTotalAll})
	if err != nil {
		t.Fatal(err)
	}
	baseAggs, err := json.Marshal(base.Aggs)
	if err != nil {
		t.Fatal(err)
	}
	var walked []string
	for from := 0; from < len(want)+5; from += 7 {
		resp, err := c.search(&Request{Query: &query.All{}, PostFilter: post, Aggs: aggs, Sort: sorts, Size: 7, From: from, TrackTotal: TrackTotalAll})
		if err != nil {
			t.Fatal(err)
		}
		if resp.Total != int64(len(want)) || resp.Total != 40 {
			t.Fatalf("from %d: total %d, want %d", from, resp.Total, len(want))
		}
		if w := docIDs(want[min(from, len(want)):min(from+7, len(want))]); !slices.Equal(hitIDs(resp.Hits), w) {
			t.Fatalf("from %d: page %v, want %v", from, hitIDs(resp.Hits), w)
		}
		walked = append(walked, hitIDs(resp.Hits)...)
		got, err := json.Marshal(resp.Aggs)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, baseAggs) {
			t.Fatalf("from %d: aggregations %s, want %s", from, got, baseAggs)
		}
	}
	if !slices.Equal(walked, docIDs(want)) {
		t.Fatalf("pages by from walk %v, want %v", walked, docIDs(want))
	}
	if bs := base.Aggs["brands"].Buckets; len(bs) != 3 {
		t.Fatalf("brands %v: want all three, the post_filter aside", bucketKeys(bs))
	}
}

// TestDeepFromReservesOnlyItsHits: a from at the end of the largest window, over a
// shard that matched three documents, allocates for those three, not for the window.
func TestDeepFromReservesOnlyItsHits(t *testing.T) {
	c := newCluster(t, 1)
	for i := range 3 {
		c.upsert(fmt.Sprintf("d%d", i), fmt.Sprintf(`{"price":%d}`, i))
	}
	c.refreshAll()
	g := c.shards[0].Acquire()
	defer g.Release()
	for _, sorts := range [][]SortField{nil, {{Field: "price"}}, {{Field: "brand", Desc: true}}} {
		r := &Request{Query: &query.All{}, Sort: sorts, Size: 10, From: MaxResultWindow - 10, NoBodies: true}
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		res, err := ExecuteShard(context.Background(), g, r)
		runtime.ReadMemStats(&after)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Hits) != 3 {
			t.Fatalf("sort %+v: %d hits, want the shard's 3", sorts, len(res.Hits))
		}
		if got := after.TotalAlloc - before.TotalAlloc; got > 4<<20 {
			t.Errorf("sort %+v: a window of %d over 3 hits allocated %d bytes", sorts, MaxResultWindow, got)
		}
	}
}
