package search

import (
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// TestSearchEqualsBruteForce is the search's correctness property: over random
// documents (every field type, missing and mistyped values, values too long for
// grams), random updates, deletes, refreshes and merges on three shards, and random
// queries (every op, nested all/any/not, hand-built leaves), ExecuteShard and Reduce
// return exactly the documents query.Match accepts, in the request's order, with the
// right totals, and search_after pages through them with no duplicate or gap.
func TestSearchEqualsBruteForce(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x5eed))
	steps, checksPer := 300, 12
	if longMode() {
		steps, checksPer = 3000, 25
	}
	c := newCluster(t, 3)
	for step := range steps {
		switch x := rng.IntN(100); {
		case x < 55:
			c.upsert(pick(rng, ids), randomDoc(rng))
		case x < 70:
			c.remove(pick(rng, ids))
		case x < 78:
			c.refresh(rng.IntN(len(c.shards)))
		case x < 80:
			c.merge(rng.IntN(len(c.shards)), 1+rng.IntN(2))
		default:
			for range checksPer {
				checkRandomSearch(t, c, rng, step)
			}
		}
	}
	c.refreshAll()
	for range 20 * checksPer {
		checkRandomSearch(t, c, rng, steps)
	}
}

func checkRandomSearch(t *testing.T, c *cluster, rng *rand.Rand, step int) {
	t.Helper()
	q, raw := randomQuery(t, rng)
	sorts := pick(rng, sortChoices)
	r := &Request{
		Query:      q,
		Sort:       sorts,
		Size:       pick(rng, []int{0, 1, 3, 10, 1000}),
		TrackTotal: pick(rng, []int{0, 1, 5, TrackTotalAll, TrackTotalNone}),
	}
	want := brute(c.docs(), q, sorts)
	got, err := c.search(r)
	if err != nil {
		t.Fatalf("step %d: search %s: %v", step, raw, err)
	}
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("step %d: query %s sort %+v size %d: "+format, append([]any{step, raw, sorts, r.Size}, args...)...)
	}
	checkTotal(t, got, int64(len(want)), r.TrackTotal, fail)
	wantTop := want[:min(len(want), r.Size)]
	if len(got.Hits) != len(wantTop) {
		fail("%d hits, want %d (%v vs %v)", len(got.Hits), len(wantTop), hitIDs(got.Hits), docIDs(wantTop))
	}
	for i, h := range got.Hits {
		d := wantTop[i]
		if h.ID != d.ID {
			fail("hit %d is %q, want %q\n got %v\nwant %v", i, h.ID, d.ID, hitIDs(got.Hits), docIDs(wantTop))
		}
		if ws := expectedSort(d, sorts); !reflect.DeepEqual(h.Sort, ws) {
			fail("hit %d (%q) sort %#v, want %#v", i, h.ID, h.Sort, ws)
		}
		if string(h.Body) != string(d.Body) {
			fail("hit %d (%q) body %s, want %s", i, h.ID, h.Body, d.Body)
		}
	}
	if rng.IntN(3) == 0 && len(want) > 0 {
		pageThrough(t, c, q, sorts, 1+rng.IntN(7), want, fail)
	}
}

func checkTotal(t *testing.T, got *Response, n int64, trackTotal int, fail func(string, ...any)) {
	t.Helper()
	track := int64(trackTotal)
	if trackTotal == 0 {
		track = DefaultTrackTotal
	}
	switch {
	case trackTotal == TrackTotalNone:
		// No total asked for: whatever is reported must be honest.
		if got.Total > n || (got.TotalRelation == RelationEq && got.Total != n) {
			fail("total %d %s with %d matched", got.Total, got.TotalRelation, n)
		}
	case track >= 0 && n > track:
		if got.Total != track || got.TotalRelation != RelationGte {
			fail("total %d %s, want %d gte (%d matched)", got.Total, got.TotalRelation, track, n)
		}
	default:
		if got.Total != n || got.TotalRelation != RelationEq {
			fail("total %d %s, want %d eq", got.Total, got.TotalRelation, n)
		}
	}
}

// pageThrough walks every page of size n and checks the walk is want, exactly.
func pageThrough(t *testing.T, c *cluster, q query.Node, sorts []SortField, n int, want []*schema.Doc, fail func(string, ...any)) {
	t.Helper()
	var seen []string
	var after []any
	for pages := 0; ; pages++ {
		if pages > len(want)+2 {
			fail("paging does not end")
		}
		got, err := c.search(&Request{Query: q, Sort: sorts, Size: n, SearchAfter: after, TrackTotal: TrackTotalAll})
		if err != nil {
			fail("page %d after %#v: %v", pages, after, err)
		}
		for _, h := range got.Hits {
			seen = append(seen, h.ID)
		}
		if got.Next == nil {
			break
		}
		after = got.Next
	}
	if w := docIDs(want); !slices.Equal(seen, w) {
		fail("pages of %d walk %v, want %v", n, seen, w)
	}
}

func hitIDs(hs []Hit) []string {
	out := make([]string, len(hs))
	for i, h := range hs {
		out[i] = h.ID
	}
	return out
}

func docIDs(ds []*schema.Doc) []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.ID
	}
	return out
}

// TestEveryLeafEqualsBruteForce runs every op on every field, with many values, alone
// and negated, each twice (the second time through the filter cache), over a corpus
// spread across several segments with deletes: the per-op plans, one by one.
func TestEveryLeafEqualsBruteForce(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x1eaf))
	c := newCluster(t, 2)
	for round := range 4 {
		for range 80 {
			c.upsert(pick(rng, ids)+pick(rng, []string{"", "-x", "-y"}), randomDoc(rng))
		}
		for range 10 {
			c.remove(pick(rng, ids))
		}
		c.refreshAll()
		if round == 2 {
			c.merge(0, 1)
		}
	}
	values := 12
	if longMode() {
		values = 60
	}
	docs := c.docs()
	defer func() { docValuesFactor = 16 }()
	for _, f := range queryFields {
		for _, op := range query.Ops {
			for range values {
				raw := fmt.Sprintf(`{"field":%s,"op":%q,"value":%s}`, jsonString(f), op, randomValue(rng, op))
				if rng.IntN(4) == 0 {
					raw = `{"not":` + raw + `}`
				}
				q, ps := query.Parse([]byte(raw))
				if len(ps) > 0 {
					continue
				}
				want := docIDs(brute(docs, q, nil))
				for pass := range 3 {
					// Pass 2 collects ranges from the point index, never doc values.
					docValuesFactor = map[int]uint64{0: 16, 1: 16, 2: 0}[pass]
					got, err := c.search(&Request{Query: q, Size: MaxSize, TrackTotal: TrackTotalAll})
					if err != nil {
						t.Fatalf("%s: %v", raw, err)
					}
					if ids := hitIDs(got.Hits); !slices.Equal(ids, want) {
						t.Fatalf("pass %d: %s\n got %q\nwant %q", pass, raw, ids, want)
					}
				}
			}
		}
	}
}
