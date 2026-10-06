package search

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
)

// TestSearchAfterUnderConcurrentWrites pages through a sorted result while another
// goroutine updates, deletes, adds and refreshes. The documented semantics: every
// document live and unchanged for the whole walk appears exactly once; the walk is in
// strictly increasing (sort values, _id) order, so nothing repeats and no position is
// visited twice.
func TestSearchAfterUnderConcurrentWrites(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x9a6e))
	c := newCluster(t, 2)
	price := func() string { return fmt.Sprintf("%d", rng.IntN(40)) } // many ties
	for i := range 300 {
		c.upsert(fmt.Sprintf("s-%03d", i), fmt.Sprintf(`{"price":%s,"brand":"b%d"}`, price(), i%7))
		c.upsert(fmt.Sprintf("v-%03d", i), fmt.Sprintf(`{"price":%s,"brand":"b%d"}`, price(), i%5))
	}
	c.refreshAll()
	stable := map[string]bool{}
	for i := range 300 {
		stable[fmt.Sprintf("s-%03d", i)] = true
	}
	for _, sorts := range [][]SortField{
		{{Field: "price"}},
		{{Field: "price", Desc: true}, {Field: "brand"}},
		nil,
		{{Field: "brand", Desc: true}},
	} {
		var mu sync.Mutex
		stop := make(chan struct{})
		done := make(chan struct{})
		wrng := rand.New(rand.NewPCG(rng.Uint64(), 1))
		go func() {
			defer close(done)
			for {
				select {
				case <-stop:
					return
				default:
				}
				mu.Lock()
				id := fmt.Sprintf("v-%03d", wrng.IntN(400))
				switch wrng.IntN(4) {
				case 0:
					c.remove(id)
				default:
					c.upsert(id, fmt.Sprintf(`{"price":%d,"brand":"b%d"}`, wrng.IntN(40), wrng.IntN(9)))
				}
				if wrng.IntN(10) == 0 {
					c.refresh(wrng.IntN(2))
				}
				mu.Unlock()
			}
		}()
		var after []any
		var prev []any
		seen := map[string]int{}
		for page := 0; ; page++ {
			resp, err := c.search(&Request{Query: &query.All{}, Sort: sorts, Size: 13, SearchAfter: after})
			if err != nil {
				t.Fatal(err)
			}
			full := append(append([]SortField{}, sorts...), SortField{Field: IDField})
			for _, h := range resp.Hits {
				seen[h.ID]++
				if prev != nil && cmpSorts(prev, h.Sort, full) >= 0 {
					t.Fatalf("sort %+v page %d: %v does not follow %v", sorts, page, h.Sort, prev)
				}
				prev = h.Sort
			}
			if resp.Next == nil {
				break
			}
			after = resp.Next
		}
		close(stop)
		<-done
		for id := range stable {
			if seen[id] != 1 {
				t.Errorf("sort %+v: stable %s seen %d times", sorts, id, seen[id])
			}
		}
	}
}

func cmpSorts(a, b []any, sorts []SortField) int {
	for i := range a {
		desc := false
		if i < len(sorts) {
			desc = sorts[i].Desc
		}
		if c := cmpValue(a[i], b[i], desc); c != 0 {
			return c
		}
	}
	return 0
}

// TestNumberSortByPoints: number sorts found by walking the point index and by reading
// every hit's value, with ties, missing values, deletes, filters and cursors, equal
// brute force.
func TestNumberSortByPoints(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x50e7))
	c := newCluster(t, 2)
	for round := range 2 {
		for i := range 2500 {
			body := `{"brand":"x"}`
			switch rng.IntN(10) {
			case 0: // missing
			case 1:
				body = fmt.Sprintf(`{"price":%d,"brand":"y"}`, rng.IntN(5)) // ties
			default:
				body = fmt.Sprintf(`{"price":%v,"brand":%q}`, math.Round(math.Exp(rng.Float64()*8)*100)/100, pick(rng, []string{"x", "y", "z"}))
			}
			c.upsert(fmt.Sprintf("d%05d", rng.IntN(2500)+round*1000+i%3), body)
		}
		c.refreshAll()
	}
	for range 300 {
		c.remove(fmt.Sprintf("d%05d", rng.IntN(5000)))
	}
	c.refreshAll()
	defer func() { sortWalkFactor = 2 }()
	docs := c.docs()
	for range 150 {
		sorts := []SortField{{Field: "price", Desc: rng.IntN(2) == 0}}
		if rng.IntN(3) == 0 {
			sorts = append(sorts, SortField{Field: "brand", Desc: rng.IntN(2) == 0})
		}
		var q query.Node = &query.All{}
		if rng.IntN(2) == 0 {
			q = mustParse(t, fmt.Sprintf(`{"field":"brand","op":"eq","value":%q}`, pick(rng, []string{"x", "y", "z"})))
		}
		want := brute(docs, q, sorts)
		size := 1 + rng.IntN(25)
		var after []any
		start := 0
		if rng.IntN(2) == 0 && len(want) > 0 {
			start = 1 + rng.IntN(len(want)-1+1) - 1
			after = expectedSort(want[start], sorts)
			start++
		}
		wantIDs := docIDs(want[min(start, len(want)):min(start+size, len(want))])
		for _, factor := range []uint64{0, 1 << 40} {
			sortWalkFactor = factor
			got, err := c.search(&Request{Query: q, Sort: sorts, Size: size, SearchAfter: after})
			if err != nil {
				t.Fatal(err)
			}
			if ids := hitIDs(got.Hits); !slices.Equal(ids, wantIDs) {
				t.Fatalf("walk factor %d, sort %+v after %v size %d:\n got %q\nwant %q", factor, sorts, after, size, ids, wantIDs)
			}
		}
	}
}
