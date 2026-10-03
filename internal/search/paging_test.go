package search

import (
	"fmt"
	"math/rand/v2"
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
