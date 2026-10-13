package search

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
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
