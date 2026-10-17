package search

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/testtier"
)

// TestTimeoutBoundsWallTime: over a large index, a search with a tiny timeout, many
// cardinality aggregations and a big terms aggregation over high-cardinality fields
// (global ordinals cold) answers within its timeout plus the reserve and a little
// slack, timed out, with the shard work, its merge and the reduce included.
func TestTimeoutBoundsWallTime(t *testing.T) {
	clearOrdsCache(t)
	defer clearOrdsCache(t)
	defer func(d uint64) { globalOrdsDensity = d }(globalOrdsDensity)
	globalOrdsDensity = 1 << 40
	c := newCluster(t, 1)
	n := testtier.Pick(60_000, 200_000)
	bulkLoad(t, c.shards[0], n, n/6, func(i int) string {
		return fmt.Sprintf(`{"brand":"b%d","tags":["t%d","u%d","v%d"],"title":"w%d x%d","price":%d}`, i, i, i/3, i%5000, i, i/7, i%1000)
	})
	aggs := map[string]Agg{"top": {Type: AggTerms, Field: "brand", Size: 10, ShardSize: 10_000}}
	for i := range 40 {
		f := []string{"brand", "tags", "title"}[i%3]
		aggs[fmt.Sprintf("c%d", i)] = Agg{Type: AggCardinality, Field: f}
	}
	search := func(timeout time.Duration) (*Response, time.Duration) {
		start := time.Now()
		r := &Request{Query: &query.All{}, Aggs: aggs, Timeout: timeout, Size: 10}
		g := c.shards[0].Acquire()
		res, err := ExecuteShard(context.Background(), g, r)
		g.Release()
		if err != nil {
			t.Fatal(err)
		}
		rctx, cancel := ReduceContext(context.Background(), time.Since(start), r)
		defer cancel()
		resp := Reduce(rctx, []*ShardResult{res}, r)
		return resp, time.Since(start)
	}
	_, untimed := search(0)
	t.Logf("untimed: %v over %d documents", untimed, n)
	clearOrdsCache(t)
	timeout := max(untimed/20, 5*time.Millisecond)
	resp, took := search(timeout)
	slack := 150 * time.Millisecond * testtier.RaceSlowdown
	bound := timeout + 2*DeadlineReserve(timeout) + slack
	t.Logf("timeout %v: took %v (bound %v), timed out %v", timeout, took, bound, resp.TimedOut)
	if took > bound {
		t.Errorf("a search with timeout %v took %v, past %v", timeout, took, bound)
	}
	if untimed > 4*bound && !resp.TimedOut {
		t.Error("a search cut far short of its work is not timed out")
	}
	waitOrdsIdle(t)
	if _, again := search(0); again > untimed*3 {
		t.Logf("after the timed-out search the ordinals are cached: %v", again)
	}
}

// TestReduceHonoursDeadline: a reduce whose context has ended answers every
// aggregation, empty, and is timed out; ReduceContext outlives the request's own
// deadline by the reserve, and ends with its cancellation.
func TestReduceHonoursDeadline(t *testing.T) {
	r := &Request{Aggs: map[string]Agg{
		"t": {Type: AggTerms, Field: "brand"},
		"f": {Type: AggFilter, Filter: &query.All{}, Aggs: map[string]Agg{"c": {Type: AggCardinality, Field: "brand"}}},
	}}
	parts := []*ShardResult{{Aggs: map[string]*AggPartial{"t": {Type: AggTerms, Buckets: []*BucketPartial{{Key: "a", DocCount: 2}}}}}}
	ended, cancel := context.WithCancel(context.Background())
	cancel()
	resp := Reduce(ended, parts, r)
	if !resp.TimedOut || resp.Aggs["t"] == nil || len(resp.Aggs["t"].Buckets) != 0 || resp.Aggs["f"] == nil {
		t.Fatalf("reduce past its deadline: timed out %v, aggs %+v", resp.TimedOut, resp.Aggs)
	}
	if resp := Reduce(context.Background(), parts, r); resp.TimedOut || len(resp.Aggs["t"].Buckets) != 1 {
		t.Fatalf("reduce in time: %+v", resp)
	}

	parent, stop := context.WithDeadline(context.Background(), time.Now().Add(20*time.Millisecond))
	defer stop()
	rctx, done := ReduceContext(parent, 0, &Request{Timeout: time.Hour})
	defer done()
	<-parent.Done()
	if rctx.Err() != nil {
		t.Fatal("the reduce context ended with the request's deadline, leaving the reduce no time")
	}
	select {
	case <-rctx.Done():
	case <-time.After(time.Second):
		t.Fatal("the reduce context outlived the request's deadline by more than the reserve")
	}
	parent, stop = context.WithCancel(context.Background())
	rctx, done = ReduceContext(parent, 0, &Request{Timeout: time.Hour})
	defer done()
	stop()
	select {
	case <-rctx.Done():
	case <-time.After(time.Second):
		t.Fatal("the reduce context outlived its request's cancellation")
	}
}
