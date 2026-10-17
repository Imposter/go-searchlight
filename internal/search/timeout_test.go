package search

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/testtier"
)

// TestTimeoutBoundsWallTime: over a large index, a search with a tiny timeout, many
// cardinality aggregations and a big terms aggregation over high-cardinality fields
// (global ordinals cold) answers within its timeout and a little slack, timed out,
// with the shard work, its merge and the reduce included.
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
		resp := Reduce(context.Background(), []*ShardResult{res}, r)
		return resp, time.Since(start)
	}
	_, untimed := search(0)
	t.Logf("untimed: %v over %d documents", untimed, n)
	clearOrdsCache(t)
	timeout := max(untimed/20, 5*time.Millisecond)
	resp, took := search(timeout)
	slack := 150 * time.Millisecond * testtier.RaceSlowdown
	bound := timeout + slack
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

// holdBuilders takes every global ordinals build slot until the returned function
// gives them back: builds queue meanwhile.
func holdBuilders(t testing.TB) func() {
	t.Helper()
	n := 0
	for ordsBuilders.tryAcquire() {
		n++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			for range n {
				ordsBuilders.release()
			}
		})
	}
}

// ordsCorpus is a shard of a few segments of high-cardinality keyword and list
// values.
func ordsCorpus(t testing.TB) *cluster {
	t.Helper()
	c := newCluster(t, 1)
	for round := range 3 {
		for i := range 2000 {
			c.upsert(fmt.Sprintf("o%d-%d", round, i), fmt.Sprintf(`{"brand":"v%d","tags":["a%d","b%d"]}`, round*2000+i, i, i%97))
		}
		c.refreshAll()
	}
	return c
}

// TestDeadlineWithoutOrdinalsMergesByTerm: a dense terms or cardinality aggregation
// whose global ordinals are still building when its search's deadline nears is
// merged by term, exactly and in time, not left empty.
func TestDeadlineWithoutOrdinalsMergesByTerm(t *testing.T) {
	clearOrdsCache(t)
	defer clearOrdsCache(t)
	defer func(d uint64) { globalOrdsDensity = d }(globalOrdsDensity)
	globalOrdsDensity = 1 << 40
	c := ordsCorpus(t)
	aggs := map[string]Agg{
		"t": {Type: AggTerms, Field: "brand", Size: 5, ShardSize: 100},
		"n": {Type: AggCardinality, Field: "tags"},
	}
	want, err := c.search(&Request{Query: &query.All{}, Aggs: aggs})
	if err != nil {
		t.Fatal(err)
	}
	clearOrdsCache(t)
	release := holdBuilders(t)
	defer release()
	for _, timeout := range []time.Duration{2 * time.Second, 0} {
		start := time.Now()
		got, err := c.search(&Request{Query: &query.All{}, Aggs: aggs, Timeout: timeout})
		if err != nil {
			t.Fatal(err)
		}
		if took := time.Since(start); took > time.Second*testtier.RaceSlowdown {
			t.Errorf("timeout %v: a search waited %v on builds queued behind others", timeout, took)
		}
		if got.TimedOut {
			t.Errorf("timeout %v: a search that merged by term in time is timed out", timeout)
		}
		for name := range aggs {
			if d := diffAgg(got.Aggs[name], want.Aggs[name], name); d != "" {
				t.Error(d)
			}
		}
	}
}

// TestBuildPoolServesSearchesFirst: a freed slot goes to a search's build (or a warm
// build a search joined, in queue order among them) before warm builds queued
// earlier.
func TestBuildPoolServesSearchesFirst(t *testing.T) {
	p := newBuildPool(1)
	if !p.tryAcquire() {
		t.Fatal("no free slot")
	}
	ctx := context.Background()
	warm1, warm2 := &poolWaiter{ch: make(chan struct{})}, &poolWaiter{ch: make(chan struct{})}
	search := &poolWaiter{ch: make(chan struct{}), urgent: true}
	order := make(chan string, 3)
	var wg sync.WaitGroup
	enqueue := func(name string, w *poolWaiter) {
		wg.Go(func() {
			if p.acquire(ctx, w) {
				order <- name
				p.release()
			}
		})
		for {
			p.mu.Lock()
			queued := slices.Contains(p.waiters, w)
			p.mu.Unlock()
			if queued {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
	enqueue("warm1", warm1)
	enqueue("warm2", warm2)
	enqueue("search", search)
	p.promote(warm2)
	p.release()
	wg.Wait()
	close(order)
	var got []string
	for name := range order {
		got = append(got, name)
	}
	if want := []string{"warm2", "search", "warm1"}; !slices.Equal(got, want) {
		t.Fatalf("slots went to %v, want %v", got, want)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if !p.tryAcquire() || p.acquire(cancelled, &poolWaiter{ch: make(chan struct{})}) {
		t.Fatal("a cancelled waiter got a slot")
	}
	p.release()
	if !p.tryAcquire() {
		t.Fatal("a cancelled waiter kept its place")
	}
}

// TestCancelDuringMergeIsAnError: a search cancelled while its shard merge waits on a
// global ordinals build returns the cancellation, not a timed-out answer.
func TestCancelDuringMergeIsAnError(t *testing.T) {
	clearOrdsCache(t)
	defer clearOrdsCache(t)
	defer func(d uint64) { globalOrdsDensity = d }(globalOrdsDensity)
	globalOrdsDensity = 1 << 40
	c := ordsCorpus(t)
	release := holdBuilders(t)
	defer release()
	defer func(d time.Duration) { ordsStartGrace = d }(ordsStartGrace)
	ordsStartGrace = time.Hour // the search waits on its queued build until cancelled
	ctx, cancel := context.WithCancel(context.Background())
	g := c.shards[0].Acquire()
	defer g.Release()
	errc := make(chan error, 1)
	go func() {
		_, err := ExecuteShard(ctx, g, &Request{Query: &query.All{}, Aggs: map[string]Agg{"n": {Type: AggCardinality, Field: "brand"}}})
		errc <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-errc:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled search: %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a cancelled search kept waiting on its build")
	}
}
