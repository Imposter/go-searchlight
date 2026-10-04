package store

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestGateLanesAndFIFO: waiters get the gate first come, first served within a lane,
// and every high-lane waiter goes before any low-lane one.
func TestGateLanesAndFIFO(t *testing.T) {
	g := &gate{}
	ctx := context.Background()
	if err := g.acquire(ctx); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var order []string
	var wg sync.WaitGroup
	enqueue := func(name string, high bool) {
		c := ctx
		if high {
			c = withHighLane(ctx)
		}
		before := func() int {
			g.mu.Lock()
			defer g.mu.Unlock()
			return len(g.lanes[laneLow]) + len(g.lanes[laneHigh])
		}()
		wg.Go(func() {
			if err := g.acquire(c); err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			g.release()
		})
		for { // wait until it is queued, so the order is known
			g.mu.Lock()
			n := len(g.lanes[laneLow]) + len(g.lanes[laneHigh])
			g.mu.Unlock()
			if n > before {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
	enqueue("low1", false)
	enqueue("low2", false)
	enqueue("high1", true)
	enqueue("low3", false)
	enqueue("high2", true)
	// A waiter that gives up leaves the queue.
	cctx, cancel := context.WithCancel(withHighLane(ctx))
	gaveUp := make(chan error, 1)
	go func() { gaveUp <- g.acquire(cctx) }()
	time.Sleep(10 * time.Millisecond)
	cancel()
	if err := <-gaveUp; err == nil {
		t.Fatal("a cancelled waiter got the gate")
	}
	g.release()
	wg.Wait()
	want := "[high1 high2 low1 low2 low3]"
	if got := fmt.Sprint(order); got != want {
		t.Fatalf("order %s, want %s", got, want)
	}
}

// TestRenewalLatencyUnderBulkLoad (probe P3): on SQLite, whose writes share one
// connection, lease renewals made while writers keep committing large batches wait
// for about one transaction, never behind the queue of bulk commits.
func TestRenewalLatencyUnderBulkLoad(t *testing.T) {
	st := sqliteHarness(t).open(t)
	ctx := context.Background()
	if _, err := st.Indexes().Create(ctx, IndexMeta{Name: "bulk", Mapping: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	id := ShardID{Index: "bulk", Shard: 0}
	if _, ok, err := st.Registry().ClaimCopy(ctx, id, "n1", 1, 10*time.Second); err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	body := []byte(`{"title":"a document long enough to make a bulk commit take a while to write out","n":1}`)
	var commits atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 6 {
		wg.Go(func() {
			for b := 0; ; b++ {
				select {
				case <-stop:
					return
				default:
				}
				batch := make([]Change, 2000)
				for i := range batch {
					batch[i] = Change{Index: "bulk", Kind: KindUpsert, ID: fmt.Sprintf("w%d-%d-%d", w, b, i), Payload: body}
				}
				if _, _, err := st.Apply(ctx, batch); err != nil {
					t.Error(err)
					return
				}
				commits.Add(1)
			}
		})
	}
	time.Sleep(300 * time.Millisecond) // the writers queue up
	began, before := time.Now(), commits.Load()
	var renewals []time.Duration
	for range 25 {
		start := time.Now()
		if _, err := st.Registry().RenewLeases(ctx, "n1", 10*time.Second); err != nil {
			t.Fatal(err)
		}
		renewals = append(renewals, time.Since(start))
		time.Sleep(40 * time.Millisecond)
	}
	elapsed, done := time.Since(began), commits.Load()-before
	close(stop)
	wg.Wait()
	var worst time.Duration
	for _, d := range renewals {
		worst = max(worst, d)
	}
	// The writers hold the one connection back to back, so the mean time a commit
	// holds it is the window over the commits made in it. Queued behind six writers
	// in the pool's random order, a renewal would often wait for several.
	if done == 0 {
		t.Fatal("no bulk commit finished during the renewals")
	}
	commit := elapsed / time.Duration(done)
	sorted := slices.Clone(renewals)
	slices.Sort(sorted)
	median, p90 := sorted[len(sorted)/2], sorted[len(sorted)*9/10]
	s, ok := st.(*sqlStore)
	if !ok {
		t.Fatalf("%T is not the SQL store", st)
	}
	truncateHold := time.Duration(s.truncateHold.Load())
	t.Logf("renewals: median %s, p90 %s, slowest %s; a bulk commit (2000 changes) holds the connection %s on average (%d commits); "+
		"log file %d bytes, %d truncations holding it up to %s",
		median, p90, worst, commit, done, s.walSize(), s.truncates.Load(), truncateHold)
	if limit := 2*commit + 50*time.Millisecond; median > limit {
		t.Fatalf("the median renewal took %s under bulk load, more than about one commit in flight (%s)", median, limit)
	}
	if limit := 4*commit + 150*time.Millisecond; p90 > limit {
		t.Fatalf("a tenth of the renewals took over %s under bulk load (limit %s): they queue behind several commits", p90, limit)
	}
	if limit := 10 * commit; truncateHold > limit {
		t.Fatalf("a truncation of the log held the write connection %s (limit %s, ten bulk commits)", truncateHold, limit)
	}
	if limit := 8*commit + 300*time.Millisecond + truncateHold; worst > limit {
		t.Fatalf("a renewal took %s under bulk load (limit %s, a truncation of the log included): it queued behind several commits", worst, limit)
	}
	if limit := 2 * s.d.TruncateAbove; s.walSize() > limit {
		t.Fatalf("a steady stream of commits left a %d-byte log (limit %d): it never restarts on its own, and was not truncated", s.walSize(), limit)
	}
}
