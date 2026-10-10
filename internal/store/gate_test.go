package store

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/testtier"
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
// connection, lease renewals made while writers keep committing large batches are not
// stuck behind the queue of bulk commits, and the log is truncated as it goes. The lane
// order that keeps a renewal ahead of the queue is TestGateLanesAndFIFO's, and
// truncate's bounds are TestTruncateBoundsItsHold's; here the latencies are measured
// and logged. The commits that land during a renewal (any that finish during its call,
// not only those granted ahead of it) are logged, not asserted. The mean commit time is
// the window over the commits made in it, since the writers hold the one connection
// back to back. Only sanity bounds fail: a median renewal past 50 commits, a truncation
// holding the connection over 10 s (how long it holds is the runner's disk, not the
// code), or a log that never restarts.
func TestRenewalLatencyUnderBulkLoad(t *testing.T) {
	testtier.Heavy(t)
	st := durableSQLiteHarness(t).open(t)
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
	var queuedAhead []int64
	for range 25 {
		start, pre := time.Now(), commits.Load()
		if _, err := st.Registry().RenewLeases(ctx, "n1", 10*time.Second); err != nil {
			t.Fatal(err)
		}
		renewals = append(renewals, time.Since(start))
		queuedAhead = append(queuedAhead, commits.Load()-pre)
		time.Sleep(40 * time.Millisecond)
	}
	elapsed, done := time.Since(began), commits.Load()-before
	close(stop)
	wg.Wait()
	var worst time.Duration
	for _, d := range renewals {
		worst = max(worst, d)
	}
	var landed int64
	for _, n := range queuedAhead {
		landed = max(landed, n)
	}
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
	t.Logf("renewals: median %s, p90 %s, slowest %s, at most %d commit(s) landed during any one; "+
		"a bulk commit (2000 changes) holds the connection %s on average (%d commits); "+
		"log file %d bytes, %d truncations holding it up to %s",
		median, p90, worst, landed, commit, done, s.walSize(), s.truncates.Load(), truncateHold)
	if limit := 50 * commit; median > limit {
		t.Fatalf("the median renewal took %s under bulk load, way past about one commit in flight (%s)", median, limit)
	}
	if limit := 10 * time.Second; truncateHold > limit {
		t.Fatalf("a truncation of the log held the write connection %s (limit %s): it copied or waited for something unbounded", truncateHold, limit)
	}
	if limit := 3 * s.d.TruncateAbove; s.truncates.Load() == 0 || s.walSize() > limit {
		t.Fatalf("a steady stream of commits left a %d-byte log after %d truncations (limit %d): it never restarts on its own",
			s.walSize(), s.truncates.Load(), limit)
	}
}

// TestTruncateBoundsItsHold: on SQLite, truncate skips (nothing run, nothing counted)
// when more than maxPending of the log is left to copy; holding the write connection,
// it waits for a reader that pins the log at most truncateBusyMS, not the connection's
// own busy_timeout, and returns with the log still there; and it leaves the
// connection's busy_timeout as it found it. The store's clock is fake, so its own
// checkpoint loop never truncates beside the test.
func TestTruncateBoundsItsHold(t *testing.T) {
	h := durableSQLiteHarness(t)
	st := h.open(t, WithClock(clock.NewFake(time.Now())))
	s, ok := st.(*sqlStore)
	if !ok {
		t.Fatalf("%T is not the SQL store", st)
	}
	ctx := context.Background()
	if _, err := st.Indexes().Create(ctx, IndexMeta{Name: "t", Mapping: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	busyTimeout := func() int64 {
		t.Helper()
		conn, err := s.w.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		var ms int64
		if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&ms); err != nil {
			t.Fatal(err)
		}
		return ms
	}
	was := busyTimeout()
	if was <= 20*truncateBusyMS {
		t.Fatalf("the write connection's busy_timeout is %d ms: too close to truncateBusyMS (%d) to tell them apart", was, truncateBusyMS)
	}

	d := *s.d
	d.PendingLog = func(string) (int64, error) { return 1 << 20, nil }
	real := s.d
	s.d = &d
	ran, err := s.truncate(ctx, 1<<20-1)
	s.d = real
	if err != nil || ran || s.truncates.Load() != 0 {
		t.Fatalf("1 MiB left to copy, at most 1 MiB - 1 allowed: ran %v, err %v, %d truncations; want a skip", ran, err, s.truncates.Load())
	}

	reader, err := sql.Open("sqlite", h.path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	tx, err := reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	var n int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if s.walSize() == 0 {
		t.Fatal("no log to truncate")
	}
	began := time.Now()
	ran, err = s.truncate(ctx, -1)
	took := time.Since(began)
	if err != nil || !ran {
		t.Fatalf("truncate under a pinning reader: ran %v, err %v", ran, err)
	}
	if limit := time.Duration(was) * time.Millisecond / 2; took >= limit {
		t.Fatalf("truncate waited %s for the reader: not bounded by truncateBusyMS (%d ms), as by the connection's own %d ms", took, truncateBusyMS, was)
	}
	if s.walSize() == 0 {
		t.Fatal("the log was truncated under a reader that pins it")
	}
	if got := busyTimeout(); got != was {
		t.Fatalf("busy_timeout %d ms after truncate, want the %d ms it found", got, was)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	if ran, err := s.truncate(ctx, -1); err != nil || !ran || s.walSize() != 0 {
		t.Fatalf("truncate with no reader: ran %v, err %v, log %d bytes; want it truncated", ran, err, s.walSize())
	}
	if got := s.truncates.Load(); got != 2 {
		t.Fatalf("%d truncations counted, want the 2 that ran", got)
	}
}
