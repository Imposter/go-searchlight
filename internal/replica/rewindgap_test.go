package replica

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
	"github.com/Imposter/go-searchlight/internal/testtier"
)

// writeDocs commits n documents of shard 0 of index, ids prefix0 to prefix<n-1>, in
// one batch, and returns the last seq.
func writeDocs(ctx context.Context, t *testing.T, st store.Store, index, prefix string, n int) int64 {
	t.Helper()
	batch := make([]store.Change, 0, n)
	for i := range n {
		batch = append(batch, upsert(index, 0, fmt.Sprintf("%s%d", prefix, i), docBody(prefix, i)))
	}
	_, last, err := st.Apply(ctx, batch)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	return last
}

// rewindCount is how many rewinds index's copies counted.
func rewindCount(t *testing.T, reader *sdkmetric.ManualReader, index string) int64 {
	t.Helper()
	return counterSum(t, reader, telemetry.MetricChangelogRewinds, telemetry.KeyIndex, index)
}

// TestRewindWhileStoppedRebuildsTheCopy: a copy stopped across a failover that lost
// commits, started again once new writes have passed its seq, sees from the mark its
// manifest kept that the change at its seq is not the one it applied, rebuilds, and
// serves the new writes.
func TestRewindWhileStoppedRebuildsTheCopy(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		st := d.open(t)
		createIndex(t, st, "w", testMapping)
		id := ShardID{Index: "w", Shard: 0}
		opts, reader := meteredOptions()
		c := newCopy(t, d.open(t), id, opts)
		c.start()
		k := mustApply(t, st, upsert("w", 0, "kept", docBody("kept", 1)))
		c.waitApplied(writeDocs(context.Background(), t, st, "w", "lost", 5))
		if err := c.stop(); err != nil {
			t.Fatal(err)
		}
		if err := c.shard().Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		rewind(context.Background(), t, d, k)
		writeDocs(context.Background(), t, st, "w", "new", 8)
		c.start()
		waitMatches(t, c, st, id)
		if n := rewindCount(t, reader, "w"); n < 1 {
			t.Fatalf("%d rewinds counted", n)
		}
	})
}

// TestRewindBeforeAPeerFetchRebuildsTheCopy: a copy fetched from a peer that has not
// yet seen the rewind holds the peer's applied changes, lost ones included; the mark
// the peer's manifest carries shows it, and the copy rebuilds from the store.
func TestRewindBeforeAPeerFetchRebuildsTheCopy(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "w", testMapping)
		id := ShardID{Index: "w", Shard: 0}
		seed := newCopy(t, d.open(t), id, testOptions())
		seed.start()
		k := mustApply(t, st, upsert("w", 0, "kept", docBody("kept", 1)))
		seed.waitApplied(writeDocs(context.Background(), t, st, "w", "lost", 5))
		if err := seed.stop(); err != nil {
			t.Fatal(err)
		}
		if err := seed.shard().Close(ctx); err != nil {
			t.Fatal(err)
		}
		rewind(context.Background(), t, d, k)
		writeDocs(context.Background(), t, st, "w", "new", 8)

		opts, reader := meteredOptions()
		f := &dirFetcher{from: seed.dir}
		opts.Fetcher = f
		c := newCopy(t, d.open(t), id, opts)
		c.start()
		waitMatches(t, c, st, id)
		if n := rewindCount(t, reader, "w"); n < 1 || f.calls != 1 {
			t.Fatalf("%d rewinds counted, %d fetches", n, f.calls)
		}
	})
}

// TestRewindDuringAsideRebuildRebuildsTheCopy: the changelog is rewound while a copy's
// replacement is built aside, after it caught up and before it is swapped in; new
// writes pass its seq. The replacement's mark shows it at its first poll, and it is
// rebuilt from the store.
func TestRewindDuringAsideRebuildRebuildsTheCopy(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "w", testMapping)
		id := ShardID{Index: "w", Shard: 0}
		opts, reader := meteredOptions()
		c := newCopy(t, d.open(t), id, opts)
		c.start()
		c.waitApplied(mustApply(t, st, upsert("w", 0, "a", docBody("a", 1))))
		if err := c.stop(); err != nil {
			t.Fatal(err)
		}
		if err := c.shard().Close(ctx); err != nil {
			t.Fatal(err)
		}
		k := mustApply(t, st, upsert("w", 0, "kept", docBody("kept", 1)))
		if err := st.Prune(ctx, id, k+1); err != nil { // past the copy: rebuilt aside
			t.Fatal(err)
		}
		writeDocs(context.Background(), t, st, "w", "lost", 5)
		var once sync.Once
		rewound := make(chan struct{})
		c.opts.hooks = &testHooks{beforeSwap: func(hctx context.Context, _ string) error {
			once.Do(func() {
				rewind(context.WithoutCancel(hctx), t, d, k)
				writeDocs(context.WithoutCancel(hctx), t, st, "w", "new", 8)
				close(rewound)
			})
			return nil
		}}
		c.start()
		select {
		case <-rewound:
		case <-time.After(time.Minute):
			t.Fatal("the aside rebuild never reached its swap")
		}
		waitMatches(t, c, st, id)
		if n := rewindCount(t, reader, "w"); n < 1 {
			t.Fatalf("%d rewinds counted", n)
		}
	})
}

// TestRewindDuringAsideCatchUpRebuildsTheCopy: the changelog is rewound while a
// replacement built aside is catching up, page by page: the catch-up sees it, and the
// copy is rebuilt from the store instead.
func TestRewindDuringAsideCatchUpRebuildsTheCopy(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "w", testMapping)
		id := ShardID{Index: "w", Shard: 0}
		fs := newFaultStore(d.open(t))
		opts, reader := meteredOptions()
		c := newCopy(t, fs, id, opts)
		c.start()
		c.waitApplied(mustApply(t, st, upsert("w", 0, "a", docBody("a", 1))))
		if err := c.stop(); err != nil {
			t.Fatal(err)
		}
		if err := c.shard().Close(ctx); err != nil {
			t.Fatal(err)
		}
		k := mustApply(t, st, upsert("w", 0, "kept", docBody("kept", 1)))
		if err := st.Prune(ctx, id, k+1); err != nil {
			t.Fatal(err)
		}
		lost := writeDocs(context.Background(), t, st, "w", "lost", 5)
		var once sync.Once
		fs.mu.Lock()
		fs.onChanges = func(cctx context.Context, seq int64) error {
			if seq >= lost-2 {
				once.Do(func() {
					rewind(context.WithoutCancel(cctx), t, d, k)
					writeDocs(context.WithoutCancel(cctx), t, st, "w", "new", 8)
				})
			}
			return nil
		}
		fs.mu.Unlock()
		c.opts.BatchSize = 2 // the catch-up reads in pages, polling the head between them
		c.start()
		waitMatches(t, c, st, id)
		if n := rewindCount(t, reader, "w"); n < 1 {
			t.Fatalf("%d rewinds counted", n)
		}
	})
}

// TestHeadBehindOnceIsNotARewind: one poll that reads a head behind the copy's seq (a
// read that reached a lagging server) rebuilds nothing; only a head behind on the next
// poll too counts as a rewind.
func TestHeadBehindOnceIsNotARewind(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		st := d.open(t)
		createIndex(t, st, "w", testMapping)
		id := ShardID{Index: "w", Shard: 0}
		fs := newFaultStore(d.open(t))
		opts, reader := meteredOptions()
		c := newCopy(t, fs, id, opts)
		c.start()
		head := c.waitApplied(writeDocs(context.Background(), t, st, "w", "a", 3)).AppliedSeq()
		var polls atomic.Int32
		fs.mu.Lock()
		fs.onTip = func(tip *store.Tip) {
			if polls.Add(1) == 1 {
				tip.Head, tip.HeadMark = head-2, store.ChangeMark{}
			}
		}
		fs.mu.Unlock()
		for polls.Load() < 3 {
			c.current().Wake()
			time.Sleep(5 * time.Millisecond)
		}
		sh := c.waitApplied(writeDocs(context.Background(), t, st, "w", "b", 2))
		if n := rewindCount(t, reader, "w"); n != 0 {
			t.Fatalf("%d rewinds counted for one lagging read", n)
		}
		if dd := diff(viewOf(t, sh), truthOf(t, st, id), false); dd != "" {
			t.Fatalf("the copy differs:\n%s", dd)
		}
	})
}

// TestNoFalseRewinds: with concurrent writers on several indexes, pruning up to the
// head (which removes the rows of idle copies' marks), an unrelated index dropped and
// recreated, and copies restarted and crashed (reopening from their manifests' marks),
// no rewind is ever counted.
func TestNoFalseRewinds(t *testing.T) {
	testtier.Heavy(t)
	forEachDialect(t, func(t *testing.T, d *db) {
		st := d.open(t)
		createIndex(t, st, "w", testMapping)
		createIndex(t, st, "v", testMapping)
		createIndex(t, st, "x", testMapping)
		opts, reader := meteredOptions()
		c0 := newCopy(t, d.open(t), ShardID{Index: "w", Shard: 0}, opts)
		c1 := newCopy(t, d.open(t), ShardID{Index: "v", Shard: 0}, opts)
		c0.start()
		c1.start()

		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		defer cancel()
		var wg sync.WaitGroup
		var xMu sync.RWMutex
		for g := range 4 {
			wg.Go(func() {
				for i := 0; ctx.Err() == nil; i++ {
					var batch []store.Change
					switch {
					case g == 0:
						batch = append(batch, upsert("w", 0, fmt.Sprintf("a%d", i%50), docBody("a", i)))
					case g == 1 && i%10 == 0:
						batch = append(batch, upsert("v", 0, fmt.Sprintf("b%d", i%5), docBody("b", i)))
					}
					xMu.RLock()
					batch = append(batch, upsert("x", 0, fmt.Sprintf("x%d", i%20), docBody("x", i)))
					_, _, _ = st.Apply(context.WithoutCancel(ctx), batch)
					xMu.RUnlock()
				}
			})
		}
		wg.Go(func() {
			for ctx.Err() == nil {
				time.Sleep(150 * time.Millisecond)
				if head, _, err := st.HeadSeq(context.WithoutCancel(ctx)); err == nil {
					_ = st.Prune(context.WithoutCancel(ctx), store.ShardID{Index: "x", Shard: 0}, head+1)
				}
			}
		})
		time.Sleep(time.Second)
		c1.restart()
		xMu.Lock()
		if err := st.Indexes().Drop(context.Background(), "x"); err != nil {
			t.Error(err)
		}
		createIndex(t, st, "x", testMapping)
		xMu.Unlock()
		time.Sleep(500 * time.Millisecond)
		c0.crash()
		time.Sleep(500 * time.Millisecond)
		c1.restart()
		wg.Wait()

		head := mustApply(t, st, upsert("w", 0, "last", docBody("last", 1)))
		c0.waitApplied(head)
		c1.waitApplied(head)
		c0.restart()
		c1.restart()
		c0.waitApplied(head)
		c1.waitApplied(head)
		if n := rewindCount(t, reader, "w") + rewindCount(t, reader, "v"); n != 0 {
			t.Fatalf("%d rewinds counted with no rewind", n)
		}
	})
}
