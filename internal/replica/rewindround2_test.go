package replica

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/store"
)

// TestHeadBehindOnceKeepsAHaltedCopyHalted: one poll that reads a head behind a halted
// copy's seq neither clears the halt nor promotes the copy: its registry row stays
// recovering.
func TestHeadBehindOnceKeepsAHaltedCopyHalted(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		const damaged = `{"title":"damaged"}`
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "c", testMapping)
		id := ShardID{Index: "c", Shard: 0}
		cp, ok, err := st.Registry().ClaimCopy(ctx, id, "node-a", 1, time.Minute)
		if err != nil || !ok {
			t.Fatal(ok, err)
		}
		fs := newFaultStore(d.open(t))
		fs.corrupt = func(c *store.Change) bool { return string(c.Payload) == damaged }
		opts := testOptions()
		opts.Copy = &cp
		opts.HaltRetryBase, opts.HaltRetryCap = 20*time.Millisecond, 50*time.Millisecond
		c := newCopy(t, fs, id, opts)
		c.start()
		c.waitApplied(mustApply(t, st, upsert("c", 0, "ok1", docBody("ok1", 1))))
		mustApply(t, st, upsert("c", 0, "bad", damaged))
		for deadline := time.Now().Add(30 * time.Second); c.current().Halt() == nil; {
			if time.Now().After(deadline) {
				t.Fatal("the copy never halted")
			}
			c.current().Wake()
			time.Sleep(5 * time.Millisecond)
		}
		waitCopyState(t, st, id, store.CopyRecovering)
		applied := c.current().Applied()
		var polls atomic.Int32
		fs.mu.Lock()
		fs.onTip = func(tip *store.Tip) {
			if polls.Add(1) == 1 {
				tip.Head = applied - 1
			}
		}
		fs.mu.Unlock()
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
			if c.current().Halt() == nil {
				t.Fatal("one lagging read cleared the halt")
			}
			copies, err := st.Registry().Copies(ctx, "c")
			if err == nil && len(copies) == 1 && copies[0].State == store.CopyServing {
				t.Fatal("one lagging read promoted the halted copy to serving")
			}
			time.Sleep(2 * time.Millisecond)
		}
		if polls.Load() < 2 {
			t.Fatalf("only %d polls in two seconds", polls.Load())
		}
	})
}

// TestRewindOnTheAsideCatchUpsLastPageRebuildsTheCopy: the changelog is rewound between
// the aside catch-up's last Tip and its last page, which then holds the new history.
// The replacement's mark of that page is unverified: it is carried into the tailer as
// pending, not trusted, and the next poll's check of the verified anchor finds the
// rewind.
func TestRewindOnTheAsideCatchUpsLastPageRebuildsTheCopy(t *testing.T) {
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
		if err := st.Prune(ctx, id, k+1); err != nil { // past the copy: rebuilt aside
			t.Fatal(err)
		}
		lost := writeDocs(ctx, t, st, "w", "lost", 5)
		var once sync.Once
		fs.mu.Lock()
		fs.onChanges = func(cctx context.Context, seq int64) error {
			if seq == lost {
				once.Do(func() {
					rewind(context.WithoutCancel(cctx), t, d, k)
					writeDocs(context.WithoutCancel(cctx), t, st, "w", "new", 8)
				})
			}
			return nil
		}
		fs.mu.Unlock()
		c.start()
		waitMatches(t, c, st, id)
		if n := rewindCount(t, reader, "w"); n < 1 {
			t.Fatalf("%d rewinds counted", n)
		}
	})
}

// TestRewindBetweenTipAndPageThenRestartRebuildsTheCopy: a poll's page is read across a
// rewind (after its Tip checked the anchor), and the copy stops before the next poll.
// Its manifest keeps the verified anchor beside the unverified mark of that page, so
// the restarted copy checks the anchor first, finds the rewind, and rebuilds.
func TestRewindBetweenTipAndPageThenRestartRebuildsTheCopy(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "w", testMapping)
		id := ShardID{Index: "w", Shard: 0}
		fs := newFaultStore(d.open(t))
		opts, reader := meteredOptions()
		opts.PollInterval, opts.WatchedPollInterval = time.Hour, time.Hour
		c := newCopy(t, fs, id, opts)
		c.start()
		k := mustApply(t, st, upsert("w", 0, "kept", docBody("kept", 1)))
		lost := writeDocs(ctx, t, st, "w", "lost", 5)
		c.waitApplied(lost)
		c.current().Wake() // one more poll: the copy's anchor is lost4's mark
		time.Sleep(200 * time.Millisecond)
		var done atomic.Bool
		fs.mu.Lock()
		fs.onChanges = func(cctx context.Context, seq int64) error {
			if seq == lost && !done.Swap(true) {
				rewind(context.WithoutCancel(cctx), t, d, k)
				writeDocs(context.WithoutCancel(cctx), t, st, "w", "new", 8)
			}
			return nil
		}
		fs.mu.Unlock()
		c.current().Wake()
		for deadline := time.Now().Add(30 * time.Second); !done.Load(); {
			if time.Now().After(deadline) {
				t.Fatal("the poll never read its page")
			}
			time.Sleep(time.Millisecond)
		}
		time.Sleep(200 * time.Millisecond)
		if err := c.stop(); err != nil {
			t.Fatal(err)
		}
		if err := c.shard().Close(ctx); err != nil {
			t.Fatal(err)
		}
		fs.mu.Lock()
		fs.onChanges = nil
		fs.mu.Unlock()
		c.opts.PollInterval, c.opts.WatchedPollInterval = 10*time.Millisecond, 200*time.Millisecond
		c.start()
		waitMatches(t, c, st, id)
		if n := rewindCount(t, reader, "w"); n < 1 {
			t.Fatalf("%d rewinds counted", n)
		}
	})
}

// TestRewindRightAfterASnapshotLoadRebuildsTheCopy: the changelog is rewound right after
// a rebuild's snapshot scan, and new writes pass the snapshot's seq. The mark the copy
// keeps comes from the scan's own snapshot, so it is the lost change's, and the first
// poll finds the rewind.
func TestRewindRightAfterASnapshotLoadRebuildsTheCopy(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "w", testMapping)
		id := ShardID{Index: "w", Shard: 0}
		fs := newFaultStore(d.open(t))
		k := mustApply(t, st, upsert("w", 0, "kept", docBody("kept", 1)))
		writeDocs(ctx, t, st, "w", "lost", 5)
		var once sync.Once
		fs.mu.Lock()
		fs.afterScan = func(sctx context.Context) {
			once.Do(func() {
				rewind(context.WithoutCancel(sctx), t, d, k)
				writeDocs(context.WithoutCancel(sctx), t, st, "w", "new", 8)
			})
		}
		fs.mu.Unlock()
		opts, reader := meteredOptions()
		c := newCopy(t, fs, id, opts)
		c.start()
		waitMatches(t, c, st, id)
		if n := rewindCount(t, reader, "w"); n < 1 {
			t.Fatalf("%d rewinds counted", n)
		}
	})
}

// TestStoppedAcrossARewindWhoseMarkRowWasPrunedRebuildsTheCopy: a copy whose marks
// name another shard's row (the head it advanced to) is stopped across a lossy
// failover. The lost seqs are reissued, two to the copy's own shard and the rest to
// the other shard, which then prunes them all (its copies applied them) before the
// copy starts again: no row is left at its marks' seqs. The copy's own shard shows
// the rewind all the same (a change of its shard it never applied, at or below the
// seq it checked), and the copy rebuilds.
func TestStoppedAcrossARewindWhoseMarkRowWasPrunedRebuildsTheCopy(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "w", testMapping)
		createIndex(t, st, "x", testMapping)
		id := ShardID{Index: "w", Shard: 0}
		opts, reader := meteredOptions()
		c := newCopy(t, d.open(t), id, opts)
		c.start()
		k := mustApply(t, st, upsert("w", 0, "kept", docBody("kept", 1)))
		mustApply(t, st, upsert("w", 0, "lost0", docBody("lost", 0)))
		var last int64
		for i := range 4 {
			last = mustApply(t, st, upsert("x", 0, fmt.Sprintf("xl%d", i), docBody("xl", i)))
		}
		c.waitApplied(last)
		for range 3 { // polls check and promote the marks, the last an x/0 row
			c.current().Wake()
			time.Sleep(100 * time.Millisecond)
		}
		if err := c.stop(); err != nil {
			t.Fatal(err)
		}
		if err := c.shard().Close(ctx); err != nil {
			t.Fatal(err)
		}
		rewind(ctx, t, d, k)
		var batch []store.Change
		for i := range 6 {
			index := "x"
			if i < 2 {
				index = "w"
			}
			batch = append(batch, upsert(index, 0, fmt.Sprintf("new%d", i), docBody("new", i)))
		}
		head := mustApply(t, st, batch...)
		if err := st.Prune(ctx, store.ShardID{Index: "x", Shard: 0}, head+1); err != nil {
			t.Fatal(err)
		}
		c.start()
		waitMatches(t, c, st, id)
		if n := rewindCount(t, reader, "w"); n < 1 {
			t.Fatalf("%d rewinds counted", n)
		}
	})
}

// TestStoppedAcrossARewindAfterIdleAdvancesRebuildsTheCopy: the copy's own shard is
// idle while another shard writes over several polls, so the copy only advances past
// its last own change. It is then stopped across a lossy failover whose reissued seqs
// land on its own shard above that change, and the other shard prunes its rows before
// the copy starts again. The own-shard check followed the idle advances, so it covers
// the reissued own changes, and the copy rebuilds.
func TestStoppedAcrossARewindAfterIdleAdvancesRebuildsTheCopy(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "w", testMapping)
		createIndex(t, st, "x", testMapping)
		id := ShardID{Index: "w", Shard: 0}
		opts, reader := meteredOptions()
		c := newCopy(t, d.open(t), id, opts)
		c.start()
		c.waitApplied(mustApply(t, st, upsert("w", 0, "a", docBody("a", 1))))
		time.Sleep(100 * time.Millisecond)
		var k int64
		for i := range 10 {
			last := mustApply(t, st, upsert("x", 0, fmt.Sprintf("x%d", i), docBody("x", i)))
			if i == 4 {
				k = last
			}
			c.current().Wake()
			c.waitApplied(last)
			time.Sleep(30 * time.Millisecond)
		}
		if err := c.stop(); err != nil {
			t.Fatal(err)
		}
		if err := c.shard().Close(ctx); err != nil {
			t.Fatal(err)
		}
		rewind(ctx, t, d, k)
		mustApply(t, st, upsert("x", 0, "nx1", docBody("nx", 1)))
		mustApply(t, st, upsert("w", 0, "new0", docBody("new", 0)), upsert("w", 0, "new1", docBody("new", 1)))
		var batch []store.Change
		for i := range 4 {
			batch = append(batch, upsert("x", 0, fmt.Sprintf("nx%d", i+2), docBody("nx", i)))
		}
		head := mustApply(t, st, batch...)
		if err := st.Prune(ctx, store.ShardID{Index: "x", Shard: 0}, head+1); err != nil {
			t.Fatal(err)
		}
		c.start()
		waitMatches(t, c, st, id)
		if n := rewindCount(t, reader, "w"); n < 1 {
			t.Fatalf("%d rewinds counted", n)
		}
	})
}
