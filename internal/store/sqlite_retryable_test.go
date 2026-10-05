package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// busyTimeout is the short busy_timeout these tests give the store's own
// connections, so SQLite's internal busy handler gives up well before the
// blocker below releases the lock: recovery can only come from the store's
// own retry on SQLITE_BUSY, not from SQLite waiting it out internally.
const busyTimeout = 200 * time.Millisecond

// busyHold is how long the blocking connection in these tests holds
// SQLite's write lock: comfortably more than one busyTimeout (so the first
// attempt must fail) and less than two (so the second succeeds, keeping the
// tests fast and their margins generous).
const busyHold = 300 * time.Millisecond

// holdSQLiteWriteLock opens a second, independent connection to path and
// has it take SQLite's write lock up front (BEGIN IMMEDIATE) and hold it for
// busyHold before releasing it. The returned func blocks until released.
func holdSQLiteWriteLock(t *testing.T, path string) (wait func()) {
	t.Helper()
	blocker, err := sql.Open("sqlite", path+"?_txlock=immediate&_busy_timeout=5000")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Close() })
	blocker.SetMaxOpenConns(1)
	btx, err := blocker.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	released := make(chan struct{})
	go func() {
		time.Sleep(busyHold)
		_ = btx.Rollback()
		close(released)
	}()
	return func() { <-released }
}

// TestSQLiteRetryable forces a real SQLITE_BUSY by holding SQLite's write
// lock on a second, independent connection to the same file, well past the
// store's own busy_timeout, then releasing it. A write that runs into that
// lock must recover once it is released, rather than failing outright: the
// dialect's Retryable must classify SQLITE_BUSY (and SQLITE_LOCKED) by the
// driver's own result code, not leave it nil as before.
func TestSQLiteRetryable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "busy.db")
	st, err := Open(context.Background(), sqliteURL(path)+"&_busy_timeout=200", quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustCreateIndex(t, st, "busy")
	reg := st.Registry()

	wait := holdSQLiteWriteLock(t, path)
	defer wait()

	// ClaimCopy's own BeginTx must hit SQLITE_BUSY at least once (busyTimeout
	// < busyHold) and still succeed once the lock is released, instead of
	// surfacing the first SQLITE_BUSY as a final error.
	shard := ShardID{Index: "busy", Shard: 0}
	start := time.Now()
	c, ok, err := reg.ClaimCopy(context.Background(), shard, "n1", 1, time.Minute)
	elapsed := time.Since(start)
	if err != nil || !ok {
		t.Fatalf("claim during contention: %+v ok=%v err=%v (after %s)", c, ok, err, elapsed)
	}
	if elapsed < busyTimeout {
		t.Fatalf("claim succeeded in %s, without ever waiting out a busy lock (busy_timeout %s): got lucky, or the test's own timing is off",
			elapsed, busyTimeout)
	}
}

// TestGroupCommitSurvivesSQLiteBusy checks that a GroupCommitter batch that
// runs into SQLITE_BUSY recovers by retrying the whole transaction, not by
// double-applying it: applyOnce's transaction is rolled back on any
// non-commit exit (including a failed Commit itself — SQLite does not
// partially apply a write before returning BUSY or LOCKED), so every retry
// starts from a transaction that was never durably applied; only one can
// ever succeed.
func TestGroupCommitSurvivesSQLiteBusy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "busy-gc.db")
	st, err := Open(context.Background(), sqliteURL(path)+"&_busy_timeout=200", quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	mustCreateIndex(t, st, "gcbusy")

	wait := holdSQLiteWriteLock(t, path)
	defer wait()

	g := NewGroupCommitter(st, GroupCommitOptions{MaxDelay: time.Hour, MaxChanges: 2})
	defer func() { _ = g.Close() }()

	batch := []Change{upsert("gcbusy", 0, "a", `{}`), upsert("gcbusy", 0, "b", `{}`)}
	start := time.Now()
	first, last, err := g.Apply(context.Background(), batch)
	elapsed := time.Since(start)
	if err != nil || last-first+1 != int64(len(batch)) {
		t.Fatalf("apply during contention: %d..%d %v (after %s)", first, last, err, elapsed)
	}
	if elapsed < busyTimeout {
		t.Fatalf("apply succeeded in %s without ever waiting out a busy lock", elapsed)
	}

	shard := ShardID{Index: "gcbusy", Shard: 0}
	changes := allChanges(t, st, shard)
	if len(changes) != len(batch) {
		t.Fatalf("changelog has %d rows, want %d: a retry double-applied the batch", len(changes), len(batch))
	}
	recs, _ := scanAll(t, st, shard)
	if len(recs) != len(batch) {
		t.Fatalf("%d documents, want %d: a retry double-applied the batch", len(recs), len(batch))
	}
}
