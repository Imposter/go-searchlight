package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestPruneBeforeSeeingTheSwitch: a store that has not seen shard sequences come on
// prunes: it finds the global sequence gone and runs again on the shard's row, having
// ended its first transaction (SQLite's one write connection would otherwise wait for
// itself).
func TestPruneBeforeSeeingTheSwitch(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		st, old := openMixed(t, h)
		other := openAgain(t, h, st)
		old.mustApply("mx", 0, "x1", "x2", "x3")
		if err := st.EnableShardSeqs(context.Background()); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := other.Prune(ctx, ShardID{Index: "mx"}, 3); err != nil {
			t.Fatalf("prune by a store that had not seen the switch: %v", err)
		}
	})
}

// TestIndexCreatedByOldNodeTakesWrites: an index a 1.x node creates after the switch
// has no sequence rows; this binary's first write or mapping change to it gives it
// rows at the floor and goes on, without a restart, and SeedMissingShards gives an
// index its rows ahead of any write.
func TestIndexCreatedByOldNodeTakesWrites(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, old := openMixed(t, h)
		if err := st.EnableShardSeqs(ctx); err != nil {
			t.Fatal(err)
		}
		floor := floorSeq(t, st)
		old.create("zombie", 2)
		b := []Change{upsert("zombie", 1, "z", `{}`)}
		if err := st.Apply(ctx, b); err != nil {
			t.Fatalf("apply to an index 1.x created after the switch: %v", err)
		}
		if b[0].Seq != floor+1 {
			t.Errorf("the first write took seq %d, want %d", b[0].Seq, floor+1)
		}
		old.create("remapped", 2)
		if _, err := st.Indexes().Update(ctx, IndexMeta{Name: "remapped", Version: 1, Mapping: []byte(`{"fields": {"x": "keyword"}}`), Settings: []byte(`{"shards": 2}`)}); err != nil {
			t.Fatalf("remap an index 1.x created after the switch: %v", err)
		}
		old.create("ahead", 3)
		if err := st.SeedMissingShards(ctx); err != nil {
			t.Fatal(err)
		}
		if n := countRows(t, st, "SELECT COUNT(*) FROM sl_shard_seq WHERE index_name = ? AND value = ?", "ahead", floor); n != 3 {
			t.Errorf("SeedMissingShards gave ahead %d rows at the floor, want 3", n)
		}
	})
}

// TestRestoreToBeforeTheSwitch: a database restored to before shard sequences came on
// has no floor; a store that had seen them on finds that at its next write, forgets
// it, and writes on the global sequence.
func TestRestoreToBeforeTheSwitch(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, old := openMixed(t, h)
		global := floorSeq(t, st)
		if err := st.EnableShardSeqs(ctx); err != nil {
			t.Fatal(err)
		}
		s := engine(st)
		for _, q := range []string{
			"DELETE FROM sl_shard_seq",
			"DELETE FROM sl_counter WHERE id = 3",
			"DELETE FROM sl_features",
			fmt.Sprintf("INSERT INTO sl_counter (id, value) VALUES (1, %d)", global),
		} {
			if _, err := s.w.ExecContext(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		b := []Change{upsert("mx", 0, "after-restore", `{}`), upsert("mx", 1, "after-restore", `{}`)}
		if err := st.Apply(ctx, b); err != nil {
			t.Fatalf("apply after the restore: %v", err)
		}
		if b[0].Seq != global+1 || b[1].Seq != global+2 {
			t.Errorf("the writes after the restore took %v, want the global sequence's %d and %d", seqsOf(b), global+1, global+2)
		}
		if on, err := st.ShardSeqs(ctx); err != nil || on {
			t.Errorf("shard sequences after the restore: %v %v", on, err)
		}
		if next := old.mustApply("mx", 0, "old")[0]; next != global+3 {
			t.Errorf("1.x took seq %d after the restore, want %d", next, global+3)
		}
	})
}

// TestSwitchFailsMidway: a switch that fails part way leaves nothing behind (no
// sequence row, the global sequence still there, no floor), the writes of both
// releases go on, and the switch succeeds when run again.
func TestSwitchFailsMidway(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, old := openMixed(t, h)
		old.create("late", 3)
		s := engine(st)
		saved := s.d.Changelog.RetireGlobal
		s.d.Changelog.RetireGlobal = "DELETE FROM sl_no_such_table"
		err := st.EnableShardSeqs(ctx)
		s.d.Changelog.RetireGlobal = saved
		if err == nil {
			t.Fatal("the broken switch succeeded")
		}
		if n := len(shardSeqRows(t, st)); n != 0 {
			t.Errorf("%d sequence rows after a failed switch", n)
		}
		if n := countRows(t, st, "SELECT COUNT(*) FROM sl_counter WHERE id = 1"); n != 1 {
			t.Error("the global sequence is gone after a failed switch")
		}
		if n := countRows(t, st, "SELECT COUNT(*) FROM sl_counter WHERE id = 3"); n != 0 {
			t.Error("a floor after a failed switch")
		}
		if on, err := st.ShardSeqs(ctx); err != nil || on {
			t.Errorf("ShardSeqs after a failed switch: %v %v", on, err)
		}
		mustApply(t, st, upsert("mx", 0, "after-fail", `{}`))
		old.mustApply("mx", 1, "old-after-fail")
		if err := st.EnableShardSeqs(ctx); err != nil {
			t.Fatal(err)
		}
		if n := len(shardSeqRows(t, st)); n != 5 {
			t.Errorf("%d sequence rows after the switch ran again, want 5", n)
		}
	})
}

// TestTwoLeadersSwitch: two nodes switching at once (a leadership handover) both
// succeed, and the rows are seeded once.
func TestTwoLeadersSwitch(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, _ := openMixed(t, h)
		other := openAgain(t, h, st)
		var e1, e2 error
		var wg sync.WaitGroup
		wg.Go(func() { e1 = st.EnableShardSeqs(ctx) })
		wg.Go(func() { e2 = other.EnableShardSeqs(ctx) })
		wg.Wait()
		if e1 != nil || e2 != nil {
			t.Errorf("switches: %v / %v", e1, e2)
		}
		if n := len(shardSeqRows(t, st)); n != 2 {
			t.Errorf("%d sequence rows, want 2", n)
		}
	})
}

// TestSwitchStall: the switch holds the global sequence while it seeds a row per shard
// of every index, so every write waits for it; with 200 indexes of 64 shards (12,800
// rows) it logs how long the switch took and the longest a write waited, which
// docs/operations.md quotes.
func TestSwitchStall(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, _ := openMixed(t, h)
		const indexes, shards = 200, 64
		s := engine(st)
		tx, err := s.w.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		q := rebind(s, "INSERT INTO sl_indexes (name, mapping, settings, version, created_at, mapping_version, uid) VALUES (?, '{}', ?, 1, 0, 1, ?)")
		for i := range indexes {
			if _, err := tx.ExecContext(ctx, q, fmt.Sprintf("big%03d", i), fmt.Sprintf(`{"shards": %d}`, shards), randName("uid")); err != nil {
				t.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		other := openAgain(t, h, st)
		var maxWait atomic.Int64
		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				t0 := time.Now()
				if err := other.Apply(ctx, []Change{upsert("mx", i%2, fmt.Sprint("w", i), `{}`)}); err != nil {
					t.Errorf("writer: %v", err)
					return
				}
				if d := int64(time.Since(t0)); d > maxWait.Load() {
					maxWait.Store(d)
				}
			}
		})
		time.Sleep(200 * time.Millisecond)
		t0 := time.Now()
		if err := st.EnableShardSeqs(ctx); err != nil {
			t.Fatal(err)
		}
		took := time.Since(t0)
		time.Sleep(200 * time.Millisecond)
		close(stop)
		wg.Wait()
		if n := countRows(t, st, "SELECT COUNT(*) FROM sl_shard_seq"); n != indexes*shards+2 {
			t.Errorf("%d sequence rows, want %d", n, indexes*shards+2)
		}
		t.Logf("%s: the switch seeded %d rows in %s; a write waited at most %s", h.dialect, indexes*shards+2, took.Round(time.Millisecond), time.Duration(maxWait.Load()).Round(time.Millisecond))
	})
}

// TestThreeMigratorsBesideOldWrites: three nodes migrate a v6 database at once while a
// 1.x node writes: every migration succeeds, every 1.x write too, and no two changes
// share a seq.
func TestThreeMigratorsBesideOldWrites(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		if h.dialect == "sqlite" {
			t.Skip("SQLite serves one node")
		}
		ctx := context.Background()
		st, _ := openAt(t, h, "0001_init.sql", "0002_body_codecs.sql", "0003_change_nonce.sql",
			"0004_mapping_types.sql", "0005_versions.sql", "0006_body_hash.sql")
		old := legacy(t, st)
		old.create("m", 4)
		for i := range 2000 {
			old.mustApply("m", i%4, fmt.Sprint("p", i))
		}
		stop := make(chan struct{})
		var wg sync.WaitGroup
		var oldErrs atomic.Int64
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := old.apply("m", i%4, fmt.Sprint("q", i)); err != nil {
					oldErrs.Add(1)
				}
			}
		})
		var errs [3]error
		var mg sync.WaitGroup
		for i := range 3 {
			o, err := Open(ctx, h.url, quiet)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = o.Close() })
			mg.Go(func() { errs[i] = o.Migrate(ctx) })
		}
		mg.Wait()
		close(stop)
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Errorf("migrator %d: %v", i, err)
			}
		}
		if n := oldErrs.Load(); n > 0 {
			t.Errorf("%d 1.x writes failed during the migrations", n)
		}
		if n := len(sharedSeqs(t, st, 0)); n != 0 {
			t.Errorf("%d shared seqs", n)
		}
	})
}
