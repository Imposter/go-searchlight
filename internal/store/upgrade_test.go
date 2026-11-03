package store

import (
	"context"
	"fmt"
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
