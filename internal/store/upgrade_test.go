package store

import (
	"context"
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
