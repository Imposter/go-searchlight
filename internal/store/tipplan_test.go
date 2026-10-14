package store

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

// planChanges is how many changes the plan tests seed: enough that a plan walking
// the changelog past a shard's newer rows reads thousands of times what an index
// probe does.
const planChanges = 200_000

// tipPlanCase is one kind of copy polling a large changelog.
type tipPlanCase struct {
	name string
	q    TipQuery
}

// tipPlanCases are a busy shard (its newest change near the head), a quiet one (its
// newest change near the first seq) and an empty one (an index with no change),
// each polled up to the head, over seedPlanChangelog's changelog.
func tipPlanCases() []tipPlanCase {
	return []tipPlanCase{
		{"busy", TipQuery{At: planChanges - 10, Shard: ShardID{Index: "i1", Shard: 2}, UpTo: planChanges}},
		{"quiet", TipQuery{At: planChanges - 1, Shard: ShardID{Index: "quiet", Shard: 0}, UpTo: planChanges}},
		{"empty", TipQuery{At: planChanges - 1, Shard: ShardID{Index: "empty", Shard: 0}, UpTo: planChanges}},
	}
}

// seedPlanChangelog fills the changelog with planChanges changes, numbered from 1 by
// the n of from (a dialect's SELECT of n over 1..planChanges): shard 0 of quiet takes
// every seventh of the first thousand, and the rest spread over four busy indexes of
// five shards. The quiet and empty indexes exist, the changelog's head is the last
// change, and the tables are analyzed.
func seedPlanChangelog(t *testing.T, st Store, from, analyze string) {
	t.Helper()
	ctx := context.Background()
	mustCreateIndex(t, st, "quiet")
	mustCreateIndex(t, st, "empty")
	quiet := "n <= 1000 AND MOD(n, 7) = 0"
	insert := fmt.Sprintf(`INSERT INTO sl_changes (seq, index_name, shard, kind, id, payload, at, mapping_version, index_uid)
SELECT n, CASE WHEN %[1]s THEN 'quiet' ELSE CONCAT('i', MOD(n, 4)) END, CASE WHEN %[1]s THEN 0 ELSE MOD(n, 5) END,
'upsert', CONCAT('d', n), '{}', 1700000000000 + n, 1, 'u' FROM (%[2]s) seeded`, quiet, from)
	for _, q := range []string{insert, fmt.Sprintf("UPDATE sl_counter SET value = %d WHERE id = 1", planChanges), analyze} {
		if _, err := engine(st).r.ExecContext(ctx, q); err != nil {
			t.Fatalf("seed the changelog: %v\n%s", err, q)
		}
	}
}

// TestTipOnPostgresReadsFewChangelogRows: the per-poll statement, run as the store
// runs it, reads a bounded number of changelog rows for a busy, a quiet and an empty
// shard, also after the five runs past which PostgreSQL may plan a prepared statement
// once for any parameters. Such a generic plan cannot tell how far below the bound a
// shard's newest change is, so it walks the primary key down from the bound past
// every newer row of every other shard.
//
// The rows are the backend's own counts of sl_changes' index and table reads, which
// it reports only once its transaction ends: the runs share one transaction, and
// the counts are compared within it.
func TestTipOnPostgresReadsFewChangelogRows(t *testing.T) {
	base := os.Getenv(envPG)
	if base == "" {
		t.Skip(envPG + " is not set")
	}
	ctx := context.Background()
	st := postgresHarness(t, base).open(t)
	s := engine(st)
	var tips tipReader = s
	seedPlanChangelog(t, st, fmt.Sprintf("SELECT n FROM generate_series(1, %d) n", planChanges), "ANALYZE sl_changes")
	const runs, perRun = 10, 20
	for _, c := range tipPlanCases() {
		t.Run(c.name, func(t *testing.T) {
			tx, err := s.r.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer rollback(tx)
			read := func() int64 {
				var n int64
				if err := tx.QueryRowContext(ctx, `SELECT pg_stat_get_xact_tuples_returned('sl_changes'::regclass)
+ (SELECT COALESCE(SUM(pg_stat_get_xact_tuples_returned(indexrelid)), 0) FROM pg_index WHERE indrelid = 'sl_changes'::regclass)`).Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n
			}
			before := read()
			for range runs {
				if _, err := tips.readTip(ctx, tx, c.q); err != nil {
					t.Fatal(err)
				}
			}
			if n := read() - before; n > runs*perRun {
				t.Fatalf("%d runs read %d changelog rows, more than %d a run", runs, n, perRun)
			}
		})
	}
}

// TestTipPlanOnMySQL: MySQL plans the per-poll statement, as the store sends it (its
// arguments interpolated, so planned for their values at every run), with a lookup of
// at most a few rows for each changelog row it joins, never a scan, for a busy, a quiet
// and an empty shard.
func TestTipPlanOnMySQL(t *testing.T) {
	base := os.Getenv(envMySQL)
	if base == "" {
		t.Skip(envMySQL + " is not set")
	}
	ctx := context.Background()
	st := mysqlHarness(t, base).open(t)
	s := engine(st)
	digits := "(SELECT 0 d UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9)"
	from := fmt.Sprintf("SELECT 1 + d0.d + 10*d1.d + 100*d2.d + 1000*d3.d + 10000*d4.d + 100000*d5.d AS n FROM %[1]s d0, %[1]s d1, %[1]s d2, %[1]s d3, %[1]s d4, %[1]s d5 WHERE 1 + d0.d + 10*d1.d + 100*d2.d + 1000*d3.d + 10000*d4.d + 100000*d5.d <= %[2]d", digits, planChanges)
	seedPlanChangelog(t, st, from, "ANALYZE TABLE sl_changes")
	for _, c := range tipPlanCases() {
		t.Run(c.name, func(t *testing.T) {
			var plan string
			if err := s.r.QueryRowContext(ctx, "EXPLAIN FORMAT=JSON "+s.d.Changelog.ReadTip, s.tipArgs(c.q)...).Scan(&plan); err != nil {
				t.Fatal(err)
			}
			var tree any
			if err := json.Unmarshal([]byte(plan), &tree); err != nil {
				t.Fatalf("%v\n%s", err, plan)
			}
			if bad := scansOf(tree); len(bad) > 0 {
				t.Fatalf("the plan scans: %s\n%s", strings.Join(bad, "; "), plan)
			}
		})
	}
}

// scansOf returns the tables an EXPLAIN FORMAT=JSON plan reads by a scan (a full
// table or index scan), or by a lookup it expects to examine more than a few rows in.
func scansOf(node any) []string {
	var out []string
	switch n := node.(type) {
	case map[string]any:
		if name, ok := n["table_name"].(string); ok {
			access, _ := n["access_type"].(string)
			rows, _ := n["rows_examined_per_scan"].(float64)
			if access == "ALL" || access == "index" || rows > 10 {
				out = append(out, fmt.Sprintf("%s (%s, %v rows)", name, access, rows))
			}
		}
		for _, v := range n {
			out = append(out, scansOf(v)...)
		}
	case []any:
		for _, v := range n {
			out = append(out, scansOf(v)...)
		}
	}
	return out
}

// tipReader runs the per-poll statement on a connection of the caller's choosing. The
// plan test calls the store through it, as the tailer calls Tip through the Store
// interface: gosec's taint analysis follows a direct call on the store from the
// test's database URL (an environment variable) into the gated pool's queries.
type tipReader interface {
	readTip(ctx context.Context, db queryer, q TipQuery) (Tip, error)
}
