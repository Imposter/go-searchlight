package replica

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/store/mysql"
	"github.com/Imposter/go-searchlight/internal/store/postgres"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// TestChangelogRewindRebuildsTheCopy is a database failover that lost its last
// commits under a running copy: the changelog and counter go back to seq k, and new
// writes take the lost seqs again. Whether the new head stays behind the copy's seq,
// lands on it, or passes it, the copy sees the changelog no longer holds what it
// applied, rebuilds from the store, and serves the new writes, not the lost ones.
func TestChangelogRewindRebuildsTheCopy(t *testing.T) {
	for _, tc := range []struct {
		name  string
		extra int // new writes beyond the number lost
	}{
		{"head behind", -2},
		{"head reused", 0},
		{"head passed", 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forEachDialect(t, func(t *testing.T, d *db) {
				st := d.open(t)
				createIndex(t, st, "w", testMapping)
				id := ShardID{Index: "w", Shard: 0}
				opts, reader := meteredOptions()
				c := newCopy(t, d.open(t), id, opts)
				c.start()
				const lost = 5
				k := mustApply(t, st, upsert("w", 0, "kept", docBody("kept", 1)), upsert("w", 1, "other", docBody("other", 1)))
				var batch []store.Change
				for i := range lost {
					batch = append(batch, upsert("w", 0, fmt.Sprintf("lost%d", i), docBody("lost", i)))
				}
				c.waitApplied(mustApply(t, st, batch...))

				rewind(context.Background(), t, d, k)
				batch = batch[:0]
				for i := range lost + tc.extra {
					batch = append(batch, upsert("w", 0, fmt.Sprintf("new%d", i), docBody("new", i)))
				}
				mustApply(t, st, batch...)

				waitMatches(t, c, st, id)
				if n := counterSum(t, reader, telemetry.MetricChangelogRewinds, telemetry.KeyIndex, "w"); n < 1 {
					t.Fatalf("%s counted %d rewinds", telemetry.MetricChangelogRewinds, n)
				}

				head := mustApply(t, st, upsert("w", 0, "after", docBody("after", 1)))
				sh := c.waitApplied(head)
				if dd := diff(viewOf(t, sh), truthOf(t, st, id), false); dd != "" {
					t.Fatalf("after the rebuild the copy differs from the store:\n%s", dd)
				}
			})
		})
	}
}

// waitMatches waits until the copy holds what the store does.
func waitMatches(t *testing.T, c *copyRunner, st store.Store, id ShardID) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		tl := c.current()
		tl.Wake()
		sh := tl.Shard()
		if tl.State() == StateTailing && sh.Refresh(context.Background()) == nil {
			dd := diff(viewOf(t, sh), truthOf(t, st, id), false)
			if dd == "" {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("the copy (at %d, %s) never matched the rewound store:\n%s", tl.Applied(), tl.State(), dd)
			}
		} else if time.Now().After(deadline) {
			t.Fatalf("the copy (at %d) never went back to tailing: %s", tl.Applied(), tl.State())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// rewind loses every commit after seq k, as a failover to a replica that had not
// received them would: their changes, the documents and queries they created, and the
// counter past k, all at once (one transaction), as a promoted replica shows them.
func rewind(ctx context.Context, t *testing.T, d *db, k int64) {
	t.Helper()
	raw := rawDB(t, d)
	tx, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, q := range []string{
		"DELETE FROM sl_changes WHERE seq > %d",
		"DELETE FROM sl_documents WHERE seq > %d",
		"DELETE FROM sl_queries WHERE seq > %d",
		"UPDATE sl_counter SET value = %d WHERE id = 1",
	} {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(q, k)); err != nil {
			t.Fatalf("rewind %q: %v", q, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("rewind: %v", err)
	}
}

// rawDB opens d outside every store, closed when the test ends.
func rawDB(t *testing.T, d *db) *sql.DB {
	t.Helper()
	u, err := url.Parse(d.url)
	if err != nil {
		t.Fatal(err)
	}
	var raw *sql.DB
	switch d.dialect {
	case "sqlite":
		path := strings.TrimPrefix(u.Path, "/")
		if _, err := os.Stat(path); err != nil {
			path = u.Path
		}
		raw, err = sql.Open("sqlite", path+"?_pragma=busy_timeout(10000)")
		if err != nil {
			t.Fatal(err)
		}
	case "postgres":
		cfg, err := postgres.Config(u)
		if err != nil {
			t.Fatal(err)
		}
		raw = stdlib.OpenDB(*cfg)
	case "mysql":
		cfg, err := mysql.Config(u)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := gomysql.NewConnector(cfg)
		if err != nil {
			t.Fatal(err)
		}
		raw = sql.OpenDB(conn)
	default:
		t.Fatalf("no raw access to %s", d.dialect)
	}
	t.Cleanup(func() { _ = raw.Close() })
	return raw
}

// TestRebuiltCopyReportsItsOwnSeq: the registry's applied_seq is the copy's
// CommittedSeq. A copy rebuilt from scratch holds only what its rebuild brought, so
// once rebuilt it reports that seq even when it is lower than one reported before
// (here, by the copy the changelog was rewound under).
func TestRebuiltCopyReportsItsOwnSeq(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		ctx := context.Background()
		st := d.open(t)
		createIndex(t, st, "r", testMapping)
		id := ShardID{Index: "r", Shard: 0}
		cp, ok, err := st.Registry().ClaimCopy(ctx, id, "node-a", 1, time.Minute)
		if err != nil || !ok {
			t.Fatalf("claim: %v %v", ok, err)
		}
		opts := testOptions()
		opts.Copy = &cp
		c := newCopy(t, d.open(t), id, opts)
		c.start()
		k := mustApply(t, st, upsert("r", 0, "kept", docBody("kept", 1)))
		lost := mustApply(t, st, upsert("r", 0, "lost0", docBody("lost", 0)), upsert("r", 0, "lost1", docBody("lost", 1)))
		c.waitApplied(lost)
		waitReported(t, st, id, func(seq int64) bool { return seq >= lost })

		rewind(context.Background(), t, d, k)
		waitMatches(t, c, st, id)
		got := waitReported(t, st, id, func(seq int64) bool { return seq <= k })
		if sh := c.shard(); got != sh.CommittedSeq() {
			t.Fatalf("reported %d, the rebuilt copy committed %d", got, sh.CommittedSeq())
		}
	})
}

// waitReported waits until the copy's applied_seq in the registry satisfies ok, and
// returns it.
func waitReported(t *testing.T, st store.Store, id ShardID, ok func(int64) bool) int64 {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		copies, err := st.Registry().Copies(context.Background(), id.Index)
		if err != nil {
			t.Fatal(err)
		}
		if len(copies) == 1 && ok(copies[0].AppliedSeq) {
			return copies[0].AppliedSeq
		}
		if time.Now().After(deadline) {
			t.Fatalf("the registry holds %+v", copies)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
