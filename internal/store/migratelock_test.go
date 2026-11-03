package store

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

// TestMigrationLockHold times migration 0007 on a changelog of
// SEARCHLIGHT_MIGRATE_ROWS rows (payloads of SEARCHLIGHT_MIGRATE_PAYLOAD bytes, 900 by
// default), while another connection reads sl_changes by seq and inserts into it the
// way a 1.x node's polls and commits do, and reports the longest either waited: on
// Postgres, the key swap holds sl_changes' ACCESS EXCLUSIVE lock for its whole build.
// It skips without the variable; SQLite only with SEARCHLIGHT_MIGRATE_SQLITE set.
func TestMigrationLockHold(t *testing.T) {
	rows, _ := strconv.Atoi(os.Getenv("SEARCHLIGHT_MIGRATE_ROWS"))
	if rows == 0 {
		t.Skip("SEARCHLIGHT_MIGRATE_ROWS is not set")
	}
	payload := 900
	if p, err := strconv.Atoi(os.Getenv("SEARCHLIGHT_MIGRATE_PAYLOAD")); err == nil && p > 0 {
		payload = p
	}
	forEachDialect(t, func(t *testing.T, h *harness) {
		if h.dialect == "sqlite" && os.Getenv("SEARCHLIGHT_MIGRATE_SQLITE") == "" {
			t.Skip("SEARCHLIGHT_MIGRATE_SQLITE is not set")
		}
		ctx := context.Background()
		st, all := openAt(t, h, "0001_init.sql", "0002_body_codecs.sql", "0003_change_nonce.sql",
			"0004_mapping_types.sql", "0005_versions.sql", "0006_body_hash.sql")
		s := engine(st)
		uid := createOldIndex(t, st, "big", `{"shards": 8}`)
		t0 := time.Now()
		var fill string
		switch h.dialect {
		case "postgres":
			fill = fmt.Sprintf("INSERT INTO sl_changes (seq, index_name, shard, kind, id, payload, at, index_uid, mapping_version) SELECT g, 'big', g %% 8, 'upsert', 'doc' || g, repeat('x', %d), 0, '%s', 2 FROM generate_series(1, %d) g", payload, uid, rows)
		case "mysql":
			fill = fmt.Sprintf("INSERT INTO sl_changes (seq, index_name, shard, kind, id, payload, at, index_uid, mapping_version) WITH RECURSIVE n (g) AS (SELECT 1 UNION ALL SELECT g + 1 FROM n WHERE g < %d) SELECT g, 'big', g %% 8, 'upsert', CONCAT('doc', g), REPEAT('x', %d), 0, '%s', 2 FROM n", rows, payload, uid)
		default:
			fill = fmt.Sprintf("INSERT INTO sl_changes (seq, index_name, shard, kind, id, payload, at, index_uid, mapping_version) WITH RECURSIVE n (g) AS (SELECT 1 UNION ALL SELECT g + 1 FROM n WHERE g < %d) SELECT g, 'big', g %% 8, 'upsert', 'doc' || g, printf('%%.%dc', 'x'), 0, '%s', 2 FROM n", rows, payload, uid)
		}
		conn, err := s.w.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if h.dialect == "mysql" {
			if _, err := conn.ExecContext(ctx, "SET SESSION cte_max_recursion_depth = 100000000"); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := conn.ExecContext(ctx, fill); err != nil {
			t.Fatalf("fill: %v", err)
		}
		if h.dialect == "postgres" {
			if _, err := conn.ExecContext(ctx, "ANALYZE sl_changes"); err != nil {
				t.Fatal(err)
			}
		}
		_ = conn.Close()
		if _, err := s.w.ExecContext(ctx, rebind(s, "UPDATE sl_counter SET value = ? WHERE id = 1"), rows); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: filled %d changes of %d bytes in %v", h.dialect, rows, payload, time.Since(t0).Round(time.Millisecond))

		other := openAgainAt(t, h, st)
		o := engine(other)
		stop := make(chan struct{})
		done := make(chan [2]time.Duration)
		go func() {
			var maxRead, maxWrite time.Duration
			next := int64(rows) + 1
			for {
				select {
				case <-stop:
					done <- [2]time.Duration{maxRead, maxWrite}
					return
				default:
				}
				r0 := time.Now()
				var n int
				_ = o.r.QueryRowContext(ctx, rebind(o, "SELECT COUNT(*) FROM sl_changes WHERE seq = ?"), next-1).Scan(&n)
				maxRead = max(maxRead, time.Since(r0))
				w0 := time.Now()
				q := "INSERT INTO sl_changes (seq, index_name, shard, kind, id, payload, at, index_uid, mapping_version) VALUES (?, 'big', 0, 'upsert', ?, '{}', 0, ?, 2)"
				if _, err := o.w.ExecContext(ctx, rebind(o, q), next+1000000000, fmt.Sprint("live", next), uid); err == nil {
					next++
				}
				maxWrite = max(maxWrite, time.Since(w0))
				time.Sleep(5 * time.Millisecond)
			}
		}()
		time.Sleep(200 * time.Millisecond)
		s.d.Migrations = all
		m0 := time.Now()
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		took := time.Since(m0)
		time.Sleep(200 * time.Millisecond)
		close(stop)
		w := <-done
		t.Logf("%s: migration 0007 on %d changes took %v; a concurrent read by seq waited at most %v, an insert at most %v",
			h.dialect, rows, took.Round(time.Millisecond), w[0].Round(time.Millisecond), w[1].Round(time.Millisecond))
	})
}

// openAgainAt opens another connection to st's database without migrating it.
func openAgainAt(t *testing.T, h *harness, st Store) Store {
	t.Helper()
	url := h.url
	if h.dialect == "sqlite" {
		url = sqliteURL(engine(st).dbPath)
	}
	other, err := Open(context.Background(), url, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	return other
}

// TestMigrateConcurrentStepRuns: on Postgres, migration 7's indexes are built outside
// its transaction first; a migration interrupted after them (here, run by hand
// beforehand) runs them again and completes, keyed by shard and seq.
func TestMigrateConcurrentStepRuns(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		if h.dialect != "postgres" {
			t.Skip("only Postgres runs migration statements outside a transaction")
		}
		ctx := context.Background()
		st, all := openAt(t, h, "0001_init.sql", "0002_body_codecs.sql", "0003_change_nonce.sql",
			"0004_mapping_types.sql", "0005_versions.sql", "0006_body_hash.sql")
		s := engine(st)
		old := legacy(t, st)
		old.create("cc", 2)
		old.mustApply("cc", 1, "a", "b")
		ms, err := loadMigrations(all)
		if err != nil {
			t.Fatal(err)
		}
		var step []string
		for _, m := range ms {
			if m.version == shardSeqSchema {
				step = m.concurrent
			}
		}
		if len(step) == 0 {
			t.Fatal("migration 7 has no concurrent step")
		}
		for _, q := range step {
			if _, err := s.w.ExecContext(ctx, q); err != nil {
				t.Fatalf("%s: %v", q, err)
			}
		}
		s.d.Migrations = all
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("migrate after an interrupted run: %v", err)
		}
		var def string
		if err := s.r.QueryRowContext(ctx, "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conname = 'sl_changes_pkey' AND conrelid = 'sl_changes'::regclass").Scan(&def); err != nil {
			t.Fatal(err)
		}
		if def != "PRIMARY KEY (index_name, shard, seq)" {
			t.Errorf("sl_changes' key after the migration: %s", def)
		}
		if n := countRows(t, st, "SELECT COUNT(*) FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'sl_changes' AND indexname IN ('sl_changes_seq', 'sl_changes_key_v7', 'sl_changes_shard_seq')"); n != 1 {
			t.Errorf("%d of sl_changes_seq, sl_changes_key_v7 and sl_changes_shard_seq remain, want sl_changes_seq alone", n)
		}
		mustApply(t, st, upsert("cc", 1, "c", `{}`))
	})
}

// TestConcurrentStepNeedsItsMigration: a NNNN_name.concurrent.sql without its
// migration is refused.
func TestConcurrentStepNeedsItsMigration(t *testing.T) {
	_, err := loadMigrations(fstest.MapFS{
		"0001_init.sql":            {Data: []byte("SELECT 1;")},
		"0002_next.concurrent.sql": {Data: []byte("SELECT 1;")},
	})
	if err == nil {
		t.Fatal("an orphan concurrent step was accepted")
	}
}

// TestFreshDatabaseMigratesInOneTransaction: on Postgres, the run that creates a
// database runs every migration in one transaction, building migration 7's indexes
// inside it: a node of the release before (a v6 binary) starting meanwhile waits, then
// finds the schema newer than it knows, rather than starting on a database that then
// switches to shard sequences at once.
func TestFreshDatabaseMigratesInOneTransaction(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		if h.dialect != "postgres" {
			t.Skip("only Postgres runs migration statements outside a transaction")
		}
		ctx := context.Background()
		slow := func(d *dialect.Dialect) {
			all := d.Migrations
			files := fstest.MapFS{}
			names, _ := fs.Glob(all, "*.sql")
			for _, n := range names {
				b, _ := fs.ReadFile(all, n)
				if n == "0007_shard_seq.concurrent.sql" {
					b = append([]byte("SELECT pg_sleep(3);\n\n"), b...)
				}
				files[n] = &fstest.MapFile{Data: b}
			}
			d.Migrations = files
		}
		a, err := Open(ctx, h.url, quiet, withDialect(slow))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = a.Close() })
		var aErr error
		var wg sync.WaitGroup
		wg.Go(func() { aErr = a.Migrate(ctx) })
		time.Sleep(1500 * time.Millisecond)
		old, err := Open(ctx, h.url, quiet, WithSchema(6))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = old.Close() })
		oldErr := old.Migrate(ctx)
		wg.Wait()
		if aErr != nil {
			t.Fatal(aErr)
		}
		on, _ := a.ShardSeqs(ctx)
		if !errors.Is(oldErr, ErrNewerSchema) || !on {
			t.Errorf("a v6 binary starting while a database was created: %v; shard sequences on after: %v; want the schema refused and them on", oldErr, on)
		}
	})
}
