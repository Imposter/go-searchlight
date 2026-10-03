package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Imposter/go-searchlight/internal/store/mysql"
	"github.com/Imposter/go-searchlight/internal/store/postgres"
)

// Environment variables naming the external databases the conformance suite
// also runs on. Unset, those dialects are skipped.
const (
	envPG    = "SEARCHLIGHT_TEST_PG_URL"
	envMySQL = "SEARCHLIGHT_TEST_MYSQL_URL"
)

// quiet drops the store's logs in tests.
var quiet = WithLogger(slog.New(slog.DiscardHandler))

// harness is one isolated database of one dialect.
type harness struct {
	dialect string
	url     string
}

// forEachDialect runs fn on a fresh database of every available dialect.
func forEachDialect(t *testing.T, fn func(t *testing.T, h *harness)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) { fn(t, sqliteHarness(t)) })
	t.Run("postgres", func(t *testing.T) {
		base := os.Getenv(envPG)
		if base == "" {
			t.Skip(envPG + " is not set")
		}
		fn(t, postgresHarness(t, base))
	})
	t.Run("mysql", func(t *testing.T) {
		base := os.Getenv(envMySQL)
		if base == "" {
			t.Skip(envMySQL + " is not set")
		}
		fn(t, mysqlHarness(t, base))
	})
}

func randName(prefix string) string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}

func sqliteURL(path string) string {
	path = filepath.ToSlash(path)
	if strings.HasPrefix(path, "/") {
		return "sqlite://" + path
	}
	return "sqlite:///" + path
}

func sqliteHarness(t *testing.T) *harness {
	return &harness{dialect: "sqlite", url: sqliteURL(filepath.Join(t.TempDir(), "searchlight.db"))}
}

// postgresHarness creates a schema for the test and points search_path at it.
func postgresHarness(t *testing.T, base string) *harness {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse %s: %v", envPG, err)
	}
	cfg, err := postgres.Config(u)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = admin.Close() })
	schema := randName("sl_t_")
	ctx := context.Background()
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("drop schema: %v", err)
		}
	})
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return &harness{dialect: "postgres", url: u.String()}
}

// mysqlHarness creates a database for the test when the account may, and
// otherwise empties the configured database of Searchlight's tables (the CI
// account is limited to its own database).
func mysqlHarness(t *testing.T, base string) *harness {
	t.Helper()
	u, err := url.Parse(base)
	if err != nil {
		t.Fatalf("parse %s: %v", envMySQL, err)
	}
	cfg, err := mysql.Config(u)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := gomysql.NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	admin := sql.OpenDB(conn)
	t.Cleanup(func() { _ = admin.Close() })
	ctx := context.Background()
	name := randName("sl_t_")
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err == nil {
		t.Cleanup(func() {
			if _, err := admin.ExecContext(context.Background(), "DROP DATABASE "+name); err != nil {
				t.Errorf("drop database: %v", err)
			}
		})
		u.Path = "/" + name
		return &harness{dialect: "mysql", url: u.String()}
	}
	// The account may not create databases (CI's may not): every test of every
	// package then shares this one, so they take turns under a named lock, held
	// by this connection until the test ends. A test that opened two such
	// databases would wait on itself (GET_LOCK is per connection) until the
	// timeout: open one per test.
	lockConn, err := admin.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var locked sql.NullInt64
	if err := lockConn.QueryRowContext(ctx, "SELECT GET_LOCK('searchlight_shared_test_db', 1800)").Scan(&locked); err != nil || locked.Int64 != 1 {
		t.Fatalf("lock the shared test database: %v (%v)", err, locked)
	}
	t.Cleanup(func() { _ = lockConn.Close() })
	rows, err := admin.QueryContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name LIKE 'sl\\_%'")
	if err != nil {
		t.Fatal(err)
	}
	var tables []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		tables = append(tables, n)
	}
	_ = rows.Close()
	for _, n := range tables {
		if _, err := admin.ExecContext(ctx, "DROP TABLE "+n); err != nil {
			t.Fatalf("drop %s: %v", n, err)
		}
	}
	return &harness{dialect: "mysql", url: base}
}

// open opens and migrates a store on the harness database. Each call is
// another node's connection to the same database.
func (h *harness) open(t *testing.T, opts ...Option) Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	st, err := Open(ctx, h.url, append([]Option{quiet}, opts...)...)
	if err != nil {
		t.Fatalf("open %s: %v", h.dialect, err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate %s: %v", h.dialect, err)
	}
	return st
}

// engine returns the engine behind a store, for test hooks.
func engine(st Store) *sqlStore {
	switch s := st.(type) {
	case *sqlStore:
		return s
	case *watchingStore:
		return s.sqlStore
	}
	panic(fmt.Sprintf("unexpected store %T", st))
}

func mustCreateIndex(t *testing.T, st Store, name string) {
	t.Helper()
	if _, err := st.Indexes().Create(context.Background(), IndexMeta{Name: name}); err != nil && !errors.Is(err, ErrExists) {
		t.Fatalf("create index %s: %v", name, err)
	}
}

func upsert(index string, shard int, id, body string) Change {
	return Change{Index: index, Shard: shard, Kind: KindUpsert, ID: id, Payload: []byte(body)}
}

func del(index string, shard int, id string) Change {
	return Change{Index: index, Shard: shard, Kind: KindDelete, ID: id}
}

func mustApply(t *testing.T, st Applier, batch ...Change) (int64, int64) {
	t.Helper()
	first, last, err := st.Apply(context.Background(), batch)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if last-first+1 != int64(len(batch)) {
		t.Fatalf("apply returned %d..%d for %d changes", first, last, len(batch))
	}
	return first, last
}

// allChanges pages through a shard's changelog.
func allChanges(t *testing.T, st Store, shard ShardID) []Change {
	t.Helper()
	var out []Change
	after := int64(0)
	for {
		page, err := st.ChangesAfter(context.Background(), shard, after, 97)
		if err != nil {
			t.Fatalf("changes after %d: %v", after, err)
		}
		if len(page) == 0 {
			return out
		}
		out = append(out, page...)
		after = page[len(page)-1].Seq
	}
}

func scanAll(t *testing.T, st Store, shard ShardID) (map[string]Record, int64) {
	t.Helper()
	out := make(map[string]Record)
	first := true
	asOf, err := st.ScanShard(context.Background(), shard, func(r Record) error {
		isFirst := first
		first = false
		if r.Kind == RecordMapping {
			if !isFirst {
				t.Errorf("the mapping record is not the first")
			}
			return nil // scanMapping reads it
		}
		k := "d:" + r.ID
		if r.Kind == RecordQuery {
			k = "q:" + r.ID
		}
		out[k] = r
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	return out, asOf
}

func counterValue(t *testing.T, st Store) int64 {
	t.Helper()
	var v int64
	s := engine(st)
	if err := s.r.QueryRowContext(context.Background(), s.q.readCounter).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func countRows(t *testing.T, st Store, query string, args ...any) int {
	t.Helper()
	s := engine(st)
	var n int
	if err := s.r.QueryRowContext(context.Background(), s.bind(query), args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}
