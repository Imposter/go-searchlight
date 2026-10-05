package replica

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/stdlib"
	"go.opentelemetry.io/otel/attribute"

	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/store/mysql"
	"github.com/Imposter/go-searchlight/internal/store/postgres"
	"github.com/Imposter/go-searchlight/internal/store/storetest"
)

// The external databases the suite also runs on; unset, those dialects are skipped.
const (
	envPG    = "SEARCHLIGHT_TEST_PG_URL"
	envMySQL = "SEARCHLIGHT_TEST_MYSQL_URL"
)

var quietLogger = slog.New(slog.DiscardHandler)

// db is one isolated database of one dialect.
type db struct {
	dialect string
	url     string
	appName string // postgres: the application_name its connections carry
}

// forEachDialect runs fn on a fresh database of every available dialect.
func forEachDialect(t *testing.T, fn func(t *testing.T, d *db)) {
	t.Helper()
	forDialects(t, storetest.SQLiteURL, fn)
}

// forEachDurableDialect is forEachDialect with SQLite's synchronous=FULL, for tests
// of durability, crashes and fsyncs.
func forEachDurableDialect(t *testing.T, fn func(t *testing.T, d *db)) {
	t.Helper()
	forDialects(t, storetest.DurableSQLiteURL, fn)
}

func forDialects(t *testing.T, sqliteURL func(path string) string, fn func(t *testing.T, d *db)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		fn(t, &db{dialect: "sqlite", url: sqliteURL(storetest.Migrated(t, filepath.Join(t.TempDir(), "searchlight.db")))})
	})
	t.Run("postgres", func(t *testing.T) {
		base := os.Getenv(envPG)
		if base == "" {
			t.Skip(envPG + " is not set")
		}
		fn(t, postgresDB(t, base))
	})
	t.Run("mysql", func(t *testing.T) {
		base := os.Getenv(envMySQL)
		if base == "" {
			t.Skip(envMySQL + " is not set")
		}
		fn(t, mysqlDB(t, base))
	})
}

func randName(prefix string) string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return prefix + hex.EncodeToString(b[:])
}

// postgresDB creates a schema for the test and points search_path at it.
func postgresDB(t *testing.T, base string) *db {
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
	schemaName := randName("sl_r_")
	ctx := context.Background()
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+schemaName); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := admin.ExecContext(context.Background(), "DROP SCHEMA "+schemaName+" CASCADE"); err != nil {
			t.Errorf("drop schema: %v", err)
		}
	})
	q := u.Query()
	q.Set("search_path", schemaName)
	q.Set("application_name", schemaName)
	u.RawQuery = q.Encode()
	return &db{dialect: "postgres", url: u.String(), appName: schemaName}
}

// mysqlDB creates a database for the test when the account may, and otherwise
// empties the configured one of Searchlight's tables.
func mysqlDB(t *testing.T, base string) *db {
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
	name := randName("sl_r_")
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err == nil {
		t.Cleanup(func() {
			if _, err := admin.ExecContext(context.Background(), "DROP DATABASE "+name); err != nil {
				t.Errorf("drop database: %v", err)
			}
		})
		u.Path = "/" + name
		return &db{dialect: "mysql", url: u.String()}
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
	return &db{dialect: "mysql", url: base}
}

// open opens and migrates a store on the database: each call is another node's
// connection.
func (d *db) open(t testing.TB) store.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	st, err := store.Open(ctx, d.url, store.WithLogger(quietLogger))
	if err != nil {
		t.Fatalf("open %s: %v", d.dialect, err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate %s: %v", d.dialect, err)
	}
	return st
}

const testMapping = `{"dynamic":true,"fields":{"title":"text","brand":"keyword","tags":"keyword_list","price":"number"}}`

func createIndex(t testing.TB, st store.Store, name, mapping string) store.IndexMeta {
	t.Helper()
	m, err := st.Indexes().Create(context.Background(), store.IndexMeta{Name: name, Mapping: []byte(mapping)})
	if err != nil {
		t.Fatalf("create index %s: %v", name, err)
	}
	return m
}

func docBody(id string, n int) string {
	return fmt.Sprintf(`{"title":"item %s v%d","brand":"Brand %d","tags":["t%d","t%d"],"price":%d}`, id, n, n%7, n%5, n%3, n%97)
}

func upsert(index string, sh int, id, body string) store.Change {
	return store.Change{Index: index, Shard: sh, Kind: store.KindUpsert, ID: id, Payload: []byte(body)}
}

func del(index string, sh int, id string) store.Change {
	return store.Change{Index: index, Shard: sh, Kind: store.KindDelete, ID: id}
}

func queryUpsert(t testing.TB, index string, sh int, id, q, meta string) store.Change {
	t.Helper()
	var m []byte
	if meta != "" {
		m = []byte(meta)
	}
	p, err := store.EncodeQueryPayload([]byte(q), m)
	if err != nil {
		t.Fatal(err)
	}
	return store.Change{Index: index, Shard: sh, Kind: store.KindQueryUpsert, ID: id, Payload: p}
}

func queryDelete(index string, sh int, id string) store.Change {
	return store.Change{Index: index, Shard: sh, Kind: store.KindQueryDelete, ID: id}
}

func mustApply(t testing.TB, st store.Store, batch ...store.Change) int64 {
	t.Helper()
	_, last, err := st.Apply(context.Background(), batch)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	return last
}

// testShardOptions are quick: a short background refresh, no merges unless asked.
func testShardOptions(id ShardID) shard.Options {
	return shard.Options{
		Index: id.Index, Shard: id.Shard,
		RefreshInterval: 20 * time.Millisecond, FlushInterval: 50 * time.Millisecond,
		Logger: quietLogger, FilterCache: shard.NewFilterCache(1<<20, nil),
	}
}

func openShard(t testing.TB, dir string, opts shard.Options) *shard.Shard {
	t.Helper()
	sh, err := OpenCopy(context.Background(), dir, nil, opts) // dir is the copy's root
	if err != nil {
		t.Fatalf("open shard %s: %v", dir, err)
	}
	return sh
}

// testOptions are a quick tailer's.
func testOptions() Options {
	return Options{
		PollInterval: 10 * time.Millisecond, WatchedPollInterval: 200 * time.Millisecond,
		ReportInterval: 20 * time.Millisecond, CatalogInterval: time.Second,
		RetryBase: 5 * time.Millisecond, RetryCap: 50 * time.Millisecond, RebuildRetryCap: 100 * time.Millisecond, RemapDebounce: 20 * time.Millisecond,
		HaltRetryBase: 20 * time.Millisecond, HaltRetryCap: 100 * time.Millisecond,
		Logger: quietLogger,
	}
}

// errInjected is a store fault a test injects: transient, as the tailer sees it.
var errInjected = errors.New("injected store fault")

// faultStore wraps a store with injectable faults: fail[op] makes the next n calls
// of op fail ("head", "changes", "scan", "get"), and onRecord runs for every record a
// scan hands over (its error ends the scan).
type faultStore struct {
	store.Store
	mu       sync.Mutex
	fail     map[string]int
	always   map[string]bool
	onRecord func(ctx context.Context, n int, r store.Record) error
}

func newFaultStore(st store.Store) *faultStore {
	return &faultStore{Store: st, fail: map[string]int{}, always: map[string]bool{}}
}

func (f *faultStore) take(op string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.always[op] {
		return errInjected
	}
	if f.fail[op] > 0 {
		f.fail[op]--
		return errInjected
	}
	return nil
}

func (f *faultStore) inject(op string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[op] += n
}

func (f *faultStore) down(op string, down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.always[op] = down
}

func (f *faultStore) HeadSeq(ctx context.Context) (int64, time.Time, error) {
	if err := f.take("head"); err != nil {
		return 0, time.Time{}, err
	}
	return f.Store.HeadSeq(ctx)
}

func (f *faultStore) ChangesAfter(ctx context.Context, id store.ShardID, seq int64, limit int) ([]store.Change, error) {
	if err := f.take("changes"); err != nil {
		return nil, err
	}
	return f.Store.ChangesAfter(ctx, id, seq, limit)
}

func (f *faultStore) ScanShard(ctx context.Context, id store.ShardID, fn func(store.Record) error) (int64, error) {
	if err := f.take("scan"); err != nil {
		return 0, err
	}
	f.mu.Lock()
	hook := f.onRecord
	f.mu.Unlock()
	n := 0
	return f.Store.ScanShard(ctx, id, func(r store.Record) error {
		n++
		if hook != nil {
			if err := hook(ctx, n, r); err != nil {
				return err
			}
		}
		return fn(r)
	})
}

func (f *faultStore) Indexes() store.IndexStore {
	return faultIndexes{IndexStore: f.Store.Indexes(), f: f}
}

type faultIndexes struct {
	store.IndexStore
	f *faultStore
}

func (x faultIndexes) Get(ctx context.Context, name string) (store.IndexMeta, error) {
	if err := x.f.take("get"); err != nil {
		return store.IndexMeta{}, err
	}
	return x.IndexStore.Get(ctx, name)
}

// updateMapping replaces index's mapping through Indexes().Update, retrying a
// conflicting version.
func updateMapping(t testing.TB, st store.Store, index string, mapping func(m *schema.Mapping)) {
	t.Helper()
	ctx := context.Background()
	for {
		meta, err := st.Indexes().Get(ctx, index)
		if err != nil {
			t.Fatalf("get %s: %v", index, err)
		}
		m, err := parseMapping(meta.Mapping)
		if err != nil {
			t.Fatal(err)
		}
		mapping(m)
		raw, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		meta.Mapping = raw
		if _, err := st.Indexes().Update(ctx, meta); err == nil {
			return
		} else if !errors.Is(err, store.ErrConflict) {
			t.Fatalf("update %s: %v", index, err)
		}
	}
}

// copyRunner is one shard copy with its tailer running in the background.
type copyRunner struct {
	t    testing.TB
	st   store.Store
	id   ShardID
	dir  string
	opts Options
	sopt shard.Options
	// clk, when set, is the tailer's clock (opts.Clock): the waits advance it by step
	// whenever the tailer is parked on a timer.
	clk  *clock.Fake
	step time.Duration

	mu     sync.Mutex
	tailer *Tailer
	cancel context.CancelFunc
	done   chan error
}

func newCopy(t testing.TB, st store.Store, id ShardID, opts Options) *copyRunner {
	t.Helper()
	c := &copyRunner{t: t, st: st, id: id, dir: filepath.Join(t.TempDir(), "copy"), opts: opts, sopt: testShardOptions(id)}
	t.Cleanup(func() {
		_ = c.stop()
		if sh := c.shard(); sh != nil {
			_ = sh.Close(context.Background())
		}
	})
	return c
}

// start opens the copy's directory and runs a tailer over it.
func (c *copyRunner) start() {
	c.t.Helper()
	sh := openShard(c.t, c.dir, c.sopt)
	c.run(sh)
}

func (c *copyRunner) run(sh *shard.Shard) {
	tl := NewTailer(c.st, sh, c.id, c.opts)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- tl.Run(ctx) }()
	c.mu.Lock()
	c.tailer, c.cancel, c.done = tl, cancel, done
	c.mu.Unlock()
}

// stop stops the tailer and returns what Run returned (nil if it was not running).
func (c *copyRunner) stop() error {
	c.mu.Lock()
	cancel, done := c.cancel, c.done
	c.cancel, c.done = nil, nil
	c.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Minute):
		c.t.Fatal("tailer did not stop")
		return nil
	}
}

func (c *copyRunner) shard() *shard.Shard {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tailer == nil {
		return nil
	}
	return c.tailer.Shard()
}

// restart stops the tailer, closes the shard cleanly, and starts again.
func (c *copyRunner) restart() {
	c.t.Helper()
	if err := c.stop(); err != nil {
		c.t.Fatalf("tailer: %v", err)
	}
	if err := c.shard().Close(context.Background()); err != nil {
		c.t.Fatalf("close: %v", err)
	}
	c.start()
}

// crash kills the copy as a crash would: the tailer stops and the shard is abandoned
// with whatever it had not committed; then it starts again from the directory.
func (c *copyRunner) crash() {
	c.t.Helper()
	c.crashOnly()
	c.start()
}

// crashOnly kills the copy as crash does, without starting it again. The shard is
// abandoned under the running tailer, as a crash would; the tailer may have swapped
// in another by the time it stops, which is abandoned too: one shard per directory.
func (c *copyRunner) crashOnly() {
	c.t.Helper()
	c.shard().Abandon()
	_ = c.stop() // it may have seen the shard close under it
	c.shard().Abandon()
}

func tctx(t testing.TB) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func (c *copyRunner) current() *Tailer {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tailer
}

// withFakeClock runs the copy's tailer on a fake clock, which its waits advance by
// step whenever the tailer is parked on a timer.
func (c *copyRunner) withFakeClock(step time.Duration) *clock.Fake {
	c.clk, c.step = clock.NewFake(time.Now()), step
	c.opts.Clock = c.clk
	return c.clk
}

// until advances the fake clock by step each time the tailer parks on a timer, until
// cond holds when it parks: the tailer is idle then, so what it did is done.
func (c *copyRunner) until(what string, cond func() bool) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for {
		if err := c.clk.BlockUntil(ctx, 1); err != nil {
			c.t.Fatalf("%s: the tailer never parked: %v", what, err)
		}
		if cond() {
			return
		}
		c.clk.Advance(c.step)
	}
}

// waitApplied waits until the copy has applied, and made searchable, seq.
func (c *copyRunner) waitApplied(seq int64) *shard.Shard {
	c.t.Helper()
	if c.clk != nil {
		return c.waitAppliedByClock(seq)
	}
	deadline := time.Now().Add(time.Minute)
	for {
		c.mu.Lock()
		tl, done := c.tailer, c.done
		c.mu.Unlock()
		select {
		case err := <-done:
			c.mu.Lock()
			c.cancel, c.done = nil, nil
			c.mu.Unlock()
			c.t.Fatalf("tailer stopped: %v", err)
		default:
		}
		tl.Wake()
		sh := tl.Shard()
		var waitErr error
		if tl.Applied() >= seq && tl.State() == StateTailing {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			waitErr = sh.WaitRefreshed(ctx, seq)
			cancel()
			if waitErr == nil {
				return sh
			}
			switch {
			case errors.Is(waitErr, shard.ErrClosed):
			case errors.Is(waitErr, context.DeadlineExceeded):
				// A slow disk: refresh it ourselves, and say why if that fails.
				if err := sh.Refresh(context.Background()); err != nil && !errors.Is(err, shard.ErrClosed) {
					c.t.Fatalf("WaitRefreshed(%d) timed out; Refresh: %v", seq, err)
				}
			default:
				c.t.Fatalf("WaitRefreshed(%d): %v", seq, waitErr)
			}
		}
		if time.Now().After(deadline) {
			c.t.Fatalf("copy at %d (%s, refreshed %d), want %d: %v", tl.Applied(), tl.State(), sh.RefreshedSeq(), seq, waitErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (c *copyRunner) waitAppliedByClock(seq int64) *shard.Shard {
	c.t.Helper()
	c.until(fmt.Sprintf("apply %d", seq), func() bool {
		tl := c.current()
		return tl.Applied() >= seq && tl.State() == StateTailing
	})
	sh := c.shard()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := sh.WaitRefreshed(ctx, seq); err != nil {
		c.t.Fatalf("WaitRefreshed(%d): %v", seq, err)
	}
	return sh
}

func attrKey(k string) attribute.Key { return attribute.Key(k) }

// copyDir copies src's files into dst, which exists.
func copyDir(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o600); err != nil {
			return err
		}
	}
	return nil
}
