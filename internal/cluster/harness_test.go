package cluster

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/store/mysql"
	"github.com/Imposter/go-searchlight/internal/store/postgres"
)

// The external databases the suite also runs on; unset, those dialects are skipped.
const (
	envPG    = "SEARCHLIGHT_TEST_PG_URL"
	envMySQL = "SEARCHLIGHT_TEST_MYSQL_URL"
)

// quietLogger discards logs, unless SL_TEST_LOG is set (a level: debug, info, warn).
var quietLogger = func() *slog.Logger {
	lv := os.Getenv("SL_TEST_LOG")
	if lv == "" {
		return slog.New(slog.DiscardHandler)
	}
	var level slog.Level
	_ = level.UnmarshalText([]byte(lv))
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}()

func TestMain(m *testing.M) {
	slog.SetDefault(quietLogger)
	os.Exit(m.Run())
}

// testToken is the cluster token test nodes share.
const testToken = "cluster-test-token-0123456789"

// db is one isolated database of one dialect.
type db struct {
	dialect string
	url     string
}

// forEachDialect runs fn on a fresh database of every available dialect.
func forEachDialect(t *testing.T, fn func(t *testing.T, d *db)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		fn(t, sqliteDB(t))
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

// forSQLiteAndPostgres runs fn on SQLite and, when it is configured and the run is not
// -short, Postgres (whose notifications drive the replica Hub): the heaviest tests skip
// MySQL, which shares one database across every package's tests in CI.
func forSQLiteAndPostgres(t *testing.T, fn func(t *testing.T, d *db)) {
	t.Helper()
	t.Run("sqlite", func(t *testing.T) {
		fn(t, sqliteDB(t))
	})
	t.Run("postgres", func(t *testing.T) {
		base := os.Getenv(envPG)
		if base == "" {
			t.Skip(envPG + " is not set")
		}
		if testing.Short() {
			t.Skip("heavy: not in -short")
		}
		fn(t, postgresDB(t, base))
	})
}

func sqliteDB(t testing.TB) *db {
	// WAL with synchronous NORMAL: several nodes share the file, and each commit's
	// fsync would serialize them all (durability across power loss is not tested).
	path := filepath.ToSlash(filepath.Join(t.TempDir(), "searchlight.db"))
	if strings.HasPrefix(path, "/") {
		return &db{dialect: "sqlite", url: "sqlite://" + path + "?_synchronous=NORMAL"}
	}
	return &db{dialect: "sqlite", url: "sqlite:///" + path + "?_synchronous=NORMAL"}
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
	schemaName := randName("sl_c_")
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
	return &db{dialect: "postgres", url: u.String()}
}

// mysqlDB creates a database for the test when the account may, and otherwise
// empties the configured one of Searchlight's tables, under a lock every package's
// tests share.
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
	name := randName("sl_c_")
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE "+name); err == nil {
		t.Cleanup(func() {
			if _, err := admin.ExecContext(context.Background(), "DROP DATABASE "+name); err != nil {
				t.Errorf("drop database: %v", err)
			}
		})
		u.Path = "/" + name
		return &db{dialect: "mysql", url: u.String()}
	}
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
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate %s: %v", d.dialect, err)
	}
	return st
}

// cluster is an in-process cluster: nodes over one database, each with its own store
// connection, data directory and localhost listener.
type cluster struct {
	t     testing.TB
	db    *db
	base  string
	mod   func(i int, o *Options)
	mu    sync.Mutex
	nodes map[int]*tnode
}

// tnode is one node of a test cluster.
type tnode struct {
	i     int
	c     *cluster
	n     *Node
	st    store.Store
	cfg   config.Config
	ln    net.Listener
	srv   *http.Server
	addr  string
	wrap  *faultStore
	alive atomic.Bool
}

func newCluster(t testing.TB, d *db, mod func(i int, o *Options)) *cluster {
	t.Helper()
	c := &cluster{t: t, db: d, base: t.TempDir(), mod: mod, nodes: map[int]*tnode{}}
	t.Cleanup(c.close)
	return c
}

// fastOptions are a test cluster's timings: a 200 ms heartbeat, a node dead after
// 3 s and leases of 4 s (SQLite serializes every node's writes, so a renewal can wait
// most of a second behind the others).
func fastOptions(o *Options) {
	o.HeartbeatInterval = 200 * time.Millisecond
	o.DeadAfter = 3 * time.Second
	o.LeaseTTL = 4 * time.Second
	o.ViewInterval = 25 * time.Millisecond
	o.CatalogInterval = 100 * time.Millisecond
	o.PruneInterval = 300 * time.Millisecond
	o.PeerTimeout = 3 * time.Second
}

func (c *cluster) config(i int) config.Config {
	cfg := config.Default()
	cfg.StoreURL = c.db.url
	cfg.NodeID = fmt.Sprintf("node-%d", i)
	cfg.DataDir = filepath.Join(c.base, fmt.Sprintf("node-%d", i))
	cfg.InsecureNoAuth = true
	cfg.ClusterToken = testToken
	cfg.RefreshInterval = 20 * time.Millisecond
	cfg.ChangelogPollInterval = 20 * time.Millisecond
	cfg.RemapDebounce = 0
	cfg.RequestTimeout = 20 * time.Second
	cfg.ShutdownTimeout = 10 * time.Second
	cfg.ShutdownGrace = 0
	cfg.MergeThreads = 1
	cfg.SearchThreads = 2
	return cfg
}

// start starts node i (again, when it ran before: same id and data directory).
func (c *cluster) start(i int) *tnode {
	c.t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		c.t.Fatal(err)
	}
	cfg := c.config(i)
	cfg.AdvertiseAddress = ln.Addr().String()
	tn := &tnode{i: i, c: c, cfg: cfg, ln: ln, addr: ln.Addr().String()}
	raw := c.db.open(c.t)
	tn.wrap = newFaultStore(raw)
	tn.st = tn.wrap
	if _, ok := raw.(store.Watcher); ok {
		tn.st = watchingFaultStore{tn.wrap} // Postgres: the replica Hub runs
	}
	o := Options{Store: tn.st, Config: cfg, Version: "test", Logger: quietLogger}
	fastOptions(&o)
	o.Engine = func(eo *node.Options) {
		eo.Logger = quietLogger
		eo.ShardOptions = func(so *shard.Options) { so.Logger = quietLogger }
	}
	if c.mod != nil {
		c.mod(i, &o)
	}
	n, err := New(context.Background(), o)
	if err != nil {
		c.t.Fatal(err)
	}
	tn.n = n
	srv, err := api.NewServer(n, nil, cfg)
	if err != nil {
		c.t.Fatal(err)
	}
	tn.srv = srv.HTTPServer()
	tn.srv.Handler = n.Handler(srv)
	go func() { _ = tn.srv.Serve(ln) }()
	if err := n.Start(context.Background()); err != nil {
		c.t.Fatal(err)
	}
	tn.alive.Store(true)
	c.mu.Lock()
	c.nodes[i] = tn
	c.mu.Unlock()
	return tn
}

// node returns node i.
func (c *cluster) node(i int) *tnode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.nodes[i]
}

// live returns the running nodes.
func (c *cluster) live() []*tnode {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*tnode
	for i := range len(c.nodes) + 8 {
		if tn := c.nodes[i]; tn != nil && tn.alive.Load() {
			out = append(out, tn)
		}
	}
	return out
}

// stop shuts node i down gracefully, as a rolling restart does: drain, then the
// listener, then the node.
func (tn *tnode) stop() {
	tn.c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tn.n.Drain(ctx)
	_ = tn.srv.Shutdown(ctx)
	if err := tn.n.Stop(ctx); err != nil {
		tn.c.t.Errorf("stop node %d: %v", tn.i, err)
	}
	tn.alive.Store(false)
	_ = tn.wrap.Close()
}

// kill stops node i as a crash would: its listener and connections drop at once, its
// shards are abandoned, no lease is released and it does not deregister.
func (tn *tnode) kill() {
	_ = tn.srv.Close()
	tn.n.kill()
	tn.alive.Store(false)
	_ = tn.wrap.Close()
}

func (c *cluster) close() {
	for _, tn := range c.live() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		_ = tn.srv.Close()
		_ = tn.n.Stop(ctx)
		cancel()
		tn.alive.Store(false)
		_ = tn.wrap.Close()
	}
}

// --- helpers ------------------------------------------------------------------------

func tctx(t testing.TB) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// eventually polls cond until it holds, or fails after d.
func eventually(t testing.TB, d time.Duration, what string, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		err := cond()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %v", what, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func createIndex(t testing.TB, n *Node, name string, shards, replicas int) {
	t.Helper()
	spec := api.IndexSpec{Settings: api.IndexSettings{Shards: shards, ReplicasPerShard: replicas}}
	if _, err := n.CreateIndex(tctx(t), name, spec); err != nil {
		t.Fatalf("create index %s: %v", name, err)
	}
}

func upsertOp(id string, v int) api.WriteOp {
	return api.WriteOp{Kind: api.OpUpsert, ID: id, Body: json.RawMessage(fmt.Sprintf(`{"title":"item %s","brand":"b%d","price":%d,"tags":["t%d"]}`, id, v%7, v, v%5))}
}

func mustWrite(t testing.TB, n *Node, index string, ops ...api.WriteOp) int64 {
	t.Helper()
	res, err := n.Write(tctx(t), index, ops, api.WriteOptions{})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	for i, it := range res.Items {
		if it.Err != nil {
			t.Fatalf("op %d: %v", i, it.Err)
		}
	}
	return res.Seq
}

// count runs a match-all search with wait_for_seq on n.
func count(ctx context.Context, n *Node, index string, wait int64) (int64, error) {
	res, err := n.Search(ctx, index, &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitForSeq: wait})
	if err != nil {
		return 0, err
	}
	return res.Total, nil
}

// waitHealth waits until every live node reports status.
func (c *cluster) waitHealth(t testing.TB, status string, d time.Duration) {
	t.Helper()
	eventually(t, d, "cluster health "+status, func() error {
		for _, tn := range c.live() {
			h, err := tn.n.Health(context.Background())
			if err != nil {
				return err
			}
			if h.Status != status {
				return fmt.Errorf("node %d says %s: %+v", tn.i, h.Status, h)
			}
		}
		return nil
	})
}

// faultStore wraps a store so a test can cut the node off from the database.
type faultStore struct {
	store.Store
	mu   sync.Mutex
	down bool
}

func newFaultStore(st store.Store) *faultStore { return &faultStore{Store: st} }

var errPartitioned = errors.New("test: the node is cut off from the database")

func (f *faultStore) cut(down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = down
}

func (f *faultStore) isDown() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.down
}

func (f *faultStore) Registry() store.RegistryStore {
	return &faultRegistry{RegistryStore: f.Store.Registry(), f: f}
}

func (f *faultStore) Indexes() store.IndexStore {
	return &faultIndexes{IndexStore: f.Store.Indexes(), f: f}
}

func (f *faultStore) ScanShard(ctx context.Context, id store.ShardID, fn func(store.Record) error) (int64, error) {
	if f.isDown() {
		return 0, errPartitioned
	}
	return f.Store.ScanShard(ctx, id, fn)
}

func (f *faultStore) Prune(ctx context.Context, id store.ShardID, below int64) error {
	if f.isDown() {
		return errPartitioned
	}
	return f.Store.Prune(ctx, id, below)
}

type faultIndexes struct {
	store.IndexStore
	f *faultStore
}

func (x *faultIndexes) Create(ctx context.Context, m store.IndexMeta) (store.IndexMeta, error) {
	if x.f.isDown() {
		return store.IndexMeta{}, errPartitioned
	}
	return x.IndexStore.Create(ctx, m)
}

func (x *faultIndexes) Get(ctx context.Context, name string) (store.IndexMeta, error) {
	if x.f.isDown() {
		return store.IndexMeta{}, errPartitioned
	}
	return x.IndexStore.Get(ctx, name)
}

func (x *faultIndexes) List(ctx context.Context) ([]store.IndexMeta, error) {
	if x.f.isDown() {
		return nil, errPartitioned
	}
	return x.IndexStore.List(ctx)
}

func (x *faultIndexes) Update(ctx context.Context, m store.IndexMeta) (store.IndexMeta, error) {
	if x.f.isDown() {
		return store.IndexMeta{}, errPartitioned
	}
	return x.IndexStore.Update(ctx, m)
}

func (x *faultIndexes) Drop(ctx context.Context, name string) error {
	if x.f.isDown() {
		return errPartitioned
	}
	return x.IndexStore.Drop(ctx, name)
}

func (f *faultStore) Apply(ctx context.Context, batch []store.Change) (int64, int64, error) {
	if f.isDown() {
		return 0, 0, errPartitioned
	}
	return f.Store.Apply(ctx, batch)
}

func (f *faultStore) ChangesAfter(ctx context.Context, id store.ShardID, seq int64, limit int) ([]store.Change, error) {
	if f.isDown() {
		return nil, errPartitioned
	}
	return f.Store.ChangesAfter(ctx, id, seq, limit)
}

func (f *faultStore) HeadSeq(ctx context.Context) (int64, time.Time, error) {
	if f.isDown() {
		return 0, time.Time{}, errPartitioned
	}
	return f.Store.HeadSeq(ctx)
}

func (f *faultStore) Ping(ctx context.Context) error {
	if f.isDown() {
		return errPartitioned
	}
	return f.Store.Ping(ctx)
}

// GetRecord and ListQueries keep the engine's record reads (store.RecordReader).
func (f *faultStore) GetRecord(ctx context.Context, kind store.RecordKind, id store.ShardID, rec string) (store.Record, error) {
	if f.isDown() {
		return store.Record{}, errPartitioned
	}
	return f.Store.(store.RecordReader).GetRecord(ctx, kind, id, rec) //nolint:forcetypeassert,errcheck // every SQL store reads records
}

func (f *faultStore) ListQueries(ctx context.Context, index, after string, limit int) ([]store.Record, error) {
	if f.isDown() {
		return nil, errPartitioned
	}
	return f.Store.(store.RecordReader).ListQueries(ctx, index, after, limit) //nolint:forcetypeassert,errcheck // every SQL store reads records
}

// watchingFaultStore is a faultStore over a store that pushes notifications.
type watchingFaultStore struct{ *faultStore }

func (w watchingFaultStore) Watch(ctx context.Context, ready func(), fn func(store.Notification)) error {
	if w.isDown() {
		return errPartitioned
	}
	return w.Store.(store.Watcher).Watch(ctx, ready, fn) //nolint:forcetypeassert,errcheck // only built over a Watcher
}

type faultRegistry struct {
	store.RegistryStore
	f *faultStore
}

func (r *faultRegistry) Heartbeat(ctx context.Context, n store.Node) error {
	if r.f.isDown() {
		return errPartitioned
	}
	return r.RegistryStore.Heartbeat(ctx, n)
}

func (r *faultRegistry) Nodes(ctx context.Context) ([]store.Node, error) {
	if r.f.isDown() {
		return nil, errPartitioned
	}
	return r.RegistryStore.Nodes(ctx)
}

func (r *faultRegistry) Copies(ctx context.Context, index string) ([]store.Copy, error) {
	if r.f.isDown() {
		return nil, errPartitioned
	}
	return r.RegistryStore.Copies(ctx, index)
}

func (r *faultRegistry) ClaimCopy(ctx context.Context, id store.ShardID, nodeID string, target int, ttl time.Duration) (store.Copy, bool, error) {
	if r.f.isDown() {
		return store.Copy{}, false, errPartitioned
	}
	return r.RegistryStore.ClaimCopy(ctx, id, nodeID, target, ttl)
}

func (r *faultRegistry) RenewLeases(ctx context.Context, nodeID string, ttl time.Duration) ([]store.ShardID, error) {
	if r.f.isDown() {
		return nil, errPartitioned
	}
	return r.RegistryStore.RenewLeases(ctx, nodeID, ttl)
}

func (r *faultRegistry) SetCopyState(ctx context.Context, c store.Copy, state store.CopyState) error {
	if r.f.isDown() {
		return errPartitioned
	}
	return r.RegistryStore.SetCopyState(ctx, c, state)
}

func (r *faultRegistry) ReportApplied(ctx context.Context, c store.Copy, seq int64) error {
	if r.f.isDown() {
		return errPartitioned
	}
	return r.RegistryStore.ReportApplied(ctx, c, seq)
}

func (r *faultRegistry) RetireCopy(ctx context.Context, c store.Copy) (bool, error) {
	if r.f.isDown() {
		return false, errPartitioned
	}
	return r.RegistryStore.RetireCopy(ctx, c)
}

func (r *faultRegistry) RemoveNode(ctx context.Context, id string) error {
	if r.f.isDown() {
		return errPartitioned
	}
	return r.RegistryStore.RemoveNode(ctx, id)
}

func (r *faultRegistry) ReleaseCopy(ctx context.Context, c store.Copy) error {
	if r.f.isDown() {
		return errPartitioned
	}
	return r.RegistryStore.ReleaseCopy(ctx, c)
}
