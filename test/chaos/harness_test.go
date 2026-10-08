//go:build chaos

package chaos

import (
	"bufio"
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/slproc"
	"github.com/Imposter/go-searchlight/internal/store/postgres"
)

const (
	envPG        = "SEARCHLIGHT_TEST_PG_URL"
	envDBRestart = "SEARCHLIGHT_CHAOS_DB_RESTART"
	// envDir, when set, keeps every node's files and searchlight.log under it (CI
	// uploads them when the suite fails).
	envDir = "SEARCHLIGHT_CHAOS_DIR"

	index        = "chaos"
	shards       = 3
	apiToken     = "chaos-api-token-0123456789abcdef"
	clusterToken = "chaos-cluster-token-0123456789"

	// oldVersion and newVersion label the two builds of the binary a rolling upgrade
	// moves between; every other scenario runs newVersion.
	oldVersion = "chaos-old"
	newVersion = "chaos-new"
)

// nodeSettings are every chaos node's settings: the production defaults, with the
// intervals that pace recovery and convergence shortened so a scenario takes a minute
// or two. Heartbeats (2 s) and the dead-node bound (10 s) stay as in production.
var nodeSettings = []string{
	"log_level=info",
	"refresh_interval=200ms",
	"flush_interval=2s",
	"changelog_poll_interval=100ms",
	"lease_ttl=6s",
	"shutdown_grace=300ms",
	"shutdown_timeout=30s",
	"pprof=true",
}

var builds struct {
	once     sync.Once
	dir      string
	old, cur string
	err      error
}

func TestMain(m *testing.M) {
	code := m.Run()
	if builds.dir != "" {
		_ = os.RemoveAll(builds.dir)
	}
	os.Exit(code)
}

// binaries builds the searchlight binary twice, as oldVersion and newVersion.
func binaries(t *testing.T) (old, cur string) {
	t.Helper()
	builds.once.Do(func() {
		dir, err := os.MkdirTemp("", "searchlight-chaos-")
		if err != nil {
			builds.err = err
			return
		}
		builds.dir = dir
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		for _, b := range []struct {
			version string
			out     *string
		}{{oldVersion, &builds.old}, {newVersion, &builds.cur}} {
			sub := filepath.Join(dir, b.version)
			if err := os.MkdirAll(sub, 0o750); err != nil {
				builds.err = err
				return
			}
			if *b.out, err = slproc.Build(ctx, sub, b.version); err != nil {
				builds.err = err
				return
			}
		}
	})
	if builds.err != nil {
		t.Fatal(builds.err)
	}
	return builds.old, builds.cur
}

// pgURL creates a schema for the test on the Postgres in SEARCHLIGHT_TEST_PG_URL and
// returns a store_url whose search_path is it, or skips the test without one.
func pgURL(t *testing.T) string {
	t.Helper()
	base := os.Getenv(envPG)
	if base == "" {
		t.Skip(envPG + " is not set: the chaos suite runs a multi-node cluster, which needs Postgres (SQLite serves one node)")
	}
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := postgres.Config(u)
	if err != nil {
		t.Fatal(err)
	}
	admin := stdlib.OpenDB(*cfg)
	t.Cleanup(func() { _ = admin.Close() })
	b := make([]byte, 6)
	_, _ = crand.Read(b)
	schema := "sl_chaos_" + hex.EncodeToString(b)
	if _, err := admin.ExecContext(t.Context(), "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		_, _ = admin.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE")
	})
	q := u.Query()
	q.Set("search_path", schema)
	q.Set("application_name", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

// cluster is a cluster of searchlight processes over one Postgres schema, behind a
// load balancer.
type cluster struct {
	t       *testing.T
	store   string
	dir     string
	members []*member
	lb      *balancer
	stop    context.CancelFunc
	// write503 forgives writes answered 503 while the database is down (spec section
	// 10): only the database restart scenario sets it. A read answered 503 or 429, and a
	// write answered 429, is a violation in every scenario.
	write503 bool
	// refusedOnly forgives a failed request to a disrupted node only when its connection
	// was refused: set while nodes stop gracefully, which must never cut a request they
	// accepted.
	refusedOnly bool
}

// member is one node of the cluster, as the load balancer sees it.
type member struct {
	*slproc.Node
	i int
	// ready is what the balancer's health check last saw (/readyz).
	ready atomic.Bool
	// disrupted marks a node a scenario is killing, stopping or restarting: requests
	// failing on it are the balancer's to fail over, not errors.
	disrupted atomic.Bool
	// bulks counts the _bulk requests in flight on it.
	bulks atomic.Int64
	log   *os.File
	// holdRecovery is the node's SEARCHLIGHT_TEST_HOLD_RECOVERY file: while it exists,
	// every copy recovery but the first the process starts waits.
	holdRecovery string
}

// newCluster starts n nodes of bin (the newVersion build when empty) with extra
// settings, waits until they are ready and starts the balancer.
func newCluster(t *testing.T, n int, bin string, extra ...string) *cluster {
	t.Helper()
	store := pgURL(t)
	_, cur := binaries(t)
	if bin == "" {
		bin = cur
	}
	dir := t.TempDir()
	if base := os.Getenv(envDir); base != "" {
		dir = filepath.Join(base, t.Name())
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &cluster{t: t, store: store, dir: dir, stop: cancel}
	c.lb = &balancer{c: c, client: &http.Client{Timeout: time.Minute}, retries: map[string]int{}}
	t.Cleanup(c.close)
	for i := range n {
		c.members = append(c.members, c.launch(i, bin, extra))
	}
	for _, m := range c.members {
		if err := m.WaitReady(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	go c.lb.healthLoop(ctx)
	return c
}

func (c *cluster) launch(i int, bin string, extra []string) *member {
	c.t.Helper()
	dir := filepath.Join(c.dir, fmt.Sprintf("node-%d", i+1))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		c.t.Fatal(err)
	}
	logFile, err := os.OpenFile(filepath.Join(dir, "searchlight.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		c.t.Fatal(err)
	}
	hold := filepath.Join(dir, "hold-recovery")
	n, err := slproc.Launch(c.t.Context(), slproc.Options{
		Bin: bin, Dir: dir, StoreURL: c.store, NodeID: fmt.Sprintf("chaos-%d", i+1),
		Token: apiToken, ClusterToken: clusterToken, Settings: append(slices.Clone(nodeSettings), extra...), Log: logFile,
		Env: []string{"SEARCHLIGHT_TEST_HOLD_RECOVERY=" + hold},
	})
	if err != nil {
		_ = logFile.Close()
		c.t.Fatal(err)
	}
	return &member{Node: n, i: i, log: logFile, holdRecovery: hold}
}

func (c *cluster) close() {
	c.stop()
	for _, m := range c.members {
		if c.t.Failed() {
			c.t.Logf("node %s, last log lines but the access log:\n%s", m.NodeID(), m.Logs().TailExcept(40, "request served"))
		}
		_ = m.Kill()
		_ = m.log.Close()
	}
}

// --- the load balancer ------------------------------------------------------------------

// balancer sends each request to a ready node, round robin, as a load balancer in front
// of the cluster would. A request failing on a node the scenario is disrupting fails
// over to the next node: the one failure a client may see with two or more copies. A
// 429 or 503 is retried, and is a violation unless the scenario forgives it (a write
// while the database is down). Anything else that fails, or a request that keeps
// failing past its deadline, is a client-visible error: a violation.
type balancer struct {
	c      *cluster
	client *http.Client
	next   atomic.Uint64

	mu         sync.Mutex
	retries    map[string]int
	violations []string
}

// healthLoop probes every node's readiness every 100 ms.
func (b *balancer) healthLoop(ctx context.Context) {
	for ctx.Err() == nil {
		for _, m := range b.c.members {
			m.ready.Store(slproc.Ready(ctx, m.AdminURL))
		}
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (b *balancer) pick() *member {
	ms := b.c.members
	start := int(b.next.Add(1))
	for k := range ms {
		if m := ms[(start+k)%len(ms)]; m.ready.Load() {
			return m
		}
	}
	return ms[start%len(ms)]
}

func (b *balancer) retried(kind string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.retries[kind]++
}

func (b *balancer) violate(format string, args ...any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.violations) < 50 {
		b.violations = append(b.violations, fmt.Sprintf(format, args...))
	}
}

// answer is a response the balancer settled on.
type answer struct {
	status int
	body   []byte
}

// do sends a request until it gets an answer that is neither a failover nor a
// retryable status, or until deadline; then it is a violation and do returns an error.
func (b *balancer) do(ctx context.Context, method, path, body string, deadline time.Duration, write bool) (answer, error) {
	end := time.Now().Add(deadline)
	kind := "read"
	if write {
		kind = "write"
	}
	var last string
	for time.Now().Before(end) {
		if ctx.Err() != nil {
			return answer{}, ctx.Err()
		}
		m := b.pick()
		a, err := b.send(ctx, m, method, path, body, write)
		switch {
		case err != nil && ctx.Err() != nil:
			return answer{}, ctx.Err()
		case err != nil:
			switch {
			case !m.disrupted.Load():
				b.violate("%s %s: %s failed while it was not disrupted: %v", method, path, m.NodeID(), err)
			case b.c.refusedOnly && !refused(err):
				b.violate("%s %s: %s cut a request while it stopped gracefully: %v", method, path, m.NodeID(), err)
			}
			if refused(err) {
				b.retried("failover refused")
			} else {
				b.retried("failover cut")
			}
			last = err.Error()
			time.Sleep(20 * time.Millisecond)
			continue
		case a.status == http.StatusTooManyRequests || a.status == http.StatusServiceUnavailable:
			b.retried(fmt.Sprintf("%s %d", kind, a.status))
			if a.status == http.StatusTooManyRequests || !write || !b.c.write503 {
				b.violate("%s %s: %s answered HTTP %d %s", method, path, m.NodeID(), a.status, truncate(bytes.TrimSpace(a.body)))
			}
			last = fmt.Sprintf("HTTP %d %s", a.status, bytes.TrimSpace(a.body))
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return a, nil
	}
	b.violate("%s %s kept failing for %s: %s", method, path, deadline, last)
	return answer{}, fmt.Errorf("%s %s kept failing: %s", method, path, last)
}

// refused reports whether err is a connection refused: the node was not listening.
func refused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) || strings.Contains(strings.ToLower(err.Error()), "refused")
}

// retriedOf is how often the balancer retried for kind ("failover refused", "failover cut",
// "write 503", ...).
func (b *balancer) retriedOf(kind string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.retries[kind]
}

func (b *balancer) send(ctx context.Context, m *member, method, path, body string, bulk bool) (answer, error) {
	if bulk {
		m.bulks.Add(1)
		defer m.bulks.Add(-1)
	}
	return call(ctx, b.client, method, m.URL+path, body)
}

func call(ctx context.Context, client *http.Client, method, u, body string) (answer, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, strings.NewReader(body))
	if err != nil {
		return answer{}, err
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		return answer{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return answer{}, err
	}
	return answer{status: resp.StatusCode, body: b}, nil
}

// --- the workload -----------------------------------------------------------------------

// doc is a document as the model holds it.
type doc struct {
	v, n  int
	brand string
}

// model is what the acknowledged writes left: every live document at its last
// acknowledged version, and the newest acknowledged seq. A batch whose answer never
// came (the load stopped mid-request) may or may not have committed: each of its
// operations is a possible outcome for its id until an acknowledged write supersedes
// it.
type model struct {
	mu    sync.Mutex
	docs  map[string]doc
	maybe map[string][]outcome
	head  int64
	acked int64
}

// outcome is a state an id may be in: a document, or absent.
type outcome struct {
	present bool
	d       doc
}

func newModel() *model { return &model{docs: map[string]doc{}, maybe: map[string][]outcome{}} }

// view is a copy of the model at one moment.
type view struct {
	docs  map[string]doc
	maybe map[string][]outcome
	head  int64
}

func (m *model) snapshot() view {
	m.mu.Lock()
	defer m.mu.Unlock()
	v := view{docs: make(map[string]doc, len(m.docs)), maybe: make(map[string][]outcome, len(m.maybe)), head: m.head}
	for k, d := range m.docs {
		v.docs[k] = d
	}
	for k, o := range m.maybe {
		v.maybe[k] = slices.Clone(o)
	}
	return v
}

// outcomes are the states id may be in.
func (v *view) outcomes(id string) []outcome {
	d, ok := v.docs[id]
	return append([]outcome{{present: ok, d: d}}, v.maybe[id]...)
}

// ids are every id the model knows, acknowledged or possible.
func (v *view) ids() []string {
	out := make([]string, 0, len(v.docs)+len(v.maybe))
	for id := range v.docs {
		out = append(out, id)
	}
	for id := range v.maybe {
		if _, ok := v.docs[id]; !ok {
			out = append(out, id)
		}
	}
	return out
}

// bounds are the fewest and most documents matching keep the model allows.
func (v *view) bounds(keep func(id string, d doc) bool) (lo, hi int64) {
	for _, id := range v.ids() {
		in, out := false, false
		for _, o := range v.outcomes(id) {
			if o.present && keep(id, o.d) {
				in = true
			} else {
				out = true
			}
		}
		if in {
			hi++
			if !out {
				lo++
			}
		}
	}
	return lo, hi
}

// createIndex creates the chaos index on the first node and waits until every node
// serves it.
func (c *cluster) createIndex() {
	c.t.Helper()
	body := fmt.Sprintf(`{"mapping": {"dynamic": "strict", "fields": {"w": "keyword", "k": "number", "v": "number", "n": "number", "brand": "keyword", "title": "text"}}, "settings": {"shards": %d}}`, shards)
	a, err := call(c.t.Context(), c.lb.client, http.MethodPut, c.members[0].URL+"/indexes/"+index, body)
	if err != nil || a.status != http.StatusCreated {
		c.t.Fatalf("create the index: %v HTTP %d %s", err, a.status, a.body)
	}
	c.waitGreen(3 * time.Minute)
}

// load is a running write and read load.
type load struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
	m      *model
	reads  atomic.Int64
	writes atomic.Int64
}

// writer ids: each writer owns keys of its own, so it alone writes them, one batch at a
// time, retrying a batch until it is acknowledged: the model is exactly what the
// acknowledged batches left.
const (
	keysPerWriter = 1500
	batchOps      = 40
	// writerPause paces each writer between batches: the load is a steady stream a
	// shared CI runner (or a laptop's disk) sustains beside three nodes and Postgres,
	// not a saturation test.
	writerPause = 25 * time.Millisecond
)

// startLoad starts writers (_bulk upserts and deletes) and readers (searches and
// counts) through the balancer.
func (c *cluster) startLoad(m *model, writers, readers int) *load {
	ctx, cancel := context.WithCancel(context.Background())
	l := &load{cancel: cancel, m: m}
	for g := range writers {
		l.wg.Go(func() { c.writer(ctx, l, g) })
	}
	for g := range readers {
		l.wg.Go(func() { c.reader(ctx, l, g) })
	}
	return l
}

// stop stops the load and waits for its requests to finish.
func (l *load) stop() {
	l.cancel()
	l.wg.Wait()
}

// waitWrites waits until the load has made n more acknowledged writes.
func (l *load) waitWrites(t *testing.T, n int64) {
	t.Helper()
	want := l.writes.Load() + n
	deadline := time.Now().Add(3 * time.Minute)
	for l.writes.Load() < want {
		if time.Now().After(deadline) {
			t.Fatalf("the load made %d of %d acknowledged writes in 3 minutes", l.writes.Load(), want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (c *cluster) writer(ctx context.Context, l *load, g int) {
	rnd := rand.New(rand.NewPCG(uint64(g)+1, uint64(time.Now().UnixNano())))
	versions, live := map[int]int{}, map[int]bool{}
	for ctx.Err() == nil {
		type op struct {
			id     string
			k, v   int
			delete bool
			brand  string
		}
		var ops []op
		var body strings.Builder
		picked := map[int]bool{}
		for len(ops) < batchOps {
			k := rnd.IntN(keysPerWriter)
			if picked[k] {
				continue
			}
			picked[k] = true
			id := fmt.Sprintf("w%d-%04d", g, k)
			if live[k] && rnd.IntN(100) < 15 {
				ops = append(ops, op{id: id, k: k, delete: true})
				fmt.Fprintf(&body, "{\"delete\": {\"id\": %q}}\n", id)
				continue
			}
			v := versions[k] + 1
			o := op{id: id, k: k, v: v, brand: fmt.Sprintf("b%d", rnd.IntN(7))}
			ops = append(ops, o)
			fmt.Fprintf(&body, "{\"upsert\": {\"id\": %q}}\n{\"w\": \"w%d\", \"k\": %d, \"v\": %d, \"n\": %d, \"brand\": %q, \"title\": \"item %d version %d\"}\n",
				id, g, k, v, g*keysPerWriter+k, o.brand, k, v)
		}
		a, err := c.lb.do(ctx, http.MethodPost, "/indexes/"+index+"/_bulk", body.String(), 3*time.Minute, true)
		if err != nil {
			l.m.mu.Lock()
			for _, o := range ops {
				d := doc{v: o.v, n: g*keysPerWriter + o.k, brand: o.brand}
				l.m.maybe[o.id] = append(l.m.maybe[o.id], outcome{present: !o.delete, d: d})
				if !o.delete {
					versions[o.k] = o.v
				}
			}
			l.m.mu.Unlock()
			if ctx.Err() == nil {
				time.Sleep(time.Second)
			}
			continue
		}
		var resp struct {
			Seq   int64 `json:"seq"`
			Items []struct {
				Status int             `json:"status"`
				Error  json.RawMessage `json:"error"`
			} `json:"items"`
		}
		if a.status != http.StatusOK || json.Unmarshal(a.body, &resp) != nil || len(resp.Items) != len(ops) {
			c.lb.violate("_bulk: HTTP %d %s", a.status, truncate(a.body))
			continue
		}
		ok := true
		for i, it := range resp.Items {
			gone := ops[i].delete && it.Status == http.StatusNotFound
			if it.Status != http.StatusOK && !gone {
				c.lb.violate("_bulk item %s: HTTP %d %s", ops[i].id, it.Status, it.Error)
				ok = false
			}
		}
		if !ok {
			continue
		}
		l.m.mu.Lock()
		for _, o := range ops {
			delete(l.m.maybe, o.id)
			if o.delete {
				delete(l.m.docs, o.id)
				live[o.k] = false
				continue
			}
			l.m.docs[o.id] = doc{v: o.v, n: g*keysPerWriter + o.k, brand: o.brand}
			versions[o.k], live[o.k] = o.v, true
		}
		l.m.head = max(l.m.head, resp.Seq)
		l.m.acked += int64(len(ops))
		l.m.mu.Unlock()
		l.writes.Add(int64(len(ops)))
		time.Sleep(writerPause)
	}
}

func (c *cluster) reader(ctx context.Context, l *load, g int) {
	rnd := rand.New(rand.NewPCG(uint64(g)+100, uint64(time.Now().UnixNano())))
	for ctx.Err() == nil {
		l.m.mu.Lock()
		head := l.m.head
		l.m.mu.Unlock()
		var path, body string
		switch rnd.IntN(3) {
		case 0:
			path, body = "/indexes/"+index+"/_count", fmt.Sprintf(`{"query": {"field": "brand", "op": "eq", "value": "b%d"}}`, rnd.IntN(7))
		case 1:
			path, body = "/indexes/"+index+"/_search", fmt.Sprintf(`{"query": {"field": "k", "op": "lt", "value": %d}, "sort": [{"n": "desc"}], "size": 20}`, rnd.IntN(keysPerWriter))
		default:
			path, body = "/indexes/"+index+"/_search", `{"size": 0, "aggs": {"brands": {"terms": {"field": "brand", "size": 10}}}}`
		}
		if !c.write503 {
			// Read-your-writes through the balancer: the newest acknowledged seq, which
			// any node checks against the database's head. While the database is down a
			// node cannot, and answers 503: that scenario reads without it.
			path += fmt.Sprintf("?wait_for_seq=%d", head)
		}
		a, err := c.lb.do(ctx, http.MethodPost, path, body, time.Minute, false)
		if err != nil {
			continue
		}
		if a.status != http.StatusOK {
			c.lb.violate("POST %s: HTTP %d %s", path, a.status, truncate(a.body))
			continue
		}
		l.reads.Add(1)
		time.Sleep(time.Duration(rnd.IntN(20)) * time.Millisecond)
	}
}

func truncate(b []byte) string {
	if len(b) > 300 {
		return string(b[:300]) + "…"
	}
	return string(b)
}

// --- the invariants ---------------------------------------------------------------------

// shardCopy is one copy as /_cluster/shards reports it.
type shardCopy struct {
	Index        string `json:"index"`
	Shard        int    `json:"shard"`
	Node         string `json:"node"`
	State        string `json:"state"`
	AppliedSeq   int64  `json:"applied_seq"`
	CommittedSeq int64  `json:"committed_seq"`
	Docs         int64  `json:"docs"`
}

// waitDurable waits until every copy on node i has flushed documents (its committed
// seq past 0): a crash from then on must find them on disk when the node reopens.
func (c *cluster) waitDurable(i int) {
	c.t.Helper()
	eventually(c.t, 2*time.Minute, c.members[i].NodeID()+"'s copies are durable", func() error {
		copies, err := c.ownCopies(c.t.Context(), c.members[i])
		if err != nil {
			return err
		}
		if len(copies) != shards {
			return fmt.Errorf("%d copies", len(copies))
		}
		for _, sc := range copies {
			if sc.CommittedSeq == 0 || sc.Docs == 0 {
				return fmt.Errorf("shard %d: committed seq %d, %d documents", sc.Shard, sc.CommittedSeq, sc.Docs)
			}
		}
		return nil
	})
}

// ownCopies are the copies of the chaos index that m holds, as m reports them.
func (c *cluster) ownCopies(ctx context.Context, m *member) ([]shardCopy, error) {
	a, err := call(ctx, c.lb.client, http.MethodGet, m.URL+"/_cluster/shards", "")
	if err != nil {
		return nil, err
	}
	if a.status != http.StatusOK {
		return nil, fmt.Errorf("/_cluster/shards: HTTP %d", a.status)
	}
	var body struct {
		Shards []shardCopy `json:"shards"`
	}
	if err := json.Unmarshal(a.body, &body); err != nil {
		return nil, err
	}
	var out []shardCopy
	for _, sc := range body.Shards {
		if sc.Index == index && sc.Node == m.NodeID() {
			out = append(out, sc)
		}
	}
	return out, nil
}

// waitGreen waits until every node reports the cluster green.
func (c *cluster) waitGreen(d time.Duration) {
	c.t.Helper()
	eventually(c.t, d, "the cluster is green on every node", func() error {
		for _, m := range c.members {
			a, err := call(c.t.Context(), c.lb.client, http.MethodGet, m.URL+"/_cluster/health", "")
			if err != nil {
				return err
			}
			var h struct {
				Status string `json:"status"`
			}
			if err := json.Unmarshal(a.body, &h); err != nil || h.Status != "green" {
				return fmt.Errorf("%s: HTTP %d %s", m.NodeID(), a.status, a.body)
			}
		}
		return nil
	})
}

// settle waits until every node is ready and every copy of every shard serves, at or
// past the newest acknowledged seq, as many documents as every other copy of the shard
// and as the model allows.
func (c *cluster) settle(m *model) {
	c.t.Helper()
	v := m.snapshot()
	lo, hi := make([]int64, shards), make([]int64, shards)
	for s := range shards {
		lo[s], hi[s] = v.bounds(func(id string, _ doc) bool { return node.ShardFor(id, shards) == s })
	}
	start := time.Now()
	eventually(c.t, 5*time.Minute, "every copy converges on the acknowledged writes", func() error {
		docs := make([]int64, shards)
		for k, mb := range c.members {
			if !slproc.Ready(c.t.Context(), mb.AdminURL) {
				return fmt.Errorf("%s is not ready", mb.NodeID())
			}
			copies, err := c.ownCopies(c.t.Context(), mb)
			if err != nil {
				return fmt.Errorf("%s: %w", mb.NodeID(), err)
			}
			if len(copies) != shards {
				return fmt.Errorf("%s holds %d copies: %+v", mb.NodeID(), len(copies), copies)
			}
			for _, sc := range copies {
				s := sc.Shard
				if sc.State != "serving" || sc.AppliedSeq < v.head || sc.Docs < lo[s] || sc.Docs > hi[s] || (k > 0 && sc.Docs != docs[s]) {
					return fmt.Errorf("%s shard %d: %s at seq %d with %d documents; want serving at %d with %d to %d (as the other copies: %d)",
						mb.NodeID(), s, sc.State, sc.AppliedSeq, sc.Docs, v.head, lo[s], hi[s], docs[s])
				}
				docs[s] = sc.Docs
			}
		}
		return nil
	})
	c.t.Logf("converged in %s: %d acknowledged documents (%d ids unacknowledged) at seq %d",
		time.Since(start).Round(time.Millisecond), len(v.docs), len(v.maybe), v.head)
}

// verify checks every node answers with exactly the documents the model allows, all
// of them alike, with the same counts, and that the load saw no client-visible error.
func (c *cluster) verify(m *model) {
	c.t.Helper()
	v := m.snapshot()
	var first map[string]int
	for _, mb := range c.members {
		got := c.scan(mb, v.head)
		var wrong []string
		for _, id := range v.ids() {
			g, served := got[id]
			if !slices.ContainsFunc(v.outcomes(id), func(o outcome) bool { return o.present == served && (!served || o.d.v == g) }) {
				wrong = append(wrong, fmt.Sprintf("%s (served %v at v%d; allowed %v)", id, served, g, v.outcomes(id)))
			}
		}
		for id := range got {
			if _, known := v.docs[id]; !known && len(v.maybe[id]) == 0 {
				wrong = append(wrong, id+" (never written)")
			}
		}
		if len(wrong) > 0 {
			c.t.Errorf("%s serves %d documents, %d of them not as the acknowledged writes left them: %v", mb.NodeID(), len(got), len(wrong), head5(wrong))
		}
		if first == nil {
			first = got
		} else if !maps.Equal(first, got) {
			c.t.Errorf("%s serves other documents than %s", mb.NodeID(), c.members[0].NodeID())
		}
		for b := range 7 {
			brand := fmt.Sprintf("b%d", b)
			lo, hi := v.bounds(func(_ string, d doc) bool { return d.brand == brand })
			var cr struct {
				Count int64 `json:"count"`
			}
			a, err := call(c.t.Context(), c.lb.client, http.MethodPost, fmt.Sprintf("%s/indexes/%s/_count?wait_for_seq=%d", mb.URL, index, v.head),
				fmt.Sprintf(`{"query": {"field": "brand", "op": "eq", "value": %q}}`, brand))
			if err != nil || a.status != http.StatusOK || json.Unmarshal(a.body, &cr) != nil {
				c.t.Errorf("%s: count of %s: %v HTTP %d %s", mb.NodeID(), brand, err, a.status, truncate(a.body))
				continue
			}
			if cr.Count < lo || cr.Count > hi {
				c.t.Errorf("%s counts %d documents of %s, want %d to %d", mb.NodeID(), cr.Count, brand, lo, hi)
			}
		}
	}
	c.lb.mu.Lock()
	defer c.lb.mu.Unlock()
	for _, viol := range c.lb.violations {
		c.t.Errorf("client-visible error: %s", viol)
	}
	c.t.Logf("acknowledged %d write operations; retried: %v", m.acked, c.lb.retries)
}

func head5(s []string) []string {
	slices.Sort(s)
	return s[:min(5, len(s))]
}

// scan pages through every document of the index on mb, as of seq, and returns each
// one's version.
func (c *cluster) scan(mb *member, seq int64) map[string]int {
	c.t.Helper()
	out := map[string]int{}
	var after json.RawMessage
	for {
		body := `{"sort": [{"n": "asc"}], "size": 1000, "fields": ["v"]`
		if after != nil {
			body += `, "search_after": ` + string(after)
		}
		body += "}"
		a, err := call(c.t.Context(), c.lb.client, http.MethodPost, fmt.Sprintf("%s/indexes/%s/_search?wait_for_seq=%d", mb.URL, index, seq), body)
		if err != nil || a.status != http.StatusOK {
			c.t.Fatalf("%s: scan: %v HTTP %d %s", mb.NodeID(), err, a.status, truncate(a.body))
		}
		var resp struct {
			Hits []struct {
				ID   string `json:"id"`
				Body struct {
					V int `json:"v"`
				} `json:"body"`
			} `json:"hits"`
			Next json.RawMessage `json:"next"`
		}
		if err := json.Unmarshal(a.body, &resp); err != nil {
			c.t.Fatal(err)
		}
		for _, h := range resp.Hits {
			if _, dup := out[h.ID]; dup {
				c.t.Errorf("%s returns %s twice", mb.NodeID(), h.ID)
			}
			out[h.ID] = h.Body.V
		}
		if len(resp.Next) == 0 || string(resp.Next) == "null" {
			return out
		}
		after = resp.Next
	}
}

// --- disruptions and probes ------------------------------------------------------------

// kill ends node i at once (SIGKILL).
func (c *cluster) kill(i int) {
	c.t.Helper()
	m := c.members[i]
	m.disrupted.Store(true)
	m.ready.Store(false)
	if err := m.Kill(); err != nil {
		c.t.Fatal(err)
	}
	c.t.Logf("killed %s", m.NodeID())
}

// restart starts node i again and waits until it is ready.
func (c *cluster) restart(i int) {
	c.t.Helper()
	m := c.members[i]
	if err := m.Start(c.t.Context()); err != nil {
		c.t.Fatal(err)
	}
	if err := m.WaitReady(c.t.Context()); err != nil {
		c.dumpGoroutines()
		c.t.Fatal(err)
	}
	m.disrupted.Store(false)
}

// dumpGoroutines saves every running node's goroutines (pprof) beside its log, for a
// scenario that got stuck.
func (c *cluster) dumpGoroutines() {
	for _, m := range c.members {
		a, err := call(context.Background(), c.lb.client, http.MethodGet, m.AdminURL+"/debug/pprof/goroutine?debug=2", "")
		if err != nil {
			continue
		}
		path := filepath.Join(c.dir, fmt.Sprintf("node-%d", m.i+1), "goroutines.txt")
		if err := os.WriteFile(path, a.body, 0o600); err == nil {
			c.t.Logf("%s's goroutines: %s", m.NodeID(), path)
		}
	}
}

// metric reads the largest value of a metric on node i's admin listener.
func (c *cluster) metric(i int, name string) float64 {
	a, err := call(c.t.Context(), c.lb.client, http.MethodGet, c.members[i].AdminURL+"/metrics", "")
	if err != nil {
		return 0
	}
	var top float64
	sc := bufio.NewScanner(bytes.NewReader(a.body))
	sc.Buffer(make([]byte, 0, 64<<10), 4<<20)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, name+"{") && !strings.HasPrefix(line, name+" ") {
			continue
		}
		f := strings.Fields(line)
		if v, err := strconv.ParseFloat(f[len(f)-1], 64); err == nil {
			top = max(top, v)
		}
	}
	return top
}

func restartDatabase(t *testing.T, cmdline, logPath string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(ctx, "cmd", "/C", cmdline)
	} else {
		cmd = exec.CommandContext(ctx, "sh", "-c", cmdline)
	}
	out, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	cmd.Stdout, cmd.Stderr = out, out
	start := time.Now()
	if err := cmd.Run(); err != nil {
		b, _ := os.ReadFile(logPath)
		t.Fatalf("%s: %v\n%s", envDBRestart, err, b)
	}
	t.Logf("the database restarted in %s", time.Since(start).Round(time.Millisecond))
}

func eventually(t *testing.T, d time.Duration, what string, cond func() error) {
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
		time.Sleep(100 * time.Millisecond)
	}
}
