package cluster

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/store/postgres"
	"github.com/Imposter/go-searchlight/internal/testtier"
)

// liveCopies returns the copies of id held under a live lease, as st reads the
// registry.
func liveCopies(t testing.TB, st store.Store, id store.ShardID) []store.Copy {
	t.Helper()
	list, err := st.Registry().Copies(context.Background(), id.Index)
	if err != nil {
		t.Fatal(err)
	}
	var out []store.Copy
	for _, c := range list { //nolint:gocritic // a short list
		if c.Shard == id && c.LeaseLeft > 0 {
			out = append(out, c)
		}
	}
	return out
}

// serving counts id's serving copies under a live lease.
func serving(t testing.TB, st store.Store, id store.ShardID) (n int, nodes []string) {
	t.Helper()
	for _, c := range liveCopies(t, st, id) { //nolint:gocritic // a short list
		if c.State == store.CopyServing {
			n++
			nodes = append(nodes, c.NodeID)
		}
	}
	slices.Sort(nodes)
	return n, nodes
}

// waitCopies waits until every shard of index has want serving copies.
func waitCopies(t testing.TB, st store.Store, index string, shards, want int, d time.Duration) {
	t.Helper()
	eventually(t, d, fmt.Sprintf("%s has %d serving copies per shard", index, want), func() error {
		for s := range shards {
			id := store.ShardID{Index: index, Shard: s}
			if n, nodes := serving(t, st, id); n != want || len(liveCopies(t, st, id)) != want {
				var rows []string
				for _, c := range liveCopies(t, st, id) { //nolint:gocritic // a short list
					rows = append(rows, fmt.Sprintf("%s/slot%d/%s/e%d/%s", c.NodeID, c.Slot, c.State, c.Epoch, c.LeaseLeft))
				}
				return fmt.Errorf("shard %d: %d serving (%v), %d live: %v", s, n, nodes, len(rows), rows)
			}
		}
		return nil
	})
}

// TestJoinAllocateAndServe: three nodes join over one database; an index with the
// default target (every node) gets a copy on each, writes through every node land on
// every copy, and health turns green.
func TestJoinAllocateAndServe(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		c := newCluster(t, d, nil)
		a := c.start(0)
		createIndex(t, a.n, "items", 2, 0)
		c.start(1)
		c.start(2)
		c.waitHealth(t, api.StatusGreen, 30*time.Second)
		waitCopies(t, a.st, "items", 2, 3, 30*time.Second)
		var last int64
		for i, tn := range c.live() {
			for k := range 10 {
				last = max(last, mustWrite(t, tn.n, "items", upsertOp(fmt.Sprintf("n%d-%d", i, k), k)))
			}
		}
		for _, tn := range c.live() {
			waitCount(t, tn.n, "items", last, int64(30))
		}
		nodes, err := a.n.Nodes(tctx(t))
		if err != nil || len(nodes) != 3 || !nodes[0].Self {
			t.Fatalf("nodes %+v (%v)", nodes, err)
		}
		shards, err := a.n.Shards(tctx(t))
		if err != nil || len(shards) != 6 {
			t.Fatalf("shards %d (%v): %+v", len(shards), err, shards)
		}
		for _, s := range shards {
			if s.State != api.ShardServing || s.Docs == 0 {
				t.Errorf("copy %+v", s)
			}
		}
		if err := a.n.Ready(tctx(t)); err != nil {
			t.Fatalf("not ready: %v", err)
		}
	})
}

// TestAllocationToTargetAndRelease: with replicas_per_shard 2 over three nodes, every
// shard has exactly two copies, spread evenly; lowering the target to 1 releases the
// extra copies, raising it to 3 places one on every node.
func TestAllocationToTargetAndRelease(t *testing.T) {
	forSQLiteAndPostgres(t, func(t *testing.T, d *db) {
		c := newCluster(t, d, nil)
		a := c.start(0)
		c.start(1)
		c.start(2)
		createIndex(t, a.n, "t2", 3, 2)
		waitCopies(t, a.st, "t2", 3, 2, 30*time.Second)
		load := map[string]int{}
		for s := range 3 {
			for _, cp := range liveCopies(t, a.st, store.ShardID{Index: "t2", Shard: s}) {
				load[cp.NodeID]++
			}
		}
		for node, k := range load {
			if k < 1 || k > 3 {
				t.Errorf("node %s holds %d copies: %v", node, k, load)
			}
		}
		if len(load) < 2 {
			t.Errorf("copies on %d nodes: %v", len(load), load)
		}
		c.waitHealth(t, api.StatusGreen, 30*time.Second)

		one := 1
		if _, err := a.n.PatchSettings(tctx(t), "t2", api.SettingsPatch{ReplicasPerShard: &one}); err != nil {
			t.Fatal(err)
		}
		waitCopies(t, a.st, "t2", 3, 1, 30*time.Second)
		three := 3
		if _, err := c.node(1).n.PatchSettings(tctx(t), "t2", api.SettingsPatch{ReplicasPerShard: &three}); err != nil {
			t.Fatal(err)
		}
		waitCopies(t, a.st, "t2", 3, 3, 30*time.Second)
		c.waitHealth(t, api.StatusGreen, 30*time.Second)
	})
}

// TestNodeKillReallocatesAndHealth: a node holding copies is killed (no release, no
// deregistration). An index whose only copy was on it goes red; once its leases
// expire the third node claims its shards, recovers them from a peer (or the store,
// for the one with no other copy), and health is green again.
func TestNodeKillReallocatesAndHealth(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		c := newCluster(t, d, nil)
		a := c.start(0)
		b := c.start(1)
		createIndex(t, a.n, "r2", 2, 2)
		waitCopies(t, a.st, "r2", 2, 2, 30*time.Second)
		var last int64
		for k := range 50 {
			last = mustWrite(t, a.n, "r2", upsertOp(fmt.Sprintf("d%d", k), k))
		}
		// A single-copy index on b alone: red when b dies.
		createIndex(t, b.n, "solo", 1, 1)
		eventually(t, 30*time.Second, "solo serves on b", func() error {
			if n, nodes := serving(t, a.st, store.ShardID{Index: "solo", Shard: 0}); n != 1 || nodes[0] != b.n.id {
				return fmt.Errorf("%d serving on %v", n, nodes)
			}
			return nil
		})
		cn := c.start(2)
		c.waitHealth(t, api.StatusGreen, 30*time.Second)

		b.kill()
		eventually(t, 30*time.Second, "health turns red (solo has no copy)", func() error {
			h, _ := a.n.Health(context.Background())
			if h.Status != api.StatusRed {
				return fmt.Errorf("%+v", h)
			}
			return nil
		})
		// The shards b held are claimed by the third node and recovered.
		waitCopies(t, a.st, "r2", 2, 2, 60*time.Second)
		waitCopies(t, a.st, "solo", 1, 1, 60*time.Second)
		c.waitHealth(t, api.StatusGreen, 60*time.Second)
		for s := range 2 {
			if _, nodes := serving(t, a.st, store.ShardID{Index: "r2", Shard: s}); slices.Contains(nodes, b.n.id) {
				t.Fatalf("shard %d still served by the dead node: %v", s, nodes)
			}
		}
		for _, tn := range []*tnode{a, cn} {
			waitCount(t, tn.n, "r2", last, int64(50))
		}
	})
}

// reader runs searches through nodes until stopped, counting client errors.
type reader struct {
	reads, errs atomic.Int64
	mu          sync.Mutex
	firstErr    error
	stop        chan struct{}
	done        sync.WaitGroup
}

func startReaders(nodes []*tnode, index string, perNode int, want func() int64) *reader {
	r := &reader{stop: make(chan struct{})}
	for _, tn := range nodes {
		for range perNode {
			r.done.Go(func() {
				for {
					select {
					case <-r.stop:
						return
					default:
					}
					floor := int64(0)
					if want != nil {
						floor = want()
					}
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					res, err := tn.n.Search(ctx, index, &search.Request{Query: &query.All{}, Size: 5, TrackTotal: search.TrackTotalAll}, api.ReadOptions{})
					cancel()
					r.reads.Add(1)
					if err == nil && res.Total < floor {
						err = fmt.Errorf("saw %d documents, at least %d were acknowledged", res.Total, floor)
					}
					if err != nil {
						r.errs.Add(1)
						r.mu.Lock()
						if r.firstErr == nil {
							r.firstErr = fmt.Errorf("node %d: %w", tn.i, err)
						}
						r.mu.Unlock()
					}
					time.Sleep(2 * time.Millisecond)
				}
			})
		}
	}
	return r
}

func (r *reader) finish(t testing.TB) {
	t.Helper()
	close(r.stop)
	r.done.Wait()
	if n := r.errs.Load(); n > 0 {
		t.Fatalf("%d of %d reads failed; the first: %v", n, r.reads.Load(), r.firstErr)
	}
	if r.reads.Load() == 0 {
		t.Fatal("no read ran")
	}
	t.Logf("%d reads, no client error", r.reads.Load())
}

// TestReadsRetryAroundDeadNode: an index with two copies of each of four shards over
// three nodes, so every node reads some shards from peers. A node is killed while
// readers on the others run: the reads routed to it fail over to the other copy, and
// no client sees an error.
func TestReadsRetryAroundDeadNode(t *testing.T) {
	testtier.Heavy(t)
	forSQLiteAndPostgres(t, func(t *testing.T, d *db) {
		c := newCluster(t, d, nil)
		a := c.start(0)
		c.start(1)
		c.start(2)
		createIndex(t, a.n, "rr", 4, 2)
		waitCopies(t, a.st, "rr", 4, 2, 30*time.Second)
		var last int64
		for k := range 40 {
			last = mustWrite(t, a.n, "rr", upsertOp(fmt.Sprintf("d%d", k), k))
		}
		for _, tn := range c.live() {
			waitCount(t, tn.n, "rr", last, int64(40))
		}
		victim := c.node(1)
		survivors := []*tnode{c.node(0), c.node(2)}
		r := startReaders(survivors, "rr", 3, func() int64 { return 40 })
		time.Sleep(300 * time.Millisecond)
		victim.kill()
		time.Sleep(5 * time.Second) // past DeadAfter and the leases: reallocation runs under the readers
		r.finish(t)
		waitCopies(t, a.st, "rr", 4, 2, 60*time.Second)
	})
}

// TestRollingRestartNoClientErrors restarts every node in turn, gracefully, while
// readers on the other nodes search and a writer writes: no client error, and every
// acknowledged write stays searchable. The stopping node holds the replies of the query
// phases it pins until its copy retires (its registry write slowed, as a busy database
// does), so searches fetch from copies that stopped serving between their phases: every
// such fetch still finds its pinned generation.
func TestRollingRestartNoClientErrors(t *testing.T) {
	testtier.Heavy(t)
	forSQLiteAndPostgres(t, func(t *testing.T, d *db) {
		var (
			c        *cluster
			stopping atomic.Pointer[tnode]
			crossed  atomic.Int64
			gone     atomic.Int64
		)
		c = newCluster(t, d, func(i int, o *Options) {
			o.Transport = &goneFetches{base: http.DefaultTransport.(*http.Transport).Clone(), gone: &gone} //nolint:forcetypeassert,errcheck // the default transport is an *http.Transport
			o.hooks.pinned = func(id store.ShardID, _ string) {
				if tn := stopping.Load(); tn != nil && tn.i == i {
					holdPinned(tn.n, id, &crossed)
				}
			}
		})
		a := c.start(0)
		c.start(1)
		c.start(2)
		createIndex(t, a.n, "roll", 3, 2)
		waitCopies(t, a.st, "roll", 3, 2, 30*time.Second)
		var acked atomic.Int64
		for k := range 30 {
			mustWrite(t, a.n, "roll", upsertOp(fmt.Sprintf("seed%d", k), k))
		}
		acked.Store(30)
		var heldAll int64
		for i := range 3 {
			var others []*tnode
			for _, tn := range c.live() {
				if tn.i != i {
					others = append(others, tn)
				}
			}
			r := startReaders(others, "roll", 2, nil)
			// A writer on another node keeps writing through the restart.
			stopW := make(chan struct{})
			var wg sync.WaitGroup
			var werr atomic.Pointer[error]
			var slowestWrite atomic.Int64
			wg.Go(func() {
				for k := 0; ; k++ {
					select {
					case <-stopW:
						return
					default:
					}
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					began := time.Now()
					res, err := others[0].n.Write(ctx, "roll", []api.WriteOp{upsertOp(fmt.Sprintf("r%d-%d", i, k), k)}, api.WriteOptions{})
					cancel()
					for d := int64(time.Since(began)); ; {
						cur := slowestWrite.Load()
						if d <= cur || slowestWrite.CompareAndSwap(cur, d) {
							break
						}
					}
					if err == nil && res.Items[0].Err != nil {
						err = res.Items[0].Err
					}
					if err != nil {
						werr.CompareAndSwap(nil, &err)
						return
					}
					acked.Add(1)
					time.Sleep(20 * time.Millisecond)
				}
			})
			slowRetire := func() { time.Sleep(50 * time.Millisecond) }
			c.node(i).wrap.onRetire.Store(&slowRetire)
			stopping.Store(c.node(i))
			time.Sleep(200 * time.Millisecond)
			c.node(i).stop()
			stopping.Store(nil)
			time.Sleep(500 * time.Millisecond)
			c.start(i)
			waitCopies(t, others[0].st, "roll", 3, 2, 60*time.Second)
			close(stopW)
			wg.Wait()
			r.finish(t)
			if e := werr.Load(); e != nil {
				t.Fatalf("a write failed during the restart of node %d: %v", i, *e)
			}
			tails := map[string]time.Duration{}
			for _, tn := range c.live() {
				for op, d := range tn.wrap.tails() {
					tails[op] = max(tails[op], d)
				}
			}
			held := crossed.Swap(0)
			heldAll += held
			t.Logf("restart of node %d: slowest write %s; slowest store calls %v; %d searches fetched from a copy that stopped serving after their query phase",
				i, time.Duration(slowestWrite.Load()).Round(time.Millisecond), roundAll(tails), held)
		}
		if n := gone.Load(); n > 0 {
			t.Fatalf("%d fetches found their pinned generation gone", n)
		}
		if heldAll == 0 {
			t.Fatal("no search fetched from a copy that stopped serving after its query phase: the restarts did not exercise the fetch path")
		}
		st := c.node(0).st
		head, _, err := st.HeadSeq(tctx(t))
		if err != nil {
			t.Fatal(err)
		}
		for _, tn := range c.live() {
			waitCount(t, tn.n, "roll", head, acked.Load())
		}
	})
}

// holdPinned holds a query phase's reply on n until n's copy of id stops serving (it is
// paused to retire, or closed), so the search fetches after it did, counting the
// replies it held that long in crossed; it gives up after 2 s.
func holdPinned(n *Node, id store.ShardID, crossed *atomic.Int64) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, hosted := n.Hosted(id); !hosted || n.Paused(id) {
			crossed.Add(1)
			return
		}
		time.Sleep(time.Millisecond)
	}
}

// goneFetches counts the fetches a peer answered 410: their pinned generation was gone.
type goneFetches struct {
	base http.RoundTripper
	gone *atomic.Int64
}

func (g *goneFetches) RoundTrip(r *http.Request) (*http.Response, error) {
	resp, err := g.base.RoundTrip(r)
	if err == nil && resp.StatusCode == http.StatusGone && r.URL.Path == peerPrefix+"fetch" {
		g.gone.Add(1)
	}
	return resp, err
}

// roundAll rounds durations to the millisecond, for logs.
func roundAll(m map[string]time.Duration) map[string]time.Duration {
	for k, d := range m {
		m[k] = d.Round(time.Millisecond)
	}
	return m
}

// TestPartitionedNodeStopsServingBeforeSteal (split-brain safety): a node cut off from
// the database cannot renew its leases. Its copy stops serving peers at its local lease
// deadline (by its own clocks, less the margin), which comes before the database's
// lease_until, so before any other node can claim the slot; and the thief's copy is
// quarantined besides. The cut-off node keeps its copy open, paused, for its own
// last-resort reads (stale) until the loss is confirmed: once healed it finds the slot
// taken, drops the copy and reads through the new one.
//
// The p2 variant (probe P2) also steps the database's clock forward: the lease expires
// in the database at once, so another node steals the slot while the cut-off node's
// local deadline is still ahead. The thief's quarantine (TTL plus margin after its
// claim, by its own clock) keeps the two from serving at once all the same.
func TestPartitionedNodeStopsServingBeforeSteal(t *testing.T) {
	testtier.Heavy(t)
	for _, stepClock := range []bool{false, true} {
		name := "partition"
		if stepClock {
			name = "p2-db-clock-step"
		}
		t.Run(name, func(t *testing.T) {
			forEachDialect(t, func(t *testing.T, d *db) {
				if stepClock && d.dialect == "mysql" {
					t.Skip("the clock step is simulated on SQLite and Postgres")
				}
				testPartition(t, d, stepClock)
			})
		})
	}
}

func testPartition(t *testing.T, d *db, stepClock bool) {
	c := newCluster(t, d, nil)
	a := c.start(0)
	createIndex(t, a.n, "sb", 1, 1)
	waitCopies(t, a.st, "sb", 1, 1, 30*time.Second)
	mustWrite(t, a.n, "sb", upsertOp("x", 1))
	b := c.start(1)
	c.waitHealth(t, api.StatusGreen, 30*time.Second)
	id := store.ShardID{Index: "sb", Shard: 0}
	if _, nodes := serving(t, b.st, id); !slices.Equal(nodes, []string{a.n.id}) {
		t.Fatalf("the copy is on %v, want node-0 alone", nodes)
	}
	if a.n.leaseFor(id) == nil {
		t.Fatal("node-0 holds no lease")
	}
	// peerServes reports whether tn's copy serves peers now: what a peer's read of it
	// gets, through the peer API's own admission (peerAPI.target), stale allowed. A read
	// it serves is held to the serving invariant by the served hook, at the readings the
	// copy was judged at: readings taken before the copy's checks would date a read
	// admitted at the quarantine's end to before it.
	peerServes := func(tn *tnode) bool {
		req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/", nil)
		tg, err := tn.n.peer.target(req, shardRef{Index: "sb", Shard: 0, AllowStale: true})
		if err != nil {
			return false
		}
		tg.Release()
		return true
	}

	var stoppedAt, startedAt atomic.Int64 // Unix nanoseconds
	start := time.Now()
	a.wrap.cut(true) // the partition
	if stepClock {
		expireLeases(t, d, "sb") // the database's clock jumps past lease_until
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	watch := func(tn *tnode, at *atomic.Int64, want bool) {
		wg.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				if peerServes(tn) == want {
					at.CompareAndSwap(0, time.Now().UnixNano())
					return
				}
				time.Sleep(time.Millisecond)
			}
		})
	}
	watch(a, &stoppedAt, false) // when does node-0 stop serving peers?
	watch(b, &startedAt, true)  // when does node-1's copy start?
	eventually(t, 60*time.Second, "node-1 takes the copy over and serves it", func() error {
		if startedAt.Load() == 0 || stoppedAt.Load() == 0 {
			return fmt.Errorf("stopped %d, started %d", stoppedAt.Load(), startedAt.Load())
		}
		return nil
	})
	close(done)
	wg.Wait()
	stop, begin := time.Unix(0, stoppedAt.Load()), time.Unix(0, startedAt.Load())
	t.Logf("partitioned; node-0 stopped serving peers after %s, node-1 began serving after %s", stop.Sub(start), begin.Sub(start))
	if !stop.Before(begin) {
		t.Fatalf("node-0 served peers until %s, after node-1 began serving the copy at %s", stop.Sub(start), begin.Sub(start))
	}
	if limit := a.n.opts.LeaseTTL + 500*time.Millisecond; stop.Sub(start) > limit {
		t.Fatalf("node-0 stopped serving peers %s after the partition, beyond its lease (%s)", stop.Sub(start), a.n.opts.LeaseTTL)
	}
	// Still cut off, node-0 cannot confirm the loss: it keeps its copy paused, for its
	// own reads only, stale. The watchdog pauses it on its next tick after the lapse.
	eventually(t, 10*time.Second, "node-0 pauses its lapsed copy", func() error {
		if lc := a.n.LocalCopies(); len(lc) != 1 || !lc[0].Paused || lc[0].Serving {
			return fmt.Errorf("node-0's copy while cut off: %+v", lc)
		}
		return nil
	})
	res, err := a.n.Search(tctx(t), "sb", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{})
	if err != nil || !res.Stale {
		t.Fatalf("node-0's last-resort read while cut off: %+v %v", res, err)
	}
	// Healed, node-0 finds the slot taken, drops its copy and reads through node-1's.
	a.wrap.cut(false)
	eventually(t, 60*time.Second, "node-0 drops its copy and reads through node-1", func() error {
		if lc := a.n.LocalCopies(); len(lc) != 0 {
			return fmt.Errorf("node-0 still hosts %+v", lc)
		}
		res, err := a.n.Search(context.Background(), "sb", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{})
		if err != nil {
			return err
		}
		if res.Total != 1 || res.Stale {
			return fmt.Errorf("total %d, stale %v", res.Total, res.Stale)
		}
		return nil
	})
}

// expireLeases makes the database consider every lease of index expired at once, as a
// forward step of its clock would.
func expireLeases(t testing.TB, d *db, index string) {
	t.Helper()
	switch d.dialect {
	case "sqlite":
		sqliteExec(t, d, "UPDATE sl_shard_copies SET lease_until = 0 WHERE index_name = ?", index)
	case "postgres":
		u, err := url.Parse(d.url)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := postgres.Config(u)
		if err != nil {
			t.Fatal(err)
		}
		db := stdlib.OpenDB(*cfg)
		defer db.Close()
		if _, err := db.ExecContext(context.Background(), "UPDATE sl_shard_copies SET lease_until = 0 WHERE index_name = $1", index); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("no clock step on %s", d.dialect)
	}
}

// sqliteExec runs a statement on d, a SQLite database, outside every node's store. A
// node's own write connections wait out a held lock (store/sqlite.DefaultBusyTimeoutMS);
// this ad hoc one needs the same busy_timeout, or a node's own writer (a heartbeat, a
// checkpoint) holding the lock when this runs fails it with SQLITE_BUSY at once.
func sqliteExec(t testing.TB, d *db, q string, args ...any) {
	t.Helper()
	u, err := url.Parse(d.url)
	if err != nil {
		t.Fatal(err)
	}
	dsn := strings.TrimPrefix(u.Path, "/")
	if _, err := os.Stat(dsn); err != nil {
		dsn = u.Path
	}
	db, err := sql.Open("sqlite", dsn+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), q, args...); err != nil {
		t.Fatal(err)
	}
}

// TestLeaseDeadlineByFakeClock: a lease surely holds until TTL less the margin after
// the clocks' readings taken before its last grant, by the monotonic clock and by the
// wall clock both (a suspended machine's monotonic clock may stand still); a slot taken
// over from another node is quarantined for TTL plus margin after the claim began.
func TestLeaseDeadlineByFakeClock(t *testing.T) {
	fake := clock.NewFake(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC))
	clk := leaseClock{c: fake, epoch: fake.Now()}
	set := func(d time.Duration) { fake.Advance(d - clk.Now()) }
	l := &lease{ttl: time.Second, margin: 100 * time.Millisecond, clock: clk}
	l.extend(0, clk.Wall())
	if !l.valid() {
		t.Fatal("a fresh lease is not valid")
	}
	set(899 * time.Millisecond)
	if !l.valid() {
		t.Fatal("not valid before the margin")
	}
	set(900 * time.Millisecond)
	if l.valid() {
		t.Fatal("valid within the margin of its deadline")
	}
	l.extend(500*time.Millisecond, clk.Wall().Add(-400*time.Millisecond)) // renewed: from the readings before the call
	if !l.valid() {
		t.Fatal("not valid after a renewal")
	}
	l.extend(100*time.Millisecond, clk.Wall().Add(-800*time.Millisecond)) // a late answer to an older renewal never moves it back
	if time.Duration(l.deadline.Load()) != 1500*time.Millisecond {
		t.Fatalf("deadline %s", time.Duration(l.deadline.Load()))
	}
	set(1400 * time.Millisecond)
	if l.valid() {
		t.Fatal("valid past deadline less margin")
	}

	// Suspend: the wall clock runs on while the monotonic one stands still.
	l.extend(clk.Now(), clk.Wall())
	fake.StepWall(5 * time.Second)
	if l.valid() {
		t.Fatal("valid after the machine slept past the lease, by its monotonic clock alone")
	}

	// Quarantine: a stolen slot serves nothing for TTL plus margin after its claim.
	n := &Node{
		opts: Options{LeaseTTL: time.Second, LeaseMargin: 100 * time.Millisecond}, clock: fake, lc: clk, id: "me",
		quarantines: map[store.ShardID]quarantine{},
	}
	from := clk.Now()
	q := n.newLease(store.Copy{TakenFrom: "other"}, from, clk.Wall(), from)
	if !q.quarantined() || !q.valid() {
		t.Fatalf("a stolen slot: quarantined %v, valid %v", q.quarantined(), q.valid())
	}
	set(from + 1099*time.Millisecond)
	if !q.quarantined() {
		t.Fatal("the quarantine ended before TTL plus margin")
	}
	if back := n.newLease(store.Copy{}, clk.Now(), clk.Wall(), clk.Now()); back.quarantine != q.quarantine {
		t.Fatalf("the slot claimed back at the same epoch: quarantined until %s, want the takeover's %s", back.quarantine, q.quarantine)
	}
	set(from + 1100*time.Millisecond)
	if q.quarantined() {
		t.Fatal("still quarantined after TTL plus margin")
	}
	if mine := n.newLease(store.Copy{}, clk.Now(), clk.Wall(), clk.Now()); mine.quarantined() {
		t.Fatal("a free slot is quarantined")
	}
	n.newLease(store.Copy{TakenFrom: "other", Epoch: 2}, clk.Now(), clk.Wall(), clk.Now())
	set(clk.Now() + 1100*time.Millisecond)
	n.newLease(store.Copy{Shard: store.ShardID{Index: "elsewhere"}}, clk.Now(), clk.Wall(), clk.Now())
	if len(n.quarantines) != 0 {
		t.Fatalf("ended quarantines are kept: %+v", n.quarantines)
	}
	q = n.newLease(store.Copy{TakenFrom: "other", Epoch: 3}, clk.Now(), clk.Wall(), clk.Now())
	if other := n.newLease(store.Copy{Epoch: 4}, clk.Now(), clk.Wall(), clk.Now()); !q.quarantined() || other.quarantined() {
		t.Fatalf("a claim at another epoch than the takeover's: quarantined %v", other.quarantined())
	}
}

// takeoverCluster starts node-0, holding the one copy of sb/0, and node-1 on a fake
// clock, then cuts node-0 off and expires its lease in the database. Node-1's loops
// stand still: the test runs its view reads and claims. mod, when set, adjusts node-1's
// options last.
func takeoverCluster(t *testing.T, mod func(o *Options)) (*db, *clock.Fake, *tnode) {
	t.Helper()
	d := sqliteDB(t)
	clk := clock.NewFake(time.Now())
	c := newCluster(t, d, func(i int, o *Options) {
		if i == 1 {
			o.Clock = clk
			o.HeartbeatInterval = time.Hour
			o.LeaseTTL = 2 * time.Hour
			o.DeadAfter = 3 * time.Hour
			o.ViewInterval = time.Hour
			o.CatalogInterval = time.Hour
			o.PruneInterval = time.Hour
			if mod != nil {
				mod(o)
			}
		}
	})
	a := c.start(0)
	createIndex(t, a.n, "sb", 1, 1)
	waitCopies(t, a.st, "sb", 1, 1, 30*time.Second)
	b := c.start(1)
	a.wrap.cut(true)
	expireLeases(t, d, "sb")
	return d, clk, b
}

// allocateNow runs one allocation pass of sb on tn over a fresh view.
func allocateNow(t *testing.T, tn *tnode) error {
	t.Helper()
	ctx := tctx(t)
	if err := tn.n.refreshView(ctx); err != nil {
		t.Fatal(err)
	}
	return tn.n.allocatePass(ctx, "sb", false, true)
}

// stealSB has node-1 take sb/0 over from node-0, quarantined.
func stealSB(t *testing.T, b *tnode) *lease {
	t.Helper()
	if err := allocateNow(t, b); err != nil {
		t.Fatal(err)
	}
	l := b.n.leaseFor(store.ShardID{Index: "sb", Shard: 0})
	if l == nil || l.copy.TakenFrom != "node-0" || !l.quarantined() {
		t.Fatalf("node-1's claim of node-0's expired slot: %+v", l)
	}
	return l
}

// TestTakeoverQuarantineSurvivesAStaleViewAndAClaimBack (#19): a node that took a slot
// over stays quarantined, TTL plus margin after its claim, whatever becomes of its lease
// meanwhile. A registry view whose read began while the claim was under way, before it
// committed, still shows the previous holder: it does not count as a takeover of the new
// copy. And a copy dropped locally and claimed back at the same epoch (the store then
// names no previous holder) keeps the takeover's quarantine. The claim hook reads a view
// while the claim is under way.
func TestTakeoverQuarantineSurvivesAStaleViewAndAClaimBack(t *testing.T) {
	_, clk, b := takeoverCluster(t, nil)
	id := store.ShardID{Index: "sb", Shard: 0}
	ctx := tctx(t)
	duringClaim := func() {
		clk.Advance(time.Millisecond)
		if err := b.n.refreshView(ctx); err != nil {
			t.Error(err)
		}
		clk.Advance(time.Millisecond)
	}
	b.wrap.onClaim.Store(&duringClaim)
	stolen := stealSB(t, b)
	b.wrap.onClaim.Store(nil)
	v := b.n.view.Load()
	if seen := v.copies[id]; len(seen) != 1 || seen[0].NodeID != "node-0" {
		t.Fatalf("the view read during the claim shows %+v, want node-0's copy", seen)
	}
	if v.takenOver(stolen) {
		t.Fatal("a view read while the claim was under way counts as a takeover of the new copy")
	}
	later := &view{at: clk.Now(), readBegan: b.n.lc.Now() + 1, copies: map[store.ShardID][]store.Copy{
		id: {{Shard: id, Slot: stolen.copy.Slot, NodeID: "node-2", Epoch: stolen.copy.Epoch + 1}},
	}}
	if !later.takenOver(stolen) {
		t.Fatal("a view read after the claim that shows another holder is not a takeover")
	}

	b.n.loseCopy(ctx, stolen)
	if err := allocateNow(t, b); err != nil {
		t.Fatal(err)
	}
	back := b.n.leaseFor(id)
	if back == nil || back == stolen || back.copy.Epoch != stolen.copy.Epoch || back.copy.TakenFrom != "" {
		t.Fatalf("node-1's claim back of its slot: %+v", back)
	}
	if back.quarantine != stolen.quarantine || !back.quarantined() {
		t.Fatalf("claimed back at the same epoch, the copy is quarantined until %s, want the takeover's %s", back.quarantine, stolen.quarantine)
	}
	if _, err := b.n.LocalTarget(ctx, "sb", 0, 0); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("a peer read of the copy claimed back in its quarantine: %v, want the quarantine's refusal", err)
	}
}

// TestPeerReadJudgedAtQuarantineEnd: a stolen copy refuses a peer's read one
// nanosecond before its quarantine ends and serves it from the end on, judged by one
// leaseClock reading taken once the target is held, after every check the copy itself
// made; the served hook reports exactly that reading. Inside LocalTarget (as the copy's
// read is admitted) the fake clock moves 5 ms on, as a busy runner's clock would
// between two readings: the read must be reported at the later reading, the one it was
// judged at, never one taken before the checks.
func TestPeerReadJudgedAtQuarantineEnd(t *testing.T) {
	var (
		inside   atomic.Pointer[func()]
		reported atomic.Int64
		calls    atomic.Int32
	)
	_, clk, b := takeoverCluster(t, func(o *Options) {
		served, servedLocal := o.hooks.served, o.hooks.servedLocal
		o.hooks.served = func(id store.ShardID, began time.Duration, wall time.Time) {
			calls.Add(1)
			reported.Store(int64(began))
			served(id, began, wall)
		}
		o.hooks.servedLocal = func(id store.ShardID, l *lease) {
			if fn := inside.Load(); fn != nil {
				(*fn)()
			}
			servedLocal(id, l)
		}
	})
	id := store.ShardID{Index: "sb", Shard: 0}
	stolen := stealSB(t, b)
	q := stolen.quarantine
	// The copy recovers first (its quarantine aside): the boundary is about the
	// quarantine alone. Its tailer's timers run on the fake clock.
	recovered := func() bool {
		for _, lc := range b.n.LocalCopies() {
			if lc.Info.Index == "sb" && lc.Info.Shard == 0 {
				return lc.Info.State == api.ShardServing
			}
		}
		return false
	}
	for deadline := time.Now().Add(20 * time.Second); !recovered(); {
		if time.Now().After(deadline) {
			t.Fatal("node-1's copy of sb/0 did not recover")
		}
		stolen.extend(b.n.lc.Now(), b.n.lc.Wall())
		clk.Advance(10 * time.Millisecond)
		time.Sleep(5 * time.Millisecond)
	}
	// Node-0, cut off from the database, stops serving peers once its own lease lapses
	// by its real clock: well before the quarantine ends, which node-1's fake clock
	// reaches at once.
	for deadline := time.Now().Add(30 * time.Second); b.c.node(0).n.peerValid(id); {
		if time.Now().After(deadline) {
			t.Fatal("node-0, cut off, still serves peers")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Renew as the clock moves (from the readings before each step), so only the
	// quarantine, never the lease, decides the read.
	for now := b.n.lc.Now(); now < q-time.Nanosecond; now = b.n.lc.Now() {
		stolen.extend(now, b.n.lc.Wall())
		clk.Advance(min(30*time.Minute, q-time.Nanosecond-now))
	}
	stolen.extend(b.n.lc.Now(), b.n.lc.Wall())
	if b.n.leaseFor(id) != stolen {
		t.Fatal("node-1's lease of sb/0 changed while the clock moved")
	}
	ref := shardRef{Index: "sb", Shard: 0, AllowStale: true}
	read := func() (node.ShardTarget, error) {
		return b.n.peer.target(httptest.NewRequestWithContext(tctx(t), http.MethodPost, "/", nil), ref)
	}

	if now := b.n.lc.Now(); now != q-time.Nanosecond {
		t.Fatalf("the clock reads %s, want %s", now, q-time.Nanosecond)
	}
	if _, err := read(); !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("a peer read 1ns before the quarantine ends: %v, want the quarantine's refusal", err)
	}
	if n := calls.Load(); n != 0 {
		t.Fatalf("a refused read was reported served %d times", n)
	}

	clk.Advance(time.Nanosecond)
	step := func() { clk.Advance(5 * time.Millisecond) }
	inside.Store(&step)
	tg, err := read()
	inside.Store(nil)
	if err != nil {
		t.Fatalf("a peer read at the quarantine's end: %v", err)
	}
	tg.Release()
	if n := calls.Load(); n != 1 {
		t.Fatalf("the read was reported served %d times, want once", n)
	}
	if got := time.Duration(reported.Load()); got != q+5*time.Millisecond {
		t.Fatalf("the read was reported at %s, want %s: the reading it was judged at, after the copy's own checks (quarantine ends %s)", got, q+5*time.Millisecond, q)
	}
}

// TestReclaimTakeoverIsQuarantined: a renewal that finds its lease expired claims the
// slot again; when another node held it meanwhile (and lapsed), that claim is a takeover
// at a new epoch. The copy is dropped, and the allocator's claim back of the slot at
// that epoch (the store names no previous holder) is quarantined all the same.
func TestReclaimTakeoverIsQuarantined(t *testing.T) {
	d, _, b := takeoverCluster(t, nil)
	id := store.ShardID{Index: "sb", Shard: 0}
	ctx := tctx(t)
	held := stealSB(t, b)
	sqliteExec(t, d, "UPDATE sl_shard_copies SET node_id = 'node-2', epoch = epoch + 100, lease_until = 0 WHERE index_name = 'sb'")

	b.n.reclaim(ctx, held)
	if l := b.n.leaseFor(id); l != nil {
		t.Fatalf("the lease reclaimed at a new epoch was kept: %+v", l)
	}
	if err := allocateNow(t, b); err != nil {
		t.Fatal(err)
	}
	back := b.n.leaseFor(id)
	if back == nil || back.copy.Epoch == held.copy.Epoch || back.copy.TakenFrom != "" {
		t.Fatalf("node-1's claim back of the slot it took from node-2: %+v", back)
	}
	if !back.quarantined() {
		t.Fatalf("the slot node-1 took from node-2 serves at once: epoch %d, quarantine %s", back.copy.Epoch, back.quarantine)
	}
}

// TestQuarantinedTakeoverKeepsItsRow: a takeover is not released while quarantined, so
// the slot's next claim cannot get a fresh epoch with no quarantine. A copy that cannot
// be hosted keeps its row, and the claim back at that epoch stays quarantined; a lowered
// target does not release a quarantined copy either.
func TestQuarantinedTakeoverKeepsItsRow(t *testing.T) {
	_, clk, b := takeoverCluster(t, nil)
	id := store.ShardID{Index: "sb", Shard: 0}
	ctx := tctx(t)
	meta, err := b.st.Indexes().Get(ctx, "sb")
	if err != nil {
		t.Fatal(err)
	}
	// A file where the index's directory belongs: the copy can be neither opened nor
	// wiped and created afresh.
	blocker := filepath.Join(b.cfg.DataDir, "indexes", meta.UID)
	if err := os.RemoveAll(blocker); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(blocker), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blocker, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := allocateNow(t, b); err == nil {
		t.Fatal("hosting a copy over a broken directory succeeded")
	}
	if l := b.n.leaseFor(id); l != nil {
		t.Fatalf("a copy that could not be hosted kept its lease: %+v", l)
	}
	rows, err := b.st.Registry().Copies(ctx, "sb")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].NodeID != "node-1" {
		t.Fatalf("the quarantined takeover's row: %+v, want node-1's", rows)
	}
	if err := os.Remove(blocker); err != nil {
		t.Fatal(err)
	}
	if err := allocateNow(t, b); err != nil {
		t.Fatal(err)
	}
	back := b.n.leaseFor(id)
	if back == nil || back.copy.Epoch != rows[0].Epoch || !back.quarantined() {
		t.Fatalf("the claim back of the takeover's row: %+v", back)
	}

	extra := b.n.newLease(store.Copy{Shard: store.ShardID{Index: "sb", Shard: 7}, Slot: 1, Epoch: 1 << 40, TakenFrom: "node-0"},
		b.n.lc.Now(), b.n.lc.Wall(), b.n.lc.Now())
	v := &view{at: clk.Now(), live: map[string]bool{"node-0": true}, copies: map[store.ShardID][]store.Copy{
		extra.copy.Shard: {{Shard: extra.copy.Shard, Slot: 0, NodeID: "node-0", State: store.CopyServing, LeaseLeft: time.Hour}},
	}}
	if err := b.n.maybeRelease(ctx, v, node.IndexView{Name: "sb", ReplicasPerShard: 1}, extra); err != nil || extra.lost {
		t.Fatalf("a lowered target released a quarantined takeover: lost %v, %v", extra.lost, err)
	}
}

// TestReadRightAfterCreateOnAnotherNode: an index created on one node is read on
// another at once, before that node's routing view or catalogue would next sync on
// their own. The reader finds the index in the store and its copies in the registry,
// and answers; a shard no node serves gets ErrNoServingCopy's 503, not a closed store.
func TestReadRightAfterCreateOnAnotherNode(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
		c := newCluster(t, d, func(i int, o *Options) {
			if i == 1 {
				o.HeartbeatInterval = 1500 * time.Millisecond
				o.ViewInterval = time.Hour
				o.CatalogInterval = time.Hour
				o.PruneInterval = time.Hour
			}
		})
		a, b := c.start(0), c.start(1)
		for k := range 5 {
			name := fmt.Sprintf("fresh%d", k)
			createIndex(t, a.n, name, 2, 0)
			if got, err := count(tctx(t), b.n, name, 0); err != nil || got != 0 {
				t.Fatalf("read of %s on the other node right after it was created: %d, %v", name, got, err)
			}
			seq := mustWrite(t, a.n, name, upsertOp("x", 1))
			got, err := count(tctx(t), b.n, name, seq)
			if err != nil || got != 1 {
				t.Fatalf("read of %s on the other node right after a write: %d, %v", name, got, err)
			}
		}
		_, err := (&clusterHooks{b.n}).Remote(tctx(t), store.ShardID{Index: "nowhere", Shard: 0}, 0)
		var ae *api.Error
		if !errors.As(err, &ae) || ae.Status != http.StatusServiceUnavailable || !errors.Is(err, ErrNoServingCopy) || errors.Is(err, store.ErrClosed) {
			t.Fatalf("a shard no node serves: %v, want ErrNoServingCopy's 503", err)
		}
	})
}

// TestNoCopyReadsBoundRegistryReads: reads of a shard no node serves, made at once by
// several clients, re-read the registry at most once per missRefreshEvery, by the
// node's clock: a burst of them shares one read, and a burst less than an interval
// after the last read waits out the rest of the interval before reading again.
func TestNoCopyReadsBoundRegistryReads(t *testing.T) {
	clk := clock.NewFake(time.Now())
	c := newCluster(t, sqliteDB(t), func(_ int, o *Options) {
		o.Clock = clk
		o.HeartbeatInterval = time.Hour
		o.LeaseTTL = 2 * time.Hour
		o.DeadAfter = 3 * time.Hour
		o.ViewInterval = time.Hour
		o.CatalogInterval = time.Hour
		o.PruneInterval = time.Hour
	})
	a := c.start(0)
	hooks := &clusterHooks{a.n}
	id := store.ShardID{Index: "nowhere", Shard: 0}
	burst := func() <-chan struct{} {
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() {
				for range 5 {
					if _, err := hooks.Remote(tctx(t), id, 0); !errors.Is(err, ErrNoServingCopy) {
						t.Errorf("a shard no node serves: %v", err)
						return
					}
				}
			})
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		return done
	}
	reads := func() int64 { return a.wrap.nodesReads.Load() }
	before := reads()
	for k := range int64(3) {
		clk.Advance(missRefreshEvery)
		<-burst()
		if got := reads() - before; got != k+1 {
			t.Fatalf("%d registry reads after %d bursts an interval apart, want one each", got, k+1)
		}
	}

	clk.Advance(missRefreshEvery / 3)
	done := burst()
	if err := clk.BlockUntilArmed(tctx(t), missRefreshEvery-missRefreshEvery/3); err != nil {
		t.Fatalf("a burst within the interval is not waiting out its rest: %v", err)
	}
	if got := reads() - before; got != 3 {
		t.Fatalf("%d registry reads: a burst within the interval read at once", got)
	}
	clk.Advance(missRefreshEvery - missRefreshEvery/3)
	<-done
	if got := reads() - before; got != 4 {
		t.Fatalf("%d registry reads once the interval ran out, want 4", got)
	}
}

// TestUnknownIndexLookupsAreCached: requests for an index no node has ask the store
// about it once per absentTTL, not once each. The node's clock is fake and stands
// still: every request falls within one absentTTL.
func TestUnknownIndexLookupsAreCached(t *testing.T) {
	clk := clock.NewFake(time.Now())
	c := newCluster(t, sqliteDB(t), func(_ int, o *Options) { o.Clock = clk; o.CatalogInterval = time.Hour })
	a := c.start(0)
	before := a.wrap.indexGets.Load()
	for range 50 {
		var ae *api.Error
		if _, err := count(tctx(t), a.n, "ghost", 0); !errors.As(err, &ae) || ae.Status != http.StatusNotFound {
			t.Fatalf("an index no node has: %v, want a 404", err)
		}
	}
	if gets := a.wrap.indexGets.Load() - before; gets != 1 {
		t.Fatalf("%d store lookups for 50 requests of an unknown index, want 1", gets)
	}
}

// TestCreateForgetsARememberedAbsence: a node that answered 404 for a name forgets
// that answer when it creates the index itself, so once it drops the index and
// another node creates it again, a read finds it although the 404 is younger than
// absentTTL by the node's clock. That clock is fake: it moves only by the one
// missRefreshEvery the read needs to see B's copy in a fresh view. Node A is at
// capacity, so its create and drop host no copy and wait on nothing; B serves.
func TestCreateForgetsARememberedAbsence(t *testing.T) {
	clk := clock.NewFake(time.Now())
	c := newCluster(t, sqliteDB(t), func(i int, o *Options) {
		o.CatalogInterval = time.Hour
		if i == 0 {
			o.Clock = clk
			o.Capacity = 1
			o.HeartbeatInterval = time.Hour
			o.LeaseTTL = 2 * time.Hour
			o.DeadAfter = 3 * time.Hour
			o.ViewInterval = time.Hour
			o.PruneInterval = time.Hour
		}
	})
	b := c.start(1)
	createIndex(t, b.n, "seed", 1, 2)
	a := c.start(0)
	if a.n.leaseCount() != 1 {
		t.Fatalf("node A holds %d copies, want its one of seed", a.n.leaseCount())
	}
	notFound := func(err error) bool {
		var ae *api.Error
		return errors.As(err, &ae) && ae.Status == http.StatusNotFound
	}
	if _, err := count(tctx(t), a.n, "ghost", 0); !notFound(err) {
		t.Fatalf("an index no node has: %v, want a 404", err)
	}
	createIndex(t, a.n, "ghost", 1, 1)
	if err := a.n.DeleteIndex(tctx(t), "ghost"); err != nil {
		t.Fatal(err)
	}
	createIndex(t, b.n, "ghost", 1, 1)
	clk.Advance(missRefreshEvery)
	if got, err := count(tctx(t), a.n, "ghost", 0); err != nil || got != 0 {
		t.Fatalf("a read on A of the index B created after A dropped its own: %d, %v", got, err)
	}
}

// TestShortShutdownGraceWarns: a cluster node whose shutdown_grace is shorter than its
// routing view interval warns at startup.
func TestShortShutdownGraceWarns(t *testing.T) {
	for _, grace := range []time.Duration{0, time.Second} {
		logs := &warnings{}
		c := newCluster(t, sqliteDB(t), func(_ int, o *Options) {
			o.Config.ShutdownGrace = grace
			o.ViewInterval = 500 * time.Millisecond
			o.Logger = slog.New(logs)
		})
		c.start(0)
		if got, want := logs.has("shutdown_grace is shorter than the routing view interval"), grace == 0; got != want {
			t.Fatalf("shutdown_grace %s: warned %v, want %v", grace, got, want)
		}
	}
}

type warnings struct {
	mu   sync.Mutex
	msgs []string
}

func (h *warnings) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelWarn }

func (h *warnings) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.msgs = append(h.msgs, r.Message)
	return nil
}

func (h *warnings) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *warnings) WithGroup(string) slog.Handler      { return h }

func (h *warnings) has(prefix string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.ContainsFunc(h.msgs, func(m string) bool { return strings.HasPrefix(m, prefix) })
}

// TestEnsureIndexAcrossNodes: a client behind a load balancer checks for an index on
// node B (404), creates it through node A, then writes and reads through B. Plain reads
// on B may still answer 404 for a moment, but the write, a mapping change and a read
// that waits for the write all find the index.
func TestEnsureIndexAcrossNodes(t *testing.T) {
	c := newCluster(t, sqliteDB(t), func(_ int, o *Options) { o.CatalogInterval = time.Hour })
	a, b := c.start(0), c.start(1)
	notFound := func(err error) bool {
		var ae *api.Error
		return errors.As(err, &ae) && ae.Status == http.StatusNotFound
	}
	cachedOnce := false
	for k := 0; k < 10 && !cachedOnce; k++ {
		name := fmt.Sprintf("ensure%d", k)
		if _, err := b.n.GetIndex(tctx(t), name); !notFound(err) {
			t.Fatalf("the check on B before the create: %v, want a 404", err)
		}
		createIndex(t, a.n, name, 2, 0)
		_, err := b.n.GetIndex(tctx(t), name)
		cachedOnce = notFound(err)
		seq := mustWrite(t, b.n, name, upsertOp("x", 1))
		if _, err := b.n.PatchMapping(tctx(t), name, map[string]schema.FieldType{"extra": schema.Keyword}); err != nil {
			t.Fatalf("a mapping change on B right after the create on A: %v", err)
		}
		if got, err := count(tctx(t), b.n, name, seq); err != nil || got != 1 {
			t.Fatalf("a read on B waiting for the write: %d, %v", got, err)
		}
	}
	if !cachedOnce {
		t.Skip("every create on A outlasted the 404 B remembered; the sequence was not exercised")
	}
}

// TestUnclaimedShardIsLeftToItsCreator: a shard no node has claimed (an index the
// creating node has not allocated yet) is left alone by the other nodes for three
// heartbeats, so the creator's eager claim wins it; after that another node takes it.
func TestUnclaimedShardIsLeftToItsCreator(t *testing.T) {
	c := newCluster(t, sqliteDB(t), nil)
	a := c.start(0)
	c.start(1)
	settings, err := json.Marshal(api.IndexSettings{Shards: 1, ReplicasPerShard: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.st.Indexes().Create(tctx(t), store.IndexMeta{Name: "orphan", Mapping: []byte(`{}`), Settings: settings}); err != nil {
		t.Fatal(err)
	}
	created := time.Now()
	id := store.ShardID{Index: "orphan", Shard: 0}
	heartbeat := 200 * time.Millisecond
	for time.Since(created) < 2*heartbeat {
		if copies, err := a.st.Registry().Copies(tctx(t), "orphan"); err != nil || len(copies) > 0 {
			t.Fatalf("a shard no node created was claimed %s after the index appeared: %+v, %v", time.Since(created), copies, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	eventually(t, 30*time.Second, "another node claims the shard", func() error {
		if n, nodes := serving(t, a.st, id); n != 1 {
			return fmt.Errorf("%d serving on %v", n, nodes)
		}
		return nil
	})
}
