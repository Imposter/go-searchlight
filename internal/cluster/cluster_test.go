package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/store/postgres"
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
// acknowledged write stays searchable.
func TestRollingRestartNoClientErrors(t *testing.T) {
	forSQLiteAndPostgres(t, func(t *testing.T, d *db) {
		c := newCluster(t, d, nil)
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
			time.Sleep(200 * time.Millisecond)
			c.node(i).stop()
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
			t.Logf("restart of node %d: slowest write %s; slowest store calls %v", i, time.Duration(slowestWrite.Load()).Round(time.Millisecond), roundAll(tails))
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
	// gets.
	peerServes := func(tn *tnode) bool {
		tg, err := tn.n.LocalTarget(context.Background(), "sb", 0, 0)
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
	// own reads only, stale.
	lc := a.n.LocalCopies()
	if len(lc) != 1 || !lc[0].Paused || lc[0].Serving {
		t.Fatalf("node-0's copy while cut off: %+v", lc)
	}
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
	var driver, dsn string
	switch d.dialect {
	case "sqlite":
		u, err := url.Parse(d.url)
		if err != nil {
			t.Fatal(err)
		}
		driver, dsn = "sqlite", strings.TrimPrefix(u.Path, "/")
		if _, err := os.Stat(dsn); err != nil {
			dsn = u.Path
		}
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
		return
	default:
		t.Fatalf("no clock step on %s", d.dialect)
	}
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(context.Background(), "UPDATE sl_shard_copies SET lease_until = 0 WHERE index_name = ?", index); err != nil {
		t.Fatal(err)
	}
}

// TestLeaseDeadlineByFakeClock: a lease surely holds until TTL less the margin after
// the clocks' readings taken before its last grant, by the monotonic clock and by the
// wall clock both (a suspended machine's monotonic clock may stand still); a slot taken
// over from another node is quarantined for TTL plus margin after the claim began.
func TestLeaseDeadlineByFakeClock(t *testing.T) {
	clk := &fakeClock{}
	clk.wall.Store(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC).UnixNano())
	l := &lease{ttl: time.Second, margin: 100 * time.Millisecond, clock: clk}
	l.extend(0, clk.Wall())
	if !l.valid() {
		t.Fatal("a fresh lease is not valid")
	}
	clk.set(899 * time.Millisecond)
	if !l.valid() {
		t.Fatal("not valid before the margin")
	}
	clk.set(900 * time.Millisecond)
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
	clk.set(1400 * time.Millisecond)
	if l.valid() {
		t.Fatal("valid past deadline less margin")
	}

	// Suspend: the wall clock runs on while the monotonic one stands still.
	l.extend(clk.Now(), clk.Wall())
	clk.wall.Add(int64(5 * time.Second))
	if l.valid() {
		t.Fatal("valid after the machine slept past the lease, by its monotonic clock alone")
	}

	// Quarantine: a stolen slot serves nothing for TTL plus margin after its claim.
	n := &Node{opts: Options{LeaseTTL: time.Second, LeaseMargin: 100 * time.Millisecond}, clock: clk, id: "me"}
	from := clk.Now()
	q := n.newLease(store.Copy{TakenFrom: "other"}, from, clk.Wall())
	if !q.quarantined() || !q.valid() {
		t.Fatalf("a stolen slot: quarantined %v, valid %v", q.quarantined(), q.valid())
	}
	clk.set(from + 1099*time.Millisecond)
	if !q.quarantined() {
		t.Fatal("the quarantine ended before TTL plus margin")
	}
	clk.set(from + 1100*time.Millisecond)
	if q.quarantined() {
		t.Fatal("still quarantined after TTL plus margin")
	}
	if mine := n.newLease(store.Copy{TakenFrom: ""}, clk.Now(), clk.Wall()); mine.quarantined() {
		t.Fatal("a free slot is quarantined")
	}
}

// fakeClock is a Clock a test moves: a monotonic reading and a wall clock that run
// together unless a test steps one alone.
type fakeClock struct{ now, wall atomic.Int64 }

func (c *fakeClock) Now() time.Duration { return time.Duration(c.now.Load()) }
func (c *fakeClock) Wall() time.Time    { return time.Unix(0, c.wall.Load()) }
func (c *fakeClock) set(d time.Duration) {
	c.wall.Add(int64(d) - c.now.Load())
	c.now.Store(int64(d))
}
