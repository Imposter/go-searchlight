package cluster

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/store"
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
	for _, c := range list {
		if c.Shard == id && c.LeaseLeft > 0 {
			out = append(out, c)
		}
	}
	return out
}

// serving counts id's serving copies under a live lease.
func serving(t testing.TB, st store.Store, id store.ShardID) (n int, nodes []string) {
	t.Helper()
	for _, c := range liveCopies(t, st, id) {
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
				for _, c := range liveCopies(t, st, id) {
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
			wg.Go(func() {
				for k := 0; ; k++ {
					select {
					case <-stopW:
						return
					default:
					}
					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					res, err := others[0].n.Write(ctx, "roll", []api.WriteOp{upsertOp(fmt.Sprintf("r%d-%d", i, k), k)}, api.WriteOptions{})
					cancel()
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

// TestPartitionedNodeStopsServingBeforeSteal (split-brain safety): a node cut off from
// the database cannot renew its leases. It stops serving each copy at its local lease
// deadline (by its own monotonic clock, less the margin), which comes before the
// database's lease_until, so before any other node can claim the slot.
func TestPartitionedNodeStopsServingBeforeSteal(t *testing.T) {
	forEachDialect(t, func(t *testing.T, d *db) {
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

		var stoppedAt, stolenAt atomic.Int64 // Unix nanoseconds
		start := time.Now()
		a.wrap.cut(true) // the partition
		done := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() { // when does node-0 stop serving the copy?
			for {
				select {
				case <-done:
					return
				default:
				}
				served := false
				for _, lc := range a.n.LocalCopies() {
					if lc.Info.Index == "sb" && lc.Serving {
						served = true
					}
				}
				if !served {
					stoppedAt.CompareAndSwap(0, time.Now().UnixNano())
					return
				}
				time.Sleep(time.Millisecond)
			}
		})
		wg.Go(func() { // when does the registry let another node take the slot?
			for {
				select {
				case <-done:
					return
				default:
				}
				for _, cp := range liveCopies(t, b.st, id) {
					if cp.NodeID == b.n.id {
						stolenAt.CompareAndSwap(0, time.Now().UnixNano())
						return
					}
				}
				time.Sleep(time.Millisecond)
			}
		})
		eventually(t, 30*time.Second, "node-1 takes the copy over", func() error {
			if stolenAt.Load() == 0 || stoppedAt.Load() == 0 {
				return fmt.Errorf("stopped %d, stolen %d", stoppedAt.Load(), stolenAt.Load())
			}
			return nil
		})
		close(done)
		wg.Wait()
		stop, steal := time.Unix(0, stoppedAt.Load()), time.Unix(0, stolenAt.Load())
		t.Logf("partitioned; node-0 stopped serving after %s, node-1 claimed the slot after %s", stop.Sub(start), steal.Sub(start))
		if !stop.Before(steal) {
			t.Fatalf("node-0 served until %s, after node-1 claimed the copy at %s", stop.Sub(start), steal.Sub(start))
		}
		if limit := a.n.opts.LeaseTTL + 500*time.Millisecond; stop.Sub(start) > limit {
			t.Fatalf("node-0 stopped serving %s after the partition, beyond its lease (%s)", stop.Sub(start), a.n.opts.LeaseTTL)
		}
		if lc := a.n.LocalCopies(); len(lc) != 0 {
			t.Fatalf("node-0 still hosts %+v", lc)
		}
		// Healed, node-0 serves again through node-1's copy.
		a.wrap.cut(false)
		eventually(t, 30*time.Second, "node-0 reads through node-1", func() error {
			got, err := count(context.Background(), a.n, "sb", 0)
			if err != nil {
				return err
			}
			if got != 1 {
				return fmt.Errorf("counts %d", got)
			}
			return nil
		})
	})
}

// TestLeaseDeadlineByFakeClock: a lease's local deadline is the clock's reading taken
// before the renewal plus the TTL, less the margin, by the node's own clock alone.
func TestLeaseDeadlineByFakeClock(t *testing.T) {
	clk := &fakeClock{}
	l := &lease{margin: 100 * time.Millisecond, clock: clk}
	l.deadline.Store(int64(time.Second))
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
	l.extend(500*time.Millisecond, time.Second) // renewed: from the reading before the call
	if !l.valid() {
		t.Fatal("not valid after a renewal")
	}
	l.extend(100*time.Millisecond, time.Second) // a late answer to an older renewal never moves it back
	if time.Duration(l.deadline.Load()) != 1500*time.Millisecond {
		t.Fatalf("deadline %s", time.Duration(l.deadline.Load()))
	}
	clk.set(1400 * time.Millisecond)
	if l.valid() {
		t.Fatal("valid past deadline less margin")
	}
}

type fakeClock struct{ now atomic.Int64 }

func (c *fakeClock) Now() time.Duration  { return time.Duration(c.now.Load()) }
func (c *fakeClock) set(d time.Duration) { c.now.Store(int64(d)) }
