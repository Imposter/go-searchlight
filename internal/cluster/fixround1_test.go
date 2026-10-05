package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// TestDatabaseOutageOneNodeCluster (probe P1, spec section 10): a cluster of one cut
// off from the database for longer than its leases keeps serving reads, marked stale,
// as a single node does; GET falls back to its copy; readiness turns false after
// max_lag. Healed, the node claims its slots back at the same epochs and its copies
// resume without a rebuild.
func TestDatabaseOutageOneNodeCluster(t *testing.T) {
	c := newCluster(t, sqliteDB(t), nil)
	a := c.start(0)
	createIndex(t, a.n, "out", 2, 0)
	last := mustWrite(t, a.n, "out", upsertOp("a", 1), upsertOp("b", 2), upsertOp("c", 3))
	waitCount(t, a.n, "out", last, 3)
	epochs := map[int]int64{}
	for _, l := range a.n.leaseList() {
		epochs[l.copy.Shard.Shard] = l.copy.Epoch
	}

	a.wrap.cut(true)
	deadline := time.Now().Add(a.n.opts.LeaseTTL + 2*time.Second) // well past every lease
	reads := 0
	for time.Now().Before(deadline) {
		res, err := a.n.Search(tctx(t), "out", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{})
		if err != nil {
			t.Fatalf("a read failed %s into the outage: %v", time.Until(deadline), err)
		}
		if res.Total != 3 {
			t.Fatalf("total %d during the outage", res.Total)
		}
		reads++
		time.Sleep(50 * time.Millisecond)
	}
	res, err := a.n.Search(tctx(t), "out", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{})
	if err != nil || !res.Stale {
		t.Fatalf("a read past the leases: %+v %v (want stale)", res, err)
	}
	for _, lc := range a.n.LocalCopies() {
		if !lc.Paused {
			t.Fatalf("a copy past its lease is not paused: %+v", lc)
		}
	}
	doc, err := a.n.GetDocument(tctx(t), "out", "b")
	if err != nil || !doc.Stale {
		t.Fatalf("GET during the outage: %+v %v", doc, err)
	}
	if err := a.n.Ready(tctx(t)); err == nil {
		t.Fatal("ready with the database gone past max_lag")
	}
	t.Logf("%d reads served through an outage past the leases", reads)

	a.wrap.cut(false)
	eventually(t, time.Minute, "the copies resume at their epochs", func() error {
		for _, l := range a.n.leaseList() {
			if l.copy.Epoch != epochs[l.copy.Shard.Shard] {
				return fmt.Errorf("shard %d came back at epoch %d, was %d", l.copy.Shard.Shard, l.copy.Epoch, epochs[l.copy.Shard.Shard])
			}
		}
		for _, lc := range a.n.LocalCopies() {
			if lc.Paused || !lc.Serving {
				return fmt.Errorf("copy %+v", lc.Info)
			}
		}
		if len(a.n.LocalCopies()) != 2 {
			return fmt.Errorf("%d copies", len(a.n.LocalCopies()))
		}
		return a.n.Ready(context.Background())
	})
	last = mustWrite(t, a.n, "out", upsertOp("d", 4))
	waitCount(t, a.n, "out", last, 4)
}

// TestConcurrentDrainsKeepAServingCopy: the two nodes holding a shard's only serving
// copies drain at once; the store's conditional retire lets one copy retire and keeps
// the other serving, so reads through a third node never fail.
func TestConcurrentDrainsKeepAServingCopy(t *testing.T) {
	c := newCluster(t, sqliteDB(t), nil)
	a := c.start(0)
	b := c.start(1)
	createIndex(t, a.n, "dr", 1, 2)
	waitCopies(t, a.st, "dr", 1, 2, time.Minute)
	last := mustWrite(t, a.n, "dr", upsertOp("x", 1))
	reader := c.start(2)
	waitCount(t, reader.n, "dr", last, 1)
	var wg sync.WaitGroup
	for _, tn := range []*tnode{a, b} {
		wg.Go(func() { tn.n.Drain(context.Background()) })
	}
	wg.Wait()
	kept := 0
	for _, tn := range []*tnode{a, b} {
		for _, lc := range tn.n.LocalCopies() {
			if lc.Serving {
				kept++
			}
		}
	}
	if kept != 1 {
		t.Fatalf("%d serving copies left after both drained, want 1", kept)
	}
	if n, nodes := serving(t, reader.st, store.ShardID{Index: "dr", Shard: 0}); n != 1 {
		t.Fatalf("the registry shows %d serving copies (%v), want 1", n, nodes)
	}
	waitCount(t, reader.n, "dr", last, 1)
}

// TestStaleCopyNotUsedWhileAFreshOneAnswers (max_lag): of a shard's two copies, one's
// node loses the database, so its copy is stale; reads through a third node keep
// coming back current: every attempt but the last refuses a stale copy.
func TestStaleCopyNotUsedWhileAFreshOneAnswers(t *testing.T) {
	c := newCluster(t, sqliteDB(t), func(_ int, o *Options) { o.LeaseTTL = 20 * time.Second; o.DeadAfter = 15 * time.Second })
	a := c.start(0)
	b := c.start(1)
	createIndex(t, a.n, "st", 1, 2)
	waitCopies(t, a.st, "st", 1, 2, time.Minute)
	last := mustWrite(t, a.n, "st", upsertOp("x", 1))
	reader := c.start(2)
	waitCount(t, reader.n, "st", last, 1)
	b.wrap.cut(true) // b's copy cannot poll: stale, while its lease still holds
	eventually(t, 10*time.Second, "b's copy is stale", func() error {
		for _, lc := range b.n.LocalCopies() {
			if lc.Info.Stale {
				return nil
			}
		}
		return errors.New("not yet")
	})
	for range 100 {
		res, err := reader.n.Search(tctx(t), "st", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if res.Stale {
			t.Fatal("a read used the stale copy while a fresh one answers")
		}
	}
	b.wrap.cut(false)
}

// TestUnusedCopyDirectoryCollected (M7): a copy directory this node holds no copy
// from, of a shard at its target elsewhere, is removed once it has lain unused for the
// grace period; recovery staging of a finished recovery too.
func TestUnusedCopyDirectoryCollected(t *testing.T) {
	c := newCluster(t, sqliteDB(t), func(_ int, o *Options) {
		o.GCInterval = 100 * time.Millisecond
		o.CopyDirGrace = 300 * time.Millisecond
	})
	a := c.start(0)
	createIndex(t, a.n, "gc", 1, 1)
	waitCopies(t, a.st, "gc", 1, 1, time.Minute)
	b := c.start(1)
	if err := b.n.SyncCatalog(tctx(t)); err != nil {
		t.Fatal(err)
	}
	var uid string
	for _, iv := range b.n.Indexes() {
		if iv.Name == "gc" {
			uid = iv.UID
		}
	}
	leftover := filepath.Join(b.cfg.DataDir, "indexes", uid, "0")
	staging := filepath.Join(b.cfg.DataDir, "recovery", "gc.0")
	for _, dir := range []string{leftover, staging} {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "junk"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 30*time.Second, "the unused directories are removed", func() error {
		for _, dir := range []string{leftover, staging} {
			if _, err := os.Stat(dir); err == nil {
				return fmt.Errorf("%s is still there", dir)
			}
		}
		return nil
	})
}

// TestSnapshotTableBounds (I6): a snapshot older than its maximum age answers 410 (the
// recovery resumes on a fresh one), one whose copy no longer serves peers 503.
func TestSnapshotTableBounds(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC))
	st := newSnapTable(time.Minute, 50*time.Millisecond, clk)
	id := store.ShardID{Index: "s", Shard: 0}
	valid := atomic.Bool{}
	valid.Store(true)
	check := func(store.ShardID) bool { return valid.Load() }
	key := st.add(id, nil)
	if err := st.use(key, check, func(store.ShardID, *shard.Snapshot) error { return nil }); err != nil {
		t.Fatal(err)
	}
	valid.Store(false)
	var ae *api.Error
	if err := st.use(key, check, func(store.ShardID, *shard.Snapshot) error { return nil }); !errors.As(err, &ae) || ae.Status != http.StatusServiceUnavailable {
		t.Fatalf("a snapshot of a copy that no longer serves: %v", err)
	}
	valid.Store(true)
	clk.Advance(50 * time.Millisecond)
	if err := st.use(key, check, func(store.ShardID, *shard.Snapshot) error { return nil }); err != nil {
		t.Fatalf("a snapshot at its maximum age: %v", err)
	}
	clk.Advance(time.Millisecond)
	if err := st.use(key, check, func(store.ShardID, *shard.Snapshot) error { return nil }); !errors.As(err, &ae) || ae.Status != http.StatusGone {
		t.Fatalf("a snapshot past its maximum age: %v", err)
	}
}

// TestLeaseWriterAbortsALapsedStream (I6, M4): a snapshot stream stops as soon as its
// copy no longer serves peers.
func TestLeaseWriterAbortsALapsedStream(t *testing.T) {
	var valid atomic.Bool
	valid.Store(true)
	var wrote atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		lw := &leaseWriter{ResponseWriter: w, rc: http.NewResponseController(w), valid: valid.Load}
		chunk := make([]byte, 1<<10)
		for i := range 100 {
			if i == 10 {
				valid.Store(false)
			}
			if _, err := lw.Write(chunk); err != nil {
				return
			}
			wrote.Add(int64(len(chunk)))
		}
	}))
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err == nil {
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	if got := wrote.Load(); got != 10<<10 {
		t.Fatalf("the stream wrote %d bytes after the lease lapsed at 10 KiB", got)
	}
}

// TestChangelogAgeCap (I4): changes older than ChangelogRetention are pruned whatever
// copy still needs them: ageFloor finds the newest change older than the cutoff.
func TestChangelogAgeCap(t *testing.T) {
	c := newCluster(t, sqliteDB(t), func(_ int, o *Options) { o.PruneInterval = time.Hour })
	a := c.start(0)
	createIndex(t, a.n, "age", 1, 0)
	var old int64
	for k := range 5 {
		old = mustWrite(t, a.n, "age", upsertOp(fmt.Sprintf("o%d", k), k))
	}
	time.Sleep(50 * time.Millisecond)
	cutoff := time.Now()
	time.Sleep(50 * time.Millisecond)
	for k := range 5 {
		mustWrite(t, a.n, "age", upsertOp(fmt.Sprintf("n%d", k), k))
	}
	id := store.ShardID{Index: "age", Shard: 0}
	got, err := a.n.ageFloor(tctx(t), id, 0, cutoff)
	if err != nil || got != old {
		t.Fatalf("ageFloor %d (%v), want %d: the newest change before the cutoff", got, err, old)
	}
	if got, err := a.n.ageFloor(tctx(t), id, 0, cutoff.Add(-time.Hour)); err != nil || got != 0 {
		t.Fatalf("ageFloor before every change: %d (%v)", got, err)
	}
	// From a pruned start it finds where the log begins.
	if err := a.st.Prune(tctx(t), id, 3); err != nil {
		t.Fatal(err)
	}
	if got, err := a.n.ageFloor(tctx(t), id, 0, cutoff); err != nil || got != old {
		t.Fatalf("ageFloor after a prune: %d (%v), want %d", got, err, old)
	}
}

// TestStoppedNodeKeepsItsTail (I4, spec section 9): a cleanly stopped node's copies stay
// in the registry, retiring at their final applied seqs, and hold the prune floor for
// the retiring retention, so the changelog it needs to replay on restart survives the
// leader's pruning; restarted, it claims its own slots back at the same epochs.
func TestStoppedNodeKeepsItsTail(t *testing.T) {
	c := newCluster(t, sqliteDB(t), func(_ int, o *Options) { o.PruneInterval = 100 * time.Millisecond })
	a := c.start(0)
	createIndex(t, a.n, "tail", 1, 0)
	b := c.start(1)
	waitCopies(t, a.st, "tail", 1, 2, time.Minute)
	last := mustWrite(t, a.n, "tail", upsertOp("x", 1))
	waitCount(t, b.n, "tail", last, 1)
	id := store.ShardID{Index: "tail", Shard: 0}
	var applied, epoch int64
	eventually(t, time.Minute, "node-1 reports its applied seq", func() error {
		for _, cp := range liveCopies(t, a.st, id) {
			if cp.NodeID == b.n.id && cp.AppliedSeq >= last {
				applied, epoch = cp.AppliedSeq, cp.Epoch
				return nil
			}
		}
		return errors.New("not yet")
	})
	b.stop()
	for k := range 20 {
		last = mustWrite(t, a.n, "tail", upsertOp(fmt.Sprintf("y%d", k), k))
	}
	time.Sleep(a.n.opts.LeaseTTL + 1500*time.Millisecond) // past the stopped copy's lease, many prune passes
	if _, err := a.st.ChangesAfter(tctx(t), id, applied, 10); err != nil {
		t.Fatalf("the changelog after the stopped copy's seq %d was pruned: %v", applied, err)
	}
	b = c.start(1)
	eventually(t, time.Minute, "node-1 claims its slot back at the same epoch", func() error {
		l := b.n.leaseFor(id)
		if l == nil {
			return errors.New("no lease")
		}
		if l.copy.Epoch != epoch {
			return fmt.Errorf("epoch %d, was %d", l.copy.Epoch, epoch)
		}
		return nil
	})
	waitCount(t, b.n, "tail", last, 21)
}

// TestSQLiteServesOneNode (fix round 2): on SQLite a second live node refuses to join
// (production SQLite is single-node); once the first has stopped and deregistered,
// another node may take its place. In-process tests opt in to multi-node SQLite with
// their test hooks.
func TestSQLiteServesOneNode(t *testing.T) {
	c := newCluster(t, sqliteDB(t), func(_ int, o *Options) { o.hooks.allowSQLiteCluster = false })
	a := c.start(0)
	createIndex(t, a.n, "one", 1, 0)
	if _, err := c.tryStart(1); !errors.Is(err, ErrSQLiteCluster) {
		t.Fatalf("a second node on SQLite: %v, want ErrSQLiteCluster", err)
	}
	if nodes, err := a.n.Nodes(tctx(t)); err != nil || len(nodes) != 1 {
		t.Fatalf("the refused node is still registered: %+v %v", nodes, err)
	}
	a.stop()
	b := c.start(1)
	last := mustWrite(t, b.n, "one", upsertOp("x", 1))
	waitCount(t, b.n, "one", last, 1)
}

// TestDecommissionedRowsReleased (N2): the retiring rows a cleanly stopped node left
// are deleted once their leases ran out more than retiring_retention ago, while the
// node is gone from sl_nodes.
func TestDecommissionedRowsReleased(t *testing.T) {
	c := newCluster(t, sqliteDB(t), func(_ int, o *Options) {
		o.PruneInterval = 100 * time.Millisecond
		o.RetiringRetention = 300 * time.Millisecond
	})
	a := c.start(0)
	createIndex(t, a.n, "dec", 1, 0)
	b := c.start(1)
	waitCopies(t, a.st, "dec", 1, 2, time.Minute)
	gone := b.n.id
	b.stop()
	id := store.ShardID{Index: "dec", Shard: 0}
	has := func() bool {
		list, err := a.st.Registry().Copies(context.Background(), "dec")
		if err != nil {
			t.Fatal(err)
		}
		for i := range list {
			if list[i].Shard == id && list[i].NodeID == gone {
				return true
			}
		}
		return false
	}
	if !has() {
		t.Fatal("the stopped node's row is gone at once: Stop released it")
	}
	eventually(t, time.Minute, "the decommissioned node's row is released", func() error {
		if has() {
			return errors.New("still there")
		}
		return nil
	})
}

// TestProgressHighWaterMark (I4): a copy's progress counter moves only when it gets
// further than it ever got, so a recovery that keeps retrying the same work stalls.
func TestProgressHighWaterMark(t *testing.T) {
	n := &Node{}
	k := copyKey{node: "n", epoch: 1}
	for _, step := range []struct{ now, want int64 }{{10, 10}, {25, 25}, {3, 25}, {25, 25}, {24, 25}, {26, 26}} {
		if got := n.progressMark(k, step.now); got != step.want {
			t.Fatalf("progress %d after %d, want %d", got, step.now, step.want)
		}
	}
	// A copy no longer held (another epoch now) is forgotten.
	n.forgetProgress(map[copyKey]bool{{node: "n", epoch: 2}: true})
	if got := n.progressMark(k, 5); got != 5 {
		t.Fatalf("a forgotten copy's mark is %d, want 5", got)
	}
	var p pruneState
	id := store.ShardID{Index: "i"}
	p.observe(copyKey{shard: id, node: "a", epoch: 1}, 1, 1, time.Now())
	p.setBelow(id, 10)
	p.forget(&view{copies: map[store.ShardID][]store.Copy{}})
	if len(p.progress) != 0 || len(p.below) != 0 {
		t.Fatalf("the leader remembers gone copies: %v %v", p.progress, p.below)
	}
}
