package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// downTap fails every shard read sent to the peer addresses in down, and counts the
// shard reads each address is sent, and the now each percolation carries.
type downTap struct {
	base http.RoundTripper

	mu    sync.Mutex
	down  map[string]bool
	asked map[string]int
	nows  []*int64
}

func (d *downTap) RoundTrip(r *http.Request) (*http.Response, error) {
	read := false
	for _, p := range []string{"search", "percolate", "get", "counts"} {
		read = read || strings.HasPrefix(r.URL.Path, peerPrefix+p)
	}
	if !read {
		return d.base.RoundTrip(r)
	}
	if strings.HasPrefix(r.URL.Path, peerPrefix+"percolate") && r.GetBody != nil {
		if body, err := r.GetBody(); err == nil {
			var msg percolateMsg
			if json.NewDecoder(body).Decode(&msg) == nil {
				d.mu.Lock()
				d.nows = append(d.nows, msg.Now)
				d.mu.Unlock()
			}
			_ = body.Close()
		}
	}
	d.mu.Lock()
	d.asked[r.URL.Host]++
	down := d.down[r.URL.Host]
	d.mu.Unlock()
	if down {
		return nil, errors.New("tap: the peer is down")
	}
	return d.base.RoundTrip(r)
}

func (d *downTap) set(down ...string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.down = map[string]bool{}
	for _, a := range down {
		d.down[a] = true
	}
	d.asked = map[string]int{}
	d.nows = nil
}

func (d *downTap) count(addr string) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.asked[addr]
}

// quiet is a coordinator that reads the registry only when routing asks it to: its
// view, heartbeat, allocation, catalogue and pruning loops wait an hour.
func quiet(o *Options) {
	o.ViewInterval = time.Hour
	o.HeartbeatInterval = time.Hour
	o.CatalogInterval = time.Hour
	o.PruneInterval = time.Hour
	o.LeaseTTL = 2 * time.Hour
	o.DeadAfter = 3 * time.Hour
	o.PeerTimeout = time.Second
}

// rereadRig is two nodes holding every copy, b and c, and a quiet coordinator holding
// none, started while each shard had its one copy on b: its view lists b alone, though
// c serves a copy of every shard too (trailing, saved to put back). b plays a peer of
// an older release that has no endpoint for a percolation of date math.
//
// The indexes: rr (one shard, a document x and a saved query q), rm (four shards, a
// document each) and rd (one shard with a date field, a document and a saved query).
type rereadRig struct {
	tap      *downTap
	reader   *sdkmetric.ManualReader
	coord    *tnode
	b, c     *tnode
	id       store.ShardID
	seq      int64
	trailing *view
}

func newRereadRig(t *testing.T) *rereadRig {
	r := &rereadRig{tap: &downTap{base: http.DefaultTransport}, reader: sdkmetric.NewManualReader()}
	r.tap.set()
	cl := newCluster(t, sqliteDB(t), func(i int, o *Options) {
		o.Transport = r.tap
		switch i {
		case 0:
			quiet(o)
			o.Meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(r.reader)).Meter(telemetry.ScopeName)
		case 1:
			o.hooks.olderPeer = olderNoEndpoint
		}
	})
	r.b = cl.start(1)
	createIndex(t, r.b.n, "rr", 1, 1)
	createIndex(t, r.b.n, "rm", 4, 1)
	spec := api.IndexSpec{
		Mapping:  &schema.Mapping{Fields: map[string]schema.FieldType{"seen": schema.Date}},
		Settings: api.IndexSettings{Shards: 1, ReplicasPerShard: 1},
	}
	if _, err := r.b.n.CreateIndex(tctx(t), "rd", spec); err != nil {
		t.Fatal(err)
	}
	mustWrite(t, r.b.n, "rr", upsertOp("x", 1), api.WriteOp{Kind: api.OpQueryUpsert, ID: "q", Query: json.RawMessage(`{"all": []}`)})
	for k := range 8 {
		mustWrite(t, r.b.n, "rm", upsertOp(fmt.Sprintf("m%d", k), k))
	}
	r.seq = mustWrite(t, r.b.n, "rd", api.WriteOp{Kind: api.OpUpsert, ID: "d", Body: json.RawMessage(`{"seen": 1900000000000}`)},
		api.WriteOp{Kind: api.OpQueryUpsert, ID: "recent", Query: json.RawMessage(`{"field": "seen", "op": "gte", "value": "now-7d"}`)})
	waitCopies(t, r.b.st, "rm", 4, 1, time.Minute)
	waitCopies(t, r.b.st, "rd", 1, 1, time.Minute)
	r.c = cl.start(2)
	r.coord = cl.start(0)
	two := 2
	for _, index := range []string{"rr", "rm", "rd"} {
		if _, err := r.b.n.PatchSettings(tctx(t), index, api.SettingsPatch{ReplicasPerShard: &two}); err != nil {
			t.Fatal(err)
		}
	}
	waitCopies(t, r.b.st, "rr", 1, 2, time.Minute)
	waitCopies(t, r.b.st, "rm", 4, 2, time.Minute)
	waitCopies(t, r.b.st, "rd", 1, 2, time.Minute)
	waitCount(t, r.c.n, "rr", r.seq, 1)
	waitCount(t, r.c.n, "rm", r.seq, 8)
	r.id = store.ShardID{Index: "rr", Shard: 0}
	r.trailing = r.coord.n.view.Load()
	for _, id := range []store.ShardID{r.id, {Index: "rm", Shard: 3}, {Index: "rd", Shard: 0}} {
		if c := r.coord.n.candidates(id); len(c) != 1 || c[0].addr != r.b.addr {
			t.Fatalf("the coordinator routes %s to %v, want b alone", id, c)
		}
	}
	return r
}

// reset puts the coordinator's trailing view back, and the peers up.
func (r *rereadRig) reset(down ...string) {
	r.coord.n.view.Store(r.trailing)
	r.tap.set(down...)
}

// target is a read of rr's shard routed to cands, by a view read when the published
// one was: a re-read reads the registry again (or takes a view read since).
func (r *rereadRig) target(cands ...candidate) *remoteTarget {
	_, routed := r.coord.n.route(r.id)
	return &remoteTarget{n: r.coord.n, id: r.id, waitSeq: r.seq, routed: routed, cands: cands}
}

func (r *rereadRig) at(tn *tnode) candidate {
	return candidate{node: tn.n.ID(), addr: tn.addr, shard: r.id}
}

// rereads is the coordinator's copy table re-reads of op so far, by result.
func (r *rereadRig) rereads(t *testing.T, op, result string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	want := attribute.NewSet(attribute.String("op", op), attribute.String("result", result))
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok || m.Name != telemetry.MetricClusterReadRereads {
				continue
			}
			for _, dp := range sum.DataPoints {
				if dp.Attributes.Equals(&want) {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// allRereads is every re-read of op so far.
func (r *rereadRig) allRereads(t *testing.T, op string) int64 {
	return r.rereads(t, op, "found") + r.rereads(t, op, "none") + r.rereads(t, op, "timeout")
}

// rereadOps are the remote shard reads that share routing, each run on a target of rr.
var rereadOps = []struct {
	op  string
	run func(ctx context.Context, rt *remoteTarget) error
}{
	{"search", func(ctx context.Context, rt *remoteTarget) error {
		defer rt.Release()
		res, err := rt.Search(ctx, &search.Request{Index: "rr", Query: &query.All{}, Size: 10, NoBodies: true, TrackTotal: search.TrackTotalAll})
		if err == nil && len(res.Hits) != 1 {
			err = errors.New("the search found no hit")
		}
		return err
	}},
	{"percolate", func(ctx context.Context, rt *remoteTarget) error {
		ids, err := rt.Percolate(query.WithNow(ctx, time.UnixMilli(1_900_000_000_000)), nil, []schema.Doc{{ID: "d", Body: []byte(`{"title": "a"}`)}})
		if err == nil && (len(ids) != 1 || !strings.Contains(string(ids[0]), `"q"`)) {
			err = errors.New("the percolation matched no saved query")
		}
		return err
	}},
	{"get", func(ctx context.Context, rt *remoteTarget) error {
		_, found, err := rt.Get(ctx, "x")
		if err == nil && !found {
			err = errors.New("the document was not found")
		}
		return err
	}},
	{"counts", func(ctx context.Context, rt *remoteTarget) error {
		docs, _, err := rt.counts(ctx)
		if err == nil && docs != 1 {
			err = errors.New("the copy counts no document")
		}
		return err
	}},
}

// TestReadRereadsTheCopyTable (sb-0y2e.15): a coordinator whose view lists one copy of
// a shard, which fails, while the registry shows a second one serving.
func TestReadRereadsTheCopyTable(t *testing.T) {
	r := newRereadRig(t)

	// Every remote read (search, percolate, get, counts) reads the copy table again
	// once and is answered by the second copy, not a 503. A percolation carries the
	// same now to both.
	for _, tc := range rereadOps {
		t.Run(tc.op, func(t *testing.T) {
			r.reset(r.b.addr)
			before := r.rereads(t, tc.op, "found")
			if err := tc.run(tctx(t), r.target(r.at(r.b))); err != nil {
				t.Fatalf("a read whose one known copy failed: %v", err)
			}
			if b, c := r.tap.count(r.b.addr), r.tap.count(r.c.addr); b != 1 || c < 1 {
				t.Fatalf("the failing copy was asked %d times and the re-read one %d, want 1 and at least 1", b, c)
			}
			if got := r.rereads(t, tc.op, "found") - before; got != 1 {
				t.Fatalf("%d re-reads found a copy, want 1", got)
			}
			if tc.op == "percolate" {
				r.tap.mu.Lock()
				nows := r.tap.nows
				r.tap.mu.Unlock()
				if len(nows) != 2 || nows[0] == nil || nows[1] == nil || *nows[0] != *nows[1] {
					t.Fatalf("the percolations carried now %v, want the same instant twice", nows)
				}
			}
		})
	}

	// Routed by the engine (Remote, through the trailing view), a search of every
	// shard of rm at once: each shard's copy on b fails, each shard re-reads, and
	// they share one read of the copy table.
	t.Run("shards in parallel", func(t *testing.T) {
		r.reset(r.b.addr)
		before := r.rereads(t, "search", "found")
		reads := r.coord.wrap.tableReads.Load()
		got, err := count(tctx(t), r.coord.n, "rm", r.seq)
		if err != nil || got != 8 {
			t.Fatalf("a search of rm with b down: %d, %v; want 8", got, err)
		}
		if n := r.rereads(t, "search", "found") - before; n != 4 {
			t.Fatalf("%d shards re-read and found a copy, want 4", n)
		}
		if n := r.coord.wrap.tableReads.Load() - reads; n != 1 {
			t.Fatalf("the four shards read the copy table %d times, want once", n)
		}
	})

	// A shard whose view was read after it was routed takes that view: no read.
	t.Run("a view read since", func(t *testing.T) {
		r.tap.set()
		if err := r.coord.n.refreshView(tctx(t)); err != nil {
			t.Fatal(err)
		}
		reads := r.coord.wrap.tableReads.Load()
		cands, _, _ := r.coord.n.rerouteCandidates(tctx(t), r.id, r.trailing.readBegan)
		if len(cands) != 2 {
			t.Fatalf("the newer view routes to %v, want both copies", cands)
		}
		if n := r.coord.wrap.tableReads.Load() - reads; n != 0 {
			t.Fatalf("the copy table was read %d times with a newer view at hand", n)
		}
	})

	// A copy of an older release refusing a percolation of date math (errOlderLevel)
	// is as a failed one: the read re-reads and moves on.
	t.Run("older wire level", func(t *testing.T) {
		r.reset()
		before := r.rereads(t, "percolate", "found")
		res, err := r.coord.n.Percolate(tctx(t), "rd", &api.PercolateRequest{Docs: []json.RawMessage{json.RawMessage(fmt.Sprintf(`{"seen": %d}`, time.Now().UnixMilli()))}}, api.ReadOptions{WaitForSeq: r.seq})
		if err != nil {
			t.Fatal(err)
		}
		if got := string(res.Results[0].Queries); got != `["recent"]` {
			t.Fatalf("the percolation matched %s", got)
		}
		if n := r.rereads(t, "percolate", "found") - before; n != 1 {
			t.Fatalf("%d re-reads found a copy, want 1", n)
		}
	})

	// A copy that has not reached wait_for_seq answers in time with a timeout: the
	// read re-reads, and the copy that has it answers.
	t.Run("lagging copy under wait_for_seq", func(t *testing.T) {
		r.reset()
		stall := make(chan struct{})
		r.b.wrap.stallTail.Store(&stall)
		released := false
		release := func() {
			if !released {
				released = true
				r.b.wrap.stallTail.Store(nil)
				close(stall)
			}
		}
		defer release()
		eventually(t, 10*time.Second, "b's tailer is held", func() error {
			if r.b.wrap.stalled.Load() == 0 {
				return errors.New("no poll held yet")
			}
			return nil
		})
		seq := mustWrite(t, r.coord.n, "rr", upsertOp("late", 2))
		waitCount(t, r.c.n, "rr", seq, 2)
		before := r.rereads(t, "search", "found")
		got, err := count(tctx(t), r.coord.n, "rr", seq)
		if err != nil || got != 2 {
			t.Fatalf("a wait_for_seq search with b behind: %d, %v; want 2", got, err)
		}
		if n := r.rereads(t, "search", "found") - before; n != 1 {
			t.Fatalf("%d re-reads found a copy, want 1", n)
		}
		release()
		waitCount(t, r.b.n, "rr", seq, 2)
	})

	// A peer that restarted with the same node id at a new address is another copy:
	// the read routed to its old address tries it at the new one.
	t.Run("new address", func(t *testing.T) {
		r.reset(r.c.addr)
		gone := candidate{node: r.b.n.ID(), addr: "127.0.0.1:1", shard: r.id}
		if _, found, err := r.target(gone).Get(tctx(t), "x"); err != nil || !found {
			t.Fatalf("a get routed to b's old address: %v, %v", found, err)
		}
		if n := r.tap.count(r.b.addr); n != 1 {
			t.Fatalf("b at its new address was asked %d times, want 1", n)
		}
	})
}

// TestRereadFindingNothingNewIsUnavailable: when every copy fails, the read re-reads
// the copy table once, tries only the copies it has not tried, and answers 503; a
// second call of the same read re-reads no more, nor does a read whose routing
// already read the registry again.
func TestRereadFindingNothingNewIsUnavailable(t *testing.T) {
	r := newRereadRig(t)
	for _, tc := range rereadOps {
		t.Run(tc.op, func(t *testing.T) {
			r.reset(r.b.addr, r.c.addr)
			before := r.allRereads(t, tc.op)
			rt := r.target(r.at(r.b))
			err := tc.run(tctx(t), rt)
			var ae *api.Error
			if !errors.As(err, &ae) || ae.Status != http.StatusServiceUnavailable {
				t.Fatalf("a read every copy of which fails: %v, want a 503", err)
			}
			if b, c := r.tap.count(r.b.addr), r.tap.count(r.c.addr); b != 1 || c != 1 {
				t.Fatalf("the copies were asked %d and %d times, want once each", b, c)
			}
			if got := r.allRereads(t, tc.op) - before; got != 1 {
				t.Fatalf("%d re-reads, want 1", got)
			}
			if err := tc.run(tctx(t), rt); err == nil {
				t.Fatal("the read again answered with every copy down")
			}
			if b, c := r.tap.count(r.b.addr), r.tap.count(r.c.addr); b != 2 || c != 2 {
				t.Fatalf("on the second call the copies were asked %d and %d times in all, want twice each", b, c)
			}
			if got := r.allRereads(t, tc.op) - before; got != 1 {
				t.Fatalf("%d re-reads after the second call, want still 1", got)
			}
		})
	}
	t.Run("all known", func(t *testing.T) {
		r.reset(r.b.addr, r.c.addr)
		if err := r.coord.n.refreshView(tctx(t)); err != nil {
			t.Fatal(err)
		}
		before := r.rereads(t, "get", "none")
		if err := rereadOps[2].run(tctx(t), r.target(r.at(r.b), r.at(r.c))); err == nil {
			t.Fatal("a get with every copy down answered")
		}
		if b, c := r.tap.count(r.b.addr), r.tap.count(r.c.addr); b != 1 || c != 1 {
			t.Fatalf("the copies were asked %d and %d times, want once each", b, c)
		}
		if got := r.rereads(t, "get", "none") - before; got != 1 {
			t.Fatalf("%d re-reads found nothing new, want 1", got)
		}
	})
	// An index created after the coordinator's view was read: routing finds no copy,
	// reads the registry, and finds one; when that one fails the read does not read it
	// a second time.
	t.Run("routing already re-read", func(t *testing.T) {
		r.reset()
		createIndex(t, r.b.n, "late", 1, 1)
		waitCopies(t, r.b.st, "late", 1, 1, time.Minute)
		r.reset(r.b.addr, r.c.addr)
		id := store.ShardID{Index: "late", Shard: 0}
		if c := r.coord.n.candidates(id); len(c) != 0 {
			t.Fatalf("the trailing view routes the new index to %v", c)
		}
		before := r.allRereads(t, "counts")
		reads := r.coord.wrap.tableReads.Load()
		_, _, err := (&clusterHooks{n: r.coord.n}).Counts(tctx(t), id)
		var ae *api.Error
		if !errors.As(err, &ae) || ae.Status != http.StatusServiceUnavailable {
			t.Fatalf("counts of a shard whose one copy fails: %v, want a 503", err)
		}
		if n := r.coord.wrap.tableReads.Load() - reads; n != 1 {
			t.Fatalf("the copy table was read %d times, want once (routing's)", n)
		}
		if n := r.allRereads(t, "counts") - before; n != 0 {
			t.Fatalf("%d re-reads after routing's", n)
		}
		if n := r.tap.count(r.b.addr) + r.tap.count(r.c.addr); n != 1 {
			t.Fatalf("the copies were asked %d times, want once", n)
		}
	})
}

// holdTable makes tn's reads of the whole copy table wait until the test ends (or
// their own context does): a database that does not answer.
func holdTable(t *testing.T, tn *tnode) {
	release := make(chan struct{})
	hold := func(ctx context.Context) {
		select {
		case <-release:
		case <-ctx.Done():
		}
	}
	tn.wrap.holdTable.Store(&hold)
	t.Cleanup(func() {
		tn.wrap.holdTable.Store(nil)
		close(release)
	})
}

// TestRereadKeepsTheRequestBudget: a re-read waiting on a registry read that does not
// answer gives up within its share of the request's time: the read answers 503 while
// the request still has time (for the coordinator's own copy, when it holds one), and
// asks no other copy.
func TestRereadKeepsTheRequestBudget(t *testing.T) {
	r := newRereadRig(t)
	r.reset(r.b.addr)
	holdTable(t, r.coord)
	before := r.rereads(t, "get", "timeout")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	start := time.Now()
	_, _, err := r.target(r.at(r.b)).Get(ctx, "x")
	var ae *api.Error
	if !errors.As(err, &ae) || ae.Status != http.StatusServiceUnavailable || ctx.Err() != nil {
		t.Fatalf("a read whose re-read is held: %v (request %v), want a 503 in time", err, ctx.Err())
	}
	if took := time.Since(start); took > 1500*time.Millisecond {
		t.Fatalf("the read took %s of its 2s", took)
	}
	if got := r.rereads(t, "get", "timeout") - before; got != 1 {
		t.Fatalf("%d re-reads timed out, want 1", got)
	}
	if c := r.tap.count(r.c.addr); c != 0 {
		t.Fatalf("the copy the re-read would find was asked %d times", c)
	}
}

// TestRereadLeavesTimeForTheLocalCopy: a coordinator whose own copy is stale (paused)
// reads another; when every other copy fails and the re-read waits on a registry read
// that does not answer, the search still falls back to the coordinator's copy within
// its deadline.
func TestRereadLeavesTimeForTheLocalCopy(t *testing.T) {
	tap := &downTap{base: http.DefaultTransport}
	tap.set()
	cl := newCluster(t, sqliteDB(t), func(_ int, o *Options) {
		o.Transport = tap
	})
	a, b, c := cl.start(0), cl.start(1), cl.start(2)
	createIndex(t, a.n, "lf", 1, 0)
	waitCopies(t, a.st, "lf", 1, 3, time.Minute)
	waitRoutes(t, a, "lf", 1, 3, time.Minute)
	seq := mustWrite(t, a.n, "lf", upsertOp("x", 1))
	waitCount(t, a.n, "lf", seq, 1)

	cp, ok := a.n.Hosted(store.ShardID{Index: "lf", Shard: 0})
	if !ok {
		t.Fatal("a holds no copy")
	}
	if err := a.n.PauseCopy(tctx(t), cp); err != nil {
		t.Fatal(err)
	}
	tap.set(b.addr, c.addr)
	holdTable(t, a)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := a.n.Search(ctx, "lf", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{})
	if err != nil {
		t.Fatalf("a search with every other copy down and the re-read held: %v", err)
	}
	if !res.Stale || res.Total != 1 {
		t.Fatalf("the search answered %d, stale %v; want the stale copy's 1", res.Total, res.Stale)
	}
	if tap.count(b.addr)+tap.count(c.addr) == 0 {
		t.Fatal("no other copy was asked first")
	}
}

// TestRereadsOfShortRequestsAreNotStarved: requests with too little time to wait out a
// slow registry read (a 400 ms deadline gives a re-read 100 ms; the read takes 300 ms)
// each give up on it, but the read they started goes on and publishes its view, so a
// later request routed by the same trailing view takes it and is answered.
func TestRereadsOfShortRequestsAreNotStarved(t *testing.T) {
	r := newRereadRig(t)
	r.reset(r.b.addr)
	slow := func(ctx context.Context) {
		select {
		case <-time.After(300 * time.Millisecond):
		case <-ctx.Done():
		}
	}
	r.coord.wrap.holdTable.Store(&slow)
	defer r.coord.wrap.holdTable.Store(nil)
	reads := r.coord.wrap.tableReads.Load()
	answered := -1
	for i := range 20 {
		rt := r.target(r.at(r.b))
		rt.routed = r.trailing.readBegan
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		_, found, err := rt.Get(ctx, "x")
		cancel()
		if err == nil && found {
			answered = i
			break
		}
	}
	if answered < 0 {
		t.Fatal("no request of 20 was answered: every re-read was cut short")
	}
	if n := r.coord.wrap.tableReads.Load() - reads; n > 2 {
		t.Fatalf("the copy table was read %d times, want the requests to share a read", n)
	}
	if got := r.rereads(t, "get", "timeout"); got < 1 && answered > 0 {
		t.Fatalf("requests gave up on the re-read (%d) but none was counted as timed out", answered)
	}
}
