package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"math"
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

// rereadRig is three nodes over one database and an index of one shard with two
// copies: coord holds none, and reads it from b or c.
type rereadRig struct {
	tap     *downTap
	readers map[int]*sdkmetric.ManualReader
	coord   *tnode
	b, c    *tnode
	id      store.ShardID
	seq     int64
}

func newRereadRig(t *testing.T) *rereadRig {
	r := &rereadRig{tap: &downTap{base: http.DefaultTransport}, readers: map[int]*sdkmetric.ManualReader{}}
	r.tap.set()
	cl := newCluster(t, sqliteDB(t), func(i int, o *Options) {
		o.Transport = r.tap
		reader := sdkmetric.NewManualReader()
		r.readers[i] = reader
		o.Meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter(telemetry.ScopeName)
	})
	nodes := []*tnode{cl.start(0), cl.start(1), cl.start(2)}
	createIndex(t, nodes[0].n, "rr", 1, 2)
	waitCopies(t, nodes[0].st, "rr", 1, 2, time.Minute)
	r.id = store.ShardID{Index: "rr", Shard: 0}
	var holders []*tnode
	for _, tn := range nodes {
		if _, ok := tn.n.Hosted(r.id); ok {
			holders = append(holders, tn)
		} else {
			r.coord = tn
		}
	}
	if r.coord == nil || len(holders) != 2 {
		t.Fatalf("%d nodes hold the shard, want 2 and a coordinator without it", len(holders))
	}
	r.b, r.c = holders[0], holders[1]
	waitRoutes(t, r.coord, "rr", 1, 2, time.Minute)
	r.seq = mustWrite(t, r.coord.n, "rr", upsertOp("x", 1),
		api.WriteOp{Kind: api.OpQueryUpsert, ID: "q", Query: json.RawMessage(`{"all": []}`)})
	waitCount(t, r.b.n, "rr", r.seq, 1)
	waitCount(t, r.c.n, "rr", r.seq, 1)
	return r
}

// target is a read of the shard routed with a view that lists only cands, as a view
// one refresh behind the registry does: routed is that view's read, so a re-read must
// read the registry again (or take a view read since).
func (r *rereadRig) target(cands ...*tnode) *remoteTarget {
	_, routed := r.coord.n.route(r.id)
	t := &remoteTarget{n: r.coord.n, id: r.id, waitSeq: r.seq, routed: routed}
	for _, tn := range cands {
		t.cands = append(t.cands, candidate{node: tn.n.ID(), addr: tn.addr, shard: r.id})
	}
	return t
}

// rereads is the coordinator's copy table re-reads of op so far, by result.
func (r *rereadRig) rereads(t *testing.T, op, result string) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := r.readers[r.coord.i].Collect(context.Background(), &rm); err != nil {
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

// rereadOps are the remote shard reads that share routing, each run on a target.
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
// a shard, which fails, while the registry shows a second one serving: every remote
// read (search, percolate, get, counts) reads the copy table again once and is
// answered by the second copy, not a 503. A percolation carries the same now to both.
func TestReadRereadsTheCopyTable(t *testing.T) {
	r := newRereadRig(t)
	for _, tc := range rereadOps {
		t.Run(tc.op, func(t *testing.T) {
			r.tap.set(r.b.addr)
			before := r.rereads(t, tc.op, "found")
			if err := tc.run(tctx(t), r.target(r.b)); err != nil {
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
}

// TestRereadFindingNothingNewIsUnavailable: when every copy fails, the read re-reads
// the copy table once, tries only the copies it did not try yet, and answers 503; a
// second call of the same read re-reads no more.
func TestRereadFindingNothingNewIsUnavailable(t *testing.T) {
	r := newRereadRig(t)
	for _, tc := range rereadOps {
		t.Run(tc.op, func(t *testing.T) {
			r.tap.set(r.b.addr, r.c.addr)
			before := r.rereads(t, tc.op, "none") + r.rereads(t, tc.op, "found")
			rt := r.target(r.b)
			err := tc.run(tctx(t), rt)
			var ae *api.Error
			if !errors.As(err, &ae) || ae.Status != http.StatusServiceUnavailable {
				t.Fatalf("a read every copy of which fails: %v, want a 503", err)
			}
			if b, c := r.tap.count(r.b.addr), r.tap.count(r.c.addr); b != 1 || c != 1 {
				t.Fatalf("the copies were asked %d and %d times, want once each", b, c)
			}
			if got := r.rereads(t, tc.op, "none") + r.rereads(t, tc.op, "found") - before; got != 1 {
				t.Fatalf("%d re-reads, want 1", got)
			}
			if err := tc.run(tctx(t), rt); err == nil {
				t.Fatal("the read again answered with every copy down")
			}
			if b, c := r.tap.count(r.b.addr), r.tap.count(r.c.addr); b != 2 || c != 2 {
				t.Fatalf("on the second call the copies were asked %d and %d times in all, want twice each", b, c)
			}
			if got := r.rereads(t, tc.op, "none") + r.rereads(t, tc.op, "found") - before; got != 1 {
				t.Fatalf("%d re-reads after the second call, want still 1", got)
			}
		})
	}
	t.Run("all known", func(t *testing.T) {
		r.tap.set(r.b.addr, r.c.addr)
		before := r.rereads(t, "get", "none")
		if err := rereadOps[2].run(tctx(t), r.target(r.b, r.c)); err == nil {
			t.Fatal("a get with every copy down answered")
		}
		if b, c := r.tap.count(r.b.addr), r.tap.count(r.c.addr); b != 1 || c != 1 {
			t.Fatalf("the copies were asked %d and %d times, want once each", b, c)
		}
		if got := r.rereads(t, "get", "none") - before; got != 1 {
			t.Fatalf("%d re-reads found nothing new, want 1", got)
		}
	})
}

// TestRereadKeepsTheDeadline: a re-read waits on the request's deadline, not past it:
// held by the routing misses' spacing, it gives up when the request does, and the
// read fails with the deadline without asking another copy.
func TestRereadKeepsTheDeadline(t *testing.T) {
	r := newRereadRig(t)
	r.tap.set(r.b.addr)
	n := r.coord.n
	n.missMu.Lock()
	n.missReadAt = n.clock.Now().Add(time.Hour)
	n.missMu.Unlock()
	t.Cleanup(func() {
		n.missMu.Lock()
		n.missReadAt = time.Time{}
		n.missMu.Unlock()
	})
	rt := r.target(r.b)
	rt.routed = math.MaxInt64
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := rt.Get(ctx, "x")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a read whose re-read outlived its deadline: %v, want the deadline", err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Fatalf("the read took %s past its 300ms deadline", took)
	}
	if c := r.tap.count(r.c.addr); c != 0 {
		t.Fatalf("the copy the re-read would find was asked %d times", c)
	}
}
