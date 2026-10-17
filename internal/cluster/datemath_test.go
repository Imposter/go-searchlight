package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/store"
)

// fixedWall is the real clock with its wall clock stopped at one instant.
type fixedWall struct {
	clock.Real
	at time.Time
}

func (c fixedWall) Wall() time.Time { return c.at }

// A peer resolves a percolation's date math at the coordinator's now, sent with the
// request, never at its own clock; a search's shards on a peer get the query resolved.
// The two nodes' wall clocks are a month apart, and the coordinator is the node
// holding fewer copies, so it reads at least one shard from its peer.
func TestDateMathResolvesAtTheCoordinator(t *testing.T) {
	t0 := time.Date(2031, 3, 10, 13, 45, 30, 0, time.UTC)
	walls := []time.Time{t0, t0.Add(30 * 24 * time.Hour)}
	c := newCluster(t, sqliteDB(t), func(i int, o *Options) {
		o.Engine = func(eo *node.Options) { eo.Clock = fixedWall{at: walls[i]} }
	})
	nodes := []*tnode{c.start(0), c.start(1)}
	a := nodes[0]
	spec := api.IndexSpec{
		Mapping:  &schema.Mapping{Fields: map[string]schema.FieldType{"seen": schema.Date}},
		Settings: api.IndexSettings{Shards: 4, ReplicasPerShard: 1},
	}
	if _, err := a.n.CreateIndex(tctx(t), "dm", spec); err != nil {
		t.Fatal(err)
	}
	waitCopies(t, a.st, "dm", 4, 1, time.Minute)
	held := map[string]int{}
	for s := range 4 {
		for _, cp := range liveCopies(t, a.st, store.ShardID{Index: "dm", Shard: s}) {
			held[cp.NodeID]++
		}
	}
	coord := 0
	if held[nodes[1].cfg.NodeID] < held[nodes[0].cfg.NodeID] {
		coord = 1
	}
	a, t0 = nodes[coord], walls[coord]
	const window = `{"field": "seen", "op": "between", "value": ["now-1d", "now+1d"]}`
	var ops []api.WriteOp
	want := make([]string, 16)
	for i := range 16 {
		id := fmt.Sprintf("q%02d", i)
		want[i] = fmt.Sprintf("%q", id)
		ops = append(ops,
			api.WriteOp{Kind: api.OpQueryUpsert, ID: id, Query: json.RawMessage(window)},
			api.WriteOp{Kind: api.OpUpsert, ID: fmt.Sprintf("d%02d", i), Body: json.RawMessage(fmt.Sprintf(`{"seen": %d}`, t0.UnixMilli()))})
	}
	seq := mustWrite(t, a.n, "dm", ops...)

	res, err := a.n.Percolate(tctx(t), "dm", &api.PercolateRequest{Docs: []json.RawMessage{json.RawMessage(fmt.Sprintf(`{"seen": %d}`, t0.UnixMilli()))}}, api.ReadOptions{WaitForSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(res.Results[0].Queries); got != "["+strings.Join(want, ",")+"]" {
		t.Fatalf("percolate through node %d: %s", coord, got)
	}
	q, problems := query.Parse([]byte(window))
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	sr, err := a.n.Search(tctx(t), "dm", &search.Request{Query: q, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitForSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	if sr.Total != 16 {
		t.Fatalf("search through node %d counts %d", coord, sr.Total)
	}
}

// wireTap records the date math every query phase and percolation sent to a peer
// carries, and fails the first percolation sent for each shard when failFirst is set,
// so the read retries on the shard's other copy.
type wireTap struct {
	base      http.RoundTripper
	failFirst bool

	mu       sync.Mutex
	queries  []string
	nows     []*int64
	failed   map[int]bool
	attempts map[int]int
	// refuseGet fails every stored read sent to these peer addresses.
	refuseGet map[string]bool
}

func (w *wireTap) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path == peerPrefix+"get" {
		w.mu.Lock()
		refuse := w.refuseGet[r.URL.Host]
		w.mu.Unlock()
		if refuse {
			return nil, errors.New("tap: stored reads from this peer are dropped")
		}
	}
	searching, percolating := strings.HasPrefix(r.URL.Path, peerPrefix+"search"), strings.HasPrefix(r.URL.Path, peerPrefix+"percolate")
	if (!searching && !percolating) || r.Body == nil {
		return w.base.RoundTrip(r)
	}
	b, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	if searching {
		var msg searchMsg
		if json.Unmarshal(b, &msg) == nil && msg.Request != nil {
			w.queries = append(w.queries, string(msg.Request.Query))
		}
	} else {
		var msg percolateMsg
		if json.Unmarshal(b, &msg) == nil {
			w.nows = append(w.nows, msg.Now)
			w.attempts[msg.Shard]++
			if w.failFirst && !w.failed[msg.Shard] {
				w.failed[msg.Shard] = true
				w.mu.Unlock()
				return nil, errors.New("tap: the first percolation of the shard is dropped")
			}
		}
	}
	w.mu.Unlock()
	c := r.Clone(r.Context())
	c.Body = io.NopCloser(bytes.NewReader(b))
	return w.base.RoundTrip(c)
}

// A search whose shard on a peer loses its pinned generation twice runs that shard's
// query phase again: every phase, the first and the re-run ones, carries the query
// resolved once at the coordinator, though the peer's wall clock is a month on.
func TestDateMathSurvivesARequery(t *testing.T) {
	t0 := time.Date(2031, 3, 10, 13, 45, 30, 0, time.UTC)
	walls := []time.Time{t0, t0.Add(30 * 24 * time.Hour)}
	var c *cluster
	probe := &requeryProbe{}
	tap := &wireTap{failed: map[int]bool{}, attempts: map[int]int{}}
	c = newCluster(t, sqliteDB(t), func(i int, o *Options) {
		tap.base = &peerProbe{base: http.DefaultTransport.(*http.Transport).Clone(), p: probe} //nolint:forcetypeassert,errcheck // the default transport is an *http.Transport
		o.Transport = tap
		o.Engine = func(eo *node.Options) { eo.Clock = fixedWall{at: walls[i]} }
		o.hooks.pinned = func(id store.ShardID, pin string) {
			if probe.pinned(id.Shard, pin) {
				c.node(i).n.pins.remove(pin)
			}
		}
	})
	nodes := []*tnode{c.start(0), c.start(1)}
	spec := api.IndexSpec{
		Mapping:  &schema.Mapping{Fields: map[string]schema.FieldType{"seen": schema.Date}},
		Settings: api.IndexSettings{Shards: 4, ReplicasPerShard: 1},
	}
	if _, err := nodes[0].n.CreateIndex(tctx(t), "rq", spec); err != nil {
		t.Fatal(err)
	}
	waitCopies(t, nodes[0].st, "rq", 4, 1, time.Minute)
	coord, remote := 0, []int(nil)
	for i, tn := range nodes {
		var lacks []int
		for s := range 4 {
			if _, ok := tn.n.Hosted(store.ShardID{Index: "rq", Shard: s}); !ok {
				lacks = append(lacks, s)
			}
		}
		if len(lacks) > len(remote) {
			coord, remote = i, lacks
		}
	}
	at := walls[coord]
	var ops []api.WriteOp
	for k := range 40 {
		ops = append(ops, api.WriteOp{Kind: api.OpUpsert, ID: fmt.Sprintf("d%02d", k), Body: json.RawMessage(fmt.Sprintf(`{"seen": %d}`, at.Add(-time.Duration(k)*12*time.Hour).UnixMilli()))})
	}
	seq := mustWrite(t, nodes[coord].n, "rq", ops...)
	q, _ := query.Parse([]byte(`{"field": "seen", "op": "gte", "value": "now-7d"}`))
	req := func() *search.Request {
		return &search.Request{Query: q, Size: 40, Sort: []search.SortField{{Field: "seen", Desc: true}}, TrackTotal: search.TrackTotalAll}
	}
	want, err := nodes[coord].n.Search(tctx(t), "rq", req(), api.ReadOptions{WaitForSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	if want.Total != 15 {
		t.Fatalf("an undisturbed search counts %d, want 15", want.Total)
	}
	tap.mu.Lock()
	tap.queries = nil
	tap.mu.Unlock()
	probe.start(remote[0])
	got, err := nodes[coord].n.Search(tctx(t), "rq", req(), api.ReadOptions{WaitForSeq: seq})
	pins, _, _ := probe.stop()
	if err != nil {
		t.Fatal(err)
	}
	if pins[remote[0]] != 3 {
		t.Fatalf("shard %d ran %d query phases, want 3", remote[0], pins[remote[0]])
	}
	if got.Total != want.Total || len(got.Hits) != len(want.Hits) {
		t.Fatalf("after the requery: %d (%d hits), want %d", got.Total, len(got.Hits), want.Total)
	}
	tap.mu.Lock()
	defer tap.mu.Unlock()
	for _, sent := range tap.queries {
		if sent != tap.queries[0] || strings.Contains(sent, "now") {
			t.Fatalf("query phases sent %q and %q", tap.queries[0], sent)
		}
	}
	if len(tap.queries) < len(remote)+2 {
		t.Fatalf("%d query phases sent to the peer", len(tap.queries))
	}
}

// A percolation whose first read of a shard fails retries on that shard's other
// copy with the same instant: three nodes, their wall clocks a month apart.
func TestDateMathSurvivesAPeerRetry(t *testing.T) {
	t0 := time.Date(2031, 3, 10, 13, 45, 30, 0, time.UTC)
	walls := []time.Time{t0, t0.Add(30 * 24 * time.Hour), t0.Add(60 * 24 * time.Hour)}
	tap := &wireTap{base: http.DefaultTransport, failFirst: true, failed: map[int]bool{}, attempts: map[int]int{}}
	c := newCluster(t, sqliteDB(t), func(i int, o *Options) {
		o.Transport = tap
		o.Engine = func(eo *node.Options) { eo.Clock = fixedWall{at: walls[i]} }
	})
	nodes := []*tnode{c.start(0), c.start(1), c.start(2)}
	spec := api.IndexSpec{
		Mapping:  &schema.Mapping{Fields: map[string]schema.FieldType{"seen": schema.Date}},
		Settings: api.IndexSettings{Shards: 4, ReplicasPerShard: 2},
	}
	if _, err := nodes[0].n.CreateIndex(tctx(t), "pr", spec); err != nil {
		t.Fatal(err)
	}
	waitCopies(t, nodes[0].st, "pr", 4, 2, time.Minute)
	coord, lacking := 0, -1
	for i, tn := range nodes {
		for s := range 4 {
			if _, ok := tn.n.Hosted(store.ShardID{Index: "pr", Shard: s}); !ok {
				coord, lacking = i, s
			}
		}
	}
	if lacking < 0 {
		t.Fatal("every node hosts every shard")
	}
	waitRoutes(t, nodes[coord], "pr", 4, 2, time.Minute)
	at := walls[coord]
	const window = `{"field": "seen", "op": "between", "value": ["now-1d", "now+1d"]}`
	var ops []api.WriteOp
	want := make([]string, 16)
	for i := range 16 {
		want[i] = fmt.Sprintf(`"q%02d"`, i)
		ops = append(ops, api.WriteOp{Kind: api.OpQueryUpsert, ID: fmt.Sprintf("q%02d", i), Query: json.RawMessage(window)})
	}
	seq := mustWrite(t, nodes[coord].n, "pr", ops...)
	res, err := nodes[coord].n.Percolate(tctx(t), "pr", &api.PercolateRequest{Docs: []json.RawMessage{json.RawMessage(fmt.Sprintf(`{"seen": %d}`, at.UnixMilli()))}}, api.ReadOptions{WaitForSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(res.Results[0].Queries); got != "["+strings.Join(want, ",")+"]" {
		t.Fatalf("percolate through node %d: %s", coord, got)
	}
	tap.mu.Lock()
	defer tap.mu.Unlock()
	if tap.attempts[lacking] != 2 {
		t.Fatalf("shard %d was asked %d times, want a failure and a retry", lacking, tap.attempts[lacking])
	}
	for _, now := range tap.nows {
		if now == nil || *now != at.UnixMilli() {
			t.Fatalf("a percolation was sent now %v, want %d", now, at.UnixMilli())
		}
	}
}

// A query, a post_filter, a filter aggregation and a filters aggregation with an other
// bucket, each with date math, reach a peer resolved at the coordinator: the counts
// follow the coordinator's wall clock, though the peer's is a month off (at the
// peer's, the week would hold every document, or none). A global aggregation beside
// them counts every document, the query's date math aside. (A filter cannot nest under
// a global aggregation: it holds metric and bucket aggregations only.)
func TestFilterDateMathResolvesAtTheCoordinator(t *testing.T) {
	t0 := time.Date(2031, 3, 10, 13, 45, 30, 0, time.UTC)
	walls := []time.Time{t0, t0.Add(30 * 24 * time.Hour)}
	tap := &wireTap{base: http.DefaultTransport, failed: map[int]bool{}, attempts: map[int]int{}}
	c := newCluster(t, sqliteDB(t), func(i int, o *Options) {
		o.Transport = tap
		o.Engine = func(eo *node.Options) { eo.Clock = fixedWall{at: walls[i]} }
	})
	nodes := []*tnode{c.start(0), c.start(1)}
	spec := api.IndexSpec{
		Mapping:  &schema.Mapping{Fields: map[string]schema.FieldType{"seen": schema.Date}},
		Settings: api.IndexSettings{Shards: 4, ReplicasPerShard: 1},
	}
	if _, err := nodes[0].n.CreateIndex(tctx(t), "fd", spec); err != nil {
		t.Fatal(err)
	}
	waitCopies(t, nodes[0].st, "fd", 4, 1, time.Minute)
	coord := 0
	for i, tn := range nodes {
		for s := range 4 {
			if _, ok := tn.n.Hosted(store.ShardID{Index: "fd", Shard: s}); !ok {
				coord = i
			}
		}
	}
	at := walls[coord]
	var ops []api.WriteOp
	for k := range 20 {
		ops = append(ops, api.WriteOp{Kind: api.OpUpsert, ID: fmt.Sprintf("d%02d", k), Body: json.RawMessage(fmt.Sprintf(`{"seen": %d}`, at.Add(-time.Duration(k)*24*time.Hour).UnixMilli()))})
	}
	seq := mustWrite(t, nodes[coord].n, "fd", ops...)
	week, _ := query.Parse([]byte(`{"field": "seen", "op": "gte", "value": "now-7d"}`))
	day, _ := query.Parse([]byte(`{"field": "seen", "op": "gte", "value": "now-1d/d"}`))
	recent, _ := query.Parse([]byte(`{"field": "seen", "op": "gte", "value": "now-14d"}`))
	r := &search.Request{
		Query: recent, PostFilter: week, Size: 20, TrackTotal: search.TrackTotalAll,
		Aggs: map[string]search.Agg{
			"week": {Type: search.AggFilter, Filter: week},
			"by":   {Type: search.AggFilters, Filters: map[string]query.Node{"week": week, "day": day}, OtherBucketKey: "rest"},
			"all":  {Type: search.AggGlobal, Aggs: map[string]search.Agg{"seen": {Type: search.AggStats, Field: "seen"}}},
		},
	}
	res, err := nodes[coord].n.Search(tctx(t), "fd", r, api.ReadOptions{WaitForSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	by := map[string]int64{}
	for _, b := range res.Aggs["by"].Buckets {
		by[fmt.Sprint(b.Key)] = b.DocCount
	}
	if res.Total != 8 || res.Aggs["week"].DocCount != 8 || res.Aggs["all"].DocCount != 20 || res.Aggs["all"].Aggs["seen"].Count != 20 ||
		by["week"] != 8 || by["day"] != 2 || by["rest"] != 7 {
		t.Fatalf("through node %d: post_filter %d, filter %d, global %d, filters %v; want 8, 8, 20, week 8 day 2 rest 7",
			coord, res.Total, res.Aggs["week"].DocCount, res.Aggs["all"].DocCount, by)
	}
}

// dateIndexAndQueries creates index name (4 shards, replicas copies each) with seen of
// type typ, saves eight copies of query and the document "doc" seen now, and returns
// the seq of the write.
func dateIndexAndQueries(t *testing.T, n *Node, st store.Store, name string, typ schema.FieldType, replicas int, raw string) int64 {
	t.Helper()
	spec := api.IndexSpec{
		Mapping:  &schema.Mapping{Fields: map[string]schema.FieldType{"seen": typ}},
		Settings: api.IndexSettings{Shards: 4, ReplicasPerShard: replicas},
	}
	if _, err := n.CreateIndex(tctx(t), name, spec); err != nil {
		t.Fatal(err)
	}
	waitCopies(t, st, name, 4, replicas, time.Minute)
	ops := []api.WriteOp{{Kind: api.OpUpsert, ID: "doc", Body: json.RawMessage(fmt.Sprintf(`{"seen": %d}`, time.Now().UnixMilli()))}}
	for i := range 8 {
		ops = append(ops, api.WriteOp{Kind: api.OpQueryUpsert, ID: fmt.Sprintf("q%d", i), Query: json.RawMessage(raw)})
	}
	return mustWrite(t, n, name, ops...)
}

// lacking returns a live node hosting no copy of some shard of index, and that shard.
func lacking(t *testing.T, nodes []*tnode, index string) (*tnode, int) {
	t.Helper()
	for _, tn := range nodes {
		for s := range 4 {
			if _, ok := tn.n.Hosted(store.ShardID{Index: index, Shard: s}); !ok {
				return tn, s
			}
		}
	}
	t.Fatal("every node hosts every shard: no percolation reads a peer")
	return nil, 0
}

// The three percolations of one document: given, stored (its id: a Get pins the copy
// before the percolation) and a bulk write's. Each returns the matches and the error.
func percolations(t *testing.T, n *Node, index string, seq int64) map[string]func() (string, error) {
	doc := json.RawMessage(fmt.Sprintf(`{"seen": %d}`, time.Now().UnixMilli()))
	return map[string]func() (string, error){
		"given": func() (string, error) {
			res, err := n.Percolate(tctx(t), index, &api.PercolateRequest{Docs: []json.RawMessage{doc}}, api.ReadOptions{WaitForSeq: seq})
			if err != nil {
				return "", err
			}
			return string(res.Results[0].Queries), nil
		},
		"stored": func() (string, error) {
			res, err := n.Percolate(tctx(t), index, &api.PercolateRequest{IDs: []string{"doc"}}, api.ReadOptions{WaitForSeq: seq})
			if err != nil {
				return "", err
			}
			return string(res.Results[0].Queries), nil
		},
		"bulk": func() (string, error) {
			res, err := n.Write(tctx(t), index, []api.WriteOp{{Kind: api.OpUpsert, ID: "new", Body: doc}}, api.WriteOptions{Percolate: true, WaitForSeq: seq})
			if err != nil {
				return "", err
			}
			if !res.Percolated {
				return "", res.PercolateErr
			}
			return string(res.Items[0].Queries), nil
		},
	}
}

// A percolation of an index with a date field needs percolate wire level 1: a peer of
// an older release (no endpoint for it, as 1.0.0; or answering at level 0, having
// ignored the coordinator's now) is refused, and with no other copy the percolation is
// unavailable (503; a bulk write commits with percolated false) rather than answered
// without its date math, a stored document's included, whose Get chose the copy
// first. An index with no date field percolates through the older peer as before.
func TestOlderPeerRefusedForDatePercolation(t *testing.T) {
	for name, older := range map[string]olderPeer{"no endpoint": olderNoEndpoint, "drops the level": olderDropsLevel} {
		t.Run(name, func(t *testing.T) {
			c := newCluster(t, sqliteDB(t), func(_ int, o *Options) { o.hooks.olderPeer = older })
			a := c.start(0)
			c.start(1)
			for _, idx := range []struct {
				name  string
				typ   schema.FieldType
				query string
			}{
				{"plain", schema.Number, `{"field": "seen", "op": "gte", "value": 0}`},
				{"dated", schema.Date, `{"field": "seen", "op": "gte", "value": "now-7d"}`},
			} {
				seq := dateIndexAndQueries(t, a.n, a.st, idx.name, idx.typ, 1, idx.query)
				coord, _ := lacking(t, c.live(), idx.name)
				for how, run := range percolations(t, coord.n, idx.name, seq) {
					got, err := run()
					var ae *api.Error
					switch {
					case idx.typ == schema.Number && (err != nil || strings.Count(got, `"q`) != 8):
						t.Fatalf("%s, %s, through an older peer: %s, %v", idx.name, how, got, err)
					case idx.typ == schema.Date && (!errors.As(err, &ae) || ae.Status != http.StatusServiceUnavailable):
						t.Fatalf("%s, %s, through an older peer: %s, %v; want a 503", idx.name, how, got, err)
					}
				}
			}
		})
	}
}

// A shard with a copy on an older node and one on an upgraded node percolates on the
// upgraded one: given, stored and bulk. For the stored document, the tap makes the
// Get read the older copy, so the percolation that follows starts there and must move
// on, rather than fail.
func TestDatePercolationMovesPastAnOlderCopy(t *testing.T) {
	tap := &wireTap{base: http.DefaultTransport, failed: map[int]bool{}, attempts: map[int]int{}}
	c := newCluster(t, sqliteDB(t), func(i int, o *Options) {
		o.Transport = tap
		if i == 0 {
			o.hooks.olderPeer = olderNoEndpoint
		}
	})
	old := c.start(0)
	seq := dateIndexAndQueries(t, old.n, old.st, "mx", schema.Date, 1, `{"field": "seen", "op": "gte", "value": "now-7d"}`)
	b, d := c.start(1), c.start(2)
	two := 2
	if _, err := old.n.PatchSettings(tctx(t), "mx", api.SettingsPatch{ReplicasPerShard: &two}); err != nil {
		t.Fatal(err)
	}
	waitCopies(t, old.st, "mx", 4, 2, time.Minute)
	coord, _ := lacking(t, []*tnode{b, d}, "mx")
	waitRoutes(t, coord, "mx", 4, 2, time.Minute)
	tap.mu.Lock()
	tap.refuseGet = map[string]bool{}
	for _, tn := range []*tnode{b, d} {
		if tn != coord {
			tap.refuseGet[tn.addr] = true
		}
	}
	tap.mu.Unlock()
	want := `["q0","q1","q2","q3","q4","q5","q6","q7"]`
	for how, run := range percolations(t, coord.n, "mx", seq) {
		if got, err := run(); err != nil || got != want {
			t.Fatalf("%s: %s, %v; want %s", how, got, err, want)
		}
	}
}
