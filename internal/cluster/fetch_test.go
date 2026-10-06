package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/store"
)

// TestPinnedFetchSurvivesRetire: a search's query phase pins a generation on a peer,
// then the peer drains and retires that copy. The fetch phase still reads the pinned
// generation while the retirement is under way (the copy paused, the registry write in
// flight) and after it (the copy closed).
func TestPinnedFetchSurvivesRetire(t *testing.T) {
	c := newCluster(t, sqliteDB(t), nil)
	a := c.start(0)
	p := c.start(1)
	createIndex(t, a.n, "ret", 1, 0)
	waitCopies(t, a.st, "ret", 1, 2, time.Minute)
	var last int64
	for k := range 10 {
		last = mustWrite(t, a.n, "ret", upsertOp(fmt.Sprintf("r%d", k), k))
	}
	waitCount(t, p.n, "ret", last, 10)

	id := store.ShardID{Index: "ret", Shard: 0}
	rt := &remoteTarget{n: a.n, id: id, waitSeq: last, cands: []candidate{{node: p.n.ID(), addr: p.addr, shard: id}}}
	defer rt.Release()
	res, err := rt.Search(tctx(t), &search.Request{Index: "ret", Query: &query.All{}, Size: 10, NoBodies: true, TrackTotal: search.TrackTotalAll})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hits) != 10 {
		t.Fatalf("the query phase found %d hits, want 10", len(res.Hits))
	}

	retiring := make(chan struct{})
	proceed := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(proceed) }) }
	t.Cleanup(release)
	hold := func() {
		close(retiring)
		<-proceed
	}
	p.wrap.onRetire.Store(&hold)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		p.n.Drain(context.Background())
	}()
	<-retiring
	if !p.n.Paused(id) {
		t.Fatal("the copy is not paused while it retires")
	}

	fetch := func(stage string) {
		t.Helper()
		hits := slices.Clone(res.Hits)
		if err := rt.Fetch(tctx(t), hits, nil); err != nil {
			t.Fatalf("fetch %s: %v", stage, err)
		}
		for _, h := range hits {
			if !bytes.Contains(h.Body, []byte(`"item `+h.ID+`"`)) {
				t.Fatalf("fetch %s: hit %s has body %s", stage, h.ID, h.Body)
			}
		}
	}
	fetch("while the copy retires")
	release()
	<-drained
	if _, hosted := p.n.Hosted(id); hosted {
		t.Fatal("the retired copy is still hosted")
	}
	fetch("after the copy retired")
}

// TestWaitForSeqReadSurvivesDrain (#66): a wait_for_seq search parked on a node's own
// copy, which has not applied the seq, while that node drains and retires the copy. The
// read is answered by the copy on the other node, a 200, not a 503 from the closed one.
func TestWaitForSeqReadSurvivesDrain(t *testing.T) {
	c := newCluster(t, sqliteDB(t), nil)
	a := c.start(0)
	p := c.start(1)
	createIndex(t, a.n, "dr", 1, 0)
	waitCopies(t, a.st, "dr", 1, 2, time.Minute)
	var last int64
	for k := range 10 {
		last = mustWrite(t, a.n, "dr", upsertOp(fmt.Sprintf("d%d", k), k))
	}
	waitCount(t, p.n, "dr", last, 10)

	stall := make(chan struct{})
	t.Cleanup(func() { close(stall) })
	p.wrap.stallTail.Store(&stall)
	eventually(t, 10*time.Second, "node-1's tailer is held", func() error {
		if p.wrap.stalled.Load() == 0 {
			return errors.New("no poll held yet")
		}
		return nil
	})
	last = mustWrite(t, a.n, "dr", upsertOp("late", 10))
	waitCount(t, a.n, "dr", last, 11)

	type answer struct {
		status int
		body   []byte
		err    error
	}
	got := make(chan answer, 1)
	go func() {
		url := fmt.Sprintf("http://%s/indexes/dr/_search?wait_for_seq=%d", p.addr, last)
		req, err := http.NewRequestWithContext(tctx(t), http.MethodPost, url, strings.NewReader(`{"query": {"all": []}, "size": 20}`))
		if err != nil {
			got <- answer{err: err}
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			got <- answer{err: err}
			return
		}
		b, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		got <- answer{status: resp.StatusCode, body: b, err: err}
	}()
	select {
	case r := <-got:
		t.Fatalf("the read answered before the drain (%d %s, %v): it did not park on node-1's copy", r.status, r.body, r.err)
	case <-time.After(300 * time.Millisecond):
	}

	p.n.Drain(tctx(t))
	if _, hosted := p.n.Hosted(store.ShardID{Index: "dr", Shard: 0}); hosted {
		t.Fatal("the drained copy is still hosted")
	}
	r := <-got
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.status != http.StatusOK || !bytes.Contains(r.body, []byte(`"late"`)) {
		t.Fatalf("the parked read answered %d: %s, want a 200 with the late document", r.status, r.body)
	}
}

// TestSearchRequeriesShardWhoseFetchFails: the copy a shard's hits came from loses
// their pinned generation before the fetch, twice running (a rolling restart taking
// one copy after another). The search runs that shard's query phase again, and only
// that shard's, within what is left of the request's timeout, fetches again only that
// shard's bodies, and answers with the hits, order, total and aggregations an
// undisturbed search gives.
func TestSearchRequeriesShardWhoseFetchFails(t *testing.T) {
	var c *cluster
	probe := &requeryProbe{}
	c = newCluster(t, sqliteDB(t), func(i int, o *Options) {
		o.Transport = &peerProbe{base: http.DefaultTransport.(*http.Transport).Clone(), p: probe} //nolint:forcetypeassert,errcheck // the default transport is an *http.Transport
		o.hooks.pinned = func(id store.ShardID, pin string) {
			if probe.pinned(id.Shard, pin) {
				c.node(i).n.pins.remove(pin)
			}
		}
	})
	a := c.start(0)
	c.start(1)
	createIndex(t, a.n, "rq", 4, 1)
	waitCopies(t, a.st, "rq", 4, 1, time.Minute)
	var last int64
	for k := range 20 {
		last = mustWrite(t, a.n, "rq", upsertOp(fmt.Sprintf("q%02d", k), k))
	}
	var coord *tnode
	var remote []int
	for _, tn := range c.live() {
		var lacks []int
		for s := range 4 {
			if _, ok := tn.n.Hosted(store.ShardID{Index: "rq", Shard: s}); !ok {
				lacks = append(lacks, s)
			}
		}
		if len(lacks) > len(remote) {
			coord, remote = tn, lacks
		}
	}
	if coord == nil {
		t.Fatal("every node hosts every shard: no search reads a peer")
	}

	bound := func(v float64) *float64 { return &v }
	aggs := map[string]search.Agg{
		"brands":   {Type: search.AggTerms, Field: "brand"},
		"prices":   {Type: search.AggStats, Field: "price"},
		"distinct": {Type: search.AggCardinality, Field: "brand"},
		"spread":   {Type: search.AggHistogram, Field: "price", Interval: 5},
		"bands":    {Type: search.AggRange, Field: "price", Ranges: []search.Range{{To: bound(5)}, {From: bound(5), To: bound(12)}, {From: bound(12)}}},
	}
	for _, tc := range []struct {
		r           search.Request
		mustRequery bool
	}{
		{search.Request{Query: &query.All{}, Size: 20, TrackTotal: search.TrackTotalAll, Aggs: aggs, Timeout: time.Minute}, true},
		{search.Request{Query: &query.All{}, Size: 3, Sort: []search.SortField{{Field: "price", Desc: true}}, TrackTotal: 5, Aggs: aggs}, false},
	} {
		base := tc.r
		want, err := coord.n.Search(tctx(t), "rq", &base, api.ReadOptions{WaitForSeq: last})
		if err != nil {
			t.Fatal(err)
		}
		winners := map[int]bool{}
		for _, h := range want.Hits {
			winners[node.ShardFor(h.ID, 4)] = true
		}
		drop := remote[0]
		for _, s := range remote {
			if winners[s] {
				drop = s
				break
			}
		}
		if tc.mustRequery && !winners[drop] {
			t.Fatalf("no shard read from a peer has a winning hit (remote %v)", remote)
		}
		probe.start(drop)
		r := tc.r
		got, err := coord.n.Search(tctx(t), "rq", &r, api.ReadOptions{WaitForSeq: last})
		pins, fetches, timeouts := probe.stop()
		if err != nil {
			t.Fatalf("search with shard %d's pins lost twice: %v", drop, err)
		}
		wantPins, wantFetches := map[int]int{}, map[int]int{}
		for _, s := range remote {
			wantPins[s] = 1
			if winners[s] {
				wantFetches[s] = 1
			}
		}
		if winners[drop] {
			wantPins[drop], wantFetches[drop] = 3, 3
		}
		if !maps.Equal(pins, wantPins) || !maps.Equal(fetches, wantFetches) {
			t.Fatalf("query phases per shard %v, fetches %v; want %v and %v (shard %d losing its pins twice)", pins, fetches, wantPins, wantFetches, drop)
		}
		for k, d := range timeouts[drop] {
			switch {
			case tc.r.Timeout == 0 && d != 0:
				t.Fatalf("query phase %d of shard %d ran with a timeout of %s; the request set none", k, drop, d)
			case tc.r.Timeout > 0 && k == 0 && d != tc.r.Timeout:
				t.Fatalf("the first query phase of shard %d ran with a timeout of %s, want %s", drop, d, tc.r.Timeout)
			case tc.r.Timeout > 0 && k > 0 && (d <= 0 || d >= timeouts[drop][k-1]):
				t.Fatalf("query phase %d of shard %d ran with a timeout of %s after %v: not what was left of the request's", k, drop, d, timeouts[drop][:k])
			}
		}
		if got.Total != want.Total || got.TotalRelation != want.TotalRelation {
			t.Fatalf("total %d %s, want %d %s", got.Total, got.TotalRelation, want.Total, want.TotalRelation)
		}
		if len(got.Hits) != len(want.Hits) {
			t.Fatalf("%d hits, want %d", len(got.Hits), len(want.Hits))
		}
		for i := range want.Hits {
			if got.Hits[i].ID != want.Hits[i].ID || !bytes.Equal(got.Hits[i].Body, want.Hits[i].Body) {
				t.Fatalf("hit %d is %s %s, want %s %s", i, got.Hits[i].ID, got.Hits[i].Body, want.Hits[i].ID, want.Hits[i].Body)
			}
		}
		gotAggs, err := json.Marshal(got.Aggs)
		if err != nil {
			t.Fatal(err)
		}
		wantAggs, err := json.Marshal(want.Aggs)
		if err != nil {
			t.Fatal(err)
		}
		if len(want.Aggs) != len(aggs) || !bytes.Equal(gotAggs, wantAggs) {
			t.Fatalf("aggregations %s, want %s", gotAggs, wantAggs)
		}
	}
}

// requeryProbe counts, while on, the query phases each shard's peer pinned, the
// timeouts they were sent with and the fetches sent for them, and drops the first two
// pins of one shard.
type requeryProbe struct {
	mu       sync.Mutex
	on       bool
	drop     int
	pins     map[int]int
	fetches  map[int]int
	timeouts map[int][]time.Duration
	shardOf  map[string]int
}

func (p *requeryProbe) start(drop int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.on, p.drop = true, drop
	p.pins, p.fetches, p.timeouts, p.shardOf = map[int]int{}, map[int]int{}, map[int][]time.Duration{}, map[string]int{}
}

func (p *requeryProbe) stop() (pins, fetches map[int]int, timeouts map[int][]time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.on = false
	return p.pins, p.fetches, p.timeouts
}

func (p *requeryProbe) searched(s int, timeout time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.on {
		p.timeouts[s] = append(p.timeouts[s], timeout)
	}
}

// pinned counts a pin of shard s and reports whether to drop it.
func (p *requeryProbe) pinned(s int, pin string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.on {
		return false
	}
	p.pins[s]++
	p.shardOf[pin] = s
	return s == p.drop && p.pins[s] <= 2
}

func (p *requeryProbe) fetched(pin string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.shardOf[pin]; ok && p.on {
		p.fetches[s]++
	}
}

// peerProbe tells p of every query phase and fetch sent to a peer.
type peerProbe struct {
	base http.RoundTripper
	p    *requeryProbe
}

func (pp *peerProbe) RoundTrip(r *http.Request) (*http.Response, error) {
	querying, fetching := r.URL.Path == peerPrefix+"search", r.URL.Path == peerPrefix+"fetch"
	if (!querying && !fetching) || r.Body == nil {
		return pp.base.RoundTrip(r)
	}
	b, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		return nil, err
	}
	if querying {
		var msg searchMsg
		if json.Unmarshal(b, &msg) == nil && msg.Request != nil {
			pp.p.searched(msg.Shard, time.Duration(msg.Request.TimeoutNs))
		}
	} else {
		var msg fetchMsg
		if json.Unmarshal(b, &msg) == nil {
			pp.p.fetched(msg.Pin)
		}
	}
	c := r.Clone(r.Context())
	c.Body = io.NopCloser(bytes.NewReader(b))
	return pp.base.RoundTrip(c)
}
