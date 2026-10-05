package cluster

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
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

// TestSearchRequeriesShardWhoseFetchFails: the copies a shard's hits came from lose
// their pinned generations before the fetch, twice running (a rolling restart taking
// one copy after another). The search runs that shard's query phase again, reduces
// again and answers with the hits, order and total an undisturbed search gives.
func TestSearchRequeriesShardWhoseFetchFails(t *testing.T) {
	var (
		c        *cluster
		mu       sync.Mutex
		dropping bool
		pinned   = map[store.ShardID]int{}
	)
	c = newCluster(t, sqliteDB(t), func(i int, o *Options) {
		o.hooks.pinned = func(id store.ShardID, pin string) {
			mu.Lock()
			drop := dropping
			if drop {
				pinned[id]++
				drop = pinned[id] <= 2
			}
			mu.Unlock()
			if drop {
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
	for _, tn := range c.live() {
		hosted := 0
		for s := range 4 {
			if _, ok := tn.n.Hosted(store.ShardID{Index: "rq", Shard: s}); ok {
				hosted++
			}
		}
		if hosted < 4 {
			coord = tn
		}
	}
	if coord == nil {
		t.Fatal("every node hosts every shard: no search reads a peer")
	}

	for _, tc := range []struct {
		r search.Request
		// everyShard: every shard has a winning hit to fetch.
		everyShard bool
	}{
		{search.Request{Query: &query.All{}, Size: 20, TrackTotal: search.TrackTotalAll}, true},
		{search.Request{Query: &query.All{}, Size: 3, Sort: []search.SortField{{Field: "price", Desc: true}}, TrackTotal: 5}, false},
	} {
		r := tc.r
		mu.Lock()
		dropping = false
		clear(pinned)
		mu.Unlock()
		base := r
		want, err := coord.n.Search(tctx(t), "rq", &base, api.ReadOptions{WaitForSeq: last})
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		dropping = true
		mu.Unlock()
		got, err := coord.n.Search(tctx(t), "rq", &r, api.ReadOptions{WaitForSeq: last})
		if err != nil {
			t.Fatalf("search with its pins lost twice: %v", err)
		}
		mu.Lock()
		requeried := 0
		for _, n := range pinned {
			if n > 2 {
				requeried++
			}
		}
		mu.Unlock()
		if tc.everyShard && requeried == 0 {
			t.Fatalf("no shard's query phase ran a third time (pins per shard %v)", pinned)
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
	}
}
