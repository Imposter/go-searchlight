package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/store"
)

// TestWireFrom: from crosses the wire, and a search with a from past 0 and hits to
// return needs search wire level 3; one with from 0, or size 0, needs nothing new and
// carries no from, as a 1.x peer reads it.
func TestWireFrom(t *testing.T) {
	for _, tc := range []struct {
		r     *search.Request
		needs int
	}{
		{&search.Request{Query: &query.All{}, Size: 10, From: 40}, levelFrom},
		{&search.Request{Query: &query.All{}, Size: 10, From: 40, PostFilter: &query.All{}}, levelFrom},
		{&search.Request{Query: &query.All{}, Size: 10}, 0},
		{&search.Request{Query: &query.All{}, From: 40}, 0},
	} {
		wr, err := encodeRequest(tc.r)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(wr)
		if err != nil {
			t.Fatal(err)
		}
		var back wireRequest
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatal(err)
		}
		r2, err := back.decode()
		if err != nil {
			t.Fatal(err)
		}
		if wr.Needs != tc.needs || back.Needs != tc.needs || r2.From != tc.r.From {
			t.Errorf("from %d size %d: needs %d, %d after the wire, from %d; want needs %d", tc.r.From, tc.r.Size, wr.Needs, back.Needs, r2.From, tc.needs)
		}
		if hasFrom := bytes.Contains(raw, []byte(`"from"`)); hasFrom != (tc.r.From > 0) {
			t.Errorf("from %d: the wire carries from: %v (%s)", tc.r.From, hasFrom, raw)
		}
	}
	if levelNeeds(levelFrom) == levelNeeds(levelTermsFilter) {
		t.Errorf("level %d's errors name level %d's needs", levelFrom, levelTermsFilter)
	}
}

// TestFromAcrossNodes: pages by from, read through either node of a two-node cluster
// whose shards are split between them (and an index of one shard on the other node),
// are the slices of the whole sorted result, bodies included, ties broken by _id; and
// search_after from a page's next reads what the next from does.
func TestFromAcrossNodes(t *testing.T) {
	c := newCluster(t, sqliteDB(t), nil)
	a := c.start(0)
	c.start(1)
	createIndex(t, a.n, "pg", 4, 1)
	createIndex(t, a.n, "one", 1, 1)
	waitCopies(t, a.st, "pg", 4, 1, time.Minute)
	waitCopies(t, a.st, "one", 1, 1, time.Minute)
	ops := make([]api.WriteOp, 61)
	for k := range ops {
		ops[k] = api.WriteOp{Kind: api.OpUpsert, ID: fmt.Sprintf("f%02d", k), Body: json.RawMessage(fmt.Sprintf(`{"brand":"b%d","price":%d}`, k%3, k%9))}
	}
	last := map[string]int64{"pg": mustWrite(t, a.n, "pg", ops...), "one": mustWrite(t, a.n, "one", ops...)}
	sorts := []search.SortField{{Field: "price", Desc: true}}
	for _, index := range []string{"pg", "one"} {
		var remote bool
		for _, tn := range c.live() {
			if err := tn.n.SyncCatalog(tctx(t)); err != nil {
				t.Fatal(err)
			}
			for s := range 4 {
				if _, ok := tn.n.Hosted(store.ShardID{Index: index, Shard: s}); !ok && (index == "pg" || s == 0) {
					remote = true
				}
			}
			read := api.ReadOptions{WaitForSeq: last[index]}
			full, err := tn.n.Search(tctx(t), index, &search.Request{Query: &query.All{}, Sort: sorts, Size: 100, TrackTotal: search.TrackTotalAll}, read)
			if err != nil || len(full.Hits) != len(ops) {
				t.Fatalf("%s: the whole result: %v, %v", index, full, err)
			}
			for from := 0; from <= len(ops)+3; from += 7 {
				page, err := tn.n.Search(tctx(t), index, &search.Request{Query: &query.All{}, Sort: sorts, Size: 7, From: from, TrackTotal: search.TrackTotalAll}, read)
				if err != nil {
					t.Fatalf("%s from %d: %v", index, from, err)
				}
				want := full.Hits[min(from, len(full.Hits)):min(from+7, len(full.Hits))]
				if page.Total != int64(len(ops)) || len(page.Hits) != len(want) {
					t.Fatalf("%s from %d: total %d, %d hits; want %d, %d", index, from, page.Total, len(page.Hits), len(ops), len(want))
				}
				for i := range want {
					if page.Hits[i].ID != want[i].ID || !bytes.Equal(page.Hits[i].Body, want[i].Body) {
						t.Fatalf("%s from %d: hit %d is %s %s, want %s %s", index, from, i, page.Hits[i].ID, page.Hits[i].Body, want[i].ID, want[i].Body)
					}
				}
				if page.Next == nil {
					continue
				}
				after, err := tn.n.Search(tctx(t), index, &search.Request{Query: &query.All{}, Sort: sorts, Size: 7, SearchAfter: page.Next}, read)
				if err != nil {
					t.Fatal(err)
				}
				next := full.Hits[min(from+7, len(full.Hits)):min(from+14, len(full.Hits))]
				ids := func(hs []search.Hit) []string {
					out := make([]string, len(hs))
					for i := range hs {
						out[i] = hs[i].ID
					}
					return out
				}
				if !slices.Equal(ids(after.Hits), ids(next)) {
					t.Fatalf("%s: search_after from %d's next reads %v, want %v", index, from, ids(after.Hits), ids(next))
				}
			}
		}
		if !remote {
			t.Fatalf("%s: every node hosts every shard: no search read a peer", index)
		}
	}
}
