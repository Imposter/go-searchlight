package cluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
)

func nestedNode(t *testing.T, raw string) query.Node {
	t.Helper()
	q, ps := query.Parse([]byte(raw))
	if len(ps) > 0 {
		t.Fatal(ps)
	}
	return q
}

const nestedL = `{"nested": {"path": "_v", "query": {"all": [{"field": "_v.size", "op": "eq", "value": "L"}, {"field": "_v.price", "op": "lt", "value": 10}]}, "inner_hits": {"size": 1, "fields": ["_v._key"]}}}`

// TestWireNested: a nested node anywhere in a search, an aggregation's where, a sort's
// mode or where, and a request ValidateQueries marked as reading a nested field's
// objects need search wire level 6, and cross the wire as they were; a percolation
// under a mapping with a nested field needs percolate wire level 3.
func TestWireNested(t *testing.T) {
	n := nestedNode(t, nestedL)
	where := nestedNode(t, `{"field": "_v.size", "op": "eq", "value": "L"}`)
	for name, tc := range map[string]struct {
		r     *search.Request
		needs int
	}{
		"query":       {&search.Request{Query: &query.Not{Child: n}}, levelNested},
		"post_filter": {&search.Request{Query: &query.All{}, PostFilter: n}, levelNested},
		"filter":      {&search.Request{Query: &query.All{}, Aggs: map[string]search.Agg{"f": {Type: search.AggFilter, Filter: n}}}, levelNested},
		"agg where":   {&search.Request{Query: &query.All{}, Aggs: map[string]search.Agg{"t": {Type: search.AggTerms, Field: "_v.size", Where: where}}}, levelNested},
		"sort where":  {&search.Request{Query: &query.All{}, Sort: []search.SortField{{Field: "_v.price", Where: where}}}, levelNested},
		"sort mode":   {&search.Request{Query: &query.All{}, Sort: []search.SortField{{Field: "_v.price", Mode: search.SortModeMax}}}, levelNested},
		"marked":      {&search.Request{Query: &query.All{}, Nested: true}, levelNested},
		"none":        {&search.Request{Query: &query.All{}, Sort: []search.SortField{{Field: "price"}}}, 0},
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
		r, err := back.decode()
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if wr.Needs != tc.needs || back.Needs != tc.needs {
			t.Errorf("%s: needs %d, %d after the wire; want %d", name, wr.Needs, back.Needs, tc.needs)
		}
		if !bytes.Equal(query.Canonical(r.Query), query.Canonical(tc.r.Query)) {
			t.Errorf("%s: the query changed on the wire", name)
		}
		for k, a := range tc.r.Aggs {
			if (a.Where == nil) != (r.Aggs[k].Where == nil) || (a.Where != nil && !bytes.Equal(query.Canonical(a.Where), query.Canonical(r.Aggs[k].Where))) {
				t.Errorf("%s: aggregation %s's where changed on the wire", name, k)
			}
		}
		for i, f := range tc.r.Sort {
			g := r.Sort[i]
			if g.Mode != f.Mode || (f.Where == nil) != (g.Where == nil) || (f.Where != nil && !bytes.Equal(query.Canonical(f.Where), query.Canonical(g.Where))) {
				t.Errorf("%s: sort key %d changed on the wire: %+v", name, i, g)
			}
		}
	}
	if q, ok := nestedNode(t, nestedL).(*query.Nested); !ok || q.InnerHits == nil {
		t.Fatal("no inner hits")
	}
	if levelNeeds(levelNested) == levelNeeds(levelBox) {
		t.Error("level 6's errors name level 5's needs")
	}
	for mapping, want := range map[string]int{
		`{"fields": {"_v": {"type": "nested", "fields": {"seen": "date"}}, "p": {"type": "point", "x": "number", "y": "number"}}}`: percolateNested,
		`{"fields": {"_v": {"type": "nested", "fields": {}}}}`:                                                                     percolateNested,
		`{"fields": {"p": {"type": "point", "x": "number", "y": "number"}, "d": "date"}}`:                                          percolatePoints,
	} {
		if got := percolateNeeds(json.RawMessage(mapping)); got != want {
			t.Errorf("%s needs percolate level %d, want %d", mapping, got, want)
		}
	}
	if percolateLevel != percolateNested {
		t.Error("percolate level 3 is not the newest")
	}
}

// nestedCluster is two nodes holding one copy each of a four-shard index with a nested
// field, one saved nested query and twenty products with variants, every node playing
// older as older and level say; it returns a node lacking a copy of some shard, and the
// last write's seq.
func nestedCluster(t *testing.T, older olderPeer, level int) (*tnode, int64) {
	t.Helper()
	c := newCluster(t, sqliteDB(t), func(_ int, o *Options) {
		o.hooks.olderPeer, o.hooks.olderLevel = older, level
	})
	a := c.start(0)
	c.start(1)
	spec := api.IndexSpec{
		Mapping: &schema.Mapping{Fields: map[string]schema.FieldType{
			"_v": schema.Nested, "_v._key": schema.Keyword, "_v.size": schema.Keyword, "_v.price": schema.Number, "price": schema.Number,
		}},
		Settings: api.IndexSettings{Shards: 4, ReplicasPerShard: 1},
	}
	if _, err := a.n.CreateIndex(tctx(t), "nw", spec); err != nil {
		t.Fatal(err)
	}
	waitCopies(t, a.st, "nw", 4, 1, time.Minute)
	ops := []api.WriteOp{{Kind: api.OpQueryUpsert, ID: "q", Query: json.RawMessage(nestedL)}}
	for k := range 20 {
		// Product k: variant 0 is size L at price k, variant 1 size M at price 0.
		body := fmt.Sprintf(`{"price": %d, "_v": [{"_key": "p%02d-0", "size": "L", "price": %d}, {"_key": "p%02d-1", "size": "M", "price": 0}]}`, k, k, k, k)
		ops = append(ops, api.WriteOp{Kind: api.OpUpsert, ID: fmt.Sprintf("p%02d", k), Body: json.RawMessage(body)})
	}
	seq := mustWrite(t, a.n, "nw", ops...)
	coord, _ := lacking(t, c.live(), "nw")
	if err := coord.n.SyncCatalog(tctx(t)); err != nil {
		t.Fatal(err)
	}
	return coord, seq
}

func percolateNestedDoc(t *testing.T, coord *tnode, seq int64) (string, error) {
	t.Helper()
	res, err := coord.n.Percolate(tctx(t), "nw", &api.PercolateRequest{Docs: []json.RawMessage{
		json.RawMessage(`{"_v": [{"size": "L", "price": 50}, {"size": "M", "price": 1}]}`),
		json.RawMessage(`{"_v": [{"size": "L", "price": 5}]}`),
	}}, api.ReadOptions{WaitForSeq: seq})
	if err != nil {
		return "", err
	}
	out := ""
	for _, r := range res.Results {
		out += string(r.Queries)
	}
	return out, nil
}

// Nested searches (inner hits, an aggregation's where, a sort on a sub-field) and
// percolations of a nested index run on peers as on the node itself.
func TestNestedThroughPeers(t *testing.T) {
	coord, seq := nestedCluster(t, olderNone, 0)
	where := nestedNode(t, `{"field": "_v.size", "op": "eq", "value": "L"}`)
	r := &search.Request{
		Query: nestedNode(t, nestedL), TrackTotal: search.TrackTotalAll, Size: 20, Fields: []string{"price"},
		Sort: []search.SortField{{Field: "_v.price", Desc: true, Where: where}},
		Aggs: map[string]search.Agg{
			"sizes": {Type: search.AggTerms, Field: "_v.size"},
			"top":   {Type: search.AggMax, Field: "_v.price", Where: where},
		},
	}
	res, err := coord.n.Search(tctx(t), "nw", r, api.ReadOptions{WaitForSeq: seq})
	if err != nil || res.Total != 10 || len(res.Hits) != 10 {
		t.Fatalf("a nested search through a peer: %+v, %v; want 10", res, err)
	}
	if res.Hits[0].ID != "p09" || res.Hits[0].Sort[0] != 9.0 {
		t.Fatalf("the first hit by L's price, descending: %s %v", res.Hits[0].ID, res.Hits[0].Sort)
	}
	ih := res.Hits[0].InnerHits["_v"]
	if ih == nil || ih.Total != 1 || len(ih.Offsets) != 1 || ih.Offsets[0] != 0 || string(ih.Objects[0]) != `{"_key":"p09-0"}` {
		t.Fatalf("p09's inner hits: %+v", ih)
	}
	if string(res.Hits[0].Body) != `{"price":9}` {
		t.Fatalf("p09's body, limited to price: %s", res.Hits[0].Body)
	}
	sizes := res.Aggs["sizes"]
	if len(sizes.Buckets) != 2 || sizes.Buckets[0].DocCount != 10 || sizes.Buckets[1].DocCount != 10 {
		t.Fatalf("sizes: %+v", sizes.Buckets)
	}
	if top := res.Aggs["top"].Number; top == nil || *top != 9 {
		t.Fatalf("the greatest L price: %v", top)
	}
	if got, err := percolateNestedDoc(t, coord, seq); err != nil || got != `["q"]` {
		t.Fatalf("a percolation of a nested index through a peer: %s, %v", got, err)
	}
}

// A peer of the release before nested fields is not trusted with a search that reads
// a nested field's objects (a nested node, or an aggregation on a sub-field with no
// where), nor with a percolation of a nested index: with no other copy each is
// unavailable (503).
func TestOlderPeerRefusedForNested(t *testing.T) {
	unavailable := func(t *testing.T, what string, err error) {
		t.Helper()
		var ae *api.Error
		if !errors.As(err, &ae) || ae.Status != http.StatusServiceUnavailable {
			t.Fatalf("%s through an older peer: %v; want a 503", what, err)
		}
	}
	for name, older := range map[string]olderPeer{"no endpoint": olderNoEndpoint, "drops the level": olderDropsLevel} {
		t.Run(name, func(t *testing.T) {
			coord, seq := nestedCluster(t, older, levelBox)
			if res, err := coord.n.Search(tctx(t), "nw", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitForSeq: seq}); err != nil || res.Total != 20 {
				t.Fatalf("a level-0 search through a level-5 peer: %v, %v", res, err)
			}
			_, err := coord.n.Search(tctx(t), "nw", &search.Request{Query: nestedNode(t, nestedL)}, api.ReadOptions{WaitForSeq: seq})
			unavailable(t, "a nested search", err)
			_, err = coord.n.Search(tctx(t), "nw", &search.Request{Query: &query.All{}, Aggs: map[string]search.Agg{"t": {Type: search.AggTerms, Field: "_v.size"}}}, api.ReadOptions{WaitForSeq: seq})
			unavailable(t, "an aggregation on a sub-field", err)
			_, err = coord.n.Search(tctx(t), "nw", &search.Request{Query: &query.All{}, Sort: []search.SortField{{Field: "_v.price"}}}, api.ReadOptions{WaitForSeq: seq})
			unavailable(t, "a sort on a sub-field", err)

			coord, seq = nestedCluster(t, older, percolatePoints)
			_, err = percolateNestedDoc(t, coord, seq)
			unavailable(t, "a percolation of a nested index", err)
		})
	}
}

// nestedSubAggs is a terms aggregation on a sub-field with sub-aggregations of both
// kinds: the bucket's documents' price, and its objects' prices.
func nestedSubAggs() map[string]search.Agg {
	return map[string]search.Agg{"sizes": {Type: search.AggTerms, Field: "_v.size", Aggs: map[string]search.Agg{
		"lowest": {Type: search.AggMin, Field: "price"},
		"prices": {Type: search.AggStats, Field: "_v.price"},
		"by":     {Type: search.AggTerms, Field: "price", Size: 3},
	}}}
}

// A request ValidateQueries marked as holding sub-aggregations under a terms on a
// sub-field needs search wire level 7, the newest, and its sub-aggregations cross the
// wire; through a peer they answer as on the node itself, and a peer of level 6 is
// not trusted with them (503 with no other copy).
func TestWireNestedSubAggs(t *testing.T) {
	r := &search.Request{Query: &query.All{}, Aggs: nestedSubAggs(), Nested: true, NestedSubAggs: true}
	wr, err := encodeRequest(r)
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
	got, err := back.decode()
	if err != nil {
		t.Fatal(err)
	}
	if wr.Needs != levelNestedSubAggs || len(got.Aggs["sizes"].Aggs) != 3 {
		t.Fatalf("needs %d, sub-aggregations %v", wr.Needs, got.Aggs["sizes"].Aggs)
	}
	if searchLevel != levelNestedSubAggs || levelNeeds(levelNestedSubAggs) == levelNeeds(levelNested) {
		t.Error("level 7 is not the newest search level, or its errors name level 6's needs")
	}

	coord, seq := nestedCluster(t, olderNone, 0)
	res, err := coord.n.Search(tctx(t), "nw", &search.Request{Query: &query.All{}, Aggs: nestedSubAggs()}, api.ReadOptions{WaitForSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range res.Aggs["sizes"].Buckets {
		lowest, prices, by := b.Aggs["lowest"], b.Aggs["prices"], b.Aggs["by"]
		switch b.Key {
		case "l":
			if b.DocCount != 20 || *lowest.Number != 0 || prices.Count != 20 || prices.Sum != 190 || len(by.Buckets) != 3 || by.SumOtherDocCount != 17 {
				t.Fatalf("L: %d, lowest %v, prices %+v, by %+v", b.DocCount, *lowest.Number, prices, by)
			}
		case "m":
			if b.DocCount != 20 || *lowest.Number != 0 || prices.Count != 20 || prices.Sum != 0 {
				t.Fatalf("M: %d, lowest %v, prices %+v", b.DocCount, *lowest.Number, prices)
			}
		default:
			t.Fatalf("bucket %v", b.Key)
		}
	}
	for name, older := range map[string]olderPeer{"no endpoint": olderNoEndpoint, "drops the level": olderDropsLevel} {
		t.Run(name, func(t *testing.T) {
			coord, seq := nestedCluster(t, older, levelNested)
			if _, err := coord.n.Search(tctx(t), "nw", &search.Request{Query: &query.All{}, Aggs: map[string]search.Agg{"t": {Type: search.AggTerms, Field: "_v.size"}}}, api.ReadOptions{WaitForSeq: seq}); err != nil {
				t.Fatalf("a level-6 search through a level-6 peer: %v", err)
			}
			_, err := coord.n.Search(tctx(t), "nw", &search.Request{Query: &query.All{}, Aggs: nestedSubAggs()}, api.ReadOptions{WaitForSeq: seq})
			var ae *api.Error
			if !errors.As(err, &ae) || ae.Status != http.StatusServiceUnavailable {
				t.Fatalf("nested sub-aggregations through an older peer: %v; want a 503", err)
			}
		})
	}
}
