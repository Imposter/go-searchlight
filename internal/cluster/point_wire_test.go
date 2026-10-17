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

func boxNode(t *testing.T) query.Node {
	t.Helper()
	q, ps := query.Parse([]byte(`{"field": "spot", "op": "box", "value": {"x": {"gte": 5}, "y": {"lt": 9}}}`))
	if len(ps) > 0 {
		t.Fatal(ps)
	}
	return q
}

// TestWireBox: a box condition anywhere in a search (its query, post_filter, a filter
// or filters aggregation's queries, at any depth) needs search wire level 5, and
// crosses the wire as it was; a percolation under a mapping with a point field needs
// percolate wire level 2, one with a date field (and no point) level 1.
func TestWireBox(t *testing.T) {
	box := boxNode(t)
	nested := &query.All{Children: []query.Node{&query.Not{Child: &query.Any{Children: []query.Node{box}}}}}
	for name, tc := range map[string]struct {
		r     *search.Request
		needs int
	}{
		"query":       {&search.Request{Query: nested}, levelBox},
		"post_filter": {&search.Request{Query: &query.All{}, PostFilter: box}, levelBox},
		"filter": {&search.Request{Query: &query.All{}, Aggs: map[string]search.Agg{"g": {Type: search.AggGlobal, Aggs: map[string]search.Agg{
			"f": {Type: search.AggFilter, Filter: box},
		}}}}, levelBox},
		"filters": {&search.Request{Query: &query.All{}, Aggs: map[string]search.Agg{"f": {Type: search.AggFilters, Filters: map[string]query.Node{"b": box}}}}, levelBox},
		"none":    {&search.Request{Query: &query.All{}}, 0},
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
			t.Fatal(err)
		}
		if wr.Needs != tc.needs || back.Needs != tc.needs {
			t.Errorf("%s: needs %d, %d after the wire; want %d", name, wr.Needs, back.Needs, tc.needs)
		}
		if !bytes.Equal(query.Canonical(r.Query), query.Canonical(tc.r.Query)) {
			t.Errorf("%s: the query changed on the wire", name)
		}
	}
	if levelNeeds(levelBox) == levelNeeds(levelMetrics) || searchLevel != levelBox {
		t.Error("level 5 is not the newest search level, or its errors name level 4's needs")
	}
	for mapping, want := range map[string]int{
		`{"fields": {"p": {"type": "point", "x": "date", "y": "number"}, "d": "date"}}`: percolatePoints,
		`{"fields": {"p": {"type": "point", "x": "number", "y": "number"}}}`:            percolatePoints,
		`{"fields": {"d": "date", "n": "number"}}`:                                      percolateDates,
		`{"fields": {"n": "number"}}`:                                                   0,
		`{"fields": {"n": "nonsense"}}`:                                                 percolateLevel,
	} {
		if got := percolateNeeds(json.RawMessage(mapping)); got != want {
			t.Errorf("%s needs percolate level %d, want %d", mapping, got, want)
		}
	}
}

// pointCluster is two nodes holding one copy each of a four-shard index with a point
// field, one saved box query and twenty documents, every node playing older as older
// and level say; it returns a node lacking a copy of some shard, and the last write's
// seq.
func pointCluster(t *testing.T, older olderPeer, level int) (*tnode, int64) {
	t.Helper()
	c := newCluster(t, sqliteDB(t), func(_ int, o *Options) {
		o.hooks.olderPeer, o.hooks.olderLevel = older, level
	})
	a := c.start(0)
	c.start(1)
	spec := api.IndexSpec{
		Mapping:  &schema.Mapping{Fields: map[string]schema.FieldType{"spot": schema.PointType(schema.Number, schema.Number), "price": schema.Number}},
		Settings: api.IndexSettings{Shards: 4, ReplicasPerShard: 1},
	}
	if _, err := a.n.CreateIndex(tctx(t), "pw", spec); err != nil {
		t.Fatal(err)
	}
	waitCopies(t, a.st, "pw", 4, 1, time.Minute)
	ops := []api.WriteOp{{Kind: api.OpQueryUpsert, ID: "q", Query: json.RawMessage(`{"field": "spot", "op": "box", "value": {"x": {"gte": 5}, "y": {"lt": 9}}}`)}}
	for k := range 20 {
		ops = append(ops, api.WriteOp{Kind: api.OpUpsert, ID: fmt.Sprintf("p%02d", k), Body: json.RawMessage(fmt.Sprintf(`{"spot": [[%d, %d], [0, 0]], "price": %d}`, k, k%10, k))})
	}
	seq := mustWrite(t, a.n, "pw", ops...)
	coord, _ := lacking(t, c.live(), "pw")
	if err := coord.n.SyncCatalog(tctx(t)); err != nil {
		t.Fatal(err)
	}
	return coord, seq
}

func percolatePoint(t *testing.T, coord *tnode, seq int64) (string, error) {
	t.Helper()
	res, err := coord.n.Percolate(tctx(t), "pw", &api.PercolateRequest{Docs: []json.RawMessage{json.RawMessage(`{"spot": [7, 1]}`)}, IDs: []string{"p03", "p07"}}, api.ReadOptions{WaitForSeq: seq})
	if err != nil {
		return "", err
	}
	out := ""
	for _, r := range res.Results {
		out += string(r.Queries)
	}
	return out, nil
}

// Box searches and percolations of a point index run on peers as on the node itself:
// a box counts a document for a point inside on both axes, from every shard.
func TestBoxThroughPeers(t *testing.T) {
	coord, seq := pointCluster(t, olderNone, 0)
	r := &search.Request{
		Query: boxNode(t), TrackTotal: search.TrackTotalAll,
		Aggs: map[string]search.Agg{"f": {Type: search.AggFilter, Filter: boxNode(t)}},
	}
	res, err := coord.n.Search(tctx(t), "pw", r, api.ReadOptions{WaitForSeq: seq})
	if err != nil || res.Total != 13 || res.Aggs["f"].DocCount != 13 {
		t.Fatalf("a box search through a peer: %+v, %v; want 13", res, err)
	}
	if got, err := percolatePoint(t, coord, seq); err != nil || got != `["q"]["q"]` {
		t.Fatalf("a percolation of a point index through a peer: %s, %v", got, err)
	}
}

// A peer of an older release (no endpoint for the level, or answering below it) is
// not trusted with a box search, nor with a percolation of an index with a point
// field: with no other copy each is unavailable (503) rather than refused as invalid
// or answered without its boxes. What the older peer speaks is answered as before.
func TestOlderPeerRefusedForBox(t *testing.T) {
	unavailable := func(t *testing.T, what string, err error) {
		t.Helper()
		var ae *api.Error
		if !errors.As(err, &ae) || ae.Status != http.StatusServiceUnavailable {
			t.Fatalf("%s through an older peer: %v; want a 503", what, err)
		}
	}
	for name, older := range map[string]olderPeer{"no endpoint": olderNoEndpoint, "drops the level": olderDropsLevel} {
		t.Run(name, func(t *testing.T) {
			coord, seq := pointCluster(t, older, levelMetrics)
			if res, err := coord.n.Search(tctx(t), "pw", &search.Request{
				Query: &query.All{}, TrackTotal: search.TrackTotalAll,
				Aggs: map[string]search.Agg{"lo": {Type: search.AggMin, Field: "price"}},
			}, api.ReadOptions{WaitForSeq: seq}); err != nil || res.Total != 20 {
				t.Fatalf("a level-4 search through a level-4 peer: %v, %v", res, err)
			}
			_, err := coord.n.Search(tctx(t), "pw", &search.Request{Query: boxNode(t), TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitForSeq: seq})
			unavailable(t, "a box search", err)
			_, err = coord.n.Search(tctx(t), "pw", &search.Request{Query: &query.All{}, Aggs: map[string]search.Agg{"f": {Type: search.AggFilter, Filter: boxNode(t)}}}, api.ReadOptions{WaitForSeq: seq})
			unavailable(t, "a box filter aggregation", err)

			coord, seq = pointCluster(t, older, percolateDates)
			_, err = percolatePoint(t, coord, seq)
			unavailable(t, "a percolation of a point index", err)
			if _, err := coord.n.Search(tctx(t), "pw", &search.Request{Query: boxNode(t)}, api.ReadOptions{WaitForSeq: seq}); err == nil {
				t.Fatal("a box search through a level-1 peer answered")
			}
		})
	}
}
