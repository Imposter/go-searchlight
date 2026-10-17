package node_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/schema"
)

func pointIndex(t *testing.T, wall *wallClock) *node.Single {
	t.Helper()
	cfg := testConfig(t)
	n := open(t, cfg, openStore(t, cfg), func(o *node.Options) { o.Clock = wall })
	spec := api.IndexSpec{
		Mapping: &schema.Mapping{Dynamic: schema.DynamicStrict, Fields: map[string]schema.FieldType{
			"seen":  schema.PointType(schema.Date, schema.Number),
			"brand": schema.Keyword,
		}},
		Settings: api.IndexSettings{Shards: 4},
	}
	if _, err := n.CreateIndex(ctx(t), "pt", spec); err != nil {
		t.Fatal(err)
	}
	waitServing(t, n)
	return n
}

// Saved box queries keep their date math, and each percolation, given, stored or of
// a bulk write, resolves it at its own now: a document whose one recent point scores
// 5 matches "a score of at least 5 in the last week" until that point is a week old,
// while one whose recent point scores low and whose high score is old never does.
func TestPercolateBoxDateMath(t *testing.T) {
	wall := &wallClock{times: []time.Time{dateT0}}
	n := pointIndex(t, wall)
	at := func(d int) string { return dateT0.Add(-days(d)).Format(time.RFC3339) }
	seq := mustWrite(t, n, "pt",
		api.WriteOp{Kind: api.OpQueryUpsert, ID: "week5", Query: json.RawMessage(`{"field": "seen", "op": "box", "value": {"x": {"gte": "now-7d/d"}, "y": {"gte": 5}}}`)},
		api.WriteOp{Kind: api.OpQueryUpsert, ID: "acme-day", Query: json.RawMessage(`{"all": [{"field": "brand", "op": "eq", "value": "acme"}, {"field": "seen", "op": "box", "value": {"x": {"gt": "now-36h"}}}]}`)},
		api.WriteOp{Kind: api.OpQueryUpsert, ID: "any", Query: json.RawMessage(`{"field": "seen", "op": "exists"}`)},
		api.WriteOp{Kind: api.OpQueryUpsert, ID: "old", Query: json.RawMessage(`{"not": {"field": "seen", "op": "box", "value": {"x": {"gte": "now-30d"}}}}`)},
		upsert("hot", fmt.Sprintf(`{"brand": "acme", "seen": [[%q, 5], [%q, 1]]}`, at(2), at(40))),
		upsert("split", fmt.Sprintf(`{"brand": "acme", "seen": [[%q, 1], [%q, 9]]}`, at(1), at(20))),
	).Seq
	// The expectations at each now, recomputed from the points: hot holds (now-2d, 5)
	// and (now-40d, 1); split holds (now-1d, 1) and (now-20d, 9).
	for _, tc := range []struct {
		later      int
		hot, split string
	}{
		{0, `["any","week5"]`, `["acme-day","any"]`},
		{3, `["any","week5"]`, `["any"]`},
		{6, `["any"]`, `["any"]`},
		{40, `["any","old"]`, `["any","old"]`},
	} {
		now := dateT0.Add(days(tc.later))
		wall.set(now)
		res, err := n.Percolate(ctx(t), "pt", &api.PercolateRequest{IDs: []string{"hot", "split"}}, api.ReadOptions{WaitForSeq: seq})
		if err != nil {
			t.Fatal(err)
		}
		if got := string(res.Results[0].Queries); got != tc.hot {
			t.Errorf("%d days on, hot matches %s, want %s", tc.later, got, tc.hot)
		}
		if got := string(res.Results[1].Queries); got != tc.split {
			t.Errorf("%d days on, split matches %s, want %s", tc.later, got, tc.split)
		}
		wall.set(now)
		given, err := n.Percolate(ctx(t), "pt", &api.PercolateRequest{Docs: []json.RawMessage{json.RawMessage(fmt.Sprintf(`{"brand": "acme", "seen": [%q, 7]}`, now.Format(time.RFC3339)))}}, api.ReadOptions{WaitForSeq: seq})
		if err != nil {
			t.Fatal(err)
		}
		if got := string(given.Results[0].Queries); got != `["acme-day","any","week5"]` {
			t.Errorf("%d days on, a document seen now matches %s", tc.later, got)
		}
	}
	wall.set(dateT0)
	wr, err := n.Write(ctx(t), "pt", []api.WriteOp{upsert("new", fmt.Sprintf(`{"brand": "acme", "seen": [[%q, 6]]}`, at(3)))}, api.WriteOptions{Percolate: true, WaitForSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(wr.Items[0].Queries); !wr.Percolated || got != `["any","week5"]` {
		t.Fatalf("bulk percolate: %v %s", wr.Percolated, got)
	}
}

// A box search resolves its date math at the node's wall time, and counts a document
// only for a point inside on both axes; exists counts every document with the field.
func TestSearchBoxFollowsTheClock(t *testing.T) {
	wall := &wallClock{times: []time.Time{dateT0}}
	n := pointIndex(t, wall)
	var ops []api.WriteOp
	for d := range 20 {
		ops = append(ops, upsert(fmt.Sprintf("doc%d", d), fmt.Sprintf(`{"seen": [[%q, %d], [%q, 0]]}`,
			dateT0.Add(-days(d)).Format(time.RFC3339), d%2, dateT0.Format(time.RFC3339))))
	}
	ops = append(ops, upsert("none", `{"seen": []}`), upsert("bare", `{"brand": "x"}`))
	seq := mustWrite(t, n, "pt", ops...).Seq
	recentOdd := mustParseQuery(t, `{"field": "seen", "op": "box", "value": {"x": {"gte": "now-7d/d"}, "y": {"gte": 1}}}`)
	for _, tc := range []struct {
		at   time.Time
		want int64
	}{
		{dateT0, 4},
		{dateT0.Add(days(3)), 2},
		{dateT0.Add(days(30)), 0},
	} {
		wall.set(tc.at)
		if got := count(t, n, "pt", recentOdd, seq); got != tc.want {
			t.Errorf("at %s: %d documents, want %d", tc.at, got, tc.want)
		}
	}
	if got := count(t, n, "pt", mustParseQuery(t, `{"field": "seen", "op": "exists"}`), seq); got != 21 {
		t.Fatalf("exists counts %d, want 21", got)
	}
}

// A point field is added by a mapping patch, takes its documents' points from then on,
// and cannot be retyped, not even to other axes.
func TestPatchMappingAddsAPointField(t *testing.T) {
	wall := &wallClock{times: []time.Time{dateT0}}
	n := pointIndex(t, wall)
	spot := schema.PointType(schema.Number, schema.Number)
	info, err := n.PatchMapping(ctx(t), "pt", map[string]schema.FieldType{"spot": spot})
	if err != nil || info.Mapping.Fields["spot"] != spot {
		t.Fatalf("patch: %+v, %v", info, err)
	}
	seq := mustWrite(t, n, "pt", upsert("a", `{"spot": [[1, 10], [10, 1]]}`), upsert("b", `{"spot": [6, 6]}`)).Seq
	if got := count(t, n, "pt", mustParseQuery(t, `{"field": "spot", "op": "box", "value": {"x": {"gte": 5}, "y": {"gte": 5}}}`), seq); got != 1 {
		t.Fatalf("the box counts %d, want 1 (the same point on both axes)", got)
	}
	if _, err := n.PatchMapping(ctx(t), "pt", map[string]schema.FieldType{"spot": schema.PointType(schema.Number, schema.Date)}); err == nil ||
		!strings.Contains(err.Error(), "cannot become") {
		t.Fatalf("retyping a point's axes: %v", err)
	}
	cat, err := n.Fields(ctx(t), "pt", 0, api.ReadOptions{WaitForSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(cat)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `{"name":"spot","type":{"type":"point","x":"number","y":"number"}}`) {
		t.Fatalf("the catalogue: %s", raw)
	}
}
