package cluster

import (
	"encoding/json"
	"fmt"
	"strings"
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
