package node_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// wallClock is the real clock with a wall time the test sets: each Wall reads the next
// of its times, then keeps to the last. The timers stay real.
type wallClock struct {
	clock.Real
	mu    sync.Mutex
	times []time.Time
}

func (c *wallClock) Wall() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	at := c.times[0]
	if len(c.times) > 1 {
		c.times = c.times[1:]
	}
	return at
}

func (c *wallClock) set(times ...time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.times = times
}

var dateT0 = time.Date(2031, 3, 10, 13, 45, 30, 0, time.UTC)

func days(n int) time.Duration { return time.Duration(n) * 24 * time.Hour }

func dateIndex(t *testing.T, wall *wallClock) *node.Single {
	t.Helper()
	cfg := testConfig(t)
	n := open(t, cfg, openStore(t, cfg), func(o *node.Options) { o.Clock = wall })
	spec := api.IndexSpec{
		Mapping:  &schema.Mapping{Fields: map[string]schema.FieldType{"seen": schema.Date, "brand": schema.Keyword}},
		Settings: api.IndexSettings{Shards: 4},
	}
	if _, err := n.CreateIndex(ctx(t), "d", spec); err != nil {
		t.Fatal(err)
	}
	waitServing(t, n)
	return n
}

// A search resolves its date math at the node's wall time, so the same query answers
// anew once the clock moves: no cached leaf (the filter cache keys a leaf by its
// resolved bounds) answers for an earlier now, rounded or not.
func TestSearchDateMathFollowsTheClock(t *testing.T) {
	wall := &wallClock{times: []time.Time{dateT0}}
	n := dateIndex(t, wall)
	var ops []api.WriteOp
	for d := range 20 {
		ops = append(ops, upsert(fmt.Sprintf("doc%d", d), fmt.Sprintf(`{"seen": %q}`, dateT0.Add(-days(d)).Format(time.RFC3339))))
	}
	seq := mustWrite(t, n, "d", ops...).Seq
	week := mustParseQuery(t, `{"field": "seen", "op": "gte", "value": "now-7d/d"}`)
	exact := mustParseQuery(t, `{"field": "seen", "op": "gte", "value": "now-7d"}`)
	for _, tc := range []struct {
		at          time.Time
		week, exact int64
	}{
		{dateT0, 8, 8},
		{dateT0.Add(time.Hour), 8, 7},
		{dateT0.Add(days(3)), 5, 5},
		{dateT0.Add(days(30)), 0, 0},
		{dateT0, 8, 8},
	} {
		wall.set(tc.at)
		for range 4 { // past the usage sketch, into the filter cache
			if got := count(t, n, "d", week, seq); got != tc.week {
				t.Fatalf("at %s: gte now-7d/d counts %d, want %d", tc.at, got, tc.week)
			}
			if got := count(t, n, "d", exact, seq); got != tc.exact {
				t.Fatalf("at %s: gte now-7d counts %d, want %d", tc.at, got, tc.exact)
			}
		}
	}
}

// A request reads the wall clock once: every shard of a search, a percolation and a
// bulk percolation resolves at that one instant, though the clock reads otherwise on
// any later call.
func TestDateMathOneNowPerRequest(t *testing.T) {
	wall := &wallClock{times: []time.Time{dateT0}}
	n := dateIndex(t, wall)
	var ops []api.WriteOp
	for i := range 16 {
		ops = append(ops, api.WriteOp{Kind: api.OpQueryUpsert, ID: fmt.Sprintf("q%02d", i), Query: json.RawMessage(`{"field": "seen", "op": "between", "value": ["now-1d", "now+1d"]}`)})
	}
	for i := range 16 {
		ops = append(ops, upsert(fmt.Sprintf("doc%02d", i), fmt.Sprintf(`{"seen": %q}`, dateT0.Format(time.RFC3339))))
	}
	seq := mustWrite(t, n, "d", ops...).Seq
	later := dateT0.Add(days(30))
	all := make([]string, 16)
	for i := range all {
		all[i] = fmt.Sprintf("%q", fmt.Sprintf("q%02d", i))
	}
	want := "[" + strings.Join(all, ",") + "]"

	wall.set(dateT0, later)
	res, err := n.Percolate(ctx(t), "d", &api.PercolateRequest{Docs: []json.RawMessage{json.RawMessage(fmt.Sprintf(`{"seen": %q}`, dateT0.Format(time.RFC3339)))}}, api.ReadOptions{WaitForSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(res.Results[0].Queries); got != want {
		t.Fatalf("percolate: %s", got)
	}

	wall.set(dateT0, later)
	if got := count(t, n, "d", mustParseQuery(t, `{"field": "seen", "op": "between", "value": ["now-1d", "now+1d"]}`), seq); got != 16 {
		t.Fatalf("search counts %d", got)
	}

	wall.set(dateT0, later)
	wr, err := n.Write(ctx(t), "d", []api.WriteOp{upsert("new", fmt.Sprintf(`{"seen": %q}`, dateT0.Format(time.RFC3339)))}, api.WriteOptions{Percolate: true, WaitForSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(wr.Items[0].Queries); !wr.Percolated || got != want {
		t.Fatalf("bulk percolate: %v %s", wr.Percolated, got)
	}
}

func mustParseQuery(t *testing.T, raw string) query.Node {
	t.Helper()
	q, problems := query.Parse([]byte(raw))
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	return q
}
