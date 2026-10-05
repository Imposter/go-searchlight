package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/store"
)

// TestWireRoundTrip: a search request and a shard's result survive the peer API's JSON
// exactly: the reduce over a result that went through the wire equals the reduce over
// the original, aggregations (terms, histogram, stats, cardinality) and sort values
// included.
func TestWireRoundTrip(t *testing.T) {
	c := newCluster(t, sqliteDB(t), nil)
	a := c.start(0)
	createIndex(t, a.n, "wire", 1, 0)
	var last int64
	for k := range 300 {
		last = mustWrite(t, a.n, "wire", upsertOp(fmt.Sprintf("w%03d", k), k))
	}
	q, problems := query.Parse([]byte(`{"any":[{"field":"price","op":"between","value":[10,200]},{"field":"tags","op":"has","value":"t1"}]}`))
	if len(problems) > 0 {
		t.Fatal(problems)
	}
	minCount := 1
	r := &search.Request{
		Query: q, Size: 13, TrackTotal: search.TrackTotalAll, Index: "wire",
		Sort:        []search.SortField{{Field: "price", Desc: true}, {Field: "brand"}},
		SearchAfter: []any{float64(250), "b1", "w250"},
		Aggs: map[string]search.Agg{
			"brands": {Type: search.AggTerms, Field: "brand", Size: 3, MinDocCount: &minCount, Aggs: map[string]search.Agg{
				"price": {Type: search.AggStats, Field: "price"},
			}},
			"hist":  {Type: search.AggHistogram, Field: "price", Interval: 50},
			"uniq":  {Type: search.AggCardinality, Field: "brand", Precision: 14},
			"price": {Type: search.AggStats, Field: "price"},
		},
	}
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
	r2, err := back.decode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(query.Canonical(r2.Query), query.Canonical(r.Query)) {
		t.Fatalf("the query changed on the wire")
	}

	tg, err := a.n.LocalTarget(tctx(t), "wire", 0, last)
	if err != nil {
		t.Fatal(err)
	}
	defer tg.Release()
	res, err := tg.Search(tctx(t), r)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := json.Marshal(&searchReply{Result: res})
	if err != nil {
		t.Fatal(err)
	}
	var wired searchReply
	if err := json.Unmarshal(rb, &wired); err != nil {
		t.Fatal(err)
	}
	direct, _ := json.Marshal(search.Reduce([]*search.ShardResult{res}, r))
	via, _ := json.Marshal(search.Reduce([]*search.ShardResult{wired.Result}, r2))
	if !bytes.Equal(direct, via) {
		t.Fatalf("the reduce differs after the wire:\n%s\n%s", direct, via)
	}
}

// TestARSOrder: adaptive replica selection prefers the faster peer, puts a peer that
// just failed after every healthy one until its suspicion decays, and a copy that
// answered stale after fresh ones for staleFor.
func TestARSOrder(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC))
	a := newARS(clk)
	id := store.ShardID{Index: "i", Shard: 0}
	slow, fast, fresh := candidate{node: "slow", shard: id}, candidate{node: "fast", shard: id}, candidate{node: "new", shard: id}
	for range 5 {
		done := a.start(slow.node, slow.key())
		clk.Advance(20 * time.Millisecond)
		done(false, 0.02, 3)
		done = a.start(fast.node, fast.key())
		done(false, 0.001, 0)
	}
	cands := []candidate{slow, fast, fresh}
	a.order(cands, 3)
	if cands[2].node != "slow" {
		t.Fatalf("order %v: the slow peer is not last", cands)
	}
	a.start(fast.node, fast.key())(true, -1, -1) // fast's copy fails
	a.order(cands, 3)
	if cands[2].node != "fast" {
		t.Fatalf("order %v: the failed copy is not last", cands)
	}
	// Suspicion is the copy's: fast's copy of another shard is still first in line.
	other := candidate{node: "fast", shard: store.ShardID{Index: "i", Shard: 1}}
	pair := []candidate{{node: "slow", shard: other.shard}, other}
	a.order(pair, 3)
	if pair[0].node != "fast" {
		t.Fatalf("order %v: a failure of one copy is held against the peer's others", pair)
	}
	// Suspicion grows with each failure in a row, decays after its time, and a success
	// clears it.
	a.start(fast.node, fast.key())(true, -1, -1)
	a.mu.Lock()
	until := clk.Until(a.copies[fast.key()].suspectUntil)
	a.mu.Unlock()
	if until != 2*suspectBase {
		t.Fatalf("after two failures the copy is suspected for %s, want %s", until, 2*suspectBase)
	}
	clk.Advance(until - time.Millisecond)
	a.order(cands, 3)
	if cands[2].node != "fast" {
		t.Fatalf("order %v: the suspicion ended early", cands)
	}
	clk.Advance(time.Millisecond)
	a.order(cands, 3)
	if cands[2].node == "fast" {
		t.Fatalf("order %v: the suspicion did not decay", cands)
	}
	a.start(fast.node, fast.key())(true, -1, -1)
	a.start(fast.node, fast.key())(false, 0.001, 0)
	a.order(cands, 3)
	if cands[2].node == "fast" {
		t.Fatalf("order %v: a success did not clear the suspicion", cands)
	}
	a.noteStale(fresh.key(), true)
	cands = []candidate{fresh, slow}
	a.order(cands, 3)
	if cands[0].node != "slow" {
		t.Fatalf("order %v: the stale copy is first", cands)
	}
	clk.Advance(staleFor)
	a.order(cands, 3)
	if cands[0].node != "new" {
		t.Fatalf("order %v: the copy is still held stale after staleFor", cands)
	}
}

// TestPruneFloor: the floor is the lowest applied seq of the copies that count: live
// ones, and a stopped node's retiring rows for RetiringRetention after their leases
// ran out. A copy behind the others with no progress for the stall timeout stops
// counting, and counts again once it moves. Expired leases and dead nodes otherwise
// never count; a shard with no copy that counts is not pruned on its copies' account.
func TestPruneFloor(t *testing.T) {
	n := &Node{opts: Options{PruneStallTimeout: 50 * time.Millisecond, RetiringRetention: time.Minute}}
	id := store.ShardID{Index: "p", Shard: 0}
	v := &view{live: map[string]bool{"a": true, "b": true, "a2": true}}
	cp := func(node string, seq int64, left time.Duration) store.Copy {
		return store.Copy{Shard: id, NodeID: node, AppliedSeq: seq, LeaseLeft: left, State: store.CopyServing}
	}
	copies := []store.Copy{cp("a", 100, time.Second), cp("b", 40, time.Second), cp("c", 5, time.Second), cp("a2", 1, -time.Second)}
	progress := map[copyKey]int64{}
	clk := clock.NewFake(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC))
	now := clk.Now()
	if f, ok := n.pruneFloor(copies, v, progress, now); !ok || f != 40 {
		t.Fatalf("floor %d %v, want 40", f, ok)
	}
	if f, _ := n.pruneFloor(copies, v, progress, now.Add(30*time.Millisecond)); f != 40 {
		t.Fatalf("floor %d: a copy stalled for less than the timeout already stopped counting", f)
	}
	if f, ok := n.pruneFloor(copies, v, progress, now.Add(60*time.Millisecond)); !ok || f != 100 {
		t.Fatalf("floor %d %v, want 100 once b stalled for the timeout", f, ok)
	}
	// b makes progress (its node reports work done): it counts again at once.
	progress[copyKey{shard: id, node: "b"}] = 7
	if f, _ := n.pruneFloor(copies, v, progress, now.Add(70*time.Millisecond)); f != 40 {
		t.Fatalf("floor %d: a copy that moved does not count again", f)
	}
	// The most advanced copy never stalls, even idle.
	if f, _ := n.pruneFloor(copies[:1], v, progress, now.Add(time.Hour)); f != 100 {
		t.Fatalf("floor %d: the most advanced copy stopped counting", f)
	}
	// A stopped node's retiring row counts within the retention, past its lease.
	ret := cp("gone", 20, -30*time.Second)
	ret.State = store.CopyRetiring
	if f, _ := n.pruneFloor(append([]store.Copy{ret}, copies...), v, progress, now.Add(80*time.Millisecond)); f != 20 {
		t.Fatalf("floor %d: a recently retired copy does not hold the floor", f)
	}
	ret.LeaseLeft = -2 * time.Minute
	if f, _ := n.pruneFloor(append([]store.Copy{ret}, copies...), v, progress, now.Add(90*time.Millisecond)); f == 20 {
		t.Fatal("a retiring row past the retention still holds the floor")
	}
	if _, ok := n.pruneFloor([]store.Copy{cp("c", 5, time.Second)}, v, progress, now); ok {
		t.Fatal("a shard whose only copy is on a dead node is pruned on its account")
	}
}

// TestPruneBehindLiveCopies: the leader prunes each shard's changelog below the lowest
// applied seq of its live copies, and a copy behind it is never pruned past.
func TestPruneBehindLiveCopies(t *testing.T) {
	c := newCluster(t, sqliteDB(t), nil)
	a := c.start(0)
	createIndex(t, a.n, "pr", 1, 0)
	c.start(1)
	waitCopies(t, a.st, "pr", 1, 2, time.Minute)
	var last int64
	for k := range 30 {
		last = mustWrite(t, a.n, "pr", upsertOp(fmt.Sprintf("p%d", k), k))
	}
	id := store.ShardID{Index: "pr", Shard: 0}
	eventually(t, time.Minute, "the changelog is pruned behind both copies", func() error {
		if _, err := a.st.ChangesAfter(context.Background(), id, 0, 10); err == nil {
			return fmt.Errorf("nothing pruned yet")
		}
		return nil
	})
	floor := last
	for _, cp := range liveCopies(t, a.st, id) {
		floor = min(floor, cp.AppliedSeq)
	}
	if _, err := a.st.ChangesAfter(context.Background(), id, floor, 10); err != nil {
		t.Fatalf("a change after the lowest copy (%d) was pruned: %v", floor, err)
	}
}

// TestPeerAPIAuth: the internal API takes only the cluster token.
func TestPeerAPIAuth(t *testing.T) {
	c := newCluster(t, sqliteDB(t), nil)
	a := c.start(0)
	for _, tc := range []struct {
		token string
		want  int
	}{{"", http.StatusUnauthorized}, {"wrong-token-wrong-token", http.StatusUnauthorized}, {testToken, http.StatusOK}} {
		req, _ := http.NewRequestWithContext(tctx(t), http.MethodGet, "http://"+a.addr+"/_internal/copies", http.NoBody)
		if tc.token != "" {
			req.Header.Set("Authorization", "Bearer "+tc.token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Fatalf("token %q: HTTP %d, want %d", tc.token, resp.StatusCode, tc.want)
		}
	}
	// The public API is served beside it, under its own auth.
	resp, err := http.Get("http://" + a.addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz: %d", resp.StatusCode)
	}
}

// TestSingleNodeCluster: a cluster of one serves the public API exactly as a single
// node: create, bulk, read your writes, search, percolate, health green and ready, and
// a clean shutdown releases its copies and deregisters it.
func TestSingleNodeCluster(t *testing.T) {
	c := newCluster(t, sqliteDB(t), nil)
	a := c.start(0)
	do := func(method, path, body string) (int, map[string]any) {
		req, _ := http.NewRequestWithContext(tctx(t), method, "http://"+a.addr+path, strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var m map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&m)
		return resp.StatusCode, m
	}
	if code, m := do("PUT", "/indexes/one", `{"settings":{"shards":2}}`); code != 200 && code != 201 {
		t.Fatalf("create: %d %v", code, m)
	}
	code, m := do("POST", "/indexes/one/_bulk?refresh=wait_for", "{\"upsert\":{\"id\":\"a\"}}\n{\"price\":1}\n{\"upsert\":{\"id\":\"b\"}}\n{\"price\":2}\n")
	if code != 200 {
		t.Fatalf("bulk: %d %v", code, m)
	}
	if code, m := do("POST", "/indexes/one/_search", `{"query":{"all":[]},"track_total":true}`); code != 200 || m["total"].(map[string]any)["value"].(float64) != 2 { //nolint:forcetypeassert,errcheck // a test
		t.Fatalf("search: %d %v", code, m)
	}
	if code, m := do("PUT", "/indexes/one/queries/q1?refresh=wait_for", `{"query":{"field":"price","op":"gt","value":1}}`); code != 200 && code != 201 {
		t.Fatalf("query: %d %v", code, m)
	}
	if code, m := do("POST", "/indexes/one/_percolate", `{"ids":["a","b"]}`); code != 200 {
		t.Fatalf("percolate: %d %v", code, m)
	}
	if code, m := do("GET", "/_cluster/health", ""); code != 200 || m["status"] != api.StatusGreen || m["nodes"].(float64) != 1 { //nolint:forcetypeassert,errcheck // a test
		t.Fatalf("health: %d %v", code, m)
	}
	if code, _ := do("GET", "/readyz", ""); code != 200 {
		t.Fatalf("readyz: %d", code)
	}
	if code, m := do("GET", "/_cluster/shards", ""); code != 200 || len(m["shards"].([]any)) != 2 { //nolint:forcetypeassert,errcheck // a test
		t.Fatalf("shards: %d %v", code, m)
	}
	a.stop()
	st := c.db.open(t)
	defer st.Close()
	if nodes, _ := st.Registry().Nodes(context.Background()); len(nodes) != 0 {
		t.Fatalf("still registered: %+v", nodes)
	}
	// The copies stay, retiring at their final applied seqs, for the changelog to be
	// kept for a restart; their leases run out.
	copies, _ := st.Registry().Copies(context.Background(), "")
	if len(copies) != 2 {
		t.Fatalf("copies left: %+v", copies)
	}
	for _, c := range copies {
		if c.State != store.CopyRetiring || c.AppliedSeq == 0 {
			t.Fatalf("a stopped node's copy is %+v, want retiring at its applied seq", c)
		}
	}
}
