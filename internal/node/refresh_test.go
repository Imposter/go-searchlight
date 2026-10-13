package node_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/search"
)

// TestRefreshDisabledReadsRefreshOnDemand: on an index whose refresh is disabled
// nothing refreshes by itself, so a read that must see a write refreshes the copy on
// demand: a percolation after a saved-query write, a search with wait_for_seq, and a
// peer's read of the copy (LocalTarget) answer at once rather than at their deadline.
// A deadline that passes all the same is a 504 problem.
func TestRefreshDisabledReadsRefreshOnDemand(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	n := open(t, cfg, st, nil)
	spec := api.IndexSpec{
		Mapping:  &schema.Mapping{Fields: map[string]schema.FieldType{"x": schema.Number}},
		Settings: api.IndexSettings{Shards: 1, RefreshInterval: api.DisabledRefresh},
	}
	if _, err := n.CreateIndex(ctx(t), "p", spec); err != nil {
		t.Fatal(err)
	}
	waitServing(t, n)
	quick := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.Background(), 3*time.Second)
	}

	mustWrite(t, n, "p", api.WriteOp{Kind: api.OpQueryUpsert, ID: "q", Query: json.RawMessage(`{"field": "x", "op": "eq", "value": 1}`)})
	c, cancel := quick()
	res, err := n.Percolate(c, "p", &api.PercolateRequest{Docs: []json.RawMessage{json.RawMessage(`{"x": 1}`)}}, api.ReadOptions{})
	cancel()
	if err != nil || len(res.Results) != 1 || string(res.Results[0].Queries) != `["q"]` {
		t.Fatalf("percolate after a saved-query write: %+v, %v", res, err)
	}

	w := mustWrite(t, n, "p", upsert("d", `{"x": 2}`))
	c, cancel = quick()
	sr, err := n.Search(c, "p", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitForSeq: w.Seq})
	cancel()
	if err != nil || sr.Total != 1 {
		t.Fatalf("search with wait_for_seq: %+v, %v", sr, err)
	}

	w = mustWrite(t, n, "p", upsert("e", `{"x": 3}`))
	c, cancel = quick()
	target, err := n.LocalTarget(c, "p", 0, w.Seq)
	if err != nil {
		cancel()
		t.Fatalf("a peer's read with wait_for_seq: %v", err)
	}
	_, found, err := target.Get(c, "e")
	target.Release()
	cancel()
	if err != nil || !found {
		t.Fatalf("a peer's read of e: found %v, %v", found, err)
	}

	mustWrite(t, n, "p", api.WriteOp{Kind: api.OpQueryUpsert, ID: "s", Query: json.RawMessage(`{"field": "x", "op": "eq", "value": 4}`)})
	c, cancel = quick()
	wr, err := n.Write(c, "p", []api.WriteOp{upsert("f", `{"x": 4}`)}, api.WriteOptions{Refresh: api.RefreshWaitFor, Percolate: true})
	cancel()
	if err != nil || wr.TimedOut || !wr.Percolated || string(wr.Items[0].Queries) != `["s"]` {
		t.Fatalf("a refresh=wait_for, percolate=true write: %+v, %v", wr, err)
	}

	mustWrite(t, n, "p", api.WriteOp{Kind: api.OpQueryUpsert, ID: "r", Query: json.RawMessage(`{"field": "x", "op": "eq", "value": 3}`)})
	c, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	_, err = n.Percolate(c, "p", &api.PercolateRequest{Docs: []json.RawMessage{json.RawMessage(`{"x": 3}`)}}, api.ReadOptions{})
	cancel()
	var ae *api.Error
	if !errors.As(err, &ae) || ae.Status != http.StatusGatewayTimeout {
		t.Fatalf("percolate past its deadline: %v, want a 504 problem", err)
	}
}

// TestRewindLowersTheNodesHead: once a copy finds the changelog rewound (a failover
// that lost commits), the node forgets the head it knew: a wait_for_seq past the new
// head is a 400 at once again, not a wait to the deadline for a seq the database will
// reissue.
func TestRewindLowersTheNodesHead(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	n := open(t, cfg, st, nil)
	if _, err := n.CreateIndex(ctx(t), "r", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Fatal(err)
	}
	waitServing(t, n)
	kept := mustWrite(t, n, "r", upsert("kept", `{"x": 1}`))
	lost := mustWrite(t, n, "r", upsert("l1", `{"x": 2}`), upsert("l2", `{"x": 3}`), upsert("l3", `{"x": 4}`))
	if got := count(t, n, "r", &query.All{}, lost.Seq); got != 4 {
		t.Fatalf("count = %d", got)
	}
	rewindSQLite(t, cfg.StoreURL, kept.Seq)
	fresh := mustWrite(t, n, "r", upsert("n1", `{"x": 5}`))
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		res, err := n.Search(c, "r", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitForSeq: fresh.Seq})
		cancel()
		if err == nil && res.Total == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the copy never rebuilt: %+v, %v", res, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := n.Search(c, "r", &search.Request{Query: &query.All{}}, api.ReadOptions{WaitForSeq: lost.Seq})
	var ae *api.Error
	if !errors.As(err, &ae) || ae.Status != http.StatusBadRequest {
		t.Fatalf("wait_for_seq %d past the rewound head %d: %v, want a 400", lost.Seq, fresh.Seq, err)
	}
}

// rewindSQLite loses every commit after seq k from the SQLite store at storeURL, as a
// failover that lost them would.
func rewindSQLite(t *testing.T, storeURL string, k int64) {
	t.Helper()
	u, err := url.Parse(storeURL)
	if err != nil {
		t.Fatal(err)
	}
	path := strings.TrimPrefix(u.Path, "/")
	if _, err := os.Stat(path); err != nil {
		path = u.Path
	}
	raw, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(10000)")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	for _, q := range []string{
		"DELETE FROM sl_changes WHERE seq > ?",
		"DELETE FROM sl_documents WHERE seq > ?",
		"DELETE FROM sl_queries WHERE seq > ?",
		"UPDATE sl_counter SET value = ? WHERE id = 1",
	} {
		if _, err := raw.ExecContext(context.Background(), q, k); err != nil {
			t.Fatalf("rewind %q: %v", q, err)
		}
	}
}
