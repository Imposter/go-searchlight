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
// demand: a percolation after a saved-query write, a search with wait_for, and a
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
	sr, err := n.Search(c, "p", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitFor: w.Token})
	cancel()
	if err != nil || sr.Total != 1 {
		t.Fatalf("search with wait_for: %+v, %v", sr, err)
	}

	w = mustWrite(t, n, "p", upsert("e", `{"x": 3}`))
	c, cancel = quick()
	target, err := n.LocalTarget(c, "p", 0, w.Token.Seq(0))
	if err != nil {
		cancel()
		t.Fatalf("a peer's read with wait_for: %v", err)
	}
	_, _, found, err := target.Get(c, "e")
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
// that lost commits), the node forgets the head it knew: a wait_for past the new
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
	if got := count(t, n, "r", &query.All{}, lost.Token); got != 4 {
		t.Fatalf("count = %d", got)
	}
	rewindSQLite(t, cfg.StoreURL, kept.Token.Seq(0))
	deadline := time.Now().Add(30 * time.Second)
	// The copy can find the rewind before this write, and refuses it with a retryable
	// 503 while it recovers.
	var fresh *api.WriteResult
	for {
		var err error
		fresh, err = n.Write(ctx(t), "r", []api.WriteOp{upsert("n1", `{"x": 5}`)}, api.WriteOptions{})
		if err == nil {
			break
		}
		var refused *api.Error
		if !errors.As(err, &refused) || refused.Status != http.StatusServiceUnavailable || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		res, err := n.Search(c, "r", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitFor: fresh.Token})
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
	_, err := n.Search(c, "r", &search.Request{Query: &query.All{}}, api.ReadOptions{WaitFor: lost.Token})
	var ae *api.Error
	if !errors.As(err, &ae) || ae.Status != http.StatusBadRequest {
		t.Fatalf("wait_for %v past the rewound head %v: %v, want a 400", lost.Token.Seqs, fresh.Token.Seqs, err)
	}
}

// rewindSQLite loses every commit after seq k from the SQLite store at storeURL, as a
// failover that lost them would, all at once.
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
	tx, err := raw.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for _, q := range []string{
		"DELETE FROM sl_changes WHERE seq > ?",
		"DELETE FROM sl_documents WHERE seq > ?",
		"DELETE FROM sl_queries WHERE seq > ?",
		"UPDATE sl_shard_seq SET value = ? WHERE value > ?",
		"UPDATE sl_counter SET value = ? WHERE id = 1 AND value > ?",
	} {
		args := []any{k}
		if strings.Count(q, "?") == 2 {
			args = append(args, k)
		}
		if _, err := tx.ExecContext(context.Background(), q, args...); err != nil {
			t.Fatalf("rewind %q: %v", q, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("rewind: %v", err)
	}
}

// TestRewindLowersTheQuerySeq (from the M2 review): saved queries written after the
// kept seq are lost in a failover. The shard's querySeq must fall with its head, or
// every percolation would wait for a seq the rewound shard has not reached again.
func TestRewindLowersTheQuerySeq(t *testing.T) {
	cfg := testConfig(t)
	st := openStore(t, cfg)
	n := open(t, cfg, st, nil)
	if _, err := n.CreateIndex(ctx(t), "r", api.IndexSpec{Settings: api.IndexSettings{Shards: 1}}); err != nil {
		t.Fatal(err)
	}
	waitServing(t, n)
	kept := mustWrite(t, n, "r", upsert("kept", `{"x": 1}`))
	var ops []api.WriteOp
	for _, id := range []string{"q1", "q2", "q3", "q4"} {
		ops = append(ops, api.WriteOp{Kind: api.OpQueryUpsert, ID: id, Query: json.RawMessage(`{"field": "x", "op": "exists"}`)})
	}
	lost := mustWrite(t, n, "r", ops...)
	req := &api.PercolateRequest{Docs: []json.RawMessage{json.RawMessage(`{"x": 1}`)}}
	if _, err := n.Percolate(ctx(t), "r", req, api.ReadOptions{WaitFor: lost.Token}); err != nil {
		t.Fatal(err)
	}
	rewindSQLite(t, cfg.StoreURL, kept.Token.Seq(0))
	deadline := time.Now().Add(30 * time.Second)
	var fresh *api.WriteResult
	for {
		var err error
		fresh, err = n.Write(ctx(t), "r", []api.WriteOp{upsert("n1", `{"x": 5}`)}, api.WriteOptions{})
		if err == nil {
			break
		}
		var refused *api.Error
		if !errors.As(err, &refused) || refused.Status != http.StatusServiceUnavailable || time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	for {
		c, cancel := context.WithTimeout(context.Background(), time.Second)
		res, err := n.Search(c, "r", &search.Request{Query: &query.All{}, TrackTotal: search.TrackTotalAll}, api.ReadOptions{WaitFor: fresh.Token})
		cancel()
		if err == nil && res.Total == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the copy never rebuilt: %+v, %v", res, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("kept %d, lost queries at %d, fresh write at %d", kept.Token.Seq(0), lost.Token.Seq(0), fresh.Token.Seq(0))
	c, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := n.Percolate(c, "r", req, api.ReadOptions{}); err != nil {
		t.Fatalf("a percolation with no wait_for after the rewind: %v", err)
	}
}
