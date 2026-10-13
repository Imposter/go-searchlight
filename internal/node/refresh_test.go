package node_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
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
