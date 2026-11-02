package node_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/store"
)

// shardDownStore fails every commit that names one shard, as a database whose
// transactions for it fail would.
type shardDownStore struct {
	store.Store
	store.RecordReader
	shard int
}

var errShardDown = errors.New("the shard's transaction failed")

func (s shardDownStore) Apply(ctx context.Context, batch []store.Change) error {
	for i := range batch {
		if batch[i].Shard == s.shard {
			return errShardDown
		}
	}
	return s.Store.Apply(ctx, batch)
}

// TestBulkPartialFailure: each shard's ops commit apart. When one shard's transaction
// fails, its ops are answered with a 5xx in their items and the other shards' commit,
// percolated and named in the token; a write whose every op is on the failed shard is
// the request's error.
func TestBulkPartialFailure(t *testing.T) {
	cfg := testConfig(t)
	base := openStore(t, cfg)
	st := shardDownStore{Store: base, RecordReader: base.(store.RecordReader), shard: 1} //nolint:forcetypeassert,errcheck // every store reads records
	n := open(t, cfg, st, func(o *node.Options) { o.GroupCommit.MaxTransactions = 4 })
	if _, err := n.CreateIndex(ctx(t), "pf", api.IndexSpec{Settings: api.IndexSettings{Shards: 2}}); err != nil {
		t.Fatal(err)
	}
	waitServing(t, n)
	var on [2]string
	for i := 0; on[0] == "" || on[1] == ""; i++ {
		id := string(rune('a' + i))
		if s := node.ShardFor(id, 2); on[s] == "" {
			on[s] = id
		}
	}
	mustWrite(t, n, "pf", api.WriteOp{Kind: api.OpQueryUpsert, ID: on[0], Query: []byte(`{"all": []}`)})
	res, err := n.Write(ctx(t), "pf", []api.WriteOp{upsert(on[0], `{"x": 1}`), upsert(on[1], `{"x": 2}`)}, api.WriteOptions{Percolate: true})
	if err != nil {
		t.Fatalf("a write with one shard down: %v", err)
	}
	ok, down := res.Items[0], res.Items[1]
	if ok.Err != nil || ok.Seq == 0 || string(ok.Queries) != `["`+on[0]+`"]` {
		t.Fatalf("the op on the working shard: %+v", ok)
	}
	if down.Err == nil || api.ProblemFor(down.Err).Status < http.StatusInternalServerError || down.Seq != 0 {
		t.Fatalf("the op on the failed shard: %+v", down)
	}
	if res.Token == nil || res.Token.Seq(0) != ok.Seq || res.Token.Seq(1) != 0 || !res.Percolated {
		t.Fatalf("token %+v, percolated %v", res.Token, res.Percolated)
	}
	if _, err := n.Write(ctx(t), "pf", []api.WriteOp{upsert(on[1], `{"x": 3}`)}, api.WriteOptions{}); err == nil || api.ProblemFor(err).Status < http.StatusInternalServerError {
		t.Fatalf("a write on the failed shard alone: %v, want a 5xx", err)
	}
	if got := count(t, n, "pf", &query.All{}, res.Token); got != 1 {
		t.Fatalf("count = %d", got)
	}
}
