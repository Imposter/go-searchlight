package node_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/store/storetest"
)

// BenchmarkPercolateShards percolates batches of documents against 20,000 saved queries
// spread over 8 shards of one node, through the coordinator: every shard's copy is
// percolated for each batch and their matches merged.
func BenchmarkPercolateShards(b *testing.B) {
	dir := b.TempDir()
	cfg := config.Default()
	cfg.StoreURL = storetest.SQLiteURL(storetest.Migrated(b, filepath.Join(dir, "sl.db")))
	cfg.DataDir = filepath.Join(dir, "data")
	cfg.RefreshInterval = 20 * time.Millisecond
	cfg.ChangelogPollInterval = 20 * time.Millisecond
	cfg.NodeID = "n1"
	ctx := context.Background()
	st, err := store.Open(ctx, cfg.StoreURL, store.WithLogger(quiet))
	if err != nil {
		b.Fatal(err)
	}
	defer st.Close()
	n, err := node.NewSingle(ctx, node.Options{Store: st, Config: cfg, Logger: quiet})
	if err != nil {
		b.Fatal(err)
	}
	defer n.Close(ctx)
	spec := api.IndexSpec{
		Mapping:  &schema.Mapping{Fields: map[string]schema.FieldType{"brand": schema.Keyword, "price": schema.Number, "tags": schema.KeywordList}},
		Settings: api.IndexSettings{Shards: 8},
	}
	if _, err := n.CreateIndex(ctx, "pb", spec); err != nil {
		b.Fatal(err)
	}
	rng := rand.New(rand.NewPCG(7, 7))
	var seq int64
	for batch := range 20 {
		ops := make([]api.WriteOp, 1000)
		for i := range ops {
			q := fmt.Sprintf(`{"all": [{"field": "brand", "op": "eq", "value": "b%d"}, {"field": "price", "op": "lt", "value": %d}]}`, rng.IntN(500), rng.IntN(100))
			if i%3 == 0 {
				q = fmt.Sprintf(`{"field": "tags", "op": "has", "value": "t%d"}`, rng.IntN(2000))
			}
			ops[i] = api.WriteOp{Kind: api.OpQueryUpsert, ID: fmt.Sprintf("q%02d-%04d", batch, i), Query: json.RawMessage(q)}
		}
		res, err := n.Write(ctx, "pb", ops, api.WriteOptions{})
		if err != nil {
			b.Fatal(err)
		}
		seq = res.Seq
	}
	for _, docs := range []int{1, 20, 200} {
		b.Run(fmt.Sprintf("docs=%d", docs), func(b *testing.B) {
			body := make([]json.RawMessage, docs)
			for i := range body {
				body[i] = json.RawMessage(fmt.Sprintf(`{"brand": "b%d", "price": %d, "tags": ["t%d", "t%d"]}`, rng.IntN(500), rng.IntN(100), rng.IntN(2000), rng.IntN(2000)))
			}
			req := &api.PercolateRequest{Docs: body}
			if _, err := n.Percolate(ctx, "pb", req, api.ReadOptions{WaitForSeq: seq}); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			for range b.N {
				if _, err := n.Percolate(ctx, "pb", req, api.ReadOptions{}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(docs*b.N)/b.Elapsed().Seconds(), "docs/s")
		})
	}
}
