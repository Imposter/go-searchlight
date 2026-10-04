package cluster

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/search"
	"github.com/Imposter/go-searchlight/internal/store"
)

// Benchmarks of the cluster over localhost, on SQLite:
//
//	go test ./internal/cluster -run '^$' -bench . -benchtime 3s
//
// SQLite serializes every node's writes on one file, so the write numbers are a floor;
// Postgres (SEARCHLIGHT_TEST_PG_URL) is the realistic store.

func benchDB(b *testing.B) *db {
	b.Helper()
	return sqliteDB(b)
}

// BenchmarkScatterGatherSearch: a sorted top-10 search over a three-shard index of
// 30,000 documents, from one node holding every shard (nodes=1) against three nodes
// holding one shard each (nodes=3: the coordinator searches one shard locally and two
// on its peers, query then fetch, and reduces).
func BenchmarkScatterGatherSearch(b *testing.B) {
	for _, nodes := range []int{1, 3} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			c := newCluster(b, benchDB(b), heavyLoad)
			a := c.start(0)
			replicas := 0
			if nodes > 1 {
				replicas = 1
			}
			createIndex(b, a.n, "sg", 3, replicas)
			for i := 1; i < nodes; i++ {
				c.start(i)
			}
			last := bulkLoad(b, a.n, "sg", 0, 30_000, 5000)
			waitCopies(b, a.st, "sg", 3, 1, time.Minute)
			if nodes == 1 {
				waitCount(b, a.n, "sg", last, int64(30_000))
			} else {
				for _, tn := range c.live() {
					waitCount(b, tn.n, "sg", last, int64(30_000))
				}
			}
			q, problems := query.Parse([]byte(`{"field":"price","op":"between","value":[100,800]}`))
			if len(problems) > 0 {
				b.Fatal(problems)
			}
			r := func() *search.Request {
				return &search.Request{Query: q, Sort: []search.SortField{{Field: "price", Desc: true}}, Size: 10, TrackTotal: search.TrackTotalAll}
			}
			ctx := context.Background()
			b.ResetTimer()
			for b.Loop() {
				res, err := a.n.Search(ctx, "sg", r(), api.ReadOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if len(res.Hits) != 10 {
					b.Fatalf("%d hits", len(res.Hits))
				}
			}
		})
	}
}

// BenchmarkPeerRecovery: a new node recovers a 50,000-document shard from a peer; the
// throughput is the copy's bytes over the time from the node's start to its copy
// serving.
func BenchmarkPeerRecovery(b *testing.B) {
	c := newCluster(b, benchDB(b), heavyLoad)
	a := c.start(0)
	createIndex(b, a.n, "pr", 1, 0)
	last := bulkLoad(b, a.n, "pr", 0, 50_000, 5000)
	waitCount(b, a.n, "pr", last, int64(50_000))
	id := store.ShardID{Index: "pr", Shard: 0}
	total := snapshotBytes(b, a, id)
	var spent time.Duration
	var fetched int64
	b.ResetTimer()
	i := 0
	for b.Loop() {
		i++
		start := time.Now()
		tn := c.start(i)
		eventually(b, 5*time.Minute, "the copy serves", func() error {
			for _, cp := range liveCopies(b, a.st, id) {
				if cp.NodeID == tn.n.id && cp.State == store.CopyServing {
					return nil
				}
			}
			return fmt.Errorf("not yet")
		})
		spent += time.Since(start)
		fetched += tn.n.fetch.bytes.Load()
		b.StopTimer()
		tn.stop()
		b.StartTimer()
	}
	b.ReportMetric(float64(fetched)/(1<<20)/spent.Seconds(), "MB/s")
	b.ReportMetric(float64(total)/(1<<20), "copy_MB")
}

// BenchmarkWriteThroughput: concurrent bulks of 100 documents, four writers per node,
// through one node (nodes=1) and through all three (nodes=3), into a three-shard index
// every node holds.
func BenchmarkWriteThroughput(b *testing.B) {
	for _, nodes := range []int{1, 3} {
		b.Run(fmt.Sprintf("nodes=%d", nodes), func(b *testing.B) {
			c := newCluster(b, benchDB(b), heavyLoad)
			a := c.start(0)
			createIndex(b, a.n, "wt", 3, 0)
			for i := 1; i < nodes; i++ {
				c.start(i)
			}
			waitCopies(b, a.st, "wt", 3, nodes, time.Minute)
			var next atomic.Int64
			var docs atomic.Int64
			b.ResetTimer()
			start := time.Now()
			var wg sync.WaitGroup
			var failed atomic.Pointer[error]
			for _, tn := range c.live() {
				for range 4 {
					wg.Go(func() {
						for next.Add(1) <= int64(b.N) {
							ops := make([]api.WriteOp, 100)
							base := next.Load() * 100
							for k := range ops {
								ops[k] = upsertOp(fmt.Sprintf("w%d-%d", base, k), k)
							}
							res, err := tn.n.Write(context.Background(), "wt", ops, api.WriteOptions{})
							if err != nil {
								failed.CompareAndSwap(nil, &err)
								return
							}
							for _, it := range res.Items {
								if it.Err == nil {
									docs.Add(1)
								}
							}
						}
					})
				}
			}
			wg.Wait()
			if e := failed.Load(); e != nil {
				b.Fatal(*e)
			}
			b.ReportMetric(float64(docs.Load())/time.Since(start).Seconds(), "docs/s")
		})
	}
}
