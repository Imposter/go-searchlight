package percolate

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/bench/datasets"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// Percolation over slbench's own data (bench/datasets): product listings and the saved
// searches T5 percolates them against, on a percolator index holding no documents (no
// term statistics), as slbench creates it.
//
//	go test -run '^$' -bench BenchmarkPercolateProducts -benchtime 4000x ./internal/percolate/

const productsSeed = 1

// productsMapping is the products mapping slbench creates.
func productsMapping(tb testing.TB) *schema.Mapping {
	tb.Helper()
	m := &schema.Mapping{Fields: map[string]schema.FieldType{}, Dynamic: schema.DynamicStrict}
	for _, f := range datasets.Products {
		t, err := schema.ParseFieldType(string(f.Type))
		if err != nil {
			tb.Fatal(err)
		}
		m.Fields[f.Name] = t
	}
	return m
}

// productSearch is saved search i as a stored query.
func productSearch(tb testing.TB, i int64) shard.StoredQuery {
	tb.Helper()
	s := datasets.Search(productsSeed, i)
	raw, err := json.Marshal(s.Query)
	if err != nil {
		tb.Fatal(err)
	}
	n, problems := query.Parse(raw)
	if len(problems) > 0 {
		tb.Fatal(problems)
	}
	return shard.StoredQuery{ID: s.ID, Seq: i + 1, Query: n}
}

// productDocs are n listings from ordinal first on, analyzed.
func productDocs(tb testing.TB, m *schema.Mapping, first int64, n int) []schema.Doc {
	tb.Helper()
	out := make([]schema.Doc, n)
	for i := range out {
		id := datasets.ProductID(first + int64(i))
		d, _, err := schema.Analyze(m, id, datasets.AppendProduct(nil, productsSeed, first+int64(i)))
		if err != nil {
			tb.Fatal(err)
		}
		out[i] = d
	}
	return out
}

// newProductsEnv opens a shard with the products mapping and stores the first queries
// saved searches in segments query segments.
func newProductsEnv(b *testing.B, queries, segments int) *benchEnv {
	b.Helper()
	ctx := context.Background()
	m := productsMapping(b)
	s, err := shard.Open(ctx, b.TempDir(), m, shard.Options{QueryIndex: Index{}, RefreshInterval: -1, RefreshBytes: -1, DisableMerges: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close(ctx) })
	segments = max(1, segments)
	for part := range segments {
		var changes []shard.Change
		for i := part * queries / segments; i < (part+1)*queries/segments; i++ {
			q := productSearch(b, int64(i))
			changes = append(changes, shard.Change{Seq: q.Seq, Kind: shard.QueryUpsert, QueryID: q.ID, Query: q.Query})
		}
		if err := s.Apply(ctx, changes); err != nil {
			b.Fatal(err)
		}
		if err := s.Refresh(ctx); err != nil {
			b.Fatal(err)
		}
	}
	env := &benchEnv{s: s, g: s.Acquire()}
	b.Cleanup(func() { env.g.Release() })
	env.docs = productDocs(b, m, 200_000, 4096)
	return env
}

// BenchmarkPercolateProducts is T5's shape at 10k and 100k saved searches, in one query
// segment and in ten.
func BenchmarkPercolateProducts(b *testing.B) {
	for _, n := range []int{10_000, 100_000} {
		for _, segs := range []int{1, 10} {
			b.Run(fmt.Sprintf("queries=%dk/segments=%d", n/1000, segs), func(b *testing.B) {
				env := newProductsEnv(b, n, segs)
				p := New(Options{})
				if _, err := p.Percolate(context.Background(), env.g, env.docs); err != nil {
					b.Fatal(err)
				}
				b.Run("doc", func(b *testing.B) { benchDoc(b, env, p) })
				b.Run("batch", func(b *testing.B) { benchBatch(b, env, p) })
			})
		}
	}
}

// BenchmarkPercolateProductsMemory builds and opens a query segment of 1M saved
// searches, compiles every class, and reports the live heap it holds and its file
// size. Run it with -benchtime 1x.
func BenchmarkPercolateProductsMemory(b *testing.B) {
	const n = 1_000_000
	qs := make([]shard.StoredQuery, n)
	for i := range qs {
		qs[i] = productSearch(b, int64(i))
	}
	dir := b.TempDir()
	for b.Loop() {
		size, err := (Index{}).Build(context.Background(), dir, "m", qs, nil)
		if err != nil {
			b.Fatal(err)
		}
		before := liveHeap()
		start := time.Now()
		qs2, err := (Index{}).Open(dir, "m")
		if err != nil {
			b.Fatal(err)
		}
		seg, ok := qs2.(*Segment)
		if !ok {
			b.Fatalf("Open returned %T", qs2)
		}
		b.ReportMetric(time.Since(start).Seconds(), "open-s")
		b.ReportMetric(float64(liveHeap()-before)/(1<<20), "heap-MiB")
		b.ReportMetric(float64(size)/(1<<20), "file-MiB")
		runtime.KeepAlive(seg)
		_ = seg.Close()
	}
	runtime.KeepAlive(qs)
}

func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}
