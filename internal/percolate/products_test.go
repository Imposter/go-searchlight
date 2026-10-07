package percolate

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"slices"
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
// searches and percolates 200 documents against it one by one, reporting the live heap
// the open segment holds before and after, its file size, the part of the file
// percolation reads (hot: all but the stored queries), and the documents' latency. Run
// it with -benchtime 1x.
func BenchmarkPercolateProductsMemory(b *testing.B) {
	const n = 1_000_000
	qs := make([]shard.StoredQuery, n)
	for i := range qs {
		qs[i] = productSearch(b, int64(i))
	}
	docs := productDocs(b, productsMapping(b), 200_000, 200)
	dir := b.TempDir()
	for b.Loop() {
		size, err := (Index{}).Build(context.Background(), dir, "m", qs, nil)
		if err != nil {
			b.Fatal(err)
		}
		before := liveHeap()
		start := time.Now()
		opened, err := (Index{}).Open(dir, "m")
		if err != nil {
			b.Fatal(err)
		}
		seg, ok := opened.(*Segment)
		if !ok {
			b.Fatalf("Open returned %T", opened)
		}
		b.ReportMetric(time.Since(start).Seconds(), "open-s")
		b.ReportMetric(float64(liveHeap()-before)/(1<<20), "heap-MiB")
		sc := new(scratch)
		sc.fit(seg.NumQueries(), seg.NumEntries(), len(seg.fields))
		v := &view{seg: seg, n: seg.NumQueries()}
		var lat []time.Duration
		for i := range docs {
			t0 := clockNow()
			seg.collect(&docs[i], sc)
			var st docStats
			verify(v, sc, &st)
			sc.results()
			sc.reset()
			lat = append(lat, clockSince(t0))
		}
		b.ReportMetric(float64(liveHeap()-before)/(1<<20), "heap-after-MiB")
		b.ReportMetric(float64(size)/(1<<20), "file-MiB")
		b.ReportMetric(float64(size-int64(len(seg.records)+len(seg.offsets)))/(1<<20), "hot-MiB")
		reportLatency(b, lat)
		runtime.KeepAlive(sc)
		_ = seg.Close()
	}
	runtime.KeepAlive(qs)
}

// BenchmarkPercolateStages times what a single-document _percolate costs, stage by
// stage, against 100k saved searches: analysis as the server does it (for matching)
// and as it did (with grams), percolation, the response (the matches copied into the
// body, as the API writes them), and the client decoding the body into strings as
// slbench does. It reports each stage's p50, p90 and p99 over 3000 documents, and the
// p99 of the server's stages by how many saved searches a document matches. The
// percolator has the default threads (GOMAXPROCS, set by -cpu), so a document splits
// into windows as it does in a node. Run it with -benchtime 1x.
func BenchmarkPercolateStages(b *testing.B) {
	for _, segs := range []int{10, 3, 1} {
		b.Run(fmt.Sprintf("segments=%d", segs), func(b *testing.B) {
			env := newProductsEnv(b, 100_000, segs)
			m := productsMapping(b)
			p := New(Options{})
			type sample struct {
				analyzeFull, analyze, perc, response, decode time.Duration
				matches                                      int
			}
			var rows []sample
			var body []byte
			for b.Loop() {
				rows = rows[:0]
				for i := range 3100 {
					doc := datasets.AppendProduct(nil, productsSeed, 300_000+int64(i))
					var r sample
					t0 := clockNow()
					if _, _, err := schema.Analyze(m, "_percolate_0", doc); err != nil {
						b.Fatal(err)
					}
					r.analyzeFull = clockSince(t0)
					t0 = clockNow()
					d, _, err := schema.AnalyzeForMatch(m, "_percolate_0", doc)
					if err != nil {
						b.Fatal(err)
					}
					r.analyze = clockSince(t0)
					t0 = clockNow()
					ids, err := p.Percolate(context.Background(), env.g, []schema.Doc{d})
					if err != nil {
						b.Fatal(err)
					}
					r.perc = clockSince(t0)
					t0 = clockNow()
					body = append(append(append(body[:0], `{"results":[{"found":true,"queries":`...), ids[0]...), `}],"took_ms":0}`...)
					r.response = clockSince(t0)
					t0 = clockNow()
					var res struct {
						Results []struct {
							Queries []string `json:"queries"`
						} `json:"results"`
					}
					if err := json.Unmarshal(body, &res); err != nil {
						b.Fatal(err)
					}
					r.decode = clockSince(t0)
					r.matches = len(res.Results[0].Queries)
					if i >= 100 {
						rows = append(rows, r)
					}
				}
			}
			pct := func(rows []sample, get func(sample) time.Duration, q float64) time.Duration {
				v := make([]time.Duration, len(rows))
				for i, r := range rows {
					v[i] = get(r)
				}
				slices.Sort(v)
				return v[int(q*float64(len(v)-1))]
			}
			server := func(r sample) time.Duration { return r.analyze + r.perc + r.response }
			for _, st := range []struct {
				name string
				get  func(sample) time.Duration
			}{
				{"analyze (with grams)", func(r sample) time.Duration { return r.analyzeFull }},
				{"analyze for match", func(r sample) time.Duration { return r.analyze }},
				{"percolate", func(r sample) time.Duration { return r.perc }},
				{"response", func(r sample) time.Duration { return r.response }},
				{"server (analyze+percolate+response)", server},
				{"client decode", func(r sample) time.Duration { return r.decode }},
			} {
				b.Logf("%-36s p50 %9v p90 %9v p99 %9v", st.name, pct(rows, st.get, .5), pct(rows, st.get, .9), pct(rows, st.get, .99))
			}
			for _, bk := range []struct {
				name   string
				lo, hi int
			}{{"<500", 0, 500}, {"500-2k", 500, 2000}, {"2k-6k", 2000, 6000}, {">=6k", 6000, 1 << 30}} {
				var in []sample
				for _, r := range rows {
					if r.matches >= bk.lo && r.matches < bk.hi {
						in = append(in, r)
					}
				}
				if len(in) > 0 {
					b.Logf("matches %-7s n=%4d server p99 %9v, with client decode p99 %9v", bk.name, len(in), pct(in, server, .99),
						pct(in, func(r sample) time.Duration { return server(r) + r.decode }, .99))
				}
			}
		})
	}
}

func liveHeap() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}
