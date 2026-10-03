package search

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// Benchmarks at 1M scrape-bot-shaped documents (16 fields, a 60-word description),
// one shard of four segments. Building the corpus takes minutes, so the shard is
// kept under SEARCHLIGHT_BENCH_DIR (default E:/tmp/sl-bench), keyed by a hash of the
// corpus parameters, and reopened on later runs. SEARCHLIGHT_BENCH_DOCS overrides the
// document count (a quick run: 100000).
//
//	go test ./internal/search -run '^$' -bench . -benchtime 200x
//
// Each benchmark reports its p50 and p99 latency (µs), per ExecuteShard and Reduce.

const benchCorpusVersion = "v1"

func benchDocs() int {
	if s := os.Getenv("SEARCHLIGHT_BENCH_DOCS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return 1_000_000
}

func benchDir() string {
	if d := os.Getenv("SEARCHLIGHT_BENCH_DIR"); d != "" {
		return d
	}
	return "E:/tmp/sl-bench"
}

var benchMapping = &schema.Mapping{Dynamic: schema.DynamicStrict, Fields: map[string]schema.FieldType{
	"title": schema.Text, "description": schema.Text, "brand": schema.Keyword, "category": schema.Keyword,
	"vendor": schema.Keyword, "sku": schema.Keyword, "url": schema.Keyword, "tags": schema.KeywordList,
	"colors": schema.KeywordList, "price": schema.Number, "sale_price": schema.Number, "rating": schema.Number,
	"stock": schema.Number, "in_stock": schema.Bool, "created": schema.Date, "updated": schema.Date,
}}

// corpus is the benchmark's vocabulary: zipf-distributed words, as a catalogue's are.
type corpus struct {
	words      []string // description and title words, most frequent first
	brands     []string
	categories []string
	vendors    []string
	tags       []string
	colors     []string
}

func newCorpus() *corpus {
	rng := rand.New(rand.NewPCG(42, 7))
	syll := []string{"ka", "lo", "mi", "ne", "ru", "sta", "vel", "qui", "tor", "ban", "dex", "pho", "lin", "gar", "zu", "wex"}
	word := func(n int) string {
		var b strings.Builder
		for range n {
			b.WriteString(syll[rng.IntN(len(syll))])
		}
		return b.String()
	}
	c := &corpus{}
	known := []string{
		"wireless", "stainless", "steel", "bluetooth", "charger", "cable", "portable", "waterproof",
		"organic", "cotton", "leather", "premium", "compact", "rechargeable", "ergonomic", "vintage", "outdoor",
		"kitchen", "garden", "battery", "digital", "smart", "ceramic", "bamboo", "travel",
	}
	seen := map[string]bool{}
	for _, w := range known {
		seen[w] = true
	}
	for len(c.words) < 6000 {
		w := word(2 + rng.IntN(3))
		if !seen[w] {
			seen[w] = true
			c.words = append(c.words, w)
		}
	}
	// The real words sit at ranks 20..500: common, not stop words.
	for i, w := range known {
		at := 20 + i*20
		c.words = slices.Insert(c.words, at, w)
	}
	for i := range 500 {
		c.brands = append(c.brands, fmt.Sprintf("Brand %s %d", word(2), i))
	}
	for i := range 50 {
		c.categories = append(c.categories, fmt.Sprintf("Category %s", word(3)+strconv.Itoa(i)))
	}
	for i := range 2000 {
		c.vendors = append(c.vendors, fmt.Sprintf("vendor-%s-%d", word(2), i))
	}
	for i := range 200 {
		c.tags = append(c.tags, fmt.Sprintf("tag %s", word(2)+strconv.Itoa(i)))
	}
	c.colors = []string{"red", "blue", "green", "black", "white", "grey", "yellow", "pink", "orange", "purple"}
	return c
}

// zipf picks an index below n from z.
func zipf(z *rand.Zipf, n int) int {
	return int(z.Uint64() % uint64(n))
}

func (c *corpus) doc(i int) (string, []byte) {
	rng := rand.New(rand.NewPCG(uint64(i), 99))
	zw := rand.NewZipf(rng, 1.07, 2, uint64(len(c.words)-1))
	zb := rand.NewZipf(rng, 1.2, 1, uint64(len(c.brands)-1))
	words := func(n int) string {
		ws := make([]string, n)
		for j := range ws {
			ws[j] = c.words[zipf(zw, len(c.words))]
		}
		return strings.Join(ws, " ")
	}
	list := func(from []string, n int) string {
		parts := make([]string, n)
		for j := range parts {
			parts[j] = strconv.Quote(from[rng.IntN(len(from))])
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	price := math.Round(math.Exp(rng.Float64()*7)*100) / 100
	created := int64(1_600_000_000_000) + rng.Int64N(200_000_000_000)
	sale := ""
	if rng.IntN(4) == 0 {
		sale = fmt.Sprintf(`"sale_price":%v,`, math.Round(price*80)/100)
	}
	id := fmt.Sprintf("p%07d", i)
	body := fmt.Sprintf(`{"title":%q,"description":%q,"brand":%q,"category":%q,"vendor":%q,"sku":"SKU-%07d",`+
		`"url":"https://shop.example/p/%07d","tags":%s,"colors":%s,"price":%v,%s"rating":%.1f,"stock":%d,`+
		`"in_stock":%v,"created":%d,"updated":%d}`,
		words(4+rng.IntN(5)), words(60), c.brands[zipf(zb, len(c.brands))], c.categories[rng.IntN(len(c.categories))],
		c.vendors[rng.IntN(len(c.vendors))], i, i, list(c.tags, 1+rng.IntN(5)), list(c.colors, 1+rng.IntN(3)),
		price, sale, 1+rng.Float64()*4, rng.IntN(500), rng.IntN(5) > 0, created, created+rng.Int64N(10_000_000_000))
	return id, []byte(body)
}

var (
	benchOnce  sync.Once
	benchShard *shard.Shard
	errBench   error
	benchCorp  *corpus
)

// openBench opens (building first, if needed) the benchmark shard.
func openBench(b *testing.B) (*shard.Shard, *corpus) {
	b.Helper()
	benchOnce.Do(func() {
		benchCorp = newCorpus()
		n := benchDocs()
		sum := sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%v", benchCorpusVersion, n, benchMapping.Fields)))
		dir := filepath.Join(benchDir(), hex.EncodeToString(sum[:8]))
		marker := filepath.Join(dir, "complete")
		opts := shard.Options{
			RefreshInterval: -1, DisableMerges: true, FlushBytes: -1, Logger: quiet,
			FilterCache: shard.NewFilterCache(shard.DefaultFilterCacheBytes, nil),
		}
		if _, err := os.Stat(marker); err == nil {
			benchShard, errBench = shard.Open(context.Background(), filepath.Join(dir, "shard"), benchMapping, opts)
			return
		}
		_ = os.RemoveAll(dir)
		start := time.Now()
		s, err := shard.Open(context.Background(), filepath.Join(dir, "shard"), benchMapping, opts)
		if err != nil {
			errBench = err
			return
		}
		const perSegment = 250_000
		seq := int64(0)
		for lo := 0; lo < n; lo += perSegment {
			hi := min(n, lo+perSegment)
			changes := make([]shard.Change, hi-lo)
			var wg sync.WaitGroup
			workers := runtime.GOMAXPROCS(0)
			errs := make([]error, workers)
			for w := range workers {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := lo + w; i < hi; i += workers {
						id, body := benchCorp.doc(i)
						d, _, err := schema.Analyze(benchMapping, id, body)
						if err != nil {
							errs[w] = err
							return
						}
						changes[i-lo] = shard.Change{Kind: shard.Upsert, Doc: &d}
					}
				}()
			}
			wg.Wait()
			for _, err := range errs {
				if err != nil {
					errBench = err
					return
				}
			}
			for i := range changes {
				seq++
				changes[i].Seq = seq
			}
			if err := s.Apply(context.Background(), changes); err != nil {
				errBench = err
				return
			}
			if err := s.Refresh(context.Background()); err != nil {
				errBench = err
				return
			}
		}
		if err := os.WriteFile(marker, []byte(time.Now().Format(time.RFC3339)), 0o600); err != nil {
			errBench = err
			return
		}
		fmt.Fprintf(os.Stderr, "built %d documents in %v at %s\n", n, time.Since(start), dir)
		benchShard = s
	})
	if errBench != nil {
		b.Fatal(errBench)
	}
	return benchShard, benchCorp
}

// runBench runs next's request b.N times on the shard and reports p50 and p99.
func runBench(b *testing.B, next func(rng *rand.Rand) *Request) {
	s, _ := openBench(b)
	rng := rand.New(rand.NewPCG(1, 2))
	// Warm up the mappings (the OS page cache) and the rank arrays, as a serving node is.
	for range 3 {
		r := next(rng)
		g := s.Acquire()
		if _, err := ExecuteShard(context.Background(), g, r); err != nil {
			g.Release()
			b.Fatal(err)
		}
		g.Release()
	}
	lat := make([]time.Duration, 0, b.N)
	var total, verified int64
	b.ResetTimer()
	for range b.N {
		r := next(rng)
		start := nanotime()
		g := s.Acquire()
		res, err := ExecuteShard(context.Background(), g, r)
		g.Release()
		if err != nil {
			b.Fatal(err)
		}
		resp := Reduce([]*ShardResult{res}, r)
		lat = append(lat, time.Duration(nanotime()-start))
		total += resp.Total
		verified += res.Scanned
	}
	b.StopTimer()
	slices.Sort(lat)
	pct := func(p float64) float64 {
		i := min(len(lat)-1, int(math.Ceil(p*float64(len(lat))))-1)
		return float64(lat[max(i, 0)].Nanoseconds()) / 1e3
	}
	b.ReportMetric(pct(0.50), "p50-µs")
	b.ReportMetric(pct(0.99), "p99-µs")
	b.ReportMetric(float64(total)/float64(b.N), "total/op")
	b.ReportMetric(float64(verified)/float64(b.N), "verified/op")
}

func leaf(field, op string, value any) *query.Leaf {
	return &query.Leaf{Field: field, Op: op, Value: mustJSON(value)}
}

func mustJSON(v any) []byte {
	s, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return s
}

// BenchmarkTermFilter: one brand (a selective keyword term), 10 hits by _id.
func BenchmarkTermFilter(b *testing.B) {
	_, c := openBench(b)
	runBench(b, func(rng *rand.Rand) *Request {
		return &Request{Query: leaf("brand", query.OpEq, c.brands[20+rng.IntN(200)]), Size: 10}
	})
}

// BenchmarkRangeAndTerm: a category and a price range, 10 hits by _id.
func BenchmarkRangeAndTerm(b *testing.B) {
	_, c := openBench(b)
	runBench(b, func(rng *rand.Rand) *Request {
		lo := float64(rng.IntN(500))
		return &Request{Query: &query.All{Children: []query.Node{
			leaf("category", query.OpEq, c.categories[rng.IntN(len(c.categories))]),
			leaf("price", query.OpBetween, []float64{lo, lo + 100}),
		}}, Size: 10}
	})
}

// BenchmarkContainsDescription: a substring of the 60-word description, a different
// one each time (a word, part of one, or two in a row), so the filter cache rarely
// holds it: the cold cost.
func BenchmarkContainsDescription(b *testing.B) {
	_, c := openBench(b)
	runBench(b, func(rng *rand.Rand) *Request {
		w := c.words[20+rng.IntN(2000)]
		var needle string
		switch rng.IntN(3) {
		case 0:
			needle = w
		case 1:
			r := []rune(w)
			i := rng.IntN(max(1, len(r)-4))
			needle = string(r[i:min(len(r), i+4+rng.IntN(3))])
		default:
			needle = w + " " + c.words[20+rng.IntN(200)]
		}
		return &Request{Query: leaf("description", query.OpContains, needle), Size: 10}
	})
}

// BenchmarkContainsRepeated: a few popular needles, searched again and again: the
// filter cache's case.
func BenchmarkContainsRepeated(b *testing.B) {
	runBench(b, func(rng *rand.Rand) *Request {
		needle := pick(rng, []string{"wireless", "stainless steel", "bluetooth", "waterproof", "rechargeable", "ceramic"})
		return &Request{Query: leaf("description", query.OpContains, needle), Size: 10}
	})
}

// BenchmarkSortByPrice: every in-stock document, the 20 cheapest (or dearest).
func BenchmarkSortByPrice(b *testing.B) {
	runBench(b, func(rng *rand.Rand) *Request {
		return &Request{
			Query: leaf("in_stock", query.OpEq, true),
			Sort:  []SortField{{Field: "price", Desc: rng.IntN(2) == 0}},
			Size:  20,
		}
	})
}

// BenchmarkDeepSearchAfter: 20 hits by price from a cursor deep in the result.
func BenchmarkDeepSearchAfter(b *testing.B) {
	runBench(b, func(rng *rand.Rand) *Request {
		p := math.Round(math.Exp(3+rng.Float64()*3)*100) / 100 // a cursor 40-85% of the way in
		return &Request{
			Query:       &query.All{},
			Sort:        []SortField{{Field: "price"}},
			Size:        20,
			SearchAfter: []any{p, fmt.Sprintf("p%07d", rng.IntN(benchDocs()))},
		}
	})
}

// BenchmarkDeepSearchAfterByID: the default _id order from a cursor deep in.
func BenchmarkDeepSearchAfterByID(b *testing.B) {
	runBench(b, func(rng *rand.Rand) *Request {
		return &Request{Query: &query.All{}, Size: 20, SearchAfter: []any{fmt.Sprintf("p%07d", rng.IntN(benchDocs()))}}
	})
}

// BenchmarkTermsAggAll: the top brands and categories over every document.
func BenchmarkTermsAggAll(b *testing.B) {
	runBench(b, func(_ *rand.Rand) *Request {
		return &Request{Query: &query.All{}, Aggs: map[string]Agg{
			"brands":     {Type: AggTerms, Field: "brand", Size: 10},
			"categories": {Type: AggTerms, Field: "category", Size: 10},
		}}
	})
}

// BenchmarkTermsAggFiltered: the top brands of a price range, with price stats each.
func BenchmarkTermsAggFiltered(b *testing.B) {
	runBench(b, func(rng *rand.Rand) *Request {
		lo := float64(rng.IntN(200))
		return &Request{Query: leaf("price", query.OpBetween, []float64{lo, lo + 300}), Aggs: map[string]Agg{
			"brands": {Type: AggTerms, Field: "brand", Size: 10, Aggs: map[string]Agg{"p": {Type: AggStats, Field: "price"}}},
		}}
	})
}

// BenchmarkStartsWithShortPrefix: a prefix shared by many unique values (sku), from
// every document to a few thousand.
func BenchmarkStartsWithShortPrefix(b *testing.B) {
	runBench(b, func(rng *rand.Rand) *Request {
		p := pick(rng, []string{"s", "sku-0", "sku-00", fmt.Sprintf("sku-%03d", rng.IntN(1000))})
		return &Request{Query: leaf("sku", query.OpStartsWith, p), Size: 10}
	})
}

// BenchmarkContainsNoTotal: the cold contains, with track_total false: only what the
// top hits need is verified.
func BenchmarkContainsNoTotal(b *testing.B) {
	_, c := openBench(b)
	runBench(b, func(rng *rand.Rand) *Request {
		needle := c.words[20+rng.IntN(2000)]
		return &Request{Query: leaf("description", query.OpContains, needle), Size: 10, TrackTotal: TrackTotalNone}
	})
}

// BenchmarkTermsAggUnique: a terms aggregation over a field of unique values (sku).
func BenchmarkTermsAggUnique(b *testing.B) {
	runBench(b, func(_ *rand.Rand) *Request {
		return &Request{Query: &query.All{}, Aggs: map[string]Agg{"skus": {Type: AggTerms, Field: "sku", Size: 10}}}
	})
}
