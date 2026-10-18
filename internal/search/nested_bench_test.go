package search

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// Nested fields against the flat layout they replace: the same variants, as one
// document per product with its variants nested, or one document per variant with
// the product's fields copied onto it. SEARCHLIGHT_BENCH_PRODUCTS sets the products
// (default 20000, five variants each), built in a temp dir, one merged segment each.
//
//	go test ./internal/search -run '^$' -bench 'Nested|Flat' -benchtime 200x

var (
	nestedBenchMapping = &schema.Mapping{Fields: map[string]schema.FieldType{
		"brand": schema.Keyword, "_v": schema.Nested, "_v.size": schema.Keyword, "_v.price": schema.Number, "_v.in_stock": schema.Bool,
	}}
	flatBenchMapping = &schema.Mapping{Fields: map[string]schema.FieldType{
		"brand": schema.Keyword, "product": schema.Keyword, "size": schema.Keyword, "price": schema.Number, "in_stock": schema.Bool,
	}}
)

func benchProducts() int {
	if s := os.Getenv("SEARCHLIGHT_BENCH_PRODUCTS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			return n
		}
	}
	return 20_000
}

// variantBenchShard builds one shard of docs under m, merged to one segment.
func variantBenchShard(b *testing.B, m *schema.Mapping, docs []schema.Doc) *shard.Shard {
	b.Helper()
	s, err := shard.Open(context.Background(), b.TempDir(), m, shard.Options{RefreshInterval: -1, DisableMerges: true, Logger: quiet})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close(context.Background()) })
	changes := make([]shard.Change, len(docs))
	for i := range docs {
		changes[i] = shard.Change{Seq: int64(i + 1), Kind: shard.Upsert, Doc: &docs[i]}
	}
	if err := s.Apply(context.Background(), changes); err != nil {
		b.Fatal(err)
	}
	if err := s.Refresh(context.Background()); err != nil {
		b.Fatal(err)
	}
	if err := s.ForceMerge(context.Background(), 1); err != nil {
		b.Fatal(err)
	}
	return s
}

// variantShard builds the products, nested (one document each) or flat (one per
// variant), into a shard of their own.
func variantShard(b *testing.B, nestedLayout bool) *shard.Shard {
	b.Helper()
	rng := rand.New(rand.NewPCG(5, 5))
	sizes := []string{"XS", "S", "M", "L", "XL"}
	analyze := func(m *schema.Mapping, id, body string) schema.Doc {
		d, _, err := schema.Analyze(m, id, []byte(body))
		if err != nil {
			b.Fatal(err)
		}
		return d
	}
	var docs []schema.Doc
	for p := range benchProducts() {
		brand := fmt.Sprintf("brand%d", rng.IntN(200))
		var variants []string
		for k := range 5 {
			size, price, stock := sizes[k], 1000+rng.IntN(30000), rng.IntN(3) > 0
			variants = append(variants, fmt.Sprintf(`{"size":%q,"price":%d,"in_stock":%t}`, size, price, stock))
			if !nestedLayout {
				docs = append(docs, analyze(flatBenchMapping, fmt.Sprintf("p%d-%d", p, k),
					fmt.Sprintf(`{"brand":%q,"product":"p%d","size":%q,"price":%d,"in_stock":%t}`, brand, p, size, price, stock)))
			}
		}
		if nestedLayout {
			docs = append(docs, analyze(nestedBenchMapping, fmt.Sprintf("p%d", p), fmt.Sprintf(`{"brand":%q,"_v":[%s]}`, brand, strings.Join(variants, ","))))
		}
	}
	if nestedLayout {
		return variantBenchShard(b, nestedBenchMapping, docs)
	}
	return variantBenchShard(b, flatBenchMapping, docs)
}

func runVariantBench(b *testing.B, s *shard.Shard, r *Request) {
	b.Helper()
	g := s.Acquire()
	defer g.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := ExecuteShard(context.Background(), g, r); err != nil {
			b.Fatal(err)
		}
	}
}

func variantQuery(b *testing.B, raw string) query.Node {
	b.Helper()
	n, ps := query.Parse([]byte(raw))
	if len(ps) > 0 {
		b.Fatal(ps)
	}
	return n
}

// The correlated query: size L, in stock and under 150.00 on one variant.
func BenchmarkNestedQuery(b *testing.B) {
	runVariantBench(b, variantShard(b, true), &Request{Query: variantQuery(b, `{"nested":{"path":"_v","query":{"all":[{"field":"_v.size","op":"eq","value":"L"},{"field":"_v.in_stock","op":"eq","value":true},{"field":"_v.price","op":"lt","value":15000}]}}}`), Size: 20, TrackTotal: TrackTotalAll})
}

func BenchmarkFlatQuery(b *testing.B) {
	runVariantBench(b, variantShard(b, false), &Request{Query: variantQuery(b, `{"all":[{"field":"size","op":"eq","value":"L"},{"field":"in_stock","op":"eq","value":true},{"field":"price","op":"lt","value":15000}]}`), Size: 20, TrackTotal: TrackTotalAll})
}

// Sizes in stock, counting products (nested: once per product; flat: once per
// variant, which here is the same as each product has one variant of a size).
func BenchmarkNestedTerms(b *testing.B) {
	runVariantBench(b, variantShard(b, true), &Request{Query: &query.All{}, Aggs: map[string]Agg{"sizes": {
		Type: AggTerms, Field: "_v.size", Where: variantQuery(b, `{"field":"_v.in_stock","op":"eq","value":true}`),
	}}})
}

func BenchmarkFlatTerms(b *testing.B) {
	runVariantBench(b, variantShard(b, false), &Request{Query: variantQuery(b, `{"field":"in_stock","op":"eq","value":true}`), Aggs: map[string]Agg{"sizes": {Type: AggTerms, Field: "size"}}})
}

// Products by their cheapest variant in stock.
func BenchmarkNestedSort(b *testing.B) {
	runVariantBench(b, variantShard(b, true), &Request{Query: &query.All{}, Size: 20, Sort: []SortField{{Field: "_v.price", Where: variantQuery(b, `{"field":"_v.in_stock","op":"eq","value":true}`)}}})
}

func BenchmarkFlatSort(b *testing.B) {
	runVariantBench(b, variantShard(b, false), &Request{Query: variantQuery(b, `{"field":"in_stock","op":"eq","value":true}`), Size: 20, Sort: []SortField{{Field: "price"}}})
}
