package percolate

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// Percolation benchmarks over scrape-bot-shaped data: a product catalogue (store,
// brand, category, title with a model number, price, tags, stock, condition) and saved
// searches mixing keyword eq/in, number ranges, contains/words on the title and some
// nots. A shard indexes 20k catalogue documents first (the term statistics the anchors
// are chosen by), then the saved queries; the benchmark percolates other documents of
// the same distribution.
//
//	go test -run '^$' -bench BenchmarkPercolate -benchtime 4000x ./internal/percolate/
//
// BenchmarkPercolate/queries=N/doc percolates one document per call (the per-document
// latency: p50 and p99 are reported as metrics); .../batch percolates 1000 documents
// per call on every core (throughput, in docs/s).

var benchCatalog = func() (c struct {
	stores, brands, categories, tags, adjectives, models []string
},
) {
	r := rand.New(rand.NewPCG(11, 12))
	for i := range 20 {
		c.stores = append(c.stores, fmt.Sprintf("store%02d", i))
	}
	for i := range 60 {
		c.brands = append(c.brands, fmt.Sprintf("Brand%02d", i))
	}
	for i := range 200 {
		c.categories = append(c.categories, fmt.Sprintf("category %03d", i))
	}
	for i := range 30 {
		c.tags = append(c.tags, fmt.Sprintf("tag%02d", i))
	}
	c.tags = append(c.tags, "refurbished", "open box", "clearance")
	for i := range 120 {
		c.adjectives = append(c.adjectives, fmt.Sprintf("%s%c%c", []string{"ultra", "pro", "mini", "max", "slim", "quiet", "fast", "smart"}[i%8], 'a'+rune(i/26%26), 'a'+rune(i%26)))
	}
	prefixes := []string{"rtx", "gx", "xps", "kx-", "a", "sm-", "ws", "zen", "mx", "qn"}
	seen := map[string]bool{}
	for len(c.models) < 5000 {
		m := fmt.Sprintf("%s%d", prefixes[r.IntN(len(prefixes))], 100+r.IntN(9900))
		if !seen[m] {
			seen[m] = true
			c.models = append(c.models, m)
		}
	}
	return c
}()

type benchGen struct{ r *rand.Rand }

func (g *benchGen) pick(list []string) string { return list[g.r.IntN(len(list))] }

func (g *benchGen) doc(i int) (string, []byte) {
	c := &benchCatalog
	title := fmt.Sprintf("%s %s %s %s %s", g.pick(c.brands), g.pick(c.models), g.pick(c.adjectives), g.pick(c.adjectives), g.pick(c.categories))
	tags := []string{g.pick(c.tags)}
	if g.r.IntN(2) == 0 {
		tags = append(tags, g.pick(c.tags))
	}
	conditions := []string{"new", "new", "new", "used", "refurbished"}
	body, _ := json.Marshal(map[string]any{
		"store":     g.pick(c.stores),
		"brand":     g.pick(c.brands),
		"category":  g.pick(c.categories),
		"title":     title,
		"price":     float64(5+g.r.IntN(300000)) / 100,
		"tags":      tags,
		"in_stock":  g.r.IntN(4) != 0,
		"condition": conditions[g.r.IntN(len(conditions))],
	})
	return fmt.Sprintf("p%07d", i), body
}

// query returns one saved search, in the shapes scrape-bot users write.
func (g *benchGen) query() string {
	c := &benchCatalog
	price := func() int { return 10 + g.r.IntN(2500) }
	q := func(field, op string, value any) map[string]any {
		m := map[string]any{"field": field, "op": op}
		if value != nil {
			m["value"] = value
		}
		return m
	}
	var n map[string]any
	switch r := g.r.IntN(100); {
	case r < 30: // a model at a store under a price
		n = map[string]any{"all": []any{q("store", "eq", g.pick(c.stores)), q("title", "contains", g.pick(c.models)), q("price", "lte", price())}}
	case r < 45: // some brands, some words, a price band
		lo := price()
		n = map[string]any{"all": []any{
			q("brand", "in", []string{g.pick(c.brands), g.pick(c.brands), g.pick(c.brands)}),
			q("title", "words_any", []string{g.pick(c.adjectives), g.pick(c.adjectives)}),
			q("price", "between", []int{lo, lo + 200 + g.r.IntN(800)}),
		}}
	case r < 60: // a category over a price, not refurbished
		n = map[string]any{"all": []any{q("category", "eq", g.pick(c.categories)), q("price", "gte", price()), map[string]any{"not": q("tags", "has", "refurbished")}}}
	case r < 70: // tags at a store, in stock, under a price
		n = map[string]any{"all": []any{
			q("tags", "has_any", []string{g.pick(c.tags), g.pick(c.tags)}), q("store", "eq", g.pick(c.stores)),
			q("in_stock", "eq", true), q("price", "lt", price()),
		}}
	case r < 80: // either of two models at a store
		n = map[string]any{"all": []any{q("store", "eq", g.pick(c.stores)), map[string]any{"any": []any{q("title", "contains", g.pick(c.models)), q("title", "contains", g.pick(c.models))}}}}
	case r < 90: // a phrase from a brand
		n = map[string]any{"all": []any{q("title", "words_all", g.pick(c.adjectives)+" "+g.pick(c.adjectives)), q("brand", "eq", g.pick(c.brands))}}
	case r < 95: // a category, not used, under a price
		n = map[string]any{"all": []any{map[string]any{"not": q("condition", "eq", "used")}, q("price", "lt", price()), q("category", "eq", g.pick(c.categories))}}
	case r < 99: // a model prefix, not from one store
		n = map[string]any{"all": []any{q("title", "starts_with", g.pick(c.brands)+" "+g.pick(c.models)[:3]), map[string]any{"not": q("store", "eq", g.pick(c.stores))}}}
	default: // a catch-all deal alert: anything not used under a price, anchored on the range alone
		n = map[string]any{"all": []any{map[string]any{"not": q("condition", "eq", "used")}, q("price", "lt", price())}}
	}
	raw, _ := json.Marshal(n)
	return string(raw)
}

type benchEnv struct {
	s    *shard.Shard
	g    *shard.Generation
	docs []schema.Doc
}

func newBenchEnv(b *testing.B, numQueries int) *benchEnv {
	b.Helper()
	ctx := context.Background()
	m := &schema.Mapping{Fields: map[string]schema.FieldType{
		"store": schema.Keyword, "brand": schema.Keyword, "category": schema.Keyword, "title": schema.Text,
		"price": schema.Number, "tags": schema.KeywordList, "in_stock": schema.Bool, "condition": schema.Keyword,
	}}
	s, err := shard.Open(ctx, b.TempDir(), m, shard.Options{QueryIndex: Index{}, RefreshInterval: -1, FlushBytes: -1, DisableMerges: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close(ctx) })
	g := &benchGen{r: rand.New(rand.NewPCG(uint64(numQueries), 1))}
	var seq int64
	changes := make([]shard.Change, 0, 20000)
	for i := range 20000 {
		id, body := g.doc(i)
		d, _, err := schema.Analyze(m, id, body)
		if err != nil {
			b.Fatal(err)
		}
		seq++
		changes = append(changes, shard.Change{Seq: seq, Kind: shard.Upsert, Doc: &d})
	}
	if err := s.Apply(ctx, changes); err != nil {
		b.Fatal(err)
	}
	if err := s.Refresh(ctx); err != nil {
		b.Fatal(err)
	}
	changes = changes[:0]
	for i := range numQueries {
		n, problems := query.Parse([]byte(g.query()))
		if len(problems) > 0 {
			b.Fatal(problems)
		}
		seq++
		changes = append(changes, shard.Change{Seq: seq, Kind: shard.QueryUpsert, QueryID: fmt.Sprintf("search-%06d", i), Query: n})
	}
	if err := s.Apply(ctx, changes); err != nil {
		b.Fatal(err)
	}
	if err := s.Refresh(ctx); err != nil {
		b.Fatal(err)
	}
	env := &benchEnv{s: s, g: s.Acquire()}
	b.Cleanup(env.g.Release)
	for i := range 4000 {
		id, body := g.doc(1_000_000 + i)
		d, _, err := schema.Analyze(m, id, body)
		if err != nil {
			b.Fatal(err)
		}
		env.docs = append(env.docs, d)
	}
	return env
}

func BenchmarkPercolate(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("queries=%dk", n/1000), func(b *testing.B) {
			env := newBenchEnv(b, n)
			p := New(Options{})
			ctx := context.Background()
			// Warm up: compile every query a document of the distribution reaches.
			if _, err := p.Percolate(ctx, env.g, env.docs); err != nil {
				b.Fatal(err)
			}
			b.Run("doc", func(b *testing.B) {
				var lat []time.Duration
				matches, cands := 0, 0
				i := 0
				sc := new(scratch)
				for b.Loop() {
					d := env.docs[i%len(env.docs) : i%len(env.docs)+1]
					i++
					start := clockNow()
					out, err := p.Percolate(ctx, env.g, d)
					lat = append(lat, clockSince(start))
					if err != nil {
						b.Fatal(err)
					}
					matches += len(out[0])
					_ = sc
				}
				slices.Sort(lat)
				pct := func(q float64) float64 { return float64(lat[int(q*float64(len(lat)-1))].Nanoseconds()) / 1000 }
				b.ReportMetric(pct(0.50), "p50-µs")
				b.ReportMetric(pct(0.99), "p99-µs")
				b.ReportMetric(float64(matches)/float64(len(lat)), "matches/doc")
				for qi := range env.g.QuerySegments {
					if seg, ok := env.g.QuerySegments[qi].Segment.(*Segment); ok {
						sc.fit(seg.NumQueries())
						for _, d := range env.docs[:500] {
							seg.collect(&d, sc)
							cands += len(sc.cands)
							sc.reset()
						}
						b.ReportMetric(float64(seg.NumAlways()), "always-check")
					}
				}
				b.ReportMetric(float64(cands)/500, "candidates/doc")
			})
			b.Run("batch", func(b *testing.B) {
				const batch = 1000
				i, docs := 0, 0
				start := time.Now()
				for b.Loop() {
					lo := (i * batch) % len(env.docs)
					i++
					if _, err := p.Percolate(ctx, env.g, env.docs[lo:lo+batch]); err != nil {
						b.Fatal(err)
					}
					docs += batch
				}
				b.ReportMetric(float64(docs)/time.Since(start).Seconds(), "docs/s")
			})
		})
	}
}

// BenchmarkPercolateAnalyze is one document percolated from its JSON body (analysis
// included) against 100k queries.
func BenchmarkPercolateAnalyze(b *testing.B) {
	env := newBenchEnv(b, 100000)
	p := New(Options{})
	ctx := context.Background()
	raw := make([]schema.Doc, len(env.docs))
	for i, d := range env.docs {
		raw[i] = schema.Doc{ID: d.ID, Body: d.Body}
	}
	if _, err := p.Percolate(ctx, env.g, raw); err != nil {
		b.Fatal(err)
	}
	i := 0
	for b.Loop() {
		if _, err := p.Percolate(ctx, env.g, raw[i%len(raw):i%len(raw)+1]); err != nil {
			b.Fatal(err)
		}
		i++
	}
}
