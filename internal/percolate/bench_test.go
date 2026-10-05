package percolate

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
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

// benchConfig is one benchmark variant.
type benchConfig struct {
	queries int
	// long: titles of about 40 words, a 3,000-word description and a url on every
	// document, and saved searches on them.
	long bool
	// segments: the saved searches arrive in this many refreshes (query segments);
	// 0 is one.
	segments int
}

// filler is the description vocabulary.
var filler = func() []string {
	out := make([]string, 0, 600)
	for i := range 600 {
		out = append(out, fmt.Sprintf("%s%d", []string{"the", "with", "for", "and", "port", "cable", "design", "power", "quality", "shipping"}[i%10], i/10))
	}
	return out
}()

type benchGen struct {
	r    *rand.Rand
	long bool
}

func (g *benchGen) pick(list []string) string { return list[g.r.IntN(len(list))] }

func (g *benchGen) doc(i int) (string, []byte) {
	c := &benchCatalog
	store, model := g.pick(c.stores), g.pick(c.models)
	title := fmt.Sprintf("%s %s %s %s %s", g.pick(c.brands), model, g.pick(c.adjectives), g.pick(c.adjectives), g.pick(c.categories))
	tags := []string{g.pick(c.tags)}
	if g.r.IntN(2) == 0 {
		tags = append(tags, g.pick(c.tags))
	}
	conditions := []string{"new", "new", "new", "used", "refurbished"}
	fields := map[string]any{
		"store":     store,
		"brand":     g.pick(c.brands),
		"category":  g.pick(c.categories),
		"title":     title,
		"price":     float64(5+g.r.IntN(300000)) / 100,
		"tags":      tags,
		"in_stock":  g.r.IntN(4) != 0,
		"condition": conditions[g.r.IntN(len(conditions))],
	}
	if g.long {
		var t, d strings.Builder
		t.WriteString(title)
		for range 35 {
			t.WriteString(" " + g.pick(c.adjectives))
		}
		for w := range 3000 {
			switch {
			case w%300 == 0:
				d.WriteString(g.pick(c.models))
			case w%7 == 0:
				d.WriteString(g.pick(c.adjectives))
			default:
				d.WriteString(g.pick(filler))
			}
			d.WriteByte(' ')
		}
		fields["title"] = t.String()
		fields["description"] = d.String()
		fields["url"] = fmt.Sprintf("https://www.%s.example/p/%s-%d?ref=search&utm_source=feed", store, model, i)
	}
	body, _ := json.Marshal(fields)
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
	r := g.r.IntN(100)
	if g.long && r < 10 { // searches on the description and the url
		switch {
		case r < 5:
			n = map[string]any{"all": []any{q("description", "contains", g.pick(c.models)), q("store", "eq", g.pick(c.stores))}}
		case r < 8:
			n = map[string]any{"all": []any{q("url", "starts_with", "https://www."+g.pick(c.stores)+".example/p/"), q("title", "contains", g.pick(c.models))}}
		default:
			n = map[string]any{"all": []any{q("description", "words_all", g.pick(c.adjectives)+" "+g.pick(filler)), q("brand", "eq", g.pick(c.brands))}}
		}
		raw, _ := json.Marshal(n)
		return string(raw)
	}
	switch {
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

var benchMapping = &schema.Mapping{Fields: map[string]schema.FieldType{
	"store": schema.Keyword, "brand": schema.Keyword, "category": schema.Keyword, "title": schema.Text,
	"price": schema.Number, "tags": schema.KeywordList, "in_stock": schema.Bool, "condition": schema.Keyword,
	"description": schema.Text, "url": schema.Keyword,
}}

type benchEnv struct {
	s    *shard.Shard
	g    *shard.Generation
	docs []schema.Doc
}

// regenerate releases the held generation and acquires the current one.
func (env *benchEnv) regenerate() {
	env.g.Release()
	env.g = env.s.Acquire()
}

func newBenchEnv(b *testing.B, cfg benchConfig) *benchEnv {
	b.Helper()
	ctx := context.Background()
	s, err := shard.Open(ctx, b.TempDir(), benchMapping, shard.Options{QueryIndex: Index{}, RefreshInterval: -1, RefreshBytes: -1, DisableMerges: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = s.Close(ctx) })
	g := &benchGen{r: rand.New(rand.NewPCG(uint64(cfg.queries), 1)), long: cfg.long}
	var seq int64
	numDocs := 20000
	if cfg.long {
		numDocs = 5000 // statistics only; long documents are big
	}
	changes := make([]shard.Change, 0, numDocs)
	for i := range numDocs {
		id, body := g.doc(i)
		d, _, err := schema.Analyze(benchMapping, id, body)
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
	segments := max(1, cfg.segments)
	for part := range segments {
		changes = changes[:0]
		for i := part * cfg.queries / segments; i < (part+1)*cfg.queries/segments; i++ {
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
	}
	env := &benchEnv{s: s, g: s.Acquire()}
	b.Cleanup(func() { env.g.Release() })
	numTest := 4000
	if cfg.long {
		numTest = 1000
	}
	for i := range numTest {
		id, body := g.doc(1_000_000 + i)
		d, _, err := schema.Analyze(benchMapping, id, body)
		if err != nil {
			b.Fatal(err)
		}
		env.docs = append(env.docs, d)
	}
	return env
}

func reportLatency(b *testing.B, lat []time.Duration) {
	b.Helper()
	slices.Sort(lat)
	pct := func(q float64) float64 { return float64(lat[int(q*float64(len(lat)-1))].Nanoseconds()) / 1000 }
	b.ReportMetric(pct(0.50), "p50-µs")
	b.ReportMetric(pct(0.99), "p99-µs")
}

// benchDoc is the per-document latency of env: p50 and p99, matches and candidates.
func benchDoc(b *testing.B, env *benchEnv, p *Percolator) {
	b.Helper()
	ctx := context.Background()
	var lat []time.Duration
	matches, i := 0, 0
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
	}
	reportLatency(b, lat)
	b.ReportMetric(float64(matches)/float64(len(lat)), "matches/doc")
	sc := new(scratch)
	cands, always := 0, 0
	sample := env.docs[:min(500, len(env.docs))]
	for qi := range env.g.QuerySegments {
		if seg, ok := env.g.QuerySegments[qi].Segment.(*Segment); ok {
			sc.fit(seg.NumQueries(), seg.NumEntries(), len(seg.fields))
			for _, d := range sample {
				seg.collect(&d, sc)
				cands += len(sc.cands)
				sc.reset()
			}
			always += seg.NumAlways()
		}
	}
	b.ReportMetric(float64(always), "always-check")
	b.ReportMetric(float64(cands)/float64(len(sample)), "candidates/doc")
}

func benchBatch(b *testing.B, env *benchEnv, p *Percolator) {
	b.Helper()
	const batch = 1000
	i, docs := 0, 0
	start := time.Now()
	for b.Loop() {
		lo := (i * batch) % len(env.docs)
		i++
		hi := min(lo+batch, len(env.docs))
		if _, err := p.Percolate(context.Background(), env.g, env.docs[lo:hi]); err != nil {
			b.Fatal(err)
		}
		docs += hi - lo
	}
	b.ReportMetric(float64(docs)/time.Since(start).Seconds(), "docs/s")
}

// BenchmarkPercolate is the default shape at 1k, 10k and 100k saved searches.
func BenchmarkPercolate(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("queries=%dk", n/1000), func(b *testing.B) {
			env := newBenchEnv(b, benchConfig{queries: n})
			p := New(Options{})
			if _, err := p.Percolate(context.Background(), env.g, env.docs); err != nil { // warm
				b.Fatal(err)
			}
			b.Run("doc", func(b *testing.B) { benchDoc(b, env, p) })
			b.Run("batch", func(b *testing.B) { benchBatch(b, env, p) })
		})
	}
}

// BenchmarkPercolateVariants is 100k saved searches in the other shapes: long titles,
// a 3,000-word description and a url on every document (with searches on them), and
// the default searches spread over 10 query segments.
func BenchmarkPercolateVariants(b *testing.B) {
	for _, v := range []struct {
		name string
		cfg  benchConfig
	}{
		{"long", benchConfig{queries: 100000, long: true}},
		{"segments=10", benchConfig{queries: 100000, segments: 10}},
	} {
		b.Run(v.name, func(b *testing.B) {
			env := newBenchEnv(b, v.cfg)
			p := New(Options{})
			if _, err := p.Percolate(context.Background(), env.g, env.docs); err != nil {
				b.Fatal(err)
			}
			b.Run("doc", func(b *testing.B) { benchDoc(b, env, p) })
			b.Run("batch", func(b *testing.B) { benchBatch(b, env, p) })
		})
	}
}

// BenchmarkPercolateCold is the first 500 documents percolated one by one right after
// a merge of 10 query segments into one (100k saved searches), on a segment just
// mapped. Each iteration merges once: run it with -benchtime 1x.
func BenchmarkPercolateCold(b *testing.B) {
	env := newBenchEnv(b, benchConfig{queries: 100000, segments: 10})
	p := New(Options{})
	ctx := context.Background()
	var lat []time.Duration
	for b.Loop() {
		if err := env.s.ForceMerge(ctx, 1); err != nil {
			b.Fatal(err)
		}
		env.regenerate()
		merged := time.Now()
		for i := range 500 {
			start := clockNow()
			if _, err := p.Percolate(ctx, env.g, env.docs[i:i+1]); err != nil {
				b.Fatal(err)
			}
			lat = append(lat, clockSince(start))
		}
		b.ReportMetric(float64(time.Since(merged).Milliseconds()), "first500-ms")
	}
	reportLatency(b, lat)
}

// BenchmarkPercolatePairsLongDoc is the pair-probing worst case: 20k searches
// all[words_all w<i>, eq brand B] and a document holding B and m of the words.
func BenchmarkPercolatePairsLongDoc(b *testing.B) {
	seg := pairHeavySegment(b, 20000)
	for _, m := range []int{100, 1000, 3000} {
		b.Run(fmt.Sprintf("words=%d", m), func(b *testing.B) {
			d := wordsDoc(b, m)
			sc := new(scratch)
			sc.fit(seg.NumQueries(), seg.NumEntries(), len(seg.fields))
			v := &view{seg: seg, n: seg.NumQueries()}
			b.Run("probe", func(b *testing.B) {
				for b.Loop() {
					seg.collect(&d, sc)
					sc.reset()
				}
			})
			// Verifying is m true matches, each a words_all scan of the m-word text:
			// that cost is the matcher's and the output's, not the index's.
			b.Run("percolate", func(b *testing.B) {
				for b.Loop() {
					seg.collect(&d, sc)
					var st docStats
					verify(v, sc, &st)
					sc.results()
					sc.reset()
				}
			})
		})
	}
}

// BenchmarkPercolateAnalyze is one document percolated from its JSON body (analysis
// included) against 100k queries.
func BenchmarkPercolateAnalyze(b *testing.B) {
	env := newBenchEnv(b, benchConfig{queries: 100000})
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
