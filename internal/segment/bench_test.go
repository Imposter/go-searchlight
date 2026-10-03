package segment

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// benchMapping and genCorpus build scrape-bot-shaped documents: about 15 fields and a
// 60-word description, as the plan's benchmark target asks for.
func benchMapping() *schema.Mapping {
	return &schema.Mapping{Fields: map[string]schema.FieldType{
		"title":       schema.Text,
		"description": schema.Text,
		"brand":       schema.Keyword,
		"category":    schema.Keyword,
		"vendor":      schema.Keyword,
		"sku":         schema.Keyword,
		"url":         schema.Keyword,
		"tags":        schema.KeywordList,
		"price":       schema.Number,
		"sale_price":  schema.Number,
		"rating":      schema.Number,
		"stock":       schema.Number,
		"in_stock":    schema.Bool,
		"featured":    schema.Bool,
		"created":     schema.Date,
		"updated":     schema.Date,
	}}
}

// descPool is a few thousand distinct synthetic words: large enough that picking 60 of
// them per document, pseudo-randomly, gives each word a plausible real-catalog
// repetition rate (every word in a real product description - "wireless", "stainless",
// a model number - tends to recur across some sizeable fraction of a catalog, but not
// across all of it) instead of either extreme: a single fixed sentence reused for
// every document (so every word AND every trigram is shared by all 20,000 documents -
// pathologically low cardinality for the occurrence count, which makes a sorted-pairs
// term dictionary look artificially worse than a map) or words so unique that nothing
// is ever shared (unrealistically high cardinality, which would make a map's hashing
// look artificially worse than it does in practice).
var descPool = func() []string {
	words := make([]string, 4000)
	for i := range words {
		words[i] = fmt.Sprintf("tok%d", i)
	}
	return words
}()

func genDescription(i int) string {
	// A small, fast PRNG (splitmix64) seeded from the document index, not
	// math/rand, so genCorpus stays allocation-free and deterministic across runs.
	state := uint64(i)*0x9E3779B97F4A7C15 + 1
	next := func() uint64 {
		state += 0x9E3779B97F4A7C15
		z := state
		z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9
		z = (z ^ (z >> 27)) * 0x94D049BB133111EB
		return z ^ (z >> 31)
	}
	words := make([]string, 60)
	for j := range words {
		words[j] = descPool[next()%uint64(len(descPool))]
	}
	return strings.Join(words, " ")
}

func genCorpus(n int) []schema.Doc {
	m := benchMapping()
	docs := make([]schema.Doc, n)
	for i := range n {
		desc := genDescription(i)
		body := fmt.Sprintf(`{
			"title": "Product number %d for testing",
			"description": %q,
			"brand": "Brand%d",
			"category": "Category%d",
			"vendor": "Vendor%d",
			"sku": "SKU-%08d",
			"url": "https://example.com/p/%d",
			"tags": ["tag%d", "tag%d", "sale"],
			"price": %d.99,
			"sale_price": %d.50,
			"rating": %d.5,
			"stock": %d,
			"in_stock": %t,
			"featured": %t,
			"created": "2026-01-02T03:04:05Z",
			"updated": "2026-06-07T08:09:10Z"
		}`, i, desc, i%50, i%20, i%10, i, i, i%7, (i+1)%7, 10+i%100, 5+i%50, 1+i%5, i%1000, i%3 == 0, i%11 == 0)
		docs[i] = mustAnalyzeBench(m, fmt.Sprintf("doc-%d", i), body)
	}
	return docs
}

func mustAnalyzeBench(m *schema.Mapping, id, body string) schema.Doc {
	doc, _, err := schema.Analyze(m, id, []byte(body))
	if err != nil {
		panic(err)
	}
	return doc
}

// BenchmarkBuild reports docs/s and bytes/doc for sequential (Threads: 0) and threaded
// (Threads: runtime.NumCPU()) Build, as sub-benchmarks: run with
// -bench BenchmarkBuild to get both.
func BenchmarkBuild(b *testing.B) {
	for _, tc := range []struct {
		name    string
		threads int
	}{
		{"Sequential", 0},
		{"Threaded", runtime.NumCPU()},
	} {
		b.Run(tc.name, func(b *testing.B) {
			docs := genCorpus(20000)
			dir := b.TempDir()
			var lastPath string
			b.ResetTimer()
			for range b.N {
				meta, err := Build(dir, docs, BuildOptions{Threads: tc.threads})
				if err != nil {
					b.Fatal(err)
				}
				lastPath = meta.Path
			}
			elapsed := b.Elapsed().Seconds()
			b.ReportMetric(float64(len(docs))*float64(b.N)/elapsed, "docs/s")
			if info, err := os.Stat(lastPath); err == nil {
				b.ReportMetric(float64(info.Size())/float64(len(docs)), "bytes/doc")
			}
		})
	}
}

func BenchmarkPostingsLookup(b *testing.B) {
	docs := genCorpus(20000)
	dir := b.TempDir()
	meta, err := Build(dir, docs, BuildOptions{})
	if err != nil {
		b.Fatal(err)
	}
	r, err := Open(meta.Path)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()
	b.ResetTimer()
	for range b.N {
		r.Postings("brand", KindValue, "brand17")
	}
}

func BenchmarkRange(b *testing.B) {
	docs := genCorpus(20000)
	dir := b.TempDir()
	meta, err := Build(dir, docs, BuildOptions{})
	if err != nil {
		b.Fatal(err)
	}
	r, err := Open(meta.Path)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()
	nums := r.Numbers("stock")
	b.ResetTimer()
	for range b.N {
		nums.Range(100, 200, true, false)
	}
}

func BenchmarkStoredFetch(b *testing.B) {
	docs := genCorpus(20000)
	dir := b.TempDir()
	meta, err := Build(dir, docs, BuildOptions{})
	if err != nil {
		b.Fatal(err)
	}
	r, err := Open(meta.Path)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()
	n := uint32(len(docs))
	b.ResetTimer()
	for i := range b.N {
		if _, err := r.Stored(uint32(i) % n); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStoredFetchParallel drives Stored from many goroutines at once (as Task 6's
// concurrent hit fetches will), with each hitting a random ordinal rather than the
// sequential access BenchmarkStoredFetch exercises - the pattern that showed a single
// mutex-guarded decoder cache thrashing under review.
func BenchmarkStoredFetchParallel(b *testing.B) {
	docs := genCorpus(20000)
	dir := b.TempDir()
	meta, err := Build(dir, docs, BuildOptions{})
	if err != nil {
		b.Fatal(err)
	}
	r, err := Open(meta.Path)
	if err != nil {
		b.Fatal(err)
	}
	defer r.Close()
	n := uint32(len(docs))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// A distinct pseudo-random sequence per goroutine, deterministic enough to
		// need no synchronization of its own.
		x := uint32(1) + uint32(os.Getpid())
		for pb.Next() {
			x = x*1664525 + 1013904223 // a small, fast LCG; only used to pick an ordinal
			if _, err := r.Stored(x % n); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkMerge(b *testing.B) {
	docs := genCorpus(20000)
	half := len(docs) / 2
	dir := b.TempDir()
	metaA, err := Build(dir, docs[:half], BuildOptions{})
	if err != nil {
		b.Fatal(err)
	}
	metaB, err := Build(dir, docs[half:], BuildOptions{})
	if err != nil {
		b.Fatal(err)
	}
	rA, err := Open(metaA.Path)
	if err != nil {
		b.Fatal(err)
	}
	defer rA.Close()
	rB, err := Open(metaB.Path)
	if err != nil {
		b.Fatal(err)
	}
	defer rB.Close()
	b.ResetTimer()
	for range b.N {
		if _, err := Merge(dir, []*Reader{rA, rB}, nil, MergeOptions{}); err != nil {
			b.Fatal(err)
		}
	}
	elapsed := b.Elapsed().Seconds()
	b.ReportMetric(float64(len(docs))*float64(b.N)/elapsed, "docs/s")
}

// BenchmarkBuildSweep reports Build's docs/s at a range of Threads values on the
// realistic corpus, showing how throughput scales (or stops scaling) with workers.
func BenchmarkBuildSweep(b *testing.B) {
	for _, threads := range []int{1, 2, 4, 8, 16} {
		b.Run(fmt.Sprintf("T%d", threads), func(b *testing.B) {
			docs := genCorpus(20000)
			dir := b.TempDir()
			var lastPath string
			b.ResetTimer()
			for range b.N {
				meta, err := Build(dir, docs, BuildOptions{Threads: threads})
				if err != nil {
					b.Fatal(err)
				}
				lastPath = meta.Path
			}
			elapsed := b.Elapsed().Seconds()
			b.ReportMetric(float64(len(docs))*float64(b.N)/elapsed, "docs/s")
			if info, err := os.Stat(lastPath); err == nil {
				b.ReportMetric(float64(info.Size())/float64(len(docs)), "bytes/doc")
			}
		})
	}
}
