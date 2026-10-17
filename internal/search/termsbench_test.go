package search

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// Benchmarks of terms aggregations over a field of many distinct values: a listing
// index of benchDocs() documents (1M by default) in four segments, each holding a
// source (16 values, 13 of them on 95% of the documents), a keyword_list of one to
// eight items from 150,000 distinct values (half zipf-distributed, half uniform: about
// 150,000 distinct over the index) and a snapshot number. The shard is kept under
// SEARCHLIGHT_BENCH_DIR as the catalogue's is.
//
//	go test ./internal/search -run '^$' -bench 'TermsList|CardinalityMany' -benchtime 20x

const listCorpusVersion = "v1"

const listItems = 150_000

var listMapping = &schema.Mapping{Dynamic: schema.DynamicStrict, Fields: map[string]schema.FieldType{
	"source": schema.Keyword, "items": schema.KeywordList, "snapshot": schema.Number,
}}

func listSources() []string {
	out := make([]string, 16)
	for i := range out {
		out[i] = fmt.Sprintf("src-%02d", i)
	}
	return out
}

func listDoc(i int) (string, []byte) {
	rng := rand.New(rand.NewPCG(uint64(i), 77))
	z := rand.NewZipf(rng, 1.1, 4, listItems-1)
	sources := listSources()
	src := sources[13+rng.IntN(3)]
	if rng.IntN(100) < 95 {
		src = sources[rng.IntN(13)]
	}
	n := 1 + rng.IntN(8)
	items := make([]string, n)
	for j := range items {
		k := rng.IntN(listItems)
		if rng.IntN(2) == 0 {
			k = int(z.Uint64())
		}
		items[j] = strconv.Quote(fmt.Sprintf("item-%06d", k))
	}
	body := fmt.Sprintf(`{"source":%q,"items":[%s],"snapshot":%d}`, src, strings.Join(items, ","), 1_700_000_000+rng.IntN(10_000_000))
	return fmt.Sprintf("l%07d", i), []byte(body)
}

var (
	listOnce  sync.Once
	listShard *shard.Shard
	errList   error
)

// openListBench opens (building first, if needed) the listing benchmark's shard.
func openListBench(b *testing.B) *shard.Shard {
	b.Helper()
	listOnce.Do(func() {
		n := benchDocs()
		sum := sha256.Sum256(fmt.Appendf(nil, "list/%s/%d/%v/format%d", listCorpusVersion, n, listMapping.Fields, segment.FormatMajor))
		dir := filepath.Join(benchDir(), hex.EncodeToString(sum[:8]))
		marker := filepath.Join(dir, "complete")
		opts := shard.Options{
			RefreshInterval: -1, DisableMerges: true, RefreshBytes: -1, Logger: quiet,
			FilterCache: shard.NewFilterCache(shard.DefaultFilterCacheBytes, nil),
		}
		if _, err := os.Stat(marker); err == nil {
			listShard, errList = shard.Open(context.Background(), filepath.Join(dir, "shard"), listMapping, opts)
			return
		}
		_ = os.RemoveAll(dir)
		start := time.Now()
		s, err := shard.Open(context.Background(), filepath.Join(dir, "shard"), listMapping, opts)
		if err != nil {
			errList = err
			return
		}
		perSegment := (n + 3) / 4
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
						id, body := listDoc(i)
						d, _, err := schema.Analyze(listMapping, id, body)
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
					errList = err
					return
				}
			}
			for i := range changes {
				seq++
				changes[i].Seq = seq
			}
			if err := s.Apply(context.Background(), changes); err != nil {
				errList = err
				return
			}
			if err := s.Refresh(context.Background()); err != nil {
				errList = err
				return
			}
		}
		if err := s.Flush(context.Background()); err != nil {
			errList = err
			return
		}
		if err := os.WriteFile(marker, []byte(time.Now().Format(time.RFC3339)), 0o600); err != nil {
			errList = err
			return
		}
		fmt.Fprintf(os.Stderr, "built %d listing documents in %v at %s\n", n, time.Since(start), dir)
		listShard = s
	})
	if errList != nil {
		b.Fatal(errList)
	}
	return listShard
}

// listQuery is the listing benchmarks' query: 13 of the 16 sources, about 95% of the
// documents.
func listQuery() query.Node {
	return leaf("source", query.OpIn, listSources()[:13])
}

// BenchmarkTermsList: the top 100 items (shard_size 5,000) of most of the index, alone
// and with a metric sub-aggregation on the snapshot.
func BenchmarkTermsList(b *testing.B) {
	s := openListBench(b)
	for _, tc := range []struct {
		name, mode string
		subs       map[string]Agg
	}{
		{"none", "", nil},
		{"stats/depth_first", CollectDepthFirst, map[string]Agg{"s": {Type: AggStats, Field: "snapshot"}}},
		{"stats/breadth_first", CollectBreadthFirst, map[string]Agg{"s": {Type: AggStats, Field: "snapshot"}}},
		{"min/breadth_first", CollectBreadthFirst, map[string]Agg{"s": {Type: AggMin, Field: "snapshot"}}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runBenchOn(b, s, func(_ *rand.Rand) *Request {
				return &Request{Query: listQuery(), Aggs: map[string]Agg{
					"items": {Type: AggTerms, Field: "items", Size: 100, ShardSize: 5000, CollectMode: tc.mode, Aggs: tc.subs},
				}}
			})
		})
	}
}

// BenchmarkCardinalityMany: 100 cardinality aggregations of most of the index, as a
// listing page asks whether each of its facets has two values or more.
func BenchmarkCardinalityMany(b *testing.B) {
	s := openListBench(b)
	runBenchOn(b, s, func(_ *rand.Rand) *Request {
		aggs := make(map[string]Agg, 100)
		for i := range 100 {
			field := "items"
			if i%2 == 1 {
				field = "source"
			}
			aggs[fmt.Sprintf("c%02d", i)] = Agg{Type: AggCardinality, Field: field}
		}
		return &Request{Query: listQuery(), Aggs: aggs}
	})
}
