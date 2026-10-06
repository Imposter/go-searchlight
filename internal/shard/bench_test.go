package shard

import (
	"context"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
)

var (
	corpusMu sync.Mutex
	corpora  = map[int][]*schema.Doc{}
)

var benchWords = func() []string {
	rng := rand.New(rand.NewPCG(1, 1))
	words := make([]string, 4000)
	for i := range words {
		b := make([]byte, 3+rng.IntN(7))
		for j := range b {
			b[j] = byte('a' + rng.IntN(26))
		}
		words[i] = string(b)
	}
	return words
}()

// corpus returns n analyzed, product-shaped documents, ids doc-0 to doc-(n-1).
func corpus(tb testing.TB, n int) []*schema.Doc {
	corpusMu.Lock()
	defer corpusMu.Unlock()
	if docs, ok := corpora[n]; ok {
		return docs
	}
	rng := rand.New(rand.NewPCG(uint64(n), 2))
	docs := make([]*schema.Doc, n)
	for i := range docs {
		title := ""
		for j := range 6 {
			if j > 0 {
				title += " "
			}
			title += benchWords[rng.IntN(len(benchWords))]
		}
		body := fmt.Sprintf(`{"title":%q,"brand":"brand%d","tags":["t%d","t%d"],"price":%d.%02d}`,
			title, rng.IntN(200), rng.IntN(50), rng.IntN(50), rng.IntN(1000), rng.IntN(100))
		docs[i] = analyze(tb, fmt.Sprintf("doc-%d", i), body)
	}
	corpora[n] = docs
	return docs
}

func changesFor(docs []*schema.Doc, firstSeq int64) []Change {
	out := make([]Change, len(docs))
	for i, d := range docs {
		out[i] = Change{Seq: firstSeq + int64(i), Kind: Upsert, Doc: d}
	}
	return out
}

func benchOpen(b *testing.B, dir string) *Shard {
	s, err := Open(context.Background(), dir, testMapping, testOptions())
	if err != nil {
		b.Fatal(err)
	}
	return s
}

// BenchmarkApply is buffering throughput: batches of 1,000 upserts, no refresh.
func BenchmarkApply(b *testing.B) {
	docs := corpus(b, 100_000)
	s := benchOpen(b, b.TempDir())
	defer s.shutdown()
	ctx := context.Background()
	const batch = 1000
	var seq int64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i += batch {
		n := min(batch, b.N-i)
		changes := make([]Change, n)
		for j := range changes {
			seq++
			changes[j] = Change{Seq: seq, Kind: Upsert, Doc: docs[(i+j)%len(docs)]}
		}
		if err := s.Apply(ctx, changes); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "changes/s")
}

// BenchmarkRefresh is refresh latency with n new documents buffered (a segment build,
// lookups of every id in the existing segments, and the commit's fsyncs), and with n
// updates of documents spread over 10 segments, which also writes 10 deletes sidecars.
func BenchmarkRefresh(b *testing.B) {
	for _, n := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("new/%dk", n/1000), func(b *testing.B) { benchRefresh(b, n, 0) })
	}
	b.Run("update/10k-over-10-segments", func(b *testing.B) { benchRefresh(b, 10_000, 10) })
}

func benchRefresh(b *testing.B, n, segments int) {
	docs := corpus(b, max(n, 100_000))[:n]
	ctx := context.Background()
	root := b.TempDir()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		b.StopTimer()
		s := benchOpen(b, filepath.Join(root, fmt.Sprint(i)))
		seq := int64(1)
		for k := range segments { // the documents spread over segments
			part := docs[k*n/segments : (k+1)*n/segments]
			if err := s.Apply(ctx, changesFor(part, seq)); err != nil {
				b.Fatal(err)
			}
			seq += int64(len(part))
			if err := s.Refresh(ctx); err != nil {
				b.Fatal(err)
			}
		}
		if err := s.Apply(ctx, changesFor(docs, seq)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if err := s.Refresh(ctx); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		s.shutdown()
		b.StartTimer()
	}
	b.ReportMetric(float64(n)*float64(b.N)/b.Elapsed().Seconds(), "docs/s")
}

// BenchmarkAcquireRelease is a reader's cost to take and drop a generation: lock-free
// and allocation-free, from many goroutines at once.
func BenchmarkAcquireRelease(b *testing.B) {
	s := benchOpen(b, b.TempDir())
	defer s.shutdown()
	if err := s.Apply(context.Background(), changesFor(corpus(b, 1_000), 1)); err != nil {
		b.Fatal(err)
	}
	if err := s.Refresh(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			g := s.Acquire()
			g.Release()
		}
	})
}

// BenchmarkLookup is finding a document's live copy by id across 10 segments.
func BenchmarkLookup(b *testing.B) {
	docs := corpus(b, 100_000)
	s := benchOpen(b, b.TempDir())
	defer s.shutdown()
	ctx := context.Background()
	for k := range 10 {
		part := docs[k*10_000 : (k+1)*10_000]
		if err := s.Apply(ctx, changesFor(part, int64(k*10_000+1))); err != nil {
			b.Fatal(err)
		}
		if err := s.Refresh(ctx); err != nil {
			b.Fatal(err)
		}
	}
	g := s.Acquire()
	defer g.Release()
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, _, ok := g.Lookup(docs[(i*7919)%len(docs)].ID); !ok {
			b.Fatal("not found")
		}
	}
}

// BenchmarkRefreshKinds is refresh latency, and fsyncs per refresh, for three shapes
// of buffer over a shard holding one 10k-document segment: 1,000 new documents, one
// delete of an existing document, and nothing but an Advance (a seq that belongs to
// other shards). Run it with -benchtime of at most 80x (it uses fresh ids each time).
func BenchmarkRefreshKinds(b *testing.B) {
	docs := corpus(b, 100_000)
	setup := func(b *testing.B) (*Shard, int64) {
		s := benchOpen(b, b.TempDir())
		b.Cleanup(s.shutdown)
		if err := s.Apply(context.Background(), changesFor(docs[:10_000], 1)); err != nil {
			b.Fatal(err)
		}
		if err := s.Refresh(context.Background()); err != nil {
			b.Fatal(err)
		}
		return s, 10_001
	}
	run := func(b *testing.B, prepare func(s *Shard, i int, seq int64) int64) {
		s, seq := setup(b)
		ctx := context.Background()
		before := segment.SyncCounts()
		b.ResetTimer()
		for i := range b.N {
			b.StopTimer()
			seq = prepare(s, i, seq)
			b.StartTimer()
			if err := s.Refresh(ctx); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		after := segment.SyncCounts()
		b.ReportMetric(float64(after.Files-before.Files)/float64(b.N), "filesyncs/op")
		b.ReportMetric(float64(after.Dirs-before.Dirs)/float64(b.N), "dirsyncs/op")
	}
	b.Run("new-1k", func(b *testing.B) {
		run(b, func(s *Shard, i int, seq int64) int64 {
			part := docs[10_000+i*1000 : 10_000+(i+1)*1000]
			if err := s.Apply(context.Background(), changesFor(part, seq)); err != nil {
				b.Fatal(err)
			}
			return seq + int64(len(part))
		})
	})
	b.Run("delete-1", func(b *testing.B) {
		run(b, func(s *Shard, i int, seq int64) int64 {
			if err := s.Apply(context.Background(), []Change{{Seq: seq, Kind: Delete, DocID: docs[i%10_000].ID}}); err != nil {
				b.Fatal(err)
			}
			return seq + 1
		})
	})
	b.Run("advance-only", func(b *testing.B) {
		run(b, func(s *Shard, _ int, seq int64) int64 {
			if err := s.Advance(seq); err != nil {
				b.Fatal(err)
			}
			return seq + 1
		})
	})
}
