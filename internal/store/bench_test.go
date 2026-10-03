package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
)

// Store benchmarks run on SQLite, and on Postgres and MySQL when
// SEARCHLIGHT_TEST_PG_URL and SEARCHLIGHT_TEST_MYSQL_URL are set:
//
//	go test ./internal/store -run '^$' -bench . -benchtime 3s
//
// SEARCHLIGHT_BENCH_SQLITE_SYNC sets SQLite's synchronous level (OFF or
// NORMAL) in place of the store's FULL, to measure the store, not the disk.

// forEachDialectB runs fn on a fresh database of every available dialect.
func forEachDialectB(b *testing.B, fn func(b *testing.B, h *harness)) {
	b.Helper()
	b.Run("sqlite", func(b *testing.B) {
		h := sqliteHarness(b)
		if sync := os.Getenv("SEARCHLIGHT_BENCH_SQLITE_SYNC"); sync != "" {
			h.url += "?_synchronous=" + sync
		}
		fn(b, h)
	})
	b.Run("postgres", func(b *testing.B) {
		base := os.Getenv(envPG)
		if base == "" {
			b.Skip(envPG + " is not set")
		}
		fn(b, postgresHarness(b, base))
	})
	b.Run("mysql", func(b *testing.B) {
		base := os.Getenv(envMySQL)
		if base == "" {
			b.Skip(envMySQL + " is not set")
		}
		fn(b, mysqlHarness(b, base))
	})
}

// benchShards spreads benchmark changes over a few shards, as a bulk request
// does.
const benchShards = 4

// benchBody is a document of about 1 KB.
var benchBody = `{"title":"benchmark document","body":"` + strings.Repeat("lorem ipsum ", 80) + `","n":`

func benchBatch(prefix string, n int) []Change {
	batch := make([]Change, n)
	for j := range batch {
		batch[j] = upsert("bench", j%benchShards, fmt.Sprintf("%s-%d", prefix, j), benchBody+fmt.Sprint(j)+"}")
	}
	return batch
}

// BenchmarkApply commits batches of upserts of 1 KB documents: a group
// commit's transaction of 1, 100 or 1000 changes.
func BenchmarkApply(b *testing.B) {
	forEachDialectB(b, func(b *testing.B, h *harness) {
		st := h.open(b)
		mustCreateIndex(b, st, "bench")
		ctx := context.Background()
		for _, n := range []int{1, 100, 1000} {
			b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
				i := 0
				for b.Loop() {
					b.StopTimer()
					batch := benchBatch(fmt.Sprintf("a%d-%d", n, i), n)
					i++
					b.StartTimer()
					if _, _, err := st.Apply(ctx, batch); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(n*i)/b.Elapsed().Seconds(), "changes/s")
			})
		}
	})
}

// benchFill commits n changes to the bench index, 1000 at a time.
func benchFill(b *testing.B, st Store, n int) {
	b.Helper()
	for i := 0; i < n; i += 1000 {
		if _, _, err := st.Apply(context.Background(), benchBatch(fmt.Sprintf("f%d", i), min(1000, n-i))); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkChangesAfter reads a shard's changelog 1000 changes at a time.
func BenchmarkChangesAfter(b *testing.B) {
	forEachDialectB(b, func(b *testing.B, h *harness) {
		st := h.open(b)
		mustCreateIndex(b, st, "bench")
		benchFill(b, st, 4*benchShards*1000) // 4000 changes in each shard
		ctx := context.Background()
		shard := ShardID{Index: "bench", Shard: 0}
		read := 0
		for b.Loop() {
			page, err := st.ChangesAfter(ctx, shard, 0, 1000)
			if err != nil || len(page) != 1000 {
				b.Fatal(len(page), err)
			}
			read += len(page)
		}
		b.ReportMetric(float64(read)/b.Elapsed().Seconds(), "changes/s")
	})
}

// BenchmarkScanShard reads a shard of 4000 documents from one snapshot.
func BenchmarkScanShard(b *testing.B) {
	forEachDialectB(b, func(b *testing.B, h *harness) {
		st := h.open(b)
		mustCreateIndex(b, st, "bench")
		benchFill(b, st, 4*benchShards*1000)
		ctx := context.Background()
		shard := ShardID{Index: "bench", Shard: 0}
		read := 0
		for b.Loop() {
			n := 0
			if _, err := st.ScanShard(ctx, shard, func(Record) error { n++; return nil }); err != nil {
				b.Fatal(err)
			}
			read += n
		}
		b.ReportMetric(float64(read)/b.Elapsed().Seconds(), "records/s")
	})
}
