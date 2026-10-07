package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"testing"
)

// Store benchmarks run on SQLite, and on Postgres and MySQL when
// SEARCHLIGHT_TEST_PG_URL and SEARCHLIGHT_TEST_MYSQL_URL are set. Every
// sub-benchmark gets a fresh database, and a fixed iteration count keeps runs
// comparable (the tables grow as they run):
//
//	go test ./internal/store -run '^$' -bench . -benchtime 200x
//
// SEARCHLIGHT_BENCH_SQLITE_SYNC sets SQLite's synchronous level (OFF or
// NORMAL) in place of the store's FULL, to measure the store, not the disk.

// forEachDialectB runs fn for every available dialect with a function that
// makes a fresh database of it.
func forEachDialectB(b *testing.B, fn func(b *testing.B, fresh func(testing.TB) *harness)) {
	b.Helper()
	b.Run("sqlite", func(b *testing.B) {
		fn(b, func(tb testing.TB) *harness {
			h := durableSQLiteHarness(tb)
			if sync := os.Getenv("SEARCHLIGHT_BENCH_SQLITE_SYNC"); sync != "" {
				h.url += "?_synchronous=" + sync
			}
			return h
		})
	})
	b.Run("postgres", func(b *testing.B) {
		base := os.Getenv(envPG)
		if base == "" {
			b.Skip(envPG + " is not set")
		}
		fn(b, func(tb testing.TB) *harness { return postgresHarness(tb, base) })
	})
	b.Run("mysql", func(b *testing.B) {
		base := os.Getenv(envMySQL)
		if base == "" {
			b.Skip(envMySQL + " is not set")
		}
		fn(b, func(tb testing.TB) *harness { return mysqlHarness(tb, base) })
	})
}

// benchStore opens a store on a fresh database with the bench index, storing bodies
// compressed as a cluster does once every node reads them.
func benchStore(b *testing.B, fresh func(testing.TB) *harness) Store {
	b.Helper()
	st := fresh(b).open(b)
	st.CompressBodies(true)
	mustCreateIndex(b, st, "bench")
	return st
}

// benchShards spreads benchmark changes over a few shards, as a bulk request
// does.
const benchShards = 4

// benchBody is a product listing of about 900 bytes, shaped like the benchmark
// dataset's (bench/datasets): its text drawn from a few thousand words, the common
// ones far more often, so it compresses about as the dataset's documents do. n makes
// each one its own.
func benchBody(n int) string {
	r := rand.New(rand.NewPCG(uint64(n), 3))
	words := func(k int) string {
		w := make([]string, k)
		for i := range w {
			w[i] = benchWords[r.IntN(1+r.IntN(1+r.IntN(len(benchWords))))]
		}
		return strings.Join(w, " ")
	}
	return fmt.Sprintf(`{"title":%q,"description":%q,"brand":"Brand %d","category":"Home > %s","tags":[%q,%q],`+
		`"sku":"SKU-%08d","url":"https://shop.example.com/p/%s-%d","price":%d.%02d,"rating":%d.%d,"reviews":%d,`+
		`"in_stock":%t,"first_seen":"2026-09-%02dT04:55:05Z","n":%d}`,
		words(6), words(60), r.IntN(2000), words(1), words(1), words(1), n, words(1), n, r.IntN(500), r.IntN(100),
		r.IntN(5), r.IntN(10), r.IntN(1000), r.IntN(2) == 0, 1+r.IntN(28), n)
}

var benchWords = func() []string {
	out := strings.Fields("the and with for this made great quality black outdoor set travel table high electric large includes perfect")
	r := rand.New(rand.NewPCG(1, 2))
	syllables := strings.Fields("ka ne mi tru pol ver du kit bam ris dur mor fal cra mup lo pe gab sim fi vi ho nel tar kane ru")
	for len(out) < 3000 {
		var b strings.Builder
		for range 2 + r.IntN(3) {
			b.WriteString(syllables[r.IntN(len(syllables))])
		}
		out = append(out, b.String())
	}
	return out
}()

func benchBatch(prefix string, n int) []Change {
	batch := make([]Change, n)
	for j := range batch {
		batch[j] = upsert("bench", j%benchShards, fmt.Sprintf("%s-%d", prefix, j), benchBody(j))
	}
	return batch
}

// BenchmarkApply commits batches of new 1 KB documents: a group commit's
// transaction of 1, 100 or 1000 changes.
func BenchmarkApply(b *testing.B) {
	forEachDialectB(b, func(b *testing.B, fresh func(testing.TB) *harness) {
		for _, n := range []int{1, 100, 1000} {
			b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
				st := benchStore(b, fresh)
				ctx := context.Background()
				i := 0
				for b.Loop() {
					b.StopTimer()
					batch := benchBatch(fmt.Sprintf("a%d", i), n)
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

// BenchmarkApplyMix commits batches that rewrite and delete existing
// documents: of every four changes, three upsert a document that (mostly)
// exists and one deletes one, over a pool four batches large. It measures
// the conflict-update and delete paths that new documents never take.
func BenchmarkApplyMix(b *testing.B) {
	forEachDialectB(b, func(b *testing.B, fresh func(testing.TB) *harness) {
		for _, n := range []int{100, 1000} {
			b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
				st := benchStore(b, fresh)
				ctx := context.Background()
				pool := 4 * n
				id := func(k int) string { return fmt.Sprint("m-", k%pool) }
				for k := 0; k < pool; k += n {
					batch := make([]Change, n)
					for j := range batch {
						batch[j] = upsert("bench", (k+j)%benchShards, id(k+j), benchBody(k+j))
					}
					if _, _, err := st.Apply(ctx, batch); err != nil {
						b.Fatal(err)
					}
				}
				i := 0
				for b.Loop() {
					b.StopTimer()
					batch := make([]Change, n)
					for j := range batch {
						k := i*n + j
						if j%4 == 3 {
							batch[j] = del("bench", k%pool%benchShards, id(k))
						} else {
							batch[j] = upsert("bench", k%pool%benchShards, id(k), benchBody(k+i))
						}
					}
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
	forEachDialectB(b, func(b *testing.B, fresh func(testing.TB) *harness) {
		st := benchStore(b, fresh)
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
	forEachDialectB(b, func(b *testing.B, fresh func(testing.TB) *harness) {
		st := benchStore(b, fresh)
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
