package replica

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// benchStore is a SQLite store holding n documents in shard 0 of index "b", after
// one change of shard 1 (seq 1) that a benchmark's copy starts past.
func benchStore(b *testing.B, n int) (store.Store, int64) {
	b.Helper()
	st := (&db{dialect: "sqlite", url: sqliteURL(filepath.Join(b.TempDir(), "bench.db"))}).open(b)
	createIndex(b, st, "b", testMapping)
	mustApply(b, st, upsert("b", 1, "other", `{}`))
	var head int64
	for start := 0; start < n; start += 1000 {
		batch := make([]store.Change, 0, 1000)
		for i := start; i < min(start+1000, n); i++ {
			id := fmt.Sprintf("doc-%07d", i)
			batch = append(batch, upsert("b", 0, id, docBody(id, i)))
		}
		head = mustApply(b, st, batch...)
	}
	return st, head
}

func benchShardOptions() shard.Options {
	return shard.Options{Index: "b", Logger: quietLogger, RefreshInterval: time.Second, FilterCache: shard.NewFilterCache(1<<20, nil)}
}

// BenchmarkTailThroughput measures changes per second a tailer reads from the
// changelog, analyzes and applies to its shard: "applied" until the copy has them all
// in its buffer, "searchable" until a refresh has published them.
func BenchmarkTailThroughput(b *testing.B) {
	const docs = 20000
	st, head := benchStore(b, docs)
	id := ShardID{Index: "b", Shard: 0}
	b.ResetTimer()
	for _, searchable := range []bool{false, true} {
		name := "applied"
		if searchable {
			name = "searchable"
		}
		b.Run(name, func(b *testing.B) {
			var total time.Duration
			for i := range b.N {
				b.StopTimer()
				sh := openShard(b, filepath.Join(b.TempDir(), fmt.Sprint(i)), benchShardOptions())
				primeShard(b, st, sh) // tail, rather than load a snapshot
				opts := testOptions()
				opts.PollInterval = time.Hour
				tl := NewTailer(st, sh, id, opts)
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				b.StartTimer()
				start := time.Now()
				go func() { done <- tl.Run(ctx) }()
				for tl.Applied() < head {
					time.Sleep(100 * time.Microsecond)
				}
				if searchable {
					if err := sh.Refresh(ctx); err != nil {
						b.Fatal(err)
					}
					if err := sh.WaitRefreshed(ctx, head); err != nil {
						b.Fatal(err)
					}
				}
				total += time.Since(start)
				b.StopTimer()
				cancel()
				if err := <-done; err != nil {
					b.Fatal(err)
				}
				_ = sh.Close(context.Background())
				b.StartTimer()
			}
			b.ReportMetric(float64(docs*b.N)/total.Seconds(), "changes/s")
		})
	}
}

// BenchmarkRecoverSnapshot measures a copy rebuilt from the store's ScanShard: records
// per second loaded and made searchable.
func BenchmarkRecoverSnapshot(b *testing.B) {
	const docs = 20000
	st, head := benchStore(b, docs)
	id := ShardID{Index: "b", Shard: 0}
	b.ResetTimer()
	var total time.Duration
	for i := range b.N {
		b.StopTimer()
		sh := openShard(b, filepath.Join(b.TempDir(), fmt.Sprint(i)), benchShardOptions())
		b.StartTimer()
		start := time.Now()
		seq, err := Recover(context.Background(), sh, st, id)
		if err != nil || seq != head {
			b.Fatalf("Recover = %d, %v", seq, err)
		}
		if err := sh.WaitRefreshed(context.Background(), head); err != nil {
			b.Fatal(err)
		}
		total += time.Since(start)
		b.StopTimer()
		_ = sh.Close(context.Background())
		b.StartTimer()
	}
	b.ReportMetric(float64(docs*b.N)/total.Seconds(), "records/s")
}

// BenchmarkWriteToSearchable measures one write's latency from the store's Apply to
// searchable on a copy (WaitRefreshed), with the committing node waking its tailer:
// with the background refresh at a given interval, and with an explicit refresh once
// the tailer has applied it (refresh=true).
func BenchmarkWriteToSearchable(b *testing.B) {
	for _, mode := range []struct {
		name     string
		interval time.Duration
	}{
		{"refresh_interval=50ms", 50 * time.Millisecond},
		{"refresh_interval=1s", time.Second},
		{"refresh=true", -1},
	} {
		b.Run(mode.name, func(b *testing.B) {
			st, _ := benchStore(b, 0)
			id := ShardID{Index: "b", Shard: 0}
			sopts := benchShardOptions()
			sopts.RefreshInterval = mode.interval
			sh := openShard(b, b.TempDir(), sopts)
			opts := testOptions()
			opts.PollInterval = time.Hour
			tl := NewTailer(st, sh, id, opts)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- tl.Run(ctx) }()
			defer func() {
				cancel()
				<-done
				_ = tl.Shard().Close(context.Background())
			}()
			for tl.State() != StateTailing {
				time.Sleep(time.Millisecond)
			}
			lat := make([]time.Duration, 0, b.N)
			b.ResetTimer()
			for i := range b.N {
				start := time.Now()
				seq := mustApply(b, st, upsert("b", 0, fmt.Sprintf("w%d", i), docBody("w", i)))
				tl.Wake()
				if mode.interval < 0 {
					for tl.Applied() < seq {
						time.Sleep(20 * time.Microsecond)
					}
					if err := tl.Shard().Refresh(ctx); err != nil {
						b.Fatal(err)
					}
				}
				if err := tl.Shard().WaitRefreshed(ctx, seq); err != nil {
					b.Fatal(err)
				}
				lat = append(lat, time.Since(start))
			}
			b.StopTimer()
			slices.Sort(lat)
			b.ReportMetric(float64(lat[len(lat)/2].Microseconds())/1000, "p50-ms")
			b.ReportMetric(float64(lat[len(lat)*99/100].Microseconds())/1000, "p99-ms")
		})
	}
}

// primeShard gives an empty shard the index's mapping and incarnation and moves it
// past seq 1, as if it had applied it: a tailer then tails it rather than loading a
// snapshot.
func primeShard(b *testing.B, st store.Store, sh *shard.Shard) {
	b.Helper()
	ctx := context.Background()
	meta, err := st.Indexes().Get(ctx, "b")
	if err != nil {
		b.Fatal(err)
	}
	m, err := parseMapping(meta.Mapping)
	if err != nil {
		b.Fatal(err)
	}
	if err := sh.Load(ctx, []shard.Change{{Kind: shard.Remap, Mapping: m, MappingVersion: meta.MappingVersion, IndexUID: meta.UID}}); err != nil {
		b.Fatal(err)
	}
	if err := sh.Advance(1); err != nil {
		b.Fatal(err)
	}
}
