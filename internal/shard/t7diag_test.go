package shard

// T7 harness (test-only, opt-in): where a one-document refresh, and the flush after a
// run of them, spend their time, against a shard already holding 50,000 documents in
// ten segments. Run with SEARCHLIGHT_T7DIAG=1 (T7_N refreshes per mode, T7_LOAD=1 adds
// two goroutines writing and fsyncing 256 KiB files in the shard's directory):
//
//	SEARCHLIGHT_T7DIAG=1 go test ./internal/shard -run TestT7RefreshPhases -v
//
// Phases: build (the segment written, unsynced, and opened) and publish (older copies
// masked, the generation swapped); every tenth refresh is followed by a flush.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/segment"
)

func TestT7RefreshPhases(t *testing.T) {
	if os.Getenv("SEARCHLIGHT_T7DIAG") == "" {
		t.Skip("set SEARCHLIGHT_T7DIAG=1 to run the T7 harness")
	}
	n := 200
	if v, err := strconv.Atoi(os.Getenv("T7_N")); err == nil && v > 0 {
		n = v
	}
	var mu sync.Mutex
	marks := map[string]time.Time{}
	opts := testOptions()
	opts.hooks = &testHooks{at: func(p string) error {
		mu.Lock()
		marks[p] = time.Now()
		mu.Unlock()
		return nil
	}}
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(ctx, dir, testMapping, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.shutdown()
	var seq int64
	for range 10 {
		changes := make([]Change, 0, 5000)
		for range 5000 {
			seq++
			id := fmt.Sprintf("p%07d", seq)
			changes = append(changes, Change{Seq: seq, Kind: Upsert, Doc: analyze(t, id, body(id, seq))})
		}
		if err := s.Apply(ctx, changes); err != nil {
			t.Fatal(err)
		}
		if err := s.Refresh(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	var load sync.WaitGroup
	if os.Getenv("T7_LOAD") != "" {
		for w := range 2 {
			load.Go(func() {
				buf := make([]byte, 256<<10)
				path := filepath.Join(dir, fmt.Sprintf("t7load-%d.tmp", w))
				defer os.Remove(path)
				for !stop.Load() {
					f, err := os.Create(path)
					if err != nil {
						return
					}
					_, _ = f.Write(buf)
					_ = f.Sync()
					_ = f.Close()
				}
			})
		}
	}
	defer func() {
		stop.Store(true)
		load.Wait()
	}()

	type row struct{ build, publish, total time.Duration }
	pct := func(d []time.Duration, p float64) time.Duration {
		if len(d) == 0 {
			return 0
		}
		d = slices.Clone(d)
		slices.Sort(d)
		return d[int(p/100*float64(len(d)-1))].Round(100 * time.Microsecond)
	}
	for _, mode := range []string{"new-id", "update"} {
		var rows []row
		var flushes []time.Duration
		var syncs int64
		for i := range n {
			seq++
			id := fmt.Sprintf("v%07d", seq)
			if mode == "update" {
				id = fmt.Sprintf("p%07d", 1+(i*997)%50000)
			}
			if err := s.Apply(ctx, []Change{{Seq: seq, Kind: Upsert, Doc: analyze(t, id, body(id, seq))}}); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			clear(marks)
			mu.Unlock()
			before := segment.SyncCounts().Files
			t0 := time.Now()
			if err := s.Refresh(ctx); err != nil {
				t.Fatal(err)
			}
			end := time.Now()
			syncs += segment.SyncCounts().Files - before
			mu.Lock()
			built := marks[pointRefreshBuilt]
			mu.Unlock()
			rows = append(rows, row{build: built.Sub(t0), publish: end.Sub(built), total: end.Sub(t0)})
			if i%10 == 9 {
				f0 := time.Now()
				if err := s.Flush(ctx); err != nil {
					t.Fatal(err)
				}
				flushes = append(flushes, time.Since(f0))
			}
			time.Sleep(20 * time.Millisecond)
		}
		var b strings.Builder
		fmt.Fprintf(&b, "\n== refresh of one %s document over 50k docs in 10 segments (n=%d, load %v, fsyncs in refreshes %d)\n", mode, n, os.Getenv("T7_LOAD") != "", syncs)
		fmt.Fprintf(&b, "%-8s %10s %10s %10s %10s\n", "phase", "p50", "p90", "p99", "max")
		for _, c := range []struct {
			name string
			get  func(r row) time.Duration
		}{
			{"build", func(r row) time.Duration { return r.build }},
			{"publish", func(r row) time.Duration { return r.publish }},
			{"TOTAL", func(r row) time.Duration { return r.total }},
		} {
			d := make([]time.Duration, len(rows))
			for i, r := range rows {
				d[i] = c.get(r)
			}
			fmt.Fprintf(&b, "%-8s %10s %10s %10s %10s\n", c.name, pct(d, 50), pct(d, 90), pct(d, 99), pct(d, 100))
		}
		fmt.Fprintf(&b, "%-8s %10s %10s %10s %10s  (after every 10 refreshes)\n", "flush", pct(flushes, 50), pct(flushes, 90), pct(flushes, 99), pct(flushes, 100))
		t.Log(b.String())
	}
}
