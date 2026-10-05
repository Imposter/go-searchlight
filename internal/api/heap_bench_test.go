package api_test

import (
	"fmt"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/config"
)

// heapBulk is a 10,000-op bulk of about 15 MiB: scrape-bot-shaped listings with a
// long description.
func heapBulk() string {
	words := []string{"oak", "chair", "steel", "table", "lamp", "walnut", "vintage", "modern", "leather", "brass"}
	var desc strings.Builder
	for i := 0; desc.Len() < 1400; i++ {
		desc.WriteString(words[i%len(words)])
		desc.WriteByte(' ')
	}
	var b strings.Builder
	for i := range 10_000 {
		fmt.Fprintf(&b, "{\"upsert\":{\"id\":\"d%d\"}}\n{\"title\":\"Listing %d oak chair\",\"description\":%q,\"brand\":\"b%d\",\"price\":%d,\"tags\":[\"t%d\",\"all\"]}\n",
			i, i, desc.String(), i%50, i%997, i%31)
	}
	return b.String()
}

// peakHeap runs fn while sampling the live heap, returning its peak growth over
// the heap before it.
func peakHeap(fn func()) uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc
	var peak atomic.Uint64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(2 * time.Millisecond)
		defer t.Stop()
		for {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if m.HeapAlloc > peak.Load() {
				peak.Store(m.HeapAlloc)
			}
			select {
			case <-stop:
				return
			case <-t.C:
			}
		}
	}()
	fn()
	close(stop)
	<-done
	if p := peak.Load(); p > base {
		return p - base
	}
	return 0
}

// BenchmarkBulkPeakHeap measures the heap one ~15 MiB, 10,000-op _bulk takes at its
// peak, per MiB of body: the basis of inflight_amplification. Run it with
// -benchtime=3x.
func BenchmarkBulkPeakHeap(b *testing.B) {
	for _, c := range []struct{ perc, applying bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		perc := c.perc
		b.Run(fmt.Sprintf("percolate=%v/applying=%v", c.perc, c.applying), func(b *testing.B) {
			e := newEnv(b, envOpts{fakeTailers: true, cfg: func(c *config.Config) { c.RequestTimeout = 5 * time.Minute }})
			e.validate = false
			e.must(http.StatusCreated, "PUT", "/indexes/heap", `{"settings": {"shards": 2}}`)
			// With applying=false the tailers are paused, so the heap measured is
			// the request's alone (the copies' write buffers are bounded on their
			// own, by the shard's refresh bytes times its max buffer factor).
			if !c.applying {
				for s := range 2 {
					e.tailer("heap", s).Pause()
				}
			}
			body := heapBulk()
			path := fmt.Sprintf("/indexes/heap/_bulk?percolate=%v", perc)
			var worst uint64
			for b.Loop() {
				worst = max(worst, peakHeap(func() {
					if r := e.do("POST", path, body); r.status != http.StatusOK {
						b.Fatalf("bulk: %d %s", r.status, r.body[:min(len(r.body), 300)])
					}
				}))
			}
			mib := float64(len(body)) / (1 << 20)
			b.ReportMetric(mib, "body-MiB")
			b.ReportMetric(float64(worst)/(1<<20), "peak-heap-MiB")
			b.ReportMetric(float64(worst)/(1<<20)/mib, "heap-per-body-MiB")
		})
	}
}
