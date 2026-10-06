package segment

import (
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/testtier"
)

// mergeHeapPerDoc and mergeHeapPerThread bound a merge's peak heap: per document merged
// (presence, numbers, value and entry ordinals) and per merge thread (each thread's
// field buffers, up to spillAt, and its working set).
const (
	mergeHeapPerDoc    = 1 << 10
	mergeHeapPerThread = 48 << 20
)

// TestMergeMemory builds the bench's products (SL_MEM_DOCS, 200k by default) in parts of
// 200k, merges them into one with SL_MEM_THREADS threads (4 by default), and checks the
// merge's peak heap stays under mergeHeapPerDoc a document plus mergeHeapPerThread a
// thread. Measured on a million documents: about 0.6 GiB at 4 threads, 0.7 GiB at 16.
func TestMergeMemory(t *testing.T) {
	testtier.Heavy(t)
	n, err := strconv.Atoi(os.Getenv("SL_MEM_DOCS"))
	if err != nil || n <= 0 {
		n = 200_000
	}
	threads, err := strconv.Atoi(os.Getenv("SL_MEM_THREADS"))
	if err != nil || threads <= 0 {
		threads = 4
	}
	dir := t.TempDir()
	var readers []*Reader
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	for from := 0; from < n; from += 100_000 {
		meta, err := Build(dir, productRange(t, from, min(from+100_000, n)), BuildOptions{Threads: 4, NoSync: true})
		if err != nil {
			t.Fatal(err)
		}
		r, err := Open(meta.Path)
		if err != nil {
			t.Fatal(err)
		}
		readers = append(readers, r)
	}
	runtime.GC()
	debug.FreeOSMemory()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base := ms.HeapInuse
	var peak atomic.Uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(10 * time.Millisecond):
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if m.HeapInuse > peak.Load() {
				peak.Store(m.HeapInuse)
			}
		}
	}()
	start := time.Now()
	meta, err := Merge(dir, readers, nil, MergeOptions{Threads: threads})
	took := time.Since(start)
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	merged, err := Open(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	docs := uint64(merged.NumDocs())
	_ = merged.Close()
	used := peak.Load() - min(base, peak.Load())
	ceiling := docs*mergeHeapPerDoc + uint64(threads)*mergeHeapPerThread
	t.Logf("merge of %d documents, %d threads: %v, peak heap +%d MiB (%d B/doc), ceiling %d MiB",
		docs, threads, took.Round(time.Millisecond), used>>20, used/docs, ceiling>>20)
	if used > ceiling {
		t.Fatalf("the merge's peak heap grew by %d MiB, over the %d MiB ceiling", used>>20, ceiling>>20)
	}
}
