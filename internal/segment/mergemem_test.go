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
)

// TestMergeMemory builds SL_MEM_DOCS (e.g. 1000000) of the bench's products in parts
// of 200k, then merges them into one with SL_MEM_THREADS threads, and reports the peak
// Go heap the merge used.
func TestMergeMemory(t *testing.T) {
	n, _ := strconv.Atoi(os.Getenv("SL_MEM_DOCS"))
	if n <= 0 {
		t.Skip()
	}
	dir := t.TempDir()
	var readers []*Reader
	for from := 0; from < n; from += 200_000 {
		meta, err := Build(dir, productRange(t, from, min(from+200_000, n)), BuildOptions{Threads: 4, NoSync: true})
		if err != nil {
			t.Fatal(err)
		}
		r, err := Open(meta.Path)
		if err != nil {
			t.Fatal(err)
		}
		readers = append(readers, r)
	}
	threads, _ := strconv.Atoi(os.Getenv("SL_MEM_THREADS"))
	runtime.GC()
	debug.FreeOSMemory()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base := ms.HeapInuse
	var peakInuse, peakSys atomic.Uint64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			if m.HeapInuse > peakInuse.Load() {
				peakInuse.Store(m.HeapInuse)
			}
			if s := m.HeapSys - m.HeapReleased; s > peakSys.Load() {
				peakSys.Store(s)
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
	info, _ := os.Stat(meta.Path)
	t.Logf("merge of %d docs, threads %d: %v, %d MiB file, heap in use before %d MiB, peak in use +%d MiB, peak heap held %d MiB",
		n, threads, took.Round(time.Millisecond), info.Size()>>20, base>>20, (peakInuse.Load()-base)>>20, peakSys.Load()>>20)
}
