package search

import (
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
)

// HeapFloor is the heap size below which [KeepHeapFloor] keeps the garbage collector
// from starting a cycle.
const HeapFloor = 64 << 20

// KeepHeapFloor lets the heap grow to at least floor bytes between garbage collections,
// and stays out of the way above it (GOGC 100). Segments live in mapped files, not on
// the heap, so a serving node's live heap is a few megabytes: at the default GOGC every
// few searches' allocations would start a cycle, and a search that overlaps one waits
// on its assists and its share of the CPUs, which is what put cheap searches in the
// p99. After each cycle the collector's target is set to max(2·live, floor). It returns
// a function that stops tuning and restores GOGC 100.
func KeepHeapFloor(floor uint64) (stop func()) {
	t := &gcTuner{floor: floor, sample: []metrics.Sample{{Name: "/gc/heap/live:bytes"}}}
	t.tune()
	t.arm()
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.stopped = true
		debug.SetGCPercent(100)
	}
}

type gcTuner struct {
	floor   uint64
	mu      sync.Mutex
	sample  []metrics.Sample
	stopped bool
}

// gcSentinel is garbage from birth: its finalizer runs once per collection cycle.
type gcSentinel struct{ t *gcTuner }

func (t *gcTuner) arm() {
	s := &gcSentinel{t: t}
	runtime.SetFinalizer(s, func(s *gcSentinel) {
		if s.t.tune() {
			s.t.arm()
		}
	})
}

// tune sets GOGC from the live heap, unless the tuner was stopped (false).
func (t *gcTuner) tune() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return false
	}
	metrics.Read(t.sample)
	if t.sample[0].Value.Kind() != metrics.KindUint64 || t.sample[0].Value.Uint64() == 0 {
		return true // no cycle has measured the heap yet
	}
	debug.SetGCPercent(gcPercent(t.sample[0].Value.Uint64(), t.floor))
	return true
}

// maxGCPercent bounds the GOGC the tuner sets: a heap far under the floor grows at
// most this many times over before a cycle.
const maxGCPercent = 5000

// gcPercent is the GOGC that sets the next cycle's target to max(2·live, floor), at
// most maxGCPercent.
func gcPercent(live, floor uint64) int {
	if live >= floor/2 {
		return 100
	}
	return int(min(float64(floor)/float64(live)*100-100, maxGCPercent))
}
