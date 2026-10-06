// Package heapfloor keeps the garbage collector from running while the heap is small.
//
// A node keeps its segments in mapped files, not on the heap, so its live heap is a
// few megabytes. At the default GOGC a collection then starts every few searches'
// worth of allocation, and a search that overlaps one waits on its assists and shares
// the CPUs with it: that, not the searches themselves, set the p99 of cheap searches.
// Keep retunes GOGC after every collection so the next one starts when the heap
// reaches a floor, and stays at GOGC 100 once the live heap is past half of it.
package heapfloor

import (
	"math"
	"runtime"
	"runtime/debug"
	"runtime/metrics"
	"sync"
)

// maxPercent bounds the GOGC Keep sets, so a near-empty heap's target stays finite.
const maxPercent = 5000

// Effective is the floor Keep should hold for a configured floor: at most a quarter
// of the soft memory limit (GOMEMLIMIT or debug.SetMemoryLimit), so the floor never
// pushes the collector into limit-driven cycles.
func Effective(floor int64) int64 {
	if floor <= 0 {
		return 0
	}
	if limit := debug.SetMemoryLimit(-1); limit < math.MaxInt64 {
		floor = min(floor, limit/4)
	}
	return max(floor, 0)
}

// Keep holds the collector's target at about floor bytes while the live heap is under
// half of it: after each cycle, GOGC is set so that the next target, live heap plus
// (live heap + scanned stacks + globals) × GOGC/100, is floor. It returns a function
// that stops retuning and restores GOGC 100.
func Keep(floor int64) (stop func()) {
	t := &tuner{floor: floor, sample: []metrics.Sample{
		{Name: "/gc/heap/live:bytes"},
		{Name: "/gc/scan/stack:bytes"},
		{Name: "/gc/scan/globals:bytes"},
	}}
	t.tune()
	t.arm()
	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.stopped = true
		debug.SetGCPercent(100)
	}
}

type tuner struct {
	floor   int64
	mu      sync.Mutex
	sample  []metrics.Sample
	stopped bool
}

// sentinel is garbage from birth: its finalizer runs once per collection.
type sentinel struct{ t *tuner }

func (t *tuner) arm() {
	s := &sentinel{t: t}
	runtime.SetFinalizer(s, func(s *sentinel) {
		if s.t.tune() {
			s.t.arm()
		}
	})
}

// tune sets GOGC from the last collection's heap, unless the tuner was stopped
// (false). Before any collection has measured the heap it leaves GOGC alone.
func (t *tuner) tune() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return false
	}
	metrics.Read(t.sample)
	var v [3]uint64
	for i, s := range t.sample {
		if s.Value.Kind() == metrics.KindUint64 {
			v[i] = s.Value.Uint64()
		}
	}
	if v[0] == 0 {
		return true
	}
	debug.SetGCPercent(percent(v[0], v[1]+v[2], t.floor))
	return true
}

// percent is the GOGC whose next target, live + (live + roots) × GOGC/100, is floor:
// 100 when that is less, at most maxPercent.
func percent(live, roots uint64, floor int64) int {
	if floor <= 0 || float64(live) >= float64(floor)/2 {
		return 100
	}
	p := (float64(floor) - float64(live)) * 100 / float64(live+roots)
	return int(min(max(p, 100), maxPercent))
}
