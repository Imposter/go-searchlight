// Package prio gives searches priority over a node's background work on a small CPU
// budget.
//
// Go schedules goroutines alike: on one core a merge, a refresh's analysis or a bulk's
// analysis takes the processor from a search as often as the search takes it from them,
// and a size-0 count that needs 5 ms of CPU waits behind seconds of merging. A [Gate]
// lets the work that can wait step aside while searches run, without starving it:
//
//   - A search marks its CPU-bound part with [Gate.Foreground] (the search package does,
//     around each shard's execution and fetch).
//   - Background work calls [Gate.Yield] at its checkpoints: a merge every few thousand
//     documents and before each chunk it writes, a tailer's analysis before each
//     document, a bulk's analysis before each operation, the janitor before each file.
//     Yield returns at once while no search runs.
//   - While searches run, time is cut into cycles of [Period]: background work pauses
//     for the first (1 - share) of a cycle, or until the searches have all finished,
//     whichever comes first, and runs the rest. So background work keeps at least share
//     of the time whatever the reads, and a write load cannot be held off for good: its
//     buffers would fill and its clients be refused (429).
//
// The gate decides nothing alone: it only delays background goroutines at their
// checkpoints, so what runs between two checkpoints still runs whole.
package prio

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Imposter/go-searchlight/internal/clock"
)

// Period is the length of one cycle of pause and run while searches are in progress.
const Period = 100 * time.Millisecond

// DefaultShare is the background share on a node of fewer than SmallCPUs cores. Off
// (1) with more: there, searches find a core of their own.
const DefaultShare = 0.25

// SmallCPUs is the core count from which background work no longer yields by default.
const SmallCPUs = 4

// MinShare is the least share a gate takes: background work is never held off longer
// than nineteen parts in twenty.
const MinShare = 0.05

// Default returns the share a node uses when none is configured: DefaultShare with
// fewer than SmallCPUs cores (GOMAXPROCS), 1 (no yielding) otherwise.
func Default() float64 {
	if runtime.GOMAXPROCS(0) < SmallCPUs {
		return DefaultShare
	}
	return 1
}

// Gate is a node's priority of searches over background work (see the package). A nil
// *Gate, or one of share 1, never delays anything. It is safe for concurrent use.
type Gate struct {
	share float64
	clock clock.Clock

	active atomic.Int64 // searches in progress

	mu         sync.Mutex
	idle       chan struct{} // closed when active falls to 0; nil while it is 0
	pauseUntil time.Time     // the current cycle's pause ends
	runUntil   time.Time     // the current cycle ends
}

// New returns a gate under which background work keeps share of the time while searches
// run (clamped to [MinShare, 1]; 1 never pauses it), timed by clk (nil: clock.Real).
func New(share float64, clk clock.Clock) *Gate {
	if clk == nil {
		clk = clock.Real{}
	}
	return &Gate{share: min(max(share, MinShare), 1), clock: clk}
}

// Share is the background share of g's cycles (1 for a nil gate).
func (g *Gate) Share() float64 {
	if g == nil {
		return 1
	}
	return g.share
}

// Foreground marks a search's CPU-bound work in progress until the returned func is
// called (once).
func (g *Gate) Foreground() (done func()) {
	if g == nil || g.share >= 1 {
		return func() {}
	}
	g.mu.Lock()
	if g.active.Add(1) == 1 {
		g.idle = make(chan struct{})
	}
	g.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			if g.active.Add(-1) == 0 {
				close(g.idle)
				g.idle = nil
			}
			g.mu.Unlock()
		})
	}
}

// Active is the searches in progress.
func (g *Gate) Active() int64 {
	if g == nil {
		return 0
	}
	return g.active.Load()
}

// Yield is a background checkpoint: while searches run and the current cycle is in its
// pause, it waits until the pause ends, the searches all finish, or ctx ends (whose
// error it returns). It returns at once otherwise.
func (g *Gate) Yield(ctx context.Context) error {
	if g.yield(ctx.Done()) {
		return ctx.Err()
	}
	return nil
}

// Step is Yield for work that has no context to end its wait (file removal): it waits
// at most a pause.
func (g *Gate) Step() { g.yield(nil) }

// yield waits as Yield does, ending early when done closes, and reports whether it did.
func (g *Gate) yield(done <-chan struct{}) bool {
	if g == nil || g.share >= 1 || g.active.Load() == 0 {
		return false
	}
	g.mu.Lock()
	if g.idle == nil {
		g.mu.Unlock()
		return false
	}
	now := g.clock.Now()
	if !now.Before(g.runUntil) {
		pause := time.Duration((1 - g.share) * float64(Period))
		g.pauseUntil = now.Add(pause)
		g.runUntil = g.pauseUntil.Add(Period - pause)
	}
	wait := g.pauseUntil.Sub(now)
	idle := g.idle
	g.mu.Unlock()
	if wait <= 0 {
		return false
	}
	t := g.clock.NewTimer(wait)
	defer t.Stop()
	select {
	case <-t.C():
	case <-idle:
	case <-done:
		return true
	}
	return false
}

// std is the process's gate, as the search pool is the process's.
var std atomic.Pointer[Gate]

// Set makes g the process's gate (nil: none).
func Set(g *Gate) { std.Store(g) }

// Get returns the process's gate, nil when none is set.
func Get() *Gate { return std.Load() }

// Foreground is [Gate.Foreground] on the process's gate.
func Foreground() func() { return Get().Foreground() }

// Yield is [Gate.Yield] on the process's gate.
func Yield(ctx context.Context) error { return Get().Yield(ctx) }

// Step is [Gate.Step] on the process's gate.
func Step() { Get().Step() }
