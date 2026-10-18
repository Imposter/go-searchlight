package main

import (
	"runtime"
	"sync/atomic"
)

// envDiagAsyncPreempt, set to anything, makes the binary a diagnostic instead of a node:
// it reports whether the Go runtime preempts goroutines asynchronously, as
// "asyncpreemptoff=1" or "asyncpreemptoff=0", taking a second or so, and exits 0. It
// reads nothing and changes nothing. CI and the release's smoke test check the Windows
// binary with it (golang/go#79249, preempt_windows.go), and docs/operations.md gives it
// to operators.
const envDiagAsyncPreempt = "SEARCHLIGHT_DIAG_ASYNC_PREEMPT"

// asyncPreemptionOff reports whether async preemption is off, by observing it: a garbage
// collection stops the world, and a goroutine in a loop with no function calls stops
// only when the runtime preempts it asynchronously. With async preemption on, the
// collection preempts the loop and returns while it is still running; with it off, the
// collection waits for the loop to end. The loop runs for a second or so.
func asyncPreemptionOff() bool {
	runtime.GOMAXPROCS(2)
	var started, finished atomic.Bool
	var n atomic.Int64
	done := make(chan struct{})
	go func() {
		started.Store(true)
		for range 400_000_000 {
			n.Add(1)
		}
		finished.Store(true)
		close(done)
	}()
	for !started.Load() {
		runtime.Gosched()
	}
	runtime.GC()
	off := finished.Load()
	<-done
	return off
}
