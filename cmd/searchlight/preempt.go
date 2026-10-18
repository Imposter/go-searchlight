package main

import (
	"runtime"
	"sync/atomic"
)

// envTestPreemptProbe, set to anything, makes the binary report whether the Go runtime
// preempts goroutines asynchronously, as "asyncpreemptoff=1" or "asyncpreemptoff=0", and
// exit 0. Test-only: CI and the release's smoke test check the Windows binary with it
// (golang/go#79249, preempt_windows.go).
const envTestPreemptProbe = "SEARCHLIGHT_TEST_PREEMPT_PROBE"

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
