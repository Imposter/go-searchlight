//go:build windows

package search

import (
	"syscall"
	"unsafe"
)

// Windows's time.Now ticks in about half a millisecond, too coarse for per-search
// latencies: the benchmarks read the performance counter instead.
var (
	kernel32 = syscall.NewLazyDLL("kernel32.dll")
	qpc      = kernel32.NewProc("QueryPerformanceCounter")
	qpfreq   = func() int64 {
		var f int64
		_, _, _ = kernel32.NewProc("QueryPerformanceFrequency").Call(uintptr(unsafe.Pointer(&f)))
		return f
	}()
)

// nanotime is a monotonic clock in nanoseconds.
func nanotime() int64 {
	var c int64
	_, _, _ = qpc.Call(uintptr(unsafe.Pointer(&c)))
	return c/qpfreq*1e9 + c%qpfreq*1e9/qpfreq
}
