package workloads

import (
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Go's monotonic clock on Windows advances once per timer interrupt (0.5 ms at
// best), too coarse for sub-millisecond latencies, so latencies are read from
// QueryPerformanceCounter instead.
var (
	kernel32 = windows.NewLazySystemDLL("kernel32.dll")
	qpc      = kernel32.NewProc("QueryPerformanceCounter")
	qpf      = kernel32.NewProc("QueryPerformanceFrequency")
	qpcFreq  = func() int64 {
		var f int64
		if r, _, _ := qpf.Call(uintptr(unsafe.Pointer(&f))); r == 0 || f <= 0 {
			return 0
		}
		return f
	}()
	clockBase = time.Now()
)

// nanotime is a monotonic clock in nanoseconds for latencies.
func nanotime() int64 {
	if qpcFreq == 0 {
		return int64(time.Since(clockBase))
	}
	var c int64
	if r, _, _ := qpc.Call(uintptr(unsafe.Pointer(&c))); r == 0 {
		return int64(time.Since(clockBase))
	}
	// c/f seconds, in nanoseconds, without overflowing for days of uptime.
	return c/qpcFreq*int64(time.Second) + c%qpcFreq*int64(time.Second)/qpcFreq
}
