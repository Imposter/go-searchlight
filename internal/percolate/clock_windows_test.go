package percolate

import (
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// The benchmarks time single documents in microseconds; time.Now on Windows ticks in
// about half a millisecond, so they read the performance counter instead.

var (
	kernel32 = windows.NewLazySystemDLL("kernel32.dll")
	qpc      = kernel32.NewProc("QueryPerformanceCounter")
	qpf      = kernel32.NewProc("QueryPerformanceFrequency")
	qpcFreq  = func() int64 {
		var f int64
		if r, _, _ := qpf.Call(uintptr(unsafe.Pointer(&f))); r == 0 {
			return 0
		}
		return f
	}()
)

func clockNow() int64 {
	if qpcFreq == 0 {
		return time.Now().UnixNano()
	}
	var c int64
	_, _, _ = qpc.Call(uintptr(unsafe.Pointer(&c)))
	return c
}

func clockSince(start int64) time.Duration {
	if qpcFreq == 0 {
		return time.Duration(time.Now().UnixNano() - start)
	}
	d := clockNow() - start
	return time.Duration(d/qpcFreq*int64(time.Second) + d%qpcFreq*int64(time.Second)/qpcFreq)
}
