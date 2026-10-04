//go:build !windows

package workloads

import "time"

var clockBase = time.Now()

// nanotime is a monotonic clock in nanoseconds for latencies.
func nanotime() int64 { return int64(time.Since(clockBase)) }
