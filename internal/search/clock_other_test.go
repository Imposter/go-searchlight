//go:build !windows

package search

import "time"

var clockStart = time.Now()

// nanotime is a monotonic clock in nanoseconds.
func nanotime() int64 { return int64(time.Since(clockStart)) }
