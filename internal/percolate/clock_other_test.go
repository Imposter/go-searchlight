//go:build !windows

package percolate

import "time"

var clockBase = time.Now()

func clockNow() int64 { return int64(time.Since(clockBase)) }

func clockSince(start int64) time.Duration { return time.Duration(clockNow() - start) }
