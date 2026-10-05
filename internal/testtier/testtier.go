// Package testtier sorts the tests into two tiers. The short tier is what
// `go test -short ./...` runs, in under two minutes on a developer machine. The heavy
// tier runs only without -short: tests that wait on real time (cluster failover,
// rolling restarts, lease and latency bounds), the 100,000-document recovery, the
// full matrices of the byte-identity tests, and the end-to-end run of every
// benchmark workload. A heavy test whose behaviour nothing else covers keeps a
// smaller case in the short tier. CI runs both tiers under -race.
package testtier

import "testing"

// Heavy skips t in -short runs.
func Heavy(t testing.TB) {
	t.Helper()
	if testing.Short() {
		t.Skip("heavy tier: runs without -short")
	}
}

// Pick returns short in -short runs and heavy otherwise, for a test that runs a
// smaller case in the short tier and its full matrix in the heavy one.
func Pick[T any](short, heavy T) T {
	if testing.Short() {
		return short
	}
	return heavy
}
