//go:build !race

package testtier

// Race reports whether the race detector is on: it slows real-time paths several
// times over, so latency bounds scale by RaceSlowdown.
const Race = false

// RaceSlowdown is the factor latency bounds scale by under the race detector.
const RaceSlowdown = 1
