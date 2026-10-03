//go:build race

package replica

// raceEnabled: the race detector slows the convergence test several times over.
const raceEnabled = true
