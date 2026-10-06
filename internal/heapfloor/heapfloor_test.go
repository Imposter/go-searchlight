package heapfloor

import (
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

func TestPercentAimsAtTheFloor(t *testing.T) {
	const floor = 64 << 20
	for _, tc := range []struct {
		live, roots uint64
		want        int
	}{
		{2 << 20, 0, 3100},
		{2 << 20, 2 << 20, 1550},
		{16 << 20, 0, 300},
		{16 << 20, 16 << 20, 150},
		{30 << 20, 30 << 20, 100},
		{32 << 20, 0, 100},
		{1 << 30, 0, 100},
		{1, 0, maxPercent},
	} {
		if got := percent(tc.live, tc.roots, floor); got != tc.want {
			t.Errorf("percent(%d, %d) = %d, want %d", tc.live, tc.roots, got, tc.want)
		}
	}
	if got := percent(1<<20, 0, 0); got != 100 {
		t.Errorf("no floor: %d", got)
	}
}

func TestEffectiveCapsAtAQuarterOfTheLimit(t *testing.T) {
	old := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(old) })
	debug.SetMemoryLimit(100 << 20)
	if got := Effective(64 << 20); got != 25<<20 {
		t.Errorf("under a 100 MiB limit: %d", got)
	}
	debug.SetMemoryLimit(1 << 40)
	if got := Effective(64 << 20); got != 64<<20 {
		t.Errorf("under a large limit: %d", got)
	}
	if got := Effective(0); got != 0 {
		t.Errorf("off: %d", got)
	}
}

// TestKeepRetunesEachCycle: after a collection the tuner has raised GOGC for a small
// heap, and stopping it restores 100.
func TestKeepRetunesEachCycle(t *testing.T) {
	stop := Keep(1 << 40)
	t.Cleanup(stop)
	deadline := time.Now().Add(10 * time.Second)
	for {
		runtime.GC()
		pct := debug.SetGCPercent(100)
		debug.SetGCPercent(pct)
		if pct > 100 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("GOGC stayed %d", pct)
		}
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	if pct := debug.SetGCPercent(100); pct != 100 {
		t.Fatalf("after stop, GOGC is %d", pct)
	}
}
