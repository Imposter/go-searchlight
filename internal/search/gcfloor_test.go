package search

import (
	"runtime"
	"runtime/debug"
	"testing"
	"time"
)

func TestGCPercentKeepsTheFloor(t *testing.T) {
	const floor = 64 << 20
	for _, tc := range []struct {
		live uint64
		want int
	}{
		{2 << 20, 3100},
		{16 << 20, 300},
		{32 << 20, 100},
		{1 << 30, 100},
		{1 << 20, maxGCPercent},
		{1, maxGCPercent},
	} {
		if got := gcPercent(tc.live, floor); got != tc.want {
			t.Errorf("gcPercent(%d) = %d, want %d", tc.live, got, tc.want)
		}
	}
}

// TestKeepHeapFloorRetunesEachCycle: after a collection the tuner has raised GOGC for a
// small heap, and stopping it restores 100.
func TestKeepHeapFloorRetunesEachCycle(t *testing.T) {
	stop := KeepHeapFloor(1 << 40)
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
