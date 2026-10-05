package node

import (
	"fmt"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/clock"
)

// TestAbsentIndexesExpireByTheClock: a name the store did not have is answered absent
// for absentTTL by the node's clock, then asked about again; forgetting it (a create)
// ends that at once, and a lookup that raced the forget does not record its stale
// answer.
func TestAbsentIndexesExpireByTheClock(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC))
	a := absentIndexes{clock: clk}
	gen, absent := a.check("ghost")
	if absent {
		t.Fatal("an unknown name is absent before any lookup")
	}
	a.note("ghost", gen)
	if _, absent := a.check("ghost"); !absent {
		t.Fatal("a name the store just did not have is not absent")
	}
	clk.Advance(absentTTL - time.Nanosecond)
	if _, absent := a.check("ghost"); !absent {
		t.Fatal("the absence ran out before absentTTL")
	}
	clk.Advance(time.Nanosecond)
	if _, absent := a.check("ghost"); absent {
		t.Fatal("the absence outlived absentTTL")
	}

	gen, _ = a.check("made")
	a.note("made", gen)
	a.forget("made")
	if _, absent := a.check("made"); absent {
		t.Fatal("a created index is still absent")
	}
	raced, _ := a.check("raced")
	a.forget("other")
	a.note("raced", raced)
	if _, absent := a.check("raced"); absent {
		t.Fatal("a lookup that raced a forget recorded its answer")
	}
}

// TestAbsentIndexesAreBounded: past absentMax names, expired ones are dropped to make
// room, and while none has expired a new name is not recorded.
func TestAbsentIndexesAreBounded(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC))
	a := absentIndexes{clock: clk}
	for i := range absentMax {
		gen, _ := a.check("")
		a.note(fmt.Sprintf("n%d", i), gen)
	}
	gen, _ := a.check("")
	a.note("full", gen)
	if _, absent := a.check("full"); absent {
		t.Fatal("a name was recorded past absentMax")
	}
	clk.Advance(absentTTL)
	a.note("room", gen)
	if _, absent := a.check("room"); !absent || len(a.names) != 1 {
		t.Fatalf("expired names were not dropped: absent %v, %d names", absent, len(a.names))
	}
}
