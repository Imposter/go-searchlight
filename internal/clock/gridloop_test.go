package clock_test

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/clock"
)

// gridHarness runs a GridLoop on a fake clock whose task reports when it ran and then
// waits for the test to end it.
type gridHarness struct {
	t       *testing.T
	ctx     context.Context
	clk     *clock.Fake
	ticked  chan time.Time
	release chan struct{}
	done    chan struct{}
}

func runGridLoop(t *testing.T, l clock.GridLoop) *gridHarness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	h := &gridHarness{t: t, ctx: ctx, clk: l.Clock.(*clock.Fake), ticked: make(chan time.Time), release: make(chan struct{}), done: make(chan struct{})} //nolint:forcetypeassert,errcheck // the tests pass a Fake
	var running atomic.Int32
	l.Task = func() {
		if running.Add(1) != 1 {
			t.Error("two runs at once")
		}
		h.ticked <- h.clk.Now()
		<-h.release
		running.Add(-1)
	}
	runCtx, stop := context.WithCancel(ctx)
	go func() {
		defer close(h.done)
		l.Run(runCtx)
	}()
	t.Cleanup(func() {
		stop()
		<-h.done
	})
	return h
}

// tick waits for the loop to arm wait, advances to it, takes the run it starts (at
// want), lets it run for cost, and ends it.
func (h *gridHarness) tick(wait, cost time.Duration, want time.Time) {
	h.t.Helper()
	if err := h.clk.BlockUntilArmed(h.ctx, wait); err != nil {
		h.t.Fatalf("the loop is not waiting %v for its next tick (now %v): %v", wait, h.clk.Since(epoch), err)
	}
	h.clk.Advance(wait)
	h.took(cost, want)
}

// took takes the run the loop started (at want), lets it run for cost, and ends it.
func (h *gridHarness) took(cost time.Duration, want time.Time) {
	h.t.Helper()
	select {
	case at := <-h.ticked:
		if !at.Equal(want) {
			h.t.Fatalf("a run at epoch+%v, want epoch+%v", at.Sub(epoch), want.Sub(epoch))
		}
	case <-h.ctx.Done():
		h.t.Fatal("no run")
	}
	h.clk.Advance(cost)
	h.release <- struct{}{}
}

// Ticks fall on a fixed grid from the start whatever each run costs; a run that
// overruns skips the grid points it missed and is not followed by another at once;
// runs never overlap; Reanchor re-anchors the grid with the new period; Now runs at
// once and keeps the grid; a period of 0 stops ticking until the next Reanchor.
func TestGridLoopTicksOnAFixedGrid(t *testing.T) {
	var period atomic.Int64
	period.Store(int64(time.Second))
	reanchor, now := make(chan struct{}, 1), make(chan struct{}, 1)
	clk := clock.NewFake(epoch)
	h := runGridLoop(t, clock.GridLoop{
		Clock: clk, Period: func() time.Duration { return time.Duration(period.Load()) },
		Reanchor: reanchor, Now: now,
	})
	at := epoch.Add

	h.tick(time.Second, 300*time.Millisecond, at(time.Second))
	h.tick(700*time.Millisecond, 999*time.Millisecond, at(2*time.Second))
	h.tick(time.Millisecond, 0, at(3*time.Second))
	// An overrun of 2.5 periods skips the ticks at 5 s and 6 s: the next is at 7 s.
	h.tick(time.Second, 2500*time.Millisecond, at(4*time.Second))
	h.tick(500*time.Millisecond, 0, at(7*time.Second))

	clk.Advance(250 * time.Millisecond)
	period.Store(int64(400 * time.Millisecond))
	reanchor <- struct{}{}
	h.tick(400*time.Millisecond, 100*time.Millisecond, at(7650*time.Millisecond))
	h.tick(300*time.Millisecond, 0, at(8050*time.Millisecond))

	if err := clk.BlockUntilArmed(h.ctx, 400*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	clk.Advance(100 * time.Millisecond)
	now <- struct{}{}
	h.took(0, at(8150*time.Millisecond))
	h.tick(300*time.Millisecond, 0, at(8450*time.Millisecond))

	period.Store(0)
	reanchor <- struct{}{}
	for clk.Waiters() != 0 {
		if h.ctx.Err() != nil {
			t.Fatal("the loop still waits for a tick with a period of 0")
		}
		runtime.Gosched()
	}
	clk.Advance(time.Minute)
	period.Store(int64(time.Second))
	reanchor <- struct{}{}
	h.tick(time.Second, 0, at(8450*time.Millisecond+time.Minute+time.Second))
}

// A phase puts the first tick that far after the start; the grid then keeps it.
func TestGridLoopPhase(t *testing.T) {
	clk := clock.NewFake(epoch)
	h := runGridLoop(t, clock.GridLoop{Clock: clk, Period: func() time.Duration { return time.Second }, Phase: 300 * time.Millisecond})
	h.tick(300*time.Millisecond, 0, epoch.Add(300*time.Millisecond))
	h.tick(time.Second, 0, epoch.Add(1300*time.Millisecond))
}
