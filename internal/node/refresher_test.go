package node

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/clock"
)

// The refresher's schedule, on a fake clock: ticks fall on a fixed grid from its start
// whatever each refresh costs; a refresh that overruns skips the grid points it missed
// and is not followed by another at once; refreshes never overlap; a new interval
// re-anchors the grid at the change; an interval of 0 stops ticking until the next
// change.
func TestRefresherTicksOnAFixedGrid(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	clk := clock.NewFake(start)
	var interval atomic.Int64
	interval.Store(int64(time.Second))
	wake := make(chan struct{}, 1)
	ticked := make(chan time.Time)
	release := make(chan struct{})
	var running atomic.Int32
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		runGrid(runCtx, clk, func() time.Duration { return time.Duration(interval.Load()) }, wake, func() {
			if running.Add(1) != 1 {
				t.Error("two refreshes at once")
			}
			ticked <- clk.Now()
			<-release
			running.Add(-1)
		})
	}()

	// tick waits for the refresher to arm wait, advances to it, takes the refresh it
	// starts (checking its time), lets it run for cost, and ends it.
	tick := func(wait, cost time.Duration, want time.Time) {
		t.Helper()
		if err := clk.BlockUntilArmed(ctx, wait); err != nil {
			t.Fatalf("the refresher is not waiting %v for its next tick (now %v): %v", wait, clk.Since(start), err)
		}
		clk.Advance(wait)
		select {
		case at := <-ticked:
			if !at.Equal(want) {
				t.Fatalf("a refresh at start+%v, want start+%v", at.Sub(start), want.Sub(start))
			}
		case <-ctx.Done():
			t.Fatal("no refresh")
		}
		clk.Advance(cost)
		release <- struct{}{}
	}
	at := start.Add

	tick(time.Second, 300*time.Millisecond, at(time.Second))
	tick(700*time.Millisecond, 999*time.Millisecond, at(2*time.Second))
	tick(time.Millisecond, 0, at(3*time.Second))
	// An overrun of 2.5 intervals skips the ticks at 5 s and 6 s: the next is at 7 s.
	tick(time.Second, 2500*time.Millisecond, at(4*time.Second))
	tick(500*time.Millisecond, 0, at(7*time.Second))

	// A new interval, mid-period, re-anchors the grid where it changes.
	clk.Advance(250 * time.Millisecond)
	interval.Store(int64(400 * time.Millisecond))
	wake <- struct{}{}
	tick(400*time.Millisecond, 100*time.Millisecond, at(7650*time.Millisecond))
	tick(300*time.Millisecond, 0, at(8050*time.Millisecond))

	// Disabled: nothing is armed and nothing ticks; enabled again, the grid starts
	// from then.
	interval.Store(0)
	wake <- struct{}{}
	for clk.Waiters() != 0 {
		if ctx.Err() != nil {
			t.Fatal("the refresher still waits for a tick while disabled")
		}
		runtime.Gosched()
	}
	clk.Advance(time.Minute)
	interval.Store(int64(time.Second))
	wake <- struct{}{}
	tick(time.Second, 0, at(8050*time.Millisecond+time.Minute+time.Second))

	stop()
	<-done
}
