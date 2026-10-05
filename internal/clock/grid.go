package clock

import (
	"context"
	"time"
)

// Grid is a fixed-period schedule: its ticks fall at Anchor + k·Period, for k = 1, 2,
// and so on. A task run at each tick keeps that phase whatever it costs, unlike a timer
// re-armed after the task ends, whose period is the interval plus the task's run time;
// and a task that overruns skips the ticks it missed instead of running again at once
// to catch up. A zero or negative Period has no ticks.
type Grid struct {
	Anchor time.Time
	Period time.Duration
}

// NewGrid returns the grid of period anchored at anchor.
func NewGrid(anchor time.Time, period time.Duration) Grid {
	return Grid{Anchor: anchor, Period: period}
}

// Next returns the grid's first tick strictly after now, and false for a grid with no
// ticks.
func (g Grid) Next(now time.Time) (time.Time, bool) {
	if g.Period <= 0 {
		return time.Time{}, false
	}
	elapsed := max(0, now.Sub(g.Anchor))
	return g.Anchor.Add(g.Period * time.Duration(int64(elapsed/g.Period)+1)), true
}

// GridLoop runs a task on a [Grid], from one goroutine, so runs never overlap.
type GridLoop struct {
	Clock Clock
	// Period is read when the loop starts and at every Reanchor; 0 or less runs the
	// task only on Now.
	Period func() time.Duration
	// Phase, in (0, Period), puts the first tick Phase after the start instead of a
	// whole Period after it, so loops that start together need not tick together.
	Phase time.Duration
	// Reanchor, when it receives, anchors the grid anew at that moment with Period().
	Reanchor <-chan struct{}
	// Now, when it receives, runs the task at once; the grid keeps its phase.
	Now <-chan struct{}
	// Task is the work done at each tick.
	Task func()
}

// Run runs the loop until ctx ends.
func (l GridLoop) Run(ctx context.Context) {
	anchor := func(phase time.Duration) Grid {
		p := l.Period()
		if phase <= 0 || phase >= p {
			return NewGrid(l.Clock.Now(), p)
		}
		return NewGrid(l.Clock.Now().Add(phase-p), p)
	}
	grid := anchor(l.Phase)
	for {
		var fire <-chan time.Time
		var timer Timer
		if next, ok := grid.Next(l.Clock.Now()); ok {
			timer = l.Clock.NewTimer(l.Clock.Until(next))
			fire = timer.C()
		}
		run := false
		select {
		case <-ctx.Done():
		case <-l.Reanchor:
			grid = anchor(0)
		case <-l.Now:
			run = true
		case <-fire:
			run = true
		}
		if timer != nil {
			timer.Stop()
		}
		if ctx.Err() != nil {
			return
		}
		if run {
			l.Task()
		}
	}
}
