package clock

import "time"

// Grid is a fixed-period schedule: its ticks fall at Anchor + k·Period for every
// whole k. A task run at each tick keeps that phase whatever it costs, unlike a timer
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
