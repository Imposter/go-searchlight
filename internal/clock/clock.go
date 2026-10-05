// Package clock is the engine's source of time. Every timer, ticker, sleep and
// elapsed-time reading in the engine goes through a [Clock], so that production runs on
// [Real] while tests drive a [Fake] deterministically, advancing it instead of waiting.
//
// Durations are taken by the monotonic clock: [Clock.Now] carries a monotonic reading
// (on [Real]) and [Clock.Since] and [Clock.Until] subtract by it, so a stepped wall
// clock never stretches or shrinks a measured interval. [Clock.Wall] is the wall clock,
// for the one place it is semantically required: it keeps running while the machine
// is suspended, when a monotonic clock may stand still (Linux's does), so lease
// validity checks both (see internal/cluster).
//
// Direct calls of time.Now, time.Since, time.After, time.NewTimer, time.NewTicker,
// time.AfterFunc and time.Sleep are forbidden in the engine's production packages by
// the forbidigo rule in .golangci.yml.
package clock

import (
	"context"
	"time"
)

// Clock tells the time and makes timers. It is safe for concurrent use.
type Clock interface {
	// Now returns the current time, with a monotonic reading where the clock has one.
	Now() time.Time
	// Since returns the time elapsed since t, by the monotonic reading when t has one.
	Since(t time.Time) time.Duration
	// Until returns the duration until t, by the monotonic reading when t has one.
	Until(t time.Time) time.Duration
	// Wall returns the wall clock with no monotonic reading: it keeps running while
	// the machine is suspended, and it may be stepped.
	Wall() time.Time
	// NewTimer returns a timer that sends the time on its channel once d has passed.
	NewTimer(d time.Duration) Timer
	// NewTicker returns a ticker that sends the time on its channel every d; d must
	// be positive.
	NewTicker(d time.Duration) Ticker
	// After returns a channel that receives the time once d has passed.
	After(d time.Duration) <-chan time.Time
	// AfterFunc calls f in its own goroutine once d has passed. The timer's channel
	// is nil.
	AfterFunc(d time.Duration, f func()) Timer
	// Sleep waits for d to pass or ctx to end, whichever is first, and returns
	// ctx.Err() if ctx ended first.
	Sleep(ctx context.Context, d time.Duration) error
}

// Timer is a one-shot timer, with the semantics of [time.Timer] since Go 1.23: after
// Stop or Reset returns, no stale time is received from C.
type Timer interface {
	// C is the channel the time is sent on; nil for a timer made by AfterFunc.
	C() <-chan time.Time
	// Stop prevents the timer from firing. It reports whether it stopped the timer.
	Stop() bool
	// Reset re-arms the timer to fire after d. It reports whether the timer was
	// armed.
	Reset(d time.Duration) bool
}

// Ticker sends the time on its channel at a fixed period, dropping ticks a slow
// receiver misses, like [time.Ticker].
type Ticker interface {
	// C is the channel the ticks are sent on.
	C() <-chan time.Time
	// Stop turns the ticker off.
	Stop()
	// Reset stops the ticker and restarts it with period d.
	Reset(d time.Duration)
}

// Real is the process's clock: the time package.
type Real struct{}

// Now returns time.Now().
func (Real) Now() time.Time { return time.Now() }

// Since returns time.Since(t).
func (Real) Since(t time.Time) time.Duration { return time.Since(t) }

// Until returns time.Until(t).
func (Real) Until(t time.Time) time.Duration { return time.Until(t) }

// Wall returns time.Now() stripped of its monotonic reading.
func (Real) Wall() time.Time { return time.Now().Round(0) }

// NewTimer returns a [time.Timer].
func (Real) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }

// NewTicker returns a [time.Ticker].
func (Real) NewTicker(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} }

// After returns time.After(d).
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// AfterFunc returns time.AfterFunc(d, f).
func (Real) AfterFunc(d time.Duration, f func()) Timer { return realTimer{time.AfterFunc(d, f)} }

// Sleep waits for d to pass or ctx to end.
func (Real) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time        { return r.t.C }
func (r realTimer) Stop() bool                 { return r.t.Stop() }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }

type realTicker struct{ t *time.Ticker }

func (r realTicker) C() <-chan time.Time   { return r.t.C }
func (r realTicker) Stop()                 { r.t.Stop() }
func (r realTicker) Reset(d time.Duration) { r.t.Reset(d) }
