package clock

import (
	"context"
	"sync"
	"time"
)

// Fake is a [Clock] that stands still until a test moves it. [Fake.Advance] moves it
// forward and fires every timer and ticker that falls due on the way, in deadline
// order (creation order among equal deadlines), each at its own deadline. Channel
// timers deliver into a one-slot buffer, as a receiver that is not ready yet would
// later find them; AfterFunc callbacks run in their own goroutines, as with [Real].
//
// The goroutines a timer wakes run concurrently with Advance, so one that arms a new
// timer when the last fired (a poll loop, a backoff) arms it from wherever the fake
// has got to, and sees it fire at most once per Advance. Advance in steps no longer
// than the shortest period the test relies on, waiting between steps for the effect
// of each.
//
// A test that must not advance before a goroutine is parked on a timer waits for it
// with [Fake.BlockUntil] (a number of armed timers) or [Fake.BlockUntilArmed] (a timer
// of a given length). Fake is safe for concurrent use.
//
// Everything an engine component times runs on its clock, so on a Fake nothing that
// waits moves until the test advances it: a node's writes, for one, wait out the
// group commit window (store.GroupCommitOptions.MaxDelay, 2 ms by default) on it.
//
// Its times carry no monotonic reading: the wall clock and the monotonic clock are one
// unless [Fake.StepWall] moves the wall clock alone.
type Fake struct {
	mu      sync.Mutex
	now     time.Time
	skew    time.Duration
	seq     uint64
	waiters map[*fakeTimer]struct{}
	changed chan struct{}
}

// NewFake returns a fake clock that reads start (stripped of any monotonic reading).
func NewFake(start time.Time) *Fake {
	return &Fake{now: start.Round(0), waiters: map[*fakeTimer]struct{}{}, changed: make(chan struct{})}
}

// Now returns the fake's current time.
func (f *Fake) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Since returns the fake time elapsed since t.
func (f *Fake) Since(t time.Time) time.Duration { return f.Now().Sub(t) }

// Until returns the fake time until t.
func (f *Fake) Until(t time.Time) time.Duration { return t.Sub(f.Now()) }

// Wall returns the fake's wall clock: Now plus every step of StepWall.
func (f *Fake) Wall() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now.Add(f.skew)
}

// NewTimer returns a fake timer that fires once the fake has advanced by d.
func (f *Fake) NewTimer(d time.Duration) Timer {
	t := &fakeTimer{f: f, c: make(chan time.Time, 1)}
	f.arm(t, d)
	return t
}

// NewTicker returns a fake ticker of period d.
func (f *Fake) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive interval for NewTicker")
	}
	t := &fakeTimer{f: f, c: make(chan time.Time, 1), period: d}
	f.arm(t, d)
	return fakeTicker{t}
}

// After returns the channel of a new fake timer.
func (f *Fake) After(d time.Duration) <-chan time.Time { return f.NewTimer(d).C() }

// AfterFunc returns a fake timer that calls fn in its own goroutine when it fires.
func (f *Fake) AfterFunc(d time.Duration, fn func()) Timer {
	t := &fakeTimer{f: f, fn: fn}
	f.arm(t, d)
	return t
}

// Sleep waits until the fake has advanced by d, or ctx ends.
func (f *Fake) Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := f.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Advance moves the fake forward by d, firing every timer and ticker that falls due
// on the way at its own deadline. A ticker whose period is shorter than d fires at
// each of its deadlines, but its one-slot channel keeps only the first undelivered
// tick, as a [time.Ticker] drops the ticks a slow receiver misses; a timer its
// goroutine re-arms on firing fires once.
func (f *Fake) Advance(d time.Duration) {
	f.mu.Lock()
	target := f.now.Add(d)
	for {
		t := f.nextDue(target)
		if t == nil {
			break
		}
		f.now = t.when
		f.fire(t)
	}
	if target.After(f.now) {
		f.now = target
	}
	if d != 0 {
		f.notify()
	}
	f.mu.Unlock()
}

// StepWall moves the wall clock alone by d, as a suspended machine (whose monotonic
// clock stood still) or a stepped system clock does. Timers are not affected.
func (f *Fake) StepWall(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.skew += d
}

// Waiters returns how many timers, tickers and sleeps are armed.
func (f *Fake) Waiters() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.waiters)
}

// BlockUntil waits until at least n timers, tickers and sleeps are armed, or ctx
// ends.
func (f *Fake) BlockUntil(ctx context.Context, n int) error {
	for {
		f.mu.Lock()
		if len(f.waiters) >= n {
			f.mu.Unlock()
			return nil
		}
		ch := f.changed
		f.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// BlockUntilArmed waits until a timer, ticker or sleep is armed to fire d after the
// fake's current time, or ctx ends: a goroutine is parked on a wait of that length.
func (f *Fake) BlockUntilArmed(ctx context.Context, d time.Duration) error {
	for {
		f.mu.Lock()
		due := f.now.Add(d)
		for t := range f.waiters {
			if t.when.Equal(due) {
				f.mu.Unlock()
				return nil
			}
		}
		ch := f.changed
		f.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (f *Fake) nextDue(target time.Time) *fakeTimer {
	var next *fakeTimer
	for t := range f.waiters {
		if t.when.After(target) {
			continue
		}
		if next == nil || t.when.Before(next.when) || (t.when.Equal(next.when) && t.seq < next.seq) {
			next = t
		}
	}
	return next
}

func (f *Fake) fire(t *fakeTimer) {
	now := f.now
	if t.period > 0 {
		t.when = t.when.Add(t.period)
		f.seq++
		t.seq = f.seq
	} else {
		f.remove(t)
	}
	if t.fn != nil {
		go t.fn()
		return
	}
	select {
	case t.c <- now:
	default:
	}
}

func (f *Fake) arm(t *fakeTimer, d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.armLocked(t, d)
}

func (f *Fake) armLocked(t *fakeTimer, d time.Duration) {
	f.seq++
	t.seq = f.seq
	t.when = f.now.Add(d)
	if d <= 0 && t.period == 0 {
		f.waiters[t] = struct{}{}
		f.fire(t)
		f.notify()
		return
	}
	f.waiters[t] = struct{}{}
	f.notify()
}

func (f *Fake) remove(t *fakeTimer) bool {
	if _, ok := f.waiters[t]; !ok {
		return false
	}
	delete(f.waiters, t)
	f.notify()
	return true
}

func (f *Fake) notify() {
	close(f.changed)
	f.changed = make(chan struct{})
}

type fakeTimer struct {
	f      *Fake
	c      chan time.Time
	fn     func()
	period time.Duration
	when   time.Time
	seq    uint64
}

func (t *fakeTimer) C() <-chan time.Time { return t.c }

func (t *fakeTimer) Stop() bool {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	stopped := t.f.remove(t)
	return t.drain() || stopped
}

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.f.mu.Lock()
	defer t.f.mu.Unlock()
	armed := t.f.remove(t)
	armed = t.drain() || armed
	t.f.armLocked(t, d)
	return armed
}

func (t *fakeTimer) drain() bool {
	if t.c == nil {
		return false
	}
	select {
	case <-t.c:
		return true
	default:
		return false
	}
}

type fakeTicker struct{ t *fakeTimer }

func (k fakeTicker) C() <-chan time.Time { return k.t.c }
func (k fakeTicker) Stop()               { k.t.Stop() }

func (k fakeTicker) Reset(d time.Duration) {
	if d <= 0 {
		panic("clock: non-positive interval for Ticker.Reset")
	}
	k.t.f.mu.Lock()
	defer k.t.f.mu.Unlock()
	k.t.f.remove(k.t)
	k.t.drain()
	k.t.period = d
	k.t.f.armLocked(k.t, d)
}
