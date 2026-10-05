package clock_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/clock"
)

var epoch = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

func waitCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func received(c <-chan time.Time) (time.Time, bool) {
	select {
	case v := <-c:
		return v, true
	default:
		return time.Time{}, false
	}
}

func TestFakeTimerFiresAtItsDeadline(t *testing.T) {
	f := clock.NewFake(epoch)
	tm := f.NewTimer(time.Second)
	f.Advance(999 * time.Millisecond)
	if _, ok := received(tm.C()); ok {
		t.Fatal("fired early")
	}
	f.Advance(5 * time.Second)
	v, ok := received(tm.C())
	if !ok || !v.Equal(epoch.Add(time.Second)) {
		t.Fatalf("got %v %v, want the deadline %v", v, ok, epoch.Add(time.Second))
	}
	if got := f.Since(epoch); got != 5999*time.Millisecond {
		t.Fatalf("Since = %v", got)
	}
	if f.Waiters() != 0 {
		t.Fatalf("a fired timer stays armed: %d", f.Waiters())
	}
}

func TestFakeStopAndResetLeaveNoStaleValue(t *testing.T) {
	f := clock.NewFake(epoch)
	tm := f.NewTimer(time.Second)
	f.Advance(time.Second)
	if !tm.Stop() {
		t.Fatal("Stop of an undelivered timer reported nothing stopped")
	}
	if _, ok := received(tm.C()); ok {
		t.Fatal("a stale value survived Stop")
	}
	tm.Reset(time.Second)
	f.Advance(time.Second)
	tm.Reset(time.Second)
	if _, ok := received(tm.C()); ok {
		t.Fatal("a stale value survived Reset")
	}
	f.Advance(time.Second)
	if _, ok := received(tm.C()); !ok {
		t.Fatal("a reset timer did not fire")
	}
	if tm.Stop() {
		t.Fatal("Stop of a delivered timer reported a stop")
	}
}

func TestFakeFiresEachTimerAtItsOwnDeadline(t *testing.T) {
	f := clock.NewFake(epoch)
	ds := []time.Duration{3 * time.Second, time.Second, 2 * time.Second, time.Second}
	timers := make([]clock.Timer, len(ds))
	for i, d := range ds {
		timers[i] = f.NewTimer(d)
	}
	ran := make(chan time.Time, 1)
	f.AfterFunc(1500*time.Millisecond, func() { ran <- f.Now() })
	f.Advance(10 * time.Second)
	for i, tm := range timers {
		if v, ok := received(tm.C()); !ok || !v.Equal(epoch.Add(ds[i])) {
			t.Fatalf("timer %d: %v %v, want %v", i, v, ok, epoch.Add(ds[i]))
		}
	}
	if v := <-ran; v.Before(epoch.Add(1500 * time.Millisecond)) {
		t.Fatalf("AfterFunc ran at %v", v)
	}
}

func TestFakeTickerTicksEachPeriodAndDropsMissedTicks(t *testing.T) {
	f := clock.NewFake(epoch)
	k := f.NewTicker(time.Second)
	defer k.Stop()
	for i := 1; i <= 3; i++ {
		f.Advance(time.Second)
		v, ok := received(k.C())
		if !ok || !v.Equal(epoch.Add(time.Duration(i)*time.Second)) {
			t.Fatalf("tick %d: %v %v", i, v, ok)
		}
	}
	f.Advance(5 * time.Second)
	if v, ok := received(k.C()); !ok || !v.Equal(epoch.Add(4*time.Second)) {
		t.Fatalf("the first missed tick: %v %v", v, ok)
	}
	if _, ok := received(k.C()); ok {
		t.Fatal("missed ticks were queued")
	}
	k.Reset(10 * time.Second)
	f.Advance(9 * time.Second)
	if _, ok := received(k.C()); ok {
		t.Fatal("ticked before the new period")
	}
	f.Advance(time.Second)
	if _, ok := received(k.C()); !ok {
		t.Fatal("no tick at the new period")
	}
}

func TestFakeSleepAndBlockUntil(t *testing.T) {
	f := clock.NewFake(epoch)
	done := make(chan error, 1)
	go func() { done <- f.Sleep(context.Background(), time.Minute) }()
	if err := f.BlockUntil(waitCtx(t), 1); err != nil {
		t.Fatal(err)
	}
	f.Advance(time.Minute - 1)
	select {
	case <-done:
		t.Fatal("woke early")
	default:
	}
	f.Advance(1)
	if err := <-done; err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() { done <- f.Sleep(ctx, time.Minute) }()
	if err := f.BlockUntil(waitCtx(t), 1); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; err == nil {
		t.Fatal("a cancelled sleep returned nil")
	}
	if f.Waiters() != 0 {
		t.Fatalf("a cancelled sleep stays armed: %d", f.Waiters())
	}

	short, cancelShort := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancelShort()
	if err := f.BlockUntil(short, 1); err == nil {
		t.Fatal("BlockUntil returned with nothing armed")
	}
}

func TestFakeWallStepsAlone(t *testing.T) {
	f := clock.NewFake(epoch)
	start, wall := f.Now(), f.Wall()
	f.StepWall(time.Hour)
	f.Advance(time.Second)
	if got := f.Since(start); got != time.Second {
		t.Fatalf("monotonic elapsed %v", got)
	}
	if got := f.Wall().Sub(wall); got != time.Hour+time.Second {
		t.Fatalf("wall elapsed %v", got)
	}
}

func TestFakeConcurrentUse(t *testing.T) {
	f := clock.NewFake(epoch)
	var fired atomic.Int64
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				tm := f.NewTimer(time.Millisecond)
				f.Advance(time.Millisecond)
				<-tm.C()
				fired.Add(1)
			}
		}()
	}
	wg.Wait()
	if fired.Load() != 800 {
		t.Fatalf("fired %d", fired.Load())
	}
}

func TestRealSleepHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (clock.Real{}).Sleep(ctx, time.Hour); err == nil {
		t.Fatal("a cancelled sleep returned nil")
	}
	if err := (clock.Real{}).Sleep(context.Background(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

func TestFakeBlockUntilArmed(t *testing.T) {
	f := clock.NewFake(epoch)
	f.NewTimer(time.Hour)
	done := make(chan error, 1)
	go func() { done <- f.Sleep(context.Background(), time.Second) }()
	if err := f.BlockUntilArmed(waitCtx(t), time.Second); err != nil {
		t.Fatal(err)
	}
	f.Advance(time.Second)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	short, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := f.BlockUntilArmed(short, time.Second); err == nil {
		t.Fatal("BlockUntilArmed returned with no such timer armed")
	}
	if err := f.BlockUntilArmed(waitCtx(t), time.Hour-time.Second); err != nil {
		t.Fatal(err)
	}
}
