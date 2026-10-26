package prio

import (
	"context"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/clock"
)

// yielded runs Yield in a goroutine and returns a channel closed once it returns.
func yielded(ctx context.Context, g *Gate) <-chan error {
	done := make(chan error, 1)
	go func() { done <- g.Yield(ctx) }()
	return done
}

func returned(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Yield did not return")
		return nil
	}
}

func notReturned(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("Yield returned (%v) during a pause", err)
	case <-time.After(20 * time.Millisecond):
	}
}

// Without searches, and on a gate of share 1 or none, Yield never waits.
func TestYieldIsFreeWithoutSearches(t *testing.T) {
	ctx := context.Background()
	fake := clock.NewFake(time.Unix(0, 0))
	g := New(0.25, fake)
	if err := returned(t, yielded(ctx, g)); err != nil {
		t.Fatal(err)
	}
	off := New(1, fake)
	done := off.Foreground()
	defer done()
	if err := returned(t, yielded(ctx, off)); err != nil {
		t.Fatal(err)
	}
	var none *Gate
	defer none.Foreground()()
	if err := none.Yield(ctx); err != nil || none.Share() != 1 {
		t.Fatalf("a nil gate: %v, share %g", err, none.Share())
	}
}

// While a search runs, background work pauses for the first (1 - share) of a cycle,
// then runs the rest of it without pausing, then pauses again; it resumes at once when
// the searches finish, and a cancelled context ends its wait.
func TestYieldPausesForSearches(t *testing.T) {
	ctx := context.Background()
	fake := clock.NewFake(time.Unix(0, 0))
	g := New(0.25, fake)
	pause := time.Duration(0.75 * float64(Period))

	search := g.Foreground()
	waiting := yielded(ctx, g)
	notReturned(t, waiting)
	if err := fake.BlockUntil(ctx, 1); err != nil {
		t.Fatal(err)
	}
	fake.Advance(pause)
	if err := returned(t, waiting); err != nil {
		t.Fatal(err)
	}
	fake.Advance(Period/4 - time.Millisecond)
	if err := returned(t, yielded(ctx, g)); err != nil {
		t.Fatal("the run part of the cycle paused")
	}
	fake.Advance(time.Millisecond)
	waiting = yielded(ctx, g)
	notReturned(t, waiting)
	search()
	if err := returned(t, waiting); err != nil {
		t.Fatal(err)
	}
	if g.Active() != 0 {
		t.Fatalf("%d searches still active", g.Active())
	}

	other := g.Foreground()
	defer other()
	fake.Advance(Period)
	cctx, cancel := context.WithCancel(ctx)
	waiting = yielded(cctx, g)
	notReturned(t, waiting)
	cancel()
	if err := returned(t, waiting); err == nil {
		t.Fatal("a cancelled wait returned no error")
	}
}

// Every Foreground's done counts once, however often it is called.
func TestForegroundDoneOnce(t *testing.T) {
	g := New(0.5, clock.NewFake(time.Unix(0, 0)))
	a, b := g.Foreground(), g.Foreground()
	a()
	a()
	if g.Active() != 1 {
		t.Fatalf("active %d, want 1", g.Active())
	}
	b()
	if g.Active() != 0 {
		t.Fatalf("active %d, want 0", g.Active())
	}
	if New(0, nil).Share() != MinShare || New(3, nil).Share() != 1 {
		t.Fatal("shares are not clamped")
	}
}
