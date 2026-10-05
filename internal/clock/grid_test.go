package clock_test

import (
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/clock"
)

func TestGridNextIsTheFirstTickAfterNow(t *testing.T) {
	g := clock.NewGrid(epoch, time.Second)
	for _, c := range []struct {
		now, want time.Duration
	}{
		{-5 * time.Second, time.Second},
		{0, time.Second},
		{300 * time.Millisecond, time.Second},
		{time.Second, 2 * time.Second},
		{1999 * time.Millisecond, 2 * time.Second},
		{3500 * time.Millisecond, 4 * time.Second},
	} {
		got, ok := g.Next(epoch.Add(c.now))
		if !ok || !got.Equal(epoch.Add(c.want)) {
			t.Errorf("Next(anchor%+v) = %v, %v; want anchor+%v", c.now, got.Sub(epoch), ok, c.want)
		}
	}
	for _, period := range []time.Duration{0, -time.Second} {
		if _, ok := clock.NewGrid(epoch, period).Next(epoch); ok {
			t.Errorf("a grid of period %v has a tick", period)
		}
	}
}
