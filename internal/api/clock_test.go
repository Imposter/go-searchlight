package api_test

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/clock"
)

type closingStub struct {
	stub
	closed chan struct{}
}

func (c *closingStub) Close(context.Context) error {
	close(c.closed)
	return nil
}

// The shutdown grace is kept by the server's clock: while it lasts the API answers,
// with readiness false, and the listener stops and the coordinator closes only once
// the clock has run the whole grace.
func TestShutdownGraceByTheClock(t *testing.T) {
	cfg := testConfig(t)
	cfg.ShutdownGrace = time.Hour
	clk := clock.NewFake(time.Now())
	c := &closingStub{closed: make(chan struct{})}
	srv, err := api.NewServer(c, nil, cfg, api.WithClock(clk))
	if err != nil {
		t.Fatal(err)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, ln) }()
	wait, stop := context.WithTimeout(context.Background(), 30*time.Second)
	defer stop()
	status := func(path string) int {
		t.Helper()
		req, err := http.NewRequestWithContext(wait, http.MethodGet, "http://"+ln.Addr().String()+path, http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s during the grace: %v", path, err)
		}
		_ = res.Body.Close()
		return res.StatusCode
	}
	cancel()
	if err := clk.BlockUntilArmed(wait, time.Hour); err != nil {
		t.Fatalf("the server is not keeping its grace on the clock: %v", err)
	}
	clk.Advance(time.Hour - time.Millisecond)
	if got := status("/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("readyz during the grace: %d", got)
	}
	if got := status("/healthz"); got != http.StatusOK {
		t.Fatalf("healthz during the grace: %d", got)
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned before the grace ran out: %v", err)
	case <-c.closed:
		t.Fatal("the coordinator closed before the grace ran out")
	default:
	}
	clk.Advance(time.Millisecond)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-wait.Done():
		t.Fatal("Run did not return once the grace ran out")
	}
	select {
	case <-c.closed:
	default:
		t.Fatal("the coordinator was not closed")
	}
}
