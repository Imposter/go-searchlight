package api_test

import (
	"context"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
)

type slowCoordinator struct {
	api.Coordinator
	mu                           sync.Mutex
	drainDeadline, closeDeadline time.Time
	closeStarted                 time.Time
}

func (c *slowCoordinator) Ready(context.Context) error { return nil }

func (c *slowCoordinator) Drain(ctx context.Context) {
	d, _ := ctx.Deadline()
	c.mu.Lock()
	c.drainDeadline = d
	c.mu.Unlock()
	<-ctx.Done()
}

func (c *slowCoordinator) Close(ctx context.Context) error {
	d, _ := ctx.Deadline()
	c.mu.Lock()
	c.closeDeadline, c.closeStarted = d, time.Now()
	c.mu.Unlock()
	<-ctx.Done()
	return nil
}

func (c *slowCoordinator) Handler(pub http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Mounted", "yes")
		pub.ServeHTTP(w, r)
	})
}

// TestRunShutdownBudget checks Run mounts a Mounter coordinator's handler and splits
// the one budget its context's Shutdown cause fixes: the drain within a quarter of
// shutdown_timeout, the coordinator's close by the deadline less a tenth (at least 0.4
// of it even after a drain that used its whole quarter), Run back by then.
func TestRunShutdownBudget(t *testing.T) {
	cfg := testConfig(t)
	cfg.ShutdownTimeout = 2 * time.Second
	cfg.ShutdownGrace = 200 * time.Millisecond
	c := &slowCoordinator{}
	srv, err := api.NewServer(c, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx, ln) }()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+ln.Addr().String()+"/healthz", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.Header.Get("X-Mounted") != "yes" {
		t.Error("Run did not serve the API inside the coordinator's handler")
	}

	start := time.Now()
	sd := &api.Shutdown{Deadline: start.Add(cfg.ShutdownGrace + cfg.ShutdownTimeout), Grace: cfg.ShutdownGrace}
	cancel(sd)
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	returned := time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()
	const slack = 100 * time.Millisecond
	if limit := start.Add(cfg.ShutdownTimeout/4 + slack); c.drainDeadline.IsZero() || c.drainDeadline.After(limit) {
		t.Errorf("drain deadline %v after start, want within %v", c.drainDeadline.Sub(start), cfg.ShutdownTimeout/4)
	}
	if want := sd.Deadline.Add(-cfg.ShutdownTimeout / 10); !c.closeDeadline.Equal(want) {
		t.Errorf("close deadline %v after start, want %v", c.closeDeadline.Sub(start), want.Sub(start))
	}
	if got := c.closeDeadline.Sub(c.closeStarted); got < cfg.ShutdownTimeout*4/10-slack {
		t.Errorf("the coordinator's close got %v, want at least 0.4 of %v", got, cfg.ShutdownTimeout)
	}
	if returned.After(sd.Deadline.Add(-cfg.ShutdownTimeout/10 + slack)) {
		t.Errorf("Run returned %v after start, past the close deadline", returned.Sub(start))
	}
}
