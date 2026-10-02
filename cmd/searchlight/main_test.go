package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

func testConfig(t *testing.T) config.Config {
	t.Helper()
	cfg, err := config.Load([]string{"--admin_listen=127.0.0.1:0", "--listen=127.0.0.1:0", "--shutdown_timeout=5s"},
		func(k string) string {
			if k == "SEARCHLIGHT_STORE_URL" {
				return "sqlite:///tmp/searchlight-test.db"
			}
			return ""
		})
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestServeAdminEndpointsAndGracefulShutdown(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	cfg := testConfig(t)
	tel, err := telemetry.Setup(t.Context(), cfg, telemetry.WithLogOutput(io.Discard))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfg, tel, ln) }()

	base := "http://" + ln.Addr().String()
	for path, want := range map[string]string{"/healthz": `"ok"`, "/metrics": "# TYPE go_goroutines gauge"} {
		code, body := get(t, base+path)
		if code != http.StatusOK || !strings.Contains(body, want) {
			t.Errorf("%s: HTTP %d, body missing %q", path, code, want)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("serve did not stop after cancellation")
	}
	conn, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err == nil {
		_ = conn.Close()
		t.Error("admin listener still accepts connections after shutdown")
	}
}

func TestRunRefusesInvalidConfig(t *testing.T) {
	err := run(t.Context(), nil, func(string) string { return "" })
	if err == nil || !strings.Contains(err.Error(), "store_url") {
		t.Errorf("err = %v, want one naming store_url", err)
	}
}

func TestRunStopsWhenCancelled(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	env := func(k string) string {
		switch k {
		case "SEARCHLIGHT_STORE_URL":
			return "sqlite:///tmp/searchlight-test.db"
		case "SEARCHLIGHT_ADMIN_LISTEN":
			return "127.0.0.1:0"
		case "SEARCHLIGHT_LISTEN":
			return "127.0.0.1:0"
		case "SEARCHLIGHT_LOG_LEVEL":
			return "error"
		}
		return ""
	}
	if err := run(ctx, nil, env); err != nil {
		t.Fatalf("run: %v", err)
	}
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, url, http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}
