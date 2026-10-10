package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// lockedBuffer is a goroutine-safe log sink.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestRequestTelemetry checks each request's span, RED metrics by route and access
// log with its request id and trace ids.
func TestRequestTelemetry(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	cfg := testConfig(t)
	var logs lockedBuffer
	tel, err := telemetry.Setup(context.Background(), cfg, telemetry.WithLogOutput(&logs))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tel.Shutdown(context.Background()) })
	st := openStore(t, cfg)
	n, err := node.NewSingle(context.Background(), node.Options{Store: st, Config: cfg, Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close(context.Background()) })
	srv, err := api.NewServer(n, tel, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "API auth is OFF") {
		t.Errorf("no warning that auth is off: %s", logs.String())
	}
	hs := httptest.NewServer(srv)
	t.Cleanup(hs.Close)
	e := &env{t: t, url: hs.URL, client: hs.Client()}
	e.createIndex("tel", "")
	got := e.do("POST", "/indexes/tel/_search", `{}`, "X-Request-Id", "req-42", "traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if got.status != http.StatusOK {
		t.Fatalf("search: %d %s", got.status, got.body)
	}
	e.problem(e.do("POST", "/indexes/missing/_search", `{}`), http.StatusNotFound, "index_not_found")

	metrics := e.do("GET", "/metrics", "")
	for _, want := range []string{
		`searchlight_http_request_duration_seconds_count{`,
		`http_route="/indexes/{index}/_search"`,
		`http_response_status_code="404"`,
		`searchlight_http_request_errors_total{`,
		`searchlight_http_requests_inflight`,
	} {
		if !strings.Contains(string(metrics.body), want) {
			t.Errorf("/metrics is missing %s", want)
		}
	}

	var found bool
	for line := range strings.SplitSeq(logs.String(), "\n") {
		if !strings.Contains(line, `"request_id":"req-42"`) {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatal(err)
		}
		found = true
		if rec["msg"] != "request served" || rec["route"] != "/indexes/{index}/_search" || rec["status"] != 200.0 ||
			rec["index"] != "tel" || rec["duration_ms"] == nil || rec["node_id"] != cfg.NodeID {
			t.Errorf("access log = %v", rec)
		}
		// The span continues the caller's trace.
		if rec["trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" || rec["span_id"] == nil {
			t.Errorf("access log trace = %v %v", rec["trace_id"], rec["span_id"])
		}
	}
	if !found {
		t.Errorf("no access log for req-42 in %s", logs.String())
	}
}
