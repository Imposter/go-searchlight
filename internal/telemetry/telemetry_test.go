package telemetry

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/config"
)

// syncBuffer is a bytes.Buffer safe for the logger and the test to share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func noEnv(string) string { return "" }

func testConfig() config.Config {
	c := config.Default()
	c.StoreURL = "sqlite:///tmp/test.db"
	c.NodeID = "node-test"
	return c
}

// setup runs Setup with the OTEL_* environment ignored and logs captured.
func setup(t *testing.T, cfg config.Config) (*T, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	prev := slog.Default()
	tel, err := Setup(t.Context(), cfg, WithLogOutput(logs), WithVersion("test"), withEnv(noEnv))
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	t.Cleanup(func() {
		slog.SetDefault(prev)
		if err := tel.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	})
	return tel, logs
}

func logLines(t *testing.T, out string) []map[string]any {
	t.Helper()
	var lines []map[string]any
	sc := bufio.NewScanner(strings.NewReader(out))
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("log line is not JSON: %q: %v", sc.Text(), err)
		}
		lines = append(lines, m)
	}
	return lines
}

func TestLoggerEmitsJSONWithTraceIDsInsideASpan(t *testing.T) {
	tel, logs := setup(t, testConfig())

	ctx, span := tel.Tracer.Start(t.Context(), "op")
	sc := span.SpanContext()
	if !sc.IsValid() {
		t.Fatal("span context is not valid; spans must always be recorded")
	}
	tel.Logger.InfoContext(ctx, "with context")
	WithRequest(ctx).Info("bound", "k", "v")
	WithRequest(ctx).With("index", "products").WarnContext(context.Background(), "bound with attrs")
	span.End()
	tel.Logger.Info("outside")
	WithRequest(context.Background()).Info("no span")

	lines := logLines(t, logs.String())
	if len(lines) != 5 {
		t.Fatalf("got %d log lines, want 5:\n%s", len(lines), logs.String())
	}
	for i, l := range lines {
		if l[KeyNodeID] != "node-test" {
			t.Errorf("line %d: node_id = %v", i, l[KeyNodeID])
		}
		if _, ok := l["time"]; !ok {
			t.Errorf("line %d has no time: %v", i, l)
		}
	}
	for _, l := range lines[:3] {
		if l[KeyTraceID] != sc.TraceID().String() || l[KeySpanID] != sc.SpanID().String() {
			t.Errorf("%q: trace_id=%v span_id=%v, want %s %s", l["msg"], l[KeyTraceID], l[KeySpanID], sc.TraceID(), sc.SpanID())
		}
	}
	if lines[1]["k"] != "v" || lines[2]["index"] != "products" || lines[2]["level"] != "WARN" {
		t.Errorf("attributes lost: %v / %v", lines[1], lines[2])
	}
	for _, l := range lines[3:] {
		if _, ok := l[KeyTraceID]; ok {
			t.Errorf("%q outside a span has a trace_id", l["msg"])
		}
	}
}

func TestLoggerHonoursLevel(t *testing.T) {
	cfg := testConfig()
	cfg.LogLevel = slog.LevelWarn
	tel, logs := setup(t, cfg)
	tel.Logger.Info("hidden")
	tel.Logger.Warn("shown")
	if out := logs.String(); strings.Contains(out, "hidden") || !strings.Contains(out, "shown") {
		t.Errorf("level filter: %s", out)
	}
}

func TestWithRequestOnAForeignDefaultHandler(t *testing.T) {
	var buf bytes.Buffer
	l := slog.New(slog.NewJSONHandler(&buf, nil))
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{1, 2, 3},
		SpanID:     trace.SpanID{4, 5, 6},
		TraceFlags: trace.FlagsSampled,
	})
	bindSpan(l, sc).Info("x")
	if !strings.Contains(buf.String(), sc.TraceID().String()) || !strings.Contains(buf.String(), sc.SpanID().String()) {
		t.Errorf("ids missing: %s", buf.String())
	}
	if got := bindSpan(l, trace.SpanContext{}); got != l {
		t.Error("an invalid span should return the logger unchanged")
	}
}

func TestMetricsServesPrometheusFormat(t *testing.T) {
	tel, _ := setup(t, testConfig())
	in := NewInstruments(tel.Meter)
	route := metric.WithAttributes(attribute.String("route", "/indexes/{i}/_search"), attribute.Int("status", 200))
	in.Histogram(MetricHTTPRequestDuration).Record(t.Context(), 0.004, route)
	in.Counter(MetricStoreErrors).Add(t.Context(), 2, metric.WithAttributes(attribute.String("op", "apply")))
	in.Gauge(MetricShardDiskSize).Record(t.Context(), 4096)
	in.ObservableGauge(MetricReplicaLagSeq, func(_ context.Context, o metric.Float64Observer) error {
		o.Observe(7)
		return nil
	})
	if err := in.Err(); err != nil {
		t.Fatal(err)
	}

	srv := httptest.NewServer(tel.AdminHandler)
	defer srv.Close()
	code, header, body := get(t, srv.URL+"/metrics")
	if code != http.StatusOK {
		t.Fatalf("/metrics: HTTP %d", code)
	}
	if ct := header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("Content-Type = %q, want the Prometheus text format", ct)
	}
	for _, want := range []string{
		"# TYPE searchlight_http_request_duration_seconds histogram",
		`searchlight_http_request_duration_seconds_bucket{otel_scope_name="` + ScopeName,
		`le="0.005"`,
		"# TYPE searchlight_store_errors_total counter",
		"# TYPE searchlight_shard_disk_size_bytes gauge",
		"go_goroutines",
		`target_info{`,
		`service_name="searchlight"`,
		`service_instance_id="node-test"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics is missing %q", want)
		}
	}
	for _, sample := range []string{
		`(?m)^searchlight_store_errors_total\{.*op="apply".*\} 2$`,
		`(?m)^searchlight_replica_lag_seq\{.*\} 7$`,
	} {
		if !regexp.MustCompile(sample).MatchString(body) {
			t.Errorf("/metrics has no sample matching %s:\n%s", sample, body)
		}
	}
}

func TestEveryCatalogueMetricCanBeCreated(t *testing.T) {
	tel, _ := setup(t, testConfig())
	in := NewInstruments(tel.Meter)
	for _, s := range Catalog {
		switch s.Kind {
		case KindCounter:
			in.Counter(s.Name).Add(t.Context(), 1)
		case KindUpDownCounter:
			in.UpDownCounter(s.Name).Add(t.Context(), 1)
		case KindHistogram:
			in.Histogram(s.Name).Record(t.Context(), 1)
		case KindGauge:
			in.Gauge(s.Name).Record(t.Context(), 1)
		default:
			t.Errorf("%s: unknown kind %v", s.Name, s.Kind)
		}
	}
	if err := in.Err(); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(tel.AdminHandler)
	defer srv.Close()
	_, _, body := get(t, srv.URL+"/metrics")
	for _, s := range Catalog {
		prefix := strings.ReplaceAll(s.Name, ".", "_")
		if !strings.Contains(body, "# TYPE "+prefix) {
			t.Errorf("/metrics has no %s", prefix)
		}
	}
}

func TestCatalogue(t *testing.T) {
	name := regexp.MustCompile(`^searchlight(\.[a-z][a-z0-9_]*)+$`)
	seen := map[string]bool{}
	for _, s := range Catalog {
		if !name.MatchString(s.Name) {
			t.Errorf("%q is not a searchlight.* dotted name", s.Name)
		}
		if seen[s.Name] {
			t.Errorf("%q is listed twice", s.Name)
		}
		seen[s.Name] = true
		if s.Description == "" || s.Unit == "" {
			t.Errorf("%q needs a description and a unit", s.Name)
		}
		if s.Kind == KindHistogram && (len(s.Buckets) == 0 || !slices.IsSorted(s.Buckets)) {
			t.Errorf("%q: histogram buckets must be set and ascending", s.Name)
		}
	}
}

func TestInstrumentsRefusesUnknownNamesAndWrongKinds(t *testing.T) {
	in := NewInstruments(nil) // never reached: both calls fail the catalogue check
	in.Counter("searchlight.made.up").Add(t.Context(), 1)
	in.Histogram(MetricStoreErrors).Record(t.Context(), 1)
	err := in.Err()
	if err == nil {
		t.Fatal("want an error")
	}
	for _, want := range []string{"searchlight.made.up", "not in the telemetry catalogue", MetricStoreErrors, "is a counter, not a histogram"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q is missing %q", err, want)
		}
	}
}

func TestAdminHandler(t *testing.T) {
	for _, pprofOn := range []bool{false, true} {
		cfg := testConfig()
		cfg.Pprof = pprofOn
		tel, _ := setup(t, cfg)
		srv := httptest.NewServer(tel.AdminHandler)

		code, _, body := get(t, srv.URL+"/healthz")
		if code != http.StatusOK || !strings.Contains(body, `"ok"`) {
			t.Errorf("/healthz: HTTP %d %s", code, body)
		}
		code, _, _ = get(t, srv.URL+"/debug/pprof/")
		want := http.StatusNotFound
		if pprofOn {
			want = http.StatusOK
		}
		if code != want {
			t.Errorf("pprof=%v: /debug/pprof/ HTTP %d, want %d", pprofOn, code, want)
		}
		srv.Close()
	}
}

func TestShutdownIsIdempotent(t *testing.T) {
	tel, _ := setup(t, testConfig()) // its cleanup shuts down a third time
	if err := tel.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := tel.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSetupRefusesUnsupportedExporters(t *testing.T) {
	env := func(k string) string {
		if k == "OTEL_TRACES_EXPORTER" {
			return "zipkin"
		}
		return ""
	}
	_, err := Setup(t.Context(), testConfig(), WithLogOutput(io.Discard), withEnv(env))
	if err == nil || !strings.Contains(err.Error(), "OTEL_TRACES_EXPORTER") {
		t.Errorf("err = %v, want one naming OTEL_TRACES_EXPORTER", err)
	}
}

func TestOTLPSelection(t *testing.T) {
	cases := []struct {
		name     string
		env      map[string]string
		signal   string
		enabled  bool
		protocol string
		errHas   string
	}{
		{name: "unset", signal: "traces"},
		{name: "endpoint", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://c:4318"}, signal: "traces", enabled: true, protocol: protoHTTP},
		{name: "signal endpoint", env: map[string]string{"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://c:4318/v1/metrics"}, signal: "metrics", enabled: true, protocol: protoHTTP},
		{name: "other signal endpoint", env: map[string]string{"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://c:4318"}, signal: "traces"},
		{name: "grpc", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "c:4317", "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"}, signal: "traces", enabled: true, protocol: protoGRPC},
		{name: "signal protocol wins", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "x", "OTEL_EXPORTER_OTLP_PROTOCOL": "grpc", "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL": "http/protobuf"}, signal: "traces", enabled: true, protocol: protoHTTP},
		{name: "exporter otlp", env: map[string]string{"OTEL_TRACES_EXPORTER": "otlp"}, signal: "traces", enabled: true, protocol: protoHTTP},
		{name: "exporter none", env: map[string]string{"OTEL_TRACES_EXPORTER": "none", "OTEL_EXPORTER_OTLP_ENDPOINT": "x"}, signal: "traces"},
		{name: "metrics prometheus", env: map[string]string{"OTEL_METRICS_EXPORTER": "prometheus"}, signal: "metrics"},
		{name: "metrics prometheus,otlp", env: map[string]string{"OTEL_METRICS_EXPORTER": "prometheus, otlp"}, signal: "metrics", enabled: true, protocol: protoHTTP},
		{name: "sdk disabled", env: map[string]string{"OTEL_SDK_DISABLED": "true", "OTEL_EXPORTER_OTLP_ENDPOINT": "x"}, signal: "traces"},
		{name: "traces prometheus", env: map[string]string{"OTEL_TRACES_EXPORTER": "prometheus"}, signal: "traces", errHas: "OTEL_TRACES_EXPORTER"},
		{name: "bad protocol", env: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "x", "OTEL_EXPORTER_OTLP_PROTOCOL": "http/json"}, signal: "metrics", errHas: "OTEL_EXPORTER_OTLP_PROTOCOL"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			enabled, protocol, err := otlpSelection(func(k string) string { return tc.env[k] }, tc.signal)
			if tc.errHas != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errHas) {
					t.Fatalf("err = %v, want one naming %s", err, tc.errHas)
				}
				return
			}
			if err != nil || enabled != tc.enabled || protocol != tc.protocol {
				t.Errorf("got (%v, %q, %v), want (%v, %q, nil)", enabled, protocol, err, tc.enabled, tc.protocol)
			}
		})
	}
}

// get returns the status, headers and body of a GET.
func get(t *testing.T, url string) (int, http.Header, string) {
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
	return resp.StatusCode, resp.Header, string(b)
}
