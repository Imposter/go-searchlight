// Package telemetry wires Searchlight's logs, traces and metrics (spec §11):
// slog JSON logs carrying trace ids, OpenTelemetry tracing and metrics, the
// Prometheus /metrics endpoint and pprof on the admin listener.
//
// OTLP export of traces and metrics is configured entirely from the standard
// OTEL_* environment variables and is off unless they ask for it (see
// otlpSelection). The Prometheus endpoint is always on.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/config"
)

// ServiceName is the OTel service name (OTEL_SERVICE_NAME overrides it).
const ServiceName = "searchlight"

// ScopeName is the instrumentation scope of Searchlight's tracer and meter.
const ScopeName = "github.com/Imposter/go-searchlight"

// T is the process's telemetry.
type T struct {
	// Logger writes JSON logs with node_id and, inside a span, trace_id and
	// span_id. Setup also installs it as slog's default.
	Logger *slog.Logger
	// Tracer and Meter are Searchlight's instrumentation scope.
	Tracer trace.Tracer
	Meter  metric.Meter
	// TracerProvider and MeterProvider are for instrumentation libraries
	// such as otelhttp. Setup also installs them as the OTel globals.
	TracerProvider *sdktrace.TracerProvider
	MeterProvider  *sdkmetric.MeterProvider
	// AdminHandler serves GET /healthz, GET /metrics (Prometheus) and, when
	// the pprof setting is on, /debug/pprof/*.
	AdminHandler http.Handler

	shutdown []func(context.Context) error
}

// Option adjusts Setup.
type Option func(*options)

type options struct {
	logOutput io.Writer
	version   string
	getenv    func(string) string
}

// WithLogOutput sends logs to w instead of standard error.
func WithLogOutput(w io.Writer) Option { return func(o *options) { o.logOutput = w } }

// WithVersion sets the service.version resource attribute.
func WithVersion(v string) Option { return func(o *options) { o.version = v } }

// withEnv replaces os.Getenv for the OTEL_* selection (tests only; the
// exporters themselves always read the process environment).
func withEnv(getenv func(string) string) Option { return func(o *options) { o.getenv = getenv } }

// Setup builds the logger, the tracer and meter providers with their
// exporters, and the admin handler, and installs them as the slog and OTel
// globals along with the W3C trace-context and baggage propagators. Call
// Shutdown to flush the exporters.
func Setup(ctx context.Context, cfg config.Config, opts ...Option) (*T, error) {
	o := options{logOutput: os.Stderr, version: "dev", getenv: os.Getenv}
	for _, opt := range opts {
		opt(&o)
	}

	logger := NewLogger(o.logOutput, cfg.LogLevel).With("node_id", cfg.NodeID)

	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceName(ServiceName),
			semconv.ServiceVersion(o.version),
			semconv.ServiceInstanceID(cfg.NodeID),
		),
		resource.WithFromEnv(), // OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES win
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry resource: %w", err)
	}

	t := &T{Logger: logger}
	tp, err := newTracerProvider(ctx, res, o.getenv)
	if err != nil {
		return nil, err
	}
	t.TracerProvider = tp
	t.shutdown = append(t.shutdown, tp.Shutdown)

	mp, prom, err := newMeterProvider(ctx, res, o.getenv)
	if err != nil {
		return nil, errors.Join(err, t.Shutdown(ctx))
	}
	t.MeterProvider = mp
	t.shutdown = append(t.shutdown, mp.Shutdown)

	t.Tracer = tp.Tracer(ScopeName)
	t.Meter = mp.Meter(ScopeName)
	t.AdminHandler = newAdminHandler(prom, cfg.Pprof)

	slog.SetDefault(logger)
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	otel.SetErrorHandler(errorHandler{logger})
	return t, nil
}

// errorHandler logs the OTel SDK's internal errors, such as failed exports.
type errorHandler struct{ logger *slog.Logger }

func (h errorHandler) Handle(err error) {
	h.logger.Warn("opentelemetry error", "error", err)
}

// Shutdown flushes and stops the exporters, traces first so their final
// spans are counted. It is safe to call more than once.
func (t *T) Shutdown(ctx context.Context) error {
	fns := t.shutdown
	t.shutdown = nil
	var errs []error
	for _, fn := range fns {
		errs = append(errs, fn(ctx))
	}
	return errors.Join(errs...)
}

// Supported OTLP protocols.
const (
	protoGRPC = "grpc"
	protoHTTP = "http/protobuf"
)

// otlpSelection reports whether a signal ("traces" or "metrics") is exported
// over OTLP and with which protocol, following the OTel environment
// conventions:
//
//   - OTEL_SDK_DISABLED=true turns every exporter off.
//   - OTEL_<SIGNAL>_EXPORTER, a comma list, selects exporters: "otlp" turns
//     OTLP on and "none" off; for metrics "prometheus" is accepted (the
//     Prometheus endpoint is always served). Other exporters are refused.
//   - Without it, OTLP is on when OTEL_EXPORTER_OTLP_ENDPOINT or
//     OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT is set.
//   - OTEL_EXPORTER_OTLP_<SIGNAL>_PROTOCOL, else OTEL_EXPORTER_OTLP_PROTOCOL,
//     picks "http/protobuf" (the default) or "grpc".
//
// Endpoints, headers, timeouts and TLS are read by the exporters themselves
// from the standard OTEL_EXPORTER_OTLP_* variables.
func otlpSelection(getenv func(string) string, signal string) (enabled bool, protocol string, err error) {
	env := func(k string) string { return strings.TrimSpace(getenv(k)) }
	upper := strings.ToUpper(signal)
	if strings.EqualFold(env("OTEL_SDK_DISABLED"), "true") {
		return false, "", nil
	}
	selector := "OTEL_" + upper + "_EXPORTER"
	if sel := env(selector); sel != "" {
		for name := range strings.SplitSeq(sel, ",") {
			switch name = strings.TrimSpace(name); {
			case name == "otlp":
				enabled = true
			case name == "none", name == "prometheus" && signal == "metrics":
			default:
				return false, "", fmt.Errorf("%s: unsupported exporter %q (want otlp or none)", selector, name)
			}
		}
	} else {
		enabled = env("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || env("OTEL_EXPORTER_OTLP_"+upper+"_ENDPOINT") != ""
	}
	if !enabled {
		return false, "", nil
	}
	protoVar := "OTEL_EXPORTER_OTLP_" + upper + "_PROTOCOL"
	protocol = env(protoVar)
	if protocol == "" {
		protoVar = "OTEL_EXPORTER_OTLP_PROTOCOL"
		protocol = env(protoVar)
	}
	switch protocol {
	case "":
		return true, protoHTTP, nil
	case protoHTTP, protoGRPC:
		return true, protocol, nil
	default:
		return false, "", fmt.Errorf("%s: unsupported protocol %q (want %s or %s)", protoVar, protocol, protoHTTP, protoGRPC)
	}
}
