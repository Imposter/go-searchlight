package telemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// newTracerProvider builds the tracer provider. Spans are always recorded, so
// logs carry trace ids, and are exported only when OTLP traces are selected
// (see otlpSelection). The sampler follows OTEL_TRACES_SAMPLER, defaulting to
// parent-based always-on.
func newTracerProvider(ctx context.Context, res *resource.Resource, getenv func(string) string) (*sdktrace.TracerProvider, error) {
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	enabled, protocol, err := otlpSelection(getenv, "traces")
	if err != nil {
		return nil, err
	}
	if enabled {
		var exp sdktrace.SpanExporter
		if protocol == protoGRPC {
			exp, err = otlptracegrpc.New(ctx)
		} else {
			exp, err = otlptracehttp.New(ctx)
		}
		if err != nil {
			return nil, fmt.Errorf("otlp trace exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	return sdktrace.NewTracerProvider(opts...), nil
}
