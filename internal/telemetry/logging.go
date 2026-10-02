package telemetry

import (
	"context"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// Log attribute keys shared across packages (spec §11).
const (
	KeyTraceID  = "trace_id"
	KeySpanID   = "span_id"
	KeyNodeID   = "node_id"
	KeyIndex    = "index"
	KeyShard    = "shard"
	KeyRoute    = "route"
	KeyStatus   = "status"
	KeyDuration = "duration_ms"
)

// NewLogger returns a JSON logger writing to w at level. A record logged with
// a context that carries a valid span (the *Context methods, or a logger from
// WithRequest) gets trace_id and span_id attributes.
func NewLogger(w io.Writer, level slog.Leveler) *slog.Logger {
	return slog.New(&traceHandler{inner: slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})})
}

// WithRequest returns slog's default logger (the one Setup installs) bound to
// ctx's span, so every record it writes carries trace_id and span_id even
// when logged without a context. Without a valid span it returns the default
// logger unchanged.
func WithRequest(ctx context.Context) *slog.Logger {
	return bindSpan(slog.Default(), trace.SpanContextFromContext(ctx))
}

func bindSpan(l *slog.Logger, sc trace.SpanContext) *slog.Logger {
	if !sc.IsValid() {
		return l
	}
	if h, ok := l.Handler().(*traceHandler); ok {
		return slog.New(&traceHandler{inner: h.inner, span: sc})
	}
	return l.With(KeyTraceID, sc.TraceID().String(), KeySpanID, sc.SpanID().String())
}

// traceHandler adds the trace and span ids to each record. The span is the
// one bound by WithRequest, else the one in the record's context. The ids are
// added to the record, so they nest under any group opened with WithGroup.
type traceHandler struct {
	inner slog.Handler
	span  trace.SpanContext // bound by WithRequest; zero means "from ctx"
}

func (h *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	sc := h.span
	if !sc.IsValid() {
		sc = trace.SpanContextFromContext(ctx)
	}
	if sc.IsValid() {
		r = r.Clone()
		r.AddAttrs(slog.String(KeyTraceID, sc.TraceID().String()), slog.String(KeySpanID, sc.SpanID().String()))
	}
	return h.inner.Handle(ctx, r)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{inner: h.inner.WithAttrs(attrs), span: h.span}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{inner: h.inner.WithGroup(name), span: h.span}
}
