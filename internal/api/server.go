package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// Server is the public API's http.Handler. Build it with [NewServer] and serve it with
// [Server.Run], or mount it in an http.Server of your own ([Server.HTTPServer] sets
// the timeouts a public listener needs).
type Server struct {
	c       Coordinator
	cfg     config.Config
	clock   clock.Clock
	log     *slog.Logger
	tracer  trace.Tracer
	prop    propagation.TextMapPropagator
	metrics http.Handler
	auth    *authenticator
	queue   queue
	// writeBytes and readBytes are the in-flight request-byte budgets.
	writeBytes, readBytes *budget
	mux                   *http.ServeMux
	paths                 *http.ServeMux // every route's path, any method: tells a 405 from a 404
	routes                []*route
	dur                   metric.Float64Histogram
	errs                  metric.Int64Counter
	inflight              metric.Int64UpDownCounter
	draining              atomic.Bool
	// tooLarge counts 413 answers (tests confirm a refusal with it).
	tooLarge atomic.Int64
}

// route is one endpoint.
type route struct {
	method, path string
	scope        scope
	// params are the query parameters it takes.
	params []string
	// queued routes take a search_queue slot.
	queued bool
	// quiet routes (probes) log at debug.
	quiet bool
	// docBody routes take one document (or saved query): their body is bounded by
	// max_doc_bytes rather than max_body_bytes.
	docBody bool
	handler func(w http.ResponseWriter, r *http.Request, p params) error
	// attrs label its metrics.
	attrs attribute.Set
}

// Route is one endpoint as api/openapi.yaml lists it: its method and path pattern,
// the query parameters it takes and whether it needs a token.
type Route struct {
	Method, Path string
	Params       []string
	Auth         bool
}

// ServerOption adjusts NewServer.
type ServerOption func(*Server)

// WithClock sets the clock the server times requests and its shutdown grace by
// (default: clock.Real).
func WithClock(c clock.Clock) ServerOption { return func(s *Server) { s.clock = c } }

// NewServer builds the API over c. t supplies the logger, tracer, meter and the
// /metrics handler; nil uses slog's default logger and the OpenTelemetry globals. It
// reads cfg.TokensFile, failing when the file is unreadable or holds no valid token;
// with no tokens file it requires cfg.InsecureNoAuth and logs a warning, since every
// request is then served unauthenticated.
func NewServer(c Coordinator, t *telemetry.T, cfg config.Config, opts ...ServerOption) (*Server, error) {
	s := &Server{
		c:     c,
		cfg:   cfg,
		clock: clock.Real{},
		log:   slog.Default(),
		prop:  otel.GetTextMapPropagator(),
		queue: make(queue, max(1, cfg.SearchQueue)),

		writeBytes: newBudget(cfg.MaxInflightWriteBytes, cfg.MaxBodyBytes, cfg.InflightAmplification),
		readBytes:  newBudget(cfg.MaxInflightReadBytes, cfg.MaxBodyBytes, cfg.InflightAmplification),
		mux:        http.NewServeMux(),
		paths:      http.NewServeMux(),
		tracer:     otel.Tracer(telemetry.ScopeName),
	}
	for _, o := range opts {
		o(s)
	}
	meter := otel.Meter(telemetry.ScopeName)
	if t != nil {
		s.log, s.tracer, meter, s.metrics = t.Logger, t.Tracer, t.Meter, t.AdminHandler
	}
	switch {
	case cfg.TokensFile != "":
		a, err := loadTokens(cfg.TokensFile)
		if err != nil {
			return nil, err
		}
		s.auth = a
	case cfg.InsecureNoAuth:
		s.log.Warn("API auth is OFF: insecure_no_auth is set and tokens_file is empty, so anyone who reaches the listener can read and write every index")
	default:
		return nil, errors.New("tokens_file: is required; set insecure_no_auth=true to serve the API without auth")
	}
	if cfg.MaxBodyBytes <= 0 || cfg.MaxDocBytes <= 0 || cfg.MaxBulkOps <= 0 || cfg.RequestTimeout <= 0 {
		return nil, errors.New("api: max_body_bytes, max_doc_bytes, max_bulk_ops and request_timeout must be positive")
	}
	in := telemetry.NewInstruments(meter)
	s.dur = in.Histogram(telemetry.MetricHTTPRequestDuration)
	s.errs = in.Counter(telemetry.MetricHTTPRequestErrors)
	s.inflight = in.UpDownCounter(telemetry.MetricHTTPInflight)
	if err := in.Err(); err != nil {
		return nil, err
	}
	s.routes = s.table()
	registered := map[string]bool{}
	for _, rt := range s.routes {
		rt.attrs = attribute.NewSet(attribute.String("http.route", rt.path), attribute.String("http.request.method", rt.method))
		s.mux.Handle(rt.method+" "+rt.path, s.wrap(rt))
		if !registered[rt.path] {
			registered[rt.path] = true
			s.paths.Handle(rt.path, http.NotFoundHandler())
		}
	}
	fallback := &route{method: "", path: "/", attrs: attribute.NewSet(attribute.String("http.route", "unmatched")), handler: s.unmatched}
	s.mux.Handle("/", s.wrap(fallback))
	return s, nil
}

// Routes lists every endpoint, for api/openapi.yaml's test.
func (s *Server) Routes() []Route {
	out := make([]Route, len(s.routes))
	for i, rt := range s.routes {
		out[i] = Route{Method: rt.method, Path: rt.path, Params: slices.Clone(rt.params), Auth: rt.scope != scopeNone}
	}
	return out
}

// table is the API's endpoints (spec section 5).
func (s *Server) table() []*route {
	read, write := scopeRead, scopeWrite
	wait := []string{"wait_for_seq"}
	writeParams := []string{"if_seq", "op_type", "refresh"}
	return []*route{
		{method: http.MethodGet, path: "/healthz", scope: scopeNone, quiet: true, handler: s.healthz},
		{method: http.MethodGet, path: "/readyz", scope: scopeNone, quiet: true, handler: s.readyz},
		{method: http.MethodGet, path: "/metrics", scope: read, quiet: true, handler: s.metricsHandler},
		{method: http.MethodGet, path: "/_cluster/health", scope: read, handler: s.clusterHealth},
		{method: http.MethodGet, path: "/_cluster/nodes", scope: read, handler: s.clusterNodes},
		{method: http.MethodGet, path: "/_cluster/shards", scope: read, handler: s.clusterShards},

		{method: http.MethodGet, path: "/indexes", scope: read, handler: s.listIndexes},
		{method: http.MethodPut, path: "/indexes/{index}", scope: write, handler: s.createIndex},
		{method: http.MethodGet, path: "/indexes/{index}", scope: read, handler: s.getIndex},
		{method: http.MethodDelete, path: "/indexes/{index}", scope: write, handler: s.deleteIndex},
		{method: http.MethodPatch, path: "/indexes/{index}/mapping", scope: write, handler: s.patchMapping},
		{method: http.MethodPatch, path: "/indexes/{index}/settings", scope: write, handler: s.patchSettings},

		{method: http.MethodPut, path: "/indexes/{index}/docs/{id}", scope: write, params: writeParams, docBody: true, handler: s.putDoc},
		{method: http.MethodGet, path: "/indexes/{index}/docs/{id}", scope: read, params: wait, handler: s.getDoc},
		{method: http.MethodDelete, path: "/indexes/{index}/docs/{id}", scope: write, params: writeParams, handler: s.deleteDoc},
		{method: http.MethodPost, path: "/indexes/{index}/_bulk", scope: write, params: []string{"refresh", "percolate", "wait_for_seq"}, handler: s.bulk},

		{method: http.MethodPost, path: "/indexes/{index}/_search", scope: read, params: wait, queued: true, handler: s.search},
		{method: http.MethodPost, path: "/indexes/{index}/_count", scope: read, params: wait, queued: true, handler: s.count},

		{method: http.MethodPut, path: "/indexes/{index}/queries/{id}", scope: write, params: writeParams, docBody: true, handler: s.putQuery},
		{method: http.MethodGet, path: "/indexes/{index}/queries/{id}", scope: read, params: wait, handler: s.getQuery},
		{method: http.MethodDelete, path: "/indexes/{index}/queries/{id}", scope: write, params: writeParams, handler: s.deleteQuery},
		{method: http.MethodGet, path: "/indexes/{index}/queries", scope: read, params: []string{"after", "size", "wait_for_seq"}, handler: s.listQueries},

		{method: http.MethodPost, path: "/indexes/{index}/_percolate", scope: read, params: wait, queued: true, handler: s.percolate},
		{method: http.MethodGet, path: "/indexes/{index}/_fields", scope: read, params: []string{"entries", "wait_for_seq"}, queued: true, handler: s.fields},
	}
}

// ServeHTTP serves the API.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// HTTPServer returns an http.Server for s with the public listener's limits: header
// and body reads bounded by read_timeout (a client that sends too slowly is cut off),
// writes by the request deadline, idle connections closed after two minutes.
func (s *Server) HTTPServer() *http.Server {
	return &http.Server{
		Handler:           s,
		ReadHeaderTimeout: min(10*time.Second, s.cfg.ReadTimeout),
		ReadTimeout:       s.cfg.ReadTimeout,
		WriteTimeout:      s.cfg.ReadTimeout + s.cfg.RequestTimeout + 10*time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    64 << 10,
		ErrorLog:          slog.NewLogLogger(s.log.Handler(), slog.LevelWarn),
	}
}

// Run serves s on ln until ctx ends, over TLS when tls_cert and tls_key are set
// (otherwise put a TLS-terminating proxy in front). A coordinator that is a [Mounter]
// serves s inside its own handler. It owns ln.
//
// When ctx ends it shuts down gracefully within one budget, a [Shutdown]: ctx's cause
// when it is one (context.WithCancelCause), else [NewShutdown] from that moment. With
// T the shutdown_timeout:
//
//   - readiness turns false and a [Drainer] coordinator drains, within T/4;
//   - the API keeps serving for the grace, so load balancers stop sending it traffic;
//   - the listener stops accepting and requests in flight finish, within T/4 (those
//     still running then are cut off);
//   - the coordinator is closed, flushing every shard copy, by the deadline less T/10,
//     which leaves it at least 0.4 T; the last T/10 is the caller's, to close what it
//     opened.
func (s *Server) Run(ctx context.Context, ln net.Listener) error {
	srv := s.HTTPServer()
	if m, ok := s.c.(Mounter); ok {
		srv.Handler = m.Handler(s)
	}
	srv.BaseContext = func(net.Listener) context.Context { return context.WithoutCancel(ctx) }
	errc := make(chan error, 1)
	go func() {
		if s.cfg.TLSCert != "" {
			errc <- srv.ServeTLS(ln, s.cfg.TLSCert, s.cfg.TLSKey)
			return
		}
		errc <- srv.Serve(ln)
	}()
	s.log.InfoContext(ctx, "API listener serving", slog.String("address", ln.Addr().String()))
	base := context.WithoutCancel(ctx)
	select {
	case err := <-errc:
		cctx, cancel := context.WithDeadline(base, s.closeBy(NewShutdown(s.cfg, s.clock)))
		defer cancel()
		return errors.Join(fmt.Errorf("api listener: %w", err), s.c.Close(cctx))
	case <-ctx.Done():
	}
	sd := NewShutdown(s.cfg, s.clock)
	var given *Shutdown
	if errors.As(context.Cause(ctx), &given) {
		sd = given
	}
	quarter := s.cfg.ShutdownTimeout / 4

	s.draining.Store(true)
	s.log.InfoContext(ctx, "API draining: not ready", slog.Duration("grace", sd.Grace), slog.Time("deadline", sd.Deadline))
	if d, ok := s.c.(Drainer); ok {
		// A cluster node retires the copies others can stand in for, so peers stop
		// routing reads here while the listener drains.
		dctx, cancel := context.WithDeadline(base, sd.within(s.clock.Now(), quarter))
		d.Drain(dctx)
		cancel()
	}
	if sd.Grace > 0 {
		t := s.clock.NewTimer(min(sd.Grace, s.clock.Until(sd.Deadline)))
		select {
		case <-t.C():
		case err := <-errc:
			t.Stop()
			errc <- err
		}
	}
	sctx, cancel := context.WithDeadline(base, sd.within(s.clock.Now(), quarter))
	defer cancel()
	var errs []error
	if err := srv.Shutdown(sctx); err != nil {
		errs = append(errs, fmt.Errorf("api shutdown: %w", err))
		_ = srv.Close()
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		errs = append(errs, fmt.Errorf("api listener: %w", err))
	}
	cctx, ccancel := context.WithDeadline(base, s.closeBy(sd))
	defer ccancel()
	if err := s.c.Close(cctx); err != nil {
		errs = append(errs, fmt.Errorf("coordinator close: %w", err))
	}
	s.log.InfoContext(cctx, "API stopped")
	return errors.Join(errs...)
}

func (s *Server) closeBy(sd *Shutdown) time.Time {
	return sd.Deadline.Add(-s.cfg.ShutdownTimeout / 10)
}

// Shutdown is a graceful shutdown's budget, fixed when it begins. Cancel Run's context
// with one as its cause to set it; the caller then closes what it opened by Deadline.
type Shutdown struct { //nolint:errname // a cancellation cause carrying the budget, not a failure
	// Deadline is when the whole shutdown, the caller's closing included, must be done.
	Deadline time.Time
	// Grace is how long the API keeps serving after readiness turns false.
	Grace time.Duration
}

// NewShutdown returns the budget of a shutdown beginning now by c: shutdown_grace, then
// shutdown_timeout.
func NewShutdown(cfg config.Config, c clock.Clock) *Shutdown {
	return &Shutdown{Deadline: c.Now().Add(cfg.ShutdownGrace + cfg.ShutdownTimeout), Grace: cfg.ShutdownGrace}
}

func (*Shutdown) Error() string { return "graceful shutdown" }

func (sd *Shutdown) within(now time.Time, d time.Duration) time.Time {
	if end := now.Add(d); end.Before(sd.Deadline) {
		return end
	}
	return sd.Deadline
}

// reqInfo is what the middleware hands a handler through its context.
type reqInfo struct {
	id  string
	log *slog.Logger
}

type ctxKey struct{}

func info(ctx context.Context) *reqInfo {
	if ri, ok := ctx.Value(ctxKey{}).(*reqInfo); ok {
		return ri
	}
	return &reqInfo{log: slog.Default()}
}

// statusWriter records the status a handler wrote.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the connection.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// maxRequestIDLen bounds a client's X-Request-Id.
const maxRequestIDLen = 128

// requestID is the client's X-Request-Id when it is a short token of safe
// characters, else a fresh random id.
func requestID(r *http.Request) string {
	if id := r.Header.Get("X-Request-Id"); id != "" && len(id) <= maxRequestIDLen && strings.IndexFunc(id, func(c rune) bool {
		return (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' && c != '.'
	}) < 0 {
		return id
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// wrap runs rt's handler under the API's middleware: a request id, a server span
// continuing the caller's trace, the request metrics and an access log, panic
// recovery, auth, parameter checks, the request deadline, the read queue and the body
// limit. A handler's error is answered as problem JSON.
func (s *Server) wrap(rt *route) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := s.clock.Now()
		id := requestID(r)
		ctx := s.prop.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		name := rt.method + " " + rt.path
		if rt.method == "" {
			name = r.Method + " unmatched"
		}
		ctx, span := s.tracer.Start(ctx, name, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(
			attribute.String("http.request.method", r.Method),
			attribute.String("http.route", rt.path),
			attribute.String("url.path", r.URL.Path),
			attribute.String("request_id", id),
		))
		defer span.End()
		log := telemetry.WithRequest(ctx).With(slog.String("request_id", id))
		ctx = context.WithValue(ctx, ctxKey{}, &reqInfo{id: id, log: log})
		sw := &statusWriter{ResponseWriter: w}
		sw.Header().Set("X-Request-Id", id)
		routeSet := metric.WithAttributeSet(rt.attrs)
		s.inflight.Add(ctx, 1, routeSet)

		var failure *Error
		defer func() {
			if p := recover(); p != nil {
				if p == http.ErrAbortHandler { //nolint:errorlint // the sentinel is compared, never wrapped
					panic(p)
				}
				failure = &Error{Status: http.StatusInternalServerError, Code: CodeInternal, Detail: "internal error", Err: fmt.Errorf("panic: %v", p)}
				log.ErrorContext(ctx, "API handler panicked", slog.Any("panic", p), slog.String("stack", string(debug.Stack())))
				if sw.status == 0 {
					writeProblem(sw, failure, id)
				}
			}
			s.inflight.Add(ctx, -1, routeSet)
			s.finish(ctx, rt, r, sw, span, failure, start)
		}()

		// body sees what the handler reads of the request body, so a refusal knows
		// how much is left.
		body := &trackedBody{r: r.Body}
		r.Body = body
		// releaseBudget gives back the request's in-flight budget; a refusal calls
		// it before draining, so a drain holds none.
		releaseBudget := func() {}
		fail := func(e *Error) {
			failure = e
			if e.Status == http.StatusRequestEntityTooLarge {
				sw.Header().Set("Connection", "close") // no more requests on this connection
				s.tooLarge.Add(1)
			}
			writeProblem(sw, e, id)
			releaseBudget()
			s.drain(sw, r, body, e.Status)
		}
		if e := s.auth.check(r, rt.scope); e != nil {
			fail(e)
			return
		}
		p, e := parseParams(r, rt.params)
		if e != nil {
			fail(e)
			return
		}
		limit, setting := s.cfg.MaxBodyBytes, "max_body_bytes"
		if rt.docBody {
			limit, setting = s.cfg.MaxDocBytes, "max_doc_bytes"
		}
		if r.ContentLength > limit {
			fail(TooLarge("the request body is %d bytes, over %d (%s)", r.ContentLength, limit, setting))
			return
		}
		r.Body = http.MaxBytesReader(sw, r.Body, limit)
		// The in-flight byte budget: a body of known length is weighed up front, one
		// of unknown length as it is read, each byte at the heap it costs.
		b := s.readBytes
		if rt.scope == scopeWrite {
			b = s.writeBytes
		}
		if r.ContentLength > 0 {
			cost := r.ContentLength * b.factor
			if !b.reserve(cost) {
				fail(overBudget(b))
				return
			}
			releaseBudget = sync.OnceFunc(func() { b.release(cost) })
			defer releaseBudget()
		} else if r.ContentLength < 0 {
			br := &budgetReader{r: r.Body, b: b}
			r.Body = br
			releaseBudget = sync.OnceFunc(br.releaseAll)
			defer releaseBudget()
		}
		ctx, cancel := clock.WithTimeout(ctx, s.clock, s.cfg.RequestTimeout)
		defer cancel()
		if rt.queued {
			// The body is read before a slot is taken: a slow client holds none.
			body, err := readBody(r)
			if err != nil {
				fail(ProblemFor(err))
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			if !s.queue.acquire() {
				fail(TooMany(DefaultRetryAfter, "%d reads are in progress (search_queue); retry", cap(s.queue)))
				return
			}
			defer s.queue.release()
		}
		if err := rt.handler(sw, r.WithContext(ctx), p); err != nil {
			fail(ProblemFor(err))
		}
	})
}

// finish records the request's metrics, ends its span's status and logs it.
func (s *Server) finish(ctx context.Context, rt *route, r *http.Request, sw *statusWriter, span trace.Span, failure *Error, start time.Time) {
	status := sw.status
	if status == 0 {
		status = http.StatusOK
	}
	elapsed := s.clock.Since(start)
	set := metric.WithAttributeSet(attribute.NewSet(append(rt.attrs.ToSlice(), attribute.Int("http.response.status_code", status))...))
	s.dur.Record(ctx, elapsed.Seconds(), set)
	if status >= 400 {
		s.errs.Add(ctx, 1, set)
	}
	span.SetAttributes(attribute.Int("http.response.status_code", status))
	log := info(ctx).log
	attrs := []slog.Attr{
		slog.String(telemetry.KeyRoute, rt.path),
		slog.String("method", r.Method),
		slog.Int(telemetry.KeyStatus, status),
		slog.Float64(telemetry.KeyDuration, float64(elapsed.Microseconds())/1000),
	}
	if index := r.PathValue("index"); index != "" {
		attrs = append(attrs, slog.String(telemetry.KeyIndex, index))
	}
	switch {
	case status >= 500:
		span.SetStatus(codes.Error, http.StatusText(status))
		if failure != nil && failure.Err != nil {
			span.RecordError(failure.Err)
			attrs = append(attrs, slog.Any("error", failure.Err))
		}
		log.LogAttrs(ctx, slog.LevelError, "request failed", attrs...)
	case rt.quiet:
		log.LogAttrs(ctx, slog.LevelDebug, "request served", attrs...)
	default:
		log.LogAttrs(ctx, slog.LevelInfo, "request served", attrs...)
	}
}

// unmatched answers a path no route serves: 405 with Allow when the path exists under
// other methods, else 404.
func (s *Server) unmatched(w http.ResponseWriter, r *http.Request, _ params) error {
	if _, pattern := s.paths.Handler(r); pattern != "" {
		var allow []string
		for _, rt := range s.routes {
			if rt.path == pattern {
				allow = append(allow, rt.method)
			}
		}
		slices.Sort(allow)
		w.Header().Set("Allow", strings.Join(allow, ", "))
		return &Error{Status: http.StatusMethodNotAllowed, Code: CodeMethod, Detail: fmt.Sprintf("%s takes %s", pattern, strings.Join(allow, ", "))}
	}
	return NotFound(CodeNotFound, "no endpoint at %s", r.URL.Path)
}

// writeJSON writes v as a JSON response with status.
func writeJSON(w http.ResponseWriter, status int, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(append(b, '\n'))
	return nil
}

// Draining a refused request's unread body.
const (
	// minDrainBytes is the least drain reads past a refusal, whatever max_body_bytes.
	minDrainBytes = 1 << 20
	// authDrainBytes is the most drain reads past a 401 or 403: the cheapest refusal
	// to provoke, so the least it may cost.
	authDrainBytes = 256 << 10
	// drainTimeout bounds the drain: a client that sends slowly is cut off then.
	drainTimeout = time.Second
)

// trackedBody counts what is read of a request body and whether it was read to the
// end.
type trackedBody struct {
	r   io.ReadCloser
	n   int64
	eof bool
}

func (b *trackedBody) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	b.n += int64(n)
	if errors.Is(err, io.EOF) {
		b.eof = true
	}
	return n, err
}

func (b *trackedBody) Close() error { return b.r.Close() }

// drain reads and discards what is left of a refused request's body, after flushing
// the answer, so that a client still sending its body (a 413 refuses one as soon as
// it passes the limit) finishes sending and then reads the answer. Closing an HTTP/1
// connection with request bytes unread makes the operating system reset it, and a
// client still writing then sees the reset (on Windows it even loses the answer it
// had already received) instead of the answer. net/http drains 256 KiB the same way
// after a handler that did not read its body.
//
// It drains up to max_body_bytes (at least 1 MiB), or 256 KiB after a 401 or 403 (no
// token is needed to provoke one), and for at most a second, so a client that sends
// far more, or slowly, is still cut off; a body declared longer than that is not
// drained at all. The second is a read deadline: when it cannot be set the body is not
// drained, so a drain never runs unbounded. HTTP/2 needs no drain: a stream is reset
// on its own, the connection and the answer survive it.
func (s *Server) drain(w http.ResponseWriter, r *http.Request, body *trackedBody, status int) int64 {
	if body.eof || r.ContentLength == 0 || r.ProtoMajor >= 2 {
		return 0
	}
	limit := max(s.cfg.MaxBodyBytes, minDrainBytes)
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		limit = authDrainBytes
	}
	if r.ContentLength > 0 && r.ContentLength-body.n > limit {
		return 0
	}
	rc := http.NewResponseController(w)
	if err := rc.SetReadDeadline(time.Now().Add(drainTimeout)); err != nil { //nolint:forbidigo // a connection deadline is by the OS clock
		return 0 // unbounded in time: close instead
	}
	_ = rc.Flush()
	n, _ := io.Copy(io.Discard, io.LimitReader(body, limit))
	return n
}
