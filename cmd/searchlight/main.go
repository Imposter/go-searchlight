// Command searchlight runs a Searchlight node. Settings come from --flags and
// SEARCHLIGHT_* environment variables (run with -h for the list). store_url is required,
// and so is tokens_file unless insecure_no_auth is set; store_url and cluster_token may
// be read from SEARCHLIGHT_<NAME>_FILE.
//
// A node is always a cluster.Node, a cluster of one included. It starts in this order:
// telemetry and the admin listener (/healthz, /readyz, /metrics and, when enabled,
// pprof), so probes answer while the rest starts; the SQL store, opened and migrated,
// retried with backoff while the database cannot be reached (a misconfigured store,
// such as refused credentials or a missing database, fails at once, and any other error
// after 2 minutes); the cluster node and the public API on listen, served before the
// node joins, since peers may call it as soon as it registers; then the node joins the
// cluster. A signal during startup stops it cleanly.
//
// On SIGINT or SIGTERM it stops within one budget, fixed at the signal: shutdown_grace
// plus shutdown_timeout. Readiness turns false and the node retires the copies others
// can stand in for; the API keeps serving for shutdown_grace; the public listener
// drains; the node stops its remaining copies, writing their final manifests, and
// deregisters; then the store, the admin listener and telemetry close by the deadline
// (api.Server.Run splits the budget). A second signal ends the process at once.
//
// It exits 0 after a clean stop and 1, with a one-line message on standard error, when
// it cannot start or its shutdown fails.
//
// "searchlight healthcheck [--live]" probes a node on this host for container
// healthchecks: it GETs /readyz (/healthz with --live) on SEARCHLIGHT_ADMIN_LISTEN and
// exits 0 when it answers 200, else 1.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/cluster"
	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/heapfloor"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

var _ api.Mounter = (*cluster.Node)(nil)

const (
	storeRetryFirst        = time.Second
	storeRetryCap          = 30 * time.Second
	storeRetryUnclassified = 2 * time.Minute
	healthTimeout          = 2 * time.Second
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck(os.Args[2:], os.Getenv, os.Stderr))
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		stop()
	}()
	err := run(ctx, os.Args[1:], os.Getenv, os.Stderr)
	stop()
	switch {
	case errors.Is(err, flag.ErrHelp):
		config.Usage(os.Stdout)
		fmt.Fprintln(os.Stdout, "\nsearchlight healthcheck [--live]: exit 0 if this host's node is ready (live), else 1")
	case err != nil:
		fmt.Fprintln(os.Stderr, "searchlight:", oneLine(err))
		os.Exit(1)
	}
}

func oneLine(err error) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(err.Error(), "\n", "; ")), " ")
}

func run(ctx context.Context, args []string, getenv func(string) string, logOut io.Writer) error {
	cfg, err := config.Load(args, getenv)
	if err != nil {
		return err
	}
	tel, err := telemetry.Setup(ctx, cfg, telemetry.WithVersion(version), telemetry.WithLogOutput(logOut))
	if err != nil {
		return fmt.Errorf("telemetry: %w", err)
	}
	log := tel.Logger
	floor := int64(0)
	if getenv("GOGC") == "" {
		floor = heapfloor.Effective(cfg.GCHeapFloor)
	}
	if floor > 0 {
		defer heapfloor.Keep(floor)()
	}
	log.InfoContext(ctx, "garbage collector heap floor", slog.String("gc_heap_floor", config.FormatBytes(floor)), slog.Bool("gogc_set", getenv("GOGC") != ""))
	clk := clock.Real{}
	budget := &shutdownBudget{cfg: cfg, clock: clk}
	defer func() {
		sctx, cancel := budget.context(ctx)
		defer cancel()
		if err := tel.Shutdown(sctx); err != nil {
			log.WarnContext(sctx, "telemetry shutdown failed", slog.Any("error", err))
		}
	}()
	log.InfoContext(ctx, "searchlight starting", slog.String("version", version), slog.Any("config", cfg))
	if cfg.Pprof && !loopback(cfg.AdminListen) {
		log.WarnContext(ctx, "pprof is served on a non-loopback admin_listen: keep that port private", slog.String("admin_listen", cfg.AdminListen))
	}
	interrupted := func(err error) error {
		if ctx.Err() != nil {
			log.InfoContext(ctx, "searchlight stopped during startup")
			return nil //nolint:nilerr // a signal during startup is a clean stop, whatever it interrupted
		}
		return err
	}

	var lc net.ListenConfig
	adminLn, err := lc.Listen(ctx, "tcp", cfg.AdminListen)
	if err != nil {
		return interrupted(fmt.Errorf("admin_listen: %w", err))
	}
	var public atomic.Pointer[api.Server]
	admin := newAdminServer(tel, &public)
	adminDone := make(chan error, 1)
	go func() { adminDone <- admin.Serve(adminLn) }()
	log.InfoContext(ctx, "admin listener serving", slog.String("address", adminLn.Addr().String()), slog.Bool("pprof", cfg.Pprof))
	defer func() {
		sctx, cancel := budget.context(ctx)
		defer cancel()
		if err := admin.Shutdown(sctx); err != nil {
			_ = admin.Close()
		}
		<-adminDone
	}()

	st, err := openStore(ctx, cfg, tel, clk)
	if err != nil {
		return interrupted(err)
	}
	defer func() {
		sctx, cancel := budget.context(ctx)
		defer cancel()
		closed := make(chan error, 1)
		go func() { closed <- st.Close() }()
		select {
		case err := <-closed:
			if err != nil {
				log.WarnContext(sctx, "closing the store failed", slog.Any("error", err))
			}
		case <-sctx.Done():
			log.WarnContext(sctx, "closing the store did not finish by the shutdown deadline")
		}
	}()

	ln, err := lc.Listen(ctx, "tcp", cfg.Listen)
	if err != nil {
		return interrupted(fmt.Errorf("listen: %w", err))
	}
	cfg.AdvertiseAddress = boundAdvertise(cfg.AdvertiseAddress, ln.Addr())
	n, err := cluster.New(ctx, cluster.Options{
		Store: st, Config: cfg, Version: version, Clock: clk,
		Logger: log, Tracer: tel.Tracer, Meter: tel.Meter,
	})
	if err != nil {
		_ = ln.Close()
		return interrupted(err)
	}
	srv, err := api.NewServer(n, tel, cfg, api.WithClock(clk))
	if err != nil {
		_ = ln.Close()
		sctx, cancel := budget.context(ctx)
		defer cancel()
		return interrupted(errors.Join(err, n.Stop(sctx)))
	}
	public.Store(srv)

	serveCtx, stopServing := context.WithCancelCause(context.WithoutCancel(ctx))
	defer stopServing(nil)
	served := make(chan error, 1)
	go func() { served <- srv.Run(serveCtx, ln) }()
	if err := n.Start(ctx); err != nil {
		stopServing(budget.begin(false))
		return interrupted(errors.Join(err, <-served))
	}

	select {
	case err := <-served:
		if err != nil {
			return err
		}
		return errors.New("the API listener stopped")
	case <-ctx.Done():
	}
	sd := budget.begin(true)
	log.InfoContext(ctx, "searchlight stopping", slog.Time("deadline", sd.Deadline))
	stopServing(sd)
	if err := <-served; err != nil {
		return err
	}
	log.InfoContext(ctx, "searchlight stopped")
	return nil
}

type shutdownBudget struct {
	cfg   config.Config
	clock clock.Clock
	sd    *api.Shutdown
}

func (b *shutdownBudget) begin(grace bool) *api.Shutdown {
	if b.sd == nil {
		b.sd = api.NewShutdown(b.cfg, b.clock)
		if !grace {
			b.sd.Grace = 0
		}
	}
	return b.sd
}

func (b *shutdownBudget) context(parent context.Context) (context.Context, context.CancelFunc) {
	return clock.WithDeadline(context.WithoutCancel(parent), b.clock, b.begin(false).Deadline)
}

func openStore(ctx context.Context, cfg config.Config, tel *telemetry.T, clk clock.Clock) (store.Store, error) {
	return retryStore(ctx, clk, tel.Logger, func(ctx context.Context) (store.Store, error) {
		st, err := store.Open(ctx, cfg.StoreURL, store.WithLogger(tel.Logger), store.WithTracer(tel.Tracer), store.WithMeter(tel.Meter), store.WithClock(clk))
		if err != nil {
			return nil, fmt.Errorf("store: %w", err)
		}
		if err := st.Migrate(ctx); err != nil {
			_ = st.Close()
			return nil, fmt.Errorf("store migrate: %w", err)
		}
		return st, nil
	})
}

func retryStore(ctx context.Context, clk clock.Clock, log *slog.Logger, open func(context.Context) (store.Store, error)) (store.Store, error) {
	wait := storeRetryFirst
	var unclassifiedSince time.Time
	for {
		st, err := open(ctx)
		if err == nil {
			return st, nil
		}
		switch {
		case ctx.Err() != nil:
			return nil, err
		case store.Misconfigured(err):
			return nil, fmt.Errorf("%w (not retried: fix store_url or the database)", err)
		case store.Unreachable(err):
			unclassifiedSince = time.Time{}
		case unclassifiedSince.IsZero():
			unclassifiedSince = clk.Now()
		case clk.Since(unclassifiedSince) >= storeRetryUnclassified:
			return nil, fmt.Errorf("%w (gave up retrying after %s)", err, storeRetryUnclassified)
		}
		log.WarnContext(ctx, "the store cannot be opened; retrying", slog.Any("error", err), slog.Duration("retry_in", wait))
		if err := clk.Sleep(ctx, wait); err != nil {
			return nil, err
		}
		wait = min(2*wait, storeRetryCap)
	}
}

func boundAdvertise(advertise string, bound net.Addr) string {
	host, port, err := net.SplitHostPort(advertise)
	if err != nil || port != "0" {
		return advertise
	}
	_, boundPort, err := net.SplitHostPort(bound.String())
	if err != nil {
		return advertise
	}
	return net.JoinHostPort(host, boundPort)
}

func loopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func newAdminServer(tel *telemetry.T, public *atomic.Pointer[api.Server]) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/", tel.AdminHandler)
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if srv := public.Load(); srv != nil {
			srv.ServeHTTP(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"starting"}` + "\n"))
	})
	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(tel.Logger.Handler(), slog.LevelWarn),
	}
}

func healthcheck(args []string, getenv func(string) string, stderr io.Writer) int {
	fs := flag.NewFlagSet("searchlight healthcheck", flag.ContinueOnError)
	fs.SetOutput(stderr)
	live := fs.Bool("live", false, "check liveness (/healthz) rather than readiness (/readyz)")
	if err := fs.Parse(args); err != nil {
		return 1
	}
	addr := strings.TrimSpace(getenv(config.EnvPrefix + "ADMIN_LISTEN"))
	if addr == "" {
		addr = config.Default().AdminListen
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		fmt.Fprintf(stderr, "searchlight healthcheck: admin_listen %q: %v\n", addr, err)
		return 1
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	path := "/readyz"
	if *live {
		path = "/healthz"
	}
	url := "http://" + net.JoinHostPort(host, port) + path
	ctx, cancel := context.WithTimeout(context.Background(), healthTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		fmt.Fprintf(stderr, "searchlight healthcheck: %v\n", err)
		return 1
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(stderr, "searchlight healthcheck: %v\n", err)
		return 1
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(stderr, "searchlight healthcheck: %s: HTTP %d %s\n", url, resp.StatusCode, strings.TrimSpace(string(body)))
		return 1
	}
	return 0
}
