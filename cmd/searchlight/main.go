// Command searchlight runs a Searchlight node. Settings come from --flags and
// SEARCHLIGHT_* environment variables (run with -h for the list). store_url is required,
// and so is tokens_file unless insecure_no_auth is set; store_url and cluster_token may
// be read from SEARCHLIGHT_<NAME>_FILE.
//
// A node is always a cluster.Node, a cluster of one included. It starts in this order:
// telemetry and the admin listener (/healthz, /readyz, /metrics and, when enabled,
// pprof), so probes answer while the rest starts; the SQL store, opened and migrated;
// the cluster node and the public API on listen, served before the node joins, since
// peers may call it as soon as it registers; then the node joins the cluster.
//
// On SIGINT or SIGTERM it stops in the reverse order: readiness turns false and the API
// keeps serving for shutdown_grace while the node retires the copies others can stand
// in for; the public listener drains; the node stops its remaining copies, writing
// their final manifests, and deregisters; then the store, the admin listener and
// telemetry close. Each phase is bounded by shutdown_timeout. A second signal ends the
// process at once.
//
// It exits 0 after a clean stop and 1, with a one-line message on standard error, when
// it cannot start or its shutdown fails.
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
	"github.com/Imposter/go-searchlight/internal/cluster"
	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
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
	case err != nil:
		fmt.Fprintln(os.Stderr, "searchlight:", oneLine(err))
		os.Exit(1)
	}
}

// oneLine joins a multi-line error (config.Load reports every bad setting) into one
// line.
func oneLine(err error) string {
	return strings.Join(strings.Fields(strings.ReplaceAll(err.Error(), "\n", "; ")), " ")
}

// run starts the node and serves until ctx ends, then shuts it down. Logs go to
// logOut.
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
	defer func() {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
		defer cancel()
		if err := tel.Shutdown(sctx); err != nil {
			log.WarnContext(sctx, "telemetry shutdown failed", slog.Any("error", err))
		}
	}()
	log.InfoContext(ctx, "searchlight starting", slog.String("version", version), slog.Any("config", cfg))

	var lc net.ListenConfig
	adminLn, err := lc.Listen(ctx, "tcp", cfg.AdminListen)
	if err != nil {
		return fmt.Errorf("admin_listen: %w", err)
	}
	var public atomic.Pointer[api.Server]
	admin := newAdminServer(tel, &public)
	adminDone := make(chan error, 1)
	go func() { adminDone <- admin.Serve(adminLn) }()
	log.InfoContext(ctx, "admin listener serving", slog.String("address", adminLn.Addr().String()), slog.Bool("pprof", cfg.Pprof))
	defer func() {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
		defer cancel()
		if err := admin.Shutdown(sctx); err != nil {
			_ = admin.Close()
		}
		<-adminDone
	}()

	st, err := store.Open(ctx, cfg.StoreURL, store.WithLogger(log), store.WithTracer(tel.Tracer), store.WithMeter(tel.Meter))
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer func() {
		if err := st.Close(); err != nil {
			log.WarnContext(ctx, "closing the store failed", slog.Any("error", err))
		}
	}()
	if err := st.Migrate(ctx); err != nil {
		return fmt.Errorf("store migrate: %w", err)
	}

	ln, err := lc.Listen(ctx, "tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	cfg.AdvertiseAddress = boundAdvertise(cfg.AdvertiseAddress, ln.Addr())
	n, err := cluster.New(ctx, cluster.Options{
		Store: st, Config: cfg, Version: version,
		Logger: log, Tracer: tel.Tracer, Meter: tel.Meter,
	})
	if err != nil {
		_ = ln.Close()
		return err
	}
	srv, err := api.NewServer(n, tel, cfg)
	if err != nil {
		_ = ln.Close()
		return errors.Join(err, n.Stop(context.WithoutCancel(ctx)))
	}
	public.Store(srv)

	serveCtx, stopServing := context.WithCancel(ctx)
	defer stopServing()
	served := make(chan error, 1)
	go func() { served <- srv.Run(serveCtx, ln, n.Handler(srv)) }()
	if err := n.Start(ctx); err != nil {
		stopServing()
		return errors.Join(err, <-served)
	}

	select {
	case err := <-served:
		if err != nil {
			return err
		}
		return errors.New("the API listener stopped")
	case <-ctx.Done():
	}
	log.InfoContext(ctx, "searchlight stopping")
	if err := <-served; err != nil {
		return err
	}
	log.InfoContext(ctx, "searchlight stopped")
	return nil
}

// boundAdvertise returns advertise with the bound listener's port when it names port
// 0: listen asked for any free port, and advertise_address was derived from it.
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

// newAdminServer serves the telemetry admin handler (/healthz, /metrics, pprof) and
// GET /readyz: the public API's readiness once it is built, 503 before.
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
