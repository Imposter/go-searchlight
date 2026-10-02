// Command searchlight runs a Searchlight node. Settings come from --flags and
// SEARCHLIGHT_* environment variables (run with -h for the list); only
// store_url is required.
//
// This is the foundation skeleton: it loads the configuration, sets up
// telemetry and serves /healthz, /metrics and (optionally) pprof on the admin
// listener until SIGINT or SIGTERM, then shuts down gracefully.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Getenv)
	stop()
	switch {
	case errors.Is(err, flag.ErrHelp):
		config.Usage(os.Stdout)
	case err != nil:
		fmt.Fprintln(os.Stderr, "searchlight:", err)
		os.Exit(1)
	}
}

// run loads the configuration, sets up telemetry and serves until ctx ends.
func run(ctx context.Context, args []string, getenv func(string) string) error {
	cfg, err := config.Load(args, getenv)
	if err != nil {
		return err
	}
	tel, err := telemetry.Setup(ctx, cfg, telemetry.WithVersion(version))
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
		defer cancel()
		if err := tel.Shutdown(sctx); err != nil {
			tel.Logger.WarnContext(sctx, "telemetry shutdown failed", "error", err)
		}
	}()

	var lc net.ListenConfig
	adminLn, err := lc.Listen(ctx, "tcp", cfg.AdminListen)
	if err != nil {
		return fmt.Errorf("admin_listen: %w", err)
	}
	tel.Logger.InfoContext(ctx, "searchlight starting", "version", version, "config", cfg)
	return serve(ctx, cfg, tel, adminLn)
}

// serve runs the admin server on ln until ctx ends, then drains it within
// cfg.ShutdownTimeout. It owns ln.
func serve(ctx context.Context, cfg config.Config, tel *telemetry.T, ln net.Listener) error {
	admin := &http.Server{
		Handler:           tel.AdminHandler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		ErrorLog:          slog.NewLogLogger(tel.Logger.Handler(), slog.LevelWarn),
	}
	errc := make(chan error, 1)
	go func() { errc <- admin.Serve(ln) }()
	tel.Logger.InfoContext(ctx, "admin listener serving", "address", ln.Addr().String(), "pprof", cfg.Pprof)

	select {
	case err := <-errc:
		return fmt.Errorf("admin listener: %w", err)
	case <-ctx.Done():
	}

	tel.Logger.InfoContext(ctx, "searchlight stopping")
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.ShutdownTimeout)
	defer cancel()
	if err := admin.Shutdown(sctx); err != nil {
		return fmt.Errorf("admin shutdown: %w", err)
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("admin listener: %w", err)
	}
	tel.Logger.InfoContext(sctx, "searchlight stopped")
	return nil
}
