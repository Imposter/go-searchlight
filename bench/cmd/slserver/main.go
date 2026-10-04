// Command slserver runs a single Searchlight node for the benchmark: the SQL store,
// the single-node coordinator and the HTTP API (bench/internal/slserver). It stands in
// for cmd/searchlight until that binary is wired (Task 12).
//
//	slserver [-dir DIR] [-addr HOST:PORT] [-token TOKEN|auto] [-info FILE] [-- searchlight settings...]
//
// Node settings come after "--" as --name=value, or from SEARCHLIGHT_* variables, as
// cmd/searchlight reads them. Unless given: store_url is SQLite in DIR/db (synchronous
// FULL: acknowledged = durable), data_dir is DIR/data, and log_level is warn (no
// access log on the hot path). -info writes {"url", "token", "pid", "disk_paths"} once
// the node serves, for scripts.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/Imposter/go-searchlight/bench/internal/slserver"
	"github.com/Imposter/go-searchlight/internal/config"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:])
	stop()
	if err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "slserver:", err)
		os.Exit(1)
	}
}

// given reports whether a setting is on the command line or in the environment.
func given(args []string, name string) bool {
	if os.Getenv(config.EnvPrefix+strings.ToUpper(name)) != "" || os.Getenv(config.EnvPrefix+strings.ToUpper(name)+"_FILE") != "" {
		return true
	}
	return slices.ContainsFunc(args, func(a string) bool {
		a = strings.TrimLeft(a, "-")
		return a == name || strings.HasPrefix(a, name+"=")
	})
}

func run(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("slserver", flag.ContinueOnError)
	dir := fs.String("dir", "sl-bench", "directory for the database, segments and tokens file")
	addr := fs.String("addr", "127.0.0.1:8780", "API listen address")
	token := fs.String("token", "", `API token ("auto" generates one; empty serves without auth)`)
	info := fs.String("info", "", "write the URL, token, pid and disk paths here as JSON once serving")
	if err := fs.Parse(args); err != nil {
		return err
	}
	settings := fs.Args()
	abs, err := filepath.Abs(*dir)
	if err != nil {
		return err
	}
	defaultStore := !given(settings, "store_url")
	if defaultStore {
		if err := os.MkdirAll(filepath.Join(abs, "db"), 0o750); err != nil {
			return err
		}
		settings = append(settings, "--store_url=sqlite:///"+filepath.ToSlash(filepath.Join(abs, "db", "searchlight.db")))
	}
	if !given(settings, "data_dir") {
		settings = append(settings, "--data_dir="+filepath.Join(abs, "data"))
	}
	if !given(settings, "log_level") {
		settings = append(settings, "--log_level=warn")
	}
	tok := *token
	if tok == "auto" {
		tok = slserver.NewToken()
	}
	switch {
	case tok != "" && !given(settings, "tokens_file"):
		// The config refuses to load without a tokens file unless auth is off, so the
		// file the server will use is written before loading it.
		path := filepath.Join(abs, "tokens")
		if err := os.WriteFile(path, []byte(tok+" write\n"), 0o600); err != nil {
			return err
		}
		settings = append(settings, "--tokens_file="+path)
	case tok == "" && !given(settings, "tokens_file") && !given(settings, "insecure_no_auth"):
		settings = append(settings, "--insecure_no_auth")
	}
	cfg, err := config.Load(settings, os.Getenv)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			config.Usage(os.Stdout)
		}
		return err
	}
	s, err := slserver.Start(ctx, slserver.Options{Config: cfg, Dir: abs, Addr: *addr, Token: tok, Version: "bench", LogOutput: os.Stderr})
	if err != nil {
		return err
	}
	if defaultStore {
		s.DiskPaths = append(s.DiskPaths, filepath.Join(abs, "db"))
	}
	fmt.Fprintf(os.Stderr, "slserver: serving %s (store %s, data %s)\n", s.URL, redact(s.Config.StoreURL), s.Config.DataDir)
	if *info != "" {
		b, err := json.Marshal(map[string]any{"url": s.URL, "token": s.Token, "pid": os.Getpid(), "disk_paths": dedupe(s.DiskPaths)})
		if err != nil {
			return err
		}
		if err := os.WriteFile(*info, b, 0o600); err != nil {
			return err
		}
	}
	select {
	case <-ctx.Done():
	case err := <-wait(s):
		return err
	}
	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()
	return s.Close(cctx)
}

func wait(s *slserver.Server) <-chan error {
	c := make(chan error, 1)
	go func() { c <- s.Wait() }()
	return c
}

func dedupe(paths []string) []string {
	slices.Sort(paths)
	return slices.Compact(paths)
}

func redact(u string) string {
	if i := strings.Index(u, "@"); i >= 0 {
		if j := strings.Index(u, "://"); j >= 0 && j < i {
			return u[:j+3] + "…" + u[i:]
		}
	}
	return u
}
