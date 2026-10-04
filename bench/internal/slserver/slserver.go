// Package slserver runs a Searchlight node for the benchmark: the SQL store, the
// single-node coordinator and the HTTP API, wired as the API tests wire them, on a
// local listener. It stands in for cmd/searchlight (Task 12) until that binary is
// merged: the harness itself only ever talks to a URL.
package slserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/node"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// Options configure a node.
type Options struct {
	// Config is the node's configuration (config.Load or config.Default). When its
	// StoreURL is empty, a SQLite database in Dir is used; when its DataDir is the
	// default, Dir/data is used.
	Config config.Config
	// Dir holds the node's files when Config does not place them.
	Dir string
	// Addr is the listener address; "127.0.0.1:0" when empty.
	Addr string
	// Token, when set, is the API's one read-write token (a tokens file is written in
	// Dir); empty serves without auth.
	Token string
	// Version is reported in the node list.
	Version string
	// LogOutput receives the node's JSON logs (at Config.LogLevel); nil discards them.
	LogOutput io.Writer
}

// Server is a running node.
type Server struct {
	// URL is the API's base URL.
	URL string
	// Token is the bearer token to send ("" without auth).
	Token string
	// DiskPaths are the directories holding the node's durable state: the SQLite
	// database's directory and data_dir.
	DiskPaths []string
	// Config is the configuration the node runs with.
	Config config.Config

	cancel context.CancelFunc
	done   chan error
	st     store.Store
	tel    *telemetry.T
}

// NewToken returns a random API token.
func NewToken() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Start opens the store, starts the node and serves the API until Close.
func Start(ctx context.Context, o Options) (*Server, error) {
	cfg := o.Config
	if o.Dir == "" {
		o.Dir = "."
	}
	dir, err := filepath.Abs(o.Dir)
	if err != nil {
		return nil, err
	}
	o.Dir = dir
	if err := os.MkdirAll(o.Dir, 0o750); err != nil {
		return nil, err
	}
	dbDir := ""
	if cfg.StoreURL == "" {
		dbDir = filepath.Join(o.Dir, "db")
		if err := os.MkdirAll(dbDir, 0o750); err != nil {
			return nil, err
		}
		cfg.StoreURL = "sqlite:///" + filepath.ToSlash(filepath.Join(dbDir, "searchlight.db"))
	}
	if cfg.DataDir == "" || cfg.DataDir == config.Default().DataDir {
		cfg.DataDir = filepath.Join(o.Dir, "data")
	}
	if o.Token != "" {
		path := filepath.Join(o.Dir, "tokens")
		if err := os.WriteFile(path, []byte(o.Token+" write\n"), 0o600); err != nil {
			return nil, err
		}
		cfg.TokensFile, cfg.InsecureNoAuth = path, false
	} else if cfg.TokensFile == "" {
		cfg.InsecureNoAuth = true
	}
	addr := o.Addr
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	cfg.Listen = ln.Addr().String()
	if cfg.AdvertiseAddress == "" {
		cfg.AdvertiseAddress = cfg.Listen
	}
	if o.LogOutput == nil {
		o.LogOutput = io.Discard
	}
	tel, err := telemetry.Setup(ctx, cfg, telemetry.WithVersion(o.Version), telemetry.WithLogOutput(o.LogOutput))
	if err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("telemetry: %w", err)
	}
	logger := tel.Logger
	fail := func(err error) (*Server, error) {
		_ = ln.Close()
		_ = tel.Shutdown(context.WithoutCancel(ctx))
		return nil, err
	}
	st, err := store.Open(ctx, cfg.StoreURL, store.WithLogger(logger))
	if err != nil {
		return fail(fmt.Errorf("store: %w", err))
	}
	if err := st.Migrate(ctx); err != nil {
		_ = st.Close()
		return fail(fmt.Errorf("store migrate: %w", err))
	}
	n, err := node.NewSingle(ctx, node.Options{
		Store: st, Config: cfg, Version: o.Version, Logger: logger,
		Tracer: tel.Tracer, Meter: tel.Meter,
	})
	if err != nil {
		_ = st.Close()
		return fail(fmt.Errorf("node: %w", err))
	}
	srv, err := api.NewServer(n, tel, cfg)
	if err != nil {
		_ = n.Close(context.WithoutCancel(ctx))
		_ = st.Close()
		return fail(fmt.Errorf("api: %w", err))
	}
	rctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	s := &Server{URL: "http://" + cfg.Listen, Token: o.Token, Config: cfg, cancel: cancel, done: make(chan error, 1), st: st, tel: tel}
	s.DiskPaths = []string{cfg.DataDir}
	if dbDir != "" {
		s.DiskPaths = append(s.DiskPaths, dbDir)
	}
	go func() { s.done <- srv.Run(rctx, ln) }()
	return s, nil
}

// Wait blocks until the server stops on its own (or is closed), returning why.
func (s *Server) Wait() error { return <-s.done }

// Close shuts the node down gracefully: the API drains, the copies flush, the store
// closes.
func (s *Server) Close(ctx context.Context) error {
	s.cancel()
	var err error
	select {
	case err = <-s.done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return errors.Join(err, s.st.Close(), s.tel.Shutdown(sctx))
}
