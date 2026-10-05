package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/store"
)

func driveStoreRetry(ctx context.Context, t *testing.T, open func(attempt int) error) (attempts int, elapsed time.Duration, err error) {
	t.Helper()
	fk := clock.NewFake(time.Unix(1_700_000_000, 0))
	start := fk.Now()
	done := make(chan error, 1)
	go func() {
		_, err := retryStore(ctx, fk, slog.New(slog.DiscardHandler), func(context.Context) (store.Store, error) {
			attempts++
			return nil, open(attempts)
		})
		done <- err
	}()
	deadline := time.After(30 * time.Second)
	for {
		select {
		case err := <-done:
			return attempts, fk.Since(start), err
		case <-deadline:
			t.Fatal("retryStore did not return")
		default:
		}
		if fk.Waiters() > 0 {
			fk.Advance(storeRetryCap)
		} else {
			time.Sleep(time.Millisecond)
		}
	}
}

func TestRetryStore(t *testing.T) {
	dial := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}

	t.Run("misconfiguration fails at once", func(t *testing.T) {
		for _, bad := range []error{&pgconn.PgError{Code: "28P01"}, &pgconn.PgError{Code: "3D000"}} {
			attempts, _, err := driveStoreRetry(t.Context(), t, func(int) error { return bad })
			if attempts != 1 || err == nil || !strings.Contains(err.Error(), "not retried") {
				t.Errorf("%v: %d attempts, err %v; want one attempt and a not-retried error", bad, attempts, err)
			}
		}
	})

	t.Run("unreachable is retried until it opens", func(t *testing.T) {
		const failures = 10
		attempts, elapsed, err := driveStoreRetry(t.Context(), t, func(attempt int) error {
			if attempt <= failures {
				return dial
			}
			return nil
		})
		if err != nil || attempts != failures+1 {
			t.Fatalf("%d attempts, err %v; want %d and none", attempts, err, failures+1)
		}
		if elapsed <= storeRetryUnclassified {
			t.Errorf("retried for %v; the case wants longer than the unclassified cap", elapsed)
		}
	})

	t.Run("unclassified gives up after two minutes", func(t *testing.T) {
		attempts, elapsed, err := driveStoreRetry(t.Context(), t, func(int) error { return errors.New("something odd") })
		if err == nil || !strings.Contains(err.Error(), "gave up") {
			t.Fatalf("err = %v, want one that gave up", err)
		}
		if elapsed < storeRetryUnclassified || elapsed > storeRetryUnclassified+storeRetryCap {
			t.Errorf("gave up after %v (%d attempts), want about %v", elapsed, attempts, storeRetryUnclassified)
		}
	})

	t.Run("a signal stops the retries", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		attempts, _, err := driveStoreRetry(ctx, t, func(attempt int) error {
			if attempt == 3 {
				cancel()
			}
			return dial
		})
		if attempts != 3 || err == nil {
			t.Errorf("%d attempts, err %v; want 3 and an error", attempts, err)
		}
	})
}

func TestRunExitsOnABadSQLitePath(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	dir := t.TempDir()
	env := map[string]string{
		"SEARCHLIGHT_STORE_URL":        "sqlite:///" + filepath.ToSlash(filepath.Join(dir, "missing", "searchlight.db")),
		"SEARCHLIGHT_DATA_DIR":         filepath.Join(dir, "data"),
		"SEARCHLIGHT_ADMIN_LISTEN":     "127.0.0.1:0",
		"SEARCHLIGHT_LISTEN":           "127.0.0.1:0",
		"SEARCHLIGHT_INSECURE_NO_AUTH": "true",
	}
	start := time.Now()
	err := run(t.Context(), nil, func(k string) string { return env[k] }, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "not retried") {
		t.Fatalf("run = %v, want a not-retried store error", err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("run took %v to give up on a bad SQLite path", took)
	}
	if strings.Contains(oneLine(err), "\n") {
		t.Errorf("the error is not one line: %q", oneLine(err))
	}
}
