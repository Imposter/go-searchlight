package postgres

import (
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestConfig(t *testing.T) {
	for _, scheme := range []string{"postgres", "postgresql"} {
		u, err := url.Parse(scheme + "://sl:pw@db.example:5433/searchlight?sslmode=disable&search_path=s1")
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := Config(u)
		if err != nil {
			t.Fatalf("%s: %v", scheme, err)
		}
		if cfg.Host != "db.example" || cfg.Port != 5433 || cfg.User != "sl" || cfg.Password != "pw" || cfg.Database != "searchlight" {
			t.Fatalf("%s: %+v", scheme, cfg.Config)
		}
		if cfg.RuntimeParams["search_path"] != "s1" {
			t.Fatalf("%s: runtime params %v", scheme, cfg.RuntimeParams)
		}
	}
}

func TestConfigErrorHidesPassword(t *testing.T) {
	u, err := url.Parse("postgres://sl:secret@db.example:5432/x?sslmode=bogus")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Config(u); err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatalf("got %v", err)
	}
}

func TestRetryable(t *testing.T) {
	for code, want := range map[string]bool{"40001": true, "40P01": true, "23505": false} {
		if got := retryable(&pgconn.PgError{Code: code}); got != want {
			t.Fatalf("%s: %v", code, got)
		}
	}
	if retryable(errors.New("x")) {
		t.Fatal("plain error retryable")
	}
}
