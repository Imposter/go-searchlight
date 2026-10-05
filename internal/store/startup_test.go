package store_test

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Imposter/go-searchlight/internal/store"
)

func TestStartupErrorClasses(t *testing.T) {
	const (
		unreachable = "unreachable"
		fatal       = "misconfigured"
		unknown     = "unclassified"
	)
	dial := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	for _, c := range []struct {
		name string
		err  error
		want string
	}{
		{"dial refused", dial, unreachable},
		{"dial refused, wrapped", fmt.Errorf("open postgres store: %w", dial), unreachable},
		{"dns", &net.DNSError{Err: "no such host", Name: "db", IsTemporary: true}, unreachable},
		{"postgres starting up", &pgconn.PgError{Code: "57P03"}, unreachable},
		{"postgres connection failure", &pgconn.PgError{Code: "08006"}, unreachable},
		{"postgres too many connections", &pgconn.PgError{Code: "53300"}, unreachable},
		{"postgres bad password", &pgconn.PgError{Code: "28P01"}, fatal},
		{"postgres bad authorization", fmt.Errorf("ping: %w", &pgconn.PgError{Code: "28000"}), fatal},
		{"postgres unknown database", &pgconn.PgError{Code: "3D000"}, fatal},
		{"postgres syntax error", &pgconn.PgError{Code: "42601"}, unknown},
		{"mysql bad credentials", &mysql.MySQLError{Number: 1045}, fatal},
		{"mysql denied database", &mysql.MySQLError{Number: 1044}, fatal},
		{"mysql unknown database", &mysql.MySQLError{Number: 1049}, fatal},
		{"mysql cannot connect", &mysql.MySQLError{Number: 2003}, unreachable},
		{"mysql lost connection", &mysql.MySQLError{Number: 2013}, unreachable},
		{"mysql too many connections", &mysql.MySQLError{Number: 1040}, unreachable},
		{"mysql invalid connection", mysql.ErrInvalidConn, unreachable},
		{"bad connection", driver.ErrBadConn, unreachable},
		{"newer schema", fmt.Errorf("migrate: %w", store.ErrNewerSchema), fatal},
		{"invalid", store.ErrInvalid, fatal},
		{"anything else", errors.New("something odd"), unknown},
	} {
		got := unknown
		switch {
		case store.Misconfigured(c.err) && store.Unreachable(c.err):
			got = "both"
		case store.Misconfigured(c.err):
			got = fatal
		case store.Unreachable(c.err):
			got = unreachable
		}
		if got != c.want {
			t.Errorf("%s (%v): %s, want %s", c.name, c.err, got, c.want)
		}
	}
}

func TestBadSQLitePathIsMisconfigured(t *testing.T) {
	url := "sqlite:///" + filepath.ToSlash(filepath.Join(t.TempDir(), "missing", "dir", "searchlight.db"))
	st, err := store.Open(t.Context(), url)
	if err == nil {
		err = st.Migrate(t.Context())
		_ = st.Close()
	}
	if err == nil || !store.Misconfigured(err) || store.Unreachable(err) {
		t.Fatalf("a SQLite file in a missing directory: %v (misconfigured %v, unreachable %v), want misconfigured",
			err, store.Misconfigured(err), store.Unreachable(err))
	}
}
