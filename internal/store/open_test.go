package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

func TestOpenRejectsBadURLs(t *testing.T) {
	ctx := context.Background()
	for _, u := range []string{
		"mongodb://user:secret@host/db",
		"http://example.com",
		"sqlite:",
		"sqlite:///:memory:",
		"::not a url",
	} {
		_, err := Open(ctx, u)
		if err == nil {
			t.Fatalf("Open(%q) succeeded", u)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("Open(%q) error leaks the password: %v", u, err)
		}
	}
	if _, err := Open(ctx, "redis://x"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown scheme: %v", err)
	}
}

func TestOpenSelectsDialect(t *testing.T) {
	st, err := Open(context.Background(), sqliteURL(t.TempDir()+"/x.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if st.Dialect() != "sqlite" {
		t.Fatalf("dialect %s", st.Dialect())
	}
	if _, ok := st.(Watcher); ok {
		t.Fatal("sqlite store is a Watcher")
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.Ping(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("ping after close: %v", err)
	}
}

func dialectMust(scheme string) *dialect.Dialect {
	d, err := dialectFor(scheme)
	if err != nil {
		panic(err)
	}
	return d
}

func TestSplitStatements(t *testing.T) {
	got := splitStatements("-- header\nCREATE TABLE a (\n\tx INT\n);\n\n-- note\nINSERT INTO a VALUES (1);\nCREATE INDEX i ON a (x)")
	if len(got) != 3 || got[0] != "CREATE TABLE a (\n\tx INT\n)" || got[1] != "INSERT INTO a VALUES (1)" || got[2] != "CREATE INDEX i ON a (x)" {
		t.Fatalf("%q", got)
	}
}

func TestMigrationsLoad(t *testing.T) {
	for _, scheme := range []string{"sqlite", "postgres", "mysql"} {
		ms, err := loadMigrations(dialectMust(scheme).Migrations)
		if err != nil || len(ms) == 0 || ms[0].version != 1 || len(ms[0].stmts) < 10 {
			t.Fatalf("%s migrations: %v %v", scheme, ms, err)
		}
	}
}

func TestIsTransient(t *testing.T) {
	if IsTransient(nil) || IsTransient(ErrConflict) || IsTransient(&ConflictError{Positions: []int{0}, Current: []int64{0}}) || IsTransient(context.Canceled) {
		t.Fatal("expected not transient")
	}
	if !IsTransient(errors.New("connection refused")) {
		t.Fatal("expected transient")
	}
}
