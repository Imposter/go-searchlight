// Package storetest provides the SQLite databases tests run on. They run with
// synchronous=OFF, since a test never outlives the operating system that holds its
// writes, and they start as a copy of a template migrated once per process, where
// migrating each database afresh costs most of a second. Tests of durability,
// crashes and fsyncs use [DurableSQLiteURL] instead.
package storetest

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/Imposter/go-searchlight/internal/store"
)

// SQLiteURL is the store URL of the SQLite database file at path, with
// synchronous=OFF.
func SQLiteURL(path string) string {
	return DurableSQLiteURL(path) + "?_synchronous=OFF"
}

// DurableSQLiteURL is the store URL of the SQLite database file at path, with the
// store's own synchronous=FULL.
func DurableSQLiteURL(path string) string {
	path = filepath.ToSlash(path)
	if strings.HasPrefix(path, "/") {
		return "sqlite://" + path
	}
	return "sqlite:///" + path
}

// Migrated writes a migrated database to path, which must not exist yet, and
// returns path.
func Migrated(t testing.TB, path string) string {
	t.Helper()
	files, err := template()
	if err != nil {
		t.Fatalf("migrate the template database: %v", err)
	}
	for suffix, b := range files {
		if err := os.WriteFile(path+suffix, b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return path
}

// walSuffixes are the files of a closed database: the database and, should SQLite
// have left one, its write-ahead log. The shared-memory index is rebuilt on open.
var walSuffixes = []string{"", "-wal"}

var template = sync.OnceValues(func() (map[string][]byte, error) {
	dir, err := os.MkdirTemp("", "storetest")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "template.db")
	ctx := context.Background()
	st, err := store.Open(ctx, SQLiteURL(path), store.WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		return nil, err
	}
	if err := errors.Join(st.Migrate(ctx), st.Close()); err != nil {
		return nil, err
	}
	files := make(map[string][]byte, len(walSuffixes))
	for _, suffix := range walSuffixes {
		b, err := os.ReadFile(path + suffix)
		switch {
		case errors.Is(err, os.ErrNotExist) && suffix != "":
		case err != nil:
			return nil, err
		default:
			files[suffix] = b
		}
	}
	return files, nil
})
