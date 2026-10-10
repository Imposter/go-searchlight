package api_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/store/mysql"
	"github.com/Imposter/go-searchlight/internal/store/postgres"
)

// The external databases the API also runs on, as the store's conformance suite
// names them; unset, those dialects are skipped.
const (
	envPG    = "SEARCHLIGHT_TEST_PG_URL"
	envMySQL = "SEARCHLIGHT_TEST_MYSQL_URL"
)

func randName() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return "sl_api_" + hex.EncodeToString(b[:])
}

// forEachDialect runs fn with a store URL of a fresh database of every available
// dialect ("" is SQLite, the default).
func forEachDialect(t *testing.T, fn func(t *testing.T, storeURL string)) {
	t.Run("sqlite", func(t *testing.T) { fn(t, "") })
	t.Run("postgres", func(t *testing.T) {
		base := os.Getenv(envPG)
		if base == "" {
			t.Skip(envPG + " is not set")
		}
		u, err := url.Parse(base)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := postgres.Config(u)
		if err != nil {
			t.Fatal(err)
		}
		admin := stdlib.OpenDB(*cfg)
		t.Cleanup(func() { _ = admin.Close() })
		schema := randName()
		if _, err := admin.ExecContext(context.Background(), "CREATE SCHEMA "+schema); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP SCHEMA "+schema+" CASCADE") })
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		fn(t, u.String())
	})
	t.Run("mysql", func(t *testing.T) {
		base := os.Getenv(envMySQL)
		if base == "" {
			t.Skip(envMySQL + " is not set")
		}
		u, err := url.Parse(base)
		if err != nil {
			t.Fatal(err)
		}
		cfg, err := mysql.Config(u)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := gomysql.NewConnector(cfg)
		if err != nil {
			t.Fatal(err)
		}
		admin := sql.OpenDB(conn)
		t.Cleanup(func() { _ = admin.Close() })
		name := randName()
		if _, err := admin.ExecContext(context.Background(), "CREATE DATABASE "+name); err != nil {
			t.Skipf("the account cannot create a database: %v", err)
		}
		t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), "DROP DATABASE "+name) })
		u.Path = "/" + name
		fn(t, u.String())
	})
}

// TestEndToEndOnEveryDialect runs the API's main paths on each SQL dialect.
func TestEndToEndOnEveryDialect(t *testing.T) {
	forEachDialect(t, func(t *testing.T, storeURL string) {
		e := newEnv(t, envOpts{cfg: func(c *config.Config) {
			if storeURL != "" {
				c.StoreURL = storeURL
			}
		}})
		e.createIndex("e2e", `{"mapping": {"fields": {"brand": "keyword", "price": "number"}}, "settings": {"shards": 2}}`)
		q := e.must(http.StatusOK, "PUT", "/indexes/e2e/queries/cheap", `{"query": {"field": "price", "op": "lt", "value": 10}, "meta": {"owner": "x"}}`)
		w := e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/e2e/_bulk?percolate=true&wait_for_seq=%d", seqOf(t, q)), ndjson(
			`{"upsert": {"id": "a"}}`, `{"brand": "acme", "price": 5}`,
			`{"upsert": {"id": "b"}}`, `{"brand": "globex", "price": 50}`,
		))
		items := w["items"].([]any)                                                                                 //nolint:forcetypeassert,errcheck // the shape
		if fmt.Sprint(items[0].(map[string]any)["queries"], items[1].(map[string]any)["queries"]) != "[cheap] []" { //nolint:forcetypeassert,errcheck // the shape
			t.Errorf("bulk percolation = %v", items)
		}
		r := e.must(http.StatusOK, "POST", fmt.Sprintf("/indexes/e2e/_search?wait_for_seq=%d", seqOf(t, w)), `{"sort": [{"price": "desc"}]}`)
		if fmt.Sprint(ids(r)) != "[b a]" {
			t.Errorf("search = %v", r)
		}
		doc := e.must(http.StatusOK, "GET", "/indexes/e2e/docs/a", "")
		seq := int64(doc["seq"].(float64)) //nolint:forcetypeassert,errcheck // the shape
		e.problem(e.do("PUT", fmt.Sprintf("/indexes/e2e/docs/a?if_seq=%d", seq+1000), `{"price": 1}`), http.StatusConflict, "conflict")
		e.must(http.StatusOK, "PUT", fmt.Sprintf("/indexes/e2e/docs/a?if_seq=%d&refresh=wait_for", seq), `{"price": 1}`)
		if l := e.must(http.StatusOK, "GET", "/indexes/e2e/queries", ""); len(l["queries"].([]any)) != 1 { //nolint:forcetypeassert,errcheck // the shape
			t.Errorf("queries = %v", l)
		}
		e.must(http.StatusOK, "DELETE", "/indexes/e2e", "")
		e.problem(e.do("GET", "/indexes/e2e/docs/a", ""), http.StatusNotFound, "index_not_found")
	})
}
