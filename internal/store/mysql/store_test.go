package mysql

import (
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

func mustParse(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

// The form CI sets SEARCHLIGHT_TEST_MYSQL_URL in.
func TestConfigFromCIURL(t *testing.T) {
	cfg, err := Config(mustParse(t, "mysql://searchlight:searchlight@127.0.0.1:3306/searchlight"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.User != "searchlight" || cfg.Passwd != "searchlight" || cfg.Net != "tcp" || cfg.Addr != "127.0.0.1:3306" || cfg.DBName != "searchlight" {
		t.Fatalf("config %+v", cfg)
	}
	if !cfg.InterpolateParams || !cfg.ClientFoundRows || cfg.MultiStatements || cfg.Loc != time.UTC {
		t.Fatalf("store settings not forced: %+v", cfg)
	}
}

func TestDSNRoundTrip(t *testing.T) {
	dsn, err := DSN(mustParse(t, "mysql://u%40x:p%40ss%3Aw%2Frd@db.example:3307/my%2Ddb?timeout=5s&tls=skip-verify"))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse %q: %v", dsn, err)
	}
	if cfg.User != "u@x" || cfg.Passwd != "p@ss:w/rd" || cfg.Addr != "db.example:3307" || cfg.DBName != "my-db" {
		t.Fatalf("round trip %+v", cfg)
	}
	if cfg.Timeout != 5*time.Second || cfg.TLSConfig != "skip-verify" {
		t.Fatalf("params lost: timeout %s tls %q", cfg.Timeout, cfg.TLSConfig)
	}
}

func TestConfigDefaultsPort(t *testing.T) {
	for raw, addr := range map[string]string{
		"mysql://u@db/x":      "db:3306",
		"mysql://u@[::1]/x":   "[::1]:3306",
		"mysql://u@[::1]:9/x": "[::1]:9",
	} {
		cfg, err := Config(mustParse(t, raw))
		if err != nil || cfg.Addr != addr {
			t.Fatalf("%s: addr %q, err %v; want %q", raw, cfg.Addr, err, addr)
		}
	}
}

func TestConfigErrors(t *testing.T) {
	for _, raw := range []string{"mysql:///db", "mysql://u:secret@host", "mysql://u:secret@host/db?timeout=forever"} {
		_, err := Config(mustParse(t, raw))
		if err == nil {
			t.Fatalf("%s: no error", raw)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("%s: error leaks the password: %v", raw, err)
		}
	}
}

func TestClaimAssignmentOrder(t *testing.T) {
	q, args := Dialect().Claim(dialect.ClaimArgs{Index: "i", Shard: 1, Node: "n", TTLms: 1000})
	// node_id must be assigned after state and applied_seq, and lease_until
	// last, because MySQL evaluates the assignments in order.
	iState, iApplied := strings.Index(q, "state = IF"), strings.Index(q, "applied_seq = IF")
	iEpoch := strings.Index(q, "epoch = IF")
	iNode, iLease := strings.Index(q, "node_id = IF"), strings.Index(q, "lease_until = IF")
	if iState < 0 || iState > iApplied || iApplied > iEpoch || iEpoch > iNode || iNode > iLease {
		t.Fatalf("assignment order wrong:\n%s", q)
	}
	if strings.Count(q, "?") != len(args) {
		t.Fatalf("%d placeholders for %d args", strings.Count(q, "?"), len(args))
	}
}
