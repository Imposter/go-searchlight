package sqlite

import (
	"net/url"
	"strings"
	"testing"
)

func mustParse(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestPath(t *testing.T) {
	for raw, want := range map[string]string{
		"sqlite:///var/lib/searchlight/searchlight.db": "/var/lib/searchlight/searchlight.db",
		"sqlite:///C:/data/searchlight.db":             "C:/data/searchlight.db",
		"sqlite://data/searchlight.db":                 "data/searchlight.db",
		"sqlite:searchlight.db":                        "searchlight.db",
		"sqlite:///tmp/x.db?_busy_timeout=5":           "/tmp/x.db",
	} {
		got, err := Path(mustParse(t, raw))
		if err != nil || got != want {
			t.Fatalf("%s: %q %v, want %q", raw, got, err, want)
		}
	}
	for _, raw := range []string{"sqlite:", "sqlite://", "sqlite:///:memory:", "sqlite:file:x.db"} {
		if _, err := Path(mustParse(t, raw)); err == nil {
			t.Fatalf("%s: no error", raw)
		}
	}
}

func TestDSNs(t *testing.T) {
	w, r, err := DSNs(mustParse(t, "sqlite:///tmp/x.db?_pragma=cache_size(-2000)"))
	if err != nil {
		t.Fatal(err)
	}
	for _, dsn := range []string{w, r} {
		path, query, _ := strings.Cut(dsn, "?")
		q, err := url.ParseQuery(query)
		if err != nil || path != "/tmp/x.db" {
			t.Fatalf("dsn %q: %v", dsn, err)
		}
		if q.Get("_journal_mode") != "WAL" || q.Get("_busy_timeout") != "30000" || q.Get("_synchronous") != "FULL" ||
			q.Get("_pragma") != "cache_size(-2000)" {
			t.Fatalf("dsn %q", dsn)
		}
	}
	if !strings.Contains(w, "_txlock=immediate") || !strings.Contains(r, "_txlock=deferred") {
		t.Fatalf("writer %q, reader %q", w, r)
	}
	// Settings the URL gives win; the ones the store manages are refused.
	w, _, err = DSNs(mustParse(t, "sqlite:///x.db?_busy_timeout=5&_synchronous=NORMAL"))
	if err != nil || !strings.Contains(w, "_busy_timeout=5&") || !strings.Contains(w, "_synchronous=NORMAL") {
		t.Fatalf("overrides: %q %v", w, err)
	}
	for _, raw := range []string{
		"sqlite:///x.db?_txlock=deferred", "sqlite:///x.db?_journal_mode=DELETE", "sqlite:///x.db?_locking_mode=EXCLUSIVE",
		"sqlite:///x.db?_pragma=locking_mode(EXCLUSIVE)", "sqlite:///x.db?_pragma=LOCKING_MODE%3Dexclusive",
		"sqlite:///x.db?_pragma=journal_mode(DELETE)", "sqlite:///x.db?_pragma=wal_autocheckpoint(1000)",
	} {
		if _, _, err := DSNs(mustParse(t, raw)); err == nil {
			t.Fatalf("%s accepted", raw)
		}
	}
}
