package config

import (
	"bytes"
	"errors"
	"flag"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// envOf returns a getenv over a fixed map.
func envOf(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

const sqliteURL = "sqlite:///tmp/searchlight.db"

// insecure is the environment of a node with no tokens file, which Load refuses
// unless auth is explicitly off.
const insecure = "SEARCHLIGHT_INSECURE_NO_AUTH"

func TestLoadDefaults(t *testing.T) {
	c, err := Load(nil, envOf(map[string]string{"SEARCHLIGHT_STORE_URL": sqliteURL, insecure: "true"}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	host, _ := os.Hostname()
	if host == "" {
		host = "localhost"
	}
	want := Config{
		StoreURL:              sqliteURL,
		Listen:                ":8780",
		AdminListen:           "127.0.0.1:8781",
		AdvertiseAddress:      host + ":8780",
		NodeID:                host,
		DataDir:               "data",
		RefreshInterval:       time.Second,
		FlushInterval:         10 * time.Second,
		MaxLag:                2 * time.Second,
		ChangelogPollInterval: 500 * time.Millisecond,
		RemapDebounce:         2 * time.Second,
		HaltRetryBase:         30 * time.Second,
		HaltRetryCap:          10 * time.Minute,
		RebuildRetryCap:       2 * time.Minute,
		MergeBudget:           64 << 20,
		MergeThreads:          max(1, runtime.GOMAXPROCS(0)/4),
		SearchThreads:         runtime.GOMAXPROCS(0),
		LogLevel:              slog.LevelInfo,
		ShutdownTimeout:       time.Minute,
		InsecureNoAuth:        true,
		MaxBodyBytes:          16 << 20,
		MaxDocBytes:           4 << 20,
		MaxBulkOps:            10_000,
		RequestTimeout:        30 * time.Second,
		ReadTimeout:           time.Minute,
		SearchQueue:           1000,

		MaxInflightWriteBytes: 512 << 20,
		MaxInflightReadBytes:  256 << 20,
		InflightAmplification: 10,
		DropTimeout:           10 * time.Minute,
		PruneStallTimeout:     15 * time.Minute,
		RetiringRetention:     15 * time.Minute,
		ChangelogRetention:    24 * time.Hour,
		ShutdownGrace:         2 * time.Second,
		MaxIndexFields:        1000,
	}
	if c != want {
		t.Errorf("defaults:\n got %+v\nwant %+v", c, want)
	}
}

func TestLoadEnvironmentOverrides(t *testing.T) {
	tokens := filepath.Join(t.TempDir(), "tokens")
	if err := os.WriteFile(tokens, []byte("t1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"SEARCHLIGHT_STORE_URL":                "postgres://sl:pw@db:5432/sl?sslmode=disable",
		"SEARCHLIGHT_LISTEN":                   "0.0.0.0:9000",
		"SEARCHLIGHT_ADMIN_LISTEN":             "127.0.0.1:9001",
		"SEARCHLIGHT_ADVERTISE_ADDRESS":        "node-a.internal:9000",
		"SEARCHLIGHT_NODE_ID":                  "node-a",
		"SEARCHLIGHT_DATA_DIR":                 "/var/lib/searchlight",
		"SEARCHLIGHT_TOKENS_FILE":              tokens,
		"SEARCHLIGHT_CLUSTER_TOKEN":            "s3cret",
		"SEARCHLIGHT_REFRESH_INTERVAL":         "250ms",
		"SEARCHLIGHT_FLUSH_INTERVAL":           "5s",
		"SEARCHLIGHT_MAX_LAG":                  "5s",
		"SEARCHLIGHT_CHANGELOG_POLL_INTERVAL":  "2s",
		"SEARCHLIGHT_MERGE_BUDGET":             "128MB",
		"SEARCHLIGHT_MERGE_THREADS":            "3",
		"SEARCHLIGHT_SEARCH_THREADS":           "7",
		"SEARCHLIGHT_LOG_LEVEL":                "DEBUG",
		"SEARCHLIGHT_PPROF":                    "true",
		"SEARCHLIGHT_SHUTDOWN_TIMEOUT":         "1m",
		"SEARCHLIGHT_MAX_BODY_BYTES":           "8MiB",
		"SEARCHLIGHT_MAX_DOC_BYTES":            "1MiB",
		"SEARCHLIGHT_MAX_BULK_OPS":             "500",
		"SEARCHLIGHT_REQUEST_TIMEOUT":          "5s",
		"SEARCHLIGHT_READ_TIMEOUT":             "20s",
		"SEARCHLIGHT_SEARCH_QUEUE":             "64",
		"SEARCHLIGHT_MAX_INFLIGHT_WRITE_BYTES": "64MiB",
		"SEARCHLIGHT_MAX_INFLIGHT_READ_BYTES":  "32MiB",
		"SEARCHLIGHT_INFLIGHT_AMPLIFICATION":   "4",
		"SEARCHLIGHT_DROP_TIMEOUT":             "1m",
		"SEARCHLIGHT_SHUTDOWN_GRACE":           "0s",
	}
	c, err := Load(nil, envOf(env))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := Config{
		StoreURL:              "postgres://sl:pw@db:5432/sl?sslmode=disable",
		Listen:                "0.0.0.0:9000",
		AdminListen:           "127.0.0.1:9001",
		AdvertiseAddress:      "node-a.internal:9000",
		NodeID:                "node-a",
		DataDir:               "/var/lib/searchlight",
		TokensFile:            tokens,
		ClusterToken:          "s3cret",
		RefreshInterval:       250 * time.Millisecond,
		FlushInterval:         5 * time.Second,
		MaxLag:                5 * time.Second,
		ChangelogPollInterval: 2 * time.Second,
		RemapDebounce:         2 * time.Second,
		HaltRetryBase:         30 * time.Second,
		HaltRetryCap:          10 * time.Minute,
		RebuildRetryCap:       2 * time.Minute,
		MergeBudget:           128e6,
		MergeThreads:          3,
		SearchThreads:         7,
		LogLevel:              slog.LevelDebug,
		Pprof:                 true,
		ShutdownTimeout:       time.Minute,
		MaxBodyBytes:          8 << 20,
		MaxDocBytes:           1 << 20,
		MaxBulkOps:            500,
		RequestTimeout:        5 * time.Second,
		ReadTimeout:           20 * time.Second,
		SearchQueue:           64,

		MaxInflightWriteBytes: 64 << 20,
		MaxInflightReadBytes:  32 << 20,
		InflightAmplification: 4,
		DropTimeout:           time.Minute,
		PruneStallTimeout:     15 * time.Minute,
		RetiringRetention:     15 * time.Minute,
		ChangelogRetention:    24 * time.Hour,
		MaxIndexFields:        1000,
	}
	if c != want {
		t.Errorf("env overrides:\n got %+v\nwant %+v", c, want)
	}
}

func TestLoadFlagsWinOverEnvironment(t *testing.T) {
	env := envOf(map[string]string{
		"SEARCHLIGHT_STORE_URL":        sqliteURL,
		insecure:                       "true",
		"SEARCHLIGHT_REFRESH_INTERVAL": "5s",
		"SEARCHLIGHT_PPROF":            "false",
	})
	c, err := Load([]string{"--refresh_interval=2s", "-pprof", "--listen", "127.0.0.1:7000"}, env)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.RefreshInterval != 2*time.Second || !c.Pprof || c.Listen != "127.0.0.1:7000" {
		t.Errorf("flags did not win: %+v", c)
	}
	if c.AdvertiseAddress != "127.0.0.1:7000" {
		t.Errorf("advertise_address derived from listen = %q", c.AdvertiseAddress)
	}
}

func TestLoadSecretFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(path, []byte("  from-file\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := Load(nil, envOf(map[string]string{
		"SEARCHLIGHT_STORE_URL":          sqliteURL,
		"SEARCHLIGHT_CLUSTER_TOKEN_FILE": path,
		insecure:                         "true",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.ClusterToken != "from-file" {
		t.Errorf("cluster_token = %q, want from-file", c.ClusterToken)
	}

	_, err = Load(nil, envOf(map[string]string{
		"SEARCHLIGHT_STORE_URL":          sqliteURL,
		"SEARCHLIGHT_CLUSTER_TOKEN":      "x",
		"SEARCHLIGHT_CLUSTER_TOKEN_FILE": path,
	}))
	if err == nil || !strings.Contains(err.Error(), "cluster_token") {
		t.Errorf("both token and token file: err = %v, want one naming cluster_token", err)
	}
}

func TestLoadInvalidValuesNameTheSetting(t *testing.T) {
	base := map[string]string{"SEARCHLIGHT_STORE_URL": sqliteURL, insecure: "true"}
	cases := []struct {
		setting string
		env     map[string]string
		args    []string
	}{
		{"store_url", map[string]string{"SEARCHLIGHT_STORE_URL": ""}, nil},
		{"store_url", map[string]string{"SEARCHLIGHT_STORE_URL": "redis://localhost"}, nil},
		{"store_url", map[string]string{"SEARCHLIGHT_STORE_URL": "://nope"}, nil},
		{"listen", map[string]string{"SEARCHLIGHT_LISTEN": "8780"}, nil},
		{"admin_listen", map[string]string{"SEARCHLIGHT_ADMIN_LISTEN": "localhost"}, nil},
		{"admin_listen", map[string]string{"SEARCHLIGHT_LISTEN": ":1", "SEARCHLIGHT_ADMIN_LISTEN": ":1"}, nil},
		{"advertise_address", map[string]string{"SEARCHLIGHT_ADVERTISE_ADDRESS": ":9000"}, nil},
		{"tokens_file", map[string]string{"SEARCHLIGHT_TOKENS_FILE": filepath.Join(t.TempDir(), "missing")}, nil},
		{"refresh_interval", map[string]string{"SEARCHLIGHT_REFRESH_INTERVAL": "soon"}, nil},
		{"refresh_interval", map[string]string{"SEARCHLIGHT_REFRESH_INTERVAL": "0s"}, nil},
		{"flush_interval", map[string]string{"SEARCHLIGHT_FLUSH_INTERVAL": "0s"}, nil},
		{"max_lag", map[string]string{"SEARCHLIGHT_MAX_LAG": "-1s"}, nil},
		{"changelog_poll_interval", map[string]string{"SEARCHLIGHT_CHANGELOG_POLL_INTERVAL": "0s"}, nil},
		{"merge_budget", map[string]string{"SEARCHLIGHT_MERGE_BUDGET": "lots"}, nil},
		{"merge_budget", map[string]string{"SEARCHLIGHT_MERGE_BUDGET": "-5MiB"}, nil},
		{"merge_threads", map[string]string{"SEARCHLIGHT_MERGE_THREADS": "0"}, nil},
		{"search_threads", map[string]string{"SEARCHLIGHT_SEARCH_THREADS": "many"}, nil},
		{"log_level", map[string]string{"SEARCHLIGHT_LOG_LEVEL": "loud"}, nil},
		{"pprof", map[string]string{"SEARCHLIGHT_PPROF": "maybe"}, nil},
		{"shutdown_timeout", map[string]string{"SEARCHLIGHT_SHUTDOWN_TIMEOUT": "0"}, nil},
		{"tokens_file", map[string]string{insecure: "false"}, nil},
		{"insecure_no_auth", map[string]string{insecure: "perhaps"}, nil},
		{"max_body_bytes", map[string]string{"SEARCHLIGHT_MAX_BODY_BYTES": "32MiB"}, nil},
		{"max_body_bytes", map[string]string{"SEARCHLIGHT_MAX_BODY_BYTES": "100"}, nil},
		{"max_doc_bytes", map[string]string{"SEARCHLIGHT_MAX_DOC_BYTES": "17MiB"}, nil},
		{"max_doc_bytes", map[string]string{"SEARCHLIGHT_MAX_DOC_BYTES": "0"}, nil},
		{"max_bulk_ops", map[string]string{"SEARCHLIGHT_MAX_BULK_OPS": "0"}, nil},
		{"request_timeout", map[string]string{"SEARCHLIGHT_REQUEST_TIMEOUT": "0s"}, nil},
		{"read_timeout", map[string]string{"SEARCHLIGHT_READ_TIMEOUT": "never"}, nil},
		{"search_queue", map[string]string{"SEARCHLIGHT_SEARCH_QUEUE": "-1"}, nil},
		{"max_inflight_write_bytes", map[string]string{"SEARCHLIGHT_MAX_INFLIGHT_WRITE_BYTES": "1MiB"}, nil},
		{"max_inflight_read_bytes", map[string]string{"SEARCHLIGHT_MAX_INFLIGHT_READ_BYTES": "lots"}, nil},
		{"max_inflight_write_bytes", map[string]string{"SEARCHLIGHT_INFLIGHT_AMPLIFICATION": "64"}, nil},
		{"inflight_amplification", map[string]string{"SEARCHLIGHT_INFLIGHT_AMPLIFICATION": "0"}, nil},
		{"drop_timeout", map[string]string{"SEARCHLIGHT_DROP_TIMEOUT": "0s"}, nil},
		{"max_index_fields", map[string]string{"SEARCHLIGHT_MAX_INDEX_FIELDS": "0"}, nil},
		{"shutdown_grace", map[string]string{"SEARCHLIGHT_SHUTDOWN_GRACE": "-1s"}, nil},
		{"tls_cert", map[string]string{"SEARCHLIGHT_TLS_KEY": "key.pem"}, nil},
		{"tls_cert", map[string]string{"SEARCHLIGHT_TLS_CERT": filepath.Join(t.TempDir(), "nope.pem"), "SEARCHLIGHT_TLS_KEY": filepath.Join(t.TempDir(), "nope.key")}, nil},
		{"search_threads", nil, []string{"--search_threads=-2"}},
		{"data_dir", nil, []string{"--data_dir="}},
		{"bogus", nil, []string{"--bogus=1"}},
	}
	for _, tc := range cases {
		env := map[string]string{}
		for k, v := range base {
			env[k] = v
		}
		for k, v := range tc.env {
			env[k] = v
		}
		_, err := Load(tc.args, envOf(env))
		if err == nil {
			t.Errorf("%s with env %v args %v: want an error", tc.setting, tc.env, tc.args)
			continue
		}
		if !strings.Contains(err.Error(), tc.setting) {
			t.Errorf("%s: error %q does not name the setting", tc.setting, err)
		}
	}
}

func TestLoadRefusesNoAuthUnlessInsecure(t *testing.T) {
	_, err := Load(nil, envOf(map[string]string{"SEARCHLIGHT_STORE_URL": sqliteURL}))
	if err == nil || !strings.Contains(err.Error(), "insecure_no_auth") {
		t.Errorf("no tokens_file: err = %v, want one naming insecure_no_auth", err)
	}
	c, err := Load([]string{"--insecure_no_auth"}, envOf(map[string]string{"SEARCHLIGHT_STORE_URL": sqliteURL}))
	if err != nil || !c.InsecureNoAuth {
		t.Errorf("--insecure_no_auth: %+v, %v", c, err)
	}
}

// A removed setting is refused by name, as a flag or an environment variable, with what
// replaced it.
func TestLoadRefusesRemovedSettings(t *testing.T) {
	const want = "seq_persist_interval was removed; flush_interval (default 10s) now persists the seq"
	base := map[string]string{"SEARCHLIGHT_STORE_URL": sqliteURL, insecure: "true"}
	if _, err := Load([]string{"--seq_persist_interval=30s"}, envOf(base)); err == nil || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "--seq_persist_interval") {
		t.Errorf("the flag: err = %v, want %q naming the flag", err, want)
	}
	env := maps.Clone(base)
	env["SEARCHLIGHT_SEQ_PERSIST_INTERVAL"] = "30s"
	if _, err := Load(nil, envOf(env)); err == nil || !strings.Contains(err.Error(), want) || !strings.Contains(err.Error(), "SEARCHLIGHT_SEQ_PERSIST_INTERVAL") {
		t.Errorf("the environment variable: err = %v, want %q naming the variable", err, want)
	}
	if _, err := Load(nil, envOf(base)); err != nil {
		t.Errorf("without it: %v", err)
	}
}

func TestLoadReportsEveryError(t *testing.T) {
	_, err := Load(nil, envOf(map[string]string{
		"SEARCHLIGHT_MAX_LAG":   "x",
		"SEARCHLIGHT_LOG_LEVEL": "y",
	}))
	if err == nil {
		t.Fatal("want an error")
	}
	for _, name := range []string{"store_url", "max_lag", "log_level"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("error %q is missing %s", err, name)
		}
	}
}

func TestLoadStoreURLErrorHidesPassword(t *testing.T) {
	_, err := Load(nil, envOf(map[string]string{"SEARCHLIGHT_STORE_URL": "redis://u:hunter2@h/0"}))
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Errorf("err = %v; want an error without the password", err)
	}
}

func TestLoadHelpAndPositionalArgs(t *testing.T) {
	if _, err := Load([]string{"-h"}, envOf(nil)); !errors.Is(err, flag.ErrHelp) {
		t.Errorf("-h: err = %v, want flag.ErrHelp", err)
	}
	if _, err := Load([]string{"serve"}, envOf(map[string]string{"SEARCHLIGHT_STORE_URL": sqliteURL, insecure: "true"})); err == nil {
		t.Error("positional argument: want an error")
	}
}

func TestLogValueRedactsSecrets(t *testing.T) {
	c, err := Load(nil, envOf(map[string]string{
		"SEARCHLIGHT_STORE_URL":     "postgres://sl:hunter2@db/sl",
		"SEARCHLIGHT_CLUSTER_TOKEN": "tops3cret",
		insecure:                    "true",
	}))
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("config", "config", c)
	out := buf.String()
	for _, secret := range []string{"hunter2", "tops3cret"} {
		if strings.Contains(out, secret) {
			t.Errorf("log leaks %q: %s", secret, out)
		}
	}
	for _, want := range []string{`"store_url":"postgres://sl:xxxxx@db/sl"`, `"refresh_interval":"1s"`, `"merge_budget":"64MiB"`} {
		if !strings.Contains(out, want) {
			t.Errorf("log %s is missing %s", out, want)
		}
	}
}

func TestUsageListsEverySetting(t *testing.T) {
	var buf bytes.Buffer
	Usage(&buf)
	for i := range settings {
		if !strings.Contains(buf.String(), "--"+settings[i].name) || !strings.Contains(buf.String(), settings[i].envName()) {
			t.Errorf("usage is missing %s", settings[i].name)
		}
	}
}

func TestParseBytes(t *testing.T) {
	ok := map[string]int64{
		"0": 0, "1048576": 1 << 20, "512KiB": 512 << 10, "64MiB": 64 << 20, "2GiB": 2 << 30,
		"1TiB": 1 << 40, "10KB": 10e3, "5MB": 5e6, "1GB": 1e9, "1TB": 1e12, "7B": 7, "64 MiB": 64 << 20,
	}
	for in, want := range ok {
		got, err := ParseBytes(in)
		if err != nil || got != want {
			t.Errorf("ParseBytes(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "MiB", "1.5GiB", "-1", "1mib", "9999999999TiB", "12XB"} {
		if _, err := ParseBytes(in); err == nil {
			t.Errorf("ParseBytes(%q): want an error", in)
		}
	}
	for n, want := range map[int64]string{0: "0", 1: "1", 1 << 20: "1MiB", 1536: "1536", 3 << 30: "3GiB", 1e6: "1000000"} {
		if got := FormatBytes(n); got != want {
			t.Errorf("FormatBytes(%d) = %q, want %q", n, got, want)
		}
	}
}
