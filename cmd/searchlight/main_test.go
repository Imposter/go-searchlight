package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunRefusesInvalidConfig(t *testing.T) {
	err := run(t.Context(), nil, func(string) string { return "" }, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "store_url") {
		t.Errorf("err = %v, want one naming store_url", err)
	}
}

func TestOneLine(t *testing.T) {
	err := errors.Join(errors.New("store_url: is required"), errors.New("listen: want host:port,\n  got \"x\""))
	if got, want := oneLine(err), `store_url: is required; listen: want host:port,; got "x"`; got != want {
		t.Errorf("oneLine = %q, want %q", got, want)
	}
}

func TestBoundAdvertise(t *testing.T) {
	bound := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 4242}
	for advertise, want := range map[string]string{
		"node-a:0":       "node-a:4242",
		"node-a:8780":    "node-a:8780",
		"10.0.0.7:0":     "10.0.0.7:4242",
		"[fe80::1]:0":    "[fe80::1]:4242",
		"no-port-at-all": "no-port-at-all",
	} {
		if got := boundAdvertise(advertise, bound); got != want {
			t.Errorf("boundAdvertise(%q) = %q, want %q", advertise, got, want)
		}
	}
}

func TestLoopback(t *testing.T) {
	for addr, want := range map[string]bool{
		"127.0.0.1:8781": true, "[::1]:8781": true, "localhost:8781": true,
		":8781": false, "0.0.0.0:8781": false, "10.0.0.7:8781": false, "node-a:8781": false, "junk": false,
	} {
		if got := loopback(addr); got != want {
			t.Errorf("loopback(%q) = %v, want %v", addr, got, want)
		}
	}
}

func TestHealthcheck(t *testing.T) {
	var readyz atomic.Int32
	readyz.Store(http.StatusServiceUnavailable)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/readyz" {
			w.WriteHeader(int(readyz.Load()))
		}
	}))
	_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	env := func(addr string) func(string) string {
		return func(k string) string {
			if k == "SEARCHLIGHT_ADMIN_LISTEN" {
				return addr
			}
			return ""
		}
	}
	unspecified, empty := env("0.0.0.0:"+port), env(":"+port)
	for _, c := range []struct {
		name string
		args []string
		env  func(string) string
		want int
	}{
		{"not ready", nil, unspecified, 1},
		{"live", []string{"--live"}, unspecified, 0},
		{"not ready, empty host", nil, empty, 1},
		{"bad admin_listen", nil, env("no-port"), 1},
		{"unknown flag", []string{"--bogus"}, unspecified, 1},
	} {
		if got := healthcheck(c.args, c.env, io.Discard); got != c.want {
			t.Errorf("%s: healthcheck = %d, want %d", c.name, got, c.want)
		}
	}
	readyz.Store(http.StatusOK)
	if got := healthcheck(nil, empty, io.Discard); got != 0 {
		t.Errorf("ready: healthcheck = %d, want 0", got)
	}
	srv.Close()
	if got := healthcheck(nil, unspecified, io.Discard); got != 1 {
		t.Errorf("stopped: healthcheck = %d, want 1", got)
	}
}

func TestRunStopsCleanlyWhenSignalledDuringStartup(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	dir := t.TempDir()
	env := map[string]string{
		"SEARCHLIGHT_STORE_URL":        "sqlite:///" + filepath.ToSlash(filepath.Join(dir, "searchlight.db")),
		"SEARCHLIGHT_DATA_DIR":         filepath.Join(dir, "data"),
		"SEARCHLIGHT_ADMIN_LISTEN":     "127.0.0.1:0",
		"SEARCHLIGHT_LISTEN":           "127.0.0.1:0",
		"SEARCHLIGHT_INSECURE_NO_AUTH": "true",
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := run(ctx, nil, func(k string) string { return env[k] }, io.Discard); err != nil {
		t.Errorf("run with a signal during startup = %v, want a clean stop", err)
	}
}

// TestRunServesAndStopsWhenCancelled runs a node in process on SQLite: the admin
// listener answers its probes and metrics, the public API serves, and cancelling the
// context stops everything cleanly.
func TestRunServesAndStopsWhenCancelled(t *testing.T) {
	prev := slog.Default()
	t.Cleanup(func() { slog.SetDefault(prev) })
	dir := t.TempDir()
	env := map[string]string{
		"SEARCHLIGHT_STORE_URL":        "sqlite:///" + filepath.ToSlash(filepath.Join(dir, "searchlight.db")),
		"SEARCHLIGHT_DATA_DIR":         filepath.Join(dir, "data"),
		"SEARCHLIGHT_ADMIN_LISTEN":     "127.0.0.1:0",
		"SEARCHLIGHT_LISTEN":           "127.0.0.1:0",
		"SEARCHLIGHT_NODE_ID":          "inproc",
		"SEARCHLIGHT_INSECURE_NO_AUTH": "true",
		"SEARCHLIGHT_SHUTDOWN_GRACE":   "200ms",
		"SEARCHLIGHT_SHUTDOWN_TIMEOUT": "4s",
	}
	logs := newLogWatch()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, nil, func(k string) string { return env[k] }, logs)
		_ = logs.Close()
	}()

	admin := "http://" + logs.address(t, "admin listener serving")
	public := "http://" + logs.address(t, "API listener serving")
	logs.wait(t, "cluster node started")
	for path, want := range map[string]string{
		admin + "/healthz":  `"ok"`,
		admin + "/metrics":  "# TYPE go_goroutines gauge",
		public + "/healthz": `"ok"`,
		public + "/indexes": `"indexes"`,
	} {
		if code, body := get(t, path); code != http.StatusOK || !strings.Contains(body, want) {
			t.Errorf("%s: HTTP %d %s, want 200 with %q", path, code, body, want)
		}
	}
	waitFor(t, 30*time.Second, "admin /readyz to answer 200", func() bool {
		code, _ := get(t, admin+"/readyz")
		return code == http.StatusOK
	})
	adminEnv := func(k string) string {
		if k == "SEARCHLIGHT_ADMIN_LISTEN" {
			return strings.TrimPrefix(admin, "http://")
		}
		return ""
	}
	for _, args := range [][]string{nil, {"--live"}} {
		if code := healthcheck(args, adminEnv, io.Discard); code != 0 {
			t.Errorf("healthcheck %v = %d, want 0", args, code)
		}
	}

	stopped := time.Now()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run did not stop after cancellation")
	}
	if took := time.Since(stopped); took > 200*time.Millisecond+4*time.Second {
		t.Errorf("the shutdown took %v, past shutdown_grace plus shutdown_timeout", took)
	}
	for _, addr := range []string{admin, public} {
		if conn, err := net.DialTimeout("tcp", strings.TrimPrefix(addr, "http://"), time.Second); err == nil {
			_ = conn.Close()
			t.Errorf("%s still accepts connections after shutdown", addr)
		}
	}
}

type logWatch struct {
	*io.PipeWriter
	mu    sync.Mutex
	lines []map[string]any
	raw   strings.Builder
	added chan struct{}
}

func newLogWatch() *logWatch {
	r, w := io.Pipe()
	l := &logWatch{PipeWriter: w, added: make(chan struct{}, 1)}
	go l.collect(r)
	return l
}

func (l *logWatch) collect(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		var m map[string]any
		_ = json.Unmarshal(sc.Bytes(), &m)
		l.mu.Lock()
		l.raw.Write(sc.Bytes())
		l.raw.WriteByte('\n')
		if m != nil {
			l.lines = append(l.lines, m)
		}
		l.mu.Unlock()
		select {
		case l.added <- struct{}{}:
		default:
		}
	}
	_, _ = io.Copy(io.Discard, r)
}

func (l *logWatch) find(msg string) map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.lines {
		if m["msg"] == msg {
			return m
		}
	}
	return nil
}

func (l *logWatch) all(msg string) []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []map[string]any
	for _, m := range l.lines {
		if m["msg"] == msg {
			out = append(out, m)
		}
	}
	return out
}

func (l *logWatch) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.raw.String()
}

func (l *logWatch) wait(t *testing.T, msg string) map[string]any {
	t.Helper()
	deadline := time.After(time.Minute)
	for {
		if m := l.find(msg); m != nil {
			return m
		}
		select {
		case <-l.added:
		case <-deadline:
			t.Fatalf("no %q log line; the log:\n%s", msg, l.text())
		}
	}
}

func (l *logWatch) address(t *testing.T, msg string) string {
	t.Helper()
	addr, ok := l.wait(t, msg)["address"].(string)
	if !ok {
		t.Fatalf("the %q log line names no address", msg)
	}
	return addr
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	return call(t, http.MethodGet, url, "", "")
}

func call(t *testing.T, method, url, token, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(b)
}
