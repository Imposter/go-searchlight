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
	"path/filepath"
	"strings"
	"sync"
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
		"SEARCHLIGHT_SHUTDOWN_GRACE":   "0s",
		"SEARCHLIGHT_SHUTDOWN_TIMEOUT": "10s",
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

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("run did not stop after cancellation")
	}
	for _, addr := range []string{admin, public} {
		if conn, err := net.DialTimeout("tcp", strings.TrimPrefix(addr, "http://"), time.Second); err == nil {
			_ = conn.Close()
			t.Errorf("%s still accepts connections after shutdown", addr)
		}
	}
}

// logWatch collects a node's JSON log lines and finds them by message.
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

// find returns the first line logged with msg after the first skip of them, or nil.
func (l *logWatch) find(msg string, skip int) map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.lines {
		if m["msg"] == msg {
			if skip == 0 {
				return m
			}
			skip--
		}
	}
	return nil
}

// text returns everything logged so far.
func (l *logWatch) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.raw.String()
}

// wait returns the first line logged with msg, failing the test after a minute.
func (l *logWatch) wait(t *testing.T, msg string) map[string]any {
	t.Helper()
	return l.waitNth(t, msg, 0)
}

// waitNth returns the line logged with msg after the first skip of them, failing the
// test after a minute.
func (l *logWatch) waitNth(t *testing.T, msg string, skip int) map[string]any {
	t.Helper()
	deadline := time.After(time.Minute)
	for {
		if m := l.find(msg, skip); m != nil {
			return m
		}
		select {
		case <-l.added:
		case <-deadline:
			t.Fatalf("no %q log line; the log:\n%s", msg, l.text())
		}
	}
}

// address returns the address the first line logged with msg names.
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
