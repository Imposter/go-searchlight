// Package slproc runs the searchlight binary (cmd/searchlight) as a child process, as
// an operator would: the benchmark (bench/cmd/slbench) and the chaos suite
// (test/chaos) drive real nodes through it rather than an engine wired in process.
//
// A [Node] is one node: its data directory, its tokens file and, unless a store URL is
// given, a SQLite database beside them. It listens on loopback ports it picks at its
// first start and keeps across restarts, so its URL never changes. It is stopped
// gracefully (SIGTERM; CTRL_BREAK on Windows, which needs a console) or killed outright
// (SIGKILL; TerminateProcess on Windows), and started again on the same files. Its
// JSON log lines are kept for the caller to wait on or inspect. A node never outlives
// the process that started it: Linux kills it when that process dies (Pdeathsig), and
// on Windows it belongs to a job object that kills it then.
package slproc

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
)

// Build compiles cmd/searchlight into dir and returns the binary's path. It runs the go
// tool on PATH from the current directory, which must be inside this module.
func Build(ctx context.Context, dir, version string) (string, error) {
	goTool, err := exec.LookPath("go")
	if err != nil {
		return "", fmt.Errorf("slproc: no go tool on PATH to build searchlight with: %w", err)
	}
	bin := filepath.Join(dir, "searchlight")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	cmd := exec.CommandContext(ctx, goTool, "build", "-trimpath", "-ldflags", "-X main.version="+version, "-o", bin,
		"github.com/Imposter/go-searchlight/cmd/searchlight")
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("slproc: go build: %w\n%s", err, out)
	}
	return bin, nil
}

// NewToken returns a random API token.
func NewToken() string {
	b := make([]byte, 24)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Options configure a [Node].
type Options struct {
	// Bin is the searchlight binary.
	Bin string
	// Dir holds the node's files: data_dir (Dir/data), the tokens file and, without
	// StoreURL, the SQLite database (Dir/db/searchlight.db).
	Dir string
	// StoreURL is the node's store_url; empty means SQLite in Dir.
	StoreURL string
	// NodeID is the node's node_id; empty means "node".
	NodeID string
	// Token is the API's one read-write token; empty generates one.
	Token string
	// ClusterToken, when set, opens the peer API (a cluster).
	ClusterToken string
	// Settings are further node settings, each "name=value" (passed as --name=value).
	Settings []string
	// Log, when set, receives the node's standard error (its JSON log) as it comes.
	Log io.Writer
	// ReadyTimeout bounds waiting for a start; 0 means 5 minutes.
	ReadyTimeout time.Duration
	// Env are further environment variables for the node, each "NAME=value", set
	// after the parent's SEARCHLIGHT_* variables are removed (test-only knobs such
	// as SEARCHLIGHT_TEST_HOLD_RECOVERY).
	Env []string
}

// Node is a searchlight child process.
type Node struct {
	// URL is the public API's base URL, and AdminURL the admin listener's.
	URL, AdminURL string
	// Token is the API bearer token.
	Token string

	opts       Options
	listen     string
	admin      string
	mu         sync.Mutex
	cmd        *exec.Cmd
	done       chan struct{}
	exitErr    error
	logs       *Logs
	tokensFile string
}

// Launch starts a node and returns once its admin listener answers, before it is ready
// (it may still be opening its store, joining the cluster or recovering its copies):
// [Node.WaitReady] waits for that.
func Launch(ctx context.Context, o Options) (*Node, error) {
	if o.Bin == "" || o.Dir == "" {
		return nil, errors.New("slproc: Options.Bin and Options.Dir are required")
	}
	dir, err := filepath.Abs(o.Dir)
	if err != nil {
		return nil, err
	}
	o.Dir = dir
	if o.NodeID == "" {
		o.NodeID = "node"
	}
	if o.Token == "" {
		o.Token = NewToken()
	}
	if o.ReadyTimeout <= 0 {
		o.ReadyTimeout = 5 * time.Minute
	}
	if err := os.MkdirAll(filepath.Join(o.Dir, "data"), 0o750); err != nil {
		return nil, err
	}
	if o.StoreURL == "" {
		if err := os.MkdirAll(filepath.Join(o.Dir, "db"), 0o750); err != nil {
			return nil, err
		}
		o.StoreURL = SQLiteURL(filepath.Join(o.Dir, "db", "searchlight.db"))
	}
	listen, err := freeAddress(ctx)
	if err != nil {
		return nil, err
	}
	admin, err := freeAddress(ctx)
	if err != nil {
		return nil, err
	}
	n := &Node{
		opts: o, Token: o.Token, listen: listen, admin: admin, tokensFile: filepath.Join(o.Dir, "tokens"),
		URL: "http://" + listen, AdminURL: "http://" + admin,
	}
	if err := os.WriteFile(n.tokensFile, []byte(o.Token+" write\n"), 0o600); err != nil {
		return nil, err
	}
	if err := n.Start(ctx); err != nil {
		return nil, err
	}
	return n, nil
}

// freeAddress is a loopback address with a port nothing listens on now. The port is
// chosen once, at a node's launch, and kept across its restarts.
func freeAddress(ctx context.Context) (string, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := ln.Addr().String()
	return addr, ln.Close()
}

// SQLiteURL is the store_url of a SQLite database file.
func SQLiteURL(path string) string {
	p := filepath.ToSlash(path)
	if strings.HasPrefix(p, "/") {
		return "sqlite://" + p
	}
	return "sqlite:///" + p
}

// DiskPaths are the directories holding the node's durable state: data_dir and, on
// SQLite, the database's directory.
func (n *Node) DiskPaths() []string {
	paths := []string{filepath.Join(n.opts.Dir, "data")}
	if strings.HasPrefix(n.opts.StoreURL, "sqlite:") {
		paths = append(paths, filepath.Join(n.opts.Dir, "db"))
	}
	return paths
}

// DataDir is the node's data_dir.
func (n *Node) DataDir() string { return filepath.Join(n.opts.Dir, "data") }

// NodeID is the node's node_id.
func (n *Node) NodeID() string { return n.opts.NodeID }

// args are the node's command line.
func (n *Node) args() []string {
	args := []string{
		"--store_url=" + n.opts.StoreURL,
		"--data_dir=" + n.DataDir(),
		"--tokens_file=" + n.tokensFile,
		"--node_id=" + n.opts.NodeID,
		"--listen=" + n.listen,
		"--admin_listen=" + n.admin,
	}
	if n.opts.ClusterToken != "" {
		args = append(args, "--cluster_token="+n.opts.ClusterToken)
	}
	for _, s := range n.opts.Settings {
		args = append(args, "--"+strings.TrimPrefix(s, "--"))
	}
	return args
}

// Start starts the node again after a stop or a kill, on the same files and ports, and
// returns once its admin listener answers. A node that exits or does not answer within
// ReadyTimeout is killed, and Start fails.
func (n *Node) Start(ctx context.Context) error {
	n.mu.Lock()
	if n.cmd != nil {
		select {
		case <-n.done:
		default:
			n.mu.Unlock()
			return errors.New("slproc: the node is running")
		}
	}
	logs := newLogs(n.opts.Log)
	cmd := exec.Command(n.opts.Bin, n.args()...) //nolint:gosec,noctx // the binary under test, started on purpose and outliving ctx
	cmd.Env = append(slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "SEARCHLIGHT_") }), n.opts.Env...)
	cmd.Stderr = logs
	cmd.Stdout = io.Discard
	prepareInterrupt(cmd)
	if err := cmd.Start(); err != nil {
		n.mu.Unlock()
		return fmt.Errorf("slproc: starting %s: %w", n.opts.Bin, err)
	}
	done := make(chan struct{})
	n.cmd, n.done, n.logs, n.exitErr = cmd, done, logs, nil
	n.mu.Unlock()
	go func() {
		err := cmd.Wait()
		_ = logs.Close()
		n.mu.Lock()
		n.exitErr = err
		n.mu.Unlock()
		close(done)
	}()
	if err := adoptChild(cmd); err != nil {
		_ = n.Kill()
		return err
	}
	if err := n.waitFor(ctx, "/healthz"); err != nil {
		_ = n.Kill()
		return err
	}
	return nil
}

// waitFor waits until the admin listener answers path with 200.
func (n *Node) waitFor(ctx context.Context, path string) error {
	ctx, cancel := context.WithTimeout(ctx, n.opts.ReadyTimeout)
	defer cancel()
	for {
		select {
		case <-n.Exited():
			return fmt.Errorf("slproc: %s exited while starting: %w\n%s", n.opts.NodeID, n.ExitErr(), n.Logs().Tail(40))
		default:
		}
		if answers(ctx, n.AdminURL+path) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("slproc: %s does not answer %s: %w\n%s", n.opts.NodeID, path, ctx.Err(), n.Logs().Tail(40))
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// WaitReady waits until the node's /readyz answers 200.
func (n *Node) WaitReady(ctx context.Context) error { return n.waitFor(ctx, "/readyz") }

// Ready reports whether the admin listener at adminURL answers /readyz with 200.
func Ready(ctx context.Context, adminURL string) bool { return answers(ctx, adminURL+"/readyz") }

// answers reports whether url answers a GET with 200 within 2 s.
func answers(ctx context.Context, url string) bool {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// Exited is closed once the process has exited.
func (n *Node) Exited() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.done
}

// ExitErr is how the process exited (nil for exit code 0); valid once Exited is
// closed.
func (n *Node) ExitErr() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.exitErr
}

// PID is the running process's id (0 once it has exited).
func (n *Node) PID() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd == nil || n.cmd.Process == nil {
		return 0
	}
	select {
	case <-n.done:
		return 0
	default:
		return n.cmd.Process.Pid
	}
}

// Logs are the current process's log lines.
func (n *Node) Logs() *Logs {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.logs
}

// Stop stops the node gracefully, as an operator's SIGTERM does, and waits for it to
// exit within timeout. A node that cannot be interrupted (Windows without a console)
// or that outlives timeout is killed, and Stop reports it. A non-zero exit is an error.
func (n *Node) Stop(timeout time.Duration) error {
	n.mu.Lock()
	cmd, done := n.cmd, n.done
	n.mu.Unlock()
	if cmd == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	default:
	}
	if err := interrupt(cmd); err != nil {
		_ = n.Kill()
		return fmt.Errorf("slproc: %s could not be interrupted, so it was killed: %w", n.opts.NodeID, err)
	}
	select {
	case <-done:
	case <-time.After(timeout):
		_ = n.Kill()
		return fmt.Errorf("slproc: %s did not stop within %s, so it was killed\n%s", n.opts.NodeID, timeout, n.Logs().Tail(40))
	}
	if err := n.ExitErr(); err != nil {
		return fmt.Errorf("slproc: %s exited with %w after the interrupt\n%s", n.opts.NodeID, err, n.Logs().Tail(40))
	}
	return nil
}

// Kill ends the process at once, as a crash would (SIGKILL; TerminateProcess on
// Windows), and waits for it to exit.
func (n *Node) Kill() error {
	n.mu.Lock()
	cmd, done := n.cmd, n.done
	n.mu.Unlock()
	if cmd == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	default:
	}
	err := cmd.Process.Kill()
	<-done
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}

// UseBinary makes the node's next Start run bin: an upgrade, once the node is stopped.
func (n *Node) UseBinary(bin string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.opts.Bin = bin
}

// Restart stops the node gracefully within timeout and starts it again, returning
// once its admin listener answers.
func (n *Node) Restart(ctx context.Context, timeout time.Duration) error {
	if err := n.Stop(timeout); err != nil {
		return err
	}
	return n.Start(ctx)
}

// Logs collects a process's JSON log lines: every line but the access log's ("request
// served", which Tail still shows), up to maxLines, the oldest dropped past it.
type Logs struct {
	*io.PipeWriter
	mu    sync.Mutex
	lines []map[string]any
	raw   []string
	added chan struct{}
}

// maxRawLines bounds the raw lines kept for Tail, and maxLines the parsed ones kept for
// Find, All and Wait.
const (
	maxRawLines = 2000
	maxLines    = 100_000
)

func newLogs(tee io.Writer) *Logs {
	r, w := io.Pipe()
	l := &Logs{PipeWriter: w, added: make(chan struct{})}
	go l.collect(r, tee)
	return l
}

func (l *Logs) collect(r io.Reader, tee io.Writer) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if tee != nil {
			_, _ = tee.Write(append(slices.Clone(line), '\n'))
		}
		var m map[string]any
		_ = json.Unmarshal(line, &m)
		l.mu.Lock()
		l.raw = append(l.raw, string(line))
		if len(l.raw) > maxRawLines {
			l.raw = slices.Delete(l.raw, 0, len(l.raw)-maxRawLines)
		}
		if m != nil && m["msg"] != "request served" {
			l.lines = append(l.lines, m)
			if len(l.lines) > maxLines {
				l.lines = slices.Delete(l.lines, 0, len(l.lines)-maxLines)
			}
		}
		close(l.added)
		l.added = make(chan struct{})
		l.mu.Unlock()
	}
	_, _ = io.Copy(io.Discard, r)
}

// Find returns the first line whose msg is msg, or nil.
func (l *Logs) Find(msg string) map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, m := range l.lines {
		if m["msg"] == msg {
			return m
		}
	}
	return nil
}

// All returns every line whose msg is msg.
func (l *Logs) All(msg string) []map[string]any {
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

// Wait waits for a line whose msg is msg, until ctx ends or stop is closed.
func (l *Logs) Wait(ctx context.Context, msg string, stop <-chan struct{}) (map[string]any, error) {
	for {
		l.mu.Lock()
		added := l.added
		l.mu.Unlock()
		if m := l.Find(msg); m != nil {
			return m, nil
		}
		select {
		case <-added:
		case <-stop:
			if m := l.Find(msg); m != nil {
				return m, nil
			}
			return nil, errors.New("the process exited")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Tail returns the last k raw lines.
func (l *Logs) Tail(k int) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.raw[max(0, len(l.raw)-k):], "\n")
}

// TailExcept returns the last k raw lines whose msg is none of msgs (the access log's
// "request served", say).
func (l *Logs) TailExcept(k int, msgs ...string) string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for i := len(l.raw) - 1; i >= 0 && len(out) < k; i-- {
		if !slices.ContainsFunc(msgs, func(m string) bool { return strings.Contains(l.raw[i], `"msg":"`+m+`"`) }) {
			out = append(out, l.raw[i])
		}
	}
	slices.Reverse(out)
	return strings.Join(out, "\n")
}
