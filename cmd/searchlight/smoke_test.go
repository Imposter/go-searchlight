package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/testtier"
)

const (
	smokeGrace   = 200 * time.Millisecond
	smokeTimeout = 20 * time.Second
)

// TestSmokeBinary builds the searchlight binary and runs it as an operator would, on
// SQLite in a temporary directory: it creates an index, bulk-loads it, searches and
// percolates, then interrupts the process (SIGTERM; CTRL_BREAK on Windows). The process
// must exit 0 within its shutdown budget having written every shard's final manifest at
// or past the last write's seq, and a restarted node must reopen those shards (not
// rebuild them) at that seq and serve the data.
func TestSmokeBinary(t *testing.T) {
	testtier.Heavy(t)
	if ok, why := canInterrupt(); !ok {
		t.Skip(why)
	}
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go tool on PATH to build the binary with")
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "searchlight")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	build := exec.CommandContext(t.Context(), goTool, "build", "-trimpath", "-ldflags", "-X main.version=smoke", "-o", bin, ".")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}

	token := "smoke-token-0123456789abcdef"
	tokens := filepath.Join(dir, "tokens")
	if err := os.WriteFile(tokens, []byte("# the smoke test's one token\n"+token+" write\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(dir, "data")
	args := []string{
		"--store_url=sqlite:///" + filepath.ToSlash(filepath.Join(dir, "searchlight.db")),
		"--data_dir=" + dataDir,
		"--tokens_file=" + tokens,
		"--node_id=smoke",
		"--listen=127.0.0.1:0",
		"--admin_listen=127.0.0.1:0",
		"--shutdown_grace=" + smokeGrace.String(),
		"--shutdown_timeout=" + smokeTimeout.String(),
		"--log_level=debug",
	}

	n := startBinary(t, bin, args)
	if code, body := call(t, http.MethodGet, n.public+"/indexes", "", ""); code != http.StatusUnauthorized {
		t.Errorf("GET /indexes without a token: HTTP %d %s, want 401", code, body)
	}
	n.must(t, http.StatusCreated, http.MethodPut, "/indexes/smoke",
		`{"mapping": {"dynamic": "strict", "fields": {"brand": "keyword", "price": "number", "title": "text"}}, "settings": {"shards": 2}}`)
	lastSeq := seqOf(t, n.must(t, http.StatusOK, http.MethodPut, "/indexes/smoke/queries/cheap",
		`{"query": {"field": "price", "op": "lt", "value": 10}, "meta": {"search_id": 1}}`))
	lastSeq = max(lastSeq, seqOf(t, n.must(t, http.StatusOK, http.MethodPut, "/indexes/smoke/queries/acme",
		`{"query": {"field": "brand", "op": "eq", "value": "Acme"}}`)))

	const docs = 500
	var bulk strings.Builder
	for i := range docs {
		brand := "Acme"
		if i%2 == 1 {
			brand = "Other"
		}
		fmt.Fprintf(&bulk, "{\"upsert\": {\"id\": \"d%03d\"}}\n{\"brand\": %q, \"price\": %d, \"title\": \"item %d\"}\n", i, brand, i, i)
	}
	var br struct {
		Seq    int64 `json:"seq"`
		Errors bool  `json:"errors"`
		Items  []struct {
			Status int `json:"status"`
		} `json:"items"`
	}
	n.decode(t, http.StatusOK, http.MethodPost, "/indexes/smoke/_bulk?refresh=wait_for", bulk.String(), &br)
	if br.Errors || len(br.Items) != docs {
		t.Fatalf("bulk: errors=%v, %d items, want %d without errors", br.Errors, len(br.Items), docs)
	}
	lastSeq = max(lastSeq, br.Seq)

	var sr struct {
		Total struct {
			Value int64 `json:"value"`
		} `json:"total"`
		Hits []hit `json:"hits"`
	}
	n.decode(t, http.StatusOK, http.MethodPost, fmt.Sprintf("/indexes/smoke/_search?wait_for_seq=%d", lastSeq),
		`{"query": {"all": [{"field": "brand", "op": "eq", "value": "acme"}, {"field": "price", "op": "lt", "value": 20}]}, "sort": [{"price": "desc"}], "size": 3, "track_total": true}`, &sr)
	if got := hitIDs(sr.Hits); sr.Total.Value != 10 || !slices.Equal(got, []string{"d018", "d016", "d014"}) {
		t.Errorf("search: total %d, hits %v; want 10 and [d018 d016 d014]", sr.Total.Value, got)
	}

	var pr struct {
		Results []struct {
			ID      string   `json:"id"`
			Found   bool     `json:"found"`
			Queries []string `json:"queries"`
		} `json:"results"`
	}
	n.decode(t, http.StatusOK, http.MethodPost, fmt.Sprintf("/indexes/smoke/_percolate?wait_for_seq=%d", lastSeq),
		`{"docs": [{"brand": "ACME", "price": 5}], "ids": ["d001", "d002"]}`, &pr)
	if got := fmt.Sprint(pr.Results); got != "[{ true [acme cheap]} {d001 true [cheap]} {d002 true [acme cheap]}]" {
		t.Errorf("percolate = %s", got)
	}

	n.stop(t)
	manifests := readManifests(t, dataDir)
	if len(manifests) != 2 {
		t.Fatalf("found %d shard manifests under %s, want 2: %v", len(manifests), dataDir, manifests)
	}
	for path, seq := range manifests {
		if seq < lastSeq {
			t.Errorf("%s covers seq %d, below the last write's %d", path, seq, lastSeq)
		}
	}

	n = startBinary(t, bin, args)
	var shards struct {
		Shards []struct {
			Shard        int    `json:"shard"`
			State        string `json:"state"`
			CommittedSeq int64  `json:"committed_seq"`
		} `json:"shards"`
	}
	waitFor(t, time.Minute, "the reopened shards to serve", func() bool {
		n.decode(t, http.StatusOK, http.MethodGet, "/_cluster/shards", "", &shards)
		serving := 0
		for _, s := range shards.Shards {
			if s.State == "serving" {
				serving++
			}
		}
		return serving == 2
	})
	assertReopened(t, n, lastSeq)
	for _, s := range shards.Shards {
		if s.CommittedSeq < lastSeq {
			t.Errorf("shard %d reopened at committed seq %d, below the last write's %d", s.Shard, s.CommittedSeq, lastSeq)
		}
	}
	var count struct {
		Count int64 `json:"count"`
	}
	n.decode(t, http.StatusOK, http.MethodPost, "/indexes/smoke/_count", `{"query": {"field": "brand", "op": "eq", "value": "acme"}}`, &count)
	if count.Count != docs/2 {
		t.Errorf("after the restart %d documents match, want %d", count.Count, docs/2)
	}
	n.stop(t)
}

type binaryNode struct {
	cmd           *exec.Cmd
	logs          *logWatch
	done          chan struct{}
	exitErr       error
	public, admin string
	token         string
}

func startBinary(t *testing.T, bin string, args []string) *binaryNode {
	t.Helper()
	logs := newLogWatch()
	cmd := exec.Command(bin, args...)
	cmd.Env = slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "SEARCHLIGHT_") })
	cmd.Stderr = logs
	prepareInterrupt(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	n := &binaryNode{cmd: cmd, logs: logs, done: make(chan struct{}), token: "smoke-token-0123456789abcdef"}
	go func() {
		n.exitErr = cmd.Wait()
		_ = logs.Close()
		close(n.done)
	}()
	t.Cleanup(func() {
		select {
		case <-n.done:
		default:
			_ = cmd.Process.Kill()
			<-n.done
		}
	})
	n.admin = "http://" + logs.address(t, "admin listener serving")
	n.public = "http://" + logs.address(t, "API listener serving")
	waitFor(t, time.Minute, "the node to report ready", func() bool {
		select {
		case <-n.done:
			t.Fatalf("the node exited while starting: %v\n%s", n.exitErr, logs.text())
		default:
		}
		code, _ := get(t, n.admin+"/readyz")
		return code == http.StatusOK
	})
	return n
}

func (n *binaryNode) stop(t *testing.T) {
	t.Helper()
	if err := interrupt(n.cmd); err != nil {
		t.Fatalf("interrupt: %v", err)
	}
	select {
	case <-n.done:
		if n.exitErr != nil {
			t.Fatalf("the node exited with %v after the interrupt\n%s", n.exitErr, n.logs.text())
		}
	case <-time.After(smokeGrace + smokeTimeout + 10*time.Second):
		t.Fatalf("the node did not exit within its shutdown budget\n%s", n.logs.text())
	}
	for _, msg := range []string{"searchlight stopping", "cluster node stopped", "searchlight stopped"} {
		if n.logs.find(msg) == nil {
			t.Errorf("no %q log line at shutdown", msg)
		}
	}
}

func (n *binaryNode) must(t *testing.T, want int, method, path, body string) map[string]any {
	t.Helper()
	var m map[string]any
	n.decode(t, want, method, path, body, &m)
	return m
}

func (n *binaryNode) decode(t *testing.T, want int, method, path, body string, v any) {
	t.Helper()
	code, got := call(t, method, n.public+path, n.token, body)
	if code != want {
		t.Fatalf("%s %s: HTTP %d %s, want %d", method, path, code, got, want)
	}
	if err := json.Unmarshal([]byte(got), v); err != nil {
		t.Fatalf("%s %s: %v in %s", method, path, err, got)
	}
}

func seqOf(t *testing.T, m map[string]any) int64 {
	t.Helper()
	seq, ok := m["seq"].(float64)
	if !ok || seq <= 0 {
		t.Fatalf("no seq in %v", m)
	}
	return int64(seq)
}

type hit struct {
	ID string `json:"id"`
}

func hitIDs(hits []hit) []string {
	ids := make([]string, len(hits))
	for i, h := range hits {
		ids[i] = h.ID
	}
	return ids
}

func readManifests(t *testing.T, dataDir string) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	err := filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() != "manifest" {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		header, body, ok := bytes.Cut(raw, []byte("\n"))
		if !ok || !bytes.HasPrefix(header, []byte("SLMANIFEST ")) {
			return fmt.Errorf("%s: no manifest header", path)
		}
		var m struct {
			Seq int64 `json:"seq"`
		}
		if err := json.Unmarshal(body, &m); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		out[path] = m.Seq
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func assertReopened(t *testing.T, n *binaryNode, lastSeq int64) {
	t.Helper()
	opened := n.logs.all("shard opened")
	if len(opened) != 2 {
		t.Errorf("%d shards opened on restart, want 2", len(opened))
	}
	for _, m := range opened {
		seq, _ := m["seq"].(float64)
		docs, _ := m["documents"].(float64)
		if int64(seq) < lastSeq || docs == 0 {
			t.Errorf("a shard reopened at seq %v with %v documents, want its manifest's (seq >= %d, documents)", m["seq"], m["documents"], lastSeq)
		}
	}
	if m := n.logs.find("shard copy must be rebuilt"); m != nil {
		t.Errorf("a copy was rebuilt on restart rather than reopened: %v", m)
	}
	_, metrics := get(t, n.admin+"/metrics")
	for line := range strings.Lines(metrics) {
		if strings.HasPrefix(line, "searchlight_replica_recoveries_total{") {
			t.Errorf("a copy recovered on restart: %s", strings.TrimSpace(line))
		}
	}
}
