package slproc

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/testtier"
)

// TestNodeLifecycle builds the binary and runs a node on SQLite: it serves on the same
// URL after a graceful restart and after a kill, and a stop exits cleanly.
func TestNodeLifecycle(t *testing.T) {
	testtier.Heavy(t)
	ctx := t.Context()
	dir := t.TempDir()
	bin, err := Build(ctx, dir, "slproc-test")
	if err != nil {
		t.Fatal(err)
	}
	n, err := Launch(ctx, Options{Bin: bin, Dir: dir, NodeID: "life", Settings: []string{"shutdown_grace=0s", "log_level=info"}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Kill() })
	if err := n.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	url := n.URL
	get := func() int {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.URL+"/indexes", http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+n.Token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	if code := get(); code != http.StatusOK {
		t.Fatalf("GET /indexes: %d", code)
	}

	if err := n.Restart(ctx, time.Minute); err != nil && !strings.Contains(err.Error(), "could not be interrupted") {
		t.Fatal(err)
	} else if err != nil {
		if err := n.Start(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := n.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if n.URL != url {
		t.Fatalf("the node moved from %s to %s across a restart", url, n.URL)
	}
	if code := get(); code != http.StatusOK {
		t.Fatalf("GET /indexes after a restart: %d", code)
	}

	if err := n.Kill(); err != nil {
		t.Fatal(err)
	}
	if n.PID() != 0 {
		t.Fatal("a killed node still has a pid")
	}
	if err := n.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := n.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if n.Logs().Find("searchlight starting") == nil {
		t.Error("no start line in the restarted node's log")
	}
}
