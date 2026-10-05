package slproc

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/testtier"
)

// envOrphanHelper makes the test binary, run as a child of TestNodesDieWithTheirParent,
// launch a node from the binary it names, print the node's admin URL and die without
// stopping it.
const envOrphanHelper = "SLPROC_ORPHAN_HELPER_BIN"

func TestMain(m *testing.M) {
	if bin := os.Getenv(envOrphanHelper); bin != "" {
		orphanHelper(bin)
	}
	os.Exit(m.Run())
}

func orphanHelper(bin string) {
	dir, err := os.MkdirTemp(filepath.Dir(bin), "orphan-")
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(2)
	}
	n, err := Launch(context.Background(), Options{Bin: bin, Dir: dir, NodeID: "orphan"})
	if err != nil {
		fmt.Println("error:", err)
		os.Exit(2)
	}
	fmt.Println("pid:", n.PID())
	fmt.Println("admin:", n.AdminURL)
	os.Exit(1)
}

// TestNodesDieWithTheirParent: a node outlives no process that started it, however that
// process ends (Pdeathsig on Linux, a kill-on-close job object on Windows).
func TestNodesDieWithTheirParent(t *testing.T) {
	testtier.Heavy(t)
	ctx := t.Context()
	dir := t.TempDir()
	bin, err := Build(ctx, dir, "slproc-orphan")
	if err != nil {
		t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, self, "-test.run=^$")
	cmd.Env = append(os.Environ(), envOrphanHelper+"="+bin)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var admin string
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		if rest, ok := strings.CutPrefix(sc.Text(), "admin: "); ok {
			admin = rest
		}
		if rest, ok := strings.CutPrefix(sc.Text(), "pid: "); ok {
			if pid, err := strconv.Atoi(rest); err == nil {
				t.Cleanup(func() {
					if p, err := os.FindProcess(pid); err == nil {
						_ = p.Kill()
					}
				})
			}
		}
		if strings.HasPrefix(sc.Text(), "error: ") {
			t.Fatal(sc.Text())
		}
	}
	_ = cmd.Wait()
	if admin == "" {
		t.Fatal("the helper launched no node")
	}
	deadline := time.Now().Add(30 * time.Second)
	for answers(ctx, admin+"/healthz") {
		if time.Now().After(deadline) {
			t.Fatalf("the node at %s still runs 30 s after the process that started it died", admin)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
