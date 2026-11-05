package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"

	"github.com/Imposter/go-searchlight/internal/testtier"
)

func TestGodebugSets(t *testing.T) {
	for _, c := range []struct {
		godebug string
		want    bool
	}{
		{"", false},
		{"asyncpreemptoff=1", true},
		{"asyncpreemptoff=0", true},
		{"gctrace=1,asyncpreemptoff=0", true},
		{"asyncpreemptoff", false},
		{"xasyncpreemptoff=1", false},
		{"gctrace=1", false},
	} {
		if got := godebugSets(c.godebug, "asyncpreemptoff"); got != c.want {
			t.Errorf("godebugSets(%q) = %v, want %v", c.godebug, got, c.want)
		}
	}
}

// buildSearchlight builds cmd/searchlight into dir as name.exe with the go build flags
// args, as the release does (CGO_ENABLED=0, -trimpath).
func buildSearchlight(t *testing.T, dir, name string, args ...string) string {
	t.Helper()
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go tool on PATH to build the binary with")
	}
	bin := filepath.Join(dir, name+".exe")
	args = append(append([]string{"build", "-trimpath"}, args...), "-o", bin, "./cmd/searchlight")
	cmd := exec.CommandContext(t.Context(), goTool, args...)
	cmd.Dir = repoRoot(t)
	cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build %s: %v\n%s", name, err, out)
	}
	return bin
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// TestWindowsBinaryAsyncPreemptOff builds the binary as the release does (tools/ldflags.sh)
// and as "go install" does (no flags), and checks, by observing the runtime, that each
// runs with async preemption off by default (golang/go#79249) and on when GODEBUG says
// asyncpreemptoff=0. The plain build gets there by starting itself again, which must
// pass on the child's exit code and output.
func TestWindowsBinaryAsyncPreemptOff(t *testing.T) {
	testtier.Heavy(t)
	sh, err := exec.LookPath("sh")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("no sh on PATH to run tools/ldflags.sh with")
		}
		t.Skip("no sh on PATH to run tools/ldflags.sh with")
	}
	ldflags := exec.CommandContext(t.Context(), sh, "tools/ldflags.sh", "preempt")
	ldflags.Dir = repoRoot(t)
	out, err := ldflags.Output()
	if err != nil {
		t.Fatalf("tools/ldflags.sh: %v", err)
	}
	flags := strings.TrimSpace(string(out))
	if !strings.Contains(flags, "asyncpreemptoff=1") {
		t.Fatalf("tools/ldflags.sh gave %q, without asyncpreemptoff=1", flags)
	}
	dir := t.TempDir()
	release := buildSearchlight(t, dir, "release", "-ldflags", flags)
	plain := buildSearchlight(t, dir, "plain")

	env := func(extra ...string) []string {
		var e []string
		for _, kv := range os.Environ() {
			if k, _, _ := strings.Cut(kv, "="); !strings.EqualFold(k, "GODEBUG") && k != envDiagAsyncPreempt {
				e = append(e, kv)
			}
		}
		return append(e, extra...)
	}
	for _, bin := range []string{release, plain} {
		for _, c := range []struct {
			godebug, want string
		}{
			{"", "asyncpreemptoff=1"},
			{"gctrace=0", "asyncpreemptoff=1"},
			{"asyncpreemptoff=0", "asyncpreemptoff=0"},
		} {
			cmd := exec.CommandContext(t.Context(), bin)
			cmd.Env = env(envDiagAsyncPreempt + "=1")
			if c.godebug != "" {
				cmd.Env = append(cmd.Env, "GODEBUG="+c.godebug)
			}
			out, err := cmd.CombinedOutput()
			if got := strings.TrimSpace(string(out)); err != nil || got != c.want {
				t.Errorf("%s with GODEBUG=%q: %v, %q; want %q", filepath.Base(bin), c.godebug, err, got, c.want)
			}
		}
		cmd := exec.CommandContext(t.Context(), bin, "version", "extra")
		cmd.Env = env()
		out, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(string(out), "takes no arguments") {
			t.Errorf("%s version extra: %v, %q; want exit status 1 and the refusal", filepath.Base(bin), err, out)
		}
	}
}

// TestWindowsRerun runs a node built without tools/ldflags.sh's flags, which starts itself
// again. Sharing this test's console, a CTRL_BREAK to its process group must stop the
// node cleanly. As a detached process, as a service host might start it, it has no
// console to share: the stop the parent passes on (its stop event, set here as the
// parent sets it on a signal) must stop the node cleanly, and the parent must exit with
// the node's status; killing the parent must end the node.
func TestWindowsRerun(t *testing.T) {
	testtier.Heavy(t)
	dir := t.TempDir()
	bin := buildSearchlight(t, dir, "plain")
	t.Setenv("GODEBUG", "")
	detached := func(cmd *exec.Cmd) {
		cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: windows.DETACHED_PROCESS}
	}
	args := func(name string) []string {
		return []string{
			"--store_url=sqlite:///" + filepath.ToSlash(filepath.Join(dir, name+".db")),
			"--data_dir=" + filepath.Join(dir, name),
			"--insecure_no_auth",
			"--node_id=" + name,
			"--listen=127.0.0.1:0",
			"--admin_listen=127.0.0.1:0",
			"--shutdown_grace=" + smokeGrace.String(),
		}
	}

	if ok, why := canInterrupt(); ok {
		n := startBinary(t, bin, args("console"))
		if childOf(t, n.cmd.Process.Pid) == 0 {
			t.Fatal("the plain build did not start itself again")
		}
		n.stop(t)
	} else {
		t.Log(why)
	}

	n := startBinaryWith(t, bin, args("stopped"), detached)
	if childOf(t, n.cmd.Process.Pid) == 0 {
		t.Fatal("the plain build did not start itself again")
	}
	name := fmt.Sprintf(`Local\searchlight-stop-%d`, n.cmd.Process.Pid)
	event, err := windows.OpenEvent(windows.EVENT_MODIFY_STATE, false, windows.StringToUTF16Ptr(name))
	if err != nil {
		t.Fatalf("open the parent's stop event %s: %v", name, err)
	}
	defer windows.CloseHandle(event) //nolint:errcheck // a handle only used here
	if err := windows.SetEvent(event); err != nil {
		t.Fatal(err)
	}
	select {
	case <-n.done:
		if n.exitErr != nil {
			t.Fatalf("the node exited with %v after its parent's stop\n%s", n.exitErr, n.logs.text())
		}
	case <-time.After(smokeGrace + 30*time.Second):
		t.Fatalf("the node did not stop on its parent's stop event\n%s", n.logs.text())
	}
	if n.logs.find("searchlight stopped") == nil {
		t.Errorf("no \"searchlight stopped\" after the parent's stop\n%s", n.logs.text())
	}

	n = startBinaryWith(t, bin, args("killed"), detached)
	child := childOf(t, n.cmd.Process.Pid)
	h, err := windows.OpenProcess(windows.SYNCHRONIZE, false, child)
	if err != nil {
		t.Fatalf("open the node's process: %v", err)
	}
	defer windows.CloseHandle(h) //nolint:errcheck // a handle only used here
	if err := n.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if ev, err := windows.WaitForSingleObject(h, 10_000); err != nil || ev != windows.WAIT_OBJECT_0 {
		t.Fatalf("the node outlived its killed parent: %v, %#x", err, ev)
	}
}

// childOf returns the process id of a child of the process pid, or 0 when it has none.
func childOf(t *testing.T, pid int) uint32 {
	t.Helper()
	snap, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPPROCESS, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer windows.CloseHandle(snap) //nolint:errcheck // a handle only used here
	entry := windows.ProcessEntry32{Size: uint32(unsafe.Sizeof(windows.ProcessEntry32{}))}
	for err = windows.Process32First(snap, &entry); err == nil; err = windows.Process32Next(snap, &entry) {
		if entry.ParentProcessID == uint32(pid) {
			return entry.ProcessID
		}
	}
	return 0
}
