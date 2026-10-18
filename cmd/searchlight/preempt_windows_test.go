package main

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

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

// TestWindowsBinaryAsyncPreemptOff builds the binary as the release does (tools/ldflags.sh)
// and as "go install" does (no flags), and checks, by observing the runtime, that each
// runs with async preemption off by default (golang/go#79249) and on when GODEBUG says
// asyncpreemptoff=0. The plain build gets there by starting itself again, which must
// pass on the child's exit code and output.
func TestWindowsBinaryAsyncPreemptOff(t *testing.T) {
	testtier.Heavy(t)
	goTool, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go tool on PATH to build the binary with")
	}
	sh, err := exec.LookPath("sh")
	if err != nil {
		if os.Getenv("CI") != "" {
			t.Fatal("no sh on PATH to run tools/ldflags.sh with")
		}
		t.Skip("no sh on PATH to run tools/ldflags.sh with")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	ldflags := exec.CommandContext(t.Context(), sh, "tools/ldflags.sh", "preempt")
	ldflags.Dir = root
	out, err := ldflags.Output()
	if err != nil {
		t.Fatalf("tools/ldflags.sh: %v", err)
	}
	flags := strings.TrimSpace(string(out))
	if !strings.Contains(flags, "asyncpreemptoff=1") {
		t.Fatalf("tools/ldflags.sh gave %q, without asyncpreemptoff=1", flags)
	}
	dir := t.TempDir()
	build := func(name string, args ...string) string {
		bin := filepath.Join(dir, name+".exe")
		args = append(append([]string{"build", "-trimpath"}, args...), "-o", bin, "./cmd/searchlight")
		cmd := exec.CommandContext(t.Context(), goTool, args...)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("go build %s: %v\n%s", name, err, out)
		}
		return bin
	}
	release := build("release", "-ldflags", flags)
	plain := build("plain")

	env := func(extra ...string) []string {
		var e []string
		for _, kv := range os.Environ() {
			if k, _, _ := strings.Cut(kv, "="); !strings.EqualFold(k, "GODEBUG") && k != envTestPreemptProbe {
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
			cmd.Env = env(envTestPreemptProbe + "=1")
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
