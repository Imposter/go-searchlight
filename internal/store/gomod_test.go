package store

import (
	"os"
	"regexp"
	"testing"
)

// The store's drivers decide which Go the module needs; the Global
// Constraint is go 1.25, so a dependency bump must not raise it, nor the
// toolchain line, which picks the Go 1.25 release the binaries are built with.
func TestGoDirective(t *testing.T) {
	b, err := os.ReadFile("../../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^go (\S+)$`).FindSubmatch(b)
	if len(m) < 2 || string(m[1]) != "1.25.0" {
		t.Fatalf("go.mod go directive is %q, want 1.25.0", m)
	}
	if tc := regexp.MustCompile(`(?m)^toolchain (\S+)$`).FindSubmatch(b); tc != nil &&
		!regexp.MustCompile(`^go1\.25\.\d+$`).Match(tc[1]) {
		t.Fatalf("go.mod toolchain is %s, want a Go 1.25 release", tc[1])
	}
}
