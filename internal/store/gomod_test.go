package store

import (
	"os"
	"regexp"
	"testing"
)

// The store's drivers decide which Go the module needs; the Global
// Constraint is go 1.25, so a dependency bump must not raise it.
func TestGoDirective(t *testing.T) {
	b, err := os.ReadFile("../../go.mod")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^go (\S+)$`).FindSubmatch(b)
	if m == nil || string(m[1]) != "1.25.0" {
		t.Fatalf("go.mod go directive is %q, want 1.25.0", m)
	}
	if regexp.MustCompile(`(?m)^toolchain `).Match(b) {
		t.Fatal("go.mod has a toolchain line")
	}
}
