package analysis

import (
	"encoding/json"
	"math/big"
	"strings"
	"testing"
	"time"
)

// TestHugeIntegerLiteralIsFast: a multi-megabyte integer literal is refused at once,
// not parsed by big.Int in superlinear time.
func TestHugeIntegerLiteralIsFast(t *testing.T) {
	for _, lit := range []string{"1" + strings.Repeat("0", 4_000_000), "-" + strings.Repeat("9", 4_000_000)} {
		start := time.Now()
		if _, ok := Number(json.Number(lit)); ok {
			t.Fatal("a 4M-digit integer is a number")
		}
		if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
			t.Fatalf("Number of a 4M-digit literal took %v", elapsed)
		}
	}
	huge := new(big.Int).Lsh(big.NewInt(1), 4_000_000)
	start := time.Now()
	if _, ok := Number(huge); ok || time.Since(start) > 100*time.Millisecond {
		t.Fatalf("Number(2**4e6) = %v after %v", ok, time.Since(start))
	}
	// 309 digits can still be a float64; 310 cannot.
	if _, ok := Number(json.Number("1" + strings.Repeat("0", 308))); !ok {
		t.Fatal("1e308 written out is no number")
	}
	if _, ok := Number(json.Number("1" + strings.Repeat("0", 309))); ok {
		t.Fatal("1e309 written out is a number")
	}
}
