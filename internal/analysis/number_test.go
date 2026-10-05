package analysis

import (
	"encoding/json"
	"math/big"
	"strconv"
	"strings"
	"testing"
)

// TestHugeIntegerLiteralIsFast: a multi-megabyte integer literal is refused at once,
// not parsed by big.Int in superlinear time. The proof is the allocations, not a
// stopwatch: the refusal allocates no more than strconv's range error (its copy of
// the literal), where a big.Int parse allocates its words and conversion buffers many
// times over. BenchmarkHugeIntegerLiteral times it.
func TestHugeIntegerLiteralIsFast(t *testing.T) {
	for _, lit := range []string{"1" + strings.Repeat("0", 4_000_000), "-" + strings.Repeat("9", 4_000_000)} {
		var ok bool
		allocs := testing.AllocsPerRun(3, func() { _, ok = Number(json.Number(lit)) })
		if ok {
			t.Fatal("a 4M-digit integer is a number")
		}
		rangeErr := testing.AllocsPerRun(3, func() { _, _ = strconv.ParseInt(lit, 10, 64) })
		if allocs > rangeErr {
			t.Fatalf("Number of a 4M-digit literal made %.0f allocations, strconv's range error %.0f: it was parsed", allocs, rangeErr)
		}
	}
	huge := new(big.Int).Lsh(big.NewInt(1), 4_000_000)
	var ok bool
	if allocs := testing.AllocsPerRun(3, func() { _, ok = Number(huge) }); ok || allocs > 0 {
		t.Fatalf("Number(2**4e6) = %v with %.0f allocations", ok, allocs)
	}
	// 309 digits can still be a float64; 310 cannot.
	if _, ok := Number(json.Number("1" + strings.Repeat("0", 308))); !ok {
		t.Fatal("1e308 written out is no number")
	}
	if _, ok := Number(json.Number("1" + strings.Repeat("0", 309))); ok {
		t.Fatal("1e309 written out is a number")
	}
}

func BenchmarkHugeIntegerLiteral(b *testing.B) {
	lit := json.Number("1" + strings.Repeat("0", 4_000_000))
	b.ReportAllocs()
	for b.Loop() {
		if _, ok := Number(lit); ok {
			b.Fatal("a 4M-digit integer is a number")
		}
	}
}
