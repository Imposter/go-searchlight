package analysis

import (
	"testing"

	"golang.org/x/text/unicode/norm"
)

func runes(rs ...rune) string { return string(rs) }

// TestNFKC pins NFKC against Python's unicodedata.normalize("NFKC"), including the
// supplementary-plane cases x/text's NFKC gets wrong.
func TestNFKC(t *testing.T) {
	tests := []struct {
		name     string
		in, want string
		xtextBug bool // x/text's own NFKC differs from Python here
	}{
		{"CJK Ext B starter before an acute", runes(0x20041, 0x301), runes(0x20041, 0x301), true},
		{"Linear B starter before an acute", runes(0x10041, 0x301), runes(0x10041, 0x301), true},
		{"CJK Ext G starter before a circumflex", runes(0x30065, 0x302), runes(0x30065, 0x302), true},
		{"emoji before an acute", runes(0x1F600, 0x301), runes(0x1F600, 0x301), false},
		{"Kaithi supplementary pair composes", runes(0x11099, 0x110BA), runes(0x1109A), false},
		{"excluded supplementary composite decomposes", runes(0x1D15E), runes(0x1D157, 0x1D165), false},
		{"mark blocked by a supplementary mark of higher class", runes('a', 0x1D165, 0x301), runes(0xE1, 0x1D165), false},
		{"Hangul LVT", runes(0x1100, 0x1161, 0x11A8), runes(0xAC01), false},
		{"canonical reordering before composition", runes('e', 0x301, 0x327), runes(0x229, 0x301), false},
		{"full-width letter then acute", runes(0xFF21, 0x301), runes(0xC1), false},
		{"ASCII", "plain text", "plain text", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := NFKC(tc.in); got != tc.want {
				t.Errorf("NFKC(%+q) = %+q, want %+q", tc.in, got, tc.want)
			}
			if tc.xtextBug && norm.NFKC.String(tc.in) == tc.want {
				t.Logf("x/text's NFKC now agrees on %+q: the workaround may be retired", tc.in)
			}
		})
	}
}

func BenchmarkNFKC(b *testing.B) {
	ascii := "Nike Air Max 90 Running Shoe"
	mixed := "Stra" + runes(0xDF) + "e caf" + runes(0xE9) + " " + runes(0x1F600) + " " + runes(0x20041, 0x301)
	b.Run("ascii", func(b *testing.B) {
		for b.Loop() {
			_ = NFKC(ascii)
		}
	})
	b.Run("supplementary", func(b *testing.B) {
		for b.Loop() {
			_ = NFKC(mixed)
		}
	})
}
