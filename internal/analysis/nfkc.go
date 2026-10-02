package analysis

import (
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// NFKC returns s in Unicode normalization form KC, as Python's
// unicodedata.normalize("NFKC", s) does.
//
// golang.org/x/text's NFKC (and NFC) composes a supplementary-plane starter followed by a
// combining mark as if it were the BMP character with the same low 16 bits
// ("\U00020041́" becomes "Á"). So a string that holds a supplementary character next
// to a non-starter, or one that decomposes, is normalized here as NFKD (which x/text gets
// right) followed by canonical composition over the pairs in tables.go, generated from
// Python. Any other string takes x/text's NFKC as it is.
func NFKC(s string) string {
	if nfkcSafe(s) {
		return norm.NFKC.String(s)
	}
	return string(compose([]rune(norm.NFKD.String(s))))
}

// properties returns x/text's normalization properties of r under form f.
func properties(f norm.Form, r rune) norm.Properties {
	var buf [utf8.UTFMax]byte
	n := utf8.EncodeRune(buf[:], r)
	return f.Properties(buf[:n])
}

// ccc returns r's canonical combining class.
func ccc(r rune) uint8 {
	return properties(norm.NFD, r).CCC()
}

// nfkcSafe reports whether x/text's NFKC can be trusted with s: no supplementary character
// is a non-starter, decomposes, or is followed by a non-starter.
func nfkcSafe(s string) bool {
	after := false // the previous rune was supplementary
	for _, r := range s {
		if r < 0x10000 {
			if after && r >= 0x300 && ccc(r) != 0 {
				return false
			}
			after = false
			continue
		}
		if after && ccc(r) != 0 {
			return false
		}
		p := properties(norm.NFKD, r)
		if p.CCC() != 0 || p.Decomposition() != nil {
			return false
		}
		after = true
	}
	return true
}

// compose applies canonical composition to a decomposed, canonically ordered rune
// sequence, in place: each character composes with the last starter unless something
// between them blocks it (a starter, or a mark of the same or a higher class).
func compose(rs []rune) []rune {
	if len(rs) == 0 {
		return rs
	}
	starter := -1
	last := 256 // the class of the last rune kept; 256: no starter yet
	if ccc(rs[0]) == 0 {
		starter, last = 0, 0
	}
	kept := 1
	for _, r := range rs[1:] {
		class := int(ccc(r))
		if starter >= 0 && (last == 0 || last < class) {
			if composite, ok := composePair(rs[starter], r); ok {
				rs[starter] = composite
				continue
			}
		}
		if class == 0 {
			starter = kept
		}
		last = class
		rs[kept] = r
		kept++
	}
	return rs[:kept]
}

// Hangul syllable composition constants (Unicode section 3.12).
const (
	hangulS      = 0xAC00
	hangulL      = 0x1100
	hangulV      = 0x1161
	hangulT      = 0x11A7
	hangulLCount = 19
	hangulVCount = 21
	hangulTCount = 28
	hangulSCount = hangulLCount * hangulVCount * hangulTCount
)

// composePair returns the primary composite of a and b, if they have one.
func composePair(a, b rune) (rune, bool) {
	switch {
	case a >= hangulL && a < hangulL+hangulLCount && b >= hangulV && b < hangulV+hangulVCount:
		return hangulS + ((a-hangulL)*hangulVCount+(b-hangulV))*hangulTCount, true
	case a >= hangulS && a < hangulS+hangulSCount && (a-hangulS)%hangulTCount == 0 &&
		b > hangulT && b < hangulT+hangulTCount:
		return a + (b - hangulT), true
	}
	composite, ok := compositions[uint64(a)<<21|uint64(b)] //nolint:gosec // runes are non-negative here
	return composite, ok
}
