// Package analysis holds the text and value normalizers every index and query shares.
//
// Each function is byte-for-byte compatible with scrape-bot's Python, which the fixtures
// in testdata/parity pin (tools/parity/gen.py writes them from scrape-bot's own code):
//
//   - [Normalize] is " ".join(s.split()).casefold();
//   - [Words] is words_of: NFKC, Normalize, runs of non-\w folded to one space, padded;
//   - [Entries] is list_entries: split on ", ", stripped, blanks dropped;
//   - [Number] and [AsText] are number and as_text over JSON values;
//   - [Trigrams] and [Similarity] are pg_trgm's trigrams and similarity, as
//     trigram_similarity computes them;
//   - [Clean] is clean_text: a NUL becomes U+FFFD.
//
// Python's whitespace set, its \w class, str.casefold and str.lower differ from Go's
// unicode package and golang.org/x/text/cases (Python 3.13 carries Unicode 15.1, and
// x/text folds Cherokee capitals to lowercase where CaseFolding.txt does not), so those
// come from tables.go, generated from the same Python as the fixtures. NFKC is
// golang.org/x/text/unicode/norm, which the fixtures check code point by code point.
//
// Strings are UTF-8. An invalid byte reads as U+FFFD, as a Go range loop reads it; JSON
// decoding already replaces invalid UTF-8 that way, so a document never carries one.
package analysis

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// replacement is what a NUL is stored as: Postgres text, and so scrape-bot, cannot hold one.
const replacement = "\ufffd"

// Clean returns s with every NUL replaced by U+FFFD (scrape-bot's clean_text).
func Clean(s string) string {
	if strings.IndexByte(s, 0) < 0 {
		return s
	}
	return strings.ReplaceAll(s, "\x00", replacement)
}

// asciiSpace marks the ASCII characters str.isspace accepts, including the
// information separators U+001C..U+001F that Go's unicode.IsSpace leaves out.
var asciiSpace = [utf8.RuneSelf]bool{
	'\t': true, '\n': true, '\v': true, '\f': true, '\r': true,
	0x1c: true, 0x1d: true, 0x1e: true, 0x1f: true, ' ': true,
}

// IsSpace reports whether Python's str.isspace holds for r: what str.split() and
// str.strip() treat as whitespace.
func IsSpace(r rune) bool {
	if r < utf8.RuneSelf {
		return asciiSpace[r]
	}
	return unicode.Is(pySpace, r)
}

// Normalize returns s as text is compared: whitespace runs folded to one space, leading
// and trailing whitespace dropped, then full Unicode case folding (Python's
// " ".join(s.split()).casefold()). It does not replace NUL; see [Clean].
func Normalize(s string) string {
	if isNormalASCII(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	pending := false
	for _, r := range s {
		if IsSpace(r) {
			pending = b.Len() > 0
			continue
		}
		if pending {
			b.WriteByte(' ')
			pending = false
		}
		writeFolded(&b, r)
	}
	return b.String()
}

// isNormalASCII reports whether s is ASCII that Normalize leaves as it is: no capital,
// no whitespace but single interior spaces.
func isNormalASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= utf8.RuneSelf, c >= 'A' && c <= 'Z':
			return false
		case c == ' ':
			if i == 0 || i == len(s)-1 || s[i+1] == ' ' {
				return false
			}
		case asciiSpace[c]:
			return false
		}
	}
	return true
}

// writeFolded writes str.casefold() of one code point.
func writeFolded(b *strings.Builder, r rune) {
	if r < utf8.RuneSelf {
		if r >= 'A' && r <= 'Z' {
			r += 'a' - 'A'
		}
		b.WriteRune(r)
		return
	}
	if folded, ok := foldMap[r]; ok {
		b.WriteString(folded)
		return
	}
	b.WriteRune(r)
}

// strip returns s without leading and trailing whitespace (Python's str.strip()).
func strip(s string) string {
	return strings.TrimFunc(s, IsSpace)
}
