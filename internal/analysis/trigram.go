package analysis

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// capitalSigma is the one character whose lowercase depends on its context.
const capitalSigma = 'Σ'

// Lower returns s lowercased as Python's str.lower() does it: the full Unicode lowercase
// mapping of each code point (İ becomes "i\u0307"), and U+03A3 as final sigma (ς) where it ends
// a word, by the Final_Sigma context.
func Lower(s string) string {
	ascii := true
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			ascii = false
			break
		}
	}
	if ascii {
		return strings.ToLower(s)
	}
	if !strings.ContainsRune(s, capitalSigma) {
		var b strings.Builder
		b.Grow(len(s))
		for _, r := range s {
			writeLower(&b, r)
		}
		return b.String()
	}
	runes := []rune(s)
	var b strings.Builder
	b.Grow(len(s))
	for i, r := range runes {
		if r == capitalSigma {
			if finalSigma(runes, i) {
				b.WriteRune('ς')
			} else {
				b.WriteRune('σ')
			}
			continue
		}
		writeLower(&b, r)
	}
	return b.String()
}

func writeLower(b *strings.Builder, r rune) {
	if r < utf8.RuneSelf {
		if r >= 'A' && r <= 'Z' {
			r += 'a' - 'A'
		}
		b.WriteRune(r)
		return
	}
	if lowered, ok := lowerMap[r]; ok {
		b.WriteString(lowered)
		return
	}
	b.WriteRune(r)
}

// finalSigma is Python's handle_capital_sigma: the nearest character before i that is not
// case-ignorable is cased, and the nearest after it, if any, is not.
func finalSigma(runes []rune, i int) bool {
	j := i - 1
	for j >= 0 && unicode.Is(caseIgnorable, runes[j]) {
		j--
	}
	if j < 0 || !unicode.Is(casedNotIgnorable, runes[j]) {
		return false
	}
	j = i + 1
	for j < len(runes) && unicode.Is(caseIgnorable, runes[j]) {
		j++
	}
	return j == len(runes) || !unicode.Is(casedNotIgnorable, runes[j])
}

// isTrigramWord reports whether r belongs to a word as pg_trgm splits text: a letter or a
// digit (\w without '_').
func isTrigramWord(r rune) bool {
	return r != '_' && IsWord(r)
}

// trigramSet returns s's pg_trgm trigrams: s lowercased, split into runs of letters and
// digits, each run padded with two spaces before and one after, and every 3-rune window.
func trigramSet(s string) map[string]struct{} {
	grams := make(map[string]struct{})
	lowered := Lower(s)
	var word []rune
	flush := func() {
		if len(word) == 0 {
			return
		}
		padded := make([]rune, 0, len(word)+3)
		padded = append(padded, ' ', ' ')
		padded = append(padded, word...)
		padded = append(padded, ' ')
		for k := 0; k+3 <= len(padded); k++ {
			grams[string(padded[k:k+3])] = struct{}{}
		}
		word = word[:0]
	}
	for _, r := range lowered {
		if isTrigramWord(r) {
			word = append(word, r)
		} else {
			flush()
		}
	}
	flush()
	return grams
}

// Trigrams returns s's pg_trgm trigrams (see [Similarity]), distinct and sorted by code
// point.
func Trigrams(s string) []string {
	set := trigramSet(s)
	if len(set) == 0 {
		return nil
	}
	out := make([]string, 0, len(set))
	for gram := range set {
		out = append(out, gram)
	}
	slices.Sort(out)
	return out
}

// Substrings3 returns every distinct 3-rune substring of s, sorted by code point. Over a
// normalized text these are the grams a contains or starts_with needle of three or more
// runes must share with any text that holds it, so they anchor and prefilter those ops.
// A string shorter than three runes has none.
func Substrings3(s string) []string {
	starts := make([]int, 0, len(s)+1)
	for i := range s {
		starts = append(starts, i)
	}
	if len(starts) < 3 {
		return nil
	}
	starts = append(starts, len(s))
	out := make([]string, 0, len(starts)-3)
	for k := 0; k+3 < len(starts); k++ {
		out = append(out, s[starts[k]:starts[k+3]])
	}
	slices.Sort(out)
	return slices.Compact(out)
}
