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

// runeBits is how many bits a code point takes in a packed trigram key.
const runeBits = 21

// packTrigram packs three code points into one key, 21 bits each. Keys order as the
// trigrams' code points do, so sorted keys are sorted trigrams.
func packTrigram(a, b, c rune) uint64 {
	return uint64(a)<<(2*runeBits) | uint64(b)<<runeBits | uint64(c) //nolint:gosec // code points are 0..0x10FFFF
}

// TrigramKeys returns s's pg_trgm trigrams as packed keys (three 21-bit code points),
// sorted and distinct: s lowercased ([Lower]), split into runs of letters and digits,
// each run padded with two spaces before and one after, and every 3-rune window. These
// are what [SimilarityKeys] compares; compute a text's keys once and reuse them.
func TrigramKeys(s string) []uint64 {
	lowered := Lower(s)
	keys := make([]uint64, 0, len(lowered)+1)
	before2, before1 := rune(' '), rune(' ')
	inWord := false
	for _, r := range lowered {
		if isTrigramWord(r) {
			if !inWord {
				before2, before1, inWord = ' ', ' ', true
			}
			keys = append(keys, packTrigram(before2, before1, r))
			before2, before1 = before1, r
			continue
		}
		if inWord {
			keys = append(keys, packTrigram(before2, before1, ' '))
			inWord = false
		}
	}
	if inWord {
		keys = append(keys, packTrigram(before2, before1, ' '))
	}
	if len(keys) == 0 {
		return nil
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// Trigrams returns s's pg_trgm trigrams (see [TrigramKeys]) as strings, distinct and
// sorted by code point.
func Trigrams(s string) []string {
	keys := TrigramKeys(s)
	if keys == nil {
		return nil
	}
	const mask = 1<<runeBits - 1
	out := make([]string, len(keys))
	for i, key := range keys {
		out[i] = string([]rune{rune(key >> (2 * runeBits)), rune(key >> runeBits & mask), rune(key & mask)})
	}
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
