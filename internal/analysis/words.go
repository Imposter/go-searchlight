package analysis

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// NoWords is what [Words] returns for text that holds no word.
const NoWords = " "

// IsWord reports whether r is a word character as Python's re module reads \w in a str
// pattern: a letter or a numeric character (str.isalnum()), or '_'.
func IsWord(r rune) bool {
	if r < utf8.RuneSelf {
		return r == '_' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
	}
	return unicode.Is(pyWord, r)
}

// Words returns s's words, space-separated and padded with one space on each side
// (" a b "), as scrape-bot's words_of spells them: NFKC first (a ligature or a full-width
// letter reads as its plain form), then [Normalize], then every run of non-word
// characters becomes one space. Text with no word is [NoWords].
//
// The padding lets a phrase match whole words only: Words(phrase) is a substring of
// Words(text) exactly when the phrase's words appear in the text consecutively.
func Words(s string) string {
	folded := Normalize(NFKC(s))
	var b strings.Builder
	b.Grow(len(folded) + 2)
	b.WriteByte(' ')
	gap := false
	for _, r := range folded {
		if !IsWord(r) {
			gap = true
			continue
		}
		if gap && b.Len() > 1 {
			b.WriteByte(' ')
		}
		gap = false
		b.WriteRune(r)
	}
	if b.Len() == 1 {
		return NoWords
	}
	b.WriteByte(' ')
	return b.String()
}

// EachWord calls fn with every word of a [Words] string (" a b "), in order.
func EachWord(words string, fn func(word string)) {
	for words != "" {
		i := strings.IndexByte(words, ' ')
		if i < 0 {
			fn(words)
			return
		}
		if i > 0 {
			fn(words[:i])
		}
		words = words[i+1:]
	}
}
