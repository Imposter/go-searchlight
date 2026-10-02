package analysis

import (
	"encoding/json"
	"math"
	"math/big"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"
)

// TestReviewFocusText pins the non-ASCII and edge inputs of the plan's Review Focus 1.
func TestReviewFocusText(t *testing.T) {
	long := strings.Repeat("Straße ", 72)[:500]
	tests := []struct {
		name, in, normalize, words string
	}{
		{"sharp s folds to ss", "Straße", "strasse", " strasse "},
		{"capital sharp s", "ẞ", "ss", " ss "},
		{"dotted capital I keeps its dot as a mark", "İstanbul", "i\u0307stanbul", " i stanbul "},
		{"dotless i", "ı", "ı", " ı "},
		{"ligature folds, and NFKC splits it for words", "ﬁsh", "fish", " fish "},
		{"ffl ligature", "ﬄ", "ffl", " ffl "},
		{"NUL is kept by Normalize and is no word", "a\x00b", "a\x00b", " a b "},
		{"empty", "", "", NoWords},
		{"only whitespace", " \t\n\u3000", "", NoWords},
		{"mixed whitespace, Python's set", "a\x1cb\u00a0c\u2028d\u3000e\u0085f", "a b c d e f", " a b c d e f "},
		{"zero-width space is no whitespace", "a\u200bb", "a\u200bb", " a b "},
		{"CJK", "日本語 テキスト", "日本語 テキスト", " 日本語 テキスト "},
		{"CJK Extension I (Unicode 15.1) is a letter", "\U0002EBF0", "\U0002EBF0", " \U0002EBF0 "},
		{"emoji are no words", "I ❤\ufe0f 😀 pizza", "i ❤\ufe0f 😀 pizza", " i pizza "},
		{"combining mark composes under NFKC", "cafe\u0301", "cafe\u0301", " café "},
		{"lone combining mark is no word", "a\u0332b", "a\u0332b", " a b "},
		{"full-width letters", "Ｆｕｌｌ", "ｆｕｌｌ", " full "},
		{"Cherokee folds to capitals", "ꭰᏸ", "ᎠᏰ", " ᎠᏰ "},
		{"final sigma folds to sigma", "ΟΔΟΣ", "οδοσ", " οδοσ "},
		{"underscore is a word character", "a_b-c", "a_b-c", " a_b c "},
		{"500 characters", long, strings.ToLower(strings.ReplaceAll(strings.TrimSpace(long), "ß", "ss")), ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Normalize(tc.in); got != tc.normalize {
				t.Errorf("Normalize(%+q) = %+q, want %+q", tc.in, got, tc.normalize)
			}
			if tc.words != "" {
				if got := Words(tc.in); got != tc.words {
					t.Errorf("Words(%+q) = %+q, want %+q", tc.in, got, tc.words)
				}
			}
		})
	}
	if got := Clean("a\x00b\x00"); got != "a\ufffdb\ufffd" {
		t.Errorf("Clean = %+q", got)
	}
	if got := Clean("plain"); got != "plain" {
		t.Errorf("Clean(plain) = %+q", got)
	}
}

func TestEntries(t *testing.T) {
	tests := []struct {
		in             string
		entries, terms []string
	}{
		{"", nil, nil},
		{", , ", nil, nil},
		{"a, , b", []string{"a", "b"}, []string{"a", "b"}},
		{"A, a, A ", []string{"A", "a", "A"}, []string{"a"}},
		{"a,b", []string{"a,b"}, []string{"a,b"}},
		{"a,  b", []string{"a", "b"}, []string{"a", "b"}},
		{"S, ", []string{"S"}, []string{"s"}},
		{"Straße, STRASSE", []string{"Straße", "STRASSE"}, []string{"strasse"}},
		{"x\x00, y", []string{"x\x00", "y"}, []string{"x\ufffd", "y"}},
		{"\u3000a\u3000, b\x1c", []string{"a", "b"}, []string{"a", "b"}},
	}
	for _, tc := range tests {
		got := Entries(tc.in)
		if !slices.Equal(got, tc.entries) {
			t.Errorf("Entries(%+q) = %+q, want %+q", tc.in, got, tc.entries)
		}
		if terms := EntryTerms(got); !slices.Equal(terms, tc.terms) {
			t.Errorf("EntryTerms(%+q) = %+q, want %+q", got, terms, tc.terms)
		}
	}
}

func TestNumber(t *testing.T) {
	huge, _ := new(big.Int).SetString("1"+strings.Repeat("0", 400), 10)
	tests := []struct {
		name  string
		in    any
		want  float64
		ok    bool
		text  string
		texts bool
	}{
		{"int literal", json.Number("12"), 12, true, "12", true},
		{"numeric string is no number", "12", 0, false, "12", true},
		{"bool is no number", true, 0, false, "true", true},
		{"float literal keeps its kind", json.Number("1E1"), 10, true, "10.0", true},
		{"negative zero int", json.Number("-0"), 0, true, "0", true},
		{"negative zero float", json.Number("-0.0"), math.Copysign(0, -1), true, "-0.0", true},
		{"overflowing literal", json.Number("1e400"), 0, false, "inf", true},
		{"10**400 literal", json.Number(huge.String()), 0, false, huge.String(), true},
		{"10**400 big.Int", huge, 0, false, huge.String(), true},
		{"NaN", math.NaN(), 0, false, "nan", true},
		{"inf", math.Inf(1), 0, false, "inf", true},
		{"-inf", math.Inf(-1), 0, false, "-inf", true},
		{"float64", 1e16, 1e16, true, "1e+16", true},
		{"int64 above 2**53 rounds half to even", int64(9007199254740993), 9007199254740992, true, "9007199254740993", true},
		{"uint64", uint64(math.MaxUint64), 18446744073709551616, true, "18446744073709551615", true},
		{"nil", nil, 0, false, "", false},
		{"array", []any{json.Number("1")}, 0, false, "", false},
		{"object", map[string]any{}, 0, false, "", false},
		{"not a JSON literal", json.Number("0x10"), 0, false, "", false},
		{"Infinity is no JSON literal", json.Number("Infinity"), 0, false, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := Number(tc.in)
			if ok != tc.ok || ok && math.Float64bits(got) != math.Float64bits(tc.want) {
				t.Errorf("Number(%v) = %v, %v, want %v, %v", tc.in, got, ok, tc.want, tc.ok)
			}
			text, ok := AsText(tc.in)
			if ok != tc.texts || text != tc.text {
				t.Errorf("AsText(%v) = %q, %v, want %q, %v", tc.in, text, ok, tc.text, tc.texts)
			}
		})
	}
}

func TestFloatText(t *testing.T) {
	tests := map[float64]string{
		0:                      "0.0",
		1:                      "1.0",
		0.1:                    "0.1",
		1e-4:                   "0.0001",
		1e-5:                   "1e-05",
		1.5e-7:                 "1.5e-07",
		1234567890123456:       "1234567890123456.0",
		1e16:                   "1e+16",
		12345678901234567890.0: "1.2345678901234567e+19",
		1.5e300:                "1.5e+300",
		5e-324:                 "5e-324",
		math.MaxFloat64:        "1.7976931348623157e+308",
		-2.5:                   "-2.5",
	}
	for in, want := range tests {
		if got := FloatText(in); got != want {
			t.Errorf("FloatText(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestLowerFinalSigma(t *testing.T) {
	tests := []struct{ in, want string }{
		{"ΟΔΟΣ", "οδος"},
		{"ΣΑΣ", "σας"},
		{"Σ", "σ"},
		{"aΣ", "aς"},
		{"aΣb", "aσb"},
		{"aΣ.", "aς."},
		{"a'Σ", "a'ς"},
		{"aΣ'b", "aσ'b"},
		{" Σ ", " σ "},
		{"İ", "i\u0307"},
		{"ABC", "abc"},
		{"ὈΔΥΣΣΕΎΣ", "ὀδυσσεύς"},
	}
	for _, tc := range tests {
		if got := Lower(tc.in); got != tc.want {
			t.Errorf("Lower(%+q) = %+q, want %+q", tc.in, got, tc.want)
		}
	}
}

func TestTrigramsAndSimilarity(t *testing.T) {
	if got, want := Trigrams("Cat"), []string{"  c", " ca", "at ", "cat"}; !slices.Equal(got, want) {
		t.Errorf("Trigrams(Cat) = %q, want %q", got, want)
	}
	if got := Trigrams("!!"); got != nil {
		t.Errorf("Trigrams(!!) = %q, want none", got)
	}
	if got := Similarity("word", "word"); got != 1 {
		t.Errorf("Similarity(same) = %v", got)
	}
	if got := Similarity("word", ""); got != 0 {
		t.Errorf("Similarity(word, empty) = %v", got)
	}
	// pg_trgm: "word" has 5 trigrams, "words" 6, and they share 4: 4/7 as a float32.
	if got := Similarity("word", "words"); got != float64(float32(4.0/7.0)) {
		t.Errorf("Similarity(word, words) = %v", got)
	}
}

func TestSubstrings3(t *testing.T) {
	tests := map[string][]string{
		"":      nil,
		"ab":    nil,
		"abc":   {"abc"},
		"abab":  {"aba", "bab"},
		"aaaa":  {"aaa"},
		"日本語です": {"日本語", "本語で", "語です"},
		"a😀b":   {"a😀b"},
	}
	for in, want := range tests {
		if got := Substrings3(in); !slices.Equal(got, want) {
			t.Errorf("Substrings3(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestSubstrings3Anchors: a needle of three or more runes inside a text shares every one
// of its 3-rune substrings with the text.
func TestSubstrings3Anchors(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for range 2000 {
		text := Normalize(randomString(rng, 30))
		runes := []rune(text)
		if len(runes) < 3 {
			continue
		}
		start := rng.IntN(len(runes) - 2)
		end := start + 3 + rng.IntN(len(runes)-start-2)
		needle := string(runes[start:end])
		grams := Substrings3(text)
		for _, gram := range Substrings3(needle) {
			if _, found := slices.BinarySearch(grams, gram); !found {
				t.Fatalf("needle %+q of %+q: gram %+q missing", needle, text, gram)
			}
		}
	}
}

var randomPools = [][]rune{
	[]rune("abcXYZ019 _-,.!'"),
	[]rune("\t\n\v\f\r\x1c\x1d\x1e\x1f \u0085\u00a0\u1680\u2000\u2028\u202f\u3000"),
	[]rune("ßẞİıﬁﬀﬃŉǰΐᾈᾳΣσςΟΔ\u0345\u0301\u0332ÄÖÜé"),
	[]rune("Ꭰᏸꭰ日本語中文한국어\U0002EBF0ＡＢ①²½Ⅻ㎏"),
	[]rune("😀👍🏽❤\ufe0f\u200d\u200b\ufeff\x00\ufffd"),
}

func randomString(rng *rand.Rand, longest int) string {
	n := rng.IntN(longest + 1)
	var b strings.Builder
	for range n {
		pool := randomPools[rng.IntN(len(randomPools))]
		b.WriteRune(pool[rng.IntN(len(pool))])
	}
	return b.String()
}

func TestNormalizeProperties(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for range 5000 {
		s := randomString(rng, 40)
		once := Normalize(s)
		if twice := Normalize(once); twice != once {
			t.Fatalf("Normalize not idempotent on %+q: %+q then %+q", s, once, twice)
		}
		if !utf8.ValidString(once) {
			t.Fatalf("Normalize(%+q) is not valid UTF-8", s)
		}
		entries := Entries(once)
		for _, entry := range entries {
			if Normalize(entry) != entry {
				t.Fatalf("entry %+q of normalized %+q is not normalized", entry, once)
			}
		}
		if again := Entries(Normalize(strings.Join(entries, ListSeparator))); !slices.Equal(again, entries) {
			t.Fatalf("Entries(Normalize(%+q)) = %+q, joined and split again %+q", s, entries, again)
		}
		if terms := EntryTerms(entries); !slices.Equal(terms, EntryTerms(terms)) {
			t.Fatalf("EntryTerms not stable on %+q", entries)
		}
	}
	// Every case folding is stable: folding a folded code point changes nothing.
	everyRune(func(r rune) {
		folded := Normalize(string(r))
		if Normalize(folded) != folded {
			t.Errorf("fold of %U is not stable", r)
		}
	})
}

func TestInvalidUTF8ReadsAsReplacement(t *testing.T) {
	if got := Normalize("a\xffB"); got != "a\ufffdb" {
		t.Errorf("Normalize = %+q", got)
	}
	if got := Words("a\xffb"); got != " a b " {
		t.Errorf("Words = %+q", got)
	}
}

func BenchmarkNormalizeASCII(b *testing.B) {
	s := "nike air max 90 running shoe, white"
	for b.Loop() {
		_ = Normalize(s)
	}
}

func BenchmarkNormalizeMixed(b *testing.B) {
	s := "Nike Air Max 90 – Straße Édition, Größe 42"
	for b.Loop() {
		_ = Normalize(s)
	}
}

func BenchmarkWords(b *testing.B) {
	s := "Nike Air Max 90 – Straße Édition, Größe 42"
	for b.Loop() {
		_ = Words(s)
	}
}

func BenchmarkSimilarity(b *testing.B) {
	for b.Loop() {
		_ = Similarity("nike air max 90", "nike air max 270")
	}
}
