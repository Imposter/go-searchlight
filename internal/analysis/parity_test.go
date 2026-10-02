package analysis

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// fixture is a testdata/parity file: where it came from, and its data.
type fixture[T any] struct {
	Generator map[string]any `json:"generator"`
	Data      T              `json:"data"`
}

func loadFixture[T any](t *testing.T, name string) T {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "parity", name))
	if err != nil {
		t.Fatalf("read fixture: %v (regenerate with make parity)", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var f fixture[T]
	if err := dec.Decode(&f); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	if got := f.Generator["unicode"]; got != UnicodeVersion {
		t.Fatalf("%s was generated with Unicode %v, tables.go with %s: regenerate both (make parity)", name, got, UnicodeVersion)
	}
	return f.Data
}

// everyRune calls fn for every Unicode scalar value (no surrogates).
func everyRune(fn func(r rune)) {
	for r := rune(0); r <= utf8.MaxRune; r++ {
		if r >= 0xD800 && r <= 0xDFFF {
			continue
		}
		fn(r)
	}
}

// atLeast fails the test when a fixture holds fewer than n cases: a truncated or empty
// fixture must not pass vacuously.
func atLeast(t *testing.T, what string, got, n int) {
	t.Helper()
	if got < n {
		t.Fatalf("%s: %d cases, want at least %d (regenerate with make parity)", what, got, n)
	}
}

func runeKeyed(t *testing.T, m map[string]string) map[rune]string {
	t.Helper()
	out := make(map[rune]string, len(m))
	for key, value := range m {
		code, err := strconv.Atoi(key)
		if err != nil {
			t.Fatalf("bad code point %q", key)
		}
		out[rune(code)] = value
	}
	return out
}

func TestParityNormalize(t *testing.T) {
	data := loadFixture[struct {
		Cases []struct {
			In        string `json:"in"`
			Normalize string `json:"normalize"`
			Clean     string `json:"clean"`
		} `json:"cases"`
		Casefold   map[string]string `json:"casefold"`
		Whitespace []rune            `json:"whitespace"`
	}](t, "normalize.json")
	atLeast(t, "normalize cases", len(data.Cases), 500)
	atLeast(t, "casefold", len(data.Casefold), 1000)

	for _, c := range data.Cases {
		if got := Normalize(c.In); got != c.Normalize {
			t.Errorf("Normalize(%+q) = %+q, want %+q", c.In, got, c.Normalize)
		}
		if got := Clean(c.In); got != c.Clean {
			t.Errorf("Clean(%+q) = %+q, want %+q", c.In, got, c.Clean)
		}
	}

	fold := runeKeyed(t, data.Casefold)
	space := make(map[rune]bool, len(data.Whitespace))
	for _, r := range data.Whitespace {
		space[r] = true
	}
	failures := 0
	everyRune(func(r rune) {
		want, ok := fold[r]
		if !ok {
			want = string(r)
		}
		if space[r] {
			want = ""
		}
		if got := Normalize(string(r)); got != want && failures < 20 {
			failures++
			t.Errorf("Normalize(%U) = %+q, want %+q", r, got, want)
		}
		if IsSpace(r) != space[r] && failures < 20 {
			failures++
			t.Errorf("IsSpace(%U) = %v, want %v", r, IsSpace(r), space[r])
		}
	})
}

func TestParityWords(t *testing.T) {
	data := loadFixture[struct {
		Cases []struct {
			In    string `json:"in"`
			Words string `json:"words"`
		} `json:"cases"`
		Word [][2]rune         `json:"word"`
		NFKC map[string]string `json:"nfkc"`
	}](t, "words.json")
	atLeast(t, "words cases", len(data.Cases), 500)
	atLeast(t, "nfkc", len(data.NFKC), 1000)

	for _, c := range data.Cases {
		if got := Words(c.In); got != c.Words {
			t.Errorf("Words(%+q) = %+q, want %+q", c.In, got, c.Words)
		}
	}

	word := map[rune]bool{}
	for _, run := range data.Word {
		for r := run[0]; r <= run[1]; r++ {
			word[r] = true
		}
	}
	nfkc := runeKeyed(t, data.NFKC)
	failures := 0
	everyRune(func(r rune) {
		if IsWord(r) != word[r] && failures < 20 {
			failures++
			t.Errorf("IsWord(%U) = %v, want %v", r, IsWord(r), word[r])
		}
		want, ok := nfkc[r]
		if !ok {
			want = string(r)
		}
		if got := norm.NFKC.String(string(r)); got != want && failures < 20 {
			failures++
			t.Errorf("NFKC(%U) = %+q, want %+q", r, got, want)
		}
	})
}

func TestParityEntries(t *testing.T) {
	data := loadFixture[[]struct {
		In      string   `json:"in"`
		Entries []string `json:"entries"`
		Terms   []string `json:"terms"`
	}](t, "entries.json")
	atLeast(t, "entries cases", len(data), 300)
	for _, c := range data {
		got := Entries(c.In)
		if !slices.Equal(got, c.Entries) {
			t.Errorf("Entries(%+q) = %+q, want %+q", c.In, got, c.Entries)
		}
		if terms := EntryTerms(got); !slices.Equal(terms, c.Terms) {
			t.Errorf("EntryTerms(Entries(%+q)) = %+q, want %+q", c.In, terms, c.Terms)
		}
	}
}

func TestParityNumbers(t *testing.T) {
	data := loadFixture[[]struct {
		JSON   *string      `json:"json"`
		Float  *string      `json:"float"`
		Number *json.Number `json:"number"`
		Bits   *string      `json:"bits"`
		Text   *string      `json:"text"`
	}](t, "numbers.json")
	atLeast(t, "numbers cases", len(data), 500)
	for _, c := range data {
		var value any
		var name string
		switch {
		case c.JSON != nil:
			name = "json " + *c.JSON
			dec := json.NewDecoder(bytes.NewReader([]byte(*c.JSON)))
			dec.UseNumber()
			if err := dec.Decode(&value); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case c.Float != nil:
			name = "float " + *c.Float
			value = map[string]float64{"nan": math.NaN(), "inf": math.Inf(1), "-inf": math.Inf(-1)}[*c.Float]
		default:
			t.Fatal("a numbers case needs json or float")
		}

		got, ok := Number(value)
		switch {
		case c.Bits == nil && ok:
			t.Errorf("Number(%s) = %v, want none", name, got)
		case c.Bits != nil && !ok:
			t.Errorf("Number(%s) = none, want %s", name, *c.Number)
		case c.Bits != nil:
			if bits := fmt.Sprintf("%016x", math.Float64bits(got)); bits != *c.Bits {
				t.Errorf("Number(%s) = %v (%s), want %s (%s)", name, got, bits, *c.Number, *c.Bits)
			}
		}

		text, ok := AsText(value)
		switch {
		case c.Text == nil && ok:
			t.Errorf("AsText(%s) = %q, want none", name, text)
		case c.Text != nil && (!ok || text != *c.Text):
			t.Errorf("AsText(%s) = %q, %v, want %q", name, text, ok, *c.Text)
		}
	}
}

func TestParitySimilarity(t *testing.T) {
	data := loadFixture[struct {
		Trigrams []struct {
			In       string   `json:"in"`
			Trigrams []string `json:"trigrams"`
		} `json:"trigrams"`
		Pairs []struct {
			A          string  `json:"a"`
			B          string  `json:"b"`
			Similarity float64 `json:"similarity"`
		} `json:"pairs"`
		Lower map[string]string `json:"lower"`
	}](t, "similarity.json")
	atLeast(t, "trigram cases", len(data.Trigrams), 300)
	atLeast(t, "similarity pairs", len(data.Pairs), 1000)
	atLeast(t, "lower", len(data.Lower), 1000)

	for _, c := range data.Trigrams {
		if got := Trigrams(c.In); !slices.Equal(got, c.Trigrams) {
			t.Errorf("Trigrams(%+q) = %+q, want %+q", c.In, got, c.Trigrams)
		}
	}
	for _, p := range data.Pairs {
		if got := Similarity(p.A, p.B); got != p.Similarity {
			t.Errorf("Similarity(%+q, %+q) = %v, want %v", p.A, p.B, got, p.Similarity)
		}
	}
	lower := runeKeyed(t, data.Lower)
	failures := 0
	everyRune(func(r rune) {
		want, ok := lower[r]
		if !ok {
			want = string(r)
		}
		if got := Lower(string(r)); got != want && failures < 20 {
			failures++
			t.Errorf("Lower(%U) = %+q, want %+q", r, got, want)
		}
	})
}
