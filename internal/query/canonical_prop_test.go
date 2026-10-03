package query

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// canonVocab holds spellings that normalize alike but differ elsewhere: combining
// sequences, ß, dotted and dotless I, ligatures, Greek with breathings and iota
// subscripts, final sigma, Cherokee, full-width digits.
var canonVocab = []string{
	"ss",
	"SS",
	"\u00df",
	"\u1e9e",
	"\u00df\u0301 x",
	"s\u015b x",
	"ss\u0301 x",
	"\u0130",
	"i\u0307",
	"I",
	"\u0131",
	"\ufb01",
	"fi",
	"FI",
	"\u1f00",
	"\u03b1\u0313",
	"\u1f08",
	"\u0391\u0313",
	"\u03a3",
	"\u03c3",
	"\u03c2",
	"\u03a3\u03af\u03c3\u03c5\u03c6\u03bf\u03c2",
	"\u03a3\u038a\u03a3\u03a5\u03a6\u039f\u03a3",
	"caf\u00e9",
	"cafe\u0301",
	"  a  b ",
	"a b",
	"A B",
	"",
	" ",
	"x",
	"ab",
	"Stra\u00dfe",
	"STRASSE",
	"x\u0000y",
	"\ufb01sh",
	"fish",
	"\u01c5",
	"\u01c6",
	"\u01c4",
	"\u1fb3",
	"\u1fbc",
	"\u0390",
	"\u03b9\u0308\u0301",
	"\uab70",
	"\u13a0",
	"\uff11\uff12",
	"12",
	"\uff38",
	"1.0",
	"1",
	"true",
}

var canonMapping = &schema.Mapping{Fields: map[string]schema.FieldType{
	"t": schema.Text, "k": schema.Keyword, "l": schema.KeywordList,
	"n": schema.Number, "b": schema.Bool, "d": schema.Date,
}}

func canonText(rng *rand.Rand) string {
	n := 1 + rng.IntN(3)
	parts := make([]string, n)
	for i := range parts {
		parts[i] = canonVocab[rng.IntN(len(canonVocab))]
	}
	return strings.Join(parts, []string{" ", "", ", ", "-"}[rng.IntN(4)])
}

func canonScalar(rng *rand.Rand) any {
	switch rng.IntN(6) {
	case 0:
		return []any{0.0, 1.0, 12.0, -0.5, 1e300}[rng.IntN(5)]
	case 1:
		return json.Number([]string{"-0", "0", "1e400", "12", "1.0", "1E1"}[rng.IntN(6)])
	case 2:
		return rng.IntN(2) == 0
	default:
		return canonText(rng)
	}
}

func canonValue(rng *rand.Rand, op string) any {
	switch rng.IntN(5) {
	case 0:
		n := rng.IntN(4)
		list := make([]any, n)
		for i := range list {
			list[i] = canonScalar(rng)
		}
		return list
	case 1:
		if op == OpSimilar || rng.IntN(4) == 0 {
			return map[string]any{"text": canonText(rng), "min": []any{0.1, 0.3, 0.5, 1.0, json.Number("0.30")}[rng.IntN(5)]}
		}
	case 2:
		return nil
	}
	return canonScalar(rng)
}

// TestEqualCanonicalMatchesAlike: two conditions with equal Canonical bytes match
// exactly the same documents. Canonical keys the search's filter cache and the
// percolator's query identity, so a collision between conditions that behave
// differently would hand one condition the other's results.
func TestEqualCanonicalMatchesAlike(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	var docs []schema.Doc
	for i := range 400 {
		body := map[string]any{}
		for _, f := range []string{"t", "k", "l", "n", "b", "d"} {
			if rng.IntN(5) > 0 {
				body[f] = canonScalar(rng)
				if f == "l" && rng.IntN(2) == 0 {
					body[f] = []any{canonText(rng), canonText(rng)}
				}
			}
		}
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		d, _, err := schema.Analyze(canonMapping, fmt.Sprintf("%d%s", i, canonText(rng)), raw)
		if err != nil {
			continue
		}
		docs = append(docs, d)
	}
	// Every vocabulary entry alone, and next to "x", in each text field: the
	// documents that tell spellings apart.
	for i, v := range canonVocab {
		for j, text := range []string{v, v + " x", "x " + v} {
			for _, f := range []string{"t", "k", "l"} {
				raw, err := json.Marshal(map[string]any{f: text})
				if err != nil {
					t.Fatal(err)
				}
				if d, _, err := schema.Analyze(canonMapping, fmt.Sprintf("v%d-%d-%s", i, j, f), raw); err == nil {
					docs = append(docs, d)
				}
			}
		}
	}
	type seen struct {
		raw     string
		matches []bool
	}
	groups := map[string]seen{}
	collisions := 0
	for range 40_000 {
		op := Ops[rng.IntN(len(Ops))]
		field := []string{"t", "k", "l", "n", "b", "d", "_id"}[rng.IntN(7)]
		val := canonValue(rng, op)
		var raw json.RawMessage
		if val != nil {
			b, err := json.Marshal(val)
			if err != nil {
				t.Fatal(err)
			}
			raw = b
		}
		leaf := &Leaf{Field: field, Op: op, Value: raw}
		key := string(Canonical(leaf))
		m := Compile(leaf)
		matches := make([]bool, len(docs))
		for i := range docs {
			matches[i] = m.Match(&docs[i])
		}
		desc := fmt.Sprintf("%s %s %s", field, op, raw)
		prev, ok := groups[key]
		if !ok {
			groups[key] = seen{desc, matches}
			continue
		}
		if prev.raw != desc {
			collisions++
		}
		for i := range docs {
			if prev.matches[i] != matches[i] {
				t.Fatalf("equal Canonical, different matches on %q:\n  %s -> %v\n  %s -> %v",
					docs[i].Body, prev.raw, prev.matches[i], desc, matches[i])
			}
		}
	}
	// Systematically: every op on every field, with two spellings that normalize alike
	// (alone, with " x", and in a list).
	for i, a := range canonVocab {
		for _, b := range canonVocab[i+1:] {
			if analysis.Clean(analysis.Normalize(a)) != analysis.Clean(analysis.Normalize(b)) {
				continue
			}
			for _, op := range Ops {
				for _, field := range []string{"t", "k", "l", "_id"} {
					for _, shape := range []func(string) any{
						func(s string) any { return s },
						func(s string) any { return s + " x" },
						func(s string) any { return []any{s, "x"} },
						func(s string) any { return map[string]any{"text": s + " x", "min": 0.5} },
					} {
						la := &Leaf{Field: field, Op: op, Value: mustMarshal(t, shape(a))}
						lb := &Leaf{Field: field, Op: op, Value: mustMarshal(t, shape(b))}
						if string(Canonical(la)) != string(Canonical(lb)) {
							continue
						}
						collisions++
						ma, mb := Compile(la), Compile(lb)
						for k := range docs {
							if ma.Match(&docs[k]) != mb.Match(&docs[k]) {
								t.Fatalf("equal Canonical, different matches on %q: %s %s %s vs %s", docs[k].Body, field, op, la.Value, lb.Value)
							}
						}
					}
				}
			}
		}
	}
	if collisions < 100 {
		t.Fatalf("only %d distinct conditions shared a Canonical: the test exercises too little", collisions)
	}
}

// The reported collision: one normalized form, different words.
func TestCanonicalWordsKeepsPhraseWords(t *testing.T) {
	a := &Leaf{Field: "t", Op: OpWordsAll, Value: json.RawMessage(`"ß́ x"`)}
	b := &Leaf{Field: "t", Op: OpWordsAll, Value: json.RawMessage(`"sś x"`)}
	if string(Canonical(a)) == string(Canonical(b)) {
		t.Fatal("different phrases share a Canonical")
	}
	never := &Leaf{Field: "t", Op: OpWordsAll, Value: json.RawMessage(`["x", "!!"]`)}
	anyOne := &Leaf{Field: "t", Op: OpWordsAny, Value: json.RawMessage(`["x", "!!"]`)}
	if string(Canonical(never)) == string(Canonical(&Leaf{Field: "t", Op: OpWordsAll, Value: json.RawMessage(`"x"`)})) {
		t.Fatal("a words_all with a wordless phrase canonicalizes like one without")
	}
	if string(Canonical(anyOne)) != string(Canonical(&Leaf{Field: "t", Op: OpWordsAny, Value: json.RawMessage(`"x"`)})) {
		t.Fatal("words_any drops a wordless phrase, so it should canonicalize alike")
	}
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
