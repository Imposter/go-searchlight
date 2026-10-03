package query

import (
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
)

func strp(s string) *string   { return &s }
func nump(n float64) *float64 { return &n }
func boolp(b bool) *bool      { return &b }

// doc builds a *schema.Doc with the given fields, for Match tests that need exact
// control over a field's shape (present, missing, or present with no typed part).
func doc(fields map[string]schema.Value) *schema.Doc {
	return &schema.Doc{Fields: fields}
}

// matchCase is one Match table-test row: query holds raw query JSON; fields are the
// document's fields (nil or omitted means the field is missing).
type matchCase struct {
	name   string
	query  string
	fields map[string]schema.Value
	want   bool
}

func runMatchCases(t *testing.T, cases []matchCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := mustParse(t, tc.query)
			d := doc(tc.fields)
			if got := Match(n, d); got != tc.want {
				t.Errorf("Match(%s, %v) = %v, want %v", tc.query, tc.fields, got, tc.want)
			}
			// Compile once, Match many: same answer, and twice in a row (immutability).
			c := Compile(n)
			if got := c.Match(d); got != tc.want {
				t.Errorf("Compile().Match(%s, %v) = %v, want %v", tc.query, tc.fields, got, tc.want)
			}
			if got := c.Match(d); got != tc.want {
				t.Errorf("Compile().Match (second call) = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMatchEq(t *testing.T) {
	runMatchCases(t, []matchCase{
		{
			"text match", `{"field": "f", "op": "eq", "value": "Acme"}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("acme")}},
			true,
		},
		{
			"text no match", `{"field": "f", "op": "eq", "value": "Acme"}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("other")}},
			false,
		},
		{
			"number match", `{"field": "f", "op": "eq", "value": 3}`,
			map[string]schema.Value{"f": {Present: true, Number: nump(3)}},
			true,
		},
		{
			"bool match", `{"field": "f", "op": "eq", "value": true}`,
			map[string]schema.Value{"f": {Present: true, Bool: boolp(true)}},
			true,
		},
		{
			"bool no match", `{"field": "f", "op": "eq", "value": true}`,
			map[string]schema.Value{"f": {Present: true, Bool: boolp(false)}},
			false,
		},
		{"missing field", `{"field": "f", "op": "eq", "value": "x"}`, nil, false},
		{
			"null field", `{"field": "f", "op": "eq", "value": "x"}`,
			map[string]schema.Value{"f": {Present: false}},
			false,
		},
		{
			"wrong-type value: text present, no Text", `{"field": "f", "op": "eq", "value": "x"}`,
			map[string]schema.Value{"f": {Present: true}},
			false,
		},
		{
			"wrong-type value: number present, no Number", `{"field": "f", "op": "eq", "value": 1}`,
			map[string]schema.Value{"f": {Present: true}},
			false,
		},
	})
}

func TestMatchNe(t *testing.T) {
	runMatchCases(t, []matchCase{
		{
			"differs", `{"field": "f", "op": "ne", "value": "Acme"}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("other")}},
			true,
		},
		{
			"equal", `{"field": "f", "op": "ne", "value": "Acme"}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("acme")}},
			false,
		},
		{"missing field matches ne", `{"field": "f", "op": "ne", "value": "x"}`, nil, true},
		{
			"null field matches ne", `{"field": "f", "op": "ne", "value": "x"}`,
			map[string]schema.Value{"f": {Present: false}},
			true,
		},
		{
			"wrong-type value matches ne", `{"field": "f", "op": "ne", "value": "x"}`,
			map[string]schema.Value{"f": {Present: true}},
			true,
		},
	})
}

func TestMatchIn(t *testing.T) {
	runMatchCases(t, []matchCase{
		{
			"text in list", `{"field": "f", "op": "in", "value": ["a", "b"]}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("b")}},
			true,
		},
		{
			"text not in list", `{"field": "f", "op": "in", "value": ["a", "b"]}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("c")}},
			false,
		},
		{
			"number in list", `{"field": "f", "op": "in", "value": [1, 2, 3]}`,
			map[string]schema.Value{"f": {Present: true, Number: nump(2)}},
			true,
		},
		{
			"bool in list", `{"field": "f", "op": "in", "value": [true]}`,
			map[string]schema.Value{"f": {Present: true, Bool: boolp(true)}},
			true,
		},
		{"missing field", `{"field": "f", "op": "in", "value": ["a"]}`, nil, false},
	})
}

func TestMatchComparisons(t *testing.T) {
	runMatchCases(t, []matchCase{
		{
			"lt true", `{"field": "f", "op": "lt", "value": 5}`,
			map[string]schema.Value{"f": {Present: true, Number: nump(3)}},
			true,
		},
		{
			"lt false", `{"field": "f", "op": "lt", "value": 5}`,
			map[string]schema.Value{"f": {Present: true, Number: nump(5)}},
			false,
		},
		{
			"lte true at boundary", `{"field": "f", "op": "lte", "value": 5}`,
			map[string]schema.Value{"f": {Present: true, Number: nump(5)}},
			true,
		},
		{
			"gt true", `{"field": "f", "op": "gt", "value": 5}`,
			map[string]schema.Value{"f": {Present: true, Number: nump(6)}},
			true,
		},
		{
			"gte true at boundary", `{"field": "f", "op": "gte", "value": 5}`,
			map[string]schema.Value{"f": {Present: true, Number: nump(5)}},
			true,
		},
		{"lt missing field", `{"field": "f", "op": "lt", "value": 5}`, nil, false},
		{
			"lt wrong-type value", `{"field": "f", "op": "lt", "value": 5}`,
			map[string]schema.Value{"f": {Present: true}},
			false,
		},
	})
}

func TestMatchBetween(t *testing.T) {
	runMatchCases(t, []matchCase{
		{
			"inside", `{"field": "f", "op": "between", "value": [1, 10]}`,
			map[string]schema.Value{"f": {Present: true, Number: nump(5)}},
			true,
		},
		{
			"at lo", `{"field": "f", "op": "between", "value": [1, 10]}`,
			map[string]schema.Value{"f": {Present: true, Number: nump(1)}},
			true,
		},
		{
			"at hi", `{"field": "f", "op": "between", "value": [1, 10]}`,
			map[string]schema.Value{"f": {Present: true, Number: nump(10)}},
			true,
		},
		{
			"outside", `{"field": "f", "op": "between", "value": [1, 10]}`,
			map[string]schema.Value{"f": {Present: true, Number: nump(11)}},
			false,
		},
		{"missing field", `{"field": "f", "op": "between", "value": [1, 10]}`, nil, false},
	})
}

func TestMatchExists(t *testing.T) {
	runMatchCases(t, []matchCase{
		{
			"exists true, present", `{"field": "f", "op": "exists", "value": true}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("x")}},
			true,
		},
		{"exists true, missing", `{"field": "f", "op": "exists", "value": true}`, nil, false},
		{
			"exists true, null", `{"field": "f", "op": "exists", "value": true}`,
			map[string]schema.Value{"f": {Present: false}},
			false,
		},
		{
			"exists false, present", `{"field": "f", "op": "exists", "value": false}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("x")}},
			false,
		},
		{"exists false, missing matches", `{"field": "f", "op": "exists", "value": false}`, nil, true},
		{
			"exists defaults to true", `{"field": "f", "op": "exists"}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("x")}},
			true,
		},
		{
			"wrong-type value still exists", `{"field": "f", "op": "exists", "value": true}`,
			map[string]schema.Value{"f": {Present: true}},
			true,
		},
	})
}

func TestMatchContains(t *testing.T) {
	runMatchCases(t, []matchCase{
		{
			"contains match", `{"field": "f", "op": "contains", "value": "cme"}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("acme corp")}},
			true,
		},
		{
			"contains no match", `{"field": "f", "op": "contains", "value": "zzz"}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("acme corp")}},
			false,
		},
		{
			"contains_any one hits", `{"field": "f", "op": "contains_any", "value": ["zzz", "cme"]}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("acme corp")}},
			true,
		},
		{
			"contains_all both hit", `{"field": "f", "op": "contains_all", "value": ["acme", "corp"]}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("acme corp")}},
			true,
		},
		{
			"contains_all one misses", `{"field": "f", "op": "contains_all", "value": ["acme", "zzz"]}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("acme corp")}},
			false,
		},
		{"missing field", `{"field": "f", "op": "contains", "value": "a"}`, nil, false},
		{
			"wrong-type value", `{"field": "f", "op": "contains", "value": "a"}`,
			map[string]schema.Value{"f": {Present: true}},
			false,
		},
		{
			"grams truncated does not affect correctness", `{"field": "f", "op": "contains", "value": "needle"}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("a long haystack holding the needle somewhere"), GramsTruncated: true}},
			true,
		},
	})
}

func TestMatchStartsWith(t *testing.T) {
	runMatchCases(t, []matchCase{
		{
			"prefix match", `{"field": "f", "op": "starts_with", "value": "Acme"}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("acme corp")}},
			true,
		},
		{
			"not a prefix", `{"field": "f", "op": "starts_with", "value": "corp"}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("acme corp")}},
			false,
		},
		{"missing field", `{"field": "f", "op": "starts_with", "value": "a"}`, nil, false},
	})
}

func TestMatchWords(t *testing.T) {
	runMatchCases(t, []matchCase{
		{
			"words_all both present", `{"field": "f", "op": "words_all", "value": ["running", "shoe"]}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("nike running shoe"), Words: " nike running shoe "}},
			true,
		},
		{
			"words_all one missing", `{"field": "f", "op": "words_all", "value": ["running", "zzz"]}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("nike running shoe"), Words: " nike running shoe "}},
			false,
		},
		{
			"words_any one present", `{"field": "f", "op": "words_any", "value": ["zzz", "running"]}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("nike running shoe"), Words: " nike running shoe "}},
			true,
		},
		{
			"phrase as consecutive words", `{"field": "f", "op": "words_any", "value": ["running shoe"]}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("nike running shoe"), Words: " nike running shoe "}},
			true,
		},
		{"words_all missing field", `{"field": "f", "op": "words_all", "value": ["a"]}`, nil, false},
		{
			"not a text value: no words", `{"field": "f", "op": "words_any", "value": ["a"]}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("a"), Words: ""}},
			false,
		},
	})
}

func TestMatchSimilar(t *testing.T) {
	runMatchCases(t, []matchCase{
		{
			"similar enough", `{"field": "f", "op": "similar", "value": {"text": "nike air max 90", "min": 0.3}}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("nike air max 270")}},
			true,
		},
		{
			"not similar enough", `{"field": "f", "op": "similar", "value": {"text": "nike air max 90", "min": 0.99}}`,
			map[string]schema.Value{"f": {Present: true, Text: strp("nike air max 270")}},
			false,
		},
		{"missing field", `{"field": "f", "op": "similar", "value": {"text": "x", "min": 0.1}}`, nil, false},
		{
			"wrong-type value", `{"field": "f", "op": "similar", "value": {"text": "x", "min": 0.1}}`,
			map[string]schema.Value{"f": {Present: true}},
			false,
		},
	})
}

func TestMatchHas(t *testing.T) {
	runMatchCases(t, []matchCase{
		{
			"has match", `{"field": "f", "op": "has", "value": "red"}`,
			map[string]schema.Value{"f": {Present: true, Entries: []string{"blue", "red"}}},
			true,
		},
		{
			"has no match", `{"field": "f", "op": "has", "value": "green"}`,
			map[string]schema.Value{"f": {Present: true, Entries: []string{"blue", "red"}}},
			false,
		},
		{
			"has_any one hits", `{"field": "f", "op": "has_any", "value": ["green", "red"]}`,
			map[string]schema.Value{"f": {Present: true, Entries: []string{"blue", "red"}}},
			true,
		},
		{
			"has_all both hit", `{"field": "f", "op": "has_all", "value": ["blue", "red"]}`,
			map[string]schema.Value{"f": {Present: true, Entries: []string{"blue", "red"}}},
			true,
		},
		{
			"has_all one misses", `{"field": "f", "op": "has_all", "value": ["blue", "green"]}`,
			map[string]schema.Value{"f": {Present: true, Entries: []string{"blue", "red"}}},
			false,
		},
		{"missing field", `{"field": "f", "op": "has", "value": "red"}`, nil, false},
		{
			"no entries", `{"field": "f", "op": "has", "value": "red"}`,
			map[string]schema.Value{"f": {Present: true}},
			false,
		},
	})
}

func TestMatchEmptyNonempty(t *testing.T) {
	runMatchCases(t, []matchCase{
		{
			"empty: no entries", `{"field": "f", "op": "empty"}`,
			map[string]schema.Value{"f": {Present: true}},
			true,
		},
		{
			"empty: has entries", `{"field": "f", "op": "empty"}`,
			map[string]schema.Value{"f": {Present: true, Entries: []string{"a"}}},
			false,
		},
		{"empty: missing field matches", `{"field": "f", "op": "empty"}`, nil, true},
		{
			"empty: null field matches", `{"field": "f", "op": "empty"}`,
			map[string]schema.Value{"f": {Present: false}},
			true,
		},
		{
			"nonempty: has entries", `{"field": "f", "op": "nonempty"}`,
			map[string]schema.Value{"f": {Present: true, Entries: []string{"a"}}},
			true,
		},
		{
			"nonempty: no entries", `{"field": "f", "op": "nonempty"}`,
			map[string]schema.Value{"f": {Present: true}},
			false,
		},
		{"nonempty: missing field", `{"field": "f", "op": "nonempty"}`, nil, false},
	})
}

func TestMatchIDPseudoField(t *testing.T) {
	runMatchCases(t, []matchCase{
		{
			"_id eq", `{"field": "_id", "op": "eq", "value": "doc1"}`,
			map[string]schema.Value{"_id": {Present: true, Text: strp("doc1")}},
			true,
		},
		{
			"_id ne", `{"field": "_id", "op": "ne", "value": "doc2"}`,
			map[string]schema.Value{"_id": {Present: true, Text: strp("doc1")}},
			true,
		},
		{"_id missing from the field map", `{"field": "_id", "op": "exists"}`, nil, false},
	})
}

func TestMatchGroupsAndNot(t *testing.T) {
	runMatchCases(t, []matchCase{
		{
			"all of two holds", `{"all": [{"field": "a", "op": "eq", "value": 1}, {"field": "b", "op": "eq", "value": 2}]}`,
			map[string]schema.Value{"a": {Present: true, Number: nump(1)}, "b": {Present: true, Number: nump(2)}},
			true,
		},
		{
			"all fails when one fails", `{"all": [{"field": "a", "op": "eq", "value": 1}, {"field": "b", "op": "eq", "value": 99}]}`,
			map[string]schema.Value{"a": {Present: true, Number: nump(1)}, "b": {Present: true, Number: nump(2)}},
			false,
		},
		{
			"any holds with one", `{"any": [{"field": "a", "op": "eq", "value": 99}, {"field": "b", "op": "eq", "value": 2}]}`,
			map[string]schema.Value{"a": {Present: true, Number: nump(1)}, "b": {Present: true, Number: nump(2)}},
			true,
		},
		{
			"any fails with none", `{"any": [{"field": "a", "op": "eq", "value": 98}, {"field": "b", "op": "eq", "value": 99}]}`,
			map[string]schema.Value{"a": {Present: true, Number: nump(1)}, "b": {Present: true, Number: nump(2)}},
			false,
		},
		{
			"not flips", `{"not": {"field": "a", "op": "eq", "value": 99}}`,
			map[string]schema.Value{"a": {Present: true, Number: nump(1)}},
			true,
		},
		{"everything root matches an empty doc", `{"all": []}`, nil, true},
		{
			"nested all/any/not", `{"all": [{"not": {"field": "a", "op": "eq", "value": 99}}, {"any": [{"field": "b", "op": "eq", "value": 2}]}]}`,
			map[string]schema.Value{"a": {Present: true, Number: nump(1)}, "b": {Present: true, Number: nump(2)}},
			true,
		},
	})
}

// TestMatchFFFDvsNUL pins the review-focus case: a contains query written with a literal
// U+FFFD matches a document whose text held a NUL, because Analyze cleans NUL to U+FFFD
// before the matcher ever sees it, and the query's value is cleaned the same way.
func TestMatchFFFDvsNUL(t *testing.T) {
	mapping := &schema.Mapping{Fields: map[string]schema.FieldType{"t": schema.Keyword}}
	body := []byte(`{"t": "a\u0000b"}`)
	d, _, err := schema.Analyze(mapping, "doc1", body)
	if err != nil {
		t.Fatalf("Analyze: %v", err)
	}
	got := d.Fields["t"].Text
	if got == nil {
		t.Fatal("Analyze: field t has no Text")
	}
	if *got != "a�b" {
		t.Fatalf("Analyze: field t = %+q, want a\\ufffdb", *got)
	}
	raw := []byte(`{"field": "t", "op": "contains", "value": "a�b"}`)
	n, problems := Parse(raw)
	if len(problems) != 0 {
		t.Fatalf("Parse: %v", problems)
	}
	if !Match(n, &d) {
		t.Fatal("a contains query on a\\ufffdb did not match a doc holding a\\x00b")
	}
}

func TestCompileInvalidOrUnknownNeverMatches(t *testing.T) {
	cases := []struct {
		name string
		n    Node
	}{
		{"nil node", nil},
		{"nil All", (*All)(nil)},
		{"nil Any", (*Any)(nil)},
		{"nil Not", (*Not)(nil)},
		{"nil Leaf", (*Leaf)(nil)},
		{"unknown op", &Leaf{Field: "f", Op: "bogus"}},
	}
	d := doc(map[string]schema.Value{"f": {Present: true, Text: strp("x")}})
	for _, tc := range cases {
		if Match(tc.n, d) {
			t.Errorf("%s matched", tc.name)
		}
	}
}

func TestCompileEqNeverAndNeAlways(t *testing.T) {
	// A value nothing equals (a list, an object, a number no float64 holds): eq never
	// holds, ne always does, whether or not the field is present.
	n := &Leaf{Field: "f", Op: OpEq, Value: []byte(`[1,2]`)}
	d := doc(map[string]schema.Value{"f": {Present: true, Number: nump(1)}})
	if Match(n, d) {
		t.Error("eq with a list value matched")
	}
	n = &Leaf{Field: "f", Op: OpNe, Value: []byte(`[1,2]`)}
	if !Match(n, d) {
		t.Error("ne with a list value did not match")
	}
	if !Match(n, doc(nil)) {
		t.Error("ne with a list value did not match a missing field")
	}
}

func TestMatchNilDocIsEmpty(t *testing.T) {
	n := mustParse(t, `{"field": "f", "op": "ne", "value": "x"}`)
	if !Match(n, nil) {
		t.Fatal("ne against a nil doc did not match")
	}
	n = mustParse(t, `{"field": "f", "op": "eq", "value": "x"}`)
	if Match(n, nil) {
		t.Fatal("eq against a nil doc matched")
	}
}

func TestMatchZeroAllocation(t *testing.T) {
	n := mustParse(t, `{"all": [
		{"field": "a", "op": "eq", "value": "acme"},
		{"field": "b", "op": "contains", "value": "nike"},
		{"field": "c", "op": "similar", "value": {"text": "nike air max 90", "min": 0.3}},
		{"field": "d", "op": "between", "value": [1, 100]}
	]}`)
	c := Compile(n)
	d := doc(map[string]schema.Value{
		"a": {Present: true, Text: strp("acme")},
		"b": {Present: true, Text: strp("nike air max 270")},
		"c": {Present: true, Text: strp("nike air max 270")},
		"d": {Present: true, Number: nump(50)},
	})
	allocs := testing.AllocsPerRun(200, func() { c.Match(d) })
	if allocs != 0 {
		t.Fatalf("Match allocates %v times", allocs)
	}
}

func BenchmarkCompile(b *testing.B) {
	raw := []byte(`{"all": [
		{"field": "a", "op": "eq", "value": "acme"},
		{"field": "b", "op": "contains", "value": "nike"},
		{"any": [{"field": "c", "op": "in", "value": ["x", "y", "z"]}, {"field": "d", "op": "exists"}]}
	]}`)
	n, problems := Parse(raw)
	if len(problems) != 0 {
		b.Fatalf("Parse: %v", problems)
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = Compile(n)
	}
}

func BenchmarkMatch(b *testing.B) {
	n, problems := Parse([]byte(`{"all": [
		{"field": "a", "op": "eq", "value": "acme"},
		{"field": "b", "op": "contains", "value": "nike"},
		{"field": "c", "op": "similar", "value": {"text": "nike air max 90", "min": 0.3}},
		{"field": "d", "op": "between", "value": [1, 100]}
	]}`))
	if len(problems) != 0 {
		b.Fatalf("Parse: %v", problems)
	}
	c := Compile(n)
	d := doc(map[string]schema.Value{
		"a": {Present: true, Text: strp("acme")},
		"b": {Present: true, Text: strp("nike air max 270")},
		"c": {Present: true, Text: strp("nike air max 270")},
		"d": {Present: true, Number: nump(50)},
	})
	b.ReportAllocs()
	for b.Loop() {
		c.Match(d)
	}
}
