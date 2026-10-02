package schema

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

// object builds a JSON object with its members in the order given (name, value, ...),
// so tests can spell NULs, repeats and order exactly.
func object(t *testing.T, members ...any) []byte {
	t.Helper()
	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i < len(members); i += 2 {
		if i > 0 {
			b.WriteByte(',')
		}
		name, err := json.Marshal(members[i])
		if err != nil {
			t.Fatal(err)
		}
		value, err := json.Marshal(members[i+1])
		if err != nil {
			t.Fatal(err)
		}
		b.Write(name)
		b.WriteByte(':')
		b.Write(value)
	}
	b.WriteByte('}')
	return []byte(b.String())
}

var replacementChar = string(utf8.RuneError)

// TestAnalyzePresenceIsOrderFree: under dynamic mapping, a value that types nothing is
// present whether or not another document has typed its field yet.
func TestAnalyzePresenceIsOrderFree(t *testing.T) {
	m := &Mapping{}
	bodies := [][]byte{object(t, "f", []any{}, "g", map[string]any{})}
	before := make([]Doc, len(bodies))
	for i, body := range bodies {
		doc, update, err := Analyze(m, "d", body)
		if err != nil || !update.Empty() {
			t.Fatalf("before: %v, update %v", err, update.Fields)
		}
		before[i] = doc
	}
	_, update, err := Analyze(m, "typer", object(t, "f", []any{"a"}, "g", "text"))
	if err != nil {
		t.Fatal(err)
	}
	merged, err := m.Merge(update)
	if err != nil {
		t.Fatal(err)
	}
	for i, body := range bodies {
		after, _, err := Analyze(merged, "d", body)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"f", "g"} {
			if before[i].Fields[name].Present != after.Fields[name].Present || !after.Fields[name].Present {
				t.Errorf("%s: present %v before typing, %v after", name, before[i].Fields[name].Present, after.Fields[name].Present)
			}
		}
	}
}

func TestAnalyzeGrams(t *testing.T) {
	m := &Mapping{Fields: map[string]FieldType{"k": Keyword}}
	atCap := strings.Repeat("ab", MaxGramChars/2)
	doc, _, err := Analyze(m, "abcdef", object(t, "k", atCap))
	if err != nil {
		t.Fatal(err)
	}
	if v := doc.Fields["k"]; v.GramsTruncated || len(v.Grams) != 2 {
		t.Errorf("%d characters: grams %q, truncated %v", MaxGramChars, v.Grams, v.GramsTruncated)
	}
	if id := doc.Fields[IDField]; id.Grams != nil || id.GramsTruncated {
		t.Errorf("_id has grams %q", id.Grams)
	}
	doc, _, err = Analyze(m, "d", object(t, "k", atCap+"c"))
	if err != nil {
		t.Fatal(err)
	}
	if v := doc.Fields["k"]; !v.GramsTruncated || v.Grams != nil || v.Text == nil {
		t.Errorf("%d characters: grams %d, truncated %v", MaxGramChars+1, len(v.Grams), v.GramsTruncated)
	}
}

// TestAnalyzeFieldNames: every member's name is read the same way whatever the mapping
// does with it, as scrape-bot's index reads it.
func TestAnalyzeFieldNames(t *testing.T) {
	longest := strings.Repeat("n", MaxFieldChars)
	tooLong := longest + "n"
	folded := "a" + replacementChar + "b"
	for _, dynamic := range []DynamicMode{DynamicTrue, DynamicFalse, DynamicStrict} {
		m := &Mapping{Dynamic: dynamic, Fields: map[string]FieldType{folded: Keyword, longest: Number}}
		body := object(t, "a\x00b", "x", tooLong, 1, "", 2, longest, 3)
		doc, update, err := Analyze(m, "d", body)
		if err != nil {
			t.Fatalf("dynamic %v: %v", dynamic, err)
		}
		if v := doc.Fields[folded]; v.Text == nil || *v.Text != "x" {
			t.Errorf("dynamic %v: the NUL name did not fold to its U+FFFD spelling: %+v", dynamic, v)
		}
		if v := doc.Fields[longest]; v.Number == nil || *v.Number != 3 {
			t.Errorf("dynamic %v: the %d-character name was not indexed", dynamic, MaxFieldChars)
		}
		for _, skipped := range []string{tooLong, ""} {
			if _, ok := doc.Fields[skipped]; ok {
				t.Errorf("dynamic %v: name %.10q was indexed", dynamic, skipped)
			}
			if _, ok := update.Fields[skipped]; ok {
				t.Errorf("dynamic %v: name %.10q was mapped", dynamic, skipped)
			}
		}
	}

	m := &Mapping{Fields: map[string]FieldType{folded: Number}}
	for _, tc := range []struct {
		body []byte
		want float64
	}{
		{object(t, "a\x00b", 1, folded, 2), 1},   // the first of two names that coincide wins
		{object(t, folded, 1, "a\x00b", 2), 1},   // either way round
		{object(t, "a\x00b", nil, folded, 2), 2}, // a null claims nothing
	} {
		doc, _, err := Analyze(m, "d", tc.body)
		if err != nil {
			t.Fatal(err)
		}
		if v := doc.Fields[folded]; v.Number == nil || *v.Number != tc.want {
			t.Errorf("%s: got %+v, want %v", tc.body, v.Number, tc.want)
		}
	}

	if _, _, err := Analyze(&Mapping{}, "d", object(t, "a", 1, IDField, "x")); err == nil {
		t.Error("_id in the body was accepted")
	}
}

func TestAnalyzeRepeatedKeys(t *testing.T) {
	m := &Mapping{Fields: map[string]FieldType{"a": Keyword, "b": Number}}
	doc, _, err := Analyze(m, "d", []byte(`{"a": "x", "b": 2, "a": "y"}`))
	if err != nil {
		t.Fatal(err)
	}
	if v := doc.Fields["a"]; *v.Text != "y" {
		t.Errorf("repeated key: %q, want the last value", *v.Text)
	}
	doc, _, err = Analyze(m, "d", []byte(`{"a": "x", "a": null}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := doc.Fields["a"]; ok {
		t.Error("a key repeated as null is still present")
	}
	members := make([]any, 0, 80)
	for i := range 40 {
		members = append(members, string(rune('a'+i%20)), i)
	}
	doc, _, err = Analyze(&Mapping{Fields: map[string]FieldType{"a": Number}}, "d", object(t, members...))
	if err != nil {
		t.Fatal(err)
	}
	if v := doc.Fields["a"]; *v.Number != 20 {
		t.Errorf("repeated key past the linear scan: %v, want 20", *v.Number)
	}
}

// TestAnalyzeStrictErrorIsDeterministic: the error names the smallest unmapped field
// name, every time, whatever order a map yields the members in.
func TestAnalyzeStrictErrorIsDeterministic(t *testing.T) {
	m := &Mapping{Dynamic: DynamicStrict, Fields: map[string]FieldType{"m": Keyword}}
	for range 50 {
		_, _, err := Analyze(m, "d", []byte(`{"m": "x", "z": 1, "a": 2, "q": 3}`))
		var verr *ValidationError
		if !errors.As(err, &verr) || verr.Field != "a" {
			t.Fatalf("err = %v, want field a", err)
		}
	}
}

func TestAnalyzeSanitizesBody(t *testing.T) {
	m := &Mapping{Fields: map[string]FieldType{"a": Keyword}}
	body := []byte("{\"a\": \"x\xff\xfey\"}")
	doc, _, err := Analyze(m, "d", body)
	if err != nil {
		t.Fatal(err)
	}
	want := "{\"a\": \"x" + replacementChar + replacementChar + "y\"}"
	if !utf8.Valid(doc.Body) || string(doc.Body) != want {
		t.Fatalf("Body = %+q, want %+q", doc.Body, want)
	}
	if got := *doc.Fields["a"].Text; got != "x"+replacementChar+replacementChar+"y" {
		t.Fatalf("text %+q disagrees with the stored body", got)
	}
	if body[8] != 0xff {
		t.Fatal("Analyze changed the caller's buffer")
	}
}
