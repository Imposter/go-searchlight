package schema

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/analysis"
)

type matchFixture struct {
	Mapping Mapping `json:"mapping"`
	Groups  []struct {
		Docs []struct {
			ID    string                    `json:"id"`
			Body  string                    `json:"body"`
			Index map[string]map[string]any `json:"index"`
		} `json:"docs"`
		Queries []struct {
			Query   json.RawMessage `json:"query"`
			Matches []string        `json:"matches"`
		} `json:"queries"`
	} `json:"groups"`
}

func loadMatchFixture(t *testing.T) matchFixture {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "parity", "match.json"))
	if err != nil {
		t.Fatalf("read fixture: %v (regenerate with make parity)", err)
	}
	var f struct {
		Data matchFixture `json:"data"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&f); err != nil {
		t.Fatalf("decode match.json: %v", err)
	}
	if len(f.Data.Groups) == 0 {
		t.Fatal("match.json holds no groups")
	}
	return f.Data
}

// TestParityIndexDocs checks Analyze against scrape-bot's own search document of every
// fixture document (search_index.index_doc): the text, words, number, flag and entries
// it stores per field are what Analyze keeps under the field's type.
func TestParityIndexDocs(t *testing.T) {
	f := loadMatchFixture(t)
	docs := 0
	for _, group := range f.Groups {
		for _, fd := range group.Docs {
			doc, update, err := Analyze(&f.Mapping, fd.ID, []byte(fd.Body))
			if err != nil {
				t.Fatalf("Analyze(%q, %s): %v", fd.ID, fd.Body, err)
			}
			if !update.Empty() {
				t.Fatalf("Analyze(%q) updated the mapping: %v", fd.ID, update.Fields)
			}
			docs++
			compareIndex(t, fd.ID+"._id", Keyword, doc.Fields[IDField], fd.Index["key"])
			for name, typ := range f.Mapping.Fields {
				compareIndex(t, fd.ID+"."+name, typ, doc.Fields[name], fd.Index[name])
			}
		}
	}
	if docs < 300 {
		t.Fatalf("only %d fixture documents", docs)
	}
}

func compareIndex(t *testing.T, where string, typ FieldType, v Value, index map[string]any) {
	t.Helper()
	if v.Present != (index != nil) {
		t.Errorf("%s: present %v, scrape-bot indexed %v", where, v.Present, index)
		return
	}
	if index == nil {
		return
	}
	text, hasText := index["t"].(string)
	switch typ {
	case Keyword, Text:
		if hasText != (v.Text != nil) || hasText && *v.Text != text {
			t.Errorf("%s: text %v, want %+q (%v)", where, deref(v.Text), text, hasText)
		}
		if typ == Text {
			want := ""
			if w, ok := index["w"].(string); ok {
				want = w
			} else if hasText {
				want = " " + text + " "
			}
			if v.Words != want {
				t.Errorf("%s: words %+q, want %+q", where, v.Words, want)
			}
		}
	case KeywordList:
		var want []string
		if entries, ok := index["e"].([]any); ok {
			for _, entry := range entries {
				want = append(want, as[string](t, entry))
			}
		} else if index["s"] == true {
			want = []string{text}
		}
		if !slices.Equal(v.Entries, want) {
			t.Errorf("%s: entries %+q, want %+q", where, v.Entries, want)
		}
	case Number, Date:
		n, hasNumber := index["n"].(json.Number)
		if hasNumber != (v.Number != nil) {
			t.Errorf("%s: number %v, scrape-bot %v", where, v.Number, index["n"])
		} else if hasNumber {
			want, _ := analysis.Number(n)
			if *v.Number != want {
				t.Errorf("%s: number %v, want %v", where, *v.Number, want)
			}
		}
	case Bool:
		b, hasBool := index["b"].(bool)
		if hasBool != (v.Bool != nil) || hasBool && *v.Bool != b {
			t.Errorf("%s: bool %v, scrape-bot %v", where, v.Bool, index["b"])
		}
	}
}

func deref(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

// TestParityMatch evaluates every fixture query over the analyzed fixture documents with
// a reference matcher written from spec section 4, and compares with scrape-bot's
// verdicts. It proves the analyzed values carry everything the semantics need; the
// production matcher (internal/query) is held to the same fixture.
func TestParityMatch(t *testing.T) {
	f := loadMatchFixture(t)
	queries := 0
	for g, group := range f.Groups {
		docs := make([]Doc, len(group.Docs))
		for i, fd := range group.Docs {
			doc, _, err := Analyze(&f.Mapping, fd.ID, []byte(fd.Body))
			if err != nil {
				t.Fatalf("Analyze(%q): %v", fd.ID, err)
			}
			docs[i] = doc
		}
		for q, fq := range group.Queries {
			node := decodeQuery(t, fq.Query)
			var got []string
			for i := range docs {
				if refMatch(t, node, &docs[i]) {
					got = append(got, docs[i].ID)
				}
			}
			if !slices.Equal(got, fq.Matches) {
				t.Errorf("group %d query %d %s:\n got  %q\n want %q", g, q, fq.Query, got, fq.Matches)
			}
			queries++
		}
	}
	if queries < 300 {
		t.Fatalf("only %d fixture queries", queries)
	}
}

func decodeQuery(t *testing.T, raw json.RawMessage) map[string]any {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var node map[string]any
	if err := dec.Decode(&node); err != nil {
		t.Fatalf("query %s: %v", raw, err)
	}
	return node
}

func refMatch(t *testing.T, node map[string]any, d *Doc) bool {
	t.Helper()
	if children, ok := node["all"].([]any); ok {
		for _, child := range children {
			if !refMatch(t, as[map[string]any](t, child), d) {
				return false
			}
		}
		return true
	}
	if children, ok := node["any"].([]any); ok {
		for _, child := range children {
			if refMatch(t, as[map[string]any](t, child), d) {
				return true
			}
		}
		return false
	}
	if child, ok := node["not"].(map[string]any); ok {
		return !refMatch(t, child, d)
	}
	return refLeaf(t, as[string](t, node["field"]), as[string](t, node["op"]), node["value"], d)
}

func refLeaf(t *testing.T, field, op string, value any, d *Doc) bool {
	t.Helper()
	v, ok := d.Fields[field]
	present := ok && v.Present
	switch op {
	case "exists":
		return present == (value != false)
	case "empty":
		return !present || len(v.Entries) == 0
	case "ne":
		return !present || !refEquals(v, value)
	}
	if !present {
		return false
	}
	switch op {
	case "eq":
		return refEquals(v, value)
	case "in":
		return slices.ContainsFunc(as[[]any](t, value), func(w any) bool { return refEquals(v, w) })
	case "lt", "lte", "gt", "gte":
		line, _ := analysis.Number(value)
		if v.Number == nil {
			return false
		}
		n := *v.Number
		return op == "lt" && n < line || op == "lte" && n <= line || op == "gt" && n > line || op == "gte" && n >= line
	case "between":
		pair := as[[]any](t, value)
		lo, _ := analysis.Number(pair[0])
		hi, _ := analysis.Number(pair[1])
		return v.Number != nil && lo <= *v.Number && *v.Number <= hi
	case "contains", "contains_any", "contains_all":
		if v.Text == nil {
			return false
		}
		return refSome(op == "contains_all", wantedTexts(value), func(needle string) bool {
			return strings.Contains(*v.Text, needle)
		})
	case "starts_with":
		return v.Text != nil && strings.HasPrefix(*v.Text, analysis.Clean(analysis.Normalize(as[string](t, value))))
	case "words_all", "words_any":
		if v.Words == "" {
			return false
		}
		var wanted []string
		for _, w := range as[[]any](t, value) {
			wanted = append(wanted, analysis.Words(as[string](t, w)))
		}
		return refSome(op == "words_all", wanted, func(words string) bool {
			return words != analysis.NoWords && strings.Contains(v.Words, words)
		})
	case "similar":
		options := as[map[string]any](t, value)
		minimum, _ := analysis.Number(options["min"])
		wanted := analysis.Clean(analysis.Normalize(as[string](t, options["text"])))
		return v.Text != nil && analysis.Similarity(*v.Text, wanted) >= minimum
	case "nonempty":
		return len(v.Entries) > 0
	case "has", "has_any", "has_all":
		return refSome(op == "has_all", wantedTexts(value), func(entry string) bool {
			_, found := slices.BinarySearch(v.Entries, entry)
			return found
		})
	}
	t.Fatalf("reference matcher: unknown op %q", op)
	return false
}

// refSome is all (every) or any of wanted holding; false when nothing is wanted.
func refSome(every bool, wanted []string, holds func(string) bool) bool {
	if len(wanted) == 0 {
		return false
	}
	if every {
		return !slices.ContainsFunc(wanted, func(w string) bool { return !holds(w) })
	}
	return slices.ContainsFunc(wanted, holds)
}

func wantedTexts(value any) []string {
	var texts []string
	switch v := value.(type) {
	case string:
		texts = append(texts, analysis.Clean(analysis.Normalize(v)))
	case []any:
		for _, w := range v {
			if s, ok := w.(string); ok {
				texts = append(texts, analysis.Clean(analysis.Normalize(s)))
			}
		}
	}
	return texts
}

func refEquals(v Value, wanted any) bool {
	switch w := wanted.(type) {
	case bool:
		return v.Bool != nil && *v.Bool == w
	case string:
		return v.Text != nil && *v.Text == analysis.Clean(analysis.Normalize(w))
	default:
		n, ok := analysis.Number(w)
		return ok && v.Number != nil && *v.Number == n
	}
}

// as asserts v's type, failing the test (not panicking) on a malformed fixture.
func as[T any](t *testing.T, v any) T {
	t.Helper()
	typed, ok := v.(T)
	if !ok {
		t.Fatalf("fixture value %v (%T) is not a %T", v, v, typed)
	}
	return typed
}
