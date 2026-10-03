package schema

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
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

// as asserts v's type, failing the test (not panicking) on a malformed fixture.
func as[T any](t *testing.T, v any) T {
	t.Helper()
	typed, ok := v.(T)
	if !ok {
		t.Fatalf("fixture value %v (%T) is not a %T", v, v, typed)
	}
	return typed
}
