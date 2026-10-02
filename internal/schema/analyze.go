package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Imposter/go-searchlight/internal/analysis"
)

// MaxIDBytes is the longest document id, in bytes.
const MaxIDBytes = 512

// Value is one field of an analyzed document: what every index structure and the
// matcher read of it.
//
// A field that is absent, or JSON null, has no Value (or one with Present false). A
// present value that does not fit its field's type (text in a number field, an object
// anywhere) keeps Present true with every typed part empty, so it matches exists and ne
// and nothing that compares a value, as in scrape-bot.
type Value struct {
	// Present is true for any value but null.
	Present bool
	// Text is a keyword or text field's comparable text: a string normalized
	// ([analysis.Normalize]) and cleaned ([analysis.Clean]), or a number or bool as
	// scrape-bot's as_text spells it. Nil for an array or object.
	Text *string
	// Words is a text field's words ([analysis.Words], " a b ") when its value is a
	// string, and "" otherwise, which no words_* condition matches.
	Words string
	// Number is a number or date field's finite value (a date in Unix milliseconds).
	Number *float64
	// Bool is a bool field's value.
	Bool *bool
	// Entries is a keyword_list field's entries, normalized and cleaned, distinct and
	// sorted ([analysis.EntryTerms]).
	Entries []string
	// Grams is every distinct 3-rune substring of Text ([analysis.Substrings3]), the
	// anchors of contains and starts_with.
	Grams []string
}

// Doc is an analyzed document. Fields holds every present field, by name, and the
// pseudo-field [IDField] (the id as a keyword); Body is the document as it was given.
type Doc struct {
	ID     string
	Fields map[string]Value
	Body   []byte
}

// ValidateID refuses an id no document may have: empty, longer than MaxIDBytes, holding a
// NUL or invalid UTF-8.
func ValidateID(id string) error {
	switch {
	case id == "":
		return &ValidationError{Field: IDField, Message: "a document id cannot be empty"}
	case len(id) > MaxIDBytes:
		return &ValidationError{Field: IDField, Message: fmt.Sprintf("a document id is at most %d bytes", MaxIDBytes)}
	case !utf8.ValidString(id):
		return &ValidationError{Field: IDField, Message: "a document id must be valid UTF-8"}
	case strings.IndexByte(id, 0) >= 0:
		return &ValidationError{Field: IDField, Message: "a document id cannot hold a NUL character"}
	}
	return nil
}

// Analyze turns a JSON object into an analyzed document under mapping m.
//
// Each top-level member is a field; null is the same as absent. A mapped field is
// analyzed by its type. An unmapped one follows m.Dynamic: with DynamicTrue its first
// value types it (a string is text, a number a number, a bool a bool, a non-empty array
// a keyword_list; null, an empty array and an object add nothing and are not indexed)
// and the returned MappingUpdate holds the fields to add with [Mapping.Merge]; with
// DynamicFalse it is kept in Body but not indexed; with DynamicStrict the document is
// refused. Errors are *ValidationError, naming the field.
func Analyze(m *Mapping, id string, body []byte) (Doc, MappingUpdate, error) {
	var update MappingUpdate
	if err := ValidateID(id); err != nil {
		return Doc{}, update, err
	}
	members, err := decodeObject(body)
	if err != nil {
		return Doc{}, update, err
	}
	if m == nil {
		m = &Mapping{}
	}
	doc := Doc{ID: id, Fields: make(map[string]Value, len(members)+1), Body: bytes.Clone(body)}
	doc.Fields[IDField] = textValue(Keyword, id)
	for _, name := range slices.Sorted(maps.Keys(members)) {
		value := members[name]
		if name == IDField {
			return Doc{}, MappingUpdate{}, &ValidationError{Field: name, Message: "is reserved for the document id"}
		}
		t, mapped := m.Fields[name]
		if !mapped {
			switch m.Dynamic {
			case DynamicFalse:
				continue
			case DynamicStrict:
				return Doc{}, MappingUpdate{}, &ValidationError{Field: name, Message: `is not in the mapping, and dynamic is "strict"`}
			default:
				inferred, ok := infer(value)
				if !ok {
					continue
				}
				if err := checkFieldName(name); err != nil {
					return Doc{}, MappingUpdate{}, err
				}
				t = inferred
				update.add(name, t)
			}
		}
		if value == nil {
			continue
		}
		doc.Fields[name] = analyzeValue(t, value)
	}
	return doc, update, nil
}

// decodeObject decodes body as one JSON object, numbers kept as json.Number.
func decodeObject(body []byte) (map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var decoded any
	if err := dec.Decode(&decoded); err != nil {
		return nil, &ValidationError{Message: "the document is not valid JSON: " + err.Error()}
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, &ValidationError{Message: "the document holds more than one JSON value"}
	}
	members, ok := decoded.(map[string]any)
	if !ok {
		return nil, &ValidationError{Message: "a document is a JSON object"}
	}
	return members, nil
}

// infer types an unmapped field from its first value; false: the value types nothing.
func infer(value any) (FieldType, bool) {
	switch v := value.(type) {
	case string:
		return Text, true
	case json.Number:
		return Number, true
	case bool:
		return Bool, true
	case []any:
		return KeywordList, len(v) > 0
	default:
		return 0, false
	}
}

func analyzeValue(t FieldType, value any) Value {
	switch t {
	case Keyword, Text:
		return textValue(t, value)
	case KeywordList:
		return Value{Present: true, Entries: listEntries(value)}
	case Number:
		if n, ok := analysis.Number(value); ok {
			return Value{Present: true, Number: &n}
		}
	case Bool:
		if b, ok := value.(bool); ok {
			return Value{Present: true, Bool: &b}
		}
	case Date:
		if ms, ok := dateMillis(value); ok {
			return Value{Present: true, Number: &ms}
		}
	}
	return Value{Present: true}
}

// textValue is a keyword or text field's value: its text, grams and (text only) words.
func textValue(t FieldType, value any) Value {
	text, ok := analysis.AsText(value)
	if !ok {
		return Value{Present: true}
	}
	text = analysis.Clean(text)
	v := Value{Present: true, Text: &text, Grams: analysis.Substrings3(text)}
	if s, isString := value.(string); isString && t == Text {
		v.Words = analysis.Words(s) // a NUL is no word character, so Words never holds one
	}
	return v
}

// listEntries is a keyword_list's entries: a string's split on ", ", or an array's
// members (a string as it is, a number or bool as its text; anything else skipped).
func listEntries(value any) []string {
	switch v := value.(type) {
	case string:
		return analysis.EntryTerms(analysis.Entries(v))
	case []any:
		entries := make([]string, 0, len(v))
		for _, member := range v {
			if text, ok := analysis.AsText(member); ok {
				entries = append(entries, text)
			}
		}
		return analysis.EntryTerms(entries)
	default:
		return nil
	}
}

// dateMillis reads a date: a number is Unix milliseconds already; a string is RFC 3339
// (2026-10-02T09:30:00Z, with or without fractional seconds) or a bare date (2026-10-02,
// midnight UTC).
func dateMillis(value any) (float64, bool) {
	if s, ok := value.(string); ok {
		for _, layout := range []string{time.RFC3339Nano, time.DateOnly} {
			if at, err := time.Parse(layout, s); err == nil {
				return float64(at.UnixMilli()), true
			}
		}
		return 0, false
	}
	return analysis.Number(value)
}
