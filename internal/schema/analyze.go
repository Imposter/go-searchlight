package schema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Imposter/go-searchlight/internal/analysis"
)

// MaxIDBytes is the longest document id, in bytes.
const MaxIDBytes = 512

// MaxGramChars is the longest value, in characters, whose grams are computed. A longer
// value has none and sets [Value.GramsTruncated].
const MaxGramChars = 1024

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
	// anchors and prefilters of contains and starts_with. It is computed only for a Text
	// of at most MaxGramChars characters, and never for the IDField.
	Grams []string
	// GramsTruncated is true when Text is longer than MaxGramChars and so has no Grams.
	// Such a value can match a contains or starts_with needle that shares no gram with
	// any index: segment term statistics, search prefilters and percolator anchors must
	// treat it as a candidate for every needle (a residual check, or the always-verify
	// path), never skip it for lacking a gram.
	GramsTruncated bool
}

// Doc is an analyzed document. Fields holds every present field, by name, and the
// pseudo-field [IDField] (the id as a keyword); Body is the document as it was given,
// with any invalid UTF-8 byte replaced by U+FFFD.
type Doc struct {
	ID     string
	Fields map[string]Value
	Body   []byte
}

// ValidateID refuses an id no document may have: empty, longer than MaxIDBytes, holding a
// NUL or invalid UTF-8, or "." or ".." (no URL path can address them: they are path
// steps, which HTTP clients and routers resolve away).
func ValidateID(id string) error {
	switch {
	case id == "":
		return &ValidationError{Field: IDField, Message: "a document id cannot be empty"}
	case id == "." || id == "..":
		return &ValidationError{Field: IDField, Message: `a document id cannot be "." or ".."`}
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
// Each top-level member is a field; a repeated key keeps its last value, as Python's
// json reads it; null is the same as absent. Every member's name is read as
// scrape-bot's index reads it, whatever the mapping: a NUL becomes U+FFFD (and of two
// names that then coincide, the first non-null one in document order wins); a name
// that no mapping may hold (empty, or longer than MaxFieldChars characters) is kept in
// Body but never indexed; IDField is refused.
//
// A mapped field is analyzed by its type. An unmapped one follows m.Dynamic. With
// DynamicTrue its first value types it (a string is text, a number a number, a bool a
// bool, a non-empty array a keyword_list), and the returned MappingUpdate holds the
// fields to add with [Mapping.Merge]; a value that types nothing (an empty array, an
// object) is still present, with no typed part, exactly as it is once another document
// has typed the field. With DynamicFalse it is kept in Body but not indexed. With
// DynamicStrict the document is refused, naming the smallest unmapped field name.
// Errors are *ValidationError, naming the field.
//
// Body is a copy of body (with invalid UTF-8 replaced): Analyze never keeps the caller's
// buffer, so a caller may reuse it, and a document sliced out of a large request does
// not pin the whole request in memory for as long as the document lives.
func Analyze(m *Mapping, id string, body []byte) (Doc, MappingUpdate, error) {
	return analyze(m, id, body, false)
}

// Check validates and types a document exactly as [Analyze] does (the same errors
// and the same MappingUpdate) and returns its body as Analyze would keep it (invalid
// UTF-8 replaced), without analyzing its values: what a writer needs before it
// commits a document, at a fraction of Analyze's memory (no text, words or grams).
// Unlike Analyze it returns body itself, not a copy, when it is valid UTF-8.
func Check(m *Mapping, id string, body []byte) ([]byte, MappingUpdate, error) {
	d, u, err := analyze(m, id, body, true)
	return d.Body, u, err
}

func analyze(m *Mapping, id string, body []byte, typesOnly bool) (Doc, MappingUpdate, error) {
	if err := ValidateID(id); err != nil {
		return Doc{}, MappingUpdate{}, err
	}
	if !typesOnly || !utf8.Valid(body) {
		body = validUTF8Copy(body)
	}
	object, err := decodeObject(body)
	if err != nil {
		return Doc{}, MappingUpdate{}, err
	}
	if m == nil {
		m = &Mapping{}
	}
	a := analyzer{
		mapping:   m,
		typesOnly: typesOnly,
		doc:       Doc{ID: id, Body: body},
	}
	if !typesOnly {
		a.doc.Fields = make(map[string]Value, len(object)+1)
		a.doc.Fields[IDField] = textValue(Keyword, id, false)
	}
	if hasNULName(object) {
		// Names that coincide once cleaned resolve by document order, which a map loses.
		members, err := decodeMembers(body)
		if err != nil {
			return Doc{}, MappingUpdate{}, err
		}
		a.claimed = map[string]bool{IDField: true}
		for _, mem := range members {
			if err := a.member(mem.name, mem.value); err != nil {
				return Doc{}, MappingUpdate{}, err
			}
		}
	} else {
		for name, value := range object {
			if err := a.member(name, value); err != nil {
				return Doc{}, MappingUpdate{}, err
			}
		}
	}
	if a.unmapped != "" {
		return Doc{}, MappingUpdate{}, &ValidationError{Field: a.unmapped, Message: `is not in the mapping, and dynamic is "strict"`}
	}
	return a.doc, a.update, nil
}

// analyzer is one Analyze call's state.
type analyzer struct {
	mapping   *Mapping
	typesOnly bool // Check: type the fields, analyze no value
	doc       Doc
	update    MappingUpdate
	claimed   map[string]bool // cleaned names taken so far, when names hold NULs
	unmapped  string          // the smallest unmapped name, under DynamicStrict
}

func (a *analyzer) member(name string, value any) error {
	if name == IDField {
		return &ValidationError{Field: name, Message: "is reserved for the document id"}
	}
	name = analysis.Clean(name)
	if name == "" || utf8.RuneCountInString(name) > MaxFieldChars {
		return nil // as scrape-bot's index: no condition can name it
	}
	if value == nil {
		return nil
	}
	if a.claimed != nil {
		if a.claimed[name] {
			return nil
		}
		a.claimed[name] = true
	}
	t, mapped := a.mapping.Fields[name]
	if !mapped {
		switch a.mapping.Dynamic {
		case DynamicFalse:
			return nil
		case DynamicStrict:
			if a.unmapped == "" || name < a.unmapped {
				a.unmapped = name
			}
			return nil
		default:
			inferred, ok := infer(value)
			if !ok {
				if !a.typesOnly {
					a.doc.Fields[name] = Value{Present: true}
				}
				return nil
			}
			t = inferred
			a.update.add(name, t)
		}
	}
	if !a.typesOnly {
		a.doc.Fields[name] = analyzeValue(t, value)
	}
	return nil
}

func hasNULName(object map[string]any) bool {
	for name := range object {
		if strings.IndexByte(name, 0) >= 0 {
			return true
		}
	}
	return false
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
	object, ok := decoded.(map[string]any)
	if !ok {
		return nil, &ValidationError{Message: "a document is a JSON object"}
	}
	return object, nil
}

// validUTF8Copy returns a copy of body with every byte that is not valid UTF-8 replaced
// by U+FFFD, one per byte, as encoding/json replaces them inside strings.
func validUTF8Copy(body []byte) []byte {
	if utf8.Valid(body) {
		return bytes.Clone(body)
	}
	out := make([]byte, 0, len(body)+len(body)/8)
	for len(body) > 0 {
		r, size := utf8.DecodeRune(body)
		if r == utf8.RuneError && size == 1 {
			out = utf8.AppendRune(out, utf8.RuneError)
		} else {
			out = append(out, body[:size]...)
		}
		body = body[size:]
	}
	return out
}

type member struct {
	name  string
	value any
}

// decodeMembers decodes body as one JSON object into its members in document order,
// numbers kept as json.Number. A repeated key keeps its first position and last value.
func decodeMembers(body []byte) ([]member, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	invalid := func(err error) error {
		return &ValidationError{Message: "the document is not valid JSON: " + err.Error()}
	}
	tok, err := dec.Token()
	if err != nil {
		return nil, invalid(err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return nil, &ValidationError{Message: "a document is a JSON object"}
	}
	var members []member
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, invalid(err)
		}
		name, ok := tok.(string)
		if !ok {
			return nil, &ValidationError{Message: "the document is not valid JSON"}
		}
		var value any
		if err := dec.Decode(&value); err != nil {
			return nil, invalid(err)
		}
		members = append(members, member{name: name, value: value})
	}
	if _, err := dec.Token(); err != nil { // the closing brace
		return nil, invalid(err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, &ValidationError{Message: "the document holds more than one JSON value"}
	}
	return dedupe(members), nil
}

// dedupe keeps each repeated key at its first position with its last value.
func dedupe(members []member) []member {
	const linear = 16 // below this, a scan beats building a map
	var index map[string]int
	out := members[:0]
	for _, mem := range members {
		at := -1
		if len(members) > linear {
			if index == nil {
				index = make(map[string]int, len(members))
			}
			if i, ok := index[mem.name]; ok {
				at = i
			}
		} else {
			for i := range out {
				if out[i].name == mem.name {
					at = i
					break
				}
			}
		}
		if at >= 0 {
			out[at].value = mem.value
			continue
		}
		if index != nil {
			index[mem.name] = len(out)
		}
		out = append(out, mem)
	}
	return out
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
		return textValue(t, value, true)
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

// textValue is a keyword or text field's value: its text, (text only) words, and, when
// grams is set, its grams up to MaxGramChars.
func textValue(t FieldType, value any, grams bool) Value {
	text, ok := analysis.AsText(value)
	if !ok {
		return Value{Present: true}
	}
	text = analysis.Clean(text)
	v := Value{Present: true, Text: &text}
	if grams {
		if utf8.RuneCountInString(text) > MaxGramChars {
			v.GramsTruncated = true
		} else {
			v.Grams = analysis.Substrings3(text)
		}
	}
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
