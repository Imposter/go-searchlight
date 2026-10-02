// Package schema holds an index's mapping (field name to type) and turns a JSON document
// into the analyzed fields every index structure and the matcher read ([Analyze]).
package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"
)

// FieldType is how a field is indexed and which operators and aggregations it takes.
type FieldType uint8

// The field types. The zero value is no type.
const (
	// Keyword is one normalized value (case-folded, whitespace folded), with its grams.
	Keyword FieldType = iota + 1
	// Text is a keyword plus word tokens, for words_all and words_any.
	Text
	// KeywordList is a set of normalized entries: a JSON array, or a string split on ", ".
	KeywordList
	// Number is a finite float64.
	Number
	// Bool is true or false.
	Bool
	// Date is Unix milliseconds, given as a number or an RFC 3339 string.
	Date
)

var typeNames = [...]string{
	Keyword:     "keyword",
	Text:        "text",
	KeywordList: "keyword_list",
	Number:      "number",
	Bool:        "bool",
	Date:        "date",
}

// Valid reports whether t is one of the field types.
func (t FieldType) Valid() bool {
	return t >= Keyword && t <= Date
}

// String returns the type's name as a mapping spells it ("keyword", "keyword_list", ...).
func (t FieldType) String() string {
	if !t.Valid() {
		return fmt.Sprintf("FieldType(%d)", uint8(t))
	}
	return typeNames[t]
}

// ParseFieldType returns the type a mapping names.
func ParseFieldType(name string) (FieldType, error) {
	for t := Keyword; t <= Date; t++ {
		if typeNames[t] == name {
			return t, nil
		}
	}
	return 0, fmt.Errorf("unknown field type %q: use one of %s", name, strings.Join(typeNames[Keyword:], ", "))
}

// MarshalText implements encoding.TextMarshaler.
func (t FieldType) MarshalText() ([]byte, error) {
	if !t.Valid() {
		return nil, fmt.Errorf("invalid field type %d", uint8(t))
	}
	return []byte(typeNames[t]), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (t *FieldType) UnmarshalText(text []byte) error {
	parsed, err := ParseFieldType(string(text))
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}

// DynamicMode says what a document's unmapped field does.
type DynamicMode uint8

const (
	// DynamicTrue types an unmapped field from its first value and adds it to the
	// mapping. It is the zero value and the default.
	DynamicTrue DynamicMode = iota
	// DynamicFalse keeps an unmapped field in the stored body but does not index it.
	DynamicFalse
	// DynamicStrict refuses a document with an unmapped field.
	DynamicStrict
)

// String returns the mode as a mapping spells it: "true", "false" or "strict".
func (d DynamicMode) String() string {
	switch d {
	case DynamicTrue:
		return "true"
	case DynamicFalse:
		return "false"
	case DynamicStrict:
		return "strict"
	default:
		return fmt.Sprintf("DynamicMode(%d)", uint8(d))
	}
}

// MarshalJSON writes true, false or "strict".
func (d DynamicMode) MarshalJSON() ([]byte, error) {
	switch d {
	case DynamicTrue:
		return []byte("true"), nil
	case DynamicFalse:
		return []byte("false"), nil
	case DynamicStrict:
		return []byte(`"strict"`), nil
	default:
		return nil, fmt.Errorf("invalid dynamic mode %d", uint8(d))
	}
}

// UnmarshalJSON reads true, false, "true", "false" or "strict".
func (d *DynamicMode) UnmarshalJSON(data []byte) error {
	switch string(bytes.TrimSpace(data)) {
	case "true", `"true"`:
		*d = DynamicTrue
	case "false", `"false"`:
		*d = DynamicFalse
	case `"strict"`:
		*d = DynamicStrict
	default:
		return fmt.Errorf(`dynamic is true, false or "strict", not %s`, data)
	}
	return nil
}

// IDField is the pseudo-field that holds a document's id, as a keyword. No mapping or
// document may name it.
const IDField = "_id"

// MaxFieldChars is the longest field name, in characters.
const MaxFieldChars = 128

// Mapping is an index's field types and what an unmapped field does. A Mapping is not
// changed once it is in use: [Mapping.Merge] returns a new one.
type Mapping struct {
	Fields  map[string]FieldType
	Dynamic DynamicMode
}

// MappingUpdate is the fields a document adds to a dynamic mapping.
type MappingUpdate struct {
	Fields map[string]FieldType
}

// Empty reports whether the update adds nothing.
func (u MappingUpdate) Empty() bool {
	return len(u.Fields) == 0
}

func (u *MappingUpdate) add(name string, t FieldType) {
	if u.Fields == nil {
		u.Fields = make(map[string]FieldType)
	}
	u.Fields[name] = t
}

// Type returns the field's type; [IDField] is a [Keyword].
func (m *Mapping) Type(field string) (FieldType, bool) {
	if field == IDField {
		return Keyword, true
	}
	if m == nil {
		return 0, false
	}
	t, ok := m.Fields[field]
	return t, ok
}

// Clone returns a copy of m that shares nothing with it.
func (m *Mapping) Clone() *Mapping {
	if m == nil {
		return &Mapping{Fields: map[string]FieldType{}}
	}
	fields := maps.Clone(m.Fields)
	if fields == nil {
		fields = map[string]FieldType{}
	}
	return &Mapping{Fields: fields, Dynamic: m.Dynamic}
}

// Merge returns m with u's fields added. Mappings are additive only: a field u gives a
// type other than the one m has is an error, and m is never changed.
func (m *Mapping) Merge(u MappingUpdate) (*Mapping, error) {
	out := m.Clone()
	for _, name := range slices.Sorted(maps.Keys(u.Fields)) {
		t := u.Fields[name]
		if err := checkField(name, t); err != nil {
			return nil, err
		}
		if old, ok := out.Fields[name]; ok {
			if old != t {
				return nil, &ValidationError{
					Field:   name,
					Message: fmt.Sprintf("is %s and cannot become %s: mappings are additive only", old, t),
				}
			}
			continue
		}
		out.Fields[name] = t
	}
	return out, nil
}

// Validate reports the first problem with m's fields (by name), if any.
func (m *Mapping) Validate() error {
	if m.Dynamic > DynamicStrict {
		return &ValidationError{Field: "dynamic", Message: `is true, false or "strict"`}
	}
	for _, name := range slices.Sorted(maps.Keys(m.Fields)) {
		if err := checkField(name, m.Fields[name]); err != nil {
			return err
		}
	}
	return nil
}

func checkField(name string, t FieldType) error {
	if err := checkFieldName(name); err != nil {
		return err
	}
	if !t.Valid() {
		return &ValidationError{Field: name, Message: fmt.Sprintf("has no valid type (%d)", uint8(t))}
	}
	return nil
}

// checkFieldName refuses a name no mapping may hold: empty, longer than MaxFieldChars,
// holding a NUL or invalid UTF-8, or the reserved IDField.
func checkFieldName(name string) error {
	switch {
	case name == "":
		return &ValidationError{Field: name, Message: "a field name cannot be empty"}
	case name == IDField:
		return &ValidationError{Field: name, Message: "is reserved for the document id"}
	case !utf8.ValidString(name):
		return &ValidationError{Field: name, Message: "a field name must be valid UTF-8"}
	case strings.IndexByte(name, 0) >= 0:
		return &ValidationError{Field: name, Message: "a field name cannot hold a NUL character"}
	case utf8.RuneCountInString(name) > MaxFieldChars:
		return &ValidationError{Field: name, Message: fmt.Sprintf("a field name is at most %d characters", MaxFieldChars)}
	}
	return nil
}

type mappingJSON struct {
	Dynamic *DynamicMode         `json:"dynamic,omitempty"`
	Fields  map[string]FieldType `json:"fields"`
}

// MarshalJSON writes {"dynamic": true|false|"strict", "fields": {"name": "keyword", ...}}.
func (m Mapping) MarshalJSON() ([]byte, error) {
	fields := m.Fields
	if fields == nil {
		fields = map[string]FieldType{}
	}
	return json.Marshal(mappingJSON{Dynamic: &m.Dynamic, Fields: fields})
}

// UnmarshalJSON reads what MarshalJSON writes; dynamic defaults to true. Unknown keys,
// unknown types and invalid field names are refused.
func (m *Mapping) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var raw mappingJSON
	if err := dec.Decode(&raw); err != nil {
		return fmt.Errorf("mapping: %w", err)
	}
	parsed := Mapping{Fields: raw.Fields}
	if parsed.Fields == nil {
		parsed.Fields = map[string]FieldType{}
	}
	if raw.Dynamic != nil {
		parsed.Dynamic = *raw.Dynamic
	}
	if err := parsed.Validate(); err != nil {
		return err
	}
	*m = parsed
	return nil
}

// ValidationError is a mapping or document that cannot be accepted. Field names where
// the problem is ("" for the document as a whole).
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return e.Message
	}
	return fmt.Sprintf("field %q: %s", e.Field, e.Message)
}
