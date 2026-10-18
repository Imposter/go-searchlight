// Package schema holds an index's mapping (field name to type) and turns a JSON document
// into the analyzed fields every index structure and the matcher read ([Analyze]).
package schema

import (
	"bytes"
	"encoding/json"
	"errors"
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

	// The point types: a list of [x, y] pairs, each axis a number or a date, as
	// Elasticsearch's point field and Lucene's XYPoint, for box queries. A mapping writes
	// one as {"type": "point", "x": "number", "y": "date"}; [PointType] makes one and
	// [FieldType.Axes] reads its axes back.
	pointNumberNumber
	pointNumberDate
	pointDateNumber
	pointDateDate

	// Nested is an array of objects, each matched apart: a nested query holds when
	// one object satisfies its whole inner query. A mapping writes one as
	// {"type": "nested", "fields": {"size": "keyword", ...}}; its sub-fields are mapped
	// by their full names, the path, a dot and the member's name (_v.size), each of
	// any type above but a point.
	Nested
)

var typeNames = [...]string{
	Keyword:     "keyword",
	Text:        "text",
	KeywordList: "keyword_list",
	Number:      "number",
	Bool:        "bool",
	Date:        "date",
	Nested:      NestedTypeName,
}

// PointTypeName is how a mapping names a point field's type.
const PointTypeName = "point"

// NestedTypeName is how a mapping names a nested field's type.
const NestedTypeName = "nested"

// MaxObjects is the most members (objects or not) one value of a nested field holds:
// a document with more is refused.
const MaxObjects = 10_000

// PointType returns the point type of axes x and y, each [Number] or [Date]; 0 (no type)
// for any other axis.
func PointType(x, y FieldType) FieldType {
	axis := func(t FieldType) (FieldType, bool) {
		switch t {
		case Number:
			return 0, true
		case Date:
			return 1, true
		}
		return 0, false
	}
	xi, okX := axis(x)
	yi, okY := axis(y)
	if !okX || !okY {
		return 0
	}
	return pointNumberNumber + 2*xi + yi
}

// IsPoint reports whether t is a point type.
func (t FieldType) IsPoint() bool { return t >= pointNumberNumber && t <= pointDateDate }

// Axes returns a point type's axes, each [Number] or [Date]; (0, 0) for any other type.
func (t FieldType) Axes() (x, y FieldType) {
	if !t.IsPoint() {
		return 0, 0
	}
	i := t - pointNumberNumber
	axis := func(bit FieldType) FieldType {
		if bit != 0 {
			return Date
		}
		return Number
	}
	return axis(i & 2), axis(i & 1)
}

// IsNested reports whether t is the nested type.
func (t FieldType) IsNested() bool { return t == Nested }

// Valid reports whether t is one of the field types.
func (t FieldType) Valid() bool {
	return t >= Keyword && t <= Nested
}

// String returns the type's name as a mapping spells it ("keyword", "keyword_list", ...),
// and a point type as point(x, y) with its axes' names: point(number, date).
func (t FieldType) String() string {
	switch {
	case t.IsPoint():
		x, y := t.Axes()
		return fmt.Sprintf("%s(%s, %s)", PointTypeName, x, y)
	case !t.Valid():
		return fmt.Sprintf("FieldType(%d)", uint8(t))
	}
	return typeNames[t]
}

// fieldTypesHint lists the types a mapping names, for errors.
var fieldTypesHint = strings.Join(typeNames[Keyword:Date+1], ", ") +
	`, {"type": "point", "x": "number"|"date", "y": "number"|"date"}, or {"type": "nested", "fields": {...}}`

// ParseFieldType returns the type a mapping names as a string: every type but a point,
// which a mapping writes as an object ([FieldType.UnmarshalJSON]). "nested" names a
// nested field's type alone; its sub-fields are given with it ([ParseFields]).
func ParseFieldType(name string) (FieldType, error) {
	for t := Keyword; t <= Date; t++ {
		if typeNames[t] == name {
			return t, nil
		}
	}
	if name == NestedTypeName {
		return Nested, nil
	}
	if name == PointTypeName {
		return 0, fmt.Errorf(`a point field names its axes: {"type": "point", "x": "number"|"date", "y": "number"|"date"}`)
	}
	return 0, fmt.Errorf("unknown field type %q: use one of %s", name, fieldTypesHint)
}

// MarshalText implements encoding.TextMarshaler, for every type but a point, which has
// no text form ([FieldType.MarshalJSON] writes it).
func (t FieldType) MarshalText() ([]byte, error) {
	if !t.Valid() || t.IsPoint() {
		return nil, fmt.Errorf("field type %s has no text form", t)
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

// pointJSON is a point type as a mapping writes it.
type pointJSON struct {
	Type string `json:"type"`
	X    string `json:"x"`
	Y    string `json:"y"`
}

// MarshalJSON writes a type's name as a string, and a point type as
// {"type": "point", "x": "number"|"date", "y": "number"|"date"}.
func (t FieldType) MarshalJSON() ([]byte, error) {
	if t.IsPoint() {
		x, y := t.Axes()
		return json.Marshal(pointJSON{Type: PointTypeName, X: x.String(), Y: y.String()})
	}
	text, err := t.MarshalText()
	if err != nil {
		return nil, err
	}
	return json.Marshal(string(text))
}

// UnmarshalJSON reads what MarshalJSON writes. A point type's object holds type, x
// and y, nothing else, each axis "number" or "date". A nested field written with its
// sub-fields is a mapping's to read ([ParseFields]), and refused here.
func (t *FieldType) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		var name string
		if err := json.Unmarshal(data, &name); err != nil {
			return fmt.Errorf("a field type is a name (%s)", fieldTypesHint)
		}
		return t.UnmarshalText([]byte(name))
	}
	if isNestedObject(data) {
		return fmt.Errorf(`a nested field's sub-fields are of the other types: no %s field nests in another`, NestedTypeName)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var p pointJSON
	if err := dec.Decode(&p); err != nil {
		return fmt.Errorf(`a point type is {"type": "point", "x": "number"|"date", "y": "number"|"date"}: %w`, err)
	}
	if p.Type != PointTypeName {
		return fmt.Errorf(`a field type written as an object is a point, {"type": "point", ...}, not %q`, p.Type)
	}
	var axes [2]FieldType
	for i, name := range []string{p.X, p.Y} {
		switch name {
		case "number":
			axes[i] = Number
		case "date":
			axes[i] = Date
		case "":
			return fmt.Errorf(`a point type needs its %s axis: "number" or "date"`, []string{"x", "y"}[i])
		default:
			return fmt.Errorf(`a point's %s axis is "number" or "date", not %q`, []string{"x", "y"}[i], name)
		}
	}
	*t = PointType(axes[0], axes[1])
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
		if t == Nested {
			for _, have := range slices.Sorted(maps.Keys(m.fields())) {
				if strings.HasPrefix(have, name+".") {
					return nil, &ValidationError{
						Field:   name,
						Message: fmt.Sprintf("cannot be nested: field %q is mapped already, and a nested field's sub-fields take the names under it", have),
					}
				}
			}
		}
		out.Fields[name] = t
	}
	if err := checkNested(out.Fields); err != nil {
		return nil, err
	}
	return out, nil
}

func (m *Mapping) fields() map[string]FieldType {
	if m == nil {
		return nil
	}
	return m.Fields
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
	return checkNested(m.Fields)
}

// checkNested refuses what nested fields forbid: a nested field whose name holds a
// dot, and a sub-field (a name under a nested field's, path.name) that is a point,
// nested, or has no name past the path.
func checkNested(fields map[string]FieldType) error {
	for _, name := range slices.Sorted(maps.Keys(fields)) {
		t := fields[name]
		if t == Nested && strings.IndexByte(name, '.') >= 0 {
			return &ValidationError{Field: name, Message: "a nested field's name holds no dot: its sub-fields' names are path.name"}
		}
		path, ok := nestedPathIn(fields, name)
		if !ok {
			continue
		}
		switch {
		case len(name) == len(path)+1:
			return &ValidationError{Field: name, Message: fmt.Sprintf("a sub-field of nested field %q needs a name after the dot", path)}
		case t.IsPoint() || t == Nested:
			return &ValidationError{Field: name, Message: fmt.Sprintf("is a sub-field of nested field %q, and a sub-field is no point and no nested field", path)}
		}
	}
	return nil
}

// nestedPathIn is the nested field of fields that field is a sub-field of: the name
// before its first dot, when fields maps that name as nested.
func nestedPathIn(fields map[string]FieldType, field string) (string, bool) {
	i := strings.IndexByte(field, '.')
	if i <= 0 || fields[field[:i]] != Nested {
		return "", false
	}
	return field[:i], true
}

// NestedPath returns the nested field field is a sub-field of (_v for _v.size), false
// when it is none's: a field of the document itself, or no field.
func (m *Mapping) NestedPath(field string) (string, bool) {
	return nestedPathIn(m.fields(), field)
}

// SubFields returns the sub-fields of nested field path, by their full names, sorted.
func (m *Mapping) SubFields(path string) []string {
	var out []string
	for name := range m.fields() {
		if p, ok := m.NestedPath(name); ok && p == path {
			out = append(out, name)
		}
	}
	slices.Sort(out)
	return out
}

// ParseFields reads a mapping's fields, {"name": type, ...}, each type as
// [FieldType.UnmarshalJSON] reads it, or a nested field, {"type": "nested", "fields":
// {"name": type, ...}}, which adds itself and each of its sub-fields under its full
// name (path.name). A name under a nested field given at the top as well
// ({"_v": nested, "_v.size": ...}) is refused: a sub-field is given in its field's
// fields. A sub-field's problem names it path.fields.name.
func ParseFields(raw map[string]json.RawMessage) (map[string]FieldType, error) {
	out := make(map[string]FieldType, len(raw))
	for _, name := range slices.Sorted(maps.Keys(raw)) {
		data := raw[name]
		if !isNestedObject(data) {
			var t FieldType
			if err := json.Unmarshal(data, &t); err != nil {
				return nil, fmt.Errorf("field %q: %w", name, err)
			}
			out[name] = t
			continue
		}
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		var n nestedJSON
		if err := dec.Decode(&n); err != nil {
			return nil, &ValidationError{Field: name, Message: `a nested field is {"type": "nested", "fields": {"name": type, ...}}: ` + err.Error()}
		}
		out[name] = Nested
		for _, sub := range slices.Sorted(maps.Keys(n.Fields)) {
			var t FieldType
			if err := json.Unmarshal(n.Fields[sub], &t); err != nil {
				return nil, &ValidationError{Field: name + ".fields." + sub, Message: err.Error()}
			}
			if sub == "" {
				return nil, &ValidationError{Field: name + ".fields", Message: "a sub-field needs a name"}
			}
			if t.IsPoint() {
				return nil, &ValidationError{Field: name + ".fields." + sub, Message: "a nested field's sub-field is no point"}
			}
			out[name+"."+sub] = t
		}
	}
	for _, name := range slices.Sorted(maps.Keys(raw)) {
		if i := strings.IndexByte(name, '.'); i > 0 && isNestedObject(raw[name[:i]]) {
			return nil, &ValidationError{Field: name, Message: fmt.Sprintf("is a name under nested field %q: give it in %q's fields", name[:i], name[:i])}
		}
	}
	return out, nil
}

// nestedJSON is a nested field as a mapping writes it, sub-fields by their own names.
type nestedJSON struct {
	Type   string                     `json:"type"`
	Fields map[string]json.RawMessage `json:"fields"`
}

// isNestedObject reports whether data is a JSON object whose type is "nested".
func isNestedObject(data []byte) bool {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || data[0] != '{' {
		return false
	}
	var probe struct {
		Type string `json:"type"`
	}
	return json.Unmarshal(data, &probe) == nil && probe.Type == NestedTypeName
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
	Dynamic *DynamicMode    `json:"dynamic,omitempty"`
	Fields  map[string]any `json:"fields"`
}

type mappingInJSON struct {
	Dynamic *DynamicMode               `json:"dynamic,omitempty"`
	Fields  map[string]json.RawMessage `json:"fields"`
}

// nestedOutJSON is a nested field as MarshalJSON writes it.
type nestedOutJSON struct {
	Type   string               `json:"type"`
	Fields map[string]FieldType `json:"fields"`
}

// MarshalJSON writes {"dynamic": true|false|"strict", "fields": {"name": "keyword", ...}},
// a nested field as {"type": "nested", "fields": {...}} with its sub-fields by their
// own names.
func (m Mapping) MarshalJSON() ([]byte, error) {
	fields := make(map[string]any, len(m.Fields))
	for name, t := range m.Fields {
		if _, sub := m.NestedPath(name); sub {
			continue
		}
		if t != Nested {
			fields[name] = t
			continue
		}
		n := nestedOutJSON{Type: NestedTypeName, Fields: map[string]FieldType{}}
		for _, full := range m.SubFields(name) {
			n.Fields[full[len(name)+1:]] = m.Fields[full]
		}
		fields[name] = n
	}
	return json.Marshal(mappingJSON{Dynamic: &m.Dynamic, Fields: fields})
}

// UnmarshalJSON reads what MarshalJSON writes ([ParseFields]); dynamic defaults to
// true. Unknown keys, unknown types and invalid field names are refused.
func (m *Mapping) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var raw mappingInJSON
	if err := dec.Decode(&raw); err != nil {
		return fmt.Errorf("mapping: %w", err)
	}
	fields, err := ParseFields(raw.Fields)
	if err != nil {
		var ve *ValidationError
		if errors.As(err, &ve) {
			return err
		}
		return fmt.Errorf("mapping: %w", err)
	}
	parsed := Mapping{Fields: fields}
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
