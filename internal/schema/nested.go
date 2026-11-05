package schema

import (
	"maps"
	"slices"

	"github.com/Imposter/go-searchlight/internal/analysis"
)

// AnalyzeObjects analyzes value, a nested field's value as [FieldValue] reads it from a
// stored body, into its members as an index of the document holds them: a sub-field m
// maps by its type, any other present and of no type (as a copy indexes an unmapped
// field, for presence alone). Nil when value holds no member, or more than
// MaxObjects.
func AnalyzeObjects(m *Mapping, path string, value any) []Object {
	if m == nil {
		m = &Mapping{}
	}
	a := analyzer{mapping: m, presenceOnly: true}
	v, err := a.nested(path, value)
	if err != nil {
		return nil
	}
	return v.Objects
}

// ObjectFieldValue returns the value of sub-field name (the member's own name, not its
// full one) of member offset of a nested field's value, as [AnalyzeObjects] reads it:
// the member whose name, NUL cleaned, is name; of several, the first non-null one in
// name order. False when the member is no object or holds no value for name.
func ObjectFieldValue(value any, offset int, name string) (any, bool) {
	list := nestedMembers(value)
	if offset < 0 || offset >= len(list) {
		return nil, false
	}
	obj, ok := list[offset].(map[string]any)
	if !ok {
		return nil, false
	}
	if v, ok := obj[name]; ok && v != nil {
		if !hasNULName(obj) {
			return v, true
		}
	}
	for _, raw := range slices.Sorted(maps.Keys(obj)) {
		if v := obj[raw]; v != nil && analysis.Clean(raw) == name {
			return v, true
		}
	}
	return nil, false
}

// MarkUntyped makes each of fields that d holds (fields a mapping m does not map,
// which dynamic typing typed) present, of no type, and marked untyped
// ([Value.Untyped]): a top-level field in d.Fields, a nested field's sub-field (under a
// field m maps as nested) in each object holding it.
func MarkUntyped(d *Doc, m *Mapping, fields map[string]FieldType) {
	for f := range fields {
		path, sub := m.NestedPath(f)
		if !sub {
			d.Fields[f] = Value{Present: true, Untyped: true}
			continue
		}
		v := d.Fields[path]
		for i := range v.Objects {
			if o := &v.Objects[i]; o.Fields != nil {
				if _, ok := o.Fields[f]; ok {
					o.Fields[f] = Value{Present: true, Untyped: true}
				}
			}
		}
	}
}
