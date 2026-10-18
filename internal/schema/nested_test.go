package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// A nested field is written with its sub-fields, read flat (each sub-field under its
// full name) and written back grouped.
func TestNestedMappingJSON(t *testing.T) {
	var m Mapping
	raw := `{"fields": {"_v": {"type": "nested", "fields": {"_key": "keyword", "size": "keyword", "price": "number", "in_stock": "bool", "seen": "date"}}, "title": "text"}}`
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatal(err)
	}
	want := map[string]FieldType{
		"_v": Nested, "_v._key": Keyword, "_v.size": Keyword, "_v.price": Number, "_v.in_stock": Bool, "_v.seen": Date, "title": Text,
	}
	if len(m.Fields) != len(want) {
		t.Fatalf("fields %v", m.Fields)
	}
	for name, typ := range want {
		if m.Fields[name] != typ {
			t.Fatalf("%s is %v, want %v", name, m.Fields[name], typ)
		}
	}
	if path, ok := m.NestedPath("_v.size"); !ok || path != "_v" {
		t.Fatalf("NestedPath(_v.size) = %q, %v", path, ok)
	}
	if _, ok := m.NestedPath("title"); ok {
		t.Fatal("title is a sub-field")
	}
	if got := m.SubFields("_v"); strings.Join(got, ",") != "_v._key,_v.in_stock,_v.price,_v.seen,_v.size" {
		t.Fatalf("sub-fields %v", got)
	}
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `{"dynamic":true,"fields":{"_v":{"type":"nested","fields":{"_key":"keyword","in_stock":"bool","price":"number","seen":"date","size":"keyword"}},"title":"text"}}` {
		t.Fatalf("marshalled %s", out)
	}
	var back Mapping
	if err := json.Unmarshal(out, &back); err != nil || len(back.Fields) != len(want) {
		t.Fatalf("round trip: %v, %v", back.Fields, err)
	}
	if got, err := json.Marshal(Nested); err != nil || string(got) != `"nested"` {
		t.Fatalf("Nested marshals as %s, %v", got, err)
	}
}

// The mapping refuses what nested fields forbid, naming the field.
func TestNestedMappingRefused(t *testing.T) {
	for raw, want := range map[string]string{
		`{"fields": {"_v": {"type": "nested", "fields": {"p": {"type": "point", "x": "number", "y": "number"}}}}}`: "_v.fields.p",
		`{"fields": {"_v": {"type": "nested", "fields": {"n": {"type": "nested", "fields": {}}}}}}`:                "_v.fields.n",
		`{"fields": {"_v": {"type": "nested", "fields": {"size": "keyword"}}, "_v.size": "keyword"}}`:              "_v.size",
		`{"fields": {"a.b": {"type": "nested", "fields": {}}}}`:                                                    "a.b",
		`{"fields": {"_v": {"type": "nested", "fields": {"size": "colour"}}}}`:                                     "_v.fields.size",
		`{"fields": {"_v": {"type": "nested", "fields": {"": "keyword"}}}}`:                                        "_v.fields",
		`{"fields": {"_v": {"type": "nested", "fields": {"size": "keyword"}, "extra": 1}}}`:                        "_v",
		`{"fields": {"x": "colour"}}`: "x",
	} {
		var m Mapping
		err := json.Unmarshal([]byte(raw), &m)
		var ve *ValidationError
		if !errors.As(err, &ve) || ve.Field != want {
			t.Errorf("%s: %v, want a problem with field %s", raw, err, want)
		}
	}
	base := &Mapping{Fields: map[string]FieldType{"_v": Nested, "_v.size": Keyword, "a.b": Keyword}}
	for name, u := range map[string]map[string]FieldType{
		"a nested field over a mapped name": {"a": Nested},
		"a point sub-field":                 {"_v.spot": PointType(Number, Number)},
		"a sub-field retyped":               {"_v.size": Number},
		"a sub-field with no name":          {"_v.": Keyword},
		"a nested field with a dot":         {"x.y": Nested},
		"a nested field becoming a keyword": {"_v": Keyword},
		"a keyword becoming a nested field": {"a.b": Nested},
		"a nested field under one":          {"_v.size.x": Nested},
	} {
		if _, err := base.Merge(MappingUpdate{Fields: u}); err == nil {
			t.Errorf("%s: merged", name)
		}
	}
	grown, err := base.Merge(MappingUpdate{Fields: map[string]FieldType{"_v.price": Number, "w": Nested, "w.n": Number}})
	if err != nil || grown.Fields["_v.price"] != Number || grown.Fields["w.n"] != Number {
		t.Fatalf("adding sub-fields: %v, %v", grown, err)
	}
}

// A nested field's value: an array of objects (or one object), a member that is no
// object holding its place; each object's sub-fields analyzed by their types, an
// unmapped one by the dynamic mode; a top-level member named like a sub-field is never
// indexed; more than MaxObjects members are refused.
func TestAnalyzeNested(t *testing.T) {
	m := &Mapping{Fields: map[string]FieldType{"_v": Nested, "_v.size": Keyword, "_v.price": Number, "_v.title": Text}}
	d, u, err := Analyze(m, "a", []byte(`{"_v": [{"size": "L", "price": 12999, "title": "Big One", "color": "red"}, "x", null, {"size": null, "price": "cheap"}], "_v.size": "M"}`))
	if err != nil {
		t.Fatal(err)
	}
	v := d.Fields["_v"]
	if !v.Present || len(v.Objects) != 4 {
		t.Fatalf("_v: %+v", v)
	}
	o := v.Objects[0]
	if !o.IsObject() || *o.Fields["_v.size"].Text != "l" || *o.Fields["_v.price"].Number != 12999 || o.Fields["_v.title"].Words == "" {
		t.Fatalf("object 0: %+v", o.Fields)
	}
	if v.Objects[1].IsObject() || v.Objects[2].IsObject() {
		t.Fatal("a member that is no object is an object")
	}
	if last := v.Objects[3]; !last.IsObject() || last.Fields["_v.size"].Present || !last.Fields["_v.price"].Present || last.Fields["_v.price"].Number != nil {
		t.Fatalf("object 3: %+v", last.Fields)
	}
	if _, ok := d.Fields["_v.size"]; ok {
		t.Fatal("a top-level member named like a sub-field is indexed")
	}
	if u.Fields["_v.color"] != Text || len(u.Fields) != 1 {
		t.Fatalf("update %v, want _v.color typed text", u.Fields)
	}
	if got := v.Objects[0].Fields["_v.color"]; got.Text == nil {
		t.Fatal("the inferred sub-field is not analyzed")
	}
	one, _, err := Analyze(m, "b", []byte(`{"_v": {"size": "S"}}`))
	if err != nil || len(one.Fields["_v"].Objects) != 1 {
		t.Fatalf("one object alone: %+v, %v", one.Fields["_v"], err)
	}
	strict := &Mapping{Fields: m.Fields, Dynamic: DynamicStrict}
	if _, _, err := Analyze(strict, "c", []byte(`{"_v": [{"size": "S", "zz": 1}]}`)); err == nil || !strings.Contains(err.Error(), "_v.zz") {
		t.Fatalf("a strict mapping's unmapped sub-field: %v", err)
	}
	if _, _, err := Check(strict, "c", []byte(`{"_v": [{"size": "S", "zz": 1}]}`)); err == nil {
		t.Fatal("Check lets the unmapped sub-field through")
	}
	members := make([]string, MaxObjects+1)
	for i := range members {
		members[i] = "{}"
	}
	body := []byte(fmt.Sprintf(`{"_v": [%s]}`, strings.Join(members, ",")))
	if _, _, err := Analyze(m, "d", body); err == nil || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("%d members: %v", len(members), err)
	}
	if _, _, err := Check(m, "d", body); err == nil {
		t.Fatalf("Check takes %d members", len(members))
	}
	unmapped, u, err := Analyze(&Mapping{}, "e", []byte(`{"_v": [{"size": "S"}]}`))
	if err != nil || u.Fields["_v"] != KeywordList || unmapped.Fields["_v"].Objects != nil {
		t.Fatalf("an unmapped array of objects: %v, %+v, %v", u.Fields, unmapped.Fields["_v"], err)
	}
}

// AnalyzeObjects reads a stored member as the index holds it, an unmapped sub-field
// present and of no type; ObjectFieldValue reads one sub-field of one member;
// MarkUntyped marks an inferred sub-field in each object holding it.
func TestNestedHelpers(t *testing.T) {
	m := &Mapping{Fields: map[string]FieldType{"_v": Nested, "_v.size": Keyword}}
	body := []byte(`{"_v": [{"size": "L", "color": "red"}, 7, {"si\u0000ze": "x", "size": "M"}]}`)
	value, ok, err := FieldValue(body, "_v")
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	objs := AnalyzeObjects(m, "_v", value)
	if len(objs) != 3 || objs[1].IsObject() || objs[0].Fields["_v.color"].Text != nil || !objs[0].Fields["_v.color"].Present {
		t.Fatalf("objects %+v", objs)
	}
	if v, ok := ObjectFieldValue(value, 2, "size"); !ok || v != "M" {
		t.Fatalf("member 2's size: %v, %v", v, ok)
	}
	if _, ok := ObjectFieldValue(value, 1, "size"); ok {
		t.Fatal("a member that is no object holds a size")
	}
	if _, ok := ObjectFieldValue(value, 3, "size"); ok {
		t.Fatal("a member past the last holds a size")
	}
	d, u, err := Analyze(m, "a", body)
	if err != nil {
		t.Fatal(err)
	}
	MarkUntyped(&d, m, u.Fields)
	if got := d.Fields["_v"].Objects[0].Fields["_v.color"]; !got.Untyped || got.Text != nil {
		t.Fatalf("the inferred sub-field: %+v", got)
	}
}
