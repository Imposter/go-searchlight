package schema

import (
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/analysis"
)

func TestFieldTypeText(t *testing.T) {
	for typ := Keyword; typ <= Date; typ++ {
		text, err := typ.MarshalText()
		if err != nil {
			t.Fatalf("MarshalText(%d): %v", typ, err)
		}
		var back FieldType
		if err := back.UnmarshalText(text); err != nil || back != typ {
			t.Fatalf("round trip of %s gave %v, %v", text, back, err)
		}
	}
	if _, err := ParseFieldType("string"); err == nil {
		t.Fatal("ParseFieldType(string) succeeded")
	}
	if _, err := FieldType(0).MarshalText(); err == nil {
		t.Fatal("MarshalText(0) succeeded")
	}
}

func TestMappingJSON(t *testing.T) {
	var m Mapping
	if err := json.Unmarshal([]byte(`{"fields": {"brand": "keyword", "tags": "keyword_list"}}`), &m); err != nil {
		t.Fatal(err)
	}
	if m.Dynamic != DynamicTrue || m.Fields["brand"] != Keyword || m.Fields["tags"] != KeywordList {
		t.Fatalf("parsed %+v", m)
	}
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"dynamic":true,"fields":{"brand":"keyword","tags":"keyword_list"}}`; string(out) != want {
		t.Fatalf("marshal = %s, want %s", out, want)
	}

	for raw, want := range map[string]DynamicMode{
		`{"dynamic": false, "fields": {}}`:    DynamicFalse,
		`{"dynamic": "false", "fields": {}}`:  DynamicFalse,
		`{"dynamic": "strict", "fields": {}}`: DynamicStrict,
		`{"dynamic": true}`:                   DynamicTrue,
	} {
		var parsed Mapping
		if err := json.Unmarshal([]byte(raw), &parsed); err != nil || parsed.Dynamic != want {
			t.Errorf("%s: dynamic %v, %v; want %v", raw, parsed.Dynamic, err, want)
		}
	}

	for _, raw := range []string{
		`{"fields": {"a": "string"}}`,
		`{"fields": {"_id": "keyword"}}`,
		`{"fields": {"": "keyword"}}`,
		`{"fields": {"a\u0000b": "keyword"}}`,
		`{"fields": {"` + strings.Repeat("x", MaxFieldChars+1) + `": "keyword"}}`,
		`{"dynamic": "maybe"}`,
		`{"fields": {}, "properties": {}}`,
	} {
		var parsed Mapping
		if err := json.Unmarshal([]byte(raw), &parsed); err == nil {
			t.Errorf("%s: accepted", raw)
		}
	}
}

func TestMerge(t *testing.T) {
	m := &Mapping{Fields: map[string]FieldType{"a": Keyword}}
	merged, err := m.Merge(MappingUpdate{Fields: map[string]FieldType{"a": Keyword, "b": Number}})
	if err != nil {
		t.Fatal(err)
	}
	if merged.Fields["b"] != Number || len(merged.Fields) != 2 {
		t.Fatalf("merged %v", merged.Fields)
	}
	if _, ok := m.Fields["b"]; ok {
		t.Fatal("Merge changed the original mapping")
	}
	_, err = m.Merge(MappingUpdate{Fields: map[string]FieldType{"a": Text}})
	var verr *ValidationError
	if !errors.As(err, &verr) || verr.Field != "a" {
		t.Fatalf("type change: %v", err)
	}
	if _, err := m.Merge(MappingUpdate{Fields: map[string]FieldType{IDField: Keyword}}); err == nil {
		t.Fatal("merged _id")
	}
	if typ, ok := m.Type(IDField); !ok || typ != Keyword {
		t.Fatal("_id is not a keyword")
	}
}

func mustAnalyze(t *testing.T, m *Mapping, id, body string) (Doc, MappingUpdate) {
	t.Helper()
	doc, update, err := Analyze(m, id, []byte(body))
	if err != nil {
		t.Fatalf("Analyze(%s): %v", body, err)
	}
	return doc, update
}

func TestAnalyzeDynamic(t *testing.T) {
	m := &Mapping{}
	doc, update := mustAnalyze(t, m, "d1",
		`{"name": "Nike Air", "price": 10, "active": true, "tags": ["a", "B"], "gone": null, "empty": [], "obj": {"x": 1}}`)
	want := map[string]FieldType{"name": Text, "price": Number, "active": Bool, "tags": KeywordList}
	if len(update.Fields) != len(want) {
		t.Fatalf("update %v, want %v", update.Fields, want)
	}
	for name, typ := range want {
		if update.Fields[name] != typ {
			t.Errorf("%s typed %v, want %v", name, update.Fields[name], typ)
		}
	}
	if _, ok := doc.Fields["gone"]; ok {
		t.Error("null was indexed")
	}
	for _, name := range []string{"empty", "obj"} {
		if v := doc.Fields[name]; !v.Present || v.Text != nil || v.Entries != nil {
			t.Errorf("%s: %+v, want present with no typed part", name, v)
		}
	}
	if got := doc.Fields["tags"].Entries; !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("tags %q", got)
	}
	if got := doc.Fields["name"]; *got.Text != "nike air" || got.Words != " nike air " {
		t.Errorf("name %+v", got)
	}

	merged, err := m.Merge(update)
	if err != nil {
		t.Fatal(err)
	}
	// Once typed, a field keeps its type: text in a number field is present, untyped.
	doc, update = mustAnalyze(t, merged, "d2", `{"price": "12", "name": 5}`)
	if !update.Empty() {
		t.Fatalf("update %v", update.Fields)
	}
	if p := doc.Fields["price"]; !p.Present || p.Number != nil || p.Text != nil {
		t.Errorf("price %+v", p)
	}
	if n := doc.Fields["name"]; *n.Text != "5" || n.Words != "" {
		t.Errorf("name %+v", n)
	}
}

func TestAnalyzeDynamicModes(t *testing.T) {
	off := &Mapping{Fields: map[string]FieldType{"a": Keyword}, Dynamic: DynamicFalse}
	doc, update := mustAnalyze(t, off, "d", `{"a": "x", "b": "y"}`)
	if !update.Empty() || doc.Fields["b"].Present || !doc.Fields["a"].Present {
		t.Fatalf("dynamic false: %v %v", doc.Fields, update.Fields)
	}
	if !strings.Contains(string(doc.Body), `"b": "y"`) {
		t.Fatal("dynamic false dropped the field from the body")
	}

	strict := &Mapping{Fields: map[string]FieldType{"a": Keyword}, Dynamic: DynamicStrict}
	_, _, err := Analyze(strict, "d", []byte(`{"a": "x", "b": "y"}`))
	var verr *ValidationError
	if !errors.As(err, &verr) || verr.Field != "b" {
		t.Fatalf("strict: %v", err)
	}
	if _, _, err := Analyze(strict, "d", []byte(`{"a": "x"}`)); err != nil {
		t.Fatalf("strict, mapped only: %v", err)
	}
}

func TestAnalyzeRefuses(t *testing.T) {
	m := &Mapping{}
	tests := map[string]struct{ id, body, field string }{
		"empty id":         {"", `{}`, IDField},
		"long id":          {strings.Repeat("x", MaxIDBytes+1), `{}`, IDField},
		"NUL in id":        {"a\x00b", `{}`, IDField},
		"invalid UTF-8 id": {"a\xffb", `{}`, IDField},
		"not JSON":         {"d", `{"a": `, ""},
		"not an object":    {"d", `[1, 2]`, ""},
		"two values":       {"d", `{} {}`, ""},
		"_id in the body":  {"d", `{"_id": "x"}`, IDField},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, _, err := Analyze(m, tc.id, []byte(tc.body))
			var verr *ValidationError
			if !errors.As(err, &verr) {
				t.Fatalf("err = %v, want a ValidationError", err)
			}
			if verr.Field != tc.field {
				t.Fatalf("field %q, want %q (%v)", verr.Field, tc.field, err)
			}
		})
	}
	if _, _, err := Analyze(m, strings.Repeat("x", MaxIDBytes), []byte(`{}`)); err != nil {
		t.Fatalf("a %d-byte id: %v", MaxIDBytes, err)
	}
}

// TestAnalyzeReviewFocus round-trips the Review Focus 1 inputs through Analyze.
func TestAnalyzeReviewFocus(t *testing.T) {
	m := &Mapping{Fields: map[string]FieldType{
		"title": Text, "brand": Keyword, "sizes": KeywordList, "price": Number, "seen": Date, "ok": Bool,
	}}
	long := strings.Repeat("é", 500)
	body := `{
		"title": "Straße İstanbul ﬁsh a\u0000b 日本 😀 cafe\u0301 ` + long + `",
		"brand": "",
		"sizes": "S, , M,  L , \u3000, s",
		"price": 1e400,
		"seen": "2026-10-02T09:30:00.5Z",
		"ok": "true"
	}`
	doc, _ := mustAnalyze(t, m, "Ünïcode-ß", body)

	title := doc.Fields["title"]
	wantText := "strasse i\u0307stanbul fish a\ufffdb 日本 \U0001f600 cafe\u0301 " + long
	if *title.Text != wantText {
		t.Errorf("title text %+q,\nwant %+q", *title.Text, wantText)
	}
	wantWords := " strasse i stanbul fish a b 日本 café " + long + " "
	if title.Words != wantWords {
		t.Errorf("title words %+q,\nwant %+q", title.Words, wantWords)
	}
	if !title.Grams || !slices.Contains(analysis.Substrings3(*title.Text), "a\ufffdb") {
		t.Errorf("title grams miss the cleaned NUL")
	}

	if brand := doc.Fields["brand"]; !brand.Present || *brand.Text != "" || brand.Grams {
		t.Errorf("empty brand %+v", brand)
	}
	if sizes := doc.Fields["sizes"].Entries; !slices.Equal(sizes, []string{"l", "m", "s"}) {
		t.Errorf("sizes %q", sizes)
	}
	if price := doc.Fields["price"]; !price.Present || price.Number != nil {
		t.Errorf("infinite price %+v", price)
	}
	if seen := doc.Fields["seen"]; seen.Number == nil || *seen.Number != 1790933400500 {
		t.Errorf("seen %v", deref64(seen.Number))
	}
	if ok := doc.Fields["ok"]; !ok.Present || ok.Bool != nil {
		t.Errorf("string in a bool field %+v", ok)
	}
	if id := doc.Fields[IDField]; *id.Text != "ünïcode-ss" {
		t.Errorf("_id text %+q", *id.Text)
	}
}

func TestAnalyzeKeywordListArray(t *testing.T) {
	m := &Mapping{Fields: map[string]FieldType{"tags": KeywordList}}
	doc, _ := mustAnalyze(t, m, "d", `{"tags": [" Red ", "red", "a, b", 10, 1.0, true, null, [], {}]}`)
	want := []string{"1.0", "10", "a, b", "red", "true"}
	if got := doc.Fields["tags"].Entries; !slices.Equal(got, want) {
		t.Fatalf("entries %q, want %q", got, want)
	}
}

func TestAnalyzeBodyIsCopied(t *testing.T) {
	body := []byte(`{"a": "x"}`)
	doc, _, err := Analyze(&Mapping{}, "d", body)
	if err != nil {
		t.Fatal(err)
	}
	body[7] = 'y'
	if string(doc.Body) != `{"a": "x"}` {
		t.Fatalf("Body aliases the caller's buffer: %s", doc.Body)
	}
}

func BenchmarkAnalyze(b *testing.B) {
	m := &Mapping{Fields: map[string]FieldType{
		"title": Text, "brand": Keyword, "sizes": KeywordList, "price": Number, "active": Bool,
	}}
	body := []byte(`{"title": "Nike Air Max 90 Running Shoe - White/Black", "brand": "Nike",
		"sizes": "7, 8, 9, 10, 11", "price": 129.99, "active": true, "extra": "kept"}`)
	for b.Loop() {
		if _, _, err := Analyze(m, "product-123", body); err != nil {
			b.Fatal(err)
		}
	}
}

func deref64(f *float64) any {
	if f == nil {
		return nil
	}
	return *f
}
