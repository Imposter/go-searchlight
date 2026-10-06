package schema

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// Check agrees with Analyze on every outcome but the values: the same errors, the
// same mapping update, the same body.
func TestCheckAgreesWithAnalyze(t *testing.T) {
	mappings := []*Mapping{
		{Fields: map[string]FieldType{"title": Text}},
		{Dynamic: DynamicStrict, Fields: map[string]FieldType{"title": Text}},
		{Dynamic: DynamicFalse, Fields: map[string]FieldType{"title": Text}},
	}
	bodies := []string{
		`{"title": "a", "price": 3, "tags": ["x"], "ok": true, "empty": [], "obj": {"a": 1}}`,
		`{"title": 5}`,
		"{\"title\": \"\xff\xfe\", \"n\u0000ame\": 1}",
		`[1]`,
		`{"title": `,
		`{"_id": "x"}`,
	}
	for mi, m := range mappings {
		for _, b := range bodies {
			doc, du, derr := Analyze(m, "id", []byte(b))
			body, cu, cerr := Check(m, "id", []byte(b))
			if fmt.Sprint(derr) != fmt.Sprint(cerr) || fmt.Sprint(du) != fmt.Sprint(cu) || !bytes.Equal(doc.Body, body) {
				t.Errorf("mapping %d, body %q:\nAnalyze %q %v %v\nCheck   %q %v %v", mi, b, doc.Body, du, derr, body, cu, cerr)
			}
		}
	}
}

// AnalyzeForMatch agrees with Analyze on everything but the grams: the same errors,
// mapping update and body bytes, and every value as Analyze makes it with its Grams and
// GramsTruncated cleared.
func TestAnalyzeForMatchAgreesWithAnalyze(t *testing.T) {
	mappings := []*Mapping{
		{Fields: map[string]FieldType{"title": Text, "brand": Keyword, "tags": KeywordList}},
		{Dynamic: DynamicStrict, Fields: map[string]FieldType{"title": Text}},
		{Dynamic: DynamicFalse, Fields: map[string]FieldType{"title": Text}},
	}
	long := strings.Repeat("filler words ", MaxGramChars/12+1)
	bodies := []string{
		`{"title": "RTX 4090 Founders", "brand": "Acme", "price": 3, "tags": ["x", "Y"], "ok": true, "empty": [], "obj": {"a": 1}}`,
		`{"title": "` + long + `", "brand": "` + long + `"}`,
		`{"title": 5, "brand": ["a"], "tags": "a, b"}`,
		"{\"title\": \"\xff\xfe\", \"n\u0000ame\": 1}",
		`[1]`,
		`{"title": `,
		`{"_id": "x"}`,
	}
	for mi, m := range mappings {
		for _, b := range bodies {
			full, fu, ferr := Analyze(m, "id", []byte(b))
			match, mu, merr := AnalyzeForMatch(m, "id", []byte(b))
			if fmt.Sprint(ferr) != fmt.Sprint(merr) || fmt.Sprint(fu) != fmt.Sprint(mu) || !bytes.Equal(full.Body, match.Body) {
				t.Fatalf("mapping %d, body %q:\nAnalyze %q %v %v\nAnalyzeForMatch %q %v %v", mi, b, full.Body, fu, ferr, match.Body, mu, merr)
			}
			for name, v := range full.Fields {
				v.Grams, v.GramsTruncated = nil, false
				if got := match.Fields[name]; !reflect.DeepEqual(got, v) {
					t.Fatalf("mapping %d, body %q, field %s: %+v, want %+v", mi, b, name, got, v)
				}
			}
			if len(match.Fields) != len(full.Fields) {
				t.Fatalf("mapping %d, body %q: %d fields, want %d", mi, b, len(match.Fields), len(full.Fields))
			}
		}
	}
}
