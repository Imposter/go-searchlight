package schema

import (
	"encoding/json"
	"testing"

	"github.com/Imposter/go-searchlight/internal/analysis"
)

// FieldValue reads what Analyze reads: a field's value through FieldValue analyzes
// to the very Value Analyze gives the document.
func TestFieldValueAgreesWithAnalyze(t *testing.T) {
	m := &Mapping{Fields: map[string]FieldType{"a": Text, "b\uFFFDc": Keyword, "n": Number}}
	bodies := []string{
		`{"a":"x","a":"y"}`,
		`{"a":null,"a":"late"}`,
		`{"b\u0000c":"nul","b\uFFFDc":"plain"}`,
		`{"b\u0000c":null,"b\uFFFDc":"plain"}`,
		`{"b\uFFFDc":"first","b\u0000c":"second","b\uFFFDc":"again"}`,
		`{"n":1.50,"a":["l"],"z":{"q":1}}`,
		`{"a":null}`,
		`{}`,
	}
	for _, body := range bodies {
		doc, _, err := Analyze(m, "id", []byte(body))
		if err != nil {
			t.Fatalf("%s: %v", body, err)
		}
		for _, f := range []string{"a", "b\uFFFDc", "n"} {
			v, ok, err := FieldValue([]byte(body), f)
			if err != nil {
				t.Fatalf("%s %s: %v", body, f, err)
			}
			want, has := doc.Fields[f]
			if ok != (has && want.Present) {
				t.Fatalf("%s %s: present %v, Analyze says %v", body, f, ok, has && want.Present)
			}
			if !ok {
				continue
			}
			got := analyzeValue(m.Fields[f], v, true)
			if a, b := mustJSON(t, got), mustJSON(t, want); a != b {
				t.Errorf("%s %s: %s, Analyze %s", body, f, a, b)
			}
		}
	}
	if _, ok, _ := FieldValue([]byte(`{"x":1}`), IDField); ok {
		t.Error("FieldValue reads _id")
	}
	v, ok, _ := FieldValue([]byte(`{"t":"Ünïcode words"}`), "t")
	if s, isString := v.(string); !ok || !isString || analysis.Words(s) != " ünïcode words " {
		t.Errorf("words: %v %v", v, ok)
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
