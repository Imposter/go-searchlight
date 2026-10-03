package schema

import (
	"bytes"
	"fmt"
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
