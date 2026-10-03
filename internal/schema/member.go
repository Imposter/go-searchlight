package schema

import (
	"bytes"
	"encoding/json"
	"unicode/utf8"

	"github.com/Imposter/go-searchlight/internal/analysis"
)

// FieldValue returns the value Analyze reads for field from a document body, with
// Analyze's member rules and no mapping: the top-level member whose name, NUL cleaned
// ([analysis.Clean]), is field; of several such names the first non-null one in
// document order, a name given twice keeping its last value; null is absent. The
// value is decoded as Analyze decodes it (numbers as json.Number). False when no
// member holds a value for field, or field is no name Analyze indexes (IDField, empty,
// longer than MaxFieldChars).
//
// It reads a stored body for what the index does not keep, a text field's words, say,
// without re-analyzing the document: so it answers as the document was indexed even
// when the mapping has changed since (a field mapped later, a strict mapping that now
// refuses another member).
func FieldValue(body []byte, field string) (any, bool, error) {
	if field == IDField || field == "" || utf8.RuneCountInString(field) > MaxFieldChars {
		return nil, false, nil
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	invalid := func(err error) error {
		return &ValidationError{Message: "the document is not valid JSON: " + err.Error()}
	}
	tok, err := dec.Token()
	if err != nil {
		return nil, false, invalid(err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return nil, false, &ValidationError{Message: "a document is a JSON object"}
	}
	type rawMember struct {
		name  string
		value json.RawMessage
	}
	var members []rawMember
	at := map[string]int{}
	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, false, invalid(err)
		}
		name, ok := tok.(string)
		if !ok {
			return nil, false, &ValidationError{Message: "the document is not valid JSON"}
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, false, invalid(err)
		}
		if analysis.Clean(name) != field {
			continue // only the names that can be field's matter
		}
		if i, seen := at[name]; seen {
			members[i].value = value // a repeated key: first position, last value
			continue
		}
		at[name] = len(members)
		members = append(members, rawMember{name, value})
	}
	for _, m := range members {
		if string(bytes.TrimSpace(m.value)) == "null" {
			continue
		}
		vd := json.NewDecoder(bytes.NewReader(m.value))
		vd.UseNumber()
		var v any
		if err := vd.Decode(&v); err != nil {
			return nil, false, invalid(err)
		}
		return v, true, nil
	}
	return nil, false, nil
}
