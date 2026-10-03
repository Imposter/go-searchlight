package query

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
)

func testMapping() *schema.Mapping {
	return &schema.Mapping{Fields: map[string]schema.FieldType{
		"kw":   schema.Keyword,
		"text": schema.Text,
		"kl":   schema.KeywordList,
		"num":  schema.Number,
		"bl":   schema.Bool,
		"dt":   schema.Date,
	}}
}

func TestValidateEverythingRoot(t *testing.T) {
	if ps := Validate(&All{}, testMapping()); len(ps) != 0 {
		t.Fatalf("Validate({all: []}) = %v", ps)
	}
}

func TestValidateNilQuery(t *testing.T) {
	ps := Validate(nil, testMapping())
	if len(ps) != 1 || ps[0].Loc != RootLoc {
		t.Fatalf("Validate(nil) = %v", ps)
	}
}

func TestValidateUnknownField(t *testing.T) {
	n := mustParse(t, `{"field": "nope", "op": "eq", "value": "x"}`)
	ps := Validate(n, testMapping())
	if len(ps) != 1 || ps[0].Loc != "query.field" {
		t.Fatalf("Validate = %v", ps)
	}
}

func TestValidateIDPseudoField(t *testing.T) {
	n := mustParse(t, `{"field": "_id", "op": "eq", "value": "doc1"}`)
	if ps := Validate(n, testMapping()); len(ps) != 0 {
		t.Fatalf("Validate(_id eq) = %v", ps)
	}
	n = mustParse(t, `{"field": "_id", "op": "exists"}`)
	if ps := Validate(n, testMapping()); len(ps) != 0 {
		t.Fatalf("Validate(_id exists) = %v", ps)
	}
	// _id is a keyword: words_all is text-only.
	n = mustParse(t, `{"field": "_id", "op": "words_all", "value": ["x"]}`)
	if ps := Validate(n, testMapping()); len(ps) == 0 {
		t.Fatal("Validate(_id words_all): no problem")
	}
	// _id works with a nil mapping too.
	n = mustParse(t, `{"field": "_id", "op": "eq", "value": "doc1"}`)
	if ps := Validate(n, nil); len(ps) != 0 {
		t.Fatalf("Validate(_id eq, nil mapping) = %v", ps)
	}
}

// TestValidateOpsByType checks every (type, op) pair against spec section 3's table:
// an op the type's list offers is accepted (given a value that fits), and every op not
// on the list is refused, naming the allowed ops.
func TestValidateOpsByType(t *testing.T) {
	fieldOf := map[schema.FieldType]string{
		schema.Keyword:     "kw",
		schema.Text:        "text",
		schema.KeywordList: "kl",
		schema.Number:      "num",
		schema.Bool:        "bl",
		schema.Date:        "dt",
	}
	valueOf := map[string]string{
		OpEq: `"x"`, OpNe: `"x"`, OpIn: `["x"]`,
		OpLt: "1", OpLte: "1", OpGt: "1", OpGte: "1", OpBetween: "[1, 2]",
		OpExists: "true", OpContains: `"x"`, OpContainsAny: `["x"]`, OpContainsAll: `["x"]`,
		OpStartsWith: `"x"`, OpWordsAll: `["x"]`, OpWordsAny: `["x"]`,
		OpSimilar: `{"text": "x", "min": 0.5}`,
		OpHas:     `"x"`, OpHasAny: `["x"]`, OpHasAll: `["x"]`,
		OpEmpty: "", OpNonempty: "",
	}
	numValueOf := map[string]string{
		OpEq: "1", OpNe: "1", OpIn: "[1]",
		OpLt: "1", OpLte: "1", OpGt: "1", OpGte: "1", OpBetween: "[1, 2]", OpExists: "true",
	}
	boolValueOf := map[string]string{OpEq: "true", OpNe: "true", OpExists: "true"}
	for ft, field := range fieldOf {
		offered := map[string]bool{}
		for _, op := range OpsFor(ft) {
			offered[op] = true
		}
		for _, op := range Ops {
			values := valueOf
			switch ft {
			case schema.Number, schema.Date:
				values = numValueOf
			case schema.Bool:
				values = boolValueOf
			}
			value, hasValue := values[op]
			if !hasValue && offered[op] {
				t.Fatalf("test table missing a value for %s on %s", op, ft)
			}
			raw := fmt.Sprintf(`{"field": %q, "op": %q`, field, op)
			if value != "" {
				raw += `, "value": ` + value
			}
			raw += "}"
			n, problems := Parse([]byte(raw))
			if len(problems) != 0 {
				t.Fatalf("Parse(%s) = %v", raw, problems)
			}
			ps := Validate(n, testMapping())
			switch {
			case offered[op] && len(ps) != 0:
				t.Errorf("%s on %s: Validate = %v, want no problem", op, ft, ps)
			case !offered[op] && len(ps) == 0:
				t.Errorf("%s on %s: Validate: no problem, want one", op, ft)
			case !offered[op]:
				if !strings.Contains(ps[0].Message, "use") {
					t.Errorf("%s on %s: message %q does not name the allowed ops", op, ft, ps[0].Message)
				}
			}
		}
	}
}

func TestValidateValueShapes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		ok   bool
	}{
		{"between ok", `{"field": "num", "op": "between", "value": [1, 2]}`, true},
		{"between lo==hi ok", `{"field": "num", "op": "between", "value": [1, 1]}`, true},
		{"between lo>hi", `{"field": "num", "op": "between", "value": [2, 1]}`, false},
		{"between wrong arity", `{"field": "num", "op": "between", "value": [1, 2, 3]}`, false},
		{"between non-number", `{"field": "num", "op": "between", "value": ["a", 2]}`, false},

		{"similar ok", `{"field": "text", "op": "similar", "value": {"text": "x", "min": 0.5}}`, true},
		{"similar min==1 ok", `{"field": "text", "op": "similar", "value": {"text": "x", "min": 1}}`, true},
		{"similar min==0", `{"field": "text", "op": "similar", "value": {"text": "x", "min": 0}}`, false},
		{"similar min>1", `{"field": "text", "op": "similar", "value": {"text": "x", "min": 1.5}}`, false},
		{"similar blank text", `{"field": "text", "op": "similar", "value": {"text": "  ", "min": 0.5}}`, false},
		{"similar missing min", `{"field": "text", "op": "similar", "value": {"text": "x"}}`, false},
		{"similar not an object", `{"field": "text", "op": "similar", "value": "x"}`, false},

		{"in on text ok", `{"field": "kw", "op": "in", "value": ["a", "b"]}`, true},
		{"in on text with a number", `{"field": "kw", "op": "in", "value": ["a", 1]}`, false},
		{"in on number ok", `{"field": "num", "op": "in", "value": [1, 2]}`, true},
		{"in empty list", `{"field": "kw", "op": "in", "value": []}`, false},

		{"contains_any ok", `{"field": "kw", "op": "contains_any", "value": ["a", "b"]}`, true},
		{"contains_any blank entry", `{"field": "kw", "op": "contains_any", "value": ["a", "  "]}`, false},
		{"contains_any empty list", `{"field": "kw", "op": "contains_any", "value": []}`, false},

		{"has ok", `{"field": "kl", "op": "has", "value": "a"}`, true},
		{"has with a comma", `{"field": "kl", "op": "has", "value": "a, b"}`, false},
		{"has blank", `{"field": "kl", "op": "has", "value": "  "}`, false},

		{"words_all ok", `{"field": "text", "op": "words_all", "value": ["a b"]}`, true},
		{"words_all no word", `{"field": "text", "op": "words_all", "value": ["!!"]}`, false},

		{"empty takes no value", `{"field": "kl", "op": "empty", "value": true}`, false},
		{"exists true", `{"field": "kw", "op": "exists", "value": true}`, true},
		{"exists non-bool", `{"field": "kw", "op": "exists", "value": "x"}`, false},

		{"eq list on keyword", `{"field": "kw", "op": "eq", "value": ["a"]}`, false},
		{"eq bool on keyword", `{"field": "kw", "op": "eq", "value": true}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n, problems := Parse([]byte(tc.raw))
			if len(problems) != 0 {
				t.Fatalf("Parse(%s) = %v", tc.raw, problems)
			}
			ps := Validate(n, testMapping())
			if (len(ps) == 0) != tc.ok {
				t.Errorf("Validate(%s) = %v, want ok=%v", tc.raw, ps, tc.ok)
			}
		})
	}
}

func TestValidateBoundsReused(t *testing.T) {
	// A tree built by hand (not through Parse) is held to the same bounds.
	n := &Leaf{Field: "kw", Op: OpContains, Value: []byte(`"` + strings.Repeat("a", MaxValueChars+1) + `"`)}
	ps := Validate(n, testMapping())
	if len(ps) == 0 {
		t.Fatal("Validate: a value over MaxValueChars built by hand: no problem")
	}
}

func TestValidateGroupAndNodeShape(t *testing.T) {
	ps := Validate(&All{Children: []Node{}}, testMapping())
	if len(ps) != 0 {
		t.Fatalf("Validate(all: []) at non-root should be fine via empty-root shortcut only if root; here it's root so ok: %v", ps)
	}
	ps = Validate(&Any{Children: []Node{&Any{Children: nil}}}, testMapping())
	if len(ps) == 0 {
		t.Fatal("Validate: nested empty any: no problem")
	}
	ps = Validate(&All{Children: []Node{nil}}, testMapping())
	if len(ps) == 0 {
		t.Fatal("Validate: a nil child: no problem")
	}
	ps = Validate(&Not{Child: nil}, testMapping())
	if len(ps) == 0 {
		t.Fatal("Validate: not with a nil child: no problem")
	}
}
