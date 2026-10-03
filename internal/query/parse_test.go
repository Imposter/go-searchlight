package query

import (
	"fmt"
	"strings"
	"testing"
)

// mustParse parses raw and fails the test if Parse reports any problem.
func mustParse(t *testing.T, raw string) Node {
	t.Helper()
	n, problems := Parse([]byte(raw))
	if len(problems) > 0 {
		t.Fatalf("Parse(%s): %v", raw, problems)
	}
	return n
}

func TestParseRootEverything(t *testing.T) {
	n, problems := Parse([]byte(`{"all": []}`))
	if len(problems) != 0 {
		t.Fatalf("problems = %v", problems)
	}
	root, ok := n.(*All)
	if !ok || len(root.Children) != 0 {
		t.Fatalf("Parse({all: []}) = %#v", n)
	}
}

func TestParseEmptyGroupRefused(t *testing.T) {
	cases := []string{
		`{"any": []}`,
		`{"all": [{"any": []}]}`,
		`{"not": {"any": []}}`,
	}
	for _, raw := range cases {
		_, problems := Parse([]byte(raw))
		if len(problems) == 0 {
			t.Errorf("Parse(%s): no problem for an empty non-root group", raw)
			continue
		}
		if !strings.Contains(problems[0].Message, "empty group") {
			t.Errorf("Parse(%s) = %v, want an empty-group message", raw, problems)
		}
	}
}

func TestParseShapes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		loc  string
	}{
		{"not a json value", `{not json`, RootLoc},
		{"trailing garbage", `{"all": []} {}`, RootLoc},
		{"node not an object", `["all"]`, RootLoc},
		{"node not an object nested", `{"all": [1]}`, "query.all.0"},
		{"all not a list", `{"all": "x"}`, "query.all"},
		{"unknown key on all", `{"all": [], "any": []}`, "query.any"},
		{"unknown key on condition", `{"field": "f", "op": "eq", "value": 1, "junk": 1}`, "query.junk"},
		{"missing field", `{"op": "eq", "value": 1}`, "query.field"},
		{"non-string field", `{"field": 1, "op": "eq", "value": 1}`, "query.field"},
		{"missing op", `{"field": "f", "value": 1}`, "query.op"},
		{"unknown op", `{"field": "f", "op": "bogus"}`, "query.op"},
		{"non-string op", `{"field": "f", "op": 1}`, "query.op"},
		{"value holds NUL", `{"field": "f", "op": "eq", "value": "a\u0000b"}`, "query.value"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, problems := Parse([]byte(tc.raw))
			if len(problems) == 0 {
				t.Fatalf("Parse(%s): no problem", tc.raw)
			}
			if problems[0].Loc != tc.loc {
				t.Errorf("Parse(%s) loc = %q, want %q (%v)", tc.raw, problems[0].Loc, tc.loc, problems)
			}
		})
	}
}

func TestParseLeafLoc(t *testing.T) {
	raw := `{"all": [{"field": "a", "op": "eq", "value": 1}, {"any": [{"field": "b", "op": "eq", "value": "x\u0000y"}]}]}`
	_, problems := Parse([]byte(raw))
	if len(problems) != 1 || problems[0].Loc != "query.all.1.any.0.value" {
		t.Fatalf("problems = %v, want one at query.all.1.any.0.value", problems)
	}
}

func TestParseNotShape(t *testing.T) {
	// "all" and "any" are checked before "not", so a node with both "all" and "not"
	// keys is an all group with "not" as its extra key.
	_, problems := Parse([]byte(`{"not": {"field": "f", "op": "eq", "value": 1}, "all": []}`))
	if len(problems) == 0 {
		t.Fatal("not alongside all: no problem")
	}
	if problems[0].Loc != "query.not" {
		t.Errorf("loc = %q, want query.not", problems[0].Loc)
	}
	// With no "all"/"any", an extra key is reported as a not's own extra key.
	_, problems = Parse([]byte(`{"not": {"field": "f", "op": "eq", "value": 1}, "op": "x"}`))
	if len(problems) == 0 {
		t.Fatal("not with an unrelated extra key: no problem")
	}
	if problems[0].Loc != "query.op" {
		t.Errorf("loc = %q, want query.op", problems[0].Loc)
	}
}

func TestParseValueShapes(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		ok   bool
	}{
		{"scalar string", `{"field": "f", "op": "eq", "value": "x"}`, true},
		{"scalar number", `{"field": "f", "op": "eq", "value": 1.5}`, true},
		{"scalar bool", `{"field": "f", "op": "eq", "value": true}`, true},
		{"null value", `{"field": "f", "op": "eq", "value": null}`, true},
		{"no value", `{"field": "f", "op": "eq"}`, true},
		{"list of scalars", `{"field": "f", "op": "in", "value": [1, "a", true]}`, true},
		{"similar object", `{"field": "f", "op": "similar", "value": {"text": "x", "min": 0.5}}`, true},
		{"similar unknown key", `{"field": "f", "op": "similar", "value": {"text": "x", "min": 0.5, "junk": 1}}`, false},
		{"list of objects", `{"field": "f", "op": "in", "value": [{}]}`, false},
		{"value is an array of arrays", `{"field": "f", "op": "in", "value": [[1]]}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, problems := Parse([]byte(tc.raw))
			if (len(problems) == 0) != tc.ok {
				t.Errorf("Parse(%s): problems = %v, want ok=%v", tc.raw, problems, tc.ok)
			}
		})
	}
}

func TestParseBoundsValueLength(t *testing.T) {
	long := strings.Repeat("a", MaxValueChars+1)
	ok := strings.Repeat("a", MaxValueChars)
	for _, tc := range []struct {
		name string
		s    string
		want bool // true: a problem is expected
	}{
		{"at the limit", ok, false},
		{"over the limit", long, true},
	} {
		raw := fmt.Sprintf(`{"field": "f", "op": "eq", "value": %q}`, tc.s)
		_, problems := Parse([]byte(raw))
		if (len(problems) > 0) != tc.want {
			t.Errorf("%s: problems = %v, want a problem=%v", tc.name, problems, tc.want)
		}
	}
}

func TestParseBoundsListLength(t *testing.T) {
	item := `"a"`
	okList := "[" + strings.TrimSuffix(strings.Repeat(item+",", MaxListItems), ",") + "]"
	longList := "[" + strings.Repeat(item+",", MaxListItems+1) + "0]"
	for _, tc := range []struct {
		name string
		list string
		want bool
	}{
		{"at the limit", okList, false},
		{"over the limit", longList, true},
	} {
		raw := fmt.Sprintf(`{"field": "f", "op": "in", "value": %s}`, tc.list)
		_, problems := Parse([]byte(raw))
		if (len(problems) > 0) != tc.want {
			t.Errorf("%s: problems = %v, want a problem=%v", tc.name, problems, tc.want)
		}
	}
}

func TestParseBoundsFieldLength(t *testing.T) {
	long := strings.Repeat("a", MaxFieldChars+1)
	raw := fmt.Sprintf(`{"field": %q, "op": "eq", "value": 1}`, long)
	_, problems := Parse([]byte(raw))
	if len(problems) == 0 {
		t.Fatal("a field name over the limit: no problem")
	}
	if problems[0].Loc != "query.field" {
		t.Errorf("loc = %q, want query.field", problems[0].Loc)
	}
}

func leafJSON() string {
	return `{"field": "f", "op": "eq", "value": 1}`
}

func groupJSON(key string, n int, leaf string) string {
	items := make([]string, n)
	for i := range items {
		items[i] = leaf
	}
	return fmt.Sprintf(`{%q: [%s]}`, key, strings.Join(items, ","))
}

func TestParseBoundsDepth(t *testing.T) {
	// MaxDepth levels of "all" nesting is fine; one more is refused.
	inner := leafJSON()
	raw := inner
	for i := 0; i < MaxDepth; i++ {
		raw = groupJSON("all", 1, raw)
	}
	if _, problems := Parse([]byte(raw)); len(problems) != 0 {
		t.Fatalf("at MaxDepth: problems = %v", problems)
	}
	raw = groupJSON("all", 1, raw)
	_, problems := Parse([]byte(raw))
	if len(problems) == 0 || !strings.Contains(problems[0].Message, "deep") {
		t.Fatalf("over MaxDepth: problems = %v", problems)
	}
}

func TestParseBoundsLeaves(t *testing.T) {
	raw := groupJSON("all", MaxLeaves, leafJSON())
	if _, problems := Parse([]byte(raw)); len(problems) != 0 {
		t.Fatalf("at MaxLeaves: problems = %v", problems)
	}
	raw = groupJSON("all", MaxLeaves+1, leafJSON())
	_, problems := Parse([]byte(raw))
	if len(problems) == 0 || !strings.Contains(problems[0].Message, "conditions") {
		t.Fatalf("over MaxLeaves: problems = %v", problems)
	}
}

// nodesCase builds a root all group of "wrapped" children (each {"any": [leaf]}, two
// nodes apiece) plus "direct" leaf children (one node apiece), to grow the node count
// independently of the leaf count (MaxLeaves is well below MaxNodes).
func nodesCase(wrapped, direct int) string {
	items := make([]string, 0, wrapped+direct)
	for i := 0; i < wrapped; i++ {
		items = append(items, groupJSON("any", 1, leafJSON()))
	}
	for i := 0; i < direct; i++ {
		items = append(items, leafJSON())
	}
	return fmt.Sprintf(`{"all": [%s]}`, strings.Join(items, ","))
}

func TestParseBoundsNodes(t *testing.T) {
	// 49 wrapped (98 nodes) + 1 direct (1 node) + the root = 100 nodes, 50 leaves: at
	// both limits, no problem.
	raw := nodesCase(49, 1)
	if _, problems := Parse([]byte(raw)); len(problems) != 0 {
		t.Fatalf("at MaxNodes: problems = %v", problems)
	}
	// 50 wrapped (100 nodes) + the root = 101 nodes, 50 leaves: over MaxNodes only.
	raw = nodesCase(50, 0)
	_, problems := Parse([]byte(raw))
	if len(problems) == 0 || !strings.Contains(problems[0].Message, "nodes") {
		t.Fatalf("over MaxNodes: problems = %v", problems)
	}
}

func TestParseKeyGivenTwiceKeepsLast(t *testing.T) {
	n, problems := Parse([]byte(`{"field": "f", "op": "eq", "value": 1, "value": 2}`))
	if len(problems) != 0 {
		t.Fatalf("problems = %v", problems)
	}
	leaf, ok := n.(*Leaf)
	if !ok || string(leaf.Value) != "2" {
		t.Fatalf("Parse kept %#v, want value 2", n)
	}
}

// Decoded reads a parsed leaf's value, and decodes a hand-built leaf's raw value the
// same way.
func TestLeafDecoded(t *testing.T) {
	parsed, ok := mustParse(t, `{"field":"a","op":"in","value":[" X ",1,true]}`).(*Leaf)
	if !ok {
		t.Fatal("not a leaf")
	}
	byHand := &Leaf{Field: "a", Op: OpIn, Value: []byte(`[" X ",1,true]`)}
	for _, l := range []*Leaf{parsed, byHand} {
		a := l.Decoded()
		if a.Kind != ArgList || len(a.List) != 3 || a.List[0].Norm != "x" || a.List[1].Number != 1 || !a.List[2].Bool {
			t.Fatalf("Decoded() = %+v", a)
		}
	}
	if a := (&Leaf{Field: "a", Op: OpExists}).Decoded(); a.Kind != ArgNone {
		t.Fatalf("no value decoded as %+v", a)
	}
}
