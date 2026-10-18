package query

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/schema"
)

func nestedMapping() *schema.Mapping {
	return &schema.Mapping{Fields: map[string]schema.FieldType{
		"brand":       schema.Keyword,
		"_v":          schema.Nested,
		"_v.size":     schema.Keyword,
		"_v.price":    schema.Number,
		"_v.in_stock": schema.Bool,
		"_v.seen":     schema.Date,
	}}
}

func mustAnalyzeNested(t *testing.T, body string) *schema.Doc {
	t.Helper()
	d, _, err := schema.Analyze(nestedMapping(), "d", []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return &d
}

// A nested node holds when one object satisfies its whole query: size L, in stock and
// under 150 on one variant, never across two; a not inside applies per object; an
// empty query holds of any object, and a member that is no object matches nothing.
func TestNestedMatch(t *testing.T) {
	q := func(inner string) Node {
		n, ps := Parse([]byte(`{"nested": {"path": "_v", "query": ` + inner + `}}`))
		if len(ps) == 0 {
			ps = Validate(n, nestedMapping())
		}
		if len(ps) > 0 {
			t.Fatal(ps)
		}
		return n
	}
	correlated := q(`{"all": [{"field": "_v.size", "op": "eq", "value": "L"}, {"field": "_v.in_stock", "op": "eq", "value": true}, {"field": "_v.price", "op": "lt", "value": 150}]}`)
	split := mustAnalyzeNested(t, `{"_v": [{"size": "L", "in_stock": false, "price": 100}, {"size": "M", "in_stock": true, "price": 100}]}`)
	one := mustAnalyzeNested(t, `{"_v": [{"size": "M", "in_stock": true}, {"size": "l", "in_stock": true, "price": 149}]}`)
	none := mustAnalyzeNested(t, `{"_v": ["L", 7]}`)
	empty := mustAnalyzeNested(t, `{"_v": []}`)
	absent := mustAnalyzeNested(t, `{"brand": "x"}`)
	if Match(correlated, split) || !Match(correlated, one) || Match(correlated, none) {
		t.Fatal("the conditions are not read on one object")
	}
	notL := q(`{"not": {"field": "_v.size", "op": "eq", "value": "L"}}`)
	if !Match(notL, split) || Match(notL, mustAnalyzeNested(t, `{"_v": [{"size": "L"}]}`)) || Match(notL, none) {
		t.Fatal("a not inside does not apply per object")
	}
	every := q(`{"all": []}`)
	if !Match(every, split) || Match(every, none) || Match(every, empty) || Match(every, absent) {
		t.Fatal("an empty query does not hold of exactly the documents with an object")
	}
	outside := &Not{Child: every}
	if !Match(outside, none) || !Match(outside, absent) || Match(outside, one) {
		t.Fatal("a not of a nested node")
	}
	c := Compile(&All{Children: []Node{&Leaf{Field: "_v.size", Op: OpEq, Value: []byte(`"L"`)}}})
	if obj := one.Fields["_v"].Objects[1]; !c.MatchObject(&obj) {
		t.Fatal("MatchObject misses its object")
	}
	if obj := none.Fields["_v"].Objects[0]; c.MatchObject(&obj) {
		t.Fatal("MatchObject matches a member that is no object")
	}
}

// Parse reads nested's path, query and inner_hits, each refusal at its loc; its levels
// count toward MaxDepth and its conditions toward MaxLeaves.
func TestNestedParse(t *testing.T) {
	n, ps := Parse([]byte(`{"nested": {"path": "_v", "query": {"field": "_v.size", "op": "eq", "value": "L"}, "inner_hits": {"name": "v", "size": 2, "fields": ["_v.size"]}}}`))
	if len(ps) > 0 {
		t.Fatal(ps)
	}
	x, ok := n.(*Nested)
	if !ok || x.Path != "_v" || x.InnerHits == nil || x.InnerHits.Size != 2 || x.InnerHits.Key("_v") != "v" || x.InnerHits.Fields[0] != "_v.size" {
		t.Fatalf("parsed %#v", n)
	}
	n, _ = Parse([]byte(`{"nested": {"path": "_v", "inner_hits": {}}}`))
	if x, ok := n.(*Nested); !ok || x.InnerHits.Size != DefaultInnerHitsSize || !isEveryObject(x.Query) {
		t.Fatalf("defaults: %#v", n)
	}
	for _, c := range []struct{ raw, loc, want string }{
		{`{"nested": 1}`, "query.nested", "nested is"},
		{`{"nested": {"path": "_v"}, "x": 1}`, "query.x", "a nested node holds only"},
		{`{"nested": {"query": {"all": []}}}`, "query.nested.path", "needs the path"},
		{`{"nested": {"path": 3}}`, "query.nested.path", "a field name"},
		{`{"nested": {"path": "_v", "q": 1}}`, "query.nested.q", "takes path, query and inner_hits"},
		{`{"nested": {"path": "_v", "query": {"any": []}}}`, "query.nested.query.any", "empty group"},
		{`{"nested": {"path": "_v", "inner_hits": {"size": -1}}}`, "query.nested.inner_hits.size", "from 0 to"},
		{`{"nested": {"path": "_v", "inner_hits": {"size": 1001}}}`, "query.nested.inner_hits.size", "from 0 to"},
		{`{"nested": {"path": "_v", "inner_hits": {"fields": "_v.size"}}}`, "query.nested.inner_hits.fields", "a list"},
		{`{"nested": {"path": "_v", "inner_hits": {"name": ""}}}`, "query.nested.inner_hits.name", "non-empty"},
		{`{"nested": {"path": "_v", "inner_hits": {"from": 1}}}`, "query.nested.inner_hits.from", "takes name, size and fields"},
		{`{"all": [{"nested": {"path": "_v", "query": {"all": [{"any": [{"all": [{"field": "_v.size", "op": "exists"}]}]}]}}}]}`, "query", "deep"},
	} {
		_, ps := Parse([]byte(c.raw))
		if len(ps) == 0 || ps[0].Loc != c.loc || !strings.Contains(ps[0].Message, c.want) {
			t.Errorf("%s: %v, want %q at %s", c.raw, ps, c.want, c.loc)
		}
	}
	leaves := make([]string, MaxLeaves)
	for i := range leaves {
		leaves[i] = `{"field": "_v.size", "op": "exists"}`
	}
	raw := `{"all": [{"field": "brand", "op": "exists"}, {"nested": {"path": "_v", "query": {"any": [` + strings.Join(leaves[1:], ",") + `]}}}]}`
	if _, ps := Parse([]byte(raw)); len(ps) > 0 {
		t.Fatalf("%d leaves: %v", MaxLeaves, ps)
	}
	raw = `{"all": [{"field": "brand", "op": "exists"}, {"nested": {"path": "_v", "query": {"any": [` + strings.Join(leaves, ",") + `]}}}]}`
	if _, ps := Parse([]byte(raw)); len(ps) == 0 || !strings.Contains(ps[0].Message, "conditions") {
		t.Fatalf("%d leaves: %v", MaxLeaves+1, ps)
	}
}

// Validate's nested rules: the path is a nested field, conditions name its sub-fields
// inside and never outside, no nested node nests, inner_hits ask for sub-fields, under
// no not and with keys of their own; ValidateInner holds an aggregation's or a sort's
// where to the inner rules.
func TestNestedValidate(t *testing.T) {
	m := nestedMapping()
	for _, c := range []struct{ raw, loc, want string }{
		{`{"field": "_v.size", "op": "eq", "value": "L"}`, "query.field", "is a sub-field of nested field"},
		{`{"nested": {"path": "brand"}}`, "query.nested.path", "is not a nested field"},
		{`{"nested": {"path": "_v", "query": {"field": "brand", "op": "eq", "value": "L"}}}`, "query.nested.query.field", "names its sub-fields"},
		{`{"nested": {"path": "_v", "query": {"nested": {"path": "_v"}}}}`, "query.nested.query.nested", "not inside another"},
		{`{"not": {"all": [{"nested": {"path": "_v", "inner_hits": {}}}]}}`, "query.not.all.0.nested.inner_hits", "under a not"},
		{`{"nested": {"path": "_v", "inner_hits": {"fields": ["brand"]}}}`, "query.nested.inner_hits.fields.0", "not a sub-field"},
		{`{"any": [{"nested": {"path": "_v", "inner_hits": {}}}, {"nested": {"path": "_v", "inner_hits": {}}}]}`, "query.any.1.nested.inner_hits", "two inner_hits"},
		{`{"nested": {"path": "_v", "query": {"field": "_v.size", "op": "lt", "value": 3}}}`, "query.nested.query.op", "use contains"},
		{`{"field": "_v", "op": "eq", "value": 3}`, "query.op", "use exists"},
	} {
		n, ps := Parse([]byte(c.raw))
		if len(ps) == 0 {
			ps = Validate(n, m)
		}
		if len(ps) == 0 || ps[0].Loc != c.loc || !strings.Contains(ps[0].Message, c.want) {
			t.Errorf("%s: %v, want %q at %s", c.raw, ps, c.want, c.loc)
		}
	}
	for _, raw := range []string{
		`{"any": [{"nested": {"path": "_v", "inner_hits": {}}}, {"nested": {"path": "_v", "inner_hits": {"name": "other"}}}]}`,
		`{"all": [{"field": "_v", "op": "exists"}, {"not": {"nested": {"path": "_v", "query": {"not": {"field": "_v.price", "op": "gt", "value": 1}}}}}]}`,
	} {
		n, ps := Parse([]byte(raw))
		if len(ps) == 0 {
			ps = Validate(n, m)
		}
		if len(ps) > 0 {
			t.Errorf("%s: %v", raw, ps)
		}
	}
	for raw, want := range map[string]string{
		`{"field": "_v.size", "op": "eq", "value": "L"}`: "",
		`{"all": []}`: "",
		`{"field": "brand", "op": "eq", "value": "L"}`:           "query.field",
		`{"nested": {"path": "_v"}}`:                             "query.nested",
		`{"not": {"field": "_v.price", "op": "lt", "value": 1}}`: "",
	} {
		n, ps := Parse([]byte(raw))
		if len(ps) == 0 {
			ps = ValidateInner(n, m, "_v")
		}
		if (want == "") != (len(ps) == 0) || (want != "" && ps[0].Loc != want) {
			t.Errorf("inner %s: %v, want a problem at %q", raw, ps, want)
		}
	}
}

// Canonical encodes a nested node's path and query, not its inner_hits; Resolve
// resolves date math inside one, keeping its inner_hits; Walk descends into it.
func TestNestedCanonicalResolveWalk(t *testing.T) {
	parse := func(raw string) Node {
		n, ps := Parse([]byte(raw))
		if len(ps) > 0 {
			t.Fatal(ps)
		}
		return n
	}
	a := parse(`{"nested": {"path": "_v", "query": {"field": "_v.size", "op": "in", "value": ["L", "m"]}, "inner_hits": {"size": 2}}}`)
	b := parse(`{"nested": {"path": "_v", "query": {"field": "_v.size", "op": "in", "value": ["M", "l"]}}}`)
	c := parse(`{"nested": {"path": "_w", "query": {"field": "_v.size", "op": "in", "value": ["M", "l"]}}}`)
	flat := parse(`{"field": "_v.size", "op": "in", "value": ["M", "l"]}`)
	if !bytes.Equal(Canonical(a), Canonical(b)) || bytes.Equal(Canonical(b), Canonical(c)) || bytes.Equal(Canonical(b), Canonical(flat)) {
		t.Fatal("canonical forms")
	}
	if !bytes.Equal(Canonical(parse(`{"nested": {"path": "_v"}}`)), Canonical(parse(`{"nested": {"path": "_v", "query": {"all": []}}}`))) {
		t.Fatal("a missing query is not every object's")
	}
	dm := parse(`{"nested": {"path": "_v", "query": {"field": "_v.seen", "op": "gte", "value": "now-1d/d"}, "inner_hits": {}}}`)
	if !HasDateMath(dm) {
		t.Fatal("date math inside a nested node is not seen")
	}
	r, ps := Resolve(dm, time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC))
	if x, ok := r.(*Nested); len(ps) > 0 || HasDateMath(r) || !ok || x.InnerHits == nil {
		t.Fatalf("resolved %#v, %v", r, ps)
	}
	var locs []string
	Walk(a, func(loc string, _ Node) bool {
		locs = append(locs, loc)
		return true
	})
	if strings.Join(locs, " ") != "query query.nested.query" {
		t.Fatalf("walked %v", locs)
	}
}
