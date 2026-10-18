package query

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/schema"
)

func boxMapping() *schema.Mapping {
	return &schema.Mapping{Fields: map[string]schema.FieldType{
		"xy":  schema.PointType(schema.Number, schema.Number),
		"at":  schema.PointType(schema.Date, schema.Number),
		"num": schema.Number,
	}}
}

func boxLeaf(field, value string) string {
	return `{"field": "` + field + `", "op": "box", "value": ` + value + `}`
}

// What box takes, each refusal at its own loc: an object of x and y, each of at most
// one lower and one upper bound, numbers, and date math only on a date axis; an axis
// with nothing between its bounds; box on a field that is no point, and a point
// field's other ops.
func TestBoxValidation(t *testing.T) {
	for _, c := range []struct{ query, loc, want string }{
		{boxLeaf("xy", `[1, 2]`), "query.value", "box needs"},
		{boxLeaf("xy", `5`), "query.value", "box needs"},
		{boxLeaf("xy", `{"z": {"gte": 1}}`), "query.value.z", `unknown key "z"`},
		{boxLeaf("xy", `{"x": 1}`), "query.value.x", "an object of bounds"},
		{boxLeaf("xy", `{"x": {}}`), "query.value.x", "needs a bound"},
		{boxLeaf("xy", `{"x": {"from": 1}}`), "query.value.x.from", `unknown bound "from"`},
		{boxLeaf("xy", `{"x": {"gt": 1, "gte": 2}}`), "query.value.x.gte", "gt or gte, not both"},
		{boxLeaf("xy", `{"y": {"lt": 1, "lte": 2}}`), "query.value.y.lte", "lt or lte, not both"},
		{boxLeaf("xy", `{"x": {"gte": true}}`), "query.value.x.gte", "a box bound is a number"},
		{boxLeaf("xy", `{"x": {"gte": [1]}}`), "query.value.x.gte", "a box bound is a number"},
		{boxLeaf("xy", `{"x": {"gte": 1e400}}`), "query.value.x.gte", "finite number"},
		{boxLeaf("xy", `{"x": {"gte": "now-7d"}}`), "query.value.x.gte", "x is a number axis"},
		{boxLeaf("at", `{"y": {"gte": "now-7d"}}`), "query.value.y.gte", "y is a number axis"},
		{boxLeaf("at", `{"x": {"gte": "now-7q"}}`), "query.value.x.gte", "unknown unit"},
		{boxLeaf("at", `{"x": {"gte": "yesterday"}}`), "query.value.x.gte", "neither a date"},
		{boxLeaf("xy", `{"x": {"gt": 5, "lt": 5}}`), "query.value.x", "holds no value"},
		{boxLeaf("xy", `{"y": {"gte": 6, "lte": 5}}`), "query.value.y", "holds no value"},
		{boxLeaf("at", `{"x": {"gte": "2026-10-02", "lt": "2026-10-01"}}`), "query.value.x", "holds no value"},
		{boxLeaf("at", `{"x": {"gte": "now", "lt": "now-1d"}}`), "query.value.x", "holds no value"},
		{boxLeaf("num", `{"x": {"gte": 1}}`), "query.op", "use eq/ne"},
		{`{"field": "xy", "op": "eq", "value": 1}`, "query.op", "use exists/box"},
		{`{"field": "xy", "op": "between", "value": [1, 2]}`, "query.op", "is point(number, number)"},
	} {
		n, ps := Parse([]byte(c.query))
		if len(ps) == 0 {
			ps = Validate(n, boxMapping())
		}
		if len(ps) == 0 || ps[0].Loc != c.loc || !strings.Contains(ps[0].Message, c.want) {
			t.Errorf("%s: %v, want %q at %s", c.query, ps, c.want, c.loc)
		}
	}
	for _, ok := range []string{
		boxLeaf("xy", `{}`),
		boxLeaf("xy", `{"x": {"gte": 1}}`),
		boxLeaf("xy", `{"x": {"gte": 1, "lt": 2}, "y": {"lte": -1e300}}`),
		boxLeaf("xy", `{"x": {"gte": 5, "lte": 5}}`),
		boxLeaf("at", `{"x": {"gte": "now-7d/d"}, "y": {"gt": 3}}`),
		boxLeaf("at", `{"x": {"gte": 1700000000000, "lt": "2026-10-01||+1M"}}`),
		boxLeaf("at", `{"x": {"gte": "now-1d", "lt": "now/d"}}`),
		`{"field": "xy", "op": "exists"}`,
		`{"not": {"field": "at", "op": "exists", "value": false}}`,
	} {
		if ps := Validate(mustParse(t, ok), boxMapping()); len(ps) != 0 {
			t.Errorf("%s: %v", ok, ps)
		}
	}
}

func pointDoc(points ...schema.Point) *schema.Doc {
	return &schema.Doc{Fields: map[string]schema.Value{"xy": {Present: true, Points: points}}}
}

// A document matches when one point lies in the box on both axes, each end open or
// closed as written, an axis left out unbounded.
func TestBoxMatch(t *testing.T) {
	split := pointDoc(schema.Point{X: 1, Y: 10}, schema.Point{X: 10, Y: 1})
	inside := pointDoc(schema.Point{X: 1, Y: 10}, schema.Point{X: 5, Y: 5})
	edge := pointDoc(schema.Point{X: 5, Y: 9})
	none := pointDoc()
	missing := &schema.Doc{Fields: map[string]schema.Value{}}
	for _, c := range []struct {
		box  string
		docs []*schema.Doc
		want []bool
	}{
		{`{"x": {"gte": 5}, "y": {"gte": 5}}`, []*schema.Doc{split, inside, edge, none, missing}, []bool{false, true, true, false, false}},
		{`{"x": {"gt": 5}, "y": {"gte": 5}}`, []*schema.Doc{split, inside, edge}, []bool{false, false, false}},
		{`{"x": {"gt": 5}}`, []*schema.Doc{split, inside, edge}, []bool{true, false, false}},
		{`{"y": {"lt": 5}}`, []*schema.Doc{split, inside, edge}, []bool{true, false, false}},
		{`{"y": {"lte": 5}}`, []*schema.Doc{split, inside, edge}, []bool{true, true, false}},
		{`{"x": {"gte": 2, "lte": 5}, "y": {"gt": 4, "lt": 10}}`, []*schema.Doc{split, inside, edge}, []bool{false, true, true}},
		{`{}`, []*schema.Doc{split, edge, none, missing}, []bool{true, true, false, false}},
	} {
		q := mustParse(t, boxLeaf("xy", c.box))
		for i, d := range c.docs {
			if got := Match(q, d); got != c.want[i] {
				t.Errorf("box %s on document %d: %v, want %v", c.box, i, got, c.want[i])
			}
			if got := Match(&Not{Child: q}, d); got == c.want[i] {
				t.Errorf("not box %s on document %d: %v", c.box, i, got)
			}
		}
	}
	exists := mustParse(t, `{"field": "xy", "op": "exists"}`)
	if !Match(exists, none) || Match(exists, missing) {
		t.Fatal("exists on a point field")
	}
	if Match(mustParse(t, boxLeaf("at", `{"x": {"gte": "now-7d"}}`)), pointDoc()) {
		t.Fatal("a box with date math not resolved matched")
	}
}

// Canonical tells boxes apart by axis, end, inclusiveness and bound, and writes alike
// the boxes that are alike however they are spelled; date math is kept parsed.
func TestBoxCanonical(t *testing.T) {
	canon := func(v string) []byte { return Canonical(mustParse(t, boxLeaf("at", v))) }
	base := canon(`{"x": {"gte": 1, "lt": 5}, "y": {"gt": 2}}`)
	for _, same := range []string{
		`{"y": {"gt": 2.0}, "x": {"lt": 5, "gte": 1e0}}`,
	} {
		if !bytes.Equal(canon(same), base) {
			t.Errorf("%s encodes apart from its equal", same)
		}
	}
	seen := map[string]string{string(base): "base"}
	for _, other := range []string{
		`{"x": {"gt": 1, "lt": 5}, "y": {"gt": 2}}`,
		`{"x": {"gte": 1, "lte": 5}, "y": {"gt": 2}}`,
		`{"x": {"gte": 1, "lt": 6}, "y": {"gt": 2}}`,
		`{"x": {"gte": 1, "lt": 5}, "y": {"gte": 2}}`,
		`{"x": {"gte": 1, "lt": 5}, "y": {"lt": 2}}`,
		`{"y": {"gte": 1, "lt": 5}, "x": {"gt": 2}}`,
		`{"x": {"gte": 1, "lt": 5}}`,
		`{"x": {"gte": "now-7d", "lt": 5}, "y": {"gt": 2}}`,
		`{"x": {"gte": "now-7m", "lt": 5}, "y": {"gt": 2}}`,
		`{"x": {"gte": "now-7d/d", "lt": 5}, "y": {"gt": 2}}`,
		`{}`,
	} {
		key := string(canon(other))
		if prev, dup := seen[key]; dup {
			t.Errorf("%s encodes as %s does", other, prev)
		}
		seen[key] = other
	}
}

// Resolve turns a box's date math into Unix milliseconds at now, rounding each end as
// the range op of its key rounds, and leaves its numbers; ResolveFixed resolves only
// the ends that do not read now.
func TestBoxResolve(t *testing.T) {
	q := mustParse(t, boxLeaf("at", `{"x": {"gte": "now-1d/d", "lte": "now/d"}, "y": {"gt": 3}}`))
	if !HasDateMath(q) {
		t.Fatal("a box with date math has none")
	}
	now := time.Date(2026, 10, 16, 13, 45, 30, 0, time.UTC)
	got, ps := Resolve(q, now)
	if len(ps) > 0 {
		t.Fatal(ps)
	}
	l, ok := got.(*Leaf)
	if !ok {
		t.Fatalf("resolved to %T", got)
	}
	want := `{"x":{"gte":1792022400000,"lte":1792195199999},"y":{"gt":3}}`
	if string(l.Value) != want || HasDateMath(got) {
		t.Fatalf("resolved to %s, want %s", l.Value, want)
	}
	again, _ := Parse([]byte(boxLeaf("at", want)))
	if !bytes.Equal(Canonical(again), Canonical(got)) {
		t.Fatal("the resolved value does not parse back to the resolved box")
	}
	if !Match(got, &schema.Doc{Fields: map[string]schema.Value{"at": {Present: true, Points: []schema.Point{{X: 1792195199999, Y: 4}}}}}) {
		t.Fatal("the day's last millisecond is outside lte now/d")
	}
	mixed := mustParse(t, boxLeaf("at", `{"x": {"gte": "2026-10-01", "lt": "now"}}`))
	fixed, ok := ResolveFixed(mixed).(*Leaf)
	if !ok {
		t.Fatal("ResolveFixed changed the leaf's kind")
	}
	if string(fixed.Value) != `{"x":{"gte":1790812800000,"lt":"now"}}` || !fixed.IsDateMath() {
		t.Fatalf("ResolveFixed: %s", fixed.Value)
	}
	if _, ps := Resolve(mustParse(t, boxLeaf("at", `{"x": {"gte": "now+300000y"}}`)), now); len(ps) != 1 || ps[0].Loc != "query.value.x.gte" {
		t.Fatalf("a bound out of range: %v", ps)
	}
}
