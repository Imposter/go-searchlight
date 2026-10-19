package query

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// maxNamedFields is the most field names a problem lists.
const maxNamedFields = 20

// Validate reports every problem with n as a query of an index with mapping m (nil: no
// field but the pseudo-field [schema.IDField]). The root {"all": []}, every document,
// has none. It checks:
//
//   - each condition's field is mapped (or is _id, a keyword);
//   - its op is one the field's type takes (spec section 3, [OpsFor]);
//   - its value is what the op takes for that type: a text, number or bool of the
//     field's sort for eq and ne, a non-empty list of them for in; a number for lt, lte,
//     gt and gte; [lo, hi] with lo <= hi for between (on a date field, each bound a
//     number or date math, [ParseDateMath], lo <= hi checked unless one reads now);
//     true or false (or nothing) for exists; nothing for empty and nonempty; a
//     non-blank text for contains, starts_with and has (one entry, so no comma); a
//     non-empty list of non-blank texts for contains_*, has_* and words_* (each of a
//     words_* list holding a word); {text, min} with a non-blank text and
//     0 < min <= 1 for similar; for box on a point field, x and y bounds ([boxProblem]);
//   - a nested node's path is a nested field, and it is in no other nested node;
//     its conditions name its path's sub-fields (path.name), which no condition
//     outside one names; its inner_hits name sub-fields of its path, a key no
//     inner_hits of another path takes (those of one path sharing a key merge), and
//     are under no not;
//
// and, as [Parse] checks them, the tree's shape and bounds and every text's length and
// NUL, so a tree built by hand is held to the same rules. Problems are in tree order; a
// condition reports its first problem only, at the loc of its field, op or value.
func Validate(n Node, m *schema.Mapping) []Problem {
	var ps problems
	if isNil(n) {
		ps.add(RootLoc, "a query is required")
		return ps
	}
	if root, ok := n.(*All); ok && len(root.Children) == 0 {
		return nil
	}
	if msg := boundsProblem(n); msg != "" {
		ps.add(RootLoc, msg)
	}
	v := validator{ps: &ps, m: m}
	v.node(n, RootLoc)
	return ps
}

// ValidateInner reports every problem with n as a nested field's inner query, as an
// aggregation's or a sort's where gives one: [Validate]'s, but every condition names a
// sub-field of path, the root {"all": []} holds of every object, and no node is nested.
func ValidateInner(n Node, m *schema.Mapping, path string) []Problem {
	var ps problems
	if isNil(n) {
		ps.add(RootLoc, "a query is required")
		return ps
	}
	if isEveryObject(n) {
		return nil
	}
	if msg := boundsProblem(n); msg != "" {
		ps.add(RootLoc, msg)
	}
	v := validator{ps: &ps, m: m, path: path, inner: true}
	v.node(n, RootLoc)
	return ps
}

// validator walks a tree for Validate and ValidateInner, knowing where it is: inside
// a nested node's query (path), under a not (negated), in an inner query (inner, which
// holds no nested node). keys are the inner hits' keys seen, each with its path.
type validator struct {
	ps      *problems
	m       *schema.Mapping
	path    string
	inner   bool
	negated bool
	keys    map[string]string
}

func (v *validator) node(n Node, loc string) {
	switch x := n.(type) {
	case *All:
		v.group(join(loc, "all"), x.Children)
	case *Any:
		v.group(join(loc, "any"), x.Children)
	case *Not:
		if isNil(x.Child) {
			v.ps.add(join(loc, "not"), msgNode)
			return
		}
		negated := v.negated
		v.negated = true
		v.node(x.Child, join(loc, "not"))
		v.negated = negated
	case *Nested:
		v.nested(x, join(loc, "nested"))
	case *Leaf:
		v.leaf(x, loc)
	}
}

func (v *validator) group(loc string, children []Node) {
	checkGroup(v.ps, loc, children)
	for i, child := range children {
		if !isNil(child) {
			v.node(child, index(loc, i))
		}
	}
}

// nested checks a nested node at loc (its "nested" key): its path is a nested field,
// it is in no other nested node, its inner hits ask for that field's sub-fields, under
// a key no node of another path takes, and are under no not (a document a not matches
// has no object to show), and its query is an inner query of the path.
func (v *validator) nested(n *Nested, loc string) {
	if v.path != "" || v.inner {
		v.ps.add(loc, "a nested node is not inside another, nor inside an inner query: objects nest one level deep")
		return
	}
	if t, ok := v.m.Type(n.Path); !ok || t != schema.Nested {
		v.ps.addf(join(loc, "path"), "%q is not a nested field of the index (nested fields: %s)", n.Path, nestedNames(v.m))
		return
	}
	if h := n.InnerHits; h != nil {
		at := join(loc, "inner_hits")
		key := h.Key(n.Path)
		switch {
		case v.negated:
			v.ps.add(at, "inner_hits cannot be under a not: a document a not matches holds no object to show")
		case v.keys[key] != "" && v.keys[key] != n.Path:
			v.ps.addf(at, "inner_hits %q are nested field %q's already: nodes share inner hits by name on one path alone (name these otherwise)", key, v.keys[key])
		case h.Size < 0 || h.Size > MaxInnerHitsSize:
			v.ps.addf(join(at, "size"), "an inner_hits size is a whole number from 0 to %d", MaxInnerHitsSize)
		case len(h.Fields) > MaxInnerHitsFields:
			v.ps.addf(join(at, "fields"), "inner_hits fields is a list of at most %d sub-field names", MaxInnerHitsFields)
		}
		if v.keys == nil {
			v.keys = map[string]string{}
		}
		if v.keys[key] == "" {
			v.keys[key] = n.Path
		}
		for i, f := range h.Fields {
			if p, ok := v.m.NestedPath(f); !ok || p != n.Path {
				v.ps.addf(index(join(at, "fields"), i), "%q is not a sub-field of nested field %q (%s)", f, n.Path, subFieldNames(v.m, n.Path))
			}
		}
	}
	at := join(loc, "query")
	switch {
	case isNil(n.Query):
		v.ps.add(at, msgNode)
	case isEveryObject(n.Query):
	default:
		v.path = n.Path
		v.node(n.Query, at)
		v.path = ""
	}
}

// leaf checks a condition: outside a nested node it names no sub-field, inside one only
// its path's sub-fields; then [checkLeaf].
func (v *validator) leaf(l *Leaf, loc string) {
	path, sub := v.m.NestedPath(l.Field)
	switch {
	case v.path == "" && sub:
		v.ps.addf(join(loc, "field"), `%q is a sub-field of nested field %q: name it inside {"nested": {"path": %q, "query": ...}}`, l.Field, path, path)
		return
	case v.path != "" && (!sub || path != v.path):
		v.ps.addf(join(loc, "field"), "inside nested %q a condition names its sub-fields (%s), not %q", v.path, subFieldNames(v.m, v.path), l.Field)
		return
	}
	checkLeaf(v.ps, loc, l, v.m)
}

// nestedNames lists m's nested fields, sorted, up to maxNamedFields; "none" when it
// has none.
func nestedNames(m *schema.Mapping) string {
	var names []string
	if m != nil {
		for _, name := range slices.Sorted(maps.Keys(m.Fields)) {
			if m.Fields[name] == schema.Nested {
				names = append(names, name)
			}
		}
	}
	return namedList(names)
}

// subFieldNames lists nested field path's sub-fields, sorted, up to maxNamedFields.
func subFieldNames(m *schema.Mapping, path string) string {
	return namedList(m.SubFields(path))
}

func namedList(names []string) string {
	switch {
	case len(names) == 0:
		return "none"
	case len(names) > maxNamedFields:
		return strings.Join(names[:maxNamedFields], ", ") + fmt.Sprintf(", and %d more", len(names)-maxNamedFields)
	}
	return strings.Join(names, ", ")
}

func checkGroup(ps *problems, loc string, children []Node) {
	if len(children) == 0 {
		ps.add(loc, msgEmptyGroup)
	}
	for i, child := range children {
		if isNil(child) {
			ps.add(index(loc, i), msgNode)
		}
	}
}

func checkLeaf(ps *problems, loc string, l *Leaf, m *schema.Mapping) {
	if !IsOp(l.Op) {
		ps.addf(join(loc, "op"), "unknown op %q; %s", l.Op, msgUnknownOp)
		return
	}
	if msg := fieldProblem(l.Field); msg != "" {
		ps.add(join(loc, "field"), msg)
		return
	}
	t, ok := m.Type(l.Field)
	if !ok {
		ps.addf(join(loc, "field"), "%q is not a field of the index (fields: %s)", l.Field, fieldNames(m))
		return
	}
	offered := OpsFor(t)
	if !slices.Contains(offered, l.Op) {
		ps.addf(join(loc, "op"), "%q is %s: use %s", l.Field, t, strings.Join(offered, "/"))
		return
	}
	at := join(loc, "value")
	a := l.Arg
	if a.Kind == ArgNone && len(l.Value) > 0 {
		var decoded []Problem
		a, decoded = decodeRaw(l.Value, l.Op, at)
		if len(decoded) > 0 {
			ps.add(decoded[0].Loc, decoded[0].Message)
			return
		}
	}
	if msg := boundedProblem(&a); msg != "" {
		ps.add(at, msg)
		return
	}
	if l.Op == OpBox {
		if sub, msg := boxProblem(&a, t); msg != "" {
			ps.add(at+sub, msg)
		}
		return
	}
	if t == schema.Date && IsDateMathOp(l.Op) {
		if sub, msg := dateBoundProblem(l.Op, &a); msg != "" {
			ps.add(at+sub, msg)
		}
		return
	}
	if msg := valueProblem(l.Op, &a, t, l.Field); msg != "" {
		ps.add(at, msg)
	}
}

// dateBoundProblem is why a is not what op (lt, lte, gt, gte or between) takes on a date
// field, a number or date math for each bound, and the loc below the value it is at;
// "" when it is. between's lo must not be past its hi when that holds at every now:
// when neither reads now, or both are now plus a fixed length (no rounding, months or
// years), whose order never changes.
func dateBoundProblem(op string, a *Arg) (string, string) {
	if op != OpBetween {
		if a.Kind == ArgString {
			_, why := checkDateMath(a.Scalar.Text)
			return "", why
		}
		if a.Kind != ArgNumber || !a.Scalar.Finite {
			return "", op + " needs a number or date math"
		}
		return "", ""
	}
	if a.Kind != ArgList || len(a.List) != 2 {
		return "", "between needs [lo, hi]"
	}
	var ends [2]float64
	relative, constant := 0, 0
	for i := range a.List {
		s := &a.List[i]
		switch {
		case s.Kind == ArgString:
			d, why := checkDateMath(s.Text)
			if why != "" {
				return "." + strconv.Itoa(i), why
			}
			ms, _ := d.Resolve(0, i == 1)
			ends[i] = float64(ms)
			if d.Relative() {
				relative++
			}
			if d.constantOffset() {
				constant++
			}
		case fits(s, "number"):
			ends[i] = s.Number
		default:
			return "", "between needs two numbers or date math, [lo, hi]"
		}
	}
	if (relative == 0 || constant == 2) && ends[0] > ends[1] {
		return "", "between needs lo <= hi"
	}
	return "", ""
}

// fieldNames lists m's fields and the pseudo-field _id, sorted, up to maxNamedFields.
func fieldNames(m *schema.Mapping) string {
	names := []string{schema.IDField}
	if m != nil {
		names = append(names, slices.Sorted(maps.Keys(m.Fields))...)
	}
	if len(names) > maxNamedFields {
		return strings.Join(names[:maxNamedFields], ", ") + fmt.Sprintf(", and %d more", len(names)-maxNamedFields)
	}
	return strings.Join(names, ", ")
}

// boundedProblem is why a's texts or list break Parse's bounds, or "".
func boundedProblem(a *Arg) string {
	if a.Kind == ArgList && len(a.List) > MaxListItems {
		return msgLongList
	}
	if msg := a.Scalar.problem(); msg != "" {
		return msg
	}
	for i := range a.List {
		if msg := a.List[i].problem(); msg != "" {
			return msg
		}
	}
	for _, key := range slices.Sorted(maps.Keys(a.Object)) {
		s := a.Object[key]
		if msg := s.problem(); msg != "" {
			return msg
		}
	}
	if a.Kind == ArgBox {
		for _, s := range a.BoundScalars() {
			if msg := s.problem(); msg != "" {
				return msg
			}
		}
	}
	return ""
}

// boxProblem is why a is not what box takes on a point field of type t, and the loc
// below the value it is at; "" when it is. Each axis's bounds are numbers, or on a date
// axis numbers or date math ([ParseDateMath]); an axis whose bounds leave no value
// between them at every now (neither reads now, or both are now plus a fixed length)
// is refused, as between's lo past its hi is.
func boxProblem(a *Arg, t schema.FieldType) (string, string) {
	if a.Kind != ArgBox || a.Box == nil {
		return "", `box needs {"x": {"gte": lo, "lt": hi}, "y": {...}}: gt or gte and lt or lte per axis, either axis optional`
	}
	xAxis, yAxis := t.Axes()
	for i, iv := range a.Box.Axes() {
		name, axis := BoxX, xAxis
		if i == 1 {
			name, axis = BoxY, yAxis
		}
		var ends [2]float64
		relative, constant := 0, 0
		for j, b := range []*Bound{&iv.Lo, &iv.Hi} {
			if !b.Set {
				continue
			}
			sub := "." + name + "." + boundKey(b, j == 1)
			s := &b.Value
			switch {
			case s.Kind == ArgString && axis == schema.Date:
				d, why := checkDateMath(s.Text)
				if why != "" {
					return sub, why
				}
				ms, _ := d.Resolve(0, b.RoundsUp(j == 1))
				ends[j] = float64(ms)
				if d.Relative() {
					relative++
				}
				if d.constantOffset() {
					constant++
				}
			case s.Kind == ArgString:
				return sub, fmt.Sprintf("%s is a number axis: its bounds are numbers; date math is for a date axis", name)
			case s.Kind != ArgNumber || !s.Finite:
				if axis == schema.Date {
					return sub, "a box bound on a date axis is a number (Unix milliseconds), a date or date math"
				}
				return sub, "a box bound is a finite number"
			default:
				ends[j] = s.Number
			}
		}
		if iv.Lo.Set && iv.Hi.Set && (relative == 0 || constant == 2) {
			lo, hi := ends[0], ends[1]
			if lo > hi || lo == hi && (!iv.Lo.Inclusive || !iv.Hi.Inclusive) {
				return "." + name, fmt.Sprintf("box's %s holds no value: its lower bound is not below its upper", name)
			}
		}
	}
	return "", ""
}

// boundKey is how a box value names bound b: gt or gte for a lower bound, lt or lte
// for an upper.
func boundKey(b *Bound, upper bool) string {
	switch {
	case upper && b.Inclusive:
		return BoxLte
	case upper:
		return BoxLt
	case b.Inclusive:
		return BoxGte
	default:
		return BoxGt
	}
}

// sortOf is what a value compared with a field of type t is: "number", "bool" or "text".
func sortOf(t schema.FieldType) string {
	switch t {
	case schema.Number, schema.Date:
		return "number"
	case schema.Bool:
		return "bool"
	default:
		return "text"
	}
}

// fits reports whether s is of that sort; a text must not be blank.
func fits(s *Scalar, sort string) bool {
	switch sort {
	case "number":
		return s.Kind == ArgNumber && s.Finite
	case "bool":
		return s.Kind == ArgBool
	default:
		return s.Kind == ArgString && !blank(s.Text)
	}
}

// nonBlankTexts returns a list value's texts, or nil unless it is a non-empty list of
// non-blank texts only.
func nonBlankTexts(a *Arg) []Scalar {
	if a.Kind != ArgList || len(a.List) == 0 {
		return nil
	}
	for i := range a.List {
		if a.List[i].Kind != ArgString || blank(a.List[i].Text) {
			return nil
		}
	}
	return a.List
}

// valueProblem is why a is not what op takes for a field of type t, or "" (scrape-bot's
// value_problem). The op is one t takes already.
func valueProblem(op string, a *Arg, t schema.FieldType, field string) string {
	switch op {
	case OpEmpty, OpNonempty:
		if a.Kind != ArgNone {
			return op + " takes no value"
		}
	case OpExists:
		if a.Kind != ArgNone && a.Kind != ArgBool {
			return "exists takes true or false"
		}
	case OpBetween:
		if a.Kind != ArgList || len(a.List) != 2 {
			return "between needs [lo, hi]"
		}
		lo, hi := &a.List[0], &a.List[1]
		if !fits(lo, "number") || !fits(hi, "number") {
			return "between needs two numbers, [lo, hi]" + dateMathHint(a, t)
		}
		if lo.Number > hi.Number {
			return "between needs lo <= hi"
		}
	case OpSimilar:
		return similarProblem(a)
	case OpWordsAll, OpWordsAny:
		texts := nonBlankTexts(a)
		if texts == nil {
			return op + " needs a list of words or phrases"
		}
		for i := range texts {
			if analysis.Words(texts[i].Text) == analysis.NoWords {
				return fmt.Sprintf("%s: %q holds no word to match", op, texts[i].Text)
			}
		}
	case OpContainsAny, OpContainsAll, OpHasAny, OpHasAll:
		if nonBlankTexts(a) == nil {
			return op + " needs a list of text values"
		}
	case OpContains, OpStartsWith, OpHas:
		if a.Kind != ArgString || blank(a.Scalar.Text) {
			return op + " needs a text value"
		}
		if op == OpHas && strings.Contains(a.Scalar.Text, ",") {
			return "has takes one entry, not a list: use has_any or has_all"
		}
	case OpIn:
		if a.Kind != ArgList || len(a.List) == 0 {
			return "in needs a list of values"
		}
		sort := sortOf(t)
		for i := range a.List {
			if !fits(&a.List[i], sort) {
				return fmt.Sprintf("in on %q needs a list of %s values", field, sort) + dateMathHint(a, t)
			}
		}
	case OpLt, OpLte, OpGt, OpGte:
		if a.Kind != ArgNumber || !a.Scalar.Finite {
			return op + " needs a number" + dateMathHint(a, t)
		}
	case OpEq, OpNe:
		sort := sortOf(t)
		if a.Kind == ArgList || a.Kind == ArgObject || !fits(&a.Scalar, sort) {
			return fmt.Sprintf("%s on %q needs a %s value", op, field, sort) + dateMathHint(a, t)
		}
	}
	return ""
}

// checkDateMath parses s as date math, returning why it is not valid: it does not
// parse, or, when it does not read now, it falls out of range rounded either way.
func checkDateMath(s string) (DateMath, string) {
	d, why := ParseDateMath(s)
	if why != "" || d.Relative() {
		return d, why
	}
	_, down := d.Resolve(0, false)
	_, up := d.Resolve(0, true)
	if !down || !up {
		return d, fmt.Sprintf("date math %q falls outside the dates a date field holds", s)
	}
	return d, ""
}

// dateMathHint explains, for a value holding a string where a number is wanted, where
// date strings apply; "" for any other value. eq, ne and in on a date field take Unix
// milliseconds only: a plain date there is refused as date math is.
func dateMathHint(a *Arg, t schema.FieldType) string {
	switch {
	case !hasDateText(a):
		return ""
	case t == schema.Date && plainDate(a):
		return "; give the date as Unix milliseconds: date strings are for lt, lte, gt, gte and between"
	case t == schema.Date:
		return "; date math is for lt, lte, gt, gte and between"
	case t == schema.Number:
		return "; date math is for date fields"
	default:
		return ""
	}
}

// plainDate reports whether a value's first string is a plain date: no now, no ||.
func plainDate(a *Arg) bool {
	text := a.Scalar.Text
	if a.Kind == ArgList {
		for i := range a.List {
			if a.List[i].Kind == ArgString {
				text = a.List[i].Text
				break
			}
		}
	}
	d, why := ParseDateMath(text)
	return why == "" && !d.Relative() && !strings.Contains(text, "||")
}

func similarProblem(a *Arg) string {
	if a.Kind != ArgObject {
		return "similar needs {text, min}"
	}
	text, ok := a.Object["text"]
	if !ok || text.Kind != ArgString || blank(text.Text) {
		return "similar needs the text to resemble (text)"
	}
	if _, ok := likeness(a); !ok {
		return "similar needs min above 0 and at most 1"
	}
	return ""
}

// likeness returns similar's minimum when a is a valid {text, min} (scrape-bot's
// likeness): a non-blank text and a finite min with 0 < min <= 1.
func likeness(a *Arg) (float64, bool) {
	if a.Kind != ArgObject {
		return 0, false
	}
	text, hasText := a.Object["text"]
	minimum, hasMin := a.Object["min"]
	if !hasText || text.Kind != ArgString || blank(text.Text) {
		return 0, false
	}
	if !hasMin || minimum.Kind != ArgNumber || !minimum.Finite {
		return 0, false
	}
	return minimum.Number, minimum.Number > 0 && minimum.Number <= 1
}
