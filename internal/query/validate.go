package query

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// maxNamedFields is the most field names a problem lists.
const maxNamedFields = 20

// Validate reports every problem with n as a query of an index with mapping m (nil: no
// field but the pseudo-field [schema.IDField]):
//
//   - each condition's field is mapped (or is _id, a keyword);
//   - its op is one the field's type takes (spec section 3, [OpsFor]);
//   - its value is what the op takes for that type: a text, number or bool of the
//     field's sort for eq and ne, a non-empty list of them for in; a number for lt, lte,
//     gt and gte; [lo, hi] with lo <= hi for between; true or false (or nothing) for
//     exists; nothing for empty and nonempty; a non-blank text for contains, starts_with
//     and has (one entry, so no comma); a non-empty list of non-blank texts for
//     contains_*, has_* and words_* (each of a words_* list holding a word); {text, min}
//     with a non-blank text and 0 < min <= 1 for similar;
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
		return nil // {"all": []}: every document
	}
	if msg := boundsProblem(n); msg != "" {
		ps.add(RootLoc, msg)
	}
	Walk(n, func(loc string, node Node) bool {
		switch x := node.(type) {
		case *All:
			checkGroup(&ps, join(loc, "all"), x.Children)
		case *Any:
			checkGroup(&ps, join(loc, "any"), x.Children)
		case *Not:
			if isNil(x.Child) {
				ps.add(join(loc, "not"), msgNode)
			}
		case *Leaf:
			checkLeaf(&ps, loc, x, m)
		}
		return true
	})
	return ps
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
	if msg := valueProblem(l.Op, &a, t, l.Field); msg != "" {
		ps.add(at, msg)
	}
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
	return ""
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
			return "between needs two numbers, [lo, hi]"
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
				return fmt.Sprintf("in on %q needs a list of %s values", field, sort)
			}
		}
	case OpLt, OpLte, OpGt, OpGte:
		if a.Kind != ArgNumber || !a.Scalar.Finite {
			return op + " needs a number"
		}
	case OpEq, OpNe:
		sort := sortOf(t)
		if a.Kind == ArgList || a.Kind == ArgObject || !fits(&a.Scalar, sort) {
			return fmt.Sprintf("%s on %q needs a %s value", op, field, sort)
		}
	}
	return ""
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
