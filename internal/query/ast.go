// Package query is Searchlight's query language: a boolean tree of conditions, parsed and
// bounded ([Parse]), checked against an index's mapping ([Validate]), and matched exactly
// against an analyzed document ([Compile], [Match]).
//
// A query is one node, written as JSON:
//
//   - {"all": [node, ...]}: every child holds;
//   - {"any": [node, ...]}: at least one child holds;
//   - {"not": node}: the child does not hold;
//   - {"field": "brand", "op": "eq", "value": "Acme"}: a condition, the leaf.
//
// The root {"all": []} matches every document; any other empty group is refused. The
// semantics are scrape-bot's (its conditions, search_query and search oracle modules),
// byte for byte, as testdata/parity/match.json pins them: text compares normalized and
// cleaned ([analysis.Normalize], [analysis.Clean]), and a missing field matches nothing
// but exists:false, empty and ne.
package query

import (
	"encoding/json"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// Bounds on one query (spec section 4).
const (
	// MaxDepth is how deep all/any groups nest; the root group is depth 1, and a not
	// adds no level.
	MaxDepth = 4
	// MaxLeaves is the most conditions one query holds.
	MaxLeaves = 50
	// MaxNodes is the most nodes (groups, nots and conditions) one query holds.
	MaxNodes = 100
	// MaxValueChars is the longest text a value (or an entry of one) holds, in characters.
	MaxValueChars = 500
	// MaxListItems is the most entries a list value holds.
	MaxListItems = 200
	// MaxFieldChars is the longest field name a condition names, in characters.
	MaxFieldChars = schema.MaxFieldChars
)

// RootLoc is the loc of a query's root node; every [Problem] loc starts with it.
const RootLoc = "query"

// The operators.
const (
	OpEq          = "eq"
	OpNe          = "ne"
	OpIn          = "in"
	OpLt          = "lt"
	OpLte         = "lte"
	OpGt          = "gt"
	OpGte         = "gte"
	OpBetween     = "between"
	OpExists      = "exists"
	OpContains    = "contains"
	OpContainsAny = "contains_any"
	OpContainsAll = "contains_all"
	OpStartsWith  = "starts_with"
	OpWordsAll    = "words_all"
	OpWordsAny    = "words_any"
	OpSimilar     = "similar"
	OpHas         = "has"
	OpHasAny      = "has_any"
	OpHasAll      = "has_all"
	OpEmpty       = "empty"
	OpNonempty    = "nonempty"
)

// Ops is every operator, in the order messages list them.
var Ops = []string{
	OpContains, OpContainsAny, OpContainsAll, OpEq, OpNe, OpIn,
	OpLt, OpLte, OpGt, OpGte, OpBetween,
	OpHas, OpHasAny, OpHasAll, OpNonempty, OpEmpty, OpExists,
	OpStartsWith, OpWordsAll, OpWordsAny, OpSimilar,
}

// IsOp reports whether op is one of [Ops].
func IsOp(op string) bool {
	_, ok := opCodes[op]
	return ok
}

// opsByType is the operators each field type takes (spec section 3), in [Ops] order.
var opsByType = map[schema.FieldType][]string{
	schema.Keyword: {
		OpContains, OpContainsAny, OpContainsAll, OpEq, OpNe, OpIn,
		OpExists, OpStartsWith, OpSimilar,
	},
	schema.Text: {
		OpContains, OpContainsAny, OpContainsAll, OpEq, OpNe, OpIn,
		OpExists, OpStartsWith, OpWordsAll, OpWordsAny, OpSimilar,
	},
	schema.KeywordList: {OpHas, OpHasAny, OpHasAll, OpNonempty, OpEmpty, OpExists},
	schema.Number:      {OpEq, OpNe, OpIn, OpLt, OpLte, OpGt, OpGte, OpBetween, OpExists},
	schema.Date:        {OpEq, OpNe, OpIn, OpLt, OpLte, OpGt, OpGte, OpBetween, OpExists},
	schema.Bool:        {OpEq, OpNe, OpExists},
}

// OpsFor returns the operators a field of type t takes, in [Ops] order (nil for no
// valid type). The caller must not change the slice.
func OpsFor(t schema.FieldType) []string {
	return opsByType[t]
}

// Node is one node of a query tree: *[All], *[Any], *[Not] or *[Leaf].
type Node interface{ isNode() }

// All holds when every child does; with no children (only as the root) it always holds.
type All struct{ Children []Node }

// Any holds when at least one child does; with no children it never holds (and only a
// tree built by hand can have one so).
type Any struct{ Children []Node }

// Not holds when its child does not.
type Not struct{ Child Node }

// Leaf is a condition: Op on Field against Value.
type Leaf struct {
	Field string
	Op    string
	// Value is the condition's value as compact JSON, nil when it has none (absent or
	// null).
	Value json.RawMessage
	// Arg is Value decoded, its texts normalized. [Parse] fills it; a Leaf built by hand
	// with only Value set is decoded when it is compiled or validated.
	Arg Arg
}

func (*All) isNode()  {}
func (*Any) isNode()  {}
func (*Not) isNode()  {}
func (*Leaf) isNode() {}

// ArgKind is the JSON shape of a value.
type ArgKind uint8

// The value shapes.
const (
	// ArgNone is no value: absent or null.
	ArgNone ArgKind = iota
	// ArgBool is true or false.
	ArgBool
	// ArgNumber is a JSON number.
	ArgNumber
	// ArgString is a JSON string.
	ArgString
	// ArgList is an array of scalars.
	ArgList
	// ArgObject is an object of scalars (similar's {text, min}).
	ArgObject
)

// Arg is a condition's value, decoded.
type Arg struct {
	Kind ArgKind
	// Scalar is the value when Kind is ArgBool, ArgNumber or ArgString.
	Scalar Scalar
	// List is the entries when Kind is ArgList.
	List []Scalar
	// Object is the members when Kind is ArgObject.
	Object map[string]Scalar
}

// Scalar is one bool, number or string of a value.
type Scalar struct {
	// Kind is ArgBool, ArgNumber or ArgString.
	Kind ArgKind
	Bool bool
	// Number is a number's value, when Finite.
	Number float64
	// Finite is false for a number no float64 holds (1e400, 10**400): it equals nothing
	// and compares with nothing, as scrape-bot's number() refuses it.
	Finite bool
	// Text is a string as written.
	Text string
	// Norm is Text as text compares: normalized and cleaned.
	Norm string
}

// texts returns the strings of a scalar or list value, as written; nil if it holds none
// (scrape-bot's _strings).
func (a *Arg) texts() []Scalar {
	switch a.Kind {
	case ArgString:
		return []Scalar{a.Scalar}
	case ArgList:
		var out []Scalar
		for _, s := range a.List {
			if s.Kind == ArgString {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

// arg returns l's decoded value, decoding Value when Arg was not filled.
func (l *Leaf) arg() Arg {
	if l.Arg.Kind != ArgNone || len(l.Value) == 0 {
		return l.Arg
	}
	a, _ := decodeRaw(l.Value, l.Op, "")
	return a
}
