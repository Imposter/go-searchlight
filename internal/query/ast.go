// Package query is Searchlight's query language: a boolean tree of conditions, parsed and
// bounded ([Parse]), checked against an index's mapping ([Validate]), and matched exactly
// against an analyzed document ([Compile], [Match]).
//
// A query is one node, written as JSON:
//
//   - {"all": [node, ...]}: every child holds;
//   - {"any": [node, ...]}: at least one child holds;
//   - {"not": node}: the child does not hold;
//   - {"field": "brand", "op": "eq", "value": "Acme"}: a condition, the leaf;
//   - {"nested": {"path": "_v", "query": node, "inner_hits": {...}}}: one object of
//     nested field _v satisfies the whole inner query, whose conditions name its
//     sub-fields (_v.size) by their full names ([Nested]).
//
// The root {"all": []} matches every document; any other empty group is refused. The
// semantics are scrape-bot's (its conditions, search_query and search oracle modules),
// byte for byte, as testdata/parity/match.json pins them: text compares normalized and
// cleaned ([analysis.Normalize], [analysis.Clean]), and a missing field matches nothing
// but exists:false, empty and ne.
//
// A date field's lt, lte, gt, gte and between also take Elasticsearch's date math
// ([ParseDateMath]): {"field": "seen_at", "op": "gte", "value": "now-7d/d"}. A query
// keeps the expression; [Resolve] turns it into Unix milliseconds at a request's now.
package query

import (
	"encoding/json"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// Bounds on one query (spec section 4).
const (
	// MaxDepth is how deep all/any groups nest; the root group is depth 1, a nested
	// node adds a level as a group does, and a not adds none.
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
	OpBox         = "box"
)

// Ops is every operator, in the order messages list them.
var Ops = []string{
	OpContains, OpContainsAny, OpContainsAll, OpEq, OpNe, OpIn,
	OpLt, OpLte, OpGt, OpGte, OpBetween,
	OpHas, OpHasAny, OpHasAll, OpNonempty, OpEmpty, OpExists,
	OpStartsWith, OpWordsAll, OpWordsAny, OpSimilar, OpBox,
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

// pointOps are the operators a point field takes, in [Ops] order.
var pointOps = []string{OpExists, OpBox}

// nestedOps are the operators a nested field itself takes: whether it holds a value.
var nestedOps = []string{OpExists}

// OpsFor returns the operators a field of type t takes, in [Ops] order (nil for no
// valid type). The caller must not change the slice.
func OpsFor(t schema.FieldType) []string {
	switch {
	case t.IsPoint():
		return pointOps
	case t.IsNested():
		return nestedOps
	}
	return opsByType[t]
}

// Node is one node of a query tree: *[All], *[Any], *[Not], *[Nested] or *[Leaf].
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

// Nested holds when one object of nested field Path satisfies Query, its every
// condition read on that object alone: "size L and in stock" holds of one variant
// that is both, never of one L variant and another in stock. A not inside applies per
// object. Query's conditions name Path's sub-fields (Path.name), and nothing else; a
// Query of {"all": []} holds of any object. InnerHits, when set, asks each hit for the
// objects that satisfy Query.
type Nested struct {
	Path      string
	Query     Node
	InnerHits *InnerHits
}

// InnerHits is what a nested node's inner_hits asks of each hit: the members of its
// path that satisfy the nested query, by their offsets in the array, the first Size of
// them with their sub-fields Fields (full names; every member when empty). It is no
// part of what the query matches.
type InnerHits struct {
	// Name keys the hit's inner hits: the path when empty. Nodes of one path under one
	// key share one set of inner hits: the members satisfying any of their queries.
	Name   string   `json:"name,omitempty"`
	Size   int      `json:"size"`
	Fields []string `json:"fields,omitempty"`
}

// Key is the name the hit's inner hits are given under: Name, or else path.
func (h *InnerHits) Key(path string) string {
	if h.Name != "" {
		return h.Name
	}
	return path
}

// Bounds of a nested node's inner_hits.
const (
	// DefaultInnerHitsSize is how many objects inner_hits returns when it names no
	// size, as Elasticsearch's.
	DefaultInnerHitsSize = 3
	// MaxInnerHitsSize is the most objects inner_hits returns per hit.
	MaxInnerHitsSize = 1000
	// MaxInnerHitsFields is the most sub-fields inner_hits names.
	MaxInnerHitsFields = 100
)

func (*All) isNode()    {}
func (*Any) isNode()    {}
func (*Not) isNode()    {}
func (*Leaf) isNode()   {}
func (*Nested) isNode() {}

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
	// ArgBox is box's bounds, {"x": {"gte": 1}, "y": {"lt": 5}} ([Box]).
	ArgBox
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
	// Box is the bounds when Kind is ArgBox.
	Box *Box
}

// Box is a box condition's value: an interval per axis, X and Y. A document matches
// when one of its points lies in both, its x in X and its y in Y.
type Box struct {
	X, Y Interval
}

// Interval is one axis of a box: a lower bound (gt or gte) and an upper one (lt or
// lte), either of which may be absent (unbounded).
type Interval struct {
	Lo, Hi Bound
}

// Bound is one end of an [Interval]. Value is a number, or on a date axis a string
// (a date, or date math, until [Resolve] resolves it).
type Bound struct {
	Set       bool
	Inclusive bool
	Value     Scalar
}

// Axes returns the box's intervals, x then y, for the callers that treat both alike.
func (b *Box) Axes() [2]*Interval { return [2]*Interval{&b.X, &b.Y} }

// The keys of a box's value.
const (
	BoxX   = "x"
	BoxY   = "y"
	BoxGt  = "gt"
	BoxGte = "gte"
	BoxLt  = "lt"
	BoxLte = "lte"
)

// RoundsUp reports whether date math at this end (an upper bound when upper) rounds
// up, as a range op's does: a gt lower bound and an lte upper bound round up, gte and
// lt down.
func (b *Bound) RoundsUp(upper bool) bool { return upper == b.Inclusive }

// BoundScalars returns the scalars of a value a date math bound may be among: the
// scalar, a list's entries, or a box's set bounds. Nil for any other value.
func (a *Arg) BoundScalars() []Scalar {
	switch a.Kind {
	case ArgBool, ArgNumber, ArgString:
		return []Scalar{a.Scalar}
	case ArgList:
		return a.List
	case ArgBox:
		if a.Box == nil {
			return nil
		}
		var out []Scalar
		for _, iv := range a.Box.Axes() {
			for _, b := range []*Bound{&iv.Lo, &iv.Hi} {
				if b.Set {
					out = append(out, b.Value)
				}
			}
		}
		return out
	default:
		return nil
	}
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
