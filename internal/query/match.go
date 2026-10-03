package query

import (
	"slices"
	"strings"
	"sync"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// Match reports whether document d satisfies n. It compiles n each call; to match one
// query against many documents, [Compile] it once.
func Match(n Node, d *schema.Doc) bool {
	return Compile(n).Match(d)
}

// Compiled is a query prepared for matching: every value normalized, phrases split into
// words, similar's trigram keys computed, in lists sorted for binary search, constant
// conditions folded and each group's children ordered cheapest first. It is immutable,
// and safe for concurrent use.
type Compiled struct {
	root cnode
}

// Compile prepares n for matching. It accepts any tree, valid or not: a condition whose
// value its op cannot read matches as scrape-bot's matcher does (eq never, ne always), a
// nil node or an unknown op never matches.
func Compile(n Node) *Compiled {
	return &Compiled{root: compileNode(n)}
}

// Match reports whether d satisfies the query, exactly (spec section 4): a missing field
// matches nothing but exists:false, empty and ne; a value of the wrong type for its
// field (Present with no typed part) is invisible to every op that reads a typed part;
// text ops compare the full normalized text, never grams, so a value too long for grams
// matches as any other. A nil d is a document with no field. Match allocates nothing.
func (c *Compiled) Match(d *schema.Doc) bool {
	var fields map[string]schema.Value
	if d != nil {
		fields = d.Fields
	}
	return c.root.match(fields)
}

// opCode is an op, as the compiled leaf switches on it.
type opCode uint8

const (
	opUnknown opCode = iota
	opEq
	opNe
	opIn
	opLt
	opLte
	opGt
	opGte
	opBetween
	opExists
	opContains // contains, contains_any, contains_all
	opStartsWith
	opWords // words_all, words_any
	opSimilar
	opHas // has, has_any, has_all
	opEmpty
	opNonempty
)

var opCodes = map[string]opCode{
	OpEq: opEq, OpNe: opNe, OpIn: opIn,
	OpLt: opLt, OpLte: opLte, OpGt: opGt, OpGte: opGte, OpBetween: opBetween,
	OpExists:   opExists,
	OpContains: opContains, OpContainsAny: opContains, OpContainsAll: opContains,
	OpStartsWith: opStartsWith,
	OpWordsAll:   opWords, OpWordsAny: opWords,
	OpSimilar: opSimilar,
	OpHas:     opHas, OpHasAny: opHas, OpHasAll: opHas,
	OpEmpty: opEmpty, OpNonempty: opNonempty,
}

type nodeKind uint8

const (
	kindFalse nodeKind = iota // never holds
	kindTrue                  // always holds
	kindAll
	kindAny
	kindNot
	kindLeaf
)

// cnode is a compiled node.
type cnode struct {
	kind     nodeKind
	cost     int
	children []cnode // all, any; not's one child
	leaf     *cleaf
}

var (
	never  = cnode{kind: kindFalse}
	always = cnode{kind: kindTrue}
)

func (n *cnode) match(fields map[string]schema.Value) bool {
	switch n.kind {
	case kindLeaf:
		return n.leaf.match(fields)
	case kindAll:
		for i := range n.children {
			if !n.children[i].match(fields) {
				return false
			}
		}
		return true
	case kindAny:
		for i := range n.children {
			if n.children[i].match(fields) {
				return true
			}
		}
		return false
	case kindNot:
		return !n.children[0].match(fields)
	case kindTrue:
		return true
	default:
		return false
	}
}

func compileNode(n Node) cnode {
	switch x := n.(type) {
	case *All:
		if x == nil {
			return never
		}
		return compileGroup(kindAll, x.Children)
	case *Any:
		if x == nil {
			return never
		}
		return compileGroup(kindAny, x.Children)
	case *Not:
		if x == nil {
			return never
		}
		child := compileNode(x.Child)
		switch child.kind {
		case kindTrue:
			return never
		case kindFalse:
			return always
		case kindNot:
			return child.children[0]
		}
		return cnode{kind: kindNot, cost: child.cost, children: []cnode{child}}
	case *Leaf:
		if x == nil {
			return never
		}
		return compileLeaf(x)
	default:
		return never
	}
}

// compileGroup folds constant children away (all: a false child is false, true children
// drop out; any the other way round), keeps a lone child as itself, and orders the rest
// cheapest first, so the matcher settles on the cheap conditions before the costly.
func compileGroup(kind nodeKind, nodes []Node) cnode {
	decided, dropped := kindFalse, kindTrue // all: false decides, true drops out
	if kind == kindAny {
		decided, dropped = kindTrue, kindFalse
	}
	children := make([]cnode, 0, len(nodes))
	for _, node := range nodes {
		child := compileNode(node)
		switch child.kind {
		case decided:
			return cnode{kind: decided}
		case dropped:
			continue
		}
		children = append(children, child)
	}
	switch len(children) {
	case 0:
		return cnode{kind: dropped} // every child dropped out: {"all": []} holds, {"any": []} not
	case 1:
		return children[0]
	}
	slices.SortStableFunc(children, func(a, b cnode) int { return a.cost - b.cost })
	cost := 0
	for i := range children {
		cost += children[i].cost
	}
	return cnode{kind: kind, cost: cost, children: children}
}

// Costs order a group's children: cheap comparisons first, similarity last.
const (
	costCompare = 1  // exists, empty, eq, a number compared
	costLookup  = 2  // in, has_*, starts_with: a binary search or a prefix
	costScan    = 4  // contains_*, words_*: a substring search
	costSimilar = 16 // similar: the document's trigrams, then a merge
)

// cleaf is a compiled condition.
type cleaf struct {
	field string
	op    opCode
	every bool // contains_all, has_all, words_all: every wanted entry (else any)

	want bool // exists: whether the field must be present

	// eq, ne: the value, by its kind.
	eqKind ArgKind
	eqBool bool
	eqNum  float64
	eqText string

	// in: the values by kind; texts and numbers sorted and distinct.
	inTexts         []string
	inNums          []float64
	inTrue, inFalse bool
	lo, hi          float64  // lt..gte: the line in lo; between: [lo, hi]
	texts           []string // contains_*, starts_with (one), has_* (sorted, distinct), words_*
	keys            []uint64 // similar: the text's trigram keys
	minimum         float64  // similar
}

func compileLeaf(l *Leaf) cnode {
	code := opCodes[l.Op]
	a := l.arg()
	c := &cleaf{field: l.Field, op: code}
	cost := costCompare
	switch code {
	case opExists:
		c.want = a.Kind != ArgBool || a.Scalar.Bool // `value is not False`
	case opEmpty, opNonempty:
	case opEq, opNe:
		if !c.setEq(&a) {
			// Nothing equals the value (a list, an object, 1e400): eq never holds, ne always.
			if code == opEq {
				return never
			}
			return always
		}
	case opIn:
		if !c.setIn(&a) {
			return never
		}
		cost = costLookup
	case opLt, opLte, opGt, opGte:
		if a.Kind != ArgNumber || !a.Scalar.Finite {
			return never
		}
		c.lo = a.Scalar.Number
	case opBetween:
		if a.Kind != ArgList || len(a.List) != 2 || !a.List[0].isNumber() || !a.List[1].isNumber() {
			return never
		}
		c.lo, c.hi = a.List[0].Number, a.List[1].Number
		if c.lo > c.hi {
			return never
		}
	case opContains:
		c.every = l.Op == OpContainsAll
		c.texts = norms(a.texts())
		if len(c.texts) == 0 {
			return never
		}
		slices.Sort(c.texts)
		c.texts = slices.Compact(c.texts)
		cost = costScan
	case opStartsWith:
		if a.Kind != ArgString {
			return never
		}
		c.texts = []string{a.Scalar.Norm}
		cost = costLookup
	case opWords:
		c.every = l.Op == OpWordsAll
		if !c.setWords(&a) {
			return never
		}
		cost = costScan
	case opSimilar:
		minimum, ok := likeness(&a)
		if !ok {
			return never
		}
		text := a.Object["text"]
		c.keys = analysis.TrigramKeys(text.Norm)
		if len(c.keys) == 0 {
			return never // similarity 0, below any minimum
		}
		c.minimum = minimum
		cost = costSimilar
	case opHas:
		c.every = l.Op == OpHasAll
		c.texts = norms(a.texts())
		if len(c.texts) == 0 {
			return never
		}
		slices.Sort(c.texts)
		c.texts = slices.Compact(c.texts)
		cost = costLookup
	default:
		return never
	}
	return cnode{kind: kindLeaf, cost: cost, leaf: c}
}

func (s *Scalar) isNumber() bool {
	return s.Kind == ArgNumber && s.Finite
}

func norms(texts []Scalar) []string {
	if len(texts) == 0 {
		return nil
	}
	out := make([]string, len(texts))
	for i := range texts {
		out[i] = texts[i].Norm
	}
	return out
}

// setEq keeps eq's or ne's value; false when nothing can equal it.
func (c *cleaf) setEq(a *Arg) bool {
	s := &a.Scalar
	switch a.Kind {
	case ArgBool:
		c.eqKind, c.eqBool = ArgBool, s.Bool
	case ArgString:
		c.eqKind, c.eqText = ArgString, s.Norm
	case ArgNumber:
		if !s.Finite {
			return false
		}
		c.eqKind, c.eqNum = ArgNumber, s.Number
	default:
		return false
	}
	return true
}

// setIn keeps in's values by kind; false when nothing can equal any of them.
func (c *cleaf) setIn(a *Arg) bool {
	if a.Kind != ArgList {
		return false
	}
	for i := range a.List {
		s := &a.List[i]
		switch s.Kind {
		case ArgBool:
			if s.Bool {
				c.inTrue = true
			} else {
				c.inFalse = true
			}
		case ArgString:
			c.inTexts = append(c.inTexts, s.Norm)
		case ArgNumber:
			if s.Finite {
				c.inNums = append(c.inNums, s.Number)
			}
		}
	}
	slices.Sort(c.inTexts)
	c.inTexts = slices.Compact(c.inTexts)
	slices.Sort(c.inNums)
	c.inNums = slices.Compact(c.inNums)
	return len(c.inTexts) > 0 || len(c.inNums) > 0 || c.inTrue || c.inFalse
}

// setWords keeps the words of each phrase (scrape-bot's wanted_words); false when the
// condition can never hold: no phrase, or, for words_all, a phrase with no word.
func (c *cleaf) setWords(a *Arg) bool {
	for _, s := range a.texts() {
		words := analysis.Words(s.Text)
		if words == analysis.NoWords {
			if c.every {
				return false
			}
			continue // a phrase with no word matches nothing
		}
		c.texts = append(c.texts, words)
	}
	slices.Sort(c.texts)
	c.texts = slices.Compact(c.texts)
	return len(c.texts) > 0
}

func (c *cleaf) match(fields map[string]schema.Value) bool {
	v, ok := fields[c.field]
	present := ok && v.Present
	switch c.op {
	case opExists:
		return present == c.want
	case opEmpty:
		return !present || len(v.Entries) == 0
	case opNe:
		return !present || !c.equals(&v)
	}
	if !present {
		return false
	}
	switch c.op {
	case opEq:
		return c.equals(&v)
	case opIn:
		return c.in(&v)
	case opLt:
		return v.Number != nil && *v.Number < c.lo
	case opLte:
		return v.Number != nil && *v.Number <= c.lo
	case opGt:
		return v.Number != nil && *v.Number > c.lo
	case opGte:
		return v.Number != nil && *v.Number >= c.lo
	case opBetween:
		return v.Number != nil && c.lo <= *v.Number && *v.Number <= c.hi
	case opContains:
		return v.Text != nil && c.some(*v.Text, strings.Contains)
	case opStartsWith:
		return v.Text != nil && strings.HasPrefix(*v.Text, c.texts[0])
	case opWords:
		return v.Words != "" && c.some(v.Words, strings.Contains)
	case opSimilar:
		return v.Text != nil && c.similar(*v.Text)
	case opHas:
		return c.has(v.Entries)
	case opNonempty:
		return len(v.Entries) > 0
	default:
		return false
	}
}

func (c *cleaf) equals(v *schema.Value) bool {
	switch c.eqKind {
	case ArgBool:
		return v.Bool != nil && *v.Bool == c.eqBool
	case ArgString:
		return v.Text != nil && *v.Text == c.eqText
	case ArgNumber:
		return v.Number != nil && *v.Number == c.eqNum
	default:
		return false
	}
}

func (c *cleaf) in(v *schema.Value) bool {
	if v.Text != nil && len(c.inTexts) > 0 {
		if _, found := slices.BinarySearch(c.inTexts, *v.Text); found {
			return true
		}
	}
	if v.Number != nil && len(c.inNums) > 0 {
		if _, found := slices.BinarySearch(c.inNums, *v.Number); found {
			return true
		}
	}
	if v.Bool != nil {
		return *v.Bool && c.inTrue || !*v.Bool && c.inFalse
	}
	return false
}

// some reports whether every (contains_all, words_all) or any wanted text holds in text.
func (c *cleaf) some(text string, holds func(text, wanted string) bool) bool {
	if c.every {
		for _, wanted := range c.texts {
			if !holds(text, wanted) {
				return false
			}
		}
		return true
	}
	for _, wanted := range c.texts {
		if holds(text, wanted) {
			return true
		}
	}
	return false
}

// has reports whether every (has_all) or any wanted entry is among entries (sorted).
func (c *cleaf) has(entries []string) bool {
	if len(entries) == 0 {
		return false
	}
	for _, wanted := range c.texts {
		_, found := slices.BinarySearch(entries, wanted)
		if found != c.every {
			return found
		}
	}
	return c.every
}

// maxPooledKeys is the largest key buffer kept for reuse; one grown by an unusually long
// text is left to the collector rather than pinned in the pool.
const maxPooledKeys = 16 << 10

// keyBuffers holds the buffers a document's trigram keys are computed into.
var keyBuffers = sync.Pool{New: func() any {
	buf := make([]uint64, 0, 256)
	return &buf
}}

func (c *cleaf) similar(text string) bool {
	buf, _ := keyBuffers.Get().(*[]uint64)
	if buf == nil {
		fresh := make([]uint64, 0, 256)
		buf = &fresh
	}
	keys := analysis.AppendTrigramKeys((*buf)[:0], text)
	holds := analysis.SimilarityKeys(keys, c.keys) >= c.minimum
	if cap(keys) <= maxPooledKeys {
		*buf = keys[:0]
		keyBuffers.Put(buf)
	}
	return holds
}
