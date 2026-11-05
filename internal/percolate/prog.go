package percolate

import (
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"strings"
	"unsafe"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// Programs are the compact compiled form verification runs: each verification class's
// query, compiled at build time into a flat byte tree that names fields by their index
// in the segment's field table, so a document is matched against it through a dense
// slice of its values (one map lookup per field per segment, none per candidate) and
// nothing is decoded, allocated or compiled per query at Open or per document.
//
// A program is one node:
//
//	all, any   tag, child count (uvarint), then per child its size (uvarint) and bytes
//	not        tag, then its child
//	true/false tag
//	leaf       tag, field (uvarint), then the tag's operands:
//	             exists           want (a byte, 0 or 1)
//	             empty, nonempty  none
//	             eq/ne text       a string
//	             eq/ne number     a float64
//	             eq/ne bool       a byte
//	             in               flags (1 true, 2 false), texts (count, strings), numbers (count, float64s)
//	             lt..gte          a float64
//	             between          two float64s
//	             contains*, words*, has*, starts_with
//	                              texts (count, strings)
//	             similar          minimum (a float64), keys (count, u64s)
//	             date             op (a byte: 0 lt, 1 lte, 2 gt, 3 gte, 4 between), then per bound
//	                              (two for between) a kind byte and a float64 (0), or now's
//	                              date math steps (1: count, then per step its unit, round and
//	                              N), applied to the percolation's now
//	             box              per end (x's lower and upper, then y's) a byte, 0 for
//	                              none, 1 exclusive, 2 inclusive, then the bound as a date
//	                              bound is written
//	nested     tag, the nested field (uvarint), the sub-fields its query reads (count,
//	           then each, uvarints), then its query: held when one object of the field
//	           does, its sub-fields read from the object
//
// where a string is a uvarint length and its bytes, and numbers are little-endian. The
// compiler mirrors [query.Compile] exactly (the same folding of constant conditions,
// the same reading of every value, children ordered cheapest first), and the evaluator
// mirrors [query.Compiled.Match], so a program's verdict is the matcher's; the
// completeness and parity tests hold the two to it.
const (
	pFalse byte = iota
	pTrue
	pAll
	pAny
	pNot
	pExists
	pEmpty
	pNonempty
	pEqText
	pNeText
	pEqNum
	pNeNum
	pEqBool
	pNeBool
	pIn
	pLt
	pLte
	pGt
	pGte
	pBetween
	pContains
	pContainsAll
	pStartsWith
	pWords
	pWordsAll
	pSimilar
	pHas
	pHasAll
	pDate
	pBox
	pNested

	numProgTags
)

// maxProgDepth bounds a program's nesting. A parsed query has at most query.MaxNodes
// nodes, so its program nests less deeply; a deeper one is a corrupt file.
const maxProgDepth = 2 * query.MaxNodes

// Costs order a group's children cheapest first, as query.Compile orders them.
const (
	costCompare = 1
	costLookup  = 2
	costScan    = 4
	costSimilar = 16
)

type pnode struct {
	tag      byte
	cost     int
	children []pnode
	field    uint32
	operands []byte
}

var (
	progNever  = pnode{tag: pFalse}
	progAlways = pnode{tag: pTrue}
)

// progCompiler compiles queries into programs, naming fields through field.
type progCompiler struct {
	field func(name string) uint32
}

func (pc *progCompiler) compile(n query.Node) []byte {
	root := pc.node(n)
	return appendProg(nil, &root)
}

func (pc *progCompiler) node(n query.Node) pnode {
	switch x := n.(type) {
	case *query.All:
		if x == nil {
			return progNever
		}
		return pc.group(pAll, x.Children)
	case *query.Any:
		if x == nil {
			return progNever
		}
		return pc.group(pAny, x.Children)
	case *query.Not:
		if x == nil {
			return progNever
		}
		child := pc.node(x.Child)
		switch child.tag {
		case pTrue:
			return progNever
		case pFalse:
			return progAlways
		case pNot:
			return child.children[0]
		}
		return pnode{tag: pNot, cost: child.cost, children: []pnode{child}}
	case *query.Leaf:
		if x == nil {
			return progNever
		}
		return pc.leaf(x)
	case *query.Nested:
		if x == nil {
			return progNever
		}
		return pc.nested(x)
	default:
		return progNever
	}
}

// nested compiles a nested node: its query, and the fields that query reads (each a
// sub-field), which evaluation fills from each object in turn. A query that never
// holds makes the node never hold; one that always does holds of any object.
func (pc *progCompiler) nested(n *query.Nested) pnode {
	child := pc.node(n.Query)
	if child.tag == pFalse {
		return progNever
	}
	var fields []uint32
	var collect func(p *pnode)
	collect = func(p *pnode) {
		switch p.tag {
		case pTrue, pFalse:
		case pAll, pAny, pNot:
			for i := range p.children {
				collect(&p.children[i])
			}
		default:
			if !slices.Contains(fields, p.field) {
				fields = append(fields, p.field)
			}
		}
	}
	collect(&child)
	slices.Sort(fields)
	out := pnode{tag: pNested, cost: costScan + child.cost, field: pc.field(n.Path), children: []pnode{child}}
	out.operands = binary.AppendUvarint(out.operands, uint64(len(fields)))
	for _, f := range fields {
		out.operands = binary.AppendUvarint(out.operands, uint64(f))
	}
	return out
}

// group folds constant children away (all: a false child is false, true children drop
// out; any the other way round), keeps a lone child as itself, and orders the rest
// cheapest first.
func (pc *progCompiler) group(tag byte, nodes []query.Node) pnode {
	decided, dropped := pFalse, pTrue
	if tag == pAny {
		decided, dropped = pTrue, pFalse
	}
	children := make([]pnode, 0, len(nodes))
	for _, node := range nodes {
		child := pc.node(node)
		switch child.tag {
		case decided:
			return pnode{tag: decided}
		case dropped:
			continue
		}
		children = append(children, child)
	}
	switch len(children) {
	case 0:
		return pnode{tag: dropped}
	case 1:
		return children[0]
	}
	slices.SortStableFunc(children, func(a, b pnode) int { return a.cost - b.cost })
	cost := 0
	for i := range children {
		cost += children[i].cost
	}
	return pnode{tag: tag, cost: cost, children: children}
}

func (pc *progCompiler) leaf(l *query.Leaf) pnode {
	a := l.Decoded()
	out := pnode{cost: costCompare, field: pc.field(l.Field)}
	if l.Op == query.OpBox {
		return boxLeaf(&a, out)
	}
	if l.IsDateMath() {
		fixed, _ := query.ResolveFixed(l).(*query.Leaf)
		if fixed.IsDateMath() {
			return dateLeaf(l.Op, &a, out)
		}
		a = fixed.Decoded()
	}
	switch l.Op {
	case query.OpExists:
		out.tag = pExists
		want := a.Kind != query.ArgBool || a.Scalar.Bool
		out.operands = append(out.operands, boolByte(want))
	case query.OpEmpty:
		out.tag = pEmpty
	case query.OpNonempty:
		out.tag = pNonempty
	case query.OpEq, query.OpNe:
		ne := l.Op == query.OpNe
		s := &a.Scalar
		switch {
		case a.Kind == query.ArgBool:
			out.tag = pick(ne, pNeBool, pEqBool)
			out.operands = append(out.operands, boolByte(s.Bool))
		case a.Kind == query.ArgString:
			out.tag = pick(ne, pNeText, pEqText)
			out.operands = appendStr(out.operands, s.Norm)
		case a.Kind == query.ArgNumber && s.Finite:
			out.tag = pick(ne, pNeNum, pEqNum)
			out.operands = appendF64(out.operands, s.Number)
		case ne:
			return progAlways
		default:
			return progNever
		}
	case query.OpIn:
		if !pc.in(&a, &out) {
			return progNever
		}
		out.tag, out.cost = pIn, costLookup
	case query.OpLt, query.OpLte, query.OpGt, query.OpGte:
		if a.Kind != query.ArgNumber || !a.Scalar.Finite {
			return progNever
		}
		out.tag = compareTags[l.Op]
		out.operands = appendF64(out.operands, a.Scalar.Number)
	case query.OpBetween:
		if a.Kind != query.ArgList || len(a.List) != 2 || !finite(&a.List[0]) || !finite(&a.List[1]) {
			return progNever
		}
		lo, hi := a.List[0].Number, a.List[1].Number
		if lo > hi {
			return progNever
		}
		out.tag = pBetween
		out.operands = appendF64(appendF64(out.operands, lo), hi)
	case query.OpContains, query.OpContainsAny, query.OpContainsAll:
		list := sortedNorms(texts(&a))
		if len(list) == 0 {
			return progNever
		}
		out.tag, out.cost = pick(l.Op == query.OpContainsAll, pContainsAll, pContains), costScan
		out.operands = appendStrs(out.operands, list)
	case query.OpStartsWith:
		if a.Kind != query.ArgString {
			return progNever
		}
		out.tag, out.cost = pStartsWith, costLookup
		out.operands = appendStrs(out.operands, []string{a.Scalar.Norm})
	case query.OpWordsAll, query.OpWordsAny:
		every := l.Op == query.OpWordsAll
		var list []string
		for _, s := range texts(&a) {
			w := analysis.Words(s.Text)
			if w == analysis.NoWords {
				if every {
					return progNever
				}
				continue
			}
			list = append(list, w)
		}
		slices.Sort(list)
		list = slices.Compact(list)
		if len(list) == 0 {
			return progNever
		}
		out.tag, out.cost = pick(every, pWordsAll, pWords), costScan
		out.operands = appendStrs(out.operands, list)
	case query.OpSimilar:
		minimum, ok := likeness(&a)
		if !ok {
			return progNever
		}
		keys := analysis.TrigramKeys(a.Object["text"].Norm)
		if len(keys) == 0 {
			return progNever
		}
		out.tag, out.cost = pSimilar, costSimilar
		out.operands = appendF64(out.operands, minimum)
		out.operands = binary.AppendUvarint(out.operands, uint64(len(keys)))
		for _, k := range keys {
			out.operands = binary.LittleEndian.AppendUint64(out.operands, k)
		}
	case query.OpHas, query.OpHasAny, query.OpHasAll:
		list := sortedNorms(texts(&a))
		if len(list) == 0 {
			return progNever
		}
		out.tag, out.cost = pick(l.Op == query.OpHasAll, pHasAll, pHas), costLookup
		out.operands = appendStrs(out.operands, list)
	default:
		return progNever
	}
	return out
}

// dateOps are the date leaf's op bytes.
var dateOps = map[string]byte{query.OpLt: 0, query.OpLte: 1, query.OpGt: 2, query.OpGte: 3, query.OpBetween: 4}

// Date bound kinds.
const (
	dateNumber = 0 // a float64
	dateNow    = 1 // now, then its steps: count, and per step unit, round (0 or 1), N (varint)
)

// dateLeaf compiles a condition with date math that reads now: a program keeps its
// steps, parsed, and each evaluation applies them to its percolation's now, so a saved
// query's program never holds an instant. A bound that does not read now is resolved
// here, once (as [query.ResolveFixed] resolves it); a condition with none such is a
// plain range, compiled to the range tags.
func dateLeaf(op string, a *query.Arg, out pnode) pnode {
	bounds := []query.Scalar{a.Scalar}
	if op == query.OpBetween {
		if a.Kind != query.ArgList || len(a.List) != 2 {
			return progNever
		}
		bounds = a.List
	}
	out.tag, out.cost = pDate, costLookup
	out.operands = append(out.operands, dateOps[op])
	for i := range bounds {
		s := &bounds[i]
		if s.Kind != query.ArgString {
			if !finite(s) {
				return progNever
			}
			out.operands = appendF64(append(out.operands, dateNumber), s.Number)
			continue
		}
		d, why := query.ParseDateMath(s.Text)
		if why != "" {
			return progNever
		}
		if !d.Relative() {
			ms, ok := d.Resolve(0, i == 1 || op == query.OpGt || op == query.OpLte)
			if !ok {
				return progNever
			}
			out.operands = appendF64(append(out.operands, dateNumber), float64(ms))
			continue
		}
		steps := d.Steps()
		out.operands = binary.AppendUvarint(append(out.operands, dateNow), uint64(len(steps)))
		for _, st := range steps {
			out.operands = binary.AppendVarint(append(out.operands, st.Unit, boolByte(st.Round)), st.N)
		}
	}
	return out
}

// Box end states.
const (
	boxNone      = 0
	boxExclusive = 1
	boxInclusive = 2
)

// boxLeaf compiles a box: each end's bound a number, a date resolved here when it does
// not read now (as [query.ResolveFixed] resolves it), or now's date math steps, applied
// at each evaluation. A box that holds no point at every now (a bound that is no
// finite number, or, with no bound reading now, an axis whose bounds leave nothing
// between them) never holds, as the matcher compiles it.
func boxLeaf(a *query.Arg, out pnode) pnode {
	if a.Kind != query.ArgBox || a.Box == nil {
		return progNever
	}
	out.tag, out.cost = pBox, costLookup
	fixed := query.Box{}
	relative := false
	for i, iv := range a.Box.Axes() {
		fixedAxis := fixed.Axes()[i]
		fixedEnds := [2]*query.Bound{&fixedAxis.Lo, &fixedAxis.Hi}
		for j, end := range []*query.Bound{&iv.Lo, &iv.Hi} {
			if !end.Set {
				out.operands = append(out.operands, boxNone)
				continue
			}
			out.operands = append(out.operands, pick[byte](end.Inclusive, boxInclusive, boxExclusive))
			roundUp := end.RoundsUp(j == 1)
			s := &end.Value
			var bound query.Scalar
			switch {
			case s.Kind != query.ArgString:
				if !finite(s) {
					return progNever
				}
				bound = *s
			default:
				d, why := query.ParseDateMath(s.Text)
				if why != "" {
					return progNever
				}
				if d.Relative() {
					relative = true
					steps := d.Steps()
					out.operands = binary.AppendUvarint(append(out.operands, dateNow), uint64(len(steps)))
					for _, st := range steps {
						out.operands = binary.AppendVarint(append(out.operands, st.Unit, boolByte(st.Round)), st.N)
					}
					continue
				}
				ms, ok := d.Resolve(0, roundUp)
				if !ok {
					return progNever
				}
				bound = query.Scalar{Kind: query.ArgNumber, Number: float64(ms), Finite: true}
			}
			out.operands = appendF64(append(out.operands, dateNumber), bound.Number)
			*fixedEnds[j] = query.Bound{Set: true, Inclusive: end.Inclusive, Value: bound}
		}
	}
	if !relative {
		if _, ok := query.CompileBox(&query.Arg{Kind: query.ArgBox, Box: &fixed}); !ok {
			return progNever
		}
	}
	return out
}

// in encodes an in's values by kind (texts and numbers sorted and distinct); false when
// nothing can equal any of them.
func (pc *progCompiler) in(a *query.Arg, out *pnode) bool {
	if a.Kind != query.ArgList {
		return false
	}
	var flags byte
	var list []string
	var nums []float64
	for i := range a.List {
		s := &a.List[i]
		switch s.Kind {
		case query.ArgBool:
			flags |= pick[byte](s.Bool, inTrue, inFalse)
		case query.ArgString:
			list = append(list, s.Norm)
		case query.ArgNumber:
			if s.Finite {
				nums = append(nums, s.Number)
			}
		}
	}
	slices.Sort(list)
	list = slices.Compact(list)
	slices.Sort(nums)
	nums = slices.Compact(nums)
	if len(list) == 0 && len(nums) == 0 && flags == 0 {
		return false
	}
	out.operands = append(out.operands, flags)
	out.operands = appendStrs(out.operands, list)
	out.operands = binary.AppendUvarint(out.operands, uint64(len(nums)))
	for _, n := range nums {
		out.operands = appendF64(out.operands, n)
	}
	return true
}

var compareTags = map[string]byte{query.OpLt: pLt, query.OpLte: pLte, query.OpGt: pGt, query.OpGte: pGte}

const (
	inTrue  = 1
	inFalse = 2
)

// likeness is similar's minimum, false when the matcher refuses the value: a text that
// is blank or not a string, or a minimum outside (0, 1].
func likeness(a *query.Arg) (float64, bool) {
	if a.Kind != query.ArgObject {
		return 0, false
	}
	text, hasText := a.Object["text"]
	minimum, hasMin := a.Object["min"]
	if !hasText || text.Kind != query.ArgString || strings.TrimFunc(text.Text, analysis.IsSpace) == "" {
		return 0, false
	}
	if !hasMin || minimum.Kind != query.ArgNumber || !minimum.Finite {
		return 0, false
	}
	return minimum.Number, minimum.Number > 0 && minimum.Number <= 1
}

func sortedNorms(scalars []query.Scalar) []string {
	out := make([]string, 0, len(scalars))
	for i := range scalars {
		out = append(out, scalars[i].Norm)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func pick[T any](cond bool, yes, no T) T {
	if cond {
		return yes
	}
	return no
}

func boolByte(b bool) byte { return pick[byte](b, 1, 0) }

func appendStr(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

func appendStrs(dst []byte, list []string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(list)))
	for _, s := range list {
		dst = appendStr(dst, s)
	}
	return dst
}

func appendF64(dst []byte, f float64) []byte {
	return binary.LittleEndian.AppendUint64(dst, math.Float64bits(f))
}

func appendProg(dst []byte, n *pnode) []byte {
	dst = append(dst, n.tag)
	switch n.tag {
	case pTrue, pFalse:
	case pAll, pAny:
		dst = binary.AppendUvarint(dst, uint64(len(n.children)))
		for i := range n.children {
			child := appendProg(nil, &n.children[i])
			dst = binary.AppendUvarint(dst, uint64(len(child)))
			dst = append(dst, child...)
		}
	case pNot:
		dst = appendProg(dst, &n.children[0])
	case pNested:
		dst = binary.AppendUvarint(dst, uint64(n.field))
		dst = append(dst, n.operands...)
		dst = appendProg(dst, &n.children[0])
	default:
		dst = binary.AppendUvarint(dst, uint64(n.field))
		dst = append(dst, n.operands...)
	}
	return dst
}

// ---- evaluating ----

func uv(p []byte) (int, []byte) {
	if p[0] < 0x80 {
		return int(p[0]), p[1:]
	}
	v, n := binary.Uvarint(p)
	return int(v), p[n:] //nolint:gosec // validated: within the program's length
}

// str reads a string of a validated program, as a view of its bytes (which stay put
// while the segment is open).
func str(p []byte) (string, []byte) {
	n, p := uv(p)
	return unsafe.String(unsafe.SliceData(p), n), p[n:]
}

func pf64(p []byte) float64 { return math.Float64frombits(binary.LittleEndian.Uint64(p)) }

func (sc *scratch) evalProg(p []byte) bool { return len(p) == 0 || sc.eval(p) }

// eval reports whether the document whose values sc holds satisfies program p.
func (sc *scratch) eval(p []byte) bool {
	tag := p[0]
	p = p[1:]
	switch tag {
	case pTrue:
		return true
	case pFalse:
		return false
	case pAll, pAny:
		all := tag == pAll
		n, p := uv(p)
		for range n {
			var size int
			size, p = uv(p)
			if sc.eval(p[:size]) != all {
				return !all
			}
			p = p[size:]
		}
		return all
	case pNot:
		return !sc.eval(p)
	case pNested:
		return sc.nested(p)
	}
	f, p := uv(p)
	v := &sc.vals[f]
	switch tag {
	case pExists:
		return v.Present == (p[0] != 0)
	case pEmpty:
		return !v.Present || len(v.Entries) == 0
	case pNeText, pNeNum, pNeBool:
		return !v.Present || !equals(tag-1, p, v)
	}
	if !v.Present {
		return false
	}
	switch tag {
	case pEqText, pEqNum, pEqBool:
		return equals(tag, p, v)
	case pIn:
		return in(p, v)
	case pLt:
		return v.Number != nil && *v.Number < pf64(p)
	case pLte:
		return v.Number != nil && *v.Number <= pf64(p)
	case pGt:
		return v.Number != nil && *v.Number > pf64(p)
	case pGte:
		return v.Number != nil && *v.Number >= pf64(p)
	case pBetween:
		return v.Number != nil && pf64(p) <= *v.Number && *v.Number <= pf64(p[8:])
	case pContains, pContainsAll:
		return v.Text != nil && some(*v.Text, p, tag == pContainsAll)
	case pStartsWith:
		if v.Text == nil {
			return false
		}
		_, p = uv(p)
		prefix, _ := str(p)
		return strings.HasPrefix(*v.Text, prefix)
	case pWords, pWordsAll:
		return v.Words != "" && some(v.Words, p, tag == pWordsAll)
	case pSimilar:
		return v.Text != nil && sc.similar(f, *v.Text, p)
	case pHas, pHasAll:
		return has(v.Entries, p, tag == pHasAll)
	case pDate:
		return v.Number != nil && sc.date(*v.Number, p)
	case pBox:
		return len(v.Points) > 0 && sc.box(v.Points, p)
	case pNonempty:
		return len(v.Entries) > 0
	default:
		return false
	}
}

// nested is a nested node's verdict (p past its tag): whether one object of its field
// satisfies its query, each object's sub-fields put in their slots of sc.vals while its
// query runs (and similar's keys of them computed again), the slots emptied after: a
// document's own fields never hold a sub-field's name.
func (sc *scratch) nested(p []byte) bool {
	f, p := uv(p)
	n, p := uv(p)
	subs := p
	for range n {
		_, p = uv(p)
	}
	child := p
	objects := sc.vals[f].Objects
	defer func() {
		q := subs
		for range n {
			var s int
			s, q = uv(q)
			sc.vals[s] = schema.Value{}
			sc.simDone[s] = false
		}
	}()
	for i := range objects {
		o := &objects[i]
		if !o.IsObject() {
			continue
		}
		q := subs
		for range n {
			var s int
			s, q = uv(q)
			sc.vals[s] = o.Fields[sc.fields[s].name]
			sc.simDone[s] = false
		}
		if sc.eval(child) {
			return true
		}
	}
	return false
}

func equals(tag byte, p []byte, v *schema.Value) bool {
	switch tag {
	case pEqText:
		want, _ := str(p)
		return v.Text != nil && *v.Text == want
	case pEqNum:
		return v.Number != nil && *v.Number == pf64(p)
	default:
		return v.Bool != nil && *v.Bool == (p[0] != 0)
	}
}

func in(p []byte, v *schema.Value) bool {
	flags := p[0]
	n, p := uv(p[1:])
	if v.Text != nil {
		for range n {
			var s string
			s, p = str(p)
			if s == *v.Text {
				return true
			}
		}
	} else {
		for range n {
			k, rest := uv(p)
			p = rest[k:]
		}
	}
	nums, p := uv(p)
	if v.Number != nil {
		for i := range nums {
			if pf64(p[8*i:]) == *v.Number {
				return true
			}
		}
	}
	if v.Bool != nil {
		return *v.Bool && flags&inTrue != 0 || !*v.Bool && flags&inFalse != 0
	}
	return false
}

func some(text string, p []byte, every bool) bool {
	n, p := uv(p)
	for range n {
		var wanted string
		wanted, p = str(p)
		if strings.Contains(text, wanted) != every {
			return !every
		}
	}
	return every
}

func has(entries []string, p []byte, every bool) bool {
	if len(entries) == 0 {
		return false
	}
	n, p := uv(p)
	for range n {
		var wanted string
		wanted, p = str(p)
		if _, found := slices.BinarySearch(entries, wanted); found != every {
			return found
		}
	}
	return every
}

// similar is similar's verdict on field f's text, the document's trigram keys computed
// once per field.
func (sc *scratch) similar(f int, text string, p []byte) bool {
	if !sc.simDone[f] {
		sc.simKeys[f] = analysis.AppendTrigramKeys(sc.simKeys[f][:0], text)
		sc.simDone[f] = true
		sc.simSet = append(sc.simSet, f)
	}
	minimum := pf64(p)
	n, p := uv(p[8:])
	sc.want = sc.want[:0]
	for i := range n {
		sc.want = append(sc.want, binary.LittleEndian.Uint64(p[8*i:]))
	}
	return analysis.SimilarityKeys(sc.simKeys[f], sc.want) >= minimum
}

// date is a date leaf's verdict on n, its bounds resolved at sc.now; a bound that
// cannot be resolved holds for nothing, as the matcher reads a resolved tree.
func (sc *scratch) date(n float64, p []byte) bool {
	op := p[0]
	p = p[1:]
	switch op {
	case 4:
		lo, ok := sc.dateBound(p, false)
		if !ok {
			return false
		}
		hi, ok := sc.dateBound(skipBound(p), true)
		return ok && lo <= n && n <= hi
	default:
		at, ok := sc.dateBound(p, op == 1 || op == 2)
		switch {
		case !ok:
			return false
		case op == 0:
			return n < at
		case op == 1:
			return n <= at
		case op == 2:
			return n > at
		default:
			return n >= at
		}
	}
}

// box is a box leaf's verdict on points, its bounds resolved at sc.now; a bound that
// cannot be resolved holds for nothing, as the matcher reads a resolved tree.
func (sc *scratch) box(points []schema.Point, p []byte) bool {
	var b query.BoxBounds
	ends := [4]struct {
		at  *float64
		inc *bool
	}{{&b.XLo, &b.XLoInc}, {&b.XHi, &b.XHiInc}, {&b.YLo, &b.YLoInc}, {&b.YHi, &b.YHiInc}}
	for i, e := range ends {
		state := p[0]
		p = p[1:]
		if state == boxNone {
			*e.at, *e.inc = math.Inf(2*(i%2)-1), true
			continue
		}
		inclusive := state == boxInclusive
		upper := i%2 == 1
		at, ok := sc.dateBound(p, upper == inclusive)
		if !ok {
			return false
		}
		*e.at, *e.inc = at, inclusive
		p = skipBound(p)
	}
	return b.Any(points)
}

func (sc *scratch) dateBound(p []byte, roundUp bool) (float64, bool) {
	if p[0] == dateNumber {
		return pf64(p[1:]), true
	}
	ms := sc.now
	n, p := uv(p[1:])
	for range n {
		st := query.DateStep{Unit: p[0], Round: p[1] != 0}
		var k int
		st.N, k = binary.Varint(p[2:])
		p = p[2+k:]
		var ok bool
		if ms, ok = st.Apply(ms, roundUp); !ok {
			return 0, false
		}
	}
	return float64(ms), true
}

func skipBound(p []byte) []byte {
	if p[0] == dateNumber {
		return p[9:]
	}
	n, p := uv(p[1:])
	for range n {
		_, k := binary.Varint(p[2:])
		p = p[2+k:]
	}
	return p
}

// ---- validating ----

var errBadProgram = errors.New("a malformed program")

// checkProg checks that p is exactly one well-formed program over numFields fields, so
// that evaluating it reads nothing out of range.
func checkProg(p []byte, numFields uint32) error { return checkProgTags(p, numFields, numProgTags) }

// checkProgTags is checkProg by a reader that knows the tags below tags alone, as an
// older build does (v1.0.0's knew those below pDate).
func checkProgTags(p []byte, numFields uint32, tags byte) error {
	n, err := progNode(p, numFields, tags, 0)
	if err != nil {
		return err
	}
	if n != len(p) {
		return errBadProgram
	}
	return nil
}

func progNode(p []byte, numFields uint32, tags byte, depth int) (int, error) {
	if len(p) == 0 || depth > maxProgDepth {
		return 0, errBadProgram
	}
	r := progReader{p: p[1:], ok: true}
	switch tag := p[0]; tag {
	case pTrue, pFalse:
	case pAll, pAny:
		n := r.uvarint()
		for i := uint64(0); i < n && r.ok; i++ {
			size := r.uvarint()
			if !r.ok || size > uint64(len(r.p)) {
				return 0, errBadProgram
			}
			got, err := progNode(r.p[:size], numFields, tags, depth+1)
			if err != nil || uint64(got) != size { //nolint:gosec // got is a length, never negative
				return 0, errBadProgram
			}
			r.p = r.p[size:]
		}
	case pNot:
		got, err := progNode(r.p, numFields, tags, depth+1)
		if err != nil {
			return 0, err
		}
		r.p = r.p[got:]
	case pNested:
		if tag >= tags || r.uvarint() >= uint64(numFields) {
			return 0, errBadProgram
		}
		n := r.uvarint()
		for i := uint64(0); i < n && r.ok; i++ {
			if r.uvarint() >= uint64(numFields) {
				return 0, errBadProgram
			}
		}
		if !r.ok {
			return 0, errBadProgram
		}
		got, err := progNode(r.p, numFields, tags, depth+1)
		if err != nil {
			return 0, err
		}
		r.p = r.p[got:]
	default:
		if tag >= tags || r.uvarint() >= uint64(numFields) {
			return 0, errBadProgram
		}
		r.operands(tag)
	}
	if !r.ok {
		return 0, errBadProgram
	}
	return len(p) - len(r.p), nil
}

type progReader struct {
	p  []byte
	ok bool
}

func (r *progReader) uvarint() uint64 {
	if !r.ok {
		return 0
	}
	v, n := binary.Uvarint(r.p)
	if n <= 0 || v > math.MaxInt32 {
		r.ok = false
		return 0
	}
	r.p = r.p[n:]
	return v
}

func (r *progReader) skip(n uint64) {
	if !r.ok || n > uint64(len(r.p)) {
		r.ok = false
		return
	}
	r.p = r.p[n:]
}

func (r *progReader) strs() {
	n := r.uvarint()
	for i := uint64(0); i < n && r.ok; i++ {
		r.skip(r.uvarint())
	}
}

func (r *progReader) operands(tag byte) {
	switch tag {
	case pExists, pEqBool, pNeBool:
		r.skip(1)
	case pEmpty, pNonempty:
	case pEqText, pNeText:
		r.skip(r.uvarint())
	case pEqNum, pNeNum, pLt, pLte, pGt, pGte:
		r.skip(8)
	case pBetween:
		r.skip(16)
	case pIn:
		r.skip(1)
		r.strs()
		r.skip(8 * r.uvarint())
	case pContains, pContainsAll, pWords, pWordsAll, pHas, pHasAll:
		r.strs()
	case pStartsWith:
		if r.uvarint() != 1 {
			r.ok = false
		}
		r.skip(r.uvarint())
	case pSimilar:
		r.skip(8)
		r.skip(8 * r.uvarint())
	case pDate:
		r.date()
	case pBox:
		for range 4 {
			if !r.ok || len(r.p) == 0 || r.p[0] > boxInclusive {
				r.ok = false
				return
			}
			state := r.p[0]
			r.skip(1)
			if state != boxNone {
				r.bound()
			}
		}
	default:
		r.ok = false
	}
}

// date reads a date leaf's operands: its op, then each bound's kind and value.
func (r *progReader) date() {
	if !r.ok || len(r.p) == 0 || r.p[0] > 4 {
		r.ok = false
		return
	}
	bounds := pick(r.p[0] == 4, 2, 1)
	r.skip(1)
	for range bounds {
		r.bound()
	}
}

// bound reads one date bound: its kind, then a float64 or now's steps.
func (r *progReader) bound() {
	if !r.ok || len(r.p) == 0 {
		r.ok = false
		return
	}
	kind := r.p[0]
	r.skip(1)
	switch kind {
	case dateNumber:
		r.skip(8)
	case dateNow:
		steps := r.uvarint()
		for i := uint64(0); i < steps && r.ok; i++ {
			if len(r.p) < 2 || !query.IsDateUnit(r.p[0]) || r.p[1] > 1 {
				r.ok = false
				return
			}
			r.skip(2)
			v, k := binary.Varint(r.p)
			if k <= 0 || v < math.MinInt32 || v > math.MaxInt32 {
				r.ok = false
				return
			}
			r.skip(uint64(k))
		}
	default:
		r.ok = false
	}
}
