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
	default:
		return progNever
	}
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
	case pNonempty:
		return len(v.Entries) > 0
	default:
		return false
	}
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

// ---- validating ----

var errBadProgram = errors.New("a malformed program")

// checkProg checks that p is exactly one well-formed program over numFields fields, so
// that evaluating it reads nothing out of range.
func checkProg(p []byte, numFields uint32) error {
	n, err := progNode(p, numFields, 0)
	if err != nil {
		return err
	}
	if n != len(p) {
		return errBadProgram
	}
	return nil
}

func progNode(p []byte, numFields uint32, depth int) (int, error) {
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
			got, err := progNode(r.p[:size], numFields, depth+1)
			if err != nil || uint64(got) != size { //nolint:gosec // got is a length, never negative
				return 0, errBadProgram
			}
			r.p = r.p[size:]
		}
	case pNot:
		got, err := progNode(r.p, numFields, depth+1)
		if err != nil {
			return 0, err
		}
		r.p = r.p[got:]
	default:
		if tag >= numProgTags || r.uvarint() >= uint64(numFields) {
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
	default:
		r.ok = false
	}
}
