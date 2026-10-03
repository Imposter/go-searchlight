package search

import (
	"cmp"
	"math"
	"slices"
	"strings"
	"sync/atomic"
	"unicode/utf8"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/segment"
)

// The plan.
//
// A query compiles once per request into a tree of pnodes. Per segment, the tree is
// evaluated against a scope (the documents still in question) and returns exactly the
// scope's documents that match:
//
//   - all evaluates its children cheapest first, each within what the previous left,
//     and stops at an empty scope;
//   - any evaluates each child within the documents no earlier child matched, so a
//     costly child never re-checks a document already in;
//   - not is the scope minus its child (negation over live documents, since the root
//     scope is the segment's live documents);
//   - a leaf asks the index for its candidates: documents that surely match, and
//     documents that may (a contains needle's grams, a phrase's words). Sure ones are
//     taken as they are, maybe ones are checked one at a time by the residual
//     ([segExec.verify]) with the leaf's own compiled matcher.
//
// Cost: each child's cardinality is estimated from the segment's term statistics
// (TermFreq, presence, number column stats), and a child that needs a residual check
// is put after every child that does not, so it checks as few documents as possible.
//
// Filter cache: a leaf whose bitmap is costly to build (a range, a union of terms, a
// verified contains) is cached per segment, keyed by its canonical bytes, once the
// same leaf has been seen in an earlier request ([leafUsage]); a cached entry is the
// leaf's exact matches over the whole segment, deletes not applied.

type nodeKind uint8

const (
	nFalse nodeKind = iota
	nTrue
	nAll
	nAny
	nNot
	nLeaf
)

type pnode struct {
	kind     nodeKind
	children []*pnode
	leaf     *leafPlan
}

// leafPlan is one condition, compiled.
type leafPlan struct {
	leaf  *query.Leaf
	field string
	op    string
	arg   query.Arg
	// key is the leaf's canonical bytes: its filter cache key.
	key string
	// match is the leaf alone, compiled: the residual check.
	match *query.Compiled
	// needsBody: the residual reads what only the stored body holds (a text field's
	// words), so it re-analyzes the body; otherwise it reads doc values.
	needsBody bool
	// cacheable: the leaf's bitmap is costly enough to cache; useCache: and it was seen
	// before, so caching it is likely to pay.
	cacheable, useCache bool
	// texts are the normalized needles of contains*, starts_with and has*.
	texts []string
	// phrases are words_*'s phrases, each as its words; never: a words_all phrase
	// with no word, so the condition never holds.
	phrases [][]string
	never   bool
	every   bool
}

func compileNode(n query.Node) *pnode {
	switch x := n.(type) {
	case *query.All:
		if x == nil {
			return &pnode{kind: nFalse}
		}
		if len(x.Children) == 0 {
			return &pnode{kind: nTrue}
		}
		p := &pnode{kind: nAll}
		for _, c := range x.Children {
			p.children = append(p.children, compileNode(c))
		}
		return p
	case *query.Any:
		if x == nil || len(x.Children) == 0 {
			return &pnode{kind: nFalse}
		}
		p := &pnode{kind: nAny}
		for _, c := range x.Children {
			p.children = append(p.children, compileNode(c))
		}
		return p
	case *query.Not:
		if x == nil {
			return &pnode{kind: nFalse}
		}
		return &pnode{kind: nNot, children: []*pnode{compileNode(x.Child)}}
	case *query.Leaf:
		if x == nil {
			return &pnode{kind: nFalse}
		}
		return &pnode{kind: nLeaf, leaf: compileLeaf(x)}
	default:
		return &pnode{kind: nFalse}
	}
}

func compileLeaf(l *query.Leaf) *leafPlan {
	lp := &leafPlan{
		leaf: l, field: l.Field, op: l.Op, arg: l.Decoded(),
		key:   string(query.Canonical(l)),
		match: query.Compile(l),
	}
	switch l.Op {
	case query.OpContains, query.OpContainsAny, query.OpContainsAll, query.OpHas, query.OpHasAny, query.OpHasAll:
		lp.every = l.Op == query.OpContainsAll || l.Op == query.OpHasAll
		lp.texts = argNorms(&lp.arg)
	case query.OpStartsWith:
		if lp.arg.Kind == query.ArgString {
			lp.texts = []string{lp.arg.Scalar.Norm}
		}
	case query.OpWordsAll, query.OpWordsAny:
		lp.every = l.Op == query.OpWordsAll
		lp.needsBody = true
		seen := map[string]bool{}
		for _, s := range argStrings(&lp.arg) {
			w := analysis.Words(s.Text)
			if w == analysis.NoWords {
				if lp.every {
					lp.never = true
				}
				continue
			}
			if !seen[w] {
				seen[w] = true
				lp.phrases = append(lp.phrases, strings.Fields(w))
			}
		}
	}
	switch l.Op {
	case query.OpLt, query.OpLte, query.OpGt, query.OpGte, query.OpBetween, query.OpIn,
		query.OpContains, query.OpContainsAny, query.OpContainsAll, query.OpStartsWith,
		query.OpWordsAll, query.OpWordsAny, query.OpSimilar, query.OpEmpty, query.OpNonempty:
		lp.cacheable = true
	case query.OpHasAny, query.OpHasAll:
		lp.cacheable = len(lp.texts) > 1
	case query.OpEq, query.OpNe:
		lp.cacheable = lp.arg.Kind == query.ArgNumber
	}
	if lp.cacheable {
		lp.useCache = leafUsage.seen(lp.key)
	}
	return lp
}

// argStrings returns a value's strings: the scalar, or a list's string entries (the
// matcher's texts()).
func argStrings(a *query.Arg) []query.Scalar {
	switch a.Kind {
	case query.ArgString:
		return []query.Scalar{a.Scalar}
	case query.ArgList:
		var out []query.Scalar
		for _, s := range a.List {
			if s.Kind == query.ArgString {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

func argNorms(a *query.Arg) []string {
	ss := argStrings(a)
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		out = append(out, s.Norm)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// usageSketch counts how often leaves are seen, approximately (one row of hashed
// counters): a leaf is cached only from its second sighting, so one-off conditions
// never churn the filter cache.
type usageSketch struct {
	counts [4096]atomic.Uint32
}

var leafUsage usageSketch

// seen counts key and reports whether it was seen before.
func (u *usageSketch) seen(key string) bool {
	h := hashString(key)
	c := &u.counts[h%uint64(len(u.counts))]
	for {
		n := c.Load()
		if n >= 1<<20 {
			return true
		}
		if c.CompareAndSwap(n, n+1) {
			return n >= 1
		}
	}
}

// estimate is a child's estimated matches in a segment and whether evaluating it
// checks documents one at a time.
type estimate struct {
	card     uint64
	residual bool
}

func (s *segExec) estimate(n *pnode) estimate {
	switch n.kind {
	case nFalse:
		return estimate{}
	case nTrue:
		return estimate{card: uint64(s.n)}
	case nNot:
		e := s.estimate(n.children[0])
		return estimate{card: uint64(s.n) - min(e.card, uint64(s.n)), residual: e.residual}
	case nAll:
		out := estimate{card: uint64(s.n)}
		for _, c := range n.children {
			e := s.estimate(c)
			out.card = min(out.card, e.card)
			out.residual = out.residual || e.residual
		}
		return out
	case nAny:
		var out estimate
		for _, c := range n.children {
			e := s.estimate(c)
			out.card = min(out.card+e.card, uint64(s.n))
			out.residual = out.residual || e.residual
		}
		return out
	default:
		return s.estimateLeaf(n.leaf)
	}
}

func (s *segExec) estimateLeaf(lp *leafPlan) estimate {
	if e, ok := s.estimates[lp]; ok {
		return e
	}
	e := s.estimateLeafUncached(lp)
	s.estimates[lp] = e
	return e
}

func (s *segExec) estimateLeafUncached(lp *leafPlan) estimate {
	if lp.useCache {
		// A cached bitmap is exact and costs nothing more to use.
		if bm, ok := s.cacheGet(lp); ok {
			return estimate{card: bm.GetCardinality()}
		}
	}
	r, f := s.r, lp.field
	n := uint64(s.n)
	present := func() uint64 { return r.Present(f).GetCardinality() }
	switch lp.op {
	case query.OpExists:
		want := lp.arg.Kind != query.ArgBool || lp.arg.Scalar.Bool
		if want {
			return estimate{card: present()}
		}
		return estimate{card: n - min(present(), n)}
	case query.OpEq, query.OpNe:
		var c uint64
		switch lp.arg.Kind {
		case query.ArgString:
			c = uint64(r.TermFreq(f, kindValue, lp.arg.Scalar.Norm))
		case query.ArgBool:
			c = uint64(r.TermFreq(f, kindValue, boolTerm(lp.arg.Scalar.Bool)))
		case query.ArgNumber:
			c = s.rangeEstimate(f, lp.arg.Scalar.Number, lp.arg.Scalar.Number)
		}
		if lp.op == query.OpNe {
			return estimate{card: n - min(c, n)}
		}
		return estimate{card: c}
	case query.OpIn:
		var c uint64
		for _, sc := range lp.arg.List {
			switch sc.Kind {
			case query.ArgString:
				c += uint64(r.TermFreq(f, kindValue, sc.Norm))
			case query.ArgBool:
				c += uint64(r.TermFreq(f, kindValue, boolTerm(sc.Bool)))
			case query.ArgNumber:
				c += s.rangeEstimate(f, sc.Number, sc.Number)
			}
		}
		return estimate{card: min(c, n)}
	case query.OpLt, query.OpLte:
		return estimate{card: s.rangeEstimate(f, negInf, lp.arg.Scalar.Number)}
	case query.OpGt, query.OpGte:
		return estimate{card: s.rangeEstimate(f, lp.arg.Scalar.Number, posInf)}
	case query.OpBetween:
		if len(lp.arg.List) == 2 {
			return estimate{card: s.rangeEstimate(f, lp.arg.List[0].Number, lp.arg.List[1].Number)}
		}
		return estimate{}
	case query.OpHas, query.OpHasAny, query.OpHasAll:
		var c uint64
		lowest := n
		for _, t := range lp.texts {
			tf := uint64(r.TermFreq(f, kindEntry, t))
			c += tf
			lowest = min(lowest, tf)
		}
		if lp.every {
			return estimate{card: lowest}
		}
		return estimate{card: min(c, n)}
	case query.OpContains, query.OpContainsAny, query.OpContainsAll:
		var c uint64
		lowest := n
		for _, t := range lp.texts {
			e := s.gramEstimate(f, t)
			c += e
			lowest = min(lowest, e)
		}
		if lp.every {
			return estimate{card: lowest, residual: true}
		}
		return estimate{card: min(c, n), residual: true}
	case query.OpStartsWith:
		if len(lp.texts) == 1 {
			return estimate{card: s.gramEstimate(f, lp.texts[0])}
		}
		return estimate{}
	case query.OpWordsAll, query.OpWordsAny:
		var c uint64
		lowest := n
		for _, words := range lp.phrases {
			e := n
			for _, w := range words {
				e = min(e, uint64(r.TermFreq(f, kindWord, w)))
			}
			c += e
			lowest = min(lowest, e)
		}
		if lp.every {
			return estimate{card: lowest, residual: true}
		}
		return estimate{card: min(c, n), residual: true}
	case query.OpSimilar:
		return estimate{card: present(), residual: true}
	case query.OpNonempty:
		return estimate{card: present()}
	case query.OpEmpty:
		return estimate{card: n}
	default:
		return estimate{}
	}
}

// gramEstimate bounds a needle's matches by its rarest gram (or the field's presence
// for a needle too short to have one).
func (s *segExec) gramEstimate(field, needle string) uint64 {
	if field == IDField || utf8.RuneCountInString(needle) < 3 {
		return s.r.Present(field).GetCardinality()
	}
	lowest := uint64(s.n)
	for _, g := range analysis.Substrings3(needle) {
		lowest = min(lowest, uint64(s.r.TermFreq(field, kindGram, g)))
	}
	return lowest + s.r.Truncated(field).GetCardinality()
}

// rangeEstimate is how many values of field fall in [lo, hi], assuming them spread
// evenly between the column's minimum and maximum.
func (s *segExec) rangeEstimate(field string, lo, hi float64) uint64 {
	st := s.r.Numbers(field).Stats()
	if st.Count == 0 || hi < lo || hi < st.Min || lo > st.Max {
		return 0
	}
	if st.Max == st.Min {
		return uint64(st.Count)
	}
	lo, hi = max(lo, st.Min), min(hi, st.Max)
	frac := (hi - lo) / (st.Max - st.Min)
	return uint64(frac*float64(st.Count)) + 1
}

// order returns children in evaluation order: index-only children first, then by
// estimated matches (ascending for all, so the scope shrinks fastest; descending for
// any, so the documents left to check shrink fastest).
func (s *segExec) order(children []*pnode, ascending bool) []*pnode {
	if len(children) < 2 {
		return children
	}
	type ranked struct {
		n *pnode
		e estimate
	}
	rs := make([]ranked, len(children))
	for i, c := range children {
		rs[i] = ranked{c, s.estimate(c)}
	}
	slices.SortStableFunc(rs, func(a, b ranked) int {
		if a.e.residual != b.e.residual {
			if a.e.residual {
				return 1
			}
			return -1
		}
		if ascending {
			return cmp.Compare(a.e.card, b.e.card)
		}
		return cmp.Compare(b.e.card, a.e.card)
	})
	out := make([]*pnode, len(rs))
	for i := range rs {
		out[i] = rs[i].n
	}
	return out
}

// eval returns the documents of scope that n matches, as a new bitmap. It never
// changes scope.
func (s *segExec) eval(n *pnode, scope *roaring.Bitmap) *roaring.Bitmap {
	if s.err != nil || scope.IsEmpty() {
		return roaring.New()
	}
	switch n.kind {
	case nFalse:
		return roaring.New()
	case nTrue:
		return scope.Clone()
	case nNot:
		out := scope.Clone()
		out.AndNot(s.eval(n.children[0], scope))
		return out
	case nAll:
		cur := scope
		for _, c := range s.order(n.children, true) {
			cur = s.eval(c, cur)
			if cur.IsEmpty() {
				break
			}
		}
		return cur
	case nAny:
		out := roaring.New()
		rest := scope
		for _, c := range s.order(n.children, false) {
			got := s.eval(c, rest)
			if got.IsEmpty() {
				continue
			}
			out.Or(got)
			rest = roaring.AndNot(rest, got)
			if rest.IsEmpty() {
				break
			}
		}
		return out
	default:
		return s.evalLeaf(n.leaf, scope)
	}
}

// evalLeaf returns the documents of scope that lp matches.
func (s *segExec) evalLeaf(lp *leafPlan, scope *roaring.Bitmap) *roaring.Bitmap {
	if lp.never {
		return roaring.New()
	}
	if lp.useCache {
		if bm, ok := s.cacheGet(lp); ok {
			return roaring.And(bm, scope)
		}
	}
	c := s.candidates(lp)
	if c.maybe == nil || c.maybe.IsEmpty() {
		if lp.useCache {
			return roaring.And(s.cachePut(lp, c.sure), scope)
		}
		return roaring.And(c.sure, scope)
	}
	if lp.useCache && 2*scope.GetCardinality() >= uint64(s.n) {
		// Most of the segment is in question anyway: verify all of it once, and cache.
		full := c.sure.Clone()
		check := roaring.AndNot(c.maybe, c.sure)
		s.verify(lp, check, full)
		if s.err != nil {
			return roaring.New()
		}
		return roaring.And(s.cachePut(lp, full), scope)
	}
	out := roaring.And(c.sure, scope)
	check := roaring.And(c.maybe, scope)
	check.AndNot(out)
	s.verify(lp, check, out)
	return out
}

// isText reports whether field holds text in this segment (a keyword column): its
// KindValue terms are texts, not a bool's true and false.
func (s *segExec) isText(field string) bool {
	return s.r.Keywords(field).Exists()
}

const (
	kindValue = segment.KindValue
	kindEntry = segment.KindEntry
	kindWord  = segment.KindWord
	kindGram  = segment.KindGram
)

var (
	negInf = math.Inf(-1)
	posInf = math.Inf(1)
)

func boolTerm(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
