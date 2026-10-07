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
// A query compiles once per request into a tree of pnodes. Per segment, it runs in two
// phases, as Lucene's two-phase iteration does.
//
// Approximation ([segExec.approximate]): each node answers over a scope (the documents
// still in question) with documents that surely match and documents that may, from
// the index alone:
//
//   - all approximates its children cheapest first, each within what the previous
//     left possible, and stops at an empty scope;
//   - any approximates each child within the documents no earlier child surely
//     matched;
//   - not is the scope minus its child's possible documents, and its child's maybe
//     ones (negation over live documents: the root scope is the segment's live
//     documents);
//   - a leaf asks the index for its candidates: documents that surely match, and
//     documents that may (a contains needle's grams, a phrase's words).
//
// Verification decides the maybe documents with each leaf's own compiled matcher (the
// residual, residual.go), only as far as the request needs: every one when there are
// aggregations or an exact total ([segExec.resolve], in batches); otherwise only the
// candidates that would enter the top hits ([segExec.check], one at a time), then as
// many more as it takes to confirm TrackTotal+1 matches across the shard, and no more.
//
// Cost: each child's cardinality is estimated from the segment's term statistics
// (TermFreq, presence, number column stats), and a child that needs a residual check
// is put after every child that does not, so it is approximated over the fewest
// documents.
//
// Filter cache: a leaf whose bitmap is costly to build (a range, a union of terms, a
// verified contains) is cached per segment, keyed by its canonical bytes, once the
// same leaf has been seen in an earlier request ([leafUsage]); a cached entry is the
// leaf's exact matches over the whole segment, deletes not applied, and is only ever
// a fully verified bitmap.

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

func compileNode(n query.Node, req uint64) *pnode {
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
			p.children = append(p.children, compileNode(c, req))
		}
		return p
	case *query.Any:
		if x == nil || len(x.Children) == 0 {
			return &pnode{kind: nFalse}
		}
		p := &pnode{kind: nAny}
		for _, c := range x.Children {
			p.children = append(p.children, compileNode(c, req))
		}
		return p
	case *query.Not:
		if x == nil {
			return &pnode{kind: nFalse}
		}
		return &pnode{kind: nNot, children: []*pnode{compileNode(x.Child, req)}}
	case *query.Leaf:
		if x == nil {
			return &pnode{kind: nFalse}
		}
		return &pnode{kind: nLeaf, leaf: compileLeaf(x, req)}
	default:
		return &pnode{kind: nFalse}
	}
}

func compileLeaf(l *query.Leaf, req uint64) *leafPlan {
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
		lp.useCache = leafUsage.seen(lp.key, req)
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
// never churn the filter cache. A request counts once however many shards it runs on
// (each slot remembers the last request that counted it), and every counter halves
// every usageHalfLife counts, so what was popular long ago fades.
type usageSketch struct {
	slots [usageSlots]usageSlot
	ticks atomic.Uint64
}

type usageSlot struct {
	count atomic.Uint32
	last  atomic.Uint64 // the request that last counted this slot
}

const (
	usageSlots    = 4096
	usageHalfLife = 64 * usageSlots
)

var leafUsage usageSketch

// seen counts key once for request req and reports whether an earlier request saw it.
func (u *usageSketch) seen(key string, req uint64) bool {
	slot := &u.slots[hashString(key)%usageSlots]
	if slot.last.Swap(req) == req {
		return slot.count.Load() >= 2 // this request counted it already
	}
	n := slot.count.Add(1)
	if u.ticks.Add(1)%usageHalfLife == 0 {
		u.halve()
	}
	return n >= 2
}

// reset forgets every count (tests that need a leaf unseen).
func (u *usageSketch) reset() {
	for i := range u.slots {
		u.slots[i].count.Store(0)
		u.slots[i].last.Store(0)
	}
}

func (u *usageSketch) halve() {
	for i := range u.slots {
		c := &u.slots[i].count
		for {
			n := c.Load()
			if c.CompareAndSwap(n, n/2) {
				break
			}
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
			c = r.Numbers(f).Count(lp.arg.Scalar.Number, lp.arg.Scalar.Number, true, true)
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
				c += r.Numbers(f).Count(sc.Number, sc.Number, true, true)
			}
		}
		return estimate{card: min(c, n)}
	case query.OpLt, query.OpLte, query.OpGt, query.OpGte, query.OpBetween:
		b, ok := rangeOf(lp)
		if !ok {
			return estimate{}
		}
		return estimate{card: r.Numbers(f).Count(b.lo, b.hi, b.incLo, b.incHi)}
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

// approx is a node's two-phase answer over its scope (Lucene's two-phase iteration):
// documents that surely match, and documents that may, which a residual check
// decides. The two are disjoint and read-only.
type approx struct {
	sure, maybe *roaring.Bitmap
}

func (a approx) possible() *roaring.Bitmap {
	if a.maybe.IsEmpty() {
		return a.sure
	}
	return roaring.Or(a.sure, a.maybe)
}

var emptyApprox = approx{sure: roaring.New(), maybe: roaring.New()}

// approximate computes n's approx over scope, recording every node's (and the order
// its children ran in) for check and resolve. It verifies nothing but leaves the
// filter cache serves or that it decides to verify whole and cache; a child is
// evaluated only over the documents its siblings left possible.
func (s *segExec) approximate(n *pnode, scope *roaring.Bitmap) approx {
	a := s.approximateNode(n, scope)
	s.approxes[n] = a
	return a
}

func (s *segExec) approximateNode(n *pnode, scope *roaring.Bitmap) approx {
	if s.err != nil || scope.IsEmpty() {
		return emptyApprox
	}
	switch n.kind {
	case nFalse:
		return emptyApprox
	case nTrue:
		return approx{sure: scope, maybe: roaring.New()}
	case nNot:
		c := s.approximate(n.children[0], scope)
		return approx{sure: roaring.AndNot(scope, c.possible()), maybe: c.maybe}
	case nAll:
		order := s.order(n.children, true)
		s.orders[n] = order
		sure, possible := scope, scope
		for _, c := range order {
			a := s.approximate(c, possible)
			sure = s.within(a.sure, sure)
			possible = a.possible()
			if possible.IsEmpty() {
				break
			}
		}
		return approx{sure: sure, maybe: roaring.AndNot(possible, sure)}
	case nAny:
		order := s.order(n.children, false)
		s.orders[n] = order
		sure, maybe := roaring.New(), roaring.New()
		rest := scope
		for _, c := range order {
			a := s.approximate(c, rest)
			sure.Or(a.sure)
			maybe.Or(a.maybe)
			rest = roaring.AndNot(rest, a.sure)
			if rest.IsEmpty() {
				break
			}
		}
		maybe.AndNot(sure)
		return approx{sure: sure, maybe: maybe}
	default:
		return s.approximateLeaf(n.leaf, scope)
	}
}

// approximateLeaf is a leaf's approx over scope: its candidates, or the filter
// cache's exact bitmap. Only a fully verified bitmap is ever cached.
func (s *segExec) approximateLeaf(lp *leafPlan, scope *roaring.Bitmap) approx {
	if lp.never {
		return emptyApprox
	}
	if lp.useCache {
		if bm, ok := s.cacheGet(lp); ok {
			return approx{sure: s.within(bm, scope), maybe: roaring.New()}
		}
	}
	if b, ok := rangeOf(lp); ok && s.preferDocValues(lp, scope) {
		return approx{sure: s.numericScan(lp.field, b, scope), maybe: roaring.New()}
	}
	c := s.candidates(lp)
	if c.maybe == nil || c.maybe.IsEmpty() {
		if lp.useCache {
			return approx{sure: s.within(s.cachePut(lp, c.sure), scope), maybe: roaring.New()}
		}
		return approx{sure: s.within(c.sure, scope), maybe: roaring.New()}
	}
	if lp.useCache && 2*scope.GetCardinality() >= uint64(s.n) {
		// Most of the segment is in question anyway: verify all of it once, and cache.
		full := c.sure.Clone()
		s.verify(lp, roaring.AndNot(c.maybe, c.sure), full)
		if s.err != nil {
			return emptyApprox
		}
		return approx{sure: s.within(s.cachePut(lp, full), scope), maybe: roaring.New()}
	}
	sure := s.within(c.sure, scope)
	maybe := roaring.And(c.maybe, scope)
	maybe.AndNot(sure)
	return approx{sure: sure, maybe: maybe}
}

// within is bm in scope, read-only: a leaf's postings or cached bitmap are used in
// place when the scope is every document of the segment.
func (s *segExec) within(bm, scope *roaring.Bitmap) *roaring.Bitmap {
	if scope == s.allDocs {
		return bm
	}
	return roaring.And(bm, scope)
}

// resolve returns exactly the documents of d that n matches, verifying in batches:
// d must lie within n's scope (the root's maybe, or a part of it).
func (s *segExec) resolve(n *pnode, d *roaring.Bitmap) *roaring.Bitmap {
	a, ok := s.approxes[n]
	if !ok || s.err != nil {
		return roaring.New()
	}
	sure := roaring.And(a.sure, d)
	m := roaring.And(a.maybe, d)
	if m.IsEmpty() {
		return sure
	}
	switch n.kind {
	case nLeaf:
		s.verify(n.leaf, m, sure)
		return sure
	case nNot:
		m.AndNot(s.resolve(n.children[0], m))
		sure.Or(m)
		return sure
	case nAll:
		for _, c := range s.orders[n] {
			if m.IsEmpty() {
				break
			}
			m = s.resolve(c, m)
		}
		sure.Or(m)
		return sure
	case nAny:
		for _, c := range s.orders[n] {
			if m.IsEmpty() {
				break
			}
			got := s.resolve(c, m)
			sure.Or(got)
			m.AndNot(got)
		}
		return sure
	default:
		return sure
	}
}

// check reports whether n matches document d (within n's scope), verifying only the
// leaves it must, one document at a time: the lazy half of the two phases, for the
// few candidates that could enter a top-k.
func (s *segExec) check(n *pnode, d uint32) bool {
	a, ok := s.approxes[n]
	if !ok {
		return false
	}
	if a.sure.Contains(d) {
		return true
	}
	if !a.maybe.Contains(d) {
		return false
	}
	switch n.kind {
	case nLeaf:
		return s.verifyDoc(n.leaf, d)
	case nNot:
		return !s.check(n.children[0], d)
	case nAll:
		for _, c := range s.orders[n] {
			if !s.check(c, d) {
				return false
			}
		}
		return true
	case nAny:
		for _, c := range s.orders[n] {
			if s.check(c, d) {
				return true
			}
		}
		return false
	default:
		return false
	}
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

// docValuesFactor bounds the scope a range is checked over on doc values instead of
// collected from the point index: at most docValuesFactor times the range's matches
// (a fraction, as collecting a match from the points costs about what reading a value
// does, but has to visit every match, in the scope or not). A variable so tests can
// force either path.
var docValuesFactor = defaultDocValuesFactor

const defaultDocValuesFactor = 0.5

// cachedRangeDiscount divides docValuesFactor for a leaf the filter cache will keep:
// its point-index bitmap is collected once and then served from the cache.
const cachedRangeDiscount = 4

// preferDocValues reports whether range leaf lp is cheaper to check over scope's values
// than to collect from the point index: when far fewer documents are in question than
// the range holds (Lucene's IndexOrDocValuesQuery).
func (s *segExec) preferDocValues(lp *leafPlan, scope *roaring.Bitmap) bool {
	limit := docValuesFactor * float64(s.estimateLeaf(lp).card)
	if lp.useCache {
		limit /= cachedRangeDiscount
	}
	return float64(scope.GetCardinality()) <= limit
}

// numBounds is a number range: [lo, hi], each end inclusive per incLo/incHi.
type numBounds struct {
	lo, hi       float64
	incLo, incHi bool
}

// rangeOf returns a range condition's bounds, as candidates computes its range: false
// when the leaf is not a range or holds nothing.
func rangeOf(lp *leafPlan) (numBounds, bool) {
	a := &lp.arg
	switch lp.op {
	case query.OpLt, query.OpLte, query.OpGt, query.OpGte:
		if a.Kind != query.ArgNumber || !a.Scalar.Finite {
			return numBounds{}, false
		}
		x := a.Scalar.Number
		switch lp.op {
		case query.OpLt:
			return numBounds{lo: negInf, hi: x, incLo: true}, true
		case query.OpLte:
			return numBounds{lo: negInf, hi: x, incLo: true, incHi: true}, true
		case query.OpGt:
			return numBounds{lo: x, hi: posInf, incHi: true}, true
		default:
			return numBounds{lo: x, hi: posInf, incLo: true, incHi: true}, true
		}
	case query.OpBetween:
		if a.Kind != query.ArgList || len(a.List) != 2 {
			return numBounds{}, false
		}
		lo, hi := a.List[0], a.List[1]
		if lo.Kind != query.ArgNumber || !lo.Finite || hi.Kind != query.ArgNumber || !hi.Finite || lo.Number > hi.Number {
			return numBounds{}, false
		}
		return numBounds{lo: lo.Number, hi: hi.Number, incLo: true, incHi: true}, true
	}
	return numBounds{}, false
}

// scanChunk is how many ordinals one parallel task of a column scan covers: one
// roaring container. A variable so tests can split small segments.
var scanChunk uint32 = 1 << 16

// numericScan returns the documents of scope whose number lies within b.
func (s *segExec) numericScan(field string, b numBounds, scope *roaring.Bitmap) *roaring.Bitmap {
	nc := s.r.Numbers(field)
	if !nc.Exists() {
		return roaring.New()
	}
	return s.columnScan(scope, func(docs, dst []uint32) []uint32 {
		return nc.Filter(docs, b.lo, b.hi, b.incLo, b.incHi, dst)
	})
}

// columnScan returns the documents of scope that keep retains (keep appends the ones
// of docs it retains to dst), scanning stretches of scanChunk ordinals in parallel on
// the search pool.
func (s *segExec) columnScan(scope *roaring.Bitmap, keep func(docs, dst []uint32) []uint32) *roaring.Bitmap {
	if scope.IsEmpty() {
		return roaring.New()
	}
	first, last := scope.Minimum()/scanChunk, scope.Maximum()/scanChunk
	chunks := int(last-first) + 1
	parts := make([]*roaring.Bitmap, chunks)
	err := runParallel(chunks, func(c int) {
		lo := uint64(first+uint32(c)) * uint64(scanChunk) //nolint:gosec // c < chunks
		part := scope
		if chunks > 1 {
			window := roaring.New()
			window.AddRange(lo, lo+uint64(scanChunk))
			part = roaring.And(scope, window)
		}
		out := roaring.New()
		buf := make([]uint32, 1024)
		kept := make([]uint32, 0, 1024)
		it := part.ManyIterator()
		for {
			n := it.NextMany(buf)
			if n == 0 {
				break
			}
			kept = keep(buf[:n], kept[:0])
			out.AddMany(kept)
		}
		parts[c] = out
	})
	if err != nil {
		s.fail(err)
		return roaring.New()
	}
	return roaring.FastOr(parts...)
}
