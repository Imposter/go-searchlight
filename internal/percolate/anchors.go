// Package percolate is Searchlight's percolator (spec section 7): it answers "which saved
// queries match this document?" by reverse-indexing the saved queries.
//
// Each saved query is reduced to its anchors ([Extract]): a set of atoms, at least one of
// which every document the query matches must hold. A query segment ([Index]) maps each
// atom to the queries anchored on it (a hash dictionary of terms to postings of queries,
// some with a filter on another field, and an interval tree per numeric field), and
// keeps an always-check list of the queries no atom can anchor. Percolating a document
// ([Percolator.Percolate]) turns it into atoms, probes the index for candidates, adds
// the always-check list, and verifies every candidate with its compiled program, which
// decides exactly as the matcher ([query.Compiled.Match]) does, so the answer is exactly
// the brute-force one: anchors and filters only ever prune queries that cannot match.
package percolate

import (
	"encoding/binary"
	"math"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// AtomKind is a kind of document atom: a fact about one field of an analyzed document
// that a query can be anchored on.
type AtomKind uint8

// The atom kinds. Each is read from a [schema.Value] exactly where the matcher reads it,
// so a document holds an atom precisely when the matching part of a condition can hold.
const (
	// AtomValue is a keyword or text field's whole comparable text (Value.Text): eq and
	// in on a string.
	AtomValue AtomKind = iota + 1
	// AtomBool is a bool field's value (Value.Bool), the term "t" or "f": eq and in on a
	// bool.
	AtomBool
	// AtomEntry is one keyword_list entry (Value.Entries): has, has_any, has_all.
	AtomEntry
	// AtomWord is one word of a text field (a token of Value.Words): words_all,
	// words_any.
	AtomWord
	// AtomGram is one 3-rune window of Value.Text, taken from the whole text (never
	// truncated, whatever Value.GramsTruncated says): contains*, starts_with.
	AtomGram
	// AtomSimKey is one pg_trgm trigram key of Value.Text ([analysis.TrigramKeys]), the
	// term its 8 bytes big-endian: similar with a positive minimum.
	AtomSimKey
	// AtomPresent is a present field (Value.Present), the term "": exists.
	AtomPresent
	// AtomText is a field with a comparable text (Value.Text non-nil), the term "": a
	// contains or starts_with needle too short for a gram, similar with no usable
	// minimum.
	AtomText
	// AtomNonempty is a keyword_list field with an entry, the term "": nonempty.
	AtomNonempty
	// AtomMember is an atom that is one half of a pair anchor (see [Anchors].Pairs), its
	// term the atom's kind byte then its term. Its postings are every query with a pair
	// it is half of, and it lists its partners (see the format notes in index.go).
	AtomMember

	numAtomKinds
)

var atomNames = [numAtomKinds]string{
	AtomValue: "value", AtomBool: "bool", AtomEntry: "entry", AtomWord: "word", AtomGram: "gram",
	AtomSimKey: "simkey", AtomPresent: "present", AtomText: "text", AtomNonempty: "nonempty",
	AtomMember: "member",
}

func (k AtomKind) String() string {
	if k > 0 && k < numAtomKinds {
		return atomNames[k]
	}
	return "AtomKind(?)"
}

// The AtomBool terms.
const (
	termTrue  = "t"
	termFalse = "f"
)

// Term is one term atom: Kind on Field, with Term (see [AtomKind] for each kind's term).
type Term struct {
	Kind  AtomKind
	Field string
	Term  string
}

// Range is one numeric atom: a number or date field whose value lies in [Lo, Hi]
// (closed; an open end is an infinity). Ranges come from eq and in on numbers (Lo ==
// Hi), lt, lte, gt, gte and between; a strict bound is kept closed, which only admits
// a candidate the exact check then refuses.
type Range struct {
	Field  string
	Lo, Hi float64
}

// Anchors are what [Extract] makes of a query: either Always (no atom implies it, so it
// is checked against every document) or a set of atoms such that every document the
// query matches holds at least one of them. An empty set (not Always, no term, no
// range) is a query that matches nothing.
type Anchors struct {
	Always bool
	Terms  []Term
	Ranges []Range
	// Pairs are conjunctive anchors: a document holding both atoms of a pair (each of a
	// pairable kind: AtomValue, AtomBool, AtomEntry or AtomWord).
	Pairs [][2]Term
	// Cost estimates the fraction of documents the query is a candidate for, from the
	// term statistics: the sum over its atoms. It decides which child of an all group
	// anchors the group, and nothing else.
	Cost float64
}

// TermStats are document statistics for choosing anchors: the shard's
// ([shard.Generation] implements it).
type TermStats = shard.TermStats

// Extract returns n's anchors (spec section 7), choosing among alternatives with stats
// (nil: fixed priors by atom kind).
//
// The rules, each sound against [query.Compiled.Match]:
//
//   - eq, in: a string is an AtomValue of its normalized text (the _id pseudo-field
//     included), a bool an AtomBool, a finite number a point Range. An in is the union
//     of its values.
//   - lt, lte, gt, gte, between: a Range.
//   - has, has_any: the union of AtomEntry; has_all: its rarest entry.
//   - words_any: the union over phrases of each phrase's anchor, the pair of its two
//     rarest distinct words (one word: that word); words_all: the cheapest phrase's
//     (every phrase must appear, so each of its words).
//   - contains, contains_any, starts_with: per needle, its rarest 3-rune window
//     (AtomGram), or AtomText for a needle of fewer than three runes; the union over
//     needles; contains_all: the rarest needle's.
//   - similar with a minimum above 0: the union of its text's AtomSimKey (a similarity
//     above 0 needs a shared trigram); otherwise AtomText.
//   - exists (true): AtomPresent; nonempty: AtomNonempty.
//   - all: the cheapest anchorable child (every child holds, so its atoms do), or, when
//     cheaper, the pairs of two children made only of value, bool, entry and word atoms
//     (each holds, so the document has an atom of each: Pairs, at most 16 per
//     group); only when no child is anchorable is the group Always. Cost is the
//     estimated fraction of documents holding the anchor (a pair's, the product).
//   - any: the union of its children; one Always child makes the group Always. An any
//     with no child (never true) is the empty set.
//   - not: Always, but not(not x) is x. ne, exists:false, empty, and the root
//     {"all": []} are Always, as is anything this list does not name (an unknown op, a
//     value the op cannot read, a nil node): when in doubt, Always.
func Extract(n query.Node, stats TermStats) Anchors {
	e := newExtractor(stats)
	set := e.node(n)
	if !set.ok {
		return Anchors{Always: true, Cost: 1}
	}
	return Anchors{Terms: set.terms, Ranges: set.ranges, Pairs: set.pairs, Cost: set.cost}
}

// Filter is a cheap condition stored with a query's term postings: a document holding
// the term is a candidate only if its value of Field passes too. It is a conjunct at
// the query's root, so every match passes it. Bool filters on Value.Bool (Lo is 1 for
// true, 0 for false); the others on Value.Number lying in [Lo, Hi].
type Filter struct {
	Field  string
	Bool   bool
	Lo, Hi float64
}

// postingFilter chooses the filter for a query anchored on terms alone: the first
// range on a finite number (as a closed range: lt and gt keep their bound, which only
// admits a candidate the program then refuses), else the first eq on a bool, among
// the leaves at n's root. False when there is none.
func postingFilter(n query.Node, set *aset) (Filter, bool) {
	if !set.ok || len(set.terms) == 0 || len(set.ranges) > 0 || len(set.pairs) > 0 {
		return Filter{}, false
	}
	var flag Filter
	found := false
	for _, l := range rootLeaves(n) {
		a := l.Decoded()
		switch l.Op {
		case query.OpLt, query.OpLte, query.OpGt, query.OpGte, query.OpBetween:
			if lo, hi, ok := leafRange(l, &a); ok {
				return Filter{Field: l.Field, Lo: lo, Hi: hi}, true
			}
		case query.OpEq:
			switch {
			case a.Kind == query.ArgNumber && a.Scalar.Finite:
				return Filter{Field: l.Field, Lo: a.Scalar.Number, Hi: a.Scalar.Number}, true
			case a.Kind == query.ArgBool && !found:
				flag, found = Filter{Field: l.Field, Bool: true, Lo: boolNumber(a.Scalar.Bool), Hi: boolNumber(a.Scalar.Bool)}, true
			}
		}
	}
	return flag, found
}

func boolNumber(b bool) float64 { return pick[float64](b, 1, 0) }

// leafRange is the closed range a range leaf's numbers lie in, false when the leaf
// holds for no number (a bound no float64 holds, or between with lo > hi).
func leafRange(l *query.Leaf, a *query.Arg) (lo, hi float64, ok bool) {
	switch l.Op {
	case query.OpLt, query.OpLte:
		if a.Kind == query.ArgNumber && a.Scalar.Finite {
			return math.Inf(-1), a.Scalar.Number, true
		}
	case query.OpGt, query.OpGte:
		if a.Kind == query.ArgNumber && a.Scalar.Finite {
			return a.Scalar.Number, math.Inf(1), true
		}
	case query.OpBetween:
		if a.Kind == query.ArgList && len(a.List) == 2 && finite(&a.List[0]) && finite(&a.List[1]) && a.List[0].Number <= a.List[1].Number {
			return a.List[0].Number, a.List[1].Number, true
		}
	}
	return 0, 0, false
}

// rootLeaves are the leaves at n's root: n itself, or the leaf children of an all.
func rootLeaves(n query.Node) []*query.Leaf {
	switch x := n.(type) {
	case *query.Leaf:
		if x != nil {
			return []*query.Leaf{x}
		}
	case *query.All:
		if x != nil {
			var out []*query.Leaf
			for _, c := range x.Children {
				if l, ok := c.(*query.Leaf); ok && l != nil {
					out = append(out, l)
				}
			}
			return out
		}
	}
	return nil
}

// fact is one thing a candidate is known to hold: a term atom, or its value of Field
// in [Lo, Hi] (a range anchor or a number filter; a bool filter is an AtomBool term).
type fact struct {
	term   Term
	isTerm bool
	field  string
	lo, hi float64
}

// provenRest returns what is left of n to verify on a candidate, and a key naming the
// leaves left out ("" for none). A query is a candidate only through one of its
// routes: a term it is anchored on (with its filter), or a range it is anchored on.
// A leaf at n's root that every route implies holds on every candidate, so the rest is
// n without it. Pair anchors prove nothing (the pair fallback adds a query on one
// half), and neither do grams, trigram keys or words.
func provenRest(n query.Node, set *aset, filter Filter, filtered bool) (query.Node, string) {
	if !set.ok || len(set.pairs) > 0 || len(set.terms)+len(set.ranges) == 0 {
		return n, ""
	}
	routes := make([][]fact, 0, len(set.terms)+len(set.ranges))
	for _, t := range set.terms {
		r := []fact{{term: t, isTerm: true}}
		if filtered && filter.Bool {
			r = append(r, fact{term: Term{Kind: AtomBool, Field: filter.Field, Term: boolTerm(filter.Lo == 1)}, isTerm: true})
		} else if filtered {
			r = append(r, fact{field: filter.Field, lo: filter.Lo, hi: filter.Hi})
		}
		routes = append(routes, r)
	}
	for _, rg := range set.ranges {
		routes = append(routes, []fact{{field: rg.Field, lo: rg.Lo, hi: rg.Hi}})
	}
	var proven []*query.Leaf
	var keys []string
	for _, l := range rootLeaves(n) {
		every := true
		for _, r := range routes {
			if !slices.ContainsFunc(r, func(f fact) bool { return f.implies(l) }) {
				every = false
				break
			}
		}
		if every {
			proven = append(proven, l)
			keys = append(keys, string(query.Canonical(l)))
		}
	}
	if len(proven) == 0 {
		return n, ""
	}
	slices.Sort(keys)
	key := strings.Join(keys, "\x00")
	all, ok := n.(*query.All)
	if !ok {
		return &query.All{}, key
	}
	rest := make([]query.Node, 0, len(all.Children))
	for _, c := range all.Children {
		if l, ok := c.(*query.Leaf); !ok || !slices.Contains(proven, l) {
			rest = append(rest, c)
		}
	}
	return &query.All{Children: rest}, key
}

// implies reports whether every document holding f satisfies leaf l, as the matcher
// reads l: f is the very atom l's value is compared with, or a range inside l's.
func (f fact) implies(l *query.Leaf) bool {
	a := l.Decoded()
	if !f.isTerm {
		if f.field != l.Field {
			return false
		}
		switch l.Op {
		case query.OpLt:
			return a.Kind == query.ArgNumber && a.Scalar.Finite && f.hi < a.Scalar.Number
		case query.OpLte:
			return a.Kind == query.ArgNumber && a.Scalar.Finite && f.hi <= a.Scalar.Number
		case query.OpGt:
			return a.Kind == query.ArgNumber && a.Scalar.Finite && f.lo > a.Scalar.Number
		case query.OpGte:
			return a.Kind == query.ArgNumber && a.Scalar.Finite && f.lo >= a.Scalar.Number
		case query.OpBetween:
			lo, hi, ok := leafRange(l, &a)
			return ok && lo <= f.lo && f.hi <= hi
		case query.OpEq:
			return a.Kind == query.ArgNumber && a.Scalar.Finite && f.lo == a.Scalar.Number && f.hi == a.Scalar.Number
		}
		return false
	}
	t := f.term
	if t.Field != l.Field {
		return false
	}
	switch {
	case t.Kind == AtomValue && l.Op == query.OpEq:
		return a.Kind == query.ArgString && a.Scalar.Norm == t.Term
	case t.Kind == AtomValue && l.Op == query.OpIn:
		return a.Kind == query.ArgList && slices.ContainsFunc(a.List, func(s query.Scalar) bool {
			return s.Kind == query.ArgString && s.Norm == t.Term
		})
	case t.Kind == AtomBool && l.Op == query.OpEq:
		return a.Kind == query.ArgBool && boolTerm(a.Scalar.Bool) == t.Term
	case t.Kind == AtomBool && l.Op == query.OpIn:
		return a.Kind == query.ArgList && slices.ContainsFunc(a.List, func(s query.Scalar) bool {
			return s.Kind == query.ArgBool && boolTerm(s.Bool) == t.Term
		})
	case t.Kind == AtomEntry && (l.Op == query.OpHas || l.Op == query.OpHasAny):
		return slices.ContainsFunc(texts(&a), func(s query.Scalar) bool { return s.Norm == t.Term })
	case t.Kind == AtomEntry && l.Op == query.OpHasAll:
		list := texts(&a)
		return len(list) > 0 && !slices.ContainsFunc(list, func(s query.Scalar) bool { return s.Norm != t.Term })
	case t.Kind == AtomPresent && l.Op == query.OpExists:
		return a.Kind != query.ArgBool || a.Scalar.Bool
	case t.Kind == AtomNonempty && l.Op == query.OpNonempty:
		return true
	}
	return false
}

// aset is an anchor set under construction; ok false is Always.
type aset struct {
	ok     bool
	terms  []Term
	ranges []Range
	pairs  [][2]Term
	cost   float64
}

var always = aset{}

// Priors are the fraction of documents an atom is assumed to occur in when the term
// statistics cannot say: with no statistics at all, and always for the atoms the
// document segments keep no statistics of.
const (
	priorValue    = 0.01
	priorBool     = 0.5
	priorEntry    = 0.02
	priorWord     = 0.03
	priorGram     = 0.05
	priorSimKey   = 0.05
	priorPresent  = 0.9
	priorText     = 0.9
	priorNonempty = 0.8
	priorPoint    = 0.01
	priorBetween  = 0.2
	priorHalf     = 0.4
	// tiebreak scales a prior into a tie-breaker added to a measured frequency, so that
	// between two terms equally rare in the documents, the kind more selective in
	// general wins.
	tiebreak = 1e-3
)

// extractor extracts anchors under one set of term statistics, caching term costs: a
// query segment's queries share many terms (a store, a category).
type extractor struct {
	stats TermStats
	docs  float64 // live documents; 0: no statistics
	costs map[string]float64
	key   []byte
}

func newExtractor(stats TermStats) *extractor {
	e := &extractor{stats: stats}
	if stats != nil {
		e.docs = float64(stats.NumDocs())
	}
	if e.docs > 0 {
		e.costs = make(map[string]float64)
	}
	return e
}

func (e *extractor) node(n query.Node) aset {
	switch x := n.(type) {
	case *query.All:
		if x == nil || len(x.Children) == 0 {
			return always // {"all": []} holds for every document
		}
		best := always
		var pairable []aset
		for _, child := range x.Children {
			c := e.node(child)
			if c.ok && (!best.ok || c.cost < best.cost) {
				best = c
			}
			if c.ok && isPairable(&c) {
				pairable = append(pairable, c)
			}
		}
		// Two children hold together, so a document holds an atom of each: a pair of
		// them anchors the group, as selective as both (assumed independent).
		for i := range pairable {
			for j := i + 1; j < len(pairable); j++ {
				a, b := &pairable[i], &pairable[j]
				if len(a.terms)*len(b.terms) > maxPairKeys {
					continue
				}
				if cost := a.cost * b.cost; !best.ok || cost < best.cost {
					best = pairSet(a, b, cost)
				}
			}
		}
		return best
	case *query.Any:
		if x == nil {
			return always
		}
		out := aset{ok: true}
		for _, child := range x.Children {
			c := e.node(child)
			if !c.ok {
				return always
			}
			out = union(out, c)
		}
		return out
	case *query.Not:
		if x == nil {
			return always
		}
		if inner, ok := x.Child.(*query.Not); ok && inner != nil {
			return e.node(inner.Child) // not(not x) holds exactly when x does
		}
		return always
	case *query.Leaf:
		if x == nil {
			return always
		}
		return e.leaf(x)
	default:
		return always
	}
}

// maxPairKeys bounds the pairs one pair of children anchors on (the product of their
// term counts).
const maxPairKeys = 16

// isPairable reports whether s is terms alone, each of a kind a document has few
// of per field (no grams, trigram keys, field-level atoms, ranges or pairs).
func isPairable(s *aset) bool {
	if len(s.ranges) > 0 || len(s.pairs) > 0 || len(s.terms) == 0 {
		return false
	}
	for _, t := range s.terms {
		if !pairableKind(t.Kind) {
			return false
		}
	}
	return true
}

func pairableKind(k AtomKind) bool {
	switch k {
	case AtomValue, AtomBool, AtomEntry, AtomWord:
		return true
	default:
		return false
	}
}

// pairSet is every pair of a term of a and a term of b; a pair of one atom with itself
// is that atom alone.
func pairSet(a, b *aset, cost float64) aset {
	out := aset{ok: true, cost: cost}
	for _, x := range a.terms {
		for _, y := range b.terms {
			if x == y {
				out.terms = append(out.terms, x)
			} else {
				out.pairs = append(out.pairs, [2]Term{x, y})
			}
		}
	}
	return out
}

func union(a, b aset) aset {
	a.terms = append(a.terms, b.terms...)
	a.ranges = append(a.ranges, b.ranges...)
	a.pairs = append(a.pairs, b.pairs...)
	a.cost += b.cost
	return a
}

// cheapest returns the cheapest of sets (each ok), or always when there is none.
func cheapest(sets []aset) aset {
	best := always
	for _, s := range sets {
		if !best.ok || s.cost < best.cost {
			best = s
		}
	}
	return best
}

func (e *extractor) leaf(l *query.Leaf) aset {
	a := l.Decoded()
	f := l.Field
	switch l.Op {
	case query.OpEq:
		return e.eq(f, &a)
	case query.OpIn:
		return e.in(f, &a)
	case query.OpLt, query.OpLte:
		if a.Kind != query.ArgNumber || !a.Scalar.Finite {
			return always
		}
		return e.rangeSet(f, math.Inf(-1), a.Scalar.Number)
	case query.OpGt, query.OpGte:
		if a.Kind != query.ArgNumber || !a.Scalar.Finite {
			return always
		}
		return e.rangeSet(f, a.Scalar.Number, math.Inf(1))
	case query.OpBetween:
		if a.Kind != query.ArgList || len(a.List) != 2 || !finite(&a.List[0]) || !finite(&a.List[1]) {
			return always
		}
		lo, hi := a.List[0].Number, a.List[1].Number
		if lo > hi {
			return always // never holds; Always is merely wasteful
		}
		return e.rangeSet(f, lo, hi)
	case query.OpExists:
		if a.Kind != query.ArgBool || a.Scalar.Bool { // the matcher's `value is not False`
			return e.term(AtomPresent, f, "")
		}
		return always
	case query.OpNonempty:
		return e.term(AtomNonempty, f, "")
	case query.OpHas, query.OpHasAny, query.OpHasAll:
		texts := texts(&a)
		if len(texts) == 0 {
			return always
		}
		sets := make([]aset, len(texts))
		for i, t := range texts {
			sets[i] = e.term(AtomEntry, f, t.Norm)
		}
		return e.combine(sets, l.Op == query.OpHasAll)
	case query.OpContains, query.OpContainsAny, query.OpContainsAll:
		texts := texts(&a)
		if len(texts) == 0 {
			return always
		}
		sets := make([]aset, len(texts))
		for i, t := range texts {
			sets[i] = e.needle(f, t.Norm)
		}
		return e.combine(sets, l.Op == query.OpContainsAll)
	case query.OpStartsWith:
		if a.Kind != query.ArgString {
			return always
		}
		return e.needle(f, a.Scalar.Norm)
	case query.OpWordsAll, query.OpWordsAny:
		return e.words(f, &a, l.Op == query.OpWordsAll)
	case query.OpSimilar:
		return e.similar(f, &a)
	default: // ne, empty, an unknown op
		return always
	}
}

// combine is every set's union (any wanted), or the cheapest one (every one wanted).
func (e *extractor) combine(sets []aset, every bool) aset {
	if every {
		return cheapest(sets)
	}
	out := aset{ok: true}
	for _, s := range sets {
		out = union(out, s)
	}
	return out
}

func finite(s *query.Scalar) bool { return s.Kind == query.ArgNumber && s.Finite }

// texts returns a scalar's or list's strings, as the matcher reads them.
func texts(a *query.Arg) []query.Scalar {
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

func (e *extractor) eq(f string, a *query.Arg) aset {
	switch a.Kind {
	case query.ArgString:
		return e.term(AtomValue, f, a.Scalar.Norm)
	case query.ArgBool:
		return e.term(AtomBool, f, boolTerm(a.Scalar.Bool))
	case query.ArgNumber:
		if !a.Scalar.Finite {
			return always
		}
		return e.rangeSet(f, a.Scalar.Number, a.Scalar.Number)
	default:
		return always
	}
}

func (e *extractor) in(f string, a *query.Arg) aset {
	if a.Kind != query.ArgList {
		return always
	}
	out := aset{ok: true}
	for i := range a.List {
		s := &a.List[i]
		switch s.Kind {
		case query.ArgString:
			out = union(out, e.term(AtomValue, f, s.Norm))
		case query.ArgBool:
			out = union(out, e.term(AtomBool, f, boolTerm(s.Bool)))
		case query.ArgNumber:
			if s.Finite { // the matcher drops a number no float64 holds: it equals nothing
				out = union(out, e.rangeSet(f, s.Number, s.Number))
			}
		}
	}
	return out // with no value the matcher can read, in never holds: the empty set
}

func boolTerm(b bool) string {
	if b {
		return termTrue
	}
	return termFalse
}

// needle anchors a contains or starts_with needle: any text holding it holds each of
// its 3-rune windows, so its rarest one; a shorter needle (or one that is not valid
// UTF-8, whose windows might not align with the text's) needs only a text.
func (e *extractor) needle(f, norm string) aset {
	if !utf8.ValidString(norm) || utf8.RuneCountInString(norm) < 3 {
		return e.term(AtomText, f, "")
	}
	best := always
	eachWindow(norm, func(gram string) {
		if s := e.term(AtomGram, f, gram); !best.ok || s.cost < best.cost {
			best = s
		}
	})
	return best
}

// eachWindow calls fn with every 3-rune window of s, in order (repeats included), as
// [analysis.Substrings3] takes them.
func eachWindow(s string, fn func(string)) {
	a, b, c := -1, -1, -1 // the starts of the three latest runes
	for i := range s {
		if a >= 0 {
			fn(s[a:i])
		}
		a, b, c = b, c, i
	}
	if a >= 0 {
		fn(s[a:])
	}
}

// eachWord calls fn with every word of a Value.Words or analysis.Words string (" a b ").
func eachWord(words string, fn func(string)) {
	for words != "" {
		i := strings.IndexByte(words, ' ')
		if i < 0 {
			fn(words)
			return
		}
		if i > 0 {
			fn(words[:i])
		}
		words = words[i+1:]
	}
}

// words anchors words_all and words_any. A phrase's words string (" a b ") is a
// substring of a document's only if each of its words is one of the document's words,
// so a phrase anchors on the pair of its two rarest distinct words, or on its word.
func (e *extractor) words(f string, a *query.Arg, every bool) aset {
	var sets []aset
	for _, t := range texts(a) {
		w := analysis.Words(t.Text) // the matcher takes words from the text as written
		if w == analysis.NoWords {
			if every {
				return always // never holds; Always is merely wasteful
			}
			continue // a phrase with no word matches nothing
		}
		var best, second aset
		eachWord(w, func(word string) {
			switch s := e.term(AtomWord, f, word); {
			case best.ok && s.terms[0] == best.terms[0], second.ok && s.terms[0] == second.terms[0]:
			case !best.ok || s.cost < best.cost:
				best, second = s, best
			case !second.ok || s.cost < second.cost:
				second = s
			}
		})
		if !best.ok {
			return always
		}
		if second.ok {
			best = pairSet(&best, &second, best.cost*second.cost)
		}
		sets = append(sets, best)
	}
	if len(sets) == 0 {
		return always
	}
	return e.combine(sets, every)
}

// similar anchors similar. With a minimum above 0, a match shares at least one trigram
// with the text (no shared trigram is a similarity of 0); otherwise (a minimum the
// matcher refuses, so the condition never holds) a text is all it can need.
func (e *extractor) similar(f string, a *query.Arg) aset {
	if a.Kind == query.ArgObject {
		text, minimum := a.Object["text"], a.Object["min"]
		if text.Kind == query.ArgString && minimum.Kind == query.ArgNumber && minimum.Finite && minimum.Number > 0 {
			if keys := analysis.TrigramKeys(text.Norm); len(keys) > 0 {
				out := aset{ok: true}
				for _, k := range keys {
					out = union(out, e.term(AtomSimKey, f, simKeyTerm(k)))
				}
				return out
			}
		}
	}
	return e.term(AtomText, f, "")
}

func simKeyTerm(k uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], k)
	return string(b[:])
}

func (e *extractor) term(kind AtomKind, field, term string) aset {
	return aset{ok: true, terms: []Term{{Kind: kind, Field: field, Term: term}}, cost: e.termCost(kind, field, term)}
}

func (e *extractor) rangeSet(field string, lo, hi float64) aset {
	cost := priorBetween
	switch {
	case lo == hi:
		cost = priorPoint
	case math.IsInf(lo, 0) || math.IsInf(hi, 0):
		cost = priorHalf
	}
	return aset{ok: true, ranges: []Range{{Field: field, Lo: lo, Hi: hi}}, cost: cost}
}

// termCost estimates the fraction of documents holding the atom: the shard's document
// frequency of the matching segment term over its live documents, plus the kind's
// prior as a tie-breaker; or the prior alone without statistics or for an atom the
// segments keep none of. Stale statistics (documents written or deleted since) only
// make the choice less selective than it could be: every choice is sound.
func (e *extractor) termCost(kind AtomKind, field, term string) float64 {
	prior := atomPrior(kind, term)
	segKind, segTerm, measured := segmentTerm(kind, term)
	if e.docs == 0 || !measured {
		return prior
	}
	e.key = append(e.key[:0], byte(kind))
	e.key = append(e.key, field...)
	e.key = append(e.key, 0)
	e.key = append(e.key, term...)
	if c, ok := e.costs[string(e.key)]; ok {
		return c
	}
	freq := float64(e.stats.DocFreq(field, segKind, segTerm)) / e.docs
	c := min(freq, 1) + prior*tiebreak
	e.costs[string(e.key)] = c
	return c
}

// segmentTerm is the document segment term an atom is counted under, false for an atom
// the segments keep no statistics of.
func segmentTerm(kind AtomKind, term string) (segment.TermKind, string, bool) {
	switch kind {
	case AtomValue:
		return segment.KindValue, term, true
	case AtomBool:
		if term == termTrue {
			return segment.KindValue, segment.TermTrue, true
		}
		return segment.KindValue, segment.TermFalse, true
	case AtomEntry:
		return segment.KindEntry, term, true
	case AtomWord:
		return segment.KindWord, term, true
	case AtomGram:
		return segment.KindGram, term, true
	default:
		return 0, "", false
	}
}

func atomPrior(kind AtomKind, term string) float64 {
	switch kind {
	case AtomValue:
		return priorValue
	case AtomBool:
		return priorBool
	case AtomEntry:
		return priorEntry
	case AtomWord:
		return priorWord
	case AtomGram:
		return gramPrior(term)
	case AtomSimKey:
		return priorSimKey
	case AtomPresent:
		return priorPresent
	case AtomText:
		return priorText
	default:
		return priorNonempty
	}
}

// gramPrior guesses a gram's frequency with no statistics: one spanning a space is
// common (word boundaries), one holding a digit is rare (model numbers).
func gramPrior(gram string) float64 {
	p := priorGram
	if strings.IndexByte(gram, ' ') >= 0 {
		p *= 2
	}
	if strings.ContainsAny(gram, "0123456789") {
		p /= 2
	}
	return p
}
