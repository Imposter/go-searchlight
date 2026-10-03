package search

import (
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/segment"
)

// cands are a leaf's candidates in one segment, over all of its documents (deleted
// ones included; the scope removes them): sure ones match, maybe ones need the residual
// check. Both are read-only (often views of the segment's mapping); maybe nil is none.
type cands struct {
	sure  *roaring.Bitmap
	maybe *roaring.Bitmap
}

// maxPrefixTerms is how many terms a starts_with unions before it scans the keyword
// column instead.
const maxPrefixTerms = 4096

// candidates asks the segment's index for lp's candidates. Every case mirrors the
// matcher ([query.Compiled]): which typed part of a value an op reads, and what a
// missing or mistyped value does. A field's type is read from the segment's own
// structures: a keyword column means text (its KindValue terms are texts), a number
// column numbers, an entry dictionary list entries, and KindValue terms without a
// keyword column a bool's true and false.
func (s *segExec) candidates(lp *leafPlan) cands {
	r, f, a := s.r, lp.field, &lp.arg
	empty := cands{sure: roaring.New()}
	switch lp.op {
	case query.OpExists:
		if a.Kind != query.ArgBool || a.Scalar.Bool {
			return cands{sure: r.Present(f)}
		}
		return cands{sure: s.flip(r.Present(f))}
	case query.OpNonempty:
		return cands{sure: s.nonempty(f)}
	case query.OpEmpty:
		return cands{sure: s.flip(s.nonempty(f))}
	case query.OpEq:
		return cands{sure: s.equal(f, a)}
	case query.OpNe:
		return cands{sure: s.flip(s.equal(f, a))}
	case query.OpIn:
		if a.Kind != query.ArgList {
			return empty
		}
		text := s.isText(f)
		var parts []*roaring.Bitmap
		for i := range a.List {
			sc := &a.List[i]
			switch sc.Kind {
			case query.ArgString:
				if text {
					parts = append(parts, r.Postings(f, kindValue, sc.Norm))
				}
			case query.ArgBool:
				if !text {
					parts = append(parts, r.Postings(f, kindValue, boolTerm(sc.Bool)))
				}
			case query.ArgNumber:
				if sc.Finite {
					parts = append(parts, r.Numbers(f).Range(sc.Number, sc.Number, true, true))
				}
			}
		}
		return cands{sure: union(parts)}
	case query.OpLt, query.OpLte, query.OpGt, query.OpGte:
		if a.Kind != query.ArgNumber || !a.Scalar.Finite {
			return empty
		}
		x, nc := a.Scalar.Number, r.Numbers(f)
		switch lp.op {
		case query.OpLt:
			return cands{sure: nc.Range(negInf, x, true, false)}
		case query.OpLte:
			return cands{sure: nc.Range(negInf, x, true, true)}
		case query.OpGt:
			return cands{sure: nc.Range(x, posInf, false, true)}
		default:
			return cands{sure: nc.Range(x, posInf, true, true)}
		}
	case query.OpBetween:
		if a.Kind != query.ArgList || len(a.List) != 2 {
			return empty
		}
		lo, hi := a.List[0], a.List[1]
		if lo.Kind != query.ArgNumber || !lo.Finite || hi.Kind != query.ArgNumber || !hi.Finite || lo.Number > hi.Number {
			return empty
		}
		return cands{sure: r.Numbers(f).Range(lo.Number, hi.Number, true, true)}
	case query.OpHas, query.OpHasAny, query.OpHasAll:
		if len(lp.texts) == 0 {
			return empty
		}
		if lp.every {
			return cands{sure: s.intersectTerms(f, kindEntry, lp.texts)}
		}
		parts := make([]*roaring.Bitmap, len(lp.texts))
		for i, t := range lp.texts {
			parts[i] = r.Postings(f, kindEntry, t)
		}
		return cands{sure: union(parts)}
	case query.OpContains, query.OpContainsAny, query.OpContainsAll:
		if len(lp.texts) == 0 || !s.isText(f) {
			return empty
		}
		out := make([]cands, len(lp.texts))
		for i, t := range lp.texts {
			out[i] = s.needle(f, t)
		}
		return combine(out, lp.every)
	case query.OpStartsWith:
		if len(lp.texts) != 1 || !s.isText(f) {
			return empty
		}
		return s.prefix(f, lp.texts[0])
	case query.OpWordsAll, query.OpWordsAny:
		if len(lp.phrases) == 0 {
			return empty
		}
		out := make([]cands, len(lp.phrases))
		for i, words := range lp.phrases {
			if len(words) == 1 {
				// One word: its postings are exact, as Words pads every word with spaces.
				out[i] = cands{sure: r.Postings(f, kindWord, words[0])}
				continue
			}
			// A phrase: its words' documents, then a check that they are consecutive.
			out[i] = cands{sure: roaring.New(), maybe: s.intersectTerms(f, kindWord, words)}
		}
		return combine(out, lp.every)
	case query.OpSimilar:
		if !s.isText(f) {
			return empty
		}
		return cands{sure: roaring.New(), maybe: s.similarCandidates(lp)}
	default:
		return empty
	}
}

// equal is eq's matches: the value's documents, by the value's kind.
func (s *segExec) equal(f string, a *query.Arg) *roaring.Bitmap {
	switch a.Kind {
	case query.ArgString:
		if s.isText(f) {
			return s.r.Postings(f, kindValue, a.Scalar.Norm)
		}
	case query.ArgBool:
		if !s.isText(f) {
			return s.r.Postings(f, kindValue, boolTerm(a.Scalar.Bool))
		}
	case query.ArgNumber:
		if a.Scalar.Finite {
			x := a.Scalar.Number
			return s.r.Numbers(f).Range(x, x, true, true)
		}
	}
	return roaring.New()
}

// nonempty is the documents with at least one list entry.
func (s *segExec) nonempty(f string) *roaring.Bitmap {
	out := roaring.New()
	mc := s.r.Entries(f)
	if !mc.Exists() {
		return out
	}
	it := s.r.Present(f).ManyIterator()
	buf := make([]uint32, 256)
	keep := make([]uint32, 0, 256)
	for {
		n := it.NextMany(buf)
		if n == 0 {
			return out
		}
		keep = keep[:0]
		for _, d := range buf[:n] {
			if mc.Count(d) > 0 {
				keep = append(keep, d)
			}
		}
		out.AddMany(keep)
	}
}

// needle is one contains needle's candidates: the documents holding all of its grams
// (sure when the needle is one gram), plus every document whose value was too long to
// have grams; a needle shorter than a gram (or on _id, which has no grams) checks
// every document with the field.
func (s *segExec) needle(f, t string) cands {
	runes := utf8.RuneCountInString(t)
	if f == IDField || runes < 3 {
		return cands{sure: roaring.New(), maybe: s.r.Present(f)}
	}
	grams := s.intersectTerms(f, kindGram, analysis.Substrings3(t))
	trunc := s.r.Truncated(f)
	if runes == 3 {
		return cands{sure: grams, maybe: trunc}
	}
	return cands{sure: roaring.New(), maybe: roaring.Or(grams, trunc)}
}

// prefix is starts_with's matches, exact: the values with the prefix are one run of
// the keyword column's sorted dictionary, [lo, hi), found by binary search. A short
// run unions its terms' postings; a long one (a one-letter prefix over unique values)
// scans the column's ordinals, which reads no term at all.
func (s *segExec) prefix(f, p string) cands {
	kc := s.r.Keywords(f)
	n := int(kc.NumTerms())
	lo := sortSearch(n, func(o int) bool { return kc.Term(uint32(o)) >= p })                              //nolint:gosec // o < n
	hi := lo + sortSearch(n-lo, func(i int) bool { return !strings.HasPrefix(kc.Term(uint32(lo+i)), p) }) //nolint:gosec // below n
	switch {
	case hi == lo:
		return cands{sure: roaring.New()}
	case hi-lo <= maxPrefixTerms:
		parts := make([]*roaring.Bitmap, 0, hi-lo)
		kc.EachTerm(uint32(lo), func(o uint32, term []byte) bool { //nolint:gosec // lo < n
			if int(o) >= hi {
				return false
			}
			parts = append(parts, s.r.Postings(f, kindValue, string(term)))
			return true
		})
		return cands{sure: union(parts)}
	default:
		from, to := uint32(lo), uint32(hi) //nolint:gosec // both at most n
		return cands{sure: s.columnScan(s.r.Present(f), func(d uint32) bool {
			o, ok := kc.Ord(d)
			return ok && o >= from && o < to
		})}
	}
}

// intersectTerms is the documents holding every term, intersected rarest first.
func (s *segExec) intersectTerms(f string, kind segment.TermKind, terms []string) *roaring.Bitmap {
	if len(terms) == 0 {
		return roaring.New()
	}
	type tf struct {
		term string
		freq uint32
	}
	byFreq := make([]tf, len(terms))
	for i, t := range terms {
		byFreq[i] = tf{t, s.r.TermFreq(f, kind, t)}
		if byFreq[i].freq == 0 {
			return roaring.New()
		}
	}
	slices.SortFunc(byFreq, func(a, b tf) int { return int(a.freq) - int(b.freq) })
	out := s.r.Postings(f, kind, byFreq[0].term)
	if len(byFreq) == 1 {
		return out
	}
	out = roaring.And(out, s.r.Postings(f, kind, byFreq[1].term))
	for _, t := range byFreq[2:] {
		if out.IsEmpty() {
			break
		}
		out.And(s.r.Postings(f, kind, t.term))
	}
	return out
}

// combine joins per-needle candidates: any is the union of each part; all is the
// intersection of the sure parts, maybe wherever every part is at least maybe.
func combine(parts []cands, every bool) cands {
	if len(parts) == 1 {
		return parts[0]
	}
	if !every {
		sure := make([]*roaring.Bitmap, 0, len(parts))
		maybe := make([]*roaring.Bitmap, 0, len(parts))
		for _, p := range parts {
			sure = append(sure, p.sure)
			if p.maybe != nil {
				maybe = append(maybe, p.maybe)
			}
		}
		return cands{sure: union(sure), maybe: union(maybe)}
	}
	sure := parts[0].sure
	possible := orMaybe(parts[0])
	for _, p := range parts[1:] {
		sure = roaring.And(sure, p.sure)
		possible = roaring.And(possible, orMaybe(p))
	}
	return cands{sure: sure, maybe: possible}
}

func orMaybe(c cands) *roaring.Bitmap {
	if c.maybe == nil {
		return c.sure
	}
	return roaring.Or(c.sure, c.maybe)
}

// union is the union of read-only bitmaps, as a new one.
func union(parts []*roaring.Bitmap) *roaring.Bitmap {
	switch len(parts) {
	case 0:
		return roaring.New()
	case 1:
		return parts[0]
	default:
		return roaring.FastOr(parts...)
	}
}

// flip is the segment's documents not in bm, as a new bitmap.
func (s *segExec) flip(bm *roaring.Bitmap) *roaring.Bitmap {
	return roaring.AndNot(s.allDocs, bm)
}
