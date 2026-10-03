package search

import (
	"math"
	"sync"
	"unicode/utf8"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// The residual check.
//
// A maybe candidate is checked with the leaf's own compiled matcher, so the residual
// can never disagree with query.Match. The matcher reads a field's analyzed value,
// which the residual rebuilds from what the segment keeps:
//
//   - a text value (contains, starts_with, similar) from the keyword column, whose
//     terms are exactly the analyzed Text; a check that only reads Text depends on
//     the value alone, so it runs once per distinct value (ordinal), and when the
//     candidates are many it walks the column's dictionary in order instead of
//     decoding values one at a time;
//   - a text field's words (a multi-word phrase) from the stored body, re-analyzed
//     with the generation's mapping (schema.Analyze), since words are not kept as doc
//     values;
//   - anything else (never needed by today's plans, kept so a fallback is always
//     exact) from every column: number, list entries, a bool from its postings.

// checkEvery is how many documents a loop handles between context checks.
const checkEvery = 1024

// verify adds to out the documents of cand that lp matches.
func (s *segExec) verify(lp *leafPlan, cand, out *roaring.Bitmap) {
	if cand.IsEmpty() || s.err != nil {
		return
	}
	s.scanned += card(cand)
	switch {
	case lp.needsBody:
		s.verifyBodies(lp, cand, out)
	case s.r.Keywords(lp.field).Exists():
		s.verifyTexts(lp, cand, out)
	default:
		s.verifyValues(lp, cand, out)
	}
}

// verifyBodies re-analyzes each candidate's stored body, in parallel chunks.
func (s *segExec) verifyBodies(lp *leafPlan, cand, out *roaring.Bitmap) {
	ords := cand.ToArray()
	keep := make([]bool, len(ords))
	errs := s.parallelChunks(len(ords), func(lo, hi int) error {
		for k := lo; k < hi; k++ {
			ord := ords[k]
			id, err := s.r.ID(ord)
			if err != nil {
				return err
			}
			body, err := s.r.Stored(ord)
			if err != nil {
				return err
			}
			doc, _, err := schema.Analyze(s.mapping, id, body)
			if err != nil {
				// A body the mapping now refuses (it only ever grows, so this is
				// damage): it matches nothing it cannot be read for.
				s.warnResidual(id, err)
				continue
			}
			keep[k] = lp.match.Match(&doc)
		}
		return nil
	})
	if errs != nil {
		s.fail(errs)
		return
	}
	if s.checkCtx() {
		return
	}
	for k, ord := range ords {
		if keep[k] {
			out.Add(ord)
		}
	}
}

// verifyChunk is how many candidates one parallel task checks.
const verifyChunk = 512

// parallelChunks runs fn over [0, n) in chunks on the search pool, stopping early
// when the context ends (the caller then sees it through checkCtx). It returns the
// first error fn returned.
func (s *segExec) parallelChunks(n int, fn func(lo, hi int) error) error {
	chunks := (n + verifyChunk - 1) / verifyChunk
	errs := make([]error, chunks)
	runParallel(chunks, func(c int) {
		if s.ctx.Err() != nil {
			return
		}
		errs[c] = fn(c*verifyChunk, min(n, (c+1)*verifyChunk))
	})
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// verifyTexts checks candidates by their keyword column value, once per distinct
// value, in parallel chunks of the candidates' sorted ordinals: a dense chunk walks its
// stretch of the dictionary once, a sparse one looks each ordinal up; neither
// allocates per value.
func (s *segExec) verifyTexts(lp *leafPlan, cand, out *roaring.Bitmap) {
	kc := s.r.Keywords(lp.field)
	present := s.r.Present(lp.field)
	docs := cand.ToArray()
	ords := make([]uint32, len(docs))
	const none = math.MaxUint32
	wanted := roaring.New()
	for i, d := range docs {
		if o, ok := kc.Ord(d); ok {
			ords[i] = o
			wanted.Add(o)
		} else {
			ords[i] = none
		}
	}
	list := wanted.ToArray()
	hit := make([]bool, len(list))
	_ = s.parallelChunks(len(list), func(lo, hi int) error {
		var text string
		doc := schema.Doc{Fields: map[string]schema.Value{lp.field: {Present: true, Text: &text}}}
		check := func(k int, term []byte) {
			// The matcher only reads the text during the call.
			text = unsafe.String(unsafe.SliceData(term), len(term))
			hit[k] = lp.match.Match(&doc)
		}
		if span := int(list[hi-1] - list[lo]); span <= 8*(hi-lo) {
			k := lo
			kc.EachTerm(list[lo], func(o uint32, term []byte) bool {
				if o == list[k] {
					check(k, term)
					k++
				}
				return k < hi
			})
			return nil
		}
		for k := lo; k < hi; k++ {
			kc.EachTerm(list[k], func(_ uint32, term []byte) bool {
				check(k, term)
				return false
			})
		}
		return nil
	})
	if s.checkCtx() {
		return
	}
	matched := roaring.New()
	for k, o := range list {
		if hit[k] {
			matched.Add(o)
		}
	}
	// Documents with no text: the field missing, or present with another type.
	var noText [2]int8 // by presence: 0 unknown, 1 no match, 2 match
	matchNoText := func(p bool) bool {
		i := 0
		if p {
			i = 1
		}
		if noText[i] == 0 {
			doc := schema.Doc{Fields: map[string]schema.Value{lp.field: {Present: p}}}
			noText[i] = 1
			if lp.match.Match(&doc) {
				noText[i] = 2
			}
		}
		return noText[i] == 2
	}
	keep := make([]uint32, 0, len(docs))
	for i, d := range docs {
		if o := ords[i]; o != none {
			if matched.Contains(o) {
				keep = append(keep, d)
			}
		} else if matchNoText(present.Contains(d)) {
			keep = append(keep, d)
		}
	}
	out.AddMany(keep)
}

// verifyValues rebuilds each candidate's value from every column.
func (s *segExec) verifyValues(lp *leafPlan, cand, out *roaring.Bitmap) {
	f := lp.field
	present := s.r.Present(f)
	nc, mc := s.r.Numbers(f), s.r.Entries(f)
	var isTrue, isFalse *roaring.Bitmap
	if !s.isText(f) {
		isTrue, isFalse = s.r.Postings(f, kindValue, "true"), s.r.Postings(f, kindValue, "false")
	}
	doc := schema.Doc{Fields: map[string]schema.Value{}}
	var ordBuf []uint32
	it := cand.Iterator()
	for i := 0; it.HasNext(); i++ {
		if i%checkEvery == 0 && s.checkCtx() {
			return
		}
		d := it.Next()
		v := schema.Value{Present: present.Contains(d)}
		if x, ok := nc.Value(d); ok {
			v.Number = &x
		}
		if mc.Exists() {
			ordBuf = mc.Ords(d, ordBuf[:0])
			if len(ordBuf) > 0 {
				v.Entries = make([]string, len(ordBuf))
				for j, o := range ordBuf {
					v.Entries[j] = mc.Term(o)
				}
			}
		}
		if isTrue != nil {
			switch {
			case isTrue.Contains(d):
				b := true
				v.Bool = &b
			case isFalse.Contains(d):
				b := false
				v.Bool = &b
			}
		}
		doc.Fields[f] = v
		if lp.match.Match(&doc) {
			out.Add(d)
		}
	}
}

// similarCandidates prefilters similar by trigrams. A document matches only if it
// shares at least ceil(min*|Q|) of the query text's trigrams Q (similarity is
// shared/(|Q|+|D|-shared), and |D| >= shared). If that is more than Q's trigrams the
// gram index cannot see (padded word edges, and runes whose lowercase is not one rune),
// every match shares one of Q's interior trigrams, three word characters in a row,
// which is a 3-rune substring of the document's text: the union of those grams'
// postings (each spelled in every way casefolded text can spell it), plus the
// documents too long to have grams, holds every match. Otherwise every document with
// the field is a candidate.
func (s *segExec) similarCandidates(lp *leafPlan) *roaring.Bitmap {
	f := lp.field
	all := s.r.Present(f)
	grams, ok := similarGrams(&lp.arg)
	if !ok || f == IDField {
		return all
	}
	parts := make([]*roaring.Bitmap, 0, len(grams)+1)
	for _, g := range grams {
		parts = append(parts, s.r.Postings(f, kindGram, g))
	}
	parts = append(parts, s.r.Truncated(f))
	return union(parts)
}

// similarGrams returns the grams similar's prefilter unions, false when it cannot
// prefilter (and every document is a candidate).
func similarGrams(a *query.Arg) ([]string, bool) {
	if a.Kind != query.ArgObject {
		return nil, false
	}
	text, okText := a.Object["text"]
	m, okMin := a.Object["min"]
	if !okText || text.Kind != query.ArgString || !okMin || m.Kind != query.ArgNumber || !m.Finite || m.Number <= 0 {
		return nil, false
	}
	keys := analysis.TrigramKeys(text.Norm)
	if len(keys) == 0 {
		return nil, false
	}
	tab := lowerTable()
	var interior []uint64
	for _, k := range keys {
		a, b, c := unpackTrigram(k)
		if a == ' ' || b == ' ' || c == ' ' || tab.unsafe[a] || tab.unsafe[b] || tab.unsafe[c] {
			continue
		}
		interior = append(interior, k)
	}
	// Similarity is rounded to a float32, so a ratio a hair under min may still pass:
	// require a little less than min.
	lower := m.Number * (1 - 1e-6)
	needed := math.Ceil(lower*float64(len(keys)) - 1e-9)
	if needed <= float64(len(keys)-len(interior)) {
		return nil, false
	}
	var grams []string
	for _, k := range interior {
		a, b, c := unpackTrigram(k)
		for _, x := range tab.spellings(a) {
			for _, y := range tab.spellings(b) {
				for _, z := range tab.spellings(c) {
					grams = append(grams, string([]rune{x, y, z}))
				}
			}
		}
	}
	return grams, true
}

const runeBits = 21

func unpackTrigram(k uint64) (rune, rune, rune) {
	const mask = 1<<runeBits - 1
	return rune(k >> (2 * runeBits)), rune(k >> runeBits & mask), rune(k & mask)
}

// lowerTab maps a rune of lowercased text back to the runes casefolded text may hold
// in its place: similar's trigrams are taken from the text lowercased (str.lower),
// while the gram index holds the casefolded text's own runes, and casefolding leaves a
// few runes that lowercasing still changes (Cherokee capitals, say).
type lowerTab struct {
	pre    map[rune][]rune // lowered rune -> the folded runes that lower to it, besides itself
	unsafe map[rune]bool   // runes a folded rune lowers to as part of several
}

func (t *lowerTab) spellings(r rune) []rune {
	return append([]rune{r}, t.pre[r]...)
}

// lowerTableMax is the last code point lowerTable looks at: no character past the
// supplementary multilingual plane has a case (TestLowerTableCoversEveryCase checks).
const lowerTableMax = 0x1FFFF

var lowerTable = sync.OnceValue(func() *lowerTab {
	t := &lowerTab{pre: map[rune][]rune{}, unsafe: map[rune]bool{}}
	seen := map[rune]bool{}
	var buf [utf8.UTFMax]byte
	for r := rune(0); r <= lowerTableMax; r++ {
		if r >= 0xD800 && r <= 0xDFFF {
			continue
		}
		n := utf8.EncodeRune(buf[:], r)
		for _, x := range analysis.Normalize(string(buf[:n])) {
			if seen[x] {
				continue
			}
			seen[x] = true
			lowered := analysis.Lower(string(x))
			if lowered == string(x) {
				continue
			}
			if utf8.RuneCountInString(lowered) == 1 {
				l, _ := utf8.DecodeRuneInString(lowered)
				t.pre[l] = append(t.pre[l], x)
				continue
			}
			for _, l := range lowered {
				t.unsafe[l] = true
			}
		}
	}
	// The final sigma: Lower spells a capital sigma ς or σ by context.
	if seen['Σ'] {
		t.pre['ς'] = append(t.pre['ς'], 'Σ')
		t.pre['σ'] = append(t.pre['σ'], 'Σ')
	}
	return t
})
