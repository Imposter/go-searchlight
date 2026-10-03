package segment

import (
	"cmp"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
)

// fieldBuilder accumulates one field's values across a segment's documents, in document
// ordinal order, for both [Build] (from [schema.Doc] values) and [Merge] (from readers).
//
// Term occurrences go into one [termPairs] per kind, which groups them by term as they
// arrive (see its doc comment for why that is the shape that keeps accumulation cheap).
// [prepareField] sorts each one's groups by term; [writeDicts] then walks the sorted
// groups in one pass, writing one dictionary entry per term, and - for the two kinds
// that feed a doc-values column, value and entry - recording each document's ordinal
// as a byproduct of that same walk, so no separate map of term to ordinal is ever
// built either.
type fieldBuilder struct {
	presence  *roaring.Bitmap
	truncated *roaring.Bitmap

	valuePairs termPairs // KindValue: text, or "true"/"false"
	entryPairs termPairs // KindEntry
	wordPairs  termPairs // KindWord
	gramPairs  termPairs // KindGram

	// hasText is set whenever a value went into valuePairs as text rather than as a
	// bool's "true"/"false": it gates building a keyword column at all, since a bool
	// field has a KindValue dictionary (eq/ne read it directly) but nothing to sort
	// or run a terms aggregation over.
	hasText bool

	numDocs []docFloat // doc, its numeric value; sorted by doc after prepareField

	// Set by prepareField, from numDocs: the number column's encoding and stats (not
	// written yet - that's writeDocValues, which needs numDocs's byte offset first -
	// and the point index's sorted (key, doc) pairs, ready to write directly.
	numEnc     numEncoding
	numStats   Stats
	numPresent []uint64
	pointPairs []pointPair

	// Set by writeDicts (from valuePairs/entryPairs), consumed by writeDocValues: each
	// document's ordinal into the KindValue or KindEntry dictionary, and how many
	// distinct terms each dictionary has.
	valueDocOrds  []docOrd
	entryDocOrds  []docOrd
	numValueTerms uint32
	numEntryTerms uint32
}

type docFloat struct {
	doc uint32
	v   float64
}

func newFieldBuilder() *fieldBuilder {
	return &fieldBuilder{presence: roaring.New(), truncated: roaring.New()}
}

func (b *fieldBuilder) addValueTerm(doc uint32, term string) { b.valuePairs.addString(term, doc) }
func (b *fieldBuilder) addEntryTerm(doc uint32, term string) { b.entryPairs.addString(term, doc) }
func (b *fieldBuilder) addWordTerm(doc uint32, term string)  { b.wordPairs.addString(term, doc) }
func (b *fieldBuilder) addGramTerm(doc uint32, term string)  { b.gramPairs.addString(term, doc) }

// prepareField does b's CPU-heavy work: sorting its four term arenas and its numeric
// values, and (from those) choosing the number column's encoding and the point index's
// sorted pairs. Safe to call concurrently with other fields' prepareField, since it
// only touches b; the result is consumed afterward, single-threaded, by writeDicts,
// writeDocValues and writePointsSection, in sorted field order, which is what makes
// those always write the same bytes regardless of how many goroutines ran prepareField.
func (b *fieldBuilder) prepareField(numDocs uint32) {
	b.valuePairs.sortGroups()
	b.entryPairs.sortGroups()
	b.wordPairs.sortGroups()
	b.gramPairs.sortGroups()
	if len(b.numDocs) == 0 {
		return
	}
	slices.SortFunc(b.numDocs, func(a, c docFloat) int { return cmp.Compare(a.doc, c.doc) })
	enc, st, present := chooseEncoding(numDocs, b.numDocs)
	if st.Count == 0 {
		return
	}
	b.numEnc, b.numStats, b.numPresent = enc, st, present
	pairs := make([]pointPair, len(b.numDocs))
	for i, dv := range b.numDocs {
		pairs[i] = pointPair{key: enc.key(dv.v), doc: dv.doc}
	}
	slices.SortFunc(pairs, func(a, c pointPair) int {
		if d := cmp.Compare(a.key, c.key); d != 0 {
			return d
		}
		return cmp.Compare(a.doc, c.doc)
	})
	b.pointPairs = pairs
}

// prepareFields runs prepareField for every named builder, using up to threads
// goroutines (0 or 1: sequential, in a plain loop). The order fields finish preparing
// in is unspecified either way; only the later, single-threaded writing pass is
// order-sensitive.
func prepareFields(names []string, builders map[string]*fieldBuilder, numDocs uint32, threads int) {
	if threads < 2 || len(names) < 2 {
		for _, name := range names {
			builders[name].prepareField(numDocs)
		}
		return
	}
	if threads > len(names) {
		threads = len(names)
	}
	work := make(chan string)
	done := make(chan struct{})
	for range threads {
		go func() {
			for name := range work {
				builders[name].prepareField(numDocs)
			}
			done <- struct{}{}
		}()
	}
	for _, name := range names {
		work <- name
	}
	close(work)
	for range threads {
		<-done
	}
}

// fieldOutput is where one field's structures landed in the file, 0 meaning absent
// (every section starts past the header, so 0 is never a real offset).
type fieldOutput struct {
	presOff, presLen   uint64
	truncOff, truncLen uint64
	dictOff            [numKinds]uint64
	keywordColOff      uint64
	multiColOff        uint64
	numberColOff       uint64
	pointsOff          uint64
}

// termPairs accumulates (term, doc) occurrences for one (field, kind) dictionary into
// groups, one per distinct term, each with its own growing docs slice - the same shape
// map[string][]uint32 gave, but keyed by map[string]int32 into a flat []termGroup
// rather than map[string][]uint32 directly.
//
// That split matters for two reasons. First, since add's caller always presents one
// field's occurrences in ascending doc order (true of every call site: [Build] and
// [Merge] both visit documents in ascending ordinal order, one field at a time), a
// repeat of the same (term, doc) pair - the same gram or word occurring more than once
// in one document's text - always arrives right after the previous occurrence of that
// term, so checking only the last entry of docs is enough to deduplicate it; nothing
// ever needs a second, later pass over the occurrences to do that. Second, growing a
// plain int32 slot, instead of a slice header, in the table that finds a term's group
// keeps that table free of pointers for the GC to trace, and avoids a map
// value's read-modify-write on every single occurrence (a `groups[idx].docs =
// append(...)` is one direct slice-element mutation instead). Both together are what
// let accumulation stay O(occurrences), with the only O(distinct terms ·
// log(distinct terms)) work being the final sort by term bytes ([sortGroups]) that
// the dictionary's ordering requires - unlike sorting the occurrences themselves,
// which would cost O(occurrences · log(occurrences)) and loses badly whenever one
// term recurs across a lot of documents (a boilerplate phrase repeated on every
// product page, say, or - as every one of a text field's trigrams is, across however
// many documents share any substring of it - routine): the dictionary's run time
// would then be dominated by comparisons between copies of the very same term, for
// no benefit over a hash lookup of it.
//
// The table finding a term's group is a small open-addressing hash table of our own,
// not Go's map[string]int32: on a profile, even keeping the map's value down to a
// bare int32, Go's generic map still cost more per lookup here than this, measured on
// the benchmark corpus, saves - a flat []int32 slot table, linear-probed, with FNV-1a
// (fast and more than adequate for this: avoiding collisions among a few thousand
// short, already-distinct terms, not any adversarial or cryptographic property) run
// directly over the candidate term's bytes, growing (and rehashing the groups it
// already has into a bigger table) past a 0.5 load factor.
type termPairs struct {
	groups []termGroup
	table  []int32 // length a power of two; 0 means empty, else index into groups, plus 1
}

type termGroup struct {
	term string
	docs []uint32
}

const termPairsInitialTable = 64 // a power of two; grown by doubling

func (t *termPairs) addString(term string, doc uint32) { t.add(stringBytes(term), doc) }

// fnv1a hashes b (3 to perhaps a few hundred bytes here: a gram, a word, a value or an
// entry), the simplest hash that is fast for such short inputs and has no observed
// trouble spreading them across the table.
func fnv1a(b []byte) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}

// add records one occurrence of term in doc. Must be called with doc non-decreasing
// across calls for the same term (true of every call site; see the type comment).
func (t *termPairs) add(term []byte, doc uint32) {
	if t.table == nil {
		t.table = make([]int32, termPairsInitialTable)
	}
	h := fnv1a(term)
	mask := uint64(len(t.table) - 1) //nolint:gosec // table is never empty here (just initialized above if it was)
	i := h & mask
	for t.table[i] != 0 {
		g := &t.groups[t.table[i]-1]
		if g.term == string(term) { // allocation-free: Go elides the conversion for ==
			if n := len(g.docs); n == 0 || g.docs[n-1] != doc {
				g.docs = append(g.docs, doc)
			}
			return
		}
		i = (i + 1) & mask
	}
	if (len(t.groups)+1)*2 > len(t.table) {
		t.grow()
		mask = uint64(len(t.table) - 1) //nolint:gosec // grow never shrinks to empty
		for i = h & mask; t.table[i] != 0; i = (i + 1) & mask {
		}
	}
	idx := int32(len(t.groups)) //nolint:gosec // one field's distinct terms stay far below 2^31
	t.groups = append(t.groups, termGroup{term: string(term), docs: []uint32{doc}})
	t.table[i] = idx + 1
}

// grow doubles the table and reinserts every existing group into it.
func (t *termPairs) grow() {
	newTable := make([]int32, len(t.table)*2)
	mask := uint64(len(newTable) - 1) //nolint:gosec // newTable is never empty
	for gi := range t.groups {
		h := fnv1a(stringBytes(t.groups[gi].term))
		i := h & mask
		for newTable[i] != 0 {
			i = (i + 1) & mask
		}
		newTable[i] = int32(gi) + 1
	}
	t.table = newTable
}

func (t *termPairs) empty() bool { return len(t.groups) == 0 }

// sortGroups sorts groups by term, ascending - the only ordering [writeTermPairsDict]
// needs, and over far fewer elements than the occurrences that built them whenever a
// term recurs across documents. Call once, before writeTermPairsDict (which assumes
// sorted groups and does not sort them itself, so that this - the only part of
// preparing a field that still costs more than O(occurrences) - can run in
// [fieldBuilder.prepareField], concurrently with other fields, while writing stays
// single-threaded).
func (t *termPairs) sortGroups() {
	slices.SortFunc(t.groups, func(a, b termGroup) int { return cmp.Compare(a.term, b.term) })
}

// docOrd is one document's ordinal into a dictionary it has a term in.
type docOrd struct{ doc, ord uint32 }

// writeTermPairsDict writes t's groups as one dictionary, in ascending term order (t
// must already be sorted: [sortGroups]). onTerm, when non-nil, is called once per
// term, in ascending ordinal order, with its ascending, already-deduplicated docs -
// letting a caller derive doc-values ordinals in the same pass, with no second lookup.
// Returns the dictionary's offset (0 for an empty t) and how many distinct terms it
// holds.
func writeTermPairsDict(w *fileWriter, t *termPairs, onTerm func(ord uint32, docs []uint32)) (uint64, uint32) {
	if t.empty() {
		return 0, 0
	}
	enc := newPostingsEncoder()
	dw := newDictWriter(w, enc)
	for i := range t.groups {
		g := &t.groups[i]
		dw.add(stringBytes(g.term), g.docs)
		if onTerm != nil {
			onTerm(uint32(i), g.docs)
		}
	}
	n := uint32(len(t.groups)) //nolint:gosec // bounded by the term count
	off, ok := dw.finish()
	if !ok {
		return 0, n
	}
	return off, n
}

// writeDicts writes b's four term dictionaries (terms section), recording each
// document's value and entry ordinals ([valueDocOrds], [entryDocOrds]) for
// writeDocValues.
func (b *fieldBuilder) writeDicts(w *fileWriter, out *fieldOutput) {
	out.dictOff[KindValue], b.numValueTerms = writeTermPairsDict(w, &b.valuePairs, func(ord uint32, docs []uint32) {
		for _, d := range docs {
			b.valueDocOrds = append(b.valueDocOrds, docOrd{doc: d, ord: ord})
		}
	})
	out.dictOff[KindEntry], b.numEntryTerms = writeTermPairsDict(w, &b.entryPairs, func(ord uint32, docs []uint32) {
		for _, d := range docs {
			b.entryDocOrds = append(b.entryDocOrds, docOrd{doc: d, ord: ord})
		}
	})
	out.dictOff[KindWord], _ = writeTermPairsDict(w, &b.wordPairs, nil)
	out.dictOff[KindGram], _ = writeTermPairsDict(w, &b.gramPairs, nil)
}

func sortByDoc(pairs []docOrd) {
	slices.SortFunc(pairs, func(a, b docOrd) int {
		if c := cmp.Compare(a.doc, b.doc); c != 0 {
			return c
		}
		return cmp.Compare(a.ord, b.ord)
	})
}

// writeDocValues writes b's keyword, multi and number columns (doc values section).
func (b *fieldBuilder) writeDocValues(w *fileWriter, numDocs uint32, out *fieldOutput) {
	if b.hasText && len(b.valueDocOrds) > 0 {
		sortByDoc(b.valueDocOrds)
		vd := b.valueDocOrds
		out.keywordColOff = writeKeywordColumn(w, numDocs, b.numValueTerms, func(yield func(doc, ord uint32)) {
			for _, p := range vd {
				yield(p.doc, p.ord)
			}
		})
	}
	if len(b.entryDocOrds) > 0 {
		sortByDoc(b.entryDocOrds)
		ed := b.entryDocOrds
		out.multiColOff = writeMultiColumn(w, numDocs, b.numEntryTerms, func(yield func(doc uint32, ords []uint32)) {
			buf := make([]uint32, 0, 8)
			i := 0
			for i < len(ed) {
				j := i
				doc := ed[i].doc
				buf = buf[:0]
				for j < len(ed) && ed[j].doc == doc {
					buf = append(buf, ed[j].ord)
					j++
				}
				yield(doc, buf)
				i = j
			}
		})
	}
	if b.numStats.Count > 0 {
		out.numberColOff = writeNumberColumn(w, numDocs, b.numEnc, b.numStats, b.numPresent, b.numDocs)
	}
}

// writePointsSection writes b's point index (points section), when it has a number
// column.
func (b *fieldBuilder) writePointsSection(w *fileWriter, numDocs uint32, out *fieldOutput) {
	if out.numberColOff == 0 || len(b.pointPairs) == 0 {
		return
	}
	out.pointsOff = writeSortedPoints(w, numDocs, b.pointPairs)
}

// writePresence writes b's presence and truncated bitmaps (presence section).
func (b *fieldBuilder) writePresence(w *fileWriter, out *fieldOutput) {
	blob := serializeBitmap(b.presence)
	out.presOff, out.presLen = w.off, uint64(len(blob))
	w.write(blob)
	if !b.truncated.IsEmpty() {
		blob2 := serializeBitmap(b.truncated)
		out.truncOff, out.truncLen = w.off, uint64(len(blob2))
		w.write(blob2)
	}
}

// writeMeta writes the META section: numDocs, the stored index's offset, and each
// field's directory entry, sorted by name.
func writeMeta(w *fileWriter, numDocs uint32, storedIndexOff uint64, names []string, outs map[string]*fieldOutput) {
	var h encoder
	h.u32(numDocs)
	h.u64(storedIndexOff)
	h.u32(uint32(len(names))) //nolint:gosec // a segment holds far fewer than 4 billion fields
	w.write(h.b)
	for _, name := range names {
		out := outs[name]
		var fh encoder
		fh.bytes([]byte(name))
		fh.u64(out.presOff)
		fh.u64(out.presLen)
		fh.u64(out.truncOff)
		fh.u64(out.truncLen)
		for k := range numKinds {
			fh.u64(out.dictOff[k])
		}
		fh.u64(out.keywordColOff)
		fh.u64(out.multiColOff)
		fh.u64(out.numberColOff)
		fh.u64(out.pointsOff)
		w.write(fh.b)
	}
}

func serializeBitmap(rb *roaring.Bitmap) []byte {
	rb.RunOptimize()
	data, err := rb.MarshalBinary()
	if err != nil {
		panic(err) // MarshalBinary over an in-memory bitmap cannot fail
	}
	return data
}
