package segment

import (
	"cmp"
	"slices"
	"sort"

	"github.com/RoaringBitmap/roaring/v2"
)

// fieldBuilder accumulates one field's values across a segment's documents, in document
// ordinal order, for both [Build] (from [schema.Doc] values) and [Merge] (from readers).
// Every per-doc slice here is appended in ascending document ordinal order by its
// caller, which is what the doc-values writers require.
type fieldBuilder struct {
	presence  *roaring.Bitmap
	truncated *roaring.Bitmap

	valueDocs map[string][]uint32 // KindValue term -> docs (text, or "true"/"false")
	entryDocs map[string][]uint32 // KindEntry term -> docs
	wordDocs  map[string][]uint32 // KindWord term -> docs
	gramDocs  map[string][]uint32 // KindGram term -> docs

	textDocs    []docString  // doc, its KindValue text (only when the value is text, not bool)
	entriesDocs []docEntries // doc, its distinct entries
	numDocs     []docFloat   // doc, its numeric value
	numEnc      numEncoding  // set by writeDocValues, read by writePointsSection
}

type docString struct {
	doc uint32
	s   string
}

type docEntries struct {
	doc uint32
	es  []string
}

type docFloat struct {
	doc uint32
	v   float64
}

func newFieldBuilder() *fieldBuilder {
	return &fieldBuilder{presence: roaring.New(), truncated: roaring.New()}
}

func (b *fieldBuilder) addValueTerm(doc uint32, term string) {
	if b.valueDocs == nil {
		b.valueDocs = make(map[string][]uint32)
	}
	b.valueDocs[term] = append(b.valueDocs[term], doc)
}

func (b *fieldBuilder) addEntryTerm(doc uint32, term string) {
	if b.entryDocs == nil {
		b.entryDocs = make(map[string][]uint32)
	}
	b.entryDocs[term] = append(b.entryDocs[term], doc)
}

func (b *fieldBuilder) addWordTerm(doc uint32, term string) {
	if b.wordDocs == nil {
		b.wordDocs = make(map[string][]uint32)
	}
	b.wordDocs[term] = append(b.wordDocs[term], doc)
}

func (b *fieldBuilder) addGramTerm(doc uint32, term string) {
	if b.gramDocs == nil {
		b.gramDocs = make(map[string][]uint32)
	}
	b.gramDocs[term] = append(b.gramDocs[term], doc)
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

// writeTermMap sorts m's terms, writes them as one dictionary and returns its offset
// (0, false when m is empty) and each term's assigned ordinal.
func writeTermMap(w *fileWriter, m map[string][]uint32) (uint64, map[string]uint32) {
	if len(m) == 0 {
		return 0, nil
	}
	terms := make([]string, 0, len(m))
	for t := range m {
		terms = append(terms, t)
	}
	sort.Strings(terms)
	enc := newPostingsEncoder()
	dw := newDictWriter(w, enc)
	ords := make(map[string]uint32, len(terms))
	for i, t := range terms {
		dw.add(stringBytes(t), m[t])
		ords[t] = uint32(i)
	}
	off, ok := dw.finish()
	if !ok {
		return 0, ords
	}
	return off, ords
}

// writeDicts writes b's four term dictionaries (terms section) and returns the field's
// value and entry ordinal maps, needed by writeDocValues for the keyword and multi
// columns.
func (b *fieldBuilder) writeDicts(w *fileWriter, out *fieldOutput) (valueOrds, entryOrds map[string]uint32) {
	out.dictOff[KindValue], valueOrds = writeTermMap(w, b.valueDocs)
	out.dictOff[KindEntry], entryOrds = writeTermMap(w, b.entryDocs)
	out.dictOff[KindWord], _ = writeTermMap(w, b.wordDocs)
	out.dictOff[KindGram], _ = writeTermMap(w, b.gramDocs)
	return valueOrds, entryOrds
}

// writeDocValues writes b's keyword, multi and number columns (doc values section).
func (b *fieldBuilder) writeDocValues(w *fileWriter, numDocs uint32, out *fieldOutput, valueOrds, entryOrds map[string]uint32) {
	if len(b.textDocs) > 0 && valueOrds != nil {
		slices.SortFunc(b.textDocs, func(a, c docString) int { return cmp.Compare(a.doc, c.doc) })
		numTerms := uint32(len(valueOrds)) //nolint:gosec // bounded by the term count
		td := b.textDocs
		out.keywordColOff = writeKeywordColumn(w, numDocs, numTerms, func(yield func(doc, ord uint32)) {
			for _, ds := range td {
				yield(ds.doc, valueOrds[ds.s])
			}
		})
	}
	if len(b.entriesDocs) > 0 && entryOrds != nil {
		slices.SortFunc(b.entriesDocs, func(a, c docEntries) int { return cmp.Compare(a.doc, c.doc) })
		numTerms := uint32(len(entryOrds)) //nolint:gosec // bounded by the term count
		ed := b.entriesDocs
		out.multiColOff = writeMultiColumn(w, numDocs, numTerms, func(yield func(doc uint32, ords []uint32)) {
			buf := make([]uint32, 0, 8)
			for _, de := range ed {
				buf = buf[:0]
				for _, e := range de.es {
					buf = append(buf, entryOrds[e])
				}
				slices.Sort(buf)
				yield(de.doc, buf)
			}
		})
	}
	if len(b.numDocs) > 0 {
		slices.SortFunc(b.numDocs, func(a, c docFloat) int { return cmp.Compare(a.doc, c.doc) })
		if off, enc, ok := writeNumberColumn(w, numDocs, b.numberSource()); ok {
			out.numberColOff = off
			b.numEnc = enc
		}
	}
}

// numberSource returns a [numberSource] over b's accumulated numeric values.
func (b *fieldBuilder) numberSource() numberSource {
	nd := b.numDocs
	return func(yield func(doc uint32, v float64)) {
		for _, dv := range nd {
			yield(dv.doc, dv.v)
		}
	}
}

// writePointsSection writes b's point index (points section), when it has a number
// column.
func (b *fieldBuilder) writePointsSection(w *fileWriter, numDocs uint32, out *fieldOutput) {
	if out.numberColOff == 0 {
		return
	}
	out.pointsOff = writePoints(w, numDocs, b.numEnc, b.numberSource())
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
