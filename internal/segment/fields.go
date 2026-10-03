package segment

import (
	"bytes"
	"cmp"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
)

// fieldBuilder accumulates one field's values across a contiguous range of documents,
// in ascending document ordinal order, for both [Build] and [Merge]. With
// BuildOptions.Threads > 1 (or, for Merge, always, up to GOMAXPROCS), there is one
// fieldBuilder per worker per field, each covering a different, disjoint range of the
// final document ordinals; [writeFieldDicts], [writeFieldDocValues],
// [writeFieldPoints] and [writeFieldPresence] combine every worker's fieldBuilder for
// one field into that field's structures, in a way that depends only on the workers'
// ranges being disjoint and ascending - never on there being just one of them - so the
// combined bytes are identical regardless of how many workers built the field.
//
// Term occurrences go into one [termPairs] per kind, which groups them by term as they
// arrive (see its doc comment for why that is the shape that keeps accumulation cheap,
// and allocation-free for a term with only one occurrence).
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

	// numDocs is this builder's numeric values, doc ascending by construction (this
	// builder only ever sees documents in ascending order, so a plain append-only
	// slice is already sorted - no sort is needed to keep it that way).
	numDocs []docFloat
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

// sortTermGroups sorts every one of b's four term dictionaries' groups by term. Call
// once a worker has finished accumulating its whole range, before its fieldBuilder is
// handed to the merge-and-write pass, which assumes every part's groups are already
// sorted and does not sort them itself.
func (b *fieldBuilder) sortTermGroups() {
	b.valuePairs.sortGroups()
	b.entryPairs.sortGroups()
	b.wordPairs.sortGroups()
	b.gramPairs.sortGroups()
}

// fieldOutput is where one field's structures landed in the file, 0 meaning absent -
// every offset here is stored as (real offset) + 1, never the real offset itself, so
// that 0 unambiguously means absent even though each structure is built into its own
// private buffer (format.go) with its own offset starting at 0: a structure that
// happens to be the very first thing in its buffer has a real offset of 0, which 0-as-
// "absent" could not otherwise be told apart from. The writers in this file
// (writeFieldDicts and friends) are what add the 1; writeSegmentParts' per-section
// fixup adds a field's position within the section on top of that (still leaving 0
// alone, still meaning absent), and Reader.parseMeta subtracts the 1 back out, after
// adding the section's own absolute start, before calling openDict and friends.
type fieldOutput struct {
	presOff, presLen   uint64
	truncOff, truncLen uint64
	dictOff            [numKinds]uint64
	keywordColOff      uint64
	multiColOff        uint64
	numberColOff       uint64
	pointsOff          uint64
}

// fieldScratch is one field's working state across the terms, doc-values and points
// sections: fieldOutput plus the doc-values inputs writeFieldDicts derives as a
// byproduct of writing the term dictionaries (so writeFieldDocValues needs no second
// pass over them) and writeFieldDocValues derives for writeFieldPoints in turn.
type fieldScratch struct {
	out *fieldOutput

	valueDocOrds, entryDocOrds   []docOrd
	numValueTerms, numEntryTerms uint32

	numEnc          numEncoding
	combinedNumDocs []docFloat // every part's numDocs, concatenated in part (range) order
}

func newFieldScratch() *fieldScratch { return &fieldScratch{out: &fieldOutput{}} }

// termPairs accumulates (term, doc) occurrences for one (field, kind) dictionary into
// groups, one per distinct term. Internally:
//
//   - term bytes are interned once per distinct term into one append-only arena, so a
//     group holds an (offset, length) into it rather than its own string;
//   - a group's docs are a singly linked chain of fixed-size blocks in one shared,
//     append-only slab ([docSlab]), rather than its own growing slice.
//
// Both together mean a term seen only once - an id, a sku, a url, anything
// effectively unique per document - costs no call to the allocator at all beyond its
// share of the arena and slab themselves, which grow in large, infrequent batches: one
// new block, one new table slot, one group appended to a slice of fixed-size structs
// with no pointers in it for the GC to trace. The map[string][]uint32 this replaced
// (and the map[string]int32-plus-slice-of-groups a first revision of this fix used)
// could not get there: both allocate a string and a slice header, each its own object,
// for every single distinct term.
//
// The table finding a term's group is a small open-addressing hash table of our own,
// not Go's map[...]: a flat []int32 slot table, linear-probed, with FNV-1a over the
// candidate term's bytes, comparing candidates against the arena directly (no string
// conversion) and growing (rehashing the groups it already has) past a 0.5 load
// factor.
//
// Accumulation stays O(occurrences). The only superlinear step is the final sort by
// term bytes ([sortGroups]), and that runs over distinct terms, not occurrences - see
// its own doc comment for why that distinction, not just avoiding a map, is what
// actually matters for a text field whose trigrams and words recur across documents.
type termPairs struct {
	arena  []byte
	groups []termGroup
	table  []int32 // length a power of two; 0 means empty, else index into groups, plus 1
	slab   docSlab
}

// termGroup is one distinct term: its bytes (a range of the owning termPairs' arena)
// and its documents (a chain in the owning termPairs' slab). Every field is a fixed-
// size scalar - no pointers - so a []termGroup needs no GC tracing of its own.
type termGroup struct {
	off, length uint32 // into the arena
	head, tail  int32  // into the slab; -1 before the first doc
	lastDoc     uint32 // the most recently appended doc, for O(1) same-document dedup
	hasDoc      bool   // false until the first doc is appended (doc 0 is a valid doc)
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
// across calls for the same term (true of every call site: [Build] and [Merge] each
// hand one fieldBuilder a strictly ascending run of document ordinals).
func (t *termPairs) add(term []byte, doc uint32) {
	idx := t.findOrCreate(term)
	g := &t.groups[idx]
	if g.hasDoc && g.lastDoc == doc {
		return // the same term occurring again in the same document: nothing new
	}
	g.head, g.tail = t.slab.append(g.head, g.tail, doc)
	g.lastDoc, g.hasDoc = doc, true
}

// findOrCreate returns term's group index, interning term into the arena and adding a
// new, empty group for it if this is the first time it has been seen.
func (t *termPairs) findOrCreate(term []byte) int32 {
	if t.table == nil {
		t.table = make([]int32, termPairsInitialTable)
	}
	h := fnv1a(term)
	mask := uint64(len(t.table) - 1) //nolint:gosec // table is never empty here (just initialized above if it was)
	i := h & mask
	for t.table[i] != 0 {
		idx := t.table[i] - 1
		g := &t.groups[idx]
		if bytes.Equal(t.arena[g.off:g.off+g.length], term) {
			return idx
		}
		i = (i + 1) & mask
	}
	if (len(t.groups)+1)*2 > len(t.table) {
		t.growTable()
		mask = uint64(len(t.table) - 1) //nolint:gosec // growTable never shrinks to empty
		for i = h & mask; t.table[i] != 0; i = (i + 1) & mask {
		}
	}
	off := len(t.arena)
	t.arena = append(t.arena, term...)
	idx := int32(len(t.groups)) //nolint:gosec // one field's distinct terms stay far below 2^31
	t.groups = append(t.groups, termGroup{off: uint32(off), length: uint32(len(term)), head: -1, tail: -1})
	t.table[i] = idx + 1
	return idx
}

// growTable doubles the table and reinserts every existing group's slot into it.
func (t *termPairs) growTable() {
	newTable := make([]int32, len(t.table)*2)
	mask := uint64(len(newTable) - 1) //nolint:gosec // newTable is never empty
	for gi := range t.groups {
		g := &t.groups[gi]
		h := fnv1a(t.arena[g.off : g.off+g.length])
		i := h & mask
		for newTable[i] != 0 {
			i = (i + 1) & mask
		}
		newTable[i] = int32(gi) + 1
	}
	t.table = newTable
}

func (t *termPairs) termBytes(g *termGroup) []byte { return t.arena[g.off : g.off+g.length] }

func (t *termPairs) appendDocs(g *termGroup, dst []uint32) []uint32 {
	return t.slab.appendDocs(g.head, dst)
}

// sortGroups sorts groups by term, ascending - the only ordering [writeMergedDict]
// needs, and over far fewer elements than the occurrences that built them whenever a
// term recurs across documents. Call once accumulation for this part is done, before
// handing it to the merge-and-write pass (which assumes sorted groups and does not
// sort them itself): that split is what lets this - the only part of building a part
// that costs more than O(occurrences) - run inside each worker's own goroutine,
// concurrently with every other worker, while writing stays single-threaded.
func (t *termPairs) sortGroups() {
	slices.SortFunc(t.groups, func(a, b termGroup) int {
		return bytes.Compare(t.termBytes(&a), t.termBytes(&b))
	})
}

// docSlab is a shared, append-only store of document-ordinal lists: every group's docs
// are a singly linked chain of fixed-size blocks inside one or two large backing
// arrays that grow rarely, in big batches - so even a single-occurrence term's one doc
// costs no call to the allocator, just the next slabBlockSize-sized slice of whichever
// backing array currently has room, and the (amortized O(1)) append to next/fill below
// that tracks it.
type docSlab struct {
	data []uint32 // current backing array; block i occupies data[i*slabBlockSize:(i+1)*slabBlockSize]
	next []int32  // next[i]: block i's successor block, or -1
	fill []uint8  // fill[i]: how many of block i's slabBlockSize slots are used
}

const (
	slabBlockSize = 64   // documents per block
	slabGrowMin   = 4096 // the backing array's minimum growth batch, in uint32s
)

// newBlock reserves one new, empty block, growing the backing array in a large batch
// first if it has no room left.
func (s *docSlab) newBlock() int32 {
	idx := int32(len(s.next)) //nolint:gosec // one field's blocks stay far below 2^31
	need := (int(idx) + 1) * slabBlockSize
	if need > len(s.data) {
		grown := max(len(s.data)*2, slabGrowMin)
		for grown < need {
			grown *= 2
		}
		newData := make([]uint32, grown)
		copy(newData, s.data)
		s.data = newData
	}
	s.next = append(s.next, -1)
	s.fill = append(s.fill, 0)
	return idx
}

func (s *docSlab) block(i int32) []uint32 {
	start := int(i) * slabBlockSize
	return s.data[start : start+slabBlockSize]
}

// append adds doc to the chain whose head and tail block indices are given (tail -1:
// no chain yet) and returns the chain's, possibly new, head and tail.
func (s *docSlab) append(head, tail int32, doc uint32) (newHead, newTail int32) {
	if tail < 0 {
		nb := s.newBlock()
		s.block(nb)[0] = doc
		s.fill[nb] = 1
		return nb, nb
	}
	if int(s.fill[tail]) < slabBlockSize {
		s.block(tail)[s.fill[tail]] = doc
		s.fill[tail]++
		return head, tail
	}
	nb := s.newBlock()
	s.next[tail] = nb
	s.block(nb)[0] = doc
	s.fill[nb] = 1
	return head, nb
}

// appendDocs appends the chain starting at head's documents, in order, to dst.
func (s *docSlab) appendDocs(head int32, dst []uint32) []uint32 {
	for b := head; b >= 0; b = s.next[b] {
		dst = append(dst, s.block(b)[:s.fill[b]]...)
	}
	return dst
}

// docOrd is one document's ordinal into a dictionary it has a term in.
type docOrd struct{ doc, ord uint32 }

// writeMergedDict k-way merges parts' already-sorted term groups ([termPairs.sortGroups])
// and writes the result as one dictionary, in ascending term order. A term present in
// more than one part gets that term's parts' docs concatenated in part order: since
// every caller gives parts in ascending order of the (disjoint) document-ordinal range
// each covers - true whether there is one part (a sequential build) or many (one per
// worker, or one per Merge input reader) - that keeps the combined list ascending with
// no re-sort, exactly the same list a single part covering every document would have
// produced. onTerm, when non-nil, is called once per distinct term, in ascending
// ordinal order, with its combined docs. Returns the dictionary's offset (0 if every
// part is empty) and how many distinct terms it holds.
func writeMergedDict(w *fileWriter, parts []*termPairs, onTerm func(ord uint32, docs []uint32)) (uint64, uint32) {
	cursor := make([]int, len(parts))
	anyLeft := func() bool {
		for p := range parts {
			if cursor[p] < len(parts[p].groups) {
				return true
			}
		}
		return false
	}
	if !anyLeft() {
		return 0, 0
	}
	enc := newPostingsEncoder()
	dw := newDictWriter(w, enc)
	var ord uint32
	docBuf := make([]uint32, 0, 64)
	for anyLeft() {
		// The smallest current term is tracked by which part holds it (minPart), never
		// by whether minTerm is nil: "" is a real term, and a part whose arena holds
		// nothing but "" hands it back as a nil slice, which a nil-means-unset check
		// would mistake for "no minimum yet" and let a later part's term beat it.
		var minTerm []byte
		minPart := -1
		for p := range parts {
			if cursor[p] >= len(parts[p].groups) {
				continue
			}
			term := parts[p].termBytes(&parts[p].groups[cursor[p]])
			if minPart < 0 || bytes.Compare(term, minTerm) < 0 {
				minTerm, minPart = term, p
			}
		}
		docBuf = docBuf[:0]
		for p := range parts {
			if cursor[p] >= len(parts[p].groups) {
				continue
			}
			g := &parts[p].groups[cursor[p]]
			if bytes.Equal(parts[p].termBytes(g), minTerm) {
				docBuf = parts[p].appendDocs(g, docBuf)
				cursor[p]++
			}
		}
		dw.add(minTerm, docBuf)
		if onTerm != nil {
			onTerm(ord, docBuf)
		}
		ord++
	}
	off, ok := dw.finish()
	if !ok {
		return 0, ord
	}
	return off, ord
}

// writeFieldDicts writes one field's four term dictionaries (terms section), merging
// parts (one fieldBuilder per worker that built any of this field, each already
// term-sorted) and recording each document's value and entry ordinals into s for
// writeFieldDocValues.
func writeFieldDicts(w *fileWriter, parts []*fieldBuilder, s *fieldScratch) {
	sel := func(which func(*fieldBuilder) *termPairs) []*termPairs {
		out := make([]*termPairs, len(parts))
		for i, p := range parts {
			out[i] = which(p)
		}
		return out
	}
	var off uint64
	off, s.numValueTerms = writeMergedDict(w,
		sel(func(p *fieldBuilder) *termPairs { return &p.valuePairs }),
		func(ord uint32, docs []uint32) {
			for _, d := range docs {
				s.valueDocOrds = append(s.valueDocOrds, docOrd{doc: d, ord: ord})
			}
		})
	if s.numValueTerms > 0 {
		s.out.dictOff[KindValue] = off + 1
	}
	off, s.numEntryTerms = writeMergedDict(w,
		sel(func(p *fieldBuilder) *termPairs { return &p.entryPairs }),
		func(ord uint32, docs []uint32) {
			for _, d := range docs {
				s.entryDocOrds = append(s.entryDocOrds, docOrd{doc: d, ord: ord})
			}
		})
	if s.numEntryTerms > 0 {
		s.out.dictOff[KindEntry] = off + 1
	}
	if off, n := writeMergedDict(w, sel(func(p *fieldBuilder) *termPairs { return &p.wordPairs }), nil); n > 0 {
		s.out.dictOff[KindWord] = off + 1
	}
	if off, n := writeMergedDict(w, sel(func(p *fieldBuilder) *termPairs { return &p.gramPairs }), nil); n > 0 {
		s.out.dictOff[KindGram] = off + 1
	}
}

func sortByDoc(pairs []docOrd) {
	slices.SortFunc(pairs, func(a, b docOrd) int {
		if c := cmp.Compare(a.doc, b.doc); c != 0 {
			return c
		}
		return cmp.Compare(a.ord, b.ord)
	})
}

// writeFieldDocValues writes one field's keyword, multi and number columns (doc
// values section), combining parts. The keyword and multi columns are driven by s's
// doc-ordinal lists, already combined across parts by writeFieldDicts; the number
// column is combined here by concatenating every part's numDocs in part order, which
// (parts cover ascending, disjoint ranges, and each part's own numDocs is itself
// already ascending - see fieldBuilder's doc comment) needs no re-sort either.
func writeFieldDocValues(w *fileWriter, parts []*fieldBuilder, numDocs uint32, s *fieldScratch) {
	hasText := false
	for _, p := range parts {
		if p.hasText {
			hasText = true
			break
		}
	}
	if hasText && len(s.valueDocOrds) > 0 {
		sortByDoc(s.valueDocOrds)
		vd := s.valueDocOrds
		off := writeKeywordColumn(w, numDocs, s.numValueTerms, func(yield func(doc, ord uint32)) {
			for _, p := range vd {
				yield(p.doc, p.ord)
			}
		})
		s.out.keywordColOff = off + 1
	}
	if len(s.entryDocOrds) > 0 {
		sortByDoc(s.entryDocOrds)
		ed := s.entryDocOrds
		off := writeMultiColumn(w, numDocs, s.numEntryTerms, func(yield func(doc uint32, ords []uint32)) {
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
		s.out.multiColOff = off + 1
	}
	var combined []docFloat
	for _, p := range parts {
		combined = append(combined, p.numDocs...)
	}
	if len(combined) == 0 {
		return
	}
	enc, st, present := chooseEncoding(numDocs, combined)
	if st.Count == 0 {
		return
	}
	off := writeNumberColumn(w, numDocs, enc, st, present, combined)
	s.out.numberColOff = off + 1
	s.numEnc = enc
	s.combinedNumDocs = combined
}

// writeFieldPoints writes one field's point index (points section), from the combined
// numDocs and chosen encoding writeFieldDocValues left in s.
func writeFieldPoints(w *fileWriter, numDocs uint32, s *fieldScratch) {
	if s.out.numberColOff == 0 || len(s.combinedNumDocs) == 0 {
		return
	}
	pairs := make([]pointPair, len(s.combinedNumDocs))
	for i, dv := range s.combinedNumDocs {
		pairs[i] = pointPair{key: s.numEnc.key(dv.v), doc: dv.doc}
	}
	sortPointPairs(pairs)
	off := writeSortedPoints(w, numDocs, pairs)
	s.out.pointsOff = off + 1
}

// writeFieldPresence writes one field's presence and truncated bitmaps (presence
// section), as the union of every part's (each part's bitmaps only ever set bits
// inside that part's own disjoint range, so a union is exactly what one bitmap built
// from the whole range would hold).
func writeFieldPresence(w *fileWriter, parts []*fieldBuilder, out *fieldOutput) {
	presence := roaring.New()
	truncated := roaring.New()
	for _, p := range parts {
		presence.Or(p.presence)
		truncated.Or(p.truncated)
	}
	blob := serializeBitmap(presence)
	out.presOff, out.presLen = w.off+1, uint64(len(blob))
	w.write(blob)
	if !truncated.IsEmpty() {
		blob2 := serializeBitmap(truncated)
		out.truncOff, out.truncLen = w.off+1, uint64(len(blob2))
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

// sortPointPairs sorts a number column's (key, doc) pairs by key, then doc: the order
// [writeSortedPoints] requires.
func sortPointPairs(pairs []pointPair) {
	slices.SortFunc(pairs, func(a, c pointPair) int {
		if d := cmp.Compare(a.key, c.key); d != 0 {
			return d
		}
		return cmp.Compare(a.doc, c.doc)
	})
}
