package segment

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

// A term dictionary holds one (field, kind)'s terms, sorted by bytes, in blocks of
// blockTerms. Each block is preceded in the file by the serialized postings of its
// terms, so a dictionary is written in one pass over sorted terms:
//
//	[postings of block 0][block 0][postings of block 1][block 1] ... [index]
//
//	block  uvarint postingsBase (absolute offset of the block's first postings)
//	       then per term: uvarint shared prefix (with the previous term in the block),
//	       uvarint suffix length, suffix, uvarint docFreq, and either uvarint doc
//	       (docFreq 1, the postings inline) or uvarint postings length (laid out
//	       back to back from postingsBase)
//	index  u32 numTerms, u32 numBlocks, u64 sumDocFreq, u64 blockOffset[numBlocks],
//	       u32 firstTermOffset[numBlocks+1], the blocks' first terms back to back
//
// The index is the sparse block index: Open keeps views of its arrays, a lookup binary
// searches the first terms (O(log blocks)) and scans one block. Term ordinals are
// positions in the sorted dictionary, so ordinal o lives in block o/blockTerms.

// blockTerms is how many terms a block holds (the last may hold fewer).
const blockTerms = 32

// dictWriter streams one term dictionary into the TERMS section.
type dictWriter struct {
	w          *fileWriter
	enc        *postingsEncoder
	pending    []pendingTerm
	termBuf    []byte // the pending terms' bytes
	postBuf    []byte // the pending terms' serialized postings
	block      encoder
	prev       []byte
	blockOffs  []uint64
	firstOffs  []uint32
	firstTerms []byte
	numTerms   uint32
	sumDocFreq uint64
}

type pendingTerm struct {
	start, end int // in termBuf
	docFreq    uint32
	single     uint32
	postLen    int
}

func newDictWriter(w *fileWriter, enc *postingsEncoder) *dictWriter {
	return &dictWriter{w: w, enc: enc, pending: make([]pendingTerm, 0, blockTerms)}
}

// add appends a term, greater than every term before it, with its documents (sorted,
// distinct, at least one).
func (d *dictWriter) add(term []byte, docs []uint32) {
	p := pendingTerm{start: len(d.termBuf), docFreq: uint32(len(docs))} //nolint:gosec // docs fit uint32 ordinals
	d.termBuf = append(d.termBuf, term...)
	p.end = len(d.termBuf)
	if len(docs) == 1 {
		p.single = docs[0]
	} else {
		blob := d.enc.encode(docs)
		d.postBuf = append(d.postBuf, blob...)
		p.postLen = len(blob)
	}
	d.pending = append(d.pending, p)
	d.numTerms++
	d.sumDocFreq += uint64(len(docs))
	if len(d.pending) == blockTerms {
		d.flushBlock()
	}
}

func (d *dictWriter) flushBlock() {
	if len(d.pending) == 0 {
		return
	}
	base := d.w.off
	d.w.write(d.postBuf)
	d.blockOffs = append(d.blockOffs, d.w.off)
	d.firstOffs = append(d.firstOffs, uint32(len(d.firstTerms))) //nolint:gosec // first terms stay far below 4 GiB
	first := d.pending[0]
	d.firstTerms = append(d.firstTerms, d.termBuf[first.start:first.end]...)
	d.block.b = d.block.b[:0]
	d.block.uvarint(base)
	d.prev = d.prev[:0]
	for _, p := range d.pending {
		term := d.termBuf[p.start:p.end]
		shared := commonPrefix(d.prev, term)
		d.block.uvarint(uint64(shared)) //nolint:gosec // commonPrefix returns a non-negative length
		d.block.bytes(term[shared:])
		d.block.uvarint(uint64(p.docFreq))
		if p.docFreq == 1 {
			d.block.uvarint(uint64(p.single))
		} else {
			d.block.uvarint(uint64(p.postLen)) //nolint:gosec // a serialized postings blob's length, non-negative
		}
		d.prev = append(d.prev[:0], term...)
	}
	d.w.write(d.block.b)
	d.pending = d.pending[:0]
	d.termBuf = d.termBuf[:0]
	d.postBuf = d.postBuf[:0]
}

// finish writes the block index and returns its offset, or false for a dictionary
// with no terms (which is not written).
func (d *dictWriter) finish() (uint64, bool) {
	d.flushBlock()
	if d.numTerms == 0 {
		return 0, false
	}
	off := d.w.off
	d.w.u32(d.numTerms)
	d.w.u32(uint32(len(d.blockOffs))) //nolint:gosec // blocks fit uint32
	d.w.u64(d.sumDocFreq)
	for _, o := range d.blockOffs {
		d.w.u64(o)
	}
	for _, o := range d.firstOffs {
		d.w.u32(o)
	}
	d.w.u32(uint32(len(d.firstTerms))) //nolint:gosec // first terms stay far below 4 GiB
	d.w.write(d.firstTerms)
	return off, true
}

func commonPrefix(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// termDict reads one dictionary in place.
type termDict struct {
	data       []byte // the whole mapping
	numTerms   uint32
	numBlocks  uint32
	sumDocFreq uint64
	blockOffs  []byte // numBlocks u64
	firstOffs  []byte // numBlocks+1 u32
	firstTerms []byte
}

// openDict parses the index at off.
func openDict(data []byte, off uint64) (*termDict, error) {
	if off > uint64(len(data)) {
		return nil, errShort
	}
	d := decoder{b: data, pos: int(off)} //nolint:gosec // bounded by len(data)
	t := &termDict{data: data}
	t.numTerms = d.u32()
	t.numBlocks = d.u32()
	t.sumDocFreq = d.u64()
	t.blockOffs = d.bytes(uint64(t.numBlocks) * 8)
	t.firstOffs = d.bytes((uint64(t.numBlocks) + 1) * 4)
	if d.err != nil {
		return nil, d.err
	}
	t.firstTerms = d.bytes(uint64(t.firstOff(t.numBlocks)))
	if d.err != nil {
		return nil, d.err
	}
	if t.numBlocks != (t.numTerms+blockTerms-1)/blockTerms {
		return nil, errShort
	}
	for i := range t.numBlocks {
		if t.blockOff(i) >= uint64(len(data)) || t.firstOff(i) > t.firstOff(i+1) {
			return nil, errShort
		}
	}
	return t, nil
}

func (t *termDict) blockOff(i uint32) uint64 {
	return binary.LittleEndian.Uint64(t.blockOffs[i*8:])
}

func (t *termDict) firstOff(i uint32) uint32 {
	return binary.LittleEndian.Uint32(t.firstOffs[i*4:])
}

func (t *termDict) firstTerm(i uint32) []byte {
	return t.firstTerms[t.firstOff(i):t.firstOff(i+1)]
}

// blockFor returns the last block whose first term is at most term, or false when term
// sorts before every term.
func (t *termDict) blockFor(term []byte) (uint32, bool) {
	lo, hi := uint32(0), t.numBlocks // the answer is the last i in [lo, hi) with first(i) <= term
	for lo < hi {
		mid := lo + (hi-lo)/2
		if bytes.Compare(t.firstTerm(mid), term) <= 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if lo == 0 {
		return 0, false
	}
	return lo - 1, true
}

// blockCursor decodes one block's entries in order.
type blockCursor struct {
	d     decoder
	post  uint64 // the next serialized postings' offset
	ord   uint32
	left  uint32
	entry termInfo
}

func (t *termDict) cursor(block uint32) blockCursor {
	c := blockCursor{
		d:    decoder{b: t.data, pos: int(t.blockOff(block))}, //nolint:gosec // checked at open
		ord:  block * blockTerms,
		left: min(blockTerms, t.numTerms-block*blockTerms),
	}
	c.post = c.d.uvarint()
	return c
}

// next decodes the next entry: its shared prefix length and suffix. False at the end
// of the block, or on a malformed block.
func (c *blockCursor) next() (shared int, suffix []byte, ok bool) {
	if c.left == 0 {
		return 0, nil, false
	}
	shared = int(c.d.uvarint()) //nolint:gosec // bounded by term length
	suffix = c.d.bytes(c.d.uvarint())
	c.entry = termInfo{ord: c.ord, docFreq: uint32(c.d.uvarint())} //nolint:gosec // written from a uint32
	if c.entry.docFreq == 1 {
		c.entry.single = uint32(c.d.uvarint()) //nolint:gosec // written from a uint32
	} else {
		n := c.d.uvarint()
		c.entry.post = region{off: c.post, n: n}
		c.post += n
	}
	if c.d.err != nil {
		c.left = 0
		return 0, nil, false
	}
	c.ord++
	c.left--
	return shared, suffix, true
}

// lookup finds term without building any term: it tracks how many leading bytes of
// term the previous entry matched, which with each entry's shared prefix decides
// whether the entry sorts before, at or after term.
func (t *termDict) lookup(term []byte) (termInfo, bool) {
	if t == nil {
		return termInfo{}, false
	}
	block, ok := t.blockFor(term)
	if !ok {
		return termInfo{}, false
	}
	c := t.cursor(block)
	matched := 0 // the previous entry equals term in its first matched bytes, and sorts before it
	for {
		shared, suffix, ok := c.next()
		if !ok {
			return termInfo{}, false
		}
		switch {
		case shared < matched:
			// This entry leaves the previous one at a byte where that one matched term,
			// with a greater byte (entries ascend): it sorts after term.
			return termInfo{}, false
		case shared > matched:
			continue // still differs from term where the previous entry did: before term
		}
		n := commonPrefix(suffix, term[matched:])
		matched += n
		switch {
		case n == len(suffix) && matched == len(term):
			return c.entry, true
		case n == len(suffix):
			continue // a proper prefix of term: before it
		case matched == len(term) || suffix[n] > term[matched]:
			return termInfo{}, false
		}
	}
}

// termAt appends ordinal ord's term to dst.
func (t *termDict) termAt(dst []byte, ord uint32) []byte {
	if t == nil || ord >= t.numTerms {
		return dst
	}
	c := t.cursor(ord / blockTerms)
	start := len(dst)
	for {
		shared, suffix, ok := c.next()
		if !ok {
			return dst[:start]
		}
		dst = append(dst[:start+shared], suffix...)
		if c.entry.ord == ord {
			return dst
		}
	}
}

// termIter walks a dictionary's terms in order from a starting block.
type termIter struct {
	t     *termDict
	block uint32
	c     blockCursor
	term  []byte
	info  termInfo
}

func (t *termDict) iter(block uint32) *termIter {
	it := &termIter{t: t, block: block}
	if t != nil && block < t.numBlocks {
		it.c = t.cursor(block)
	}
	return it
}

// next moves to the next term; false at the end.
func (it *termIter) next() bool {
	if it.t == nil {
		return false
	}
	for {
		shared, suffix, ok := it.c.next()
		if ok {
			it.term = append(it.term[:shared], suffix...)
			it.info = it.c.entry
			return true
		}
		it.block++
		if it.block >= it.t.numBlocks {
			return false
		}
		it.c = it.t.cursor(it.block)
	}
}

// stringBytes views s as bytes without copying; the bytes must not be written.
func stringBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
