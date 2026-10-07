package segment

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"unsafe"

	"github.com/RoaringBitmap/roaring/v2"
)

// A term dictionary holds one (field, kind)'s terms, sorted by bytes, in blocks of
// blockTerms. Each block is preceded in the file by the serialized postings of its
// terms, so a dictionary is written in one pass over sorted terms:
//
//	[postings of block 0][block 0][postings of block 1][block 1] ... [index]
//
//	block  uvarint postingsLen (this block's postings' byte length, so its postings
//	       start postingsLen bytes before the block itself - see below), then per
//	       term: uvarint shared prefix (with the previous term in the block),
//	       uvarint suffix length, suffix, uvarint docFreq, and either uvarint doc
//	       (docFreq 1, the postings inline) or uvarint postings length (laid out
//	       back to back, working backward from the block's own start)
//	index  u32 numTerms, u32 numBlocks, u64 sumDocFreq, u64 blockBack[numBlocks] (each
//	       block's distance back from the index itself to its own start),
//	       u32 keyOffset[numBlocks+1], the blocks' keys back to back
//
// A block's key is its first term in format 3. In format 4 it is the shortest prefix of
// its first term that sorts after the previous block's last term (the empty string for
// block 0): every term of the block sorts at or after it, and every term of the block
// before sorts before it, so a binary search over keys finds the only block a term can
// be in, as it does over first terms, at a fraction of the index's size for long terms.
//
// The index is the sparse block index: Open keeps views of its arrays, a lookup binary
// searches the keys (O(log blocks)) and scans one block. Term ordinals are positions
// in the sorted dictionary, so ordinal o lives in block o/blockTerms.
//
// Every position a dictionary's own bytes bake in is stored as a distance back from a
// point the reader already has in hand, not as a forward offset from some notional
// "dictionary start": a block's own start is "this many bytes before the index" (which
// Open already knows: it is where it started parsing, termDict.base), and that block's
// postings start is "this many bytes before the block" (which blockFor/blockOff already
// computed to find the block at all). Every one of those distances is translation
// invariant - unaffected by where either endpoint ends up in the final file - so each
// dictionary (each of a field's value, entry, word and gram dictionaries) can be built
// complete, in its own private buffer, independently of and in parallel with every
// other, and nothing about where that buffer ends up concatenated needs to be known
// until the moment it is, which is also the only moment META's one offset per
// dictionary (resolved once, in Reader.parseMeta, from the TERMS section's own absolute
// start in the footer plus META's section-relative dictOff) is filled in.

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
	keyOffs    []uint32
	keys       []byte
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
	postingsStart := d.w.off
	d.w.write(d.postBuf)
	blockStart := d.w.off
	d.blockOffs = append(d.blockOffs, blockStart)
	d.keyOffs = append(d.keyOffs, uint32(len(d.keys))) //nolint:gosec // keys stay far below 4 GiB
	first := d.termBuf[d.pending[0].start:d.pending[0].end]
	if len(d.blockOffs) > 1 {
		d.keys = append(d.keys, first[:min(commonPrefix(d.prev, first)+1, len(first))]...)
	}
	d.block.b = d.block.b[:0]
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
	var h encoder
	h.uvarint(blockStart - postingsStart)
	d.w.write(h.b)
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
		d.w.u64(off - o)
	}
	for _, o := range d.keyOffs {
		d.w.u32(o)
	}
	d.w.u32(uint32(len(d.keys))) //nolint:gosec // keys stay far below 4 GiB
	d.w.write(d.keys)
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

// termDict reads one dictionary in place. base is the index's own absolute position
// (exactly what Open resolved META's dictOff to, and what openDict is called with);
// blockOff and cursor's postings position are both resolved by subtracting a stored
// distance from a point already in hand (base, or a block's own start) rather than
// adding to one - see the type comment above for why.
type termDict struct {
	data       []byte // the whole mapping
	base       uint64 // the index's own absolute position in data
	numTerms   uint32
	numBlocks  uint32
	sumDocFreq uint64
	blockOffs  []byte // numBlocks u64, each a distance back from base
	keyOffs    []byte // numBlocks+1 u32
	keys       []byte

	// checked has bit o set once ordinal o's serialized roaring postings have passed
	// checkBitmap (see postings). Set with atomic Or and read with atomic Load, so
	// concurrent Postings calls need no lock: two racing on the same unchecked term
	// both check it, then both set the same bit, which is harmless.
	checked []atomic.Uint64
}

// openDict parses the dictionary whose index starts at the absolute position off.
func openDict(data []byte, off uint64) (*termDict, error) {
	if off > uint64(len(data)) {
		return nil, errShort
	}
	d := decoder{b: data, pos: int(off)} //nolint:gosec // bounded by len(data)
	t := &termDict{data: data, base: off}
	t.numTerms = d.u32()
	t.numBlocks = d.u32()
	t.sumDocFreq = d.u64()
	t.blockOffs = d.bytes(uint64(t.numBlocks) * 8)
	t.keyOffs = d.bytes((uint64(t.numBlocks) + 1) * 4)
	if d.err != nil {
		return nil, d.err
	}
	t.keys = d.bytes(uint64(t.keyOff(t.numBlocks)))
	if d.err != nil {
		return nil, d.err
	}
	// In uint64: numTerms+blockTerms-1 wraps in uint32 for numTerms near 2^32, which
	// would let a huge numTerms pass with no blocks and send termAt past blockOffs.
	if uint64(t.numBlocks) != (uint64(t.numTerms)+blockTerms-1)/blockTerms {
		return nil, errShort
	}
	for i := range t.numBlocks {
		back := binary.LittleEndian.Uint64(t.blockOffs[i*8:])
		if back > t.base || t.keyOff(i) > t.keyOff(i+1) {
			return nil, errShort
		}
	}
	// One bit per term: at most an eighth of the bytes the dictionary itself takes.
	t.checked = make([]atomic.Uint64, (uint64(t.numTerms)+63)/64)
	return t, nil
}

func (t *termDict) blockOff(i uint32) uint64 {
	return t.base - binary.LittleEndian.Uint64(t.blockOffs[i*8:])
}

func (t *termDict) keyOff(i uint32) uint32 {
	return binary.LittleEndian.Uint32(t.keyOffs[i*4:])
}

func (t *termDict) key(i uint32) []byte {
	return t.keys[t.keyOff(i):t.keyOff(i+1)]
}

// postings returns info's documents (info from this dictionary's lookup or iteration),
// as [bitmapAt] does, but checks a serialized roaring bitmap only the first time it is
// asked for: the mapping never changes, so once ordinal info.ord's bitmap has passed
// checkBitmap it always will, and every later call costs one atomic load instead of a
// pass over the bitmap. A bitmap that fails is not remembered, and reads as empty
// every time.
func (t *termDict) postings(info termInfo, numDocs uint32) *roaring.Bitmap {
	if info.docFreq <= 1 || info.ord >= t.numTerms {
		return bitmapAt(t.data, info, numDocs)
	}
	word, bit := &t.checked[info.ord/64], uint64(1)<<(info.ord%64)
	if word.Load()&bit != 0 {
		return viewBitmap(t.data, info.post)
	}
	rb := bitmapAt(t.data, info, numDocs)
	if !rb.IsEmpty() {
		word.Or(bit)
	}
	return rb
}

// blockFor returns the last block whose key is at most term, or false when term sorts
// before every key.
func (t *termDict) blockFor(term []byte) (uint32, bool) {
	lo, hi := uint32(0), t.numBlocks
	for lo < hi {
		mid := lo + (hi-lo)/2
		if bytes.Compare(t.key(mid), term) <= 0 {
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
	d       decoder
	post    uint64 // the next serialized postings' offset
	ord     uint32
	left    uint32
	prevLen int // the previous entry's term length (0 before the first entry)
	entry   termInfo
}

func (t *termDict) cursor(block uint32) blockCursor {
	blockStart := t.blockOff(block)
	c := blockCursor{
		d:    decoder{b: t.data, pos: int(blockStart)}, //nolint:gosec // checked at open
		ord:  block * blockTerms,
		left: min(blockTerms, t.numTerms-block*blockTerms),
	}
	postingsLen := c.d.uvarint()
	c.post = blockStart - postingsLen
	return c
}

// next decodes the next entry: its shared prefix length and suffix. False at the end
// of the block, or on a malformed block - including an entry claiming to share more
// bytes with the previous term than that term has (the first entry of a block shares
// none), which every caller would otherwise slice its term buffer past the end for.
func (c *blockCursor) next() (shared int, suffix []byte, ok bool) {
	if c.left == 0 {
		return 0, nil, false
	}
	sharedLen := c.d.uvarint()
	suffix = c.d.bytes(c.d.uvarint())
	if sharedLen > uint64(c.prevLen) { //nolint:gosec // prevLen is a length, non-negative
		c.left = 0
		return 0, nil, false
	}
	shared = int(sharedLen) //nolint:gosec // at most prevLen, checked above
	c.prevLen = shared + len(suffix)
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
