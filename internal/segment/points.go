package segment

import (
	"encoding/binary"

	"github.com/RoaringBitmap/roaring/v2"
)

// Points are a BKD-lite range index for one number column: its documents sorted by
// (key, doc), split into fixed-size blocks, each with its key range recorded so a Range
// query can skip whole blocks and add a fully-covered block's documents without
// inspecting them.
//
//	header  u32 numEntries, u32 numBlocks, u8 keyWidth (format 3 only), u8 docWidth
//	table   per block: u64 minKey, u64 maxKey, u32 count, u64 blockOffset (relative to
//	        this index's own start, like termDict's blockOffset - see termdict.go)
//	blocks  per block: packed keys (count, keyWidth; format 3 only), packed docs
//	        (count, docWidth)
//
// Format 4 drops the keys: only a block a range covers partly needs its values, at most
// two per range, and those are the number column's, read by document.
const pointsBlockSize = 128

const pointsEntryLen = 8 + 8 + 4 + 8

func pointsHeaderLen(major uint16) uint64 {
	if major < 4 {
		return 4 + 4 + 1 + 1
	}
	return 4 + 4 + 1
}

type pointPair struct {
	key uint64
	doc uint32
}

// writeSortedPoints writes pairs - a number column's (key, doc) pairs, already sorted
// by (key, doc) ascending - and returns the index's offset.
func writeSortedPoints(w *fileWriter, numDocs uint32, pairs []pointPair) uint64 {
	n := len(pairs)
	numBlocks := (n + pointsBlockSize - 1) / pointsBlockSize
	docWidth := packedWidth(bitsFor(uint64(numDocs)))

	off := w.off
	// blocksStart, and so every blockOffset written below, is relative to this index's
	// own start (off), never to w's: w.off happens to be 0 when the index is first in
	// a private per-field buffer, but a field's point index is not guaranteed to be
	// first in its buffer, and openPoints adds the index's absolute start back.
	blocksStart := pointsHeaderLen(FormatMajor) + uint64(numBlocks)*pointsEntryLen

	var h encoder
	h.u32(uint32(n))         //nolint:gosec // n fits uint32 ordinal space
	h.u32(uint32(numBlocks)) //nolint:gosec // bounded by n
	h.u8(docWidth)
	w.write(h.b)

	blockOff := blocksStart
	for i := range numBlocks {
		lo := i * pointsBlockSize
		hi := min(lo+pointsBlockSize, n)
		count := uint32(hi - lo) //nolint:gosec // bounded by pointsBlockSize
		var t encoder
		t.u64(pairs[lo].key)
		t.u64(pairs[hi-1].key)
		t.u32(count)
		t.u64(blockOff)
		w.write(t.b)
		blockOff += packedSize(uint64(count), docWidth)
	}
	for i := range numBlocks {
		lo := i * pointsBlockSize
		hi := min(lo+pointsBlockSize, n)
		pd := newPacker(w, docWidth)
		for _, p := range pairs[lo:hi] {
			pd.add(uint64(p.doc))
		}
		pd.finish()
	}
	return off
}

// points reads a BKD-lite index in place. Like termDict, blockRange's blockOffset is
// relative to the index's own absolute start (base), so it can be built in its own
// private buffer independently of every other field; Open resolves base once (openPoints)
// and blockRange adds it, so every other reader of the position it returns needs no
// further change.
type points struct {
	data       []byte
	base       uint64 // this index's own absolute start in data
	col        *numberColumn
	keyWidth   uint8 // 0 in format 4: values come from col
	hasKeys    bool
	numEntries uint32
	numBlocks  uint32
	docWidth   uint8
	table      []byte // numBlocks table entries
}

// openPoints opens the point index at off of col, in a file of the given major.
func openPoints(data []byte, off uint64, col *numberColumn, major uint16) (*points, error) {
	if off > uint64(len(data)) {
		return nil, errShort
	}
	d := decoder{b: data, pos: int(off)} //nolint:gosec // bounded by len(data)
	p := &points{data: data, base: off, col: col, hasKeys: major < 4}
	p.numEntries = d.u32()
	p.numBlocks = d.u32()
	if p.hasKeys {
		p.keyWidth = d.u8()
	}
	p.docWidth = d.u8()
	p.table = d.bytes(uint64(p.numBlocks) * pointsEntryLen)
	if d.err != nil {
		return nil, d.err
	}
	if (p.keyWidth > 56 && p.keyWidth != 64) || (p.docWidth > 56 && p.docWidth != 64) {
		return nil, errShort
	}
	// In uint64, so numEntries near 2^32 cannot wrap into a small block count.
	if uint64(p.numBlocks) != (uint64(p.numEntries)+pointsBlockSize-1)/pointsBlockSize {
		return nil, errShort
	}
	// Every block but the last holds pointsBlockSize entries, as both formats' writers
	// lay them out, together exactly numEntries, with its packed data inside data:
	// rangeDocs slices them unchecked, and a block's first entry is its index times
	// pointsBlockSize.
	var total uint64
	room := uint64(len(data)) - off
	for i := range p.numBlocks {
		_, _, count, rel := p.blockEntry(i)
		size := p.blockSize(count)
		if count == 0 || count > pointsBlockSize || (count != pointsBlockSize && i != p.numBlocks-1) || rel > room || size > room-rel {
			return nil, errShort
		}
		total += uint64(count)
	}
	if total != uint64(p.numEntries) {
		return nil, errShort
	}
	return p, nil
}

func (p *points) blockSize(count uint32) uint64 {
	n := packedSize(uint64(count), p.docWidth)
	if p.hasKeys {
		n += packedSize(uint64(count), p.keyWidth)
	}
	return n
}

// blockEntry returns block i's table entry as written: its offset relative to base.
func (p *points) blockEntry(i uint32) (minKey, maxKey uint64, count uint32, rel uint64) {
	e := p.table[i*pointsEntryLen:]
	return binary.LittleEndian.Uint64(e), binary.LittleEndian.Uint64(e[8:]), binary.LittleEndian.Uint32(e[16:]), binary.LittleEndian.Uint64(e[20:])
}

// blockRange returns block i's key range, entry count and absolute data offset, all of
// which openPoints has already checked lie inside the mapping.
func (p *points) blockRange(i uint32) (minKey, maxKey uint64, count uint32, off uint64) {
	minKey, maxKey, count, rel := p.blockEntry(i)
	return minKey, maxKey, count, p.base + rel
}

// inRange reports whether v is within [lo, hi], each bound inclusive per incLo/incHi.
func inRange(v, lo, hi float64, incLo, incHi bool) bool {
	okLo := v > lo || (incLo && v == lo)
	okHi := v < hi || (incHi && v == hi)
	return okLo && okHi
}

func (p *points) blockStart(i uint32) uint64 {
	return min(uint64(i)*pointsBlockSize, uint64(p.numEntries))
}

func (p *points) blockMin(i uint32) float64 {
	return p.col.enc.value(binary.LittleEndian.Uint64(p.table[i*pointsEntryLen:]))
}

func (p *points) blockMax(i uint32) float64 {
	return p.col.enc.value(binary.LittleEndian.Uint64(p.table[i*pointsEntryLen+8:]))
}

// searchBlocks is the first block for which f, monotone in the block index, holds
// (numBlocks when none does).
func (p *points) searchBlocks(f func(i uint32) bool) uint32 {
	lo, hi := uint32(0), p.numBlocks
	for lo < hi {
		mid := lo + (hi-lo)/2
		if f(mid) {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

// blockEntries reads block i: its documents in (value, doc) order into docs, and,
// when vals is not nil, their values into vals. A document past the column (corrupt
// data openPoints cannot see) is skipped. Both are reused buffers.
func (p *points) blockEntries(i uint32, docs []uint32, vals []float64) ([]uint32, []float64) {
	_, _, count, off := p.blockRange(i)
	var keys []byte
	if p.hasKeys {
		keyBytes := packedSize(uint64(count), p.keyWidth)
		keys, off = p.data[off:off+keyBytes], off+keyBytes
	}
	packed := p.data[off : off+packedSize(uint64(count), p.docWidth)]
	docs = docs[:0]
	if vals != nil {
		vals = vals[:0]
	}
	c := p.col
	for j := range uint64(count) {
		doc := unpack(packed, j, p.docWidth)
		if doc >= uint64(c.numDocs) {
			continue
		}
		docs = append(docs, uint32(doc)) //nolint:gosec // below numDocs, a uint32
		if vals != nil {
			var key uint64
			if p.hasKeys {
				key = unpack(keys, j, p.keyWidth)
			} else {
				key = unpack(c.packed, doc, c.width)
			}
			vals = append(vals, c.enc.value(key))
		}
	}
	return docs, vals
}

func (p *points) below(x float64, inclusive bool) uint64 {
	if p == nil || p.numBlocks == 0 {
		return 0
	}
	past := func(v float64) bool { return v > x || (!inclusive && v == x) }
	i := p.searchBlocks(func(i uint32) bool { return past(p.blockMax(i)) })
	if i == p.numBlocks {
		return uint64(p.numEntries)
	}
	n := p.blockStart(i)
	if past(p.blockMin(i)) {
		return n
	}
	var docBuf [pointsBlockSize]uint32
	var valBuf [pointsBlockSize]float64
	_, vals := p.blockEntries(i, docBuf[:0], valBuf[:0])
	for _, v := range vals {
		if past(v) {
			break
		}
		n++
	}
	return n
}

func (p *points) count(lo, hi float64, incLo, incHi bool) uint64 {
	if p == nil || hi < lo {
		return 0
	}
	upTo, before := p.below(hi, incHi), p.below(lo, !incLo)
	if upTo <= before {
		return 0
	}
	return upTo - before
}

func (p *points) candidateBlocks(lo, hi float64, incLo, incHi bool) (first, end uint32) {
	first = p.searchBlocks(func(i uint32) bool {
		v := p.blockMax(i)
		return v > lo || (incLo && v == lo)
	})
	end = p.searchBlocks(func(i uint32) bool {
		v := p.blockMin(i)
		return v > hi || (!incHi && v == hi)
	})
	return first, max(first, end)
}

// denseRangeShare: a range whose candidate blocks hold at least 1/denseRangeShare of
// the segment's documents is collected into a plain bitset and converted once, rather
// than added to a roaring bitmap block by block (each add of a block's unordered
// documents searches the bitmap's containers again).
const denseRangeShare = 64

// rangeDocs returns the documents whose value is within [lo, hi]. Only the blocks a
// binary search of the block table finds may hold such a value are read, and a block
// the range covers whole is taken without reading its values.
func (p *points) rangeDocs(lo, hi float64, incLo, incHi bool) *roaring.Bitmap {
	if p == nil || p.numBlocks == 0 || hi < lo {
		return roaring.New()
	}
	first, end := p.candidateBlocks(lo, hi, incLo, incHi)
	if first >= end {
		return roaring.New()
	}
	var words []uint64
	var result *roaring.Bitmap
	if (p.blockStart(end)-p.blockStart(first))*denseRangeShare >= uint64(p.col.numDocs) {
		words = make([]uint64, (uint64(p.col.numDocs)+63)/64)
	} else {
		result = roaring.New()
	}
	var docBuf [pointsBlockSize]uint32
	var valBuf [pointsBlockSize]float64
	buf := make([]uint32, 0, pointsBlockSize)
	for i := first; i < end; i++ {
		bmin, bmax := p.blockMin(i), p.blockMax(i)
		var docs []uint32
		if inRange(bmin, lo, hi, incLo, incHi) && inRange(bmax, lo, hi, incLo, incHi) {
			docs, _ = p.blockEntries(i, docBuf[:0], nil)
		} else {
			var vals []float64
			docs, vals = p.blockEntries(i, docBuf[:0], valBuf[:0])
			buf = buf[:0]
			for j, d := range docs {
				if inRange(vals[j], lo, hi, incLo, incHi) {
					buf = append(buf, d)
				}
			}
			docs = buf
		}
		if words != nil {
			for _, d := range docs {
				words[d>>6] |= 1 << (d & 63)
			}
		} else if len(docs) > 0 {
			result.AddMany(docs)
		}
	}
	if words != nil {
		return roaring.FromDense(words, false)
	}
	return result
}

// PointBlock is one block of a number column's point index: up to 128 documents of
// adjacent values.
type PointBlock struct {
	// Min and Max are the least and greatest values in the block.
	Min, Max float64
	// Count is how many documents the block holds.
	Count int
	p     *points
	i     uint32
}

// Docs appends the block's documents, in (value, doc) order, to dst.
func (b PointBlock) Docs(dst []uint32) []uint32 {
	var buf [pointsBlockSize]uint32
	docs, _ := b.p.blockEntries(b.i, buf[:0], nil)
	return append(dst, docs...)
}

// eachBlock calls fn with every block that may hold a value within [lo, hi]
// (inclusive), in ascending value order (descending when desc), until fn returns
// false.
func (p *points) eachBlock(lo, hi float64, desc bool, fn func(PointBlock) bool) {
	if p == nil || p.numBlocks == 0 || hi < lo {
		return
	}
	first, end := p.candidateBlocks(lo, hi, true, true)
	for k := first; k < end; k++ {
		i := k
		if desc {
			i = end - 1 - (k - first)
		}
		_, _, count, _ := p.blockEntry(i)
		if !fn(PointBlock{Min: p.blockMin(i), Max: p.blockMax(i), Count: int(count), p: p, i: i}) {
			return
		}
	}
}
