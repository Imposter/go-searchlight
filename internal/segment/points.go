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

// pointsHeaderLen is the header's size in a file of the given major.
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
	// Every block must hold 1 to pointsBlockSize entries, together exactly numEntries,
	// with its packed data inside data: rangeDocs slices them unchecked.
	var total uint64
	room := uint64(len(data)) - off
	for i := range p.numBlocks {
		_, _, count, rel := p.blockEntry(i)
		size := p.blockSize(count)
		if count == 0 || count > pointsBlockSize || rel > room || size > room-rel {
			return nil, errShort
		}
		total += uint64(count)
	}
	if total != uint64(p.numEntries) {
		return nil, errShort
	}
	return p, nil
}

// blockSize is the bytes a block of count entries takes.
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

// rangeDocs returns the documents whose value is within [lo, hi]. Matches are
// collected per block and added with one AddMany call each, rather than one Add call
// per document: AddMany amortizes roaring's container lookup and growth over the whole
// batch instead of repeating it per match, which matters here since a range query's
// match count is typically a sizeable fraction of the column, not a handful of terms.
func (p *points) rangeDocs(lo, hi float64, incLo, incHi bool) *roaring.Bitmap {
	result := roaring.New()
	if p == nil {
		return result
	}
	var buf []uint32
	for i := range p.numBlocks {
		minKey, maxKey, count, off := p.blockRange(i)
		bmin, bmax := p.col.enc.value(minKey), p.col.enc.value(maxKey)
		if bmin > hi || (bmin == hi && !incHi) {
			break // blocks ascend by key: nothing further can be in range
		}
		if bmax < lo || (bmax == lo && !incLo) {
			continue
		}
		fullyIn := inRange(bmin, lo, hi, incLo, incHi) && inRange(bmax, lo, hi, incLo, incHi)
		var keys []byte
		if p.hasKeys {
			keyBytes := packedSize(uint64(count), p.keyWidth)
			keys, off = p.data[off:off+keyBytes], off+keyBytes
		}
		docs := p.data[off : off+packedSize(uint64(count), p.docWidth)]
		buf = buf[:0]
		for j := range uint64(count) {
			doc := unpack(docs, j, p.docWidth)
			if doc >= uint64(p.col.numDocs) {
				continue
			}
			if !fullyIn {
				var key uint64
				if p.hasKeys {
					key = unpack(keys, j, p.keyWidth)
				} else {
					key = unpack(p.col.packed, doc, p.col.width)
				}
				if !inRange(p.col.enc.value(key), lo, hi, incLo, incHi) {
					continue
				}
			}
			buf = append(buf, uint32(doc)) //nolint:gosec // below numDocs, a uint32
		}
		if len(buf) > 0 {
			result.AddMany(buf)
		}
	}
	return result
}
