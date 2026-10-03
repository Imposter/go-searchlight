package segment

import (
	"cmp"
	"encoding/binary"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
)

// Points are a BKD-lite range index for one number column: its (key, doc) pairs sorted
// by key, split into fixed-size blocks, each with its key range recorded so a Range
// query can skip whole blocks and add a fully-covered block's documents without
// inspecting them.
//
//	header  u32 numEntries, u32 numBlocks, u8 keyWidth, u8 docWidth
//	table   per block: u64 minKey, u64 maxKey, u32 count, u64 blockOffset (absolute)
//	blocks  per block: packed keys (count, keyWidth), packed docs (count, docWidth)
const pointsBlockSize = 128

type pointPair struct {
	key uint64
	doc uint32
}

// writePoints writes src's (key, doc) pairs sorted by key and returns the index's
// offset. src must yield the same (doc, value) pairs as the column built with enc.
func writePoints(w *fileWriter, numDocs uint32, enc numEncoding, src numberSource) uint64 {
	var pairs []pointPair
	src(func(doc uint32, v float64) {
		pairs = append(pairs, pointPair{key: enc.key(v), doc: doc})
	})
	slices.SortFunc(pairs, func(a, b pointPair) int {
		if c := cmp.Compare(a.key, b.key); c != 0 {
			return c
		}
		return cmp.Compare(a.doc, b.doc)
	})
	n := len(pairs)
	numBlocks := (n + pointsBlockSize - 1) / pointsBlockSize
	var maxKey uint64
	for _, p := range pairs {
		maxKey = max(maxKey, p.key)
	}
	keyWidth := packedWidth(bitsFor(maxKey))
	docWidth := packedWidth(bitsFor(uint64(numDocs)))

	off := w.off
	const headerLen = 4 + 4 + 1 + 1
	const tableEntryLen = 8 + 8 + 4 + 8
	blocksStart := off + headerLen + uint64(numBlocks)*tableEntryLen

	var h encoder
	h.u32(uint32(n))         //nolint:gosec // n fits uint32 ordinal space
	h.u32(uint32(numBlocks)) //nolint:gosec // bounded by n
	h.u8(keyWidth)
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
		blockOff += packedSize(uint64(count), keyWidth) + packedSize(uint64(count), docWidth)
	}
	for i := range numBlocks {
		lo := i * pointsBlockSize
		hi := min(lo+pointsBlockSize, n)
		pk := newPacker(w, keyWidth)
		for _, p := range pairs[lo:hi] {
			pk.add(p.key)
		}
		pk.finish()
		pd := newPacker(w, docWidth)
		for _, p := range pairs[lo:hi] {
			pd.add(uint64(p.doc))
		}
		pd.finish()
	}
	return off
}

// points reads a BKD-lite index in place.
type points struct {
	data       []byte
	enc        numEncoding
	numEntries uint32
	numBlocks  uint32
	keyWidth   uint8
	docWidth   uint8
	table      []byte // numBlocks table entries
}

func openPoints(data []byte, off uint64, enc numEncoding) (*points, error) {
	if off > uint64(len(data)) {
		return nil, errShort
	}
	d := decoder{b: data, pos: int(off)} //nolint:gosec // bounded by len(data)
	p := &points{data: data, enc: enc}
	p.numEntries = d.u32()
	p.numBlocks = d.u32()
	p.keyWidth = d.u8()
	p.docWidth = d.u8()
	p.table = d.bytes(uint64(p.numBlocks) * 28)
	if d.err != nil {
		return nil, d.err
	}
	if (p.keyWidth > 56 && p.keyWidth != 64) || (p.docWidth > 56 && p.docWidth != 64) {
		return nil, errShort
	}
	wantBlocks := (p.numEntries + pointsBlockSize - 1) / pointsBlockSize
	if p.numEntries > 0 && p.numBlocks != wantBlocks {
		return nil, errShort
	}
	return p, nil
}

func (p *points) blockRange(i uint32) (minKey, maxKey uint64, count uint32, off uint64) {
	e := p.table[i*28:]
	return binary.LittleEndian.Uint64(e), binary.LittleEndian.Uint64(e[8:]), binary.LittleEndian.Uint32(e[16:]), binary.LittleEndian.Uint64(e[20:])
}

// inRange reports whether v is within [lo, hi], each bound inclusive per incLo/incHi.
func inRange(v, lo, hi float64, incLo, incHi bool) bool {
	okLo := v > lo || (incLo && v == lo)
	okHi := v < hi || (incHi && v == hi)
	return okLo && okHi
}

// rangeDocs returns the documents whose value is within [lo, hi].
func (p *points) rangeDocs(lo, hi float64, incLo, incHi bool) *roaring.Bitmap {
	result := roaring.New()
	if p == nil {
		return result
	}
	for i := range p.numBlocks {
		minKey, maxKey, count, off := p.blockRange(i)
		bmin, bmax := p.enc.value(minKey), p.enc.value(maxKey)
		if bmin > hi || (bmin == hi && !incHi) {
			break // blocks ascend by key: nothing further can be in range
		}
		if bmax < lo || (bmax == lo && !incLo) {
			continue
		}
		fullyIn := inRange(bmin, lo, hi, incLo, incHi) && inRange(bmax, lo, hi, incLo, incHi)
		keyBytes := packedSize(uint64(count), p.keyWidth)
		keys := p.data[off : off+keyBytes]
		docs := p.data[off+keyBytes : off+keyBytes+packedSize(uint64(count), p.docWidth)]
		for j := range uint64(count) {
			doc := uint32(unpack(docs, j, p.docWidth)) //nolint:gosec // written from a uint32
			if fullyIn {
				result.Add(doc)
				continue
			}
			v := p.enc.value(unpack(keys, j, p.keyWidth))
			if inRange(v, lo, hi, incLo, incHi) {
				result.Add(doc)
			}
		}
	}
	return result
}
