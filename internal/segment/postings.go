package segment

import (
	"bytes"
	"encoding/binary"
	"math/bits"

	"github.com/RoaringBitmap/roaring/v2"
)

// Postings are stored three ways. A term in one document keeps that document's ordinal
// inline in its term block (most grams of rare words, every id, most unique values),
// which costs a varint instead of a roaring header. Every other term's postings are
// either a run-optimized roaring bitmap in the portable serialization, read in place
// from the mapping by roaring's FromBuffer (the containers' data is never copied), or,
// for a word or gram term in format 4 when that is at least a fifth smaller, an
// Elias-Fano list:
//
//	u8 low width L, the low L bits of every document bit-packed (LSB first), then the
//	high parts in unary: for the i-th document, bit (doc >> L) + i is set
//
// which costs about 2 + log2(span/docFreq) bits a document, against roaring's 16 for a
// sparse container and 65536 per container for a dense one: the band between (a term
// in roughly 1 to 30 percent of the documents) is where most gram and word postings
// fall. An Elias-Fano list is decoded into a fresh bitmap on each read, so value and
// entry terms, the filters' hot path, always stay roaring views.

// Postings codecs: the low bit of a format-4 entry's postings length.
const (
	codecRoaring = 0
	codecEF      = 1
)

// termInfo is what a term's dictionary entry says about its postings.
type termInfo struct {
	ord     uint32 // the term's ordinal in its dictionary
	docFreq uint32
	single  uint32 // the one document, when docFreq is 1
	post    region // the serialized postings, when docFreq > 1
	codec   uint8  // how post is coded
}

// postingsEncoder serializes sorted document lists, reusing its buffers between terms.
type postingsEncoder struct {
	rb  *roaring.Bitmap
	buf bytes.Buffer
	ef  []byte
}

func newPostingsEncoder() *postingsEncoder {
	return &postingsEncoder{rb: roaring.New()}
}

// encode returns docs (sorted, distinct, at least two) serialized, and the codec: a
// roaring bitmap, or with ef an Elias-Fano list when that is at least a fifth smaller.
// The result is valid until the next call.
func (e *postingsEncoder) encode(docs []uint32, ef bool) ([]byte, uint8) {
	e.rb.Clear()
	e.rb.AddMany(docs)
	e.rb.RunOptimize()
	e.buf.Reset()
	if _, err := e.rb.WriteTo(&e.buf); err != nil {
		panic(err) // writing to a bytes.Buffer cannot fail
	}
	if ef && efSize(docs)*5 <= uint64(e.buf.Len())*4 { //nolint:gosec // a length, non-negative
		e.ef = appendEF(e.ef[:0], docs)
		return e.ef, codecEF
	}
	return e.buf.Bytes(), codecRoaring
}

// efLowWidth is the low width of n documents up to last: floor(log2(last/n)), or 0.
func efLowWidth(n, last uint64) uint {
	if q := last / n; q > 0 {
		return uint(bits.Len64(q) - 1)
	}
	return 0
}

// efSize is the bytes appendEF writes for docs.
func efSize(docs []uint32) uint64 {
	n, last := uint64(len(docs)), uint64(docs[len(docs)-1])
	l := efLowWidth(n, last)
	return 1 + (n*uint64(l)+7)/8 + ((last>>l)+n+7)/8
}

// appendEF appends docs (sorted, distinct, at least one) Elias-Fano coded to dst.
func appendEF(dst []byte, docs []uint32) []byte {
	n, last := uint64(len(docs)), uint64(docs[len(docs)-1])
	l := efLowWidth(n, last)
	dst = append(dst, byte(l)) //nolint:gosec // at most 31: docs are uint32
	mask := uint64(1)<<l - 1
	var acc uint64
	var nbits uint
	for _, d := range docs {
		acc |= (uint64(d) & mask) << nbits
		nbits += l
		for nbits >= 8 {
			dst = append(dst, byte(acc))
			acc >>= 8
			nbits -= 8
		}
	}
	if nbits > 0 {
		dst = append(dst, byte(acc))
	}
	start := len(dst)
	dst = append(dst, make([]byte, ((last>>l)+n+7)/8)...)
	for i, d := range docs {
		p := uint64(d)>>l + uint64(i)
		dst[start+int(p>>3)] |= 1 << (p & 7)
	}
	return dst
}

// appendEFDocs decodes an Elias-Fano list of n documents and appends them to dst. False
// when it is malformed: shorter than its low part, holding fewer than n high bits, or
// with a document out of ascending order or at or past numDocs.
func appendEFDocs(b []byte, n, numDocs uint32, dst []uint32) ([]uint32, bool) {
	if len(b) == 0 || b[0] > 32 {
		return dst, false
	}
	l := uint(b[0])
	lowBytes := (uint64(n)*uint64(l) + 7) / 8
	if lowBytes > uint64(len(b)-1) { //nolint:gosec // len(b) > 0, checked above
		return dst, false
	}
	low, high := b[1:1+lowBytes], b[1+lowBytes:]
	if uint64(n) > uint64(len(high))*8 {
		return dst, false
	}
	dst = slicesGrow(dst, int(n))
	var i uint64
	next := uint64(0) // the smallest the next document may be
	for wi := 0; wi < len(high) && i < uint64(n); wi += 8 {
		w := loadWord(high, wi)
		for w != 0 && i < uint64(n) {
			p := uint64(wi)*8 + uint64(bits.TrailingZeros64(w)) //nolint:gosec // wi and a bit index, non-negative
			w &= w - 1
			doc := (p-i)<<l | bitsAt(low, i*uint64(l), l)
			if doc < next || doc >= uint64(numDocs) {
				return dst, false
			}
			dst = append(dst, uint32(doc)) //nolint:gosec // below numDocs, a uint32
			next = doc + 1
			i++
		}
	}
	return dst, i == uint64(n)
}

func slicesGrow(s []uint32, n int) []uint32 {
	if cap(s)-len(s) >= n {
		return s
	}
	out := make([]uint32, len(s), len(s)+n)
	copy(out, s)
	return out
}

// loadWord reads up to eight bytes of b from i as a little-endian word.
func loadWord(b []byte, i int) uint64 {
	if i+8 <= len(b) {
		return binary.LittleEndian.Uint64(b[i:])
	}
	var w uint64
	for k := len(b) - 1; k >= i; k-- {
		w = w<<8 | uint64(b[k])
	}
	return w
}

// bitsAt returns the width bits (at most 32) of b at bit position pos, LSB first.
func bitsAt(b []byte, pos uint64, width uint) uint64 {
	if width == 0 {
		return 0
	}
	return loadWord(b, int(pos>>3)) >> (pos & 7) & (1<<width - 1)
}

// bitmapOfSorted returns docs (sorted, distinct) as a bitmap, building roaring's
// portable serialization directly instead of adding one document at a time.
func bitmapOfSorted(docs []uint32) *roaring.Bitmap {
	const (
		cookieNoRuns  = 12346
		arrayMaxCard  = 4096
		bitmapBytes   = 8192
		headerPerCont = 8 // key, cardinality-1, offset
	)
	rb := roaring.New()
	if len(docs) == 0 {
		return rb
	}
	containers, size := 0, 8
	for i := 0; i < len(docs); {
		j := i
		for j < len(docs) && docs[j]>>16 == docs[i]>>16 {
			j++
		}
		containers++
		size += headerPerCont
		if j-i > arrayMaxCard {
			size += bitmapBytes
		} else {
			size += 2 * (j - i)
		}
		i = j
	}
	buf := make([]byte, size)
	binary.LittleEndian.PutUint32(buf, cookieNoRuns)
	binary.LittleEndian.PutUint32(buf[4:], uint32(containers))
	desc, offs := buf[8:], buf[8+4*containers:]
	at := 8 + headerPerCont*containers
	c := 0
	for i := 0; i < len(docs); {
		j := i
		for j < len(docs) && docs[j]>>16 == docs[i]>>16 {
			j++
		}
		binary.LittleEndian.PutUint16(desc[4*c:], uint16(docs[i]>>16))
		binary.LittleEndian.PutUint16(desc[4*c+2:], uint16(j-i-1)) //nolint:gosec // at most 65535
		binary.LittleEndian.PutUint32(offs[4*c:], uint32(at))
		if j-i > arrayMaxCard {
			words := buf[at : at+bitmapBytes]
			for _, d := range docs[i:j] {
				lo := d & 0xffff
				words[lo>>3] |= 1 << (lo & 7)
			}
			at += bitmapBytes
		} else {
			for _, d := range docs[i:j] {
				binary.LittleEndian.PutUint16(buf[at:], uint16(d)) //nolint:gosec // the low 16 bits
				at += 2
			}
		}
		c++
		i = j
	}
	if _, err := rb.FromBuffer(buf); err != nil {
		panic(err) // the serialization is built above, well formed by construction
	}
	return rb
}

// bitmapAt returns a term's postings: a fresh bitmap for an inline document or an
// Elias-Fano list, or a view over the mapping for a roaring one (see [viewBitmap] for
// the view's lifetime). Either way it holds only documents below numDocs, and a
// serialized one only if it is well formed and holds exactly docFreq of them
// ([checkBitmap], [appendEFDocs]); anything else - which only a damaged or crafted file
// can hold - reads as empty, never as a bitmap whose later use could panic or claim
// documents the segment does not have.
func bitmapAt(data []byte, info termInfo, numDocs uint32) *roaring.Bitmap {
	switch info.docFreq {
	case 0:
		return roaring.New()
	case 1:
		if info.single >= numDocs {
			return roaring.New()
		}
		return roaring.BitmapOf(info.single)
	}
	b, ok := info.post.slice(data)
	if !ok {
		return roaring.New()
	}
	switch info.codec {
	case codecEF:
		docs, ok := appendEFDocs(b, info.docFreq, numDocs, nil)
		if !ok {
			return roaring.New()
		}
		return bitmapOfSorted(docs)
	case codecRoaring:
		if card, ok := checkBitmap(b, uint64(numDocs)); !ok || card != uint64(info.docFreq) {
			return roaring.New()
		}
		return viewBitmap(data, info.post)
	}
	return roaring.New()
}

// viewBitmap reads the serialized bitmap at r in place, with no copy: its containers
// hold slices of data itself. The result is only valid for as long as data's backing
// mapping stays mapped - callers that return it to package callers (Postings, Present,
// Truncated, Untyped) document that lifetime there; a caller that needs it to outlive
// the mapping must Clone() it.
//
// viewBitmap trusts r to hold a well-formed bitmap: every caller has checked it with
// [checkBitmap] first - bitmapAt on each call, Reader.parseMeta once at Open for the
// presence, truncated and untyped bitmaps. A region outside data, or one FromBuffer
// refuses, still reads as empty.
func viewBitmap(data []byte, r region) *roaring.Bitmap {
	rb := roaring.New()
	b, ok := r.slice(data)
	if !ok || r.n == 0 {
		return rb
	}
	if _, err := rb.FromBuffer(b); err != nil {
		return roaring.New()
	}
	return rb
}

// appendDocs appends a term's documents, ascending, to dst. numDocs bounds them as it
// does for [bitmapAt]: a damaged file's postings append nothing.
func appendDocs(data []byte, info termInfo, numDocs uint32, dst []uint32) []uint32 {
	switch {
	case info.docFreq == 1:
		if info.single < numDocs {
			dst = append(dst, info.single)
		}
		return dst
	case info.docFreq > 1 && info.codec == codecEF:
		b, ok := info.post.slice(data)
		if !ok {
			return dst
		}
		start := len(dst)
		out, ok := appendEFDocs(b, info.docFreq, numDocs, dst)
		if !ok {
			return out[:start]
		}
		return out
	}
	it := bitmapAt(data, info, numDocs).ManyIterator()
	var buf [256]uint32
	for {
		n := it.NextMany(buf[:])
		if n == 0 {
			return dst
		}
		dst = append(dst, buf[:n]...)
	}
}
