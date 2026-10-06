package segment

import (
	"bytes"

	"github.com/RoaringBitmap/roaring/v2"
)

// Postings are stored two ways. A term in one document keeps that document's ordinal
// inline in its term block (most grams of rare words, every id, most unique values),
// which costs a varint instead of a roaring header. Every other term's postings are a
// run-optimized roaring bitmap in the portable serialization, read in place from the
// mapping by roaring's FromBuffer: the containers' data is never copied.

// termInfo is what a term's dictionary entry says about its postings: its ordinal, its
// document frequency, and either its one document (single, when docFreq is 1) or its
// serialized postings (post).
type termInfo struct {
	ord     uint32
	docFreq uint32
	single  uint32
	post    region
}

// postingsEncoder serializes sorted document lists as roaring bitmaps, reusing its
// buffers between terms.
type postingsEncoder struct {
	rb  *roaring.Bitmap
	buf bytes.Buffer
}

func newPostingsEncoder() *postingsEncoder {
	return &postingsEncoder{rb: roaring.New()}
}

// encode returns docs (sorted, distinct, at least two) serialized; the result is valid
// until the next call.
func (e *postingsEncoder) encode(docs []uint32) []byte {
	e.rb.Clear()
	e.rb.AddMany(docs)
	e.rb.RunOptimize()
	e.buf.Reset()
	if _, err := e.rb.WriteTo(&e.buf); err != nil {
		panic(err) // writing to a bytes.Buffer cannot fail
	}
	return e.buf.Bytes()
}

// bitmapAt returns a term's postings: a fresh bitmap for an inline document, or a view
// over the mapping for a serialized one (see [viewBitmap] for the view's lifetime).
// Either way it holds only documents below numDocs, and a serialized one only if it is
// well formed and holds exactly docFreq of them ([checkBitmap]); anything else - which
// only a damaged or crafted file can hold - reads as empty, never as a bitmap whose
// later use could panic or claim documents the segment does not have.
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
	if card, ok := checkBitmap(b, uint64(numDocs)); !ok || card != uint64(info.docFreq) {
		return roaring.New()
	}
	return viewBitmap(data, info.post)
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
	if info.docFreq == 1 {
		if info.single < numDocs {
			dst = append(dst, info.single)
		}
		return dst
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
