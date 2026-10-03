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

// termInfo is what a term's dictionary entry says about its postings.
type termInfo struct {
	ord     uint32 // the term's ordinal in its dictionary
	docFreq uint32
	single  uint32 // the one document, when docFreq is 1
	post    region // the serialized bitmap, when docFreq > 1
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
// over the mapping for a serialized one. See [viewBitmap] for the view's lifetime.
func bitmapAt(data []byte, info termInfo) *roaring.Bitmap {
	if info.docFreq == 0 {
		return roaring.New()
	}
	if info.docFreq == 1 {
		return roaring.BitmapOf(info.single)
	}
	return viewBitmap(data, info.post)
}

// viewBitmap reads the serialized bitmap at r in place, with no copy: its containers
// hold slices of data itself. The result is only valid for as long as data's backing
// mapping stays mapped - callers that return it to package callers (Postings, Present,
// Truncated) document that lifetime there; a caller that needs it to outlive the
// mapping must Clone() it. A region that does not hold one (which a checksummed file
// cannot have) reads as empty.
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

// appendDocs appends a term's documents to dst, in order.
func appendDocs(dst []uint32, data []byte, info termInfo) []uint32 {
	switch info.docFreq {
	case 0:
		return dst
	case 1:
		return append(dst, info.single)
	}
	rb := viewBitmap(data, info.post)
	it := rb.ManyIterator()
	var buf [256]uint32
	for {
		n := it.NextMany(buf[:])
		if n == 0 {
			return dst
		}
		dst = append(dst, buf[:n]...)
	}
}
