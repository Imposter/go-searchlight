package segment

import (
	"math"

	"github.com/RoaringBitmap/roaring/v2"
)

// Doc values are columns indexed by document ordinal, for sorting, aggregations and
// residual filters.
//
// A number column (number, date and bool fields; a bool is 0 or 1) stores each value as
// an order-preserving unsigned key, frame-of-reference bit-packed:
//
//   - int mode: the value times 10^scale is an integer k (scale 0 to maxScale, the
//     smallest that round-trips every value bit for bit), and the key is
//     (k - kmin) / gcd. Integers, dates in milliseconds and decimal prices pack into
//     a few bits each.
//   - float mode: the key is the value's sortable bits minus the smallest's.
//
//	header  u8 mode, u8 scale, u8 width, u8 allPresent, u32 count, f64 min, f64 max,
//	        f64 sum, u64 base, u64 gcd
//	        [u64 words: the has-value bitset, unless allPresent]
//	        packed keys, one per document (0 where there is no value)
//
// A keyword column stores ordinal+1 into the field's KindValue dictionary per document
// (0: no value). A multi column (keyword_list) stores per-document offsets into one
// packed array of KindEntry ordinals, each document's ascending.

const (
	modeInt   = 0
	modeFloat = 1
	maxScale  = 6
	// maxExactInt bounds the scaled integers of int mode, so every key difference fits.
	maxExactInt = 1 << 62
)

var pow10 = [maxScale + 1]float64{1, 10, 100, 1e3, 1e4, 1e5, 1e6}

// numEncoding maps a column's values to order-preserving zero-based keys and back.
type numEncoding struct {
	mode  uint8
	scale uint8
	base  uint64 // int mode: kmin as int64 bits; float mode: the smallest sortable bits
	gcd   uint64
}

func (e numEncoding) key(v float64) uint64 {
	if e.mode == modeFloat {
		return sortableBits(v) - e.base
	}
	k := int64(math.Round(v * pow10[e.scale]))
	return uint64(k-int64(e.base)) / e.gcd //nolint:gosec // k >= kmin, so the difference is non-negative
}

func (e numEncoding) value(key uint64) float64 {
	if e.mode == modeFloat {
		return fromSortableBits(key + e.base)
	}
	k := int64(e.base) + int64(key*e.gcd) //nolint:gosec // inverse of key
	if e.scale == 0 {
		return float64(k)
	}
	return float64(k) / pow10[e.scale]
}

// sortableBits maps a float64 to a uint64 that orders as the float does.
func sortableBits(f float64) uint64 {
	b := math.Float64bits(f)
	if b>>63 != 0 {
		return ^b
	}
	return b | 1<<63
}

func fromSortableBits(k uint64) float64 {
	if k>>63 != 0 {
		return math.Float64frombits(k &^ (1 << 63))
	}
	return math.Float64frombits(^k)
}

// scaledInt returns v times 10^s as an integer, if that is exact and in range.
func scaledInt(v float64, s int) (int64, bool) {
	x := math.Round(v * pow10[s])
	if math.Abs(x) >= maxExactInt {
		return 0, false
	}
	k := int64(x)
	var back float64
	if s == 0 {
		back = float64(k)
	} else {
		back = float64(k) / pow10[s]
	}
	return k, math.Float64bits(back) == math.Float64bits(v)
}

// Stats are a number column's aggregates over the documents that have a value.
type Stats struct {
	Count    uint32
	Min, Max float64
	Sum      float64
}

// chooseEncoding picks the column's encoding and computes its stats and has-value
// bitset (nil when every document has a value), in two passes over values regardless
// of cardinality (down from four, plus a fifth that writeNumberColumn used to make on
// its own to find the packed width - eliminated below by deriving it from Stats.Max
// instead of a separate scan):
//
//   - pass 1 computes the running stats and the smallest scale (0 to maxScale) whose
//     exact-integer representation ([scaledInt]) covers every value seen so far,
//     exactly as before;
//   - pass 2, now that the final scale is fixed, re-validates every value is exact at
//     it (bailing out at the first that is not, straight to float mode) and computes
//     kmin and the encoding's gcd in the same loop, using the first value as the gcd's
//     reference point rather than kmin: gcd(k_i - k_0) equals gcd(k_i - kmin) for any
//     fixed reference drawn from the k_i themselves, a standard fact about the
//     subgroup a finite set of integers' pairwise differences generates, so kmin need
//     not be known until the loop (and hence the pass) is done.
//
// values must be sorted by doc, ascending (as [fieldBuilder.prepareField] leaves them);
// chooseEncoding does not sort them itself.
func chooseEncoding(numDocs uint32, values []docFloat) (numEncoding, Stats, []uint64) {
	st := Stats{Min: math.Inf(1), Max: math.Inf(-1)}
	present := make([]uint64, (uint64(numDocs)+63)/64)
	scale := 0
	for _, dv := range values {
		st.Count++
		st.Min = math.Min(st.Min, dv.v)
		st.Max = math.Max(st.Max, dv.v)
		st.Sum += dv.v
		present[dv.doc/64] |= 1 << (dv.doc % 64)
		for scale <= maxScale {
			if _, ok := scaledInt(dv.v, scale); ok {
				break
			}
			scale++
		}
	}
	if st.Count == numDocs {
		present = nil
	}
	enc := numEncoding{mode: modeFloat, gcd: 1}
	if scale <= maxScale {
		ok := true
		var kmin, k0 int64
		var g uint64
		for i, dv := range values {
			k, exact := scaledInt(dv.v, scale)
			if !exact {
				ok = false
				break
			}
			if i == 0 {
				kmin, k0 = k, k
				continue
			}
			if k < kmin {
				kmin = k
			}
			g = gcd(g, absDiff(k, k0))
		}
		if g == 0 {
			g = 1
		}
		if ok {
			enc = numEncoding{mode: modeInt, scale: uint8(scale), base: uint64(kmin), gcd: g}
		}
	}
	if enc.mode == modeFloat {
		enc.base = sortableBits(st.Min) // math.Min keeps -0 below +0, as the keys order them
	}
	return enc, st, present
}

func gcd(a, b uint64) uint64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

// absDiff returns |a - b| as a uint64, without risking int64 overflow on the subtraction.
func absDiff(a, b int64) uint64 {
	if a >= b {
		return uint64(a - b) //nolint:gosec // a >= b, so the difference is non-negative
	}
	return uint64(b - a) //nolint:gosec // b > a, so the difference is non-negative
}

// writeNumberColumn writes a number column from its already-chosen encoding, stats and
// has-value bitset ([chooseEncoding], normally run once in [fieldBuilder.prepareField]
// rather than here) and returns its offset. values must be sorted by doc, ascending.
func writeNumberColumn(w *fileWriter, numDocs uint32, enc numEncoding, st Stats, present []uint64, values []docFloat) uint64 {
	// The maximum key belongs to the maximum value: both scaledInt's rounding (int
	// mode) and sortableBits (float mode) are monotonic in v, so there is no need to
	// rescan values for it.
	maxKey := enc.key(st.Max)
	width := packedWidth(bitsFor(maxKey))
	off := w.off
	var h encoder
	h.u8(enc.mode)
	h.u8(enc.scale)
	h.u8(width)
	if present == nil {
		h.u8(1)
	} else {
		h.u8(0)
	}
	h.u32(st.Count)
	h.u64(math.Float64bits(st.Min))
	h.u64(math.Float64bits(st.Max))
	h.u64(math.Float64bits(st.Sum))
	h.u64(enc.base)
	h.u64(enc.gcd)
	for _, word := range present {
		h.u64(word)
	}
	w.write(h.b)
	p := newPacker(w, width)
	next := uint32(0)
	for _, dv := range values {
		for ; next < dv.doc; next++ {
			p.add(0)
		}
		p.add(enc.key(dv.v))
		next = dv.doc + 1
	}
	for ; next < numDocs; next++ {
		p.add(0)
	}
	p.finish()
	return off
}

// numberColumn reads a number column in place.
type numberColumn struct {
	enc     numEncoding
	width   uint8
	stats   Stats
	present []byte // has-value bitset words, nil when every document has a value
	packed  []byte
	numDocs uint32
	points  *points
}

func openNumberColumn(data []byte, off uint64, numDocs uint32) (*numberColumn, error) {
	if off > uint64(len(data)) {
		return nil, errShort
	}
	d := decoder{b: data, pos: int(off)} //nolint:gosec // bounded by len(data)
	c := &numberColumn{numDocs: numDocs}
	c.enc.mode = d.u8()
	c.enc.scale = d.u8()
	c.width = d.u8()
	allPresent := d.u8() == 1
	c.stats.Count = d.u32()
	c.stats.Min = math.Float64frombits(d.u64())
	c.stats.Max = math.Float64frombits(d.u64())
	c.stats.Sum = math.Float64frombits(d.u64())
	c.enc.base = d.u64()
	c.enc.gcd = d.u64()
	if !allPresent {
		c.present = d.bytes((uint64(numDocs) + 63) / 64 * 8)
	}
	c.packed = d.bytes(packedSize(uint64(numDocs), c.width))
	if d.err != nil {
		return nil, d.err
	}
	if c.enc.mode > modeFloat || c.enc.scale > maxScale || (c.width > 56 && c.width != 64) || c.enc.gcd == 0 {
		return nil, errShort
	}
	return c, nil
}

func (c *numberColumn) has(ord uint32) bool {
	if ord >= c.numDocs {
		return false
	}
	if c.present == nil {
		return true
	}
	return c.present[ord/8]>>(ord%8)&1 != 0
}

// NumericColumn is a number, date or bool field's values (a bool is 0 or 1), its stats
// and its point index. The zero NumericColumn is a field with no numbers.
type NumericColumn struct{ c *numberColumn }

// Exists reports whether any document has a value.
func (n NumericColumn) Exists() bool { return n.c != nil }

// Value returns document ord's value, if it has one.
func (n NumericColumn) Value(ord uint32) (float64, bool) {
	c := n.c
	if c == nil || !c.has(ord) {
		return 0, false
	}
	return c.enc.value(unpack(c.packed, uint64(ord), c.width)), true
}

// Stats returns the column's count, min, max and sum. A column with no values has a
// zero Count and infinite Min and Max.
func (n NumericColumn) Stats() Stats {
	if n.c == nil {
		return Stats{Min: math.Inf(1), Max: math.Inf(-1)}
	}
	return n.c.stats
}

// Range returns the documents whose value is within [lo, hi], each bound inclusive per
// incLo/incHi, served by the column's point index when it has one, or a full scan
// otherwise.
func (n NumericColumn) Range(lo, hi float64, incLo, incHi bool) *roaring.Bitmap {
	if n.c == nil {
		return roaring.New()
	}
	if n.c.points != nil {
		return n.c.points.rangeDocs(lo, hi, incLo, incHi)
	}
	result := roaring.New()
	for ord := range n.c.numDocs {
		if v, ok := n.Value(ord); ok && inRange(v, lo, hi, incLo, incHi) {
			result.Add(ord)
		}
	}
	return result
}

// keywordSource yields each document's ordinal in ascending document order.
type keywordSource func(yield func(doc, ord uint32))

func writeKeywordColumn(w *fileWriter, numDocs, numTerms uint32, src keywordSource) uint64 {
	width := packedWidth(bitsFor(uint64(numTerms)))
	off := w.off
	w.writeByte(width)
	p := newPacker(w, width)
	next := uint32(0)
	src(func(doc, ord uint32) {
		for ; next < doc; next++ {
			p.add(0)
		}
		p.add(uint64(ord) + 1)
		next = doc + 1
	})
	for ; next < numDocs; next++ {
		p.add(0)
	}
	p.finish()
	return off
}

type keywordColumn struct {
	width   uint8
	packed  []byte
	numDocs uint32
	dict    *termDict
}

func openKeywordColumn(data []byte, off uint64, numDocs uint32, dict *termDict) (*keywordColumn, error) {
	if off >= uint64(len(data)) || dict == nil {
		return nil, errShort
	}
	d := decoder{b: data, pos: int(off)} //nolint:gosec // bounded by len(data)
	c := &keywordColumn{numDocs: numDocs, dict: dict}
	c.width = d.u8()
	c.packed = d.bytes(packedSize(uint64(numDocs), c.width))
	if d.err != nil || (c.width > 56 && c.width != 64) {
		return nil, errShort
	}
	return c, nil
}

// KeywordColumn is a keyword, text or bool field's value per document, as an ordinal
// into the field's sorted KindValue dictionary: ordinals order as the values do, for
// sorting and terms aggregations. The zero KeywordColumn has no values.
type KeywordColumn struct{ c *keywordColumn }

// Exists reports whether the field has the column.
func (k KeywordColumn) Exists() bool { return k.c != nil }

// Ord returns document ord's value ordinal, if it has a value.
func (k KeywordColumn) Ord(doc uint32) (uint32, bool) {
	if k.c == nil || doc >= k.c.numDocs {
		return 0, false
	}
	v := unpack(k.c.packed, uint64(doc), k.c.width)
	if v == 0 {
		return 0, false
	}
	return uint32(v - 1), true //nolint:gosec // written from a uint32
}

// NumTerms returns how many distinct values the column's dictionary holds.
func (k KeywordColumn) NumTerms() uint32 {
	if k.c == nil {
		return 0
	}
	return k.c.dict.numTerms
}

// Term returns the value of ordinal ord.
func (k KeywordColumn) Term(ord uint32) string {
	if k.c == nil {
		return ""
	}
	return string(k.c.dict.termAt(nil, ord))
}

// AppendTerm appends the value of ordinal ord to dst.
func (k KeywordColumn) AppendTerm(dst []byte, ord uint32) []byte {
	if k.c == nil {
		return dst
	}
	return k.c.dict.termAt(dst, ord)
}

// Lookup returns term's ordinal, if any document has it.
func (k KeywordColumn) Lookup(term string) (uint32, bool) {
	if k.c == nil {
		return 0, false
	}
	info, ok := k.c.dict.lookup(stringBytes(term))
	return info.ord, ok
}

// multiSource yields each document's ascending ordinals in ascending document order.
type multiSource func(yield func(doc uint32, ords []uint32))

func writeMultiColumn(w *fileWriter, numDocs, numTerms uint32, src multiSource) uint64 {
	var total uint64
	src(func(_ uint32, ords []uint32) { total += uint64(len(ords)) })
	offWidth := packedWidth(bitsFor(total))
	ordWidth := packedWidth(bitsFor(uint64(numTerms)))
	off := w.off
	var h encoder
	h.u8(offWidth)
	h.u8(ordWidth)
	h.u64(total)
	w.write(h.b)
	p := newPacker(w, offWidth)
	next, at := uint32(0), uint64(0)
	src(func(doc uint32, ords []uint32) {
		for ; next <= doc; next++ {
			p.add(at)
		}
		at += uint64(len(ords))
	})
	for ; next <= numDocs; next++ {
		p.add(at)
	}
	p.finish()
	p = newPacker(w, ordWidth)
	src(func(_ uint32, ords []uint32) {
		for _, o := range ords {
			p.add(uint64(o))
		}
	})
	p.finish()
	return off
}

type multiColumn struct {
	offWidth, ordWidth uint8
	offs, ords         []byte
	numDocs            uint32
	dict               *termDict
}

func openMultiColumn(data []byte, off uint64, numDocs uint32, dict *termDict) (*multiColumn, error) {
	if off >= uint64(len(data)) || dict == nil {
		return nil, errShort
	}
	d := decoder{b: data, pos: int(off)} //nolint:gosec // bounded by len(data)
	c := &multiColumn{numDocs: numDocs, dict: dict}
	c.offWidth = d.u8()
	c.ordWidth = d.u8()
	total := d.u64()
	c.offs = d.bytes(packedSize(uint64(numDocs)+1, c.offWidth))
	c.ords = d.bytes(packedSize(total, c.ordWidth))
	if d.err != nil || (c.offWidth > 56 && c.offWidth != 64) || (c.ordWidth > 56 && c.ordWidth != 64) {
		return nil, errShort
	}
	return c, nil
}

// MultiColumn is a keyword_list field's entries per document, as ascending ordinals
// into the field's sorted KindEntry dictionary. The zero MultiColumn has no values.
type MultiColumn struct{ c *multiColumn }

// Exists reports whether the field has the column.
func (m MultiColumn) Exists() bool { return m.c != nil }

// Ords appends document doc's entry ordinals, ascending, to dst.
func (m MultiColumn) Ords(doc uint32, dst []uint32) []uint32 {
	c := m.c
	if c == nil || doc >= c.numDocs {
		return dst
	}
	from := unpack(c.offs, uint64(doc), c.offWidth)
	to := unpack(c.offs, uint64(doc)+1, c.offWidth)
	for i := from; i < to; i++ {
		dst = append(dst, uint32(unpack(c.ords, i, c.ordWidth))) //nolint:gosec // written from a uint32
	}
	return dst
}

// Count returns how many entries document doc has.
func (m MultiColumn) Count(doc uint32) int {
	c := m.c
	if c == nil || doc >= c.numDocs {
		return 0
	}
	return int(unpack(c.offs, uint64(doc)+1, c.offWidth) - unpack(c.offs, uint64(doc), c.offWidth)) //nolint:gosec // bounded by the entry count
}

// NumTerms returns how many distinct entries the column's dictionary holds.
func (m MultiColumn) NumTerms() uint32 {
	if m.c == nil {
		return 0
	}
	return m.c.dict.numTerms
}

// Term returns the entry of ordinal ord.
func (m MultiColumn) Term(ord uint32) string {
	if m.c == nil {
		return ""
	}
	return string(m.c.dict.termAt(nil, ord))
}

// AppendTerm appends the entry of ordinal ord to dst.
func (m MultiColumn) AppendTerm(dst []byte, ord uint32) []byte {
	if m.c == nil {
		return dst
	}
	return m.c.dict.termAt(dst, ord)
}

// Lookup returns term's ordinal, if any document has the entry.
func (m MultiColumn) Lookup(term string) (uint32, bool) {
	if m.c == nil {
		return 0, false
	}
	info, ok := m.c.dict.lookup(stringBytes(term))
	return info.ord, ok
}
