package segment

import (
	"encoding/binary"
	"math/bits"
)

// Roaring's portable serialization, as roaring.(*Bitmap).FromBuffer reads it:
//
//	cookie   u32: serialCookie | (containers-1)<<16, then a run-flag bit per container
//	         (rounded up to bytes); or serialCookieNoRun, then u32 containers
//	header   per container: u16 key, u16 cardinality-1
//	offsets  per container u32, present unless there are run flags and fewer than
//	         noOffsetThreshold containers (FromBuffer skips them; so does checkBitmap)
//	bodies   run: u16 runs, then per run u16 start, u16 length-1
//	         bitmap (cardinality > 4096): 1024 u64 words
//	         array: cardinality u16 values
const (
	serialCookieNoRun = 12346
	serialCookie      = 12347
	noOffsetThreshold = 4
	arrayMaxCard      = 4096
	bitmapWords       = 1024
)

// checkBitmap reports whether b is exactly one well-formed serialized roaring bitmap
// whose every value is below limit, and its cardinality - in time linear in len(b).
//
// FromBuffer itself checks only that the bytes it slices exist: it accepts an empty
// run container (whose Minimum then indexes past its runs), unsorted or overlapping
// runs and arrays, and a bitmap container whose stated cardinality disagrees with its
// bits, all of which later operations trust. roaring's own Validate catches most of
// that, but only after FromBuffer has built the containers, and its run-container
// check compares every run with every later one - quadratic, so a crafted container of
// 65535 runs would cost billions of comparisons, trading a panic for a hang. This is
// the same set of checks, plus the bound on values (a damaged file's bitmap must not
// claim documents the segment does not have), done in one pass over the bytes.
func checkBitmap(b []byte, limit uint64) (card uint64, ok bool) {
	d := decoder{b: b}
	cookie := d.u32()
	var size uint64
	var runFlags []byte
	switch {
	case d.err != nil:
		return 0, false
	case cookie&0xFFFF == serialCookie:
		size = uint64(cookie>>16) + 1
		runFlags = d.bytes((size + 7) / 8)
	case cookie == serialCookieNoRun:
		size = uint64(d.u32())
	default:
		return 0, false
	}
	if d.err != nil || size > 1<<16 {
		return 0, false
	}
	header := d.bytes(size * 4)
	if runFlags == nil || size >= noOffsetThreshold {
		d.bytes(size * 4)
	}
	if d.err != nil {
		return 0, false
	}
	var maxValue uint64
	prevKey := -1
	for i := range size {
		key := int(binary.LittleEndian.Uint16(header[i*4:]))
		want := uint64(binary.LittleEndian.Uint16(header[i*4+2:])) + 1
		if key <= prevKey {
			return 0, false
		}
		prevKey = key
		var n uint64
		var last int
		switch {
		case len(runFlags) > 0 && runFlags[i/8]&(1<<(i%8)) != 0:
			n, last, ok = checkRuns(&d)
		case want > arrayMaxCard:
			n, last, ok = checkBitmapWords(d.bytes(bitmapWords * 8))
		default:
			n, last, ok = checkArray(d.bytes(want * 2))
		}
		if !ok || d.err != nil || n != want {
			return 0, false
		}
		card += n
		maxValue = uint64(key)<<16 | uint64(last) //nolint:gosec // last is a 16-bit value
	}
	if d.pos != len(b) || (size > 0 && maxValue >= limit) {
		return 0, false
	}
	return card, true
}

// checkRuns reads one run container's body: at least one run, each within the 16-bit
// range, ascending and disjoint.
func checkRuns(d *decoder) (card uint64, last int, ok bool) {
	lenBytes := d.bytes(2)
	if d.err != nil {
		return 0, 0, false
	}
	nr := uint64(binary.LittleEndian.Uint16(lenBytes))
	runs := d.bytes(nr * 4)
	if d.err != nil || nr == 0 {
		return 0, 0, false
	}
	last = -1
	for j := range nr {
		start := int(binary.LittleEndian.Uint16(runs[j*4:]))
		end := start + int(binary.LittleEndian.Uint16(runs[j*4+2:]))
		if start <= last || end > 0xFFFF {
			return 0, 0, false
		}
		card += uint64(end - start + 1) //nolint:gosec // end >= start
		last = end
	}
	return card, last, true
}

// checkBitmapWords reads one bitmap container's body: its cardinality is its popcount.
func checkBitmapWords(words []byte) (card uint64, last int, ok bool) {
	if len(words) != bitmapWords*8 {
		return 0, 0, false
	}
	last = -1
	for w := range bitmapWords {
		word := binary.LittleEndian.Uint64(words[w*8:])
		if word != 0 {
			card += uint64(bits.OnesCount64(word)) //nolint:gosec // a popcount, 0 to 64
			last = w*64 + 63 - bits.LeadingZeros64(word)
		}
	}
	return card, last, last >= 0
}

// checkArray reads one array container's body: strictly ascending values.
func checkArray(vals []byte) (card uint64, last int, ok bool) {
	if len(vals) == 0 {
		return 0, 0, false
	}
	last = -1
	for j := 0; j < len(vals); j += 2 {
		v := int(binary.LittleEndian.Uint16(vals[j:]))
		if v <= last {
			return 0, 0, false
		}
		last = v
	}
	return uint64(len(vals)) / 2, last, true
}
