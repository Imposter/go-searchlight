package segment

import (
	"bytes"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

// TestPointsRelocatableWithinBuffer pins N5: a point index's block offsets are relative
// to the index's own start, so it reads correctly wherever it lands in its buffer, not
// only at buffer offset 0 (which is all a whole-segment build ever exercised, because
// each field's point index happens to be first in its private buffer).
func TestPointsRelocatableWithinBuffer(t *testing.T) {
	const numDocs = 1000
	values := make([]docFloat, numDocs)
	for i := range values {
		values[i] = docFloat{doc: uint32(i), v: float64(i % 300)}
	}
	enc, st, present := chooseEncoding(numDocs, values)
	var colBuf bytes.Buffer
	cw := newFileWriter(&colBuf)
	colOff := writeNumberColumn(cw, numDocs, enc, st, present, values)
	cw.flush()
	col, err := openNumberColumn(colBuf.Bytes(), colOff, numDocs)
	if err != nil {
		t.Fatal(err)
	}
	pairs := make([]pointPair, len(values))
	for i, dv := range values {
		pairs[i] = pointPair{key: enc.key(dv.v), doc: dv.doc}
	}
	sortPointPairs(pairs)

	for _, lead := range []int{0, 1, 333, 4096} {
		var buf bytes.Buffer
		w := newFileWriter(&buf)
		w.write(bytes.Repeat([]byte{0xEE}, lead))
		off := writeSortedPoints(w, numDocs, pairs)
		w.flush()
		p, err := openPoints(buf.Bytes(), off, col, FormatMajor)
		if err != nil {
			t.Fatalf("lead %d: openPoints: %v", lead, err)
		}
		got := p.rangeDocs(10, 20, true, false)
		want := roaring.New()
		for i := range uint32(numDocs) {
			if v := float64(i % 300); v >= 10 && v < 20 {
				want.Add(i)
			}
		}
		if !got.Equals(want) {
			t.Fatalf("lead %d: rangeDocs = %d docs, want %d", lead, got.GetCardinality(), want.GetCardinality())
		}
	}
}
