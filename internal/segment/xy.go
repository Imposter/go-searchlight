package segment

import (
	"encoding/binary"
	"math"
	"slices"

	"github.com/RoaringBitmap/roaring/v2"
)

// The XY section holds the point fields' indexes ([schema.Point]): per field, a static
// k-d tree over its (x, y, document) triples, as Lucene's BKD tree indexes XYPoint.
//
// The triples are split into leaves of xyLeafSize, the last one shorter. The tree is
// over the leaves: a node covers a run of them, [l, r), and splits it in two at
// l + ceil((r-l)/2), its points partitioned along the axis whose spread, relative to
// the whole field's on that axis, is the wider, so that each half's points are all
// at most (on that axis) the other's. Every node records the box its points span, so a
// box query visits only the nodes its box meets, takes a node it covers whole without
// reading a coordinate, and reads the coordinates of the leaves it meets in part.
//
// Coordinates are kept as sortable keys (xyKey): a float64's bits turned so that
// unsigned order is numeric order, -0 read as 0.
//
//	section  per field its tree, then the directory: u32 count, per field its name
//	         (uvarint length, bytes) and u64 tree offset (relative to the section's
//	         start); then u64 the directory's offset (relative to the section's start)
//	tree     u32 numPoints, u32 numLeaves, u32 numNodes, u8 docWidth
//	nodes    numNodes entries of u64 minX, maxX, minY, maxY, by heap index (the root
//	         is 1, node k's halves 2k and 2k+1; slot 0 and the slots the tree's shape
//	         leaves unused are zero)
//	leafOffs numLeaves u64 offsets, relative to the tree's start
//	leaf     u64 minX, u64 minY, u8 xWidth, u8 yWidth, u8 sortedBy (0 x, 1 y), then
//	         packed x - minX (count, xWidth), packed y - minY (count, yWidth), packed
//	         docs (count, docWidth)
//
// A leaf's points are sorted by the axis it spreads wider on (relative to the field's
// spread, as the nodes split), then the other, then document: a leaf a box cuts on that
// axis alone, as most leaves along a one-sided bound are, is two binary searches and a
// run of documents, with no coordinate of the other axis read.
//
// A segment with no point field has no XY section, and is written as format 4.0; one
// with an XY section is 4.1 (FormatMinor), whose addition a 4.0 reader ignores.
const xyLeafSize = 512

const (
	xyTreeHeaderLen = 4 + 4 + 4 + 1
	xyNodeLen       = 4 * 8
	xyLeafHeaderLen = 8 + 8 + 1 + 1 + 1
)

// xyPoint is one point of a field: its coordinates' keys and its document.
type xyPoint struct {
	x, y uint64
	doc  uint32
}

// xyKey is f as a sortable key: unsigned order of keys is numeric order of finite
// floats, and -0 is 0.
func xyKey(f float64) uint64 {
	if f == 0 {
		f = 0
	}
	b := math.Float64bits(f)
	if b>>63 != 0 {
		return ^b
	}
	return b | 1<<63
}

// xyValue is the float64 key k stands for.
func xyValue(k uint64) float64 {
	if k>>63 != 0 {
		return math.Float64frombits(k &^ (1 << 63))
	}
	return math.Float64frombits(^k)
}

// compareXY orders points by document, then x, then y: the canonical order a tree is
// built from, so that a Build and a Merge of the same documents write the same bytes.
func compareXY(a, b xyPoint) int {
	switch {
	case a.doc != b.doc:
		if a.doc < b.doc {
			return -1
		}
		return 1
	case a.x != b.x:
		if a.x < b.x {
			return -1
		}
		return 1
	case a.y != b.y:
		if a.y < b.y {
			return -1
		}
		return 1
	}
	return 0
}

// xyNumNodes is the heap slots a tree of numLeaves leaves uses: its highest node's
// index, plus one.
func xyNumNodes(numLeaves int) int {
	if numLeaves == 0 {
		return 0
	}
	most := 0
	var walk func(k, l, r int)
	walk = func(k, l, r int) {
		most = max(most, k)
		if r-l <= 1 {
			return
		}
		m := l + (r-l+1)/2
		walk(2*k, l, m)
		walk(2*k+1, m, r)
	}
	walk(1, 0, numLeaves)
	return most + 1
}

// xyBuilder partitions a field's points into the tree's leaves and records each node's
// box.
type xyBuilder struct {
	pts                  []xyPoint
	nodes                []uint64 // 4 per heap slot
	rootSpanX, rootSpanY float64
}

// span is how far apart keys lo and hi lie, halved so that no finite pair overflows.
func span(lo, hi uint64) float64 { return xyValue(hi)/2 - xyValue(lo)/2 }

func (b *xyBuilder) bounds(lo, hi int) (minX, maxX, minY, maxY uint64) {
	minX, minY = math.MaxUint64, math.MaxUint64
	for _, p := range b.pts[lo:hi] {
		minX, maxX = min(minX, p.x), max(maxX, p.x)
		minY, maxY = min(minY, p.y), max(maxY, p.y)
	}
	return minX, maxX, minY, maxY
}

func (b *xyBuilder) build(k, l, r int) {
	lo, hi := l*xyLeafSize, min(r*xyLeafSize, len(b.pts))
	minX, maxX, minY, maxY := b.bounds(lo, hi)
	copy(b.nodes[4*k:], []uint64{minX, maxX, minY, maxY})
	if k == 1 {
		b.rootSpanX, b.rootSpanY = span(minX, maxX), span(minY, maxY)
	}
	if r-l <= 1 {
		return
	}
	m := l + (r-l+1)/2
	xySelect(b.pts[lo:hi], m*xyLeafSize-lo, b.widerOnY(minX, maxX, minY, maxY))
	b.build(2*k, l, m)
	b.build(2*k+1, m, r)
}

// widerOnY reports whether a box of points spreads wider on y than on x, each relative
// to the field's whole spread on that axis (an axis the field does not spread on is
// never the wider).
func (b *xyBuilder) widerOnY(minX, maxX, minY, maxY uint64) bool {
	if b.rootSpanX > 0 && b.rootSpanY > 0 {
		return span(minY, maxY)/b.rootSpanY > span(minX, maxX)/b.rootSpanX
	}
	return b.rootSpanX == 0
}

// sortLeaf sorts a leaf's points by x (y when byY), then the other axis, then document.
func sortLeaf(p []xyPoint, byY bool) {
	slices.SortFunc(p, func(a, b xyPoint) int {
		if byY {
			return cmpKeys(a.y, b.y, a.x, b.x, a.doc, b.doc)
		}
		return cmpKeys(a.x, b.x, a.y, b.y, a.doc, b.doc)
	})
}

func cmpKeys(ax, bx, ay, by uint64, ad, bd uint32) int {
	switch {
	case ax != bx:
		if ax < bx {
			return -1
		}
		return 1
	case ay != by:
		if ay < by {
			return -1
		}
		return 1
	case ad != bd:
		if ad < bd {
			return -1
		}
		return 1
	}
	return 0
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// xySelect reorders p so that p[k] holds the point that would be there were p sorted
// by x (by y when byY), every point before it at most it and every point after it at
// least it: a quickselect with a median-of-three pivot, which falls back to sorting
// what is left after too many rounds.
func xySelect(p []xyPoint, k int, byY bool) {
	key := func(q *xyPoint) uint64 {
		if byY {
			return q.y
		}
		return q.x
	}
	lo, hi := 0, len(p)-1
	for rounds := 0; hi > lo; rounds++ {
		if hi-lo < 16 || rounds > 64 {
			slices.SortFunc(p[lo:hi+1], func(a, b xyPoint) int {
				ka, kb := key(&a), key(&b)
				switch {
				case ka < kb:
					return -1
				case ka > kb:
					return 1
				}
				return 0
			})
			return
		}
		mid := lo + (hi-lo)/2
		if key(&p[mid]) < key(&p[lo]) {
			p[mid], p[lo] = p[lo], p[mid]
		}
		if key(&p[hi]) < key(&p[lo]) {
			p[hi], p[lo] = p[lo], p[hi]
		}
		if key(&p[hi]) < key(&p[mid]) {
			p[hi], p[mid] = p[mid], p[hi]
		}
		pivot := key(&p[mid])
		i, j := lo, hi
		for i <= j {
			for key(&p[i]) < pivot {
				i++
			}
			for key(&p[j]) > pivot {
				j--
			}
			if i <= j {
				p[i], p[j] = p[j], p[i]
				i++
				j--
			}
		}
		switch {
		case k <= j:
			hi = j
		case k >= i:
			lo = i
		default:
			return
		}
	}
}

// writeXYTree writes one field's tree of pts (reordered as it is built) into w. pts
// must be in canonical order (compareXY) and distinct, so the bytes depend only on the
// points.
func writeXYTree(w *fileWriter, numDocs uint32, pts []xyPoint) {
	n := len(pts)
	numLeaves := (n + xyLeafSize - 1) / xyLeafSize
	numNodes := xyNumNodes(numLeaves)
	b := &xyBuilder{pts: pts, nodes: make([]uint64, 4*numNodes)}
	if numLeaves > 0 {
		b.build(1, 0, numLeaves)
	}
	docWidth := packedWidth(bitsFor(uint64(numDocs)))

	var h encoder
	h.u32(uint32(n))         //nolint:gosec // points of one segment's documents
	h.u32(uint32(numLeaves)) //nolint:gosec // bounded by n
	h.u32(uint32(numNodes))  //nolint:gosec // at most 4 * numLeaves
	h.u8(docWidth)
	for _, v := range b.nodes {
		h.u64(v)
	}
	w.write(h.b)

	type leafShape struct {
		minX, minY   uint64
		xw, yw       uint8
		byY          bool
		lo, hi, size int
	}
	shapes := make([]leafShape, numLeaves)
	rel := uint64(xyTreeHeaderLen) + uint64(numNodes)*xyNodeLen + uint64(numLeaves)*8 //nolint:gosec // counts, never negative
	var t encoder
	for i := range numLeaves {
		lo, hi := i*xyLeafSize, min((i+1)*xyLeafSize, n)
		minX, maxX, minY, maxY := b.bounds(lo, hi)
		s := leafShape{minX: minX, minY: minY, xw: packedWidth(bitsFor(maxX - minX)), yw: packedWidth(bitsFor(maxY - minY)), lo: lo, hi: hi}
		s.byY = b.widerOnY(minX, maxX, minY, maxY)
		sortLeaf(pts[lo:hi], s.byY)
		count := uint64(hi - lo)                                                                                        //nolint:gosec // at most xyLeafSize
		s.size = int(xyLeafHeaderLen + packedSize(count, s.xw) + packedSize(count, s.yw) + packedSize(count, docWidth)) //nolint:gosec // a leaf's bytes
		shapes[i] = s
		t.u64(rel)
		rel += uint64(s.size) //nolint:gosec // a leaf's bytes
	}
	w.write(t.b)
	for _, s := range shapes {
		var lh encoder
		lh.u64(s.minX)
		lh.u64(s.minY)
		lh.u8(s.xw)
		lh.u8(s.yw)
		lh.u8(boolByte(s.byY))
		w.write(lh.b)
		px := newPacker(w, s.xw)
		for _, p := range pts[s.lo:s.hi] {
			px.add(p.x - s.minX)
		}
		px.finish()
		py := newPacker(w, s.yw)
		for _, p := range pts[s.lo:s.hi] {
			py.add(p.y - s.minY)
		}
		py.finish()
		pd := newPacker(w, docWidth)
		for _, p := range pts[s.lo:s.hi] {
			pd.add(uint64(p.doc))
		}
		pd.finish()
	}
}

// canonicalXY sorts pts into canonical order and drops repeats.
func canonicalXY(pts []xyPoint) []xyPoint {
	slices.SortFunc(pts, compareXY)
	return slices.Compact(pts)
}

// xyIndex reads one field's tree in place; base is its absolute start.
type xyIndex struct {
	data      []byte
	base      uint64
	numDocs   uint32
	numPoints uint32
	numLeaves uint32
	numNodes  uint32
	docWidth  uint8
	nodes     []byte
	leafOffs  []byte
}

// openXY opens the tree at off, for a segment of numDocs documents. Its tables, and
// every leaf's header and packed data, must lie inside data, its node count be the one
// its leaf count gives, and its leaf count the one its point count gives: the reads
// that follow slice them unchecked.
func openXY(data []byte, off uint64, numDocs uint32) (*xyIndex, error) {
	if off > uint64(len(data)) {
		return nil, errShort
	}
	d := decoder{b: data, pos: int(off)} //nolint:gosec // bounded by len(data)
	t := &xyIndex{data: data, base: off, numDocs: numDocs}
	t.numPoints = d.u32()
	t.numLeaves = d.u32()
	t.numNodes = d.u32()
	t.docWidth = d.u8()
	t.nodes = d.bytes(uint64(t.numNodes) * xyNodeLen)
	t.leafOffs = d.bytes(uint64(t.numLeaves) * 8)
	if d.err != nil {
		return nil, d.err
	}
	if t.docWidth > 56 && t.docWidth != 64 {
		return nil, errShort
	}
	if uint64(t.numLeaves) != (uint64(t.numPoints)+xyLeafSize-1)/xyLeafSize || int(t.numNodes) != xyNumNodes(int(t.numLeaves)) {
		return nil, errShort
	}
	room := uint64(len(data)) - off
	for i := range t.numLeaves {
		rel := binary.LittleEndian.Uint64(t.leafOffs[8*i:])
		if rel > room || room-rel < xyLeafHeaderLen {
			return nil, errShort
		}
		at := off + rel
		xw, yw := data[at+16], data[at+17]
		if (xw > 56 && xw != 64) || (yw > 56 && yw != 64) || data[at+18] > 1 {
			return nil, errShort
		}
		count := uint64(t.leafCount(i))
		size := xyLeafHeaderLen + packedSize(count, xw) + packedSize(count, yw) + packedSize(count, t.docWidth)
		if size > room-rel {
			return nil, errShort
		}
	}
	return t, nil
}

func (t *xyIndex) leafCount(i uint32) uint32 {
	if i+1 < t.numLeaves {
		return xyLeafSize
	}
	return t.numPoints - i*xyLeafSize
}

// node returns node k's box.
func (t *xyIndex) node(k int) (minX, maxX, minY, maxY uint64) {
	e := t.nodes[k*xyNodeLen:]
	return binary.LittleEndian.Uint64(e), binary.LittleEndian.Uint64(e[8:]), binary.LittleEndian.Uint64(e[16:]), binary.LittleEndian.Uint64(e[24:])
}

// leaf is one leaf's packed parts.
type xyLeaf struct {
	minX, minY uint64
	xw, yw, dw uint8
	byY        bool
	xs, ys, ds []byte
	count      uint64
}

// x and y are point j's keys.
func (l *xyLeaf) x(j uint64) uint64 { return l.minX + unpack(l.xs, j, l.xw) }
func (l *xyLeaf) y(j uint64) uint64 { return l.minY + unpack(l.ys, j, l.yw) }
func (l *xyLeaf) doc(j uint64) uint32 {
	return uint32(unpack(l.ds, j, l.dw)) //nolint:gosec // docWidth bits, checked against numDocs where they are used
}

// run returns the points [i, j) whose key on the axis the leaf is sorted by lies in
// [lo, hi]: two binary searches.
func (l *xyLeaf) run(lo, hi uint64) (i, j uint64) {
	key := l.x
	if l.byY {
		key = l.y
	}
	search := func(f func(uint64) bool) uint64 {
		a, b := uint64(0), l.count
		for a < b {
			m := a + (b-a)/2
			if f(m) {
				b = m
			} else {
				a = m + 1
			}
		}
		return a
	}
	return search(func(m uint64) bool { return key(m) >= lo }), search(func(m uint64) bool { return key(m) > hi })
}

func (t *xyIndex) leaf(i uint32) xyLeaf {
	at := t.base + binary.LittleEndian.Uint64(t.leafOffs[8*i:])
	l := xyLeaf{
		minX: binary.LittleEndian.Uint64(t.data[at:]), minY: binary.LittleEndian.Uint64(t.data[at+8:]),
		xw: t.data[at+16], yw: t.data[at+17], byY: t.data[at+18] == 1, dw: t.docWidth, count: uint64(t.leafCount(i)),
	}
	at += xyLeafHeaderLen
	n := packedSize(l.count, l.xw)
	l.xs, at = t.data[at:at+n], at+n
	n = packedSize(l.count, l.yw)
	l.ys, at = t.data[at:at+n], at+n
	l.ds = t.data[at : at+packedSize(l.count, l.dw)]
	return l
}

// XYBox is a box over a point field's coordinates: per axis an interval, each end
// inclusive or not, an unbounded end infinite.
type XYBox struct {
	XLo, XHi, YLo, YHi             float64
	XLoInc, XHiInc, YLoInc, YHiInc bool
}

// xyRange is a box in keys: [xlo, xhi] × [ylo, yhi], inclusive.
type xyRange struct{ xlo, xhi, ylo, yhi uint64 }

// keyInterval turns one axis's interval into inclusive keys; false when it holds none.
func keyInterval(lo, hi float64, loInc, hiInc bool) (klo, khi uint64, ok bool) {
	klo, khi = 0, math.MaxUint64
	if !math.IsInf(lo, -1) {
		if math.IsNaN(lo) || math.IsInf(lo, 1) {
			return 0, 0, false
		}
		klo = xyKey(lo)
		if !loInc {
			if klo == math.MaxUint64 {
				return 0, 0, false
			}
			klo++
		}
	}
	if !math.IsInf(hi, 1) {
		if math.IsNaN(hi) || math.IsInf(hi, -1) {
			return 0, 0, false
		}
		khi = xyKey(hi)
		if !hiInc {
			if khi == 0 {
				return 0, 0, false
			}
			khi--
		}
	}
	return klo, khi, klo <= khi
}

func (b XYBox) keys() (xyRange, bool) {
	xlo, xhi, okX := keyInterval(b.XLo, b.XHi, b.XLoInc, b.XHiInc)
	ylo, yhi, okY := keyInterval(b.YLo, b.YHi, b.YLoInc, b.YHiInc)
	return xyRange{xlo, xhi, ylo, yhi}, okX && okY
}

// meets and covers say how node k's box lies to q.
func (q *xyRange) meets(minX, maxX, minY, maxY uint64) bool {
	return maxX >= q.xlo && minX <= q.xhi && maxY >= q.ylo && minY <= q.yhi
}

func (q *xyRange) covers(minX, maxX, minY, maxY uint64) bool {
	return minX >= q.xlo && maxX <= q.xhi && minY >= q.ylo && maxY <= q.yhi
}

// xyDenseShare: once a box has gathered 1/xyDenseShare of the segment's documents (a
// document more than once when several of its points are inside), it gathers them in a
// dense bitset of the segment's documents rather than a list, which must be sorted:
// past about that share, sorting the list costs more than the bitset's words.
const xyDenseShare = 1024

// xyCollector gathers a box's documents: in a list while they are few, then in a dense
// bitset of the segment's documents. A document with several points in the box is
// added more than once, harmlessly.
type xyCollector struct {
	numDocs uint32
	list    []uint32
	words   []uint64
}

func (c *xyCollector) add(doc uint32) {
	if doc >= c.numDocs {
		return
	}
	if c.words != nil {
		c.words[doc>>6] |= 1 << (doc & 63)
		return
	}
	c.list = append(c.list, doc)
	if uint64(len(c.list))*xyDenseShare >= uint64(c.numDocs) && len(c.list) >= 256 {
		c.words = make([]uint64, (uint64(c.numDocs)+63)/64)
		for _, d := range c.list {
			c.words[d>>6] |= 1 << (d & 63)
		}
		c.list = nil
	}
}

func (c *xyCollector) bitmap() *roaring.Bitmap {
	if c.words != nil {
		return roaring.FromDense(c.words, false)
	}
	slices.Sort(c.list)
	return roaring.BitmapOf(slices.Compact(c.list)...)
}

// box returns the documents with a point in q.
func (t *xyIndex) box(q xyRange) *roaring.Bitmap {
	c := &xyCollector{numDocs: t.numDocs}
	if t.numLeaves > 0 {
		t.visit(&q, c, 1, 0, int(t.numLeaves))
	}
	return c.bitmap()
}

func (t *xyIndex) visit(q *xyRange, c *xyCollector, k, l, r int) {
	minX, maxX, minY, maxY := t.node(k)
	switch {
	case !q.meets(minX, maxX, minY, maxY):
	case q.covers(minX, maxX, minY, maxY):
		for i := l; i < r; i++ {
			lf := t.leaf(uint32(i)) //nolint:gosec // below numLeaves
			for j := range lf.count {
				c.add(uint32(unpack(lf.ds, j, lf.dw))) //nolint:gosec // docWidth bits, checked against numDocs by add
			}
		}
	case r-l == 1:
		lf := t.leaf(uint32(l)) //nolint:gosec // below numLeaves
		xCut := minX < q.xlo || maxX > q.xhi
		yCut := minY < q.ylo || maxY > q.yhi
		sortedCut, otherCut := xCut, yCut
		lo, hi := q.xlo, q.xhi
		other, olo, ohi := lf.y, q.ylo, q.yhi
		if lf.byY {
			sortedCut, otherCut = yCut, xCut
			lo, hi = q.ylo, q.yhi
			other, olo, ohi = lf.x, q.xlo, q.xhi
		}
		from, to := uint64(0), lf.count
		if sortedCut {
			from, to = lf.run(lo, hi)
		}
		for j := from; j < to; j++ {
			if otherCut {
				if v := other(j); v < olo || v > ohi {
					continue
				}
			}
			c.add(lf.doc(j))
		}
	default:
		m := l + (r-l+1)/2
		t.visit(q, c, 2*k, l, m)
		t.visit(q, c, 2*k+1, m, r)
	}
}

// xyEstimateLeaves: a node of at most this many leaves that q meets in part counts
// half its points in an estimate, rather than being descended into.
const xyEstimateLeaves = 4

// estimate is about how many points lie in q, from the nodes' boxes alone.
func (t *xyIndex) estimate(q xyRange) uint64 {
	if t.numLeaves == 0 {
		return 0
	}
	var walk func(k, l, r int) uint64
	walk = func(k, l, r int) uint64 {
		minX, maxX, minY, maxY := t.node(k)
		points := uint64(min(r*xyLeafSize, int(t.numPoints)) - l*xyLeafSize) //nolint:gosec // non-negative
		switch {
		case !q.meets(minX, maxX, minY, maxY):
			return 0
		case q.covers(minX, maxX, minY, maxY):
			return points
		case r-l <= xyEstimateLeaves:
			return (points + 1) / 2
		}
		m := l + (r-l+1)/2
		return walk(2*k, l, m) + walk(2*k+1, m, r)
	}
	return walk(1, 0, int(t.numLeaves))
}

// each calls fn with every point and its document, leaf by leaf.
func (t *xyIndex) each(fn func(doc uint32, x, y uint64)) {
	for i := range t.numLeaves {
		lf := t.leaf(i)
		for j := range lf.count {
			doc := unpack(lf.ds, j, lf.dw)
			if doc >= uint64(t.numDocs) {
				continue
			}
			fn(uint32(doc), lf.minX+unpack(lf.xs, j, lf.xw), lf.minY+unpack(lf.ys, j, lf.yw)) //nolint:gosec // below numDocs
		}
	}
}

// XYIndex is a point field's index in one segment. The zero XYIndex is a field with
// no points.
type XYIndex struct{ t *xyIndex }

// XY returns field's point index; the zero [XYIndex] when it has none.
func (r *Reader) XY(field string) XYIndex {
	fi := r.fields[field]
	if fi == nil {
		return XYIndex{}
	}
	return XYIndex{t: fi.xy}
}

// Exists reports whether the field has points in the segment.
func (x XYIndex) Exists() bool { return x.t != nil && x.t.numPoints > 0 }

// NumPoints is how many points the field holds, every document's together.
func (x XYIndex) NumPoints() uint64 {
	if x.t == nil {
		return 0
	}
	return uint64(x.t.numPoints)
}

// Box returns the documents (deleted ones included) with a point in b.
func (x XYIndex) Box(b XYBox) *roaring.Bitmap {
	q, ok := b.keys()
	if x.t == nil || !ok {
		return roaring.New()
	}
	return x.t.box(q)
}

// EstimatePoints is about how many points lie in b, from the tree's node boxes alone:
// exact for nodes b covers or misses, half of the small ones it meets in part.
func (x XYIndex) EstimatePoints(b XYBox) uint64 {
	q, ok := b.keys()
	if x.t == nil || !ok {
		return 0
	}
	return x.t.estimate(q)
}

// Each calls fn with every point of the field and its document, in no particular
// order.
func (x XYIndex) Each(fn func(doc uint32, p [2]float64)) {
	if x.t == nil {
		return
	}
	x.t.each(func(doc uint32, kx, ky uint64) { fn(doc, [2]float64{xyValue(kx), xyValue(ky)}) })
}
