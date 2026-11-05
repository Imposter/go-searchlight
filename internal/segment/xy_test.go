package segment

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/schema"
)

func pointMapping() *schema.Mapping {
	return &schema.Mapping{Fields: map[string]schema.FieldType{
		"loc":   schema.PointType(schema.Number, schema.Number),
		"when":  schema.PointType(schema.Date, schema.Number),
		"price": schema.Number,
	}}
}

// pointCoord is a coordinate drawn from a small grid (so points and bounds collide),
// or anywhere, -0, negative and huge included.
func pointCoord(rng *rand.Rand) string {
	switch rng.IntN(10) {
	case 0:
		return "-0"
	case 1:
		return strconv.FormatFloat((rng.Float64()-0.5)*1e300, 'g', -1, 64)
	case 2:
		return strconv.FormatFloat(rng.NormFloat64()*1e-300, 'g', -1, 64)
	case 3:
		return strconv.FormatFloat(rng.Float64()*100-50, 'g', -1, 64)
	default:
		return strconv.Itoa(rng.IntN(40) - 10)
	}
}

// xyDocs is n documents of random point lists (up to maxPoints each), some without
// the field, some with a value that is no point.
func xyDocs(t testing.TB, rng *rand.Rand, n, maxPoints int) []schema.Doc {
	m := pointMapping()
	docs := make([]schema.Doc, 0, n)
	for i := range n {
		var members []string
		if rng.IntN(8) > 0 {
			k := rng.IntN(maxPoints + 1)
			pairs := make([]string, k)
			for j := range pairs {
				pairs[j] = "[" + pointCoord(rng) + "," + pointCoord(rng) + "]"
			}
			value := "[" + strings.Join(pairs, ",") + "]"
			switch rng.IntN(20) {
			case 0:
				value = `"nowhere"`
			case 1:
				value = "[" + pointCoord(rng) + "," + pointCoord(rng) + "]"
			}
			members = append(members, `"loc":`+value)
		}
		if rng.IntN(3) == 0 {
			ms := 1_700_000_000_000 + rng.Int64N(50)*3_600_000
			members = append(members, fmt.Sprintf(`"when":[[%d,%d],["2026-10-01T00:00:00Z",1]]`, ms, rng.IntN(5)))
		}
		if rng.IntN(2) == 0 {
			members = append(members, `"price":`+strconv.Itoa(rng.IntN(100)))
		}
		docs = append(docs, mustAnalyze(t, m, fmt.Sprintf("p%05d", i), "{"+strings.Join(members, ",")+"}"))
	}
	return docs
}

// randomXYBox is a box with each end absent, or open or closed at a coordinate drawn
// as the documents' are.
func randomXYBox(rng *rand.Rand) XYBox {
	end := func(inf float64) (float64, bool) {
		if rng.IntN(3) == 0 {
			return inf, true
		}
		f, _ := strconv.ParseFloat(pointCoord(rng), 64)
		return f, rng.IntN(2) == 0
	}
	var b XYBox
	b.XLo, b.XLoInc = end(math.Inf(-1))
	b.XHi, b.XHiInc = end(math.Inf(1))
	b.YLo, b.YLoInc = end(math.Inf(-1))
	b.YHi, b.YHiInc = end(math.Inf(1))
	return b
}

func inInterval(v, lo, hi float64, loInc, hiInc bool) bool {
	return (v > lo || loInc && v == lo) && (v < hi || hiInc && v == hi)
}

func (b XYBox) holds(p schema.Point) bool {
	return inInterval(p.X, b.XLo, b.XHi, b.XLoInc, b.XHiInc) && inInterval(p.Y, b.YLo, b.YHi, b.YLoInc, b.YHiInc)
}

// bruteBox is the ordinals of docs (in order) with a point of field in b, among live.
func bruteBox(docs []schema.Doc, field string, b XYBox) *roaring.Bitmap {
	out := roaring.New()
	for i := range docs {
		if slices.ContainsFunc(docs[i].Fields[field].Points, b.holds) {
			out.Add(uint32(i))
		}
	}
	return out
}

// Box answers exactly what a scan of every document's points does, over trees of many
// leaves, for boxes with every combination of open, closed and absent ends, on both
// axis kinds, and for a box equal to a data point's coordinates.
func TestXYBoxEqualsBruteForce(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	docs := xyDocs(t, rng, 3000, 9)
	for _, threads := range []int{1, 4} {
		dir := t.TempDir()
		meta, err := Build(dir, docs, BuildOptions{Threads: threads})
		if err != nil {
			t.Fatal(err)
		}
		r, err := Open(meta.Path)
		if err != nil {
			t.Fatal(err)
		}
		xy := r.XY("loc")
		if !xy.Exists() || xy.NumPoints() < 5*xyLeafSize {
			t.Fatalf("the corpus holds %d points: too few for a tree of many leaves", xy.NumPoints())
		}
		nonEmpty := 0
		for i := range 3000 {
			b := randomXYBox(rng)
			if i%10 == 0 {
				// A box exactly around one data point.
				d := docs[rng.IntN(len(docs))].Fields["loc"].Points
				if len(d) > 0 {
					p := d[rng.IntN(len(d))]
					b = XYBox{XLo: p.X, XHi: p.X, YLo: p.Y, YHi: math.Inf(1), XLoInc: true, XHiInc: true, YLoInc: rng.IntN(2) == 0, YHiInc: true}
				}
			}
			want := bruteBox(docs, "loc", b)
			got := xy.Box(b)
			if !got.Equals(want) {
				t.Fatalf("threads %d, box %+v: %d documents, want %d", threads, b, got.GetCardinality(), want.GetCardinality())
			}
			if !want.IsEmpty() {
				nonEmpty++
			}
			if est := xy.EstimatePoints(b); est > xy.NumPoints() {
				t.Fatalf("box %+v: estimate %d of %d points", b, est, xy.NumPoints())
			}
		}
		if nonEmpty < 500 {
			t.Fatalf("only %d boxes matched anything: the test is too sparse", nonEmpty)
		}
		all := XYBox{XLo: math.Inf(-1), XHi: math.Inf(1), YLo: math.Inf(-1), YHi: math.Inf(1), XLoInc: true, XHiInc: true, YLoInc: true, YHiInc: true}
		if est := xy.EstimatePoints(all); est != xy.NumPoints() {
			t.Fatalf("the whole plane estimates %d of %d points", est, xy.NumPoints())
		}
		when := r.XY("when")
		at := XYBox{XLo: 1_700_000_000_000 + 10*3_600_000, XHi: math.Inf(1), YLo: 2, YHi: math.Inf(1), XLoInc: true, XHiInc: true, YLoInc: true, YHiInc: true}
		if got, want := when.Box(at), bruteBox(docs, "when", at); !got.Equals(want) || want.IsEmpty() {
			t.Fatalf("a date axis box: %d documents, want %d", got.GetCardinality(), want.GetCardinality())
		}
		if r.XY("price").Exists() || r.XY("nope").Exists() || !r.XY("nope").Box(all).IsEmpty() {
			t.Fatal("a field with no points has an XY index")
		}
		_ = r.Close()
	}
}

// A document matches only when one point lies in the box on both axes: (1, 10) and
// (10, 1) are not a point inside x >= 5, y >= 5.
func TestXYSamePoint(t *testing.T) {
	m := pointMapping()
	docs := []schema.Doc{
		mustAnalyze(t, m, "split", `{"loc": [[1, 10], [10, 1]]}`),
		mustAnalyze(t, m, "inside", `{"loc": [[1, 10], [6, 6]]}`),
		mustAnalyze(t, m, "one", `{"loc": [7, 9]}`),
		mustAnalyze(t, m, "none", `{"loc": []}`),
	}
	r := mustBuild(t, docs)
	b := XYBox{XLo: 5, XHi: math.Inf(1), YLo: 5, YHi: math.Inf(1), XLoInc: true, XHiInc: true, YLoInc: true, YHiInc: true}
	if got := r.XY("loc").Box(b).ToArray(); !slices.Equal(got, []uint32{1, 2}) {
		t.Fatalf("x >= 5, y >= 5: documents %v, want [1 2]", got)
	}
	if !r.Present("loc").Contains(3) {
		t.Fatal("an empty point list is not present")
	}
}

// A merge with deletes keeps exactly the live documents' points under their new
// ordinals, whatever order its inputs' trees hold them in.
func TestXYMergeWithDeletes(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 5))
	docs := xyDocs(t, rng, 2400, 6)
	dir := t.TempDir()
	var readers []*Reader
	var deletes []*roaring.Bitmap
	var live []schema.Doc
	for i, rg := range splitRanges(uint32(len(docs)), 3) {
		part := docs[rg.start:rg.end]
		meta, err := Build(dir, part, BuildOptions{Name: fmt.Sprint("in", i)})
		if err != nil {
			t.Fatal(err)
		}
		r, err := Open(meta.Path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		readers = append(readers, r)
		del := roaring.New()
		for j := range part {
			if rng.IntN(4) == 0 {
				del.Add(uint32(j))
				continue
			}
			live = append(live, part[j])
		}
		deletes = append(deletes, del)
	}
	meta, err := Merge(dir, readers, deletes, MergeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	merged, err := Open(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = merged.Close() })
	for range 500 {
		b := randomXYBox(rng)
		if got, want := merged.XY("loc").Box(b), bruteBox(live, "loc", b); !got.Equals(want) {
			t.Fatalf("merged, box %+v: %d documents, want %d", b, got.GetCardinality(), want.GetCardinality())
		}
	}
	want := 0
	for i := range live {
		want += len(live[i].Fields["loc"].Points)
	}
	if got := merged.XY("loc").NumPoints(); got != uint64(want) {
		t.Fatalf("merged holds %d points, the live documents %d", got, want)
	}
}

// A segment with a point field is format 4.1; one without stays 4.0, as before.
func TestXYMinorVersion(t *testing.T) {
	dir := t.TempDir()
	m := pointMapping()
	for name, body := range map[string]string{"with": `{"loc": [1, 2]}`, "without": `{"price": 3}`} {
		meta, err := Build(dir, []schema.Doc{mustAnalyze(t, m, "a", body)}, BuildOptions{Name: name})
		if err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(meta.Path)
		if err != nil {
			t.Fatal(err)
		}
		want := uint16(0)
		if name == "with" {
			want = minorXY
		}
		if got := binary.LittleEndian.Uint16(data[10:]); got != want {
			t.Fatalf("a segment %s points: minor %d, want %d", name, got, want)
		}
	}
}

// The keys sort as the floats do, -0 as 0.
func TestXYKeyOrder(t *testing.T) {
	values := []float64{math.Inf(-1), -math.MaxFloat64, -1e10, -1, -math.SmallestNonzeroFloat64, 0, math.SmallestNonzeroFloat64, 1, 1e10, math.MaxFloat64, math.Inf(1)}
	for i := 1; i < len(values); i++ {
		if xyKey(values[i-1]) >= xyKey(values[i]) {
			t.Fatalf("key(%g) >= key(%g)", values[i-1], values[i])
		}
	}
	for _, v := range values {
		if got := xyValue(xyKey(v)); got != v {
			t.Fatalf("value(key(%g)) = %g", v, got)
		}
	}
	if xyKey(math.Copysign(0, -1)) != xyKey(0) {
		t.Fatal("-0 has a key of its own")
	}
}

// openXY refuses a tree whose tables do not fit its counts.
func TestXYCorruptTree(t *testing.T) {
	w := newFileWriter(nil)
	var buf spillBuffer
	w.out = &buf
	pts := make([]xyPoint, 2000)
	for i := range pts {
		pts[i] = xyPoint{x: xyKey(float64(i % 37)), y: xyKey(float64(i % 11)), doc: uint32(i / 4)}
	}
	writeXYTree(w, 500, canonicalXY(pts))
	w.flush()
	data := buf.mem
	if _, err := openXY(data, 0, 500); err != nil {
		t.Fatalf("a sound tree: %v", err)
	}
	for _, at := range []int{0, 4, 8} {
		bad := slices.Clone(data)
		binary.LittleEndian.PutUint32(bad[at:], binary.LittleEndian.Uint32(bad[at:])+1)
		if _, err := openXY(bad, 0, 500); err == nil {
			t.Fatalf("a count at byte %d off by one opened", at)
		}
	}
	if _, err := openXY(data[:len(data)-20], 0, 500); err == nil {
		t.Fatal("a truncated tree opened")
	}
}
