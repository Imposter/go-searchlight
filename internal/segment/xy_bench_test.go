package segment

import (
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// The XY benchmarks: 1M documents of 1 to 19 points each (10M points), x a date over
// a year (Unix ms), y a score in [0, 100), as a point(date, number) field of price
// observations. Boxes are the workload's: x >= T (the last share of the year) and
// y >= c (or y <= c), at several selectivities. BenchmarkXYBox runs them on the
// segment's k-d tree; BenchmarkXYSortedScan on the alternative the tree replaces for
// such boxes, the points sorted by x with each one's y beside it, scanned from T on.
const (
	xyBenchDocs  = 1_000_000
	xyBenchStart = 1_760_000_000_000
	xyBenchYear  = 365 * 86_400_000
)

var xyBench struct {
	once   sync.Once
	dir    string
	r      *Reader
	xs, ys []float64 // every point, sorted by x
	docs   []uint32
	err    error
}

func xyBenchSegment(b *testing.B) *Reader { return xyBenchSegmentT(b) }

func xyBenchSegmentT(b testing.TB) *Reader {
	b.Helper()
	xyBench.once.Do(func() {
		rng := rand.New(rand.NewPCG(42, 42))
		docs := make([]schema.Doc, xyBenchDocs)
		type pt struct {
			x, y float64
			doc  uint32
		}
		var all []pt
		for i := range docs {
			points := make([]schema.Point, 1+rng.IntN(19))
			for j := range points {
				points[j] = schema.Point{X: float64(xyBenchStart + rng.Int64N(xyBenchYear)), Y: float64(rng.IntN(10_000)) / 100}
				all = append(all, pt{points[j].X, points[j].Y, uint32(i)})
			}
			slices.SortFunc(points, schema.ComparePoints)
			docs[i] = schema.Doc{ID: fmt.Sprintf("d%07d", i), Fields: map[string]schema.Value{"seen": {Present: true, Points: points}}, Body: []byte(`{}`)}
		}
		slices.SortFunc(all, func(a, b pt) int {
			switch {
			case a.x < b.x:
				return -1
			case a.x > b.x:
				return 1
			}
			return 0
		})
		for _, p := range all {
			xyBench.xs, xyBench.ys, xyBench.docs = append(xyBench.xs, p.x), append(xyBench.ys, p.y), append(xyBench.docs, p.doc)
		}
		dir, err := os.MkdirTemp("", "xybench")
		if err != nil {
			xyBench.err = err
			return
		}
		xyBench.dir = dir
		meta, err := Build(dir, docs, BuildOptions{Threads: 4, NoSync: true})
		if err != nil {
			xyBench.err = err
			return
		}
		xyBench.r, xyBench.err = Open(meta.Path)
	})
	if xyBench.err != nil {
		b.Fatal(xyBench.err)
	}
	return xyBench.r
}

// TestMain removes the XY benchmarks' segment, which outlives any one benchmark.
func TestMain(m *testing.M) {
	code := m.Run()
	if xyBench.r != nil {
		_ = xyBench.r.Close()
	}
	if xyBench.dir != "" {
		_ = os.RemoveAll(filepath.Clean(xyBench.dir))
	}
	os.Exit(code)
}

// xyBenchBoxes are the boxes: x >= T for the last share of the year, y above (or below)
// a cut, and the matches each selects.
type xyBenchCase struct {
	name      string
	lastShare float64 // x >= the start of the year's last share
	y         float64
	above     bool
}

var xyBenchBoxes = []xyBenchCase{
	{"x>=last50%,y>=50", 0.5, 50, true},
	{"x>=last10%,y>=90", 0.1, 90, true},
	{"x>=last1%,y>=90", 0.01, 90, true},
	{"x>=last90%,y>=99", 0.9, 99, true},
	{"x>=last90%,y<=1", 0.9, 1, false},
	{"x>=last0.1%,y>=0", 0.001, 0, true},
}

func (bb *xyBenchCase) box() XYBox {
	b := XYBox{XLo: xyBenchStart + (1-bb.lastShare)*xyBenchYear, XHi: math.Inf(1), YLo: math.Inf(-1), YHi: math.Inf(1), XLoInc: true, XHiInc: true, YLoInc: true, YHiInc: true}
	if bb.above {
		b.YLo = bb.y
	} else {
		b.YHi = bb.y
	}
	return b
}

func BenchmarkXYBox(b *testing.B) {
	r := xyBenchSegment(b)
	xy := r.XY("seen")
	for i := range xyBenchBoxes {
		bb := &xyBenchBoxes[i]
		box := bb.box()
		b.Run(bb.name, func(b *testing.B) {
			var n uint64
			for b.Loop() {
				n = xy.Box(box).GetCardinality()
			}
			b.ReportMetric(float64(n), "docs")
		})
	}
}

func BenchmarkXYSortedScan(b *testing.B) {
	xyBenchSegment(b)
	xs, ys, docs := xyBench.xs, xyBench.ys, xyBench.docs
	for i := range xyBenchBoxes {
		bb := &xyBenchBoxes[i]
		box := bb.box()
		b.Run(bb.name, func(b *testing.B) {
			var n uint64
			for b.Loop() {
				words := make([]uint64, (xyBenchDocs+63)/64)
				from, _ := slices.BinarySearch(xs, box.XLo)
				for j := from; j < len(xs); j++ {
					if y := ys[j]; y >= box.YLo && y <= box.YHi {
						words[docs[j]>>6] |= 1 << (docs[j] & 63)
					}
				}
				n = roaring.FromDense(words, false).GetCardinality()
			}
			b.ReportMetric(float64(n), "docs")
		})
	}
}

func BenchmarkXYEstimate(b *testing.B) {
	xy := xyBenchSegment(b).XY("seen")
	box := xyBenchBoxes[1].box()
	for b.Loop() {
		_ = xy.EstimatePoints(box)
	}
}
