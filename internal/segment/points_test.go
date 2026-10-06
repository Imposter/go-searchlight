package segment

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// pointCorpus builds a segment of n documents whose "n" field draws from gen (a
// missing value when gen says so).
func pointCorpus(t *testing.T, n int, gen func(i int) (string, bool)) *Reader {
	t.Helper()
	m := &schema.Mapping{Fields: map[string]schema.FieldType{"n": schema.Number}}
	docs := make([]schema.Doc, 0, n)
	for i := range n {
		body := `{}`
		if v, ok := gen(i); ok {
			body = `{"n": ` + v + `}`
		}
		docs = append(docs, mustAnalyze(t, m, fmt.Sprintf("d%05d", i), body))
	}
	return mustBuild(t, docs)
}

// bruteRange is the documents of nc whose value lies within [lo, hi], read one by one.
func bruteRange(nc NumericColumn, numDocs uint32, lo, hi float64, incLo, incHi bool) *roaring.Bitmap {
	out := roaring.New()
	for d := range numDocs {
		if v, ok := nc.Value(d); ok && inRange(v, lo, hi, incLo, incHi) {
			out.Add(d)
		}
	}
	return out
}

// probeBounds are range ends worth trying on nc: its values, the values just beside
// them, and values beyond either end.
func probeBounds(rng *rand.Rand, nc NumericColumn, numDocs uint32) []float64 {
	st := nc.Stats()
	out := []float64{math.Inf(-1), math.Inf(1), st.Min, st.Max, st.Min - 1, st.Max + 1}
	for range 12 {
		if v, ok := nc.Value(rng.Uint32N(numDocs)); ok {
			out = append(out, v, math.Nextafter(v, math.Inf(1)), math.Nextafter(v, math.Inf(-1)), v+0.5)
		}
	}
	return out
}

func TestPointsCountRangeFilterAndBlocks(t *testing.T) {
	type corpus struct {
		name string
		n    int
		gen  func(rng *rand.Rand) func(i int) (string, bool)
	}
	corpora := []corpus{
		{"ints with duplicates", 3000, func(rng *rand.Rand) func(int) (string, bool) {
			return func(int) (string, bool) { return strconv.Itoa(rng.IntN(40)), true }
		}},
		{"prices, some missing", 5000, func(rng *rand.Rand) func(int) (string, bool) {
			return func(int) (string, bool) {
				if rng.IntN(5) == 0 {
					return "", false
				}
				return strconv.FormatFloat(math.Round(math.Exp(3+rng.NormFloat64())*100)/100, 'f', -1, 64), true
			}
		}},
		{"wide floats", 2000, func(rng *rand.Rand) func(int) (string, bool) {
			return func(int) (string, bool) {
				return strconv.FormatFloat((rng.Float64()-0.5)*math.Pow(10, float64(rng.IntN(40)-20)), 'g', -1, 64), true
			}
		}},
		{"one value", 700, func(*rand.Rand) func(int) (string, bool) {
			return func(int) (string, bool) { return "7", true }
		}},
		{"dates", 4000, func(rng *rand.Rand) func(int) (string, bool) {
			return func(int) (string, bool) { return strconv.FormatInt(1_700_000_000_000+rng.Int64N(1e10), 10), true }
		}},
	}
	for _, c := range corpora {
		t.Run(c.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(len(c.name)), uint64(c.n)))
			r := pointCorpus(t, c.n, c.gen(rng))
			checkPoints(t, rng, r.Numbers("n"), r.NumDocs())
		})
	}
}

func TestPointsOnPreviousMajor(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for _, name := range []string{"dense", "sparse"} {
		r := openCompatFixture(t, name)
		for _, field := range []string{"price", "created", "rating"} {
			if nc := r.Numbers(field); nc.Exists() {
				checkPoints(t, rng, nc, r.NumDocs())
			}
		}
	}
}

func checkPoints(t *testing.T, rng *rand.Rand, nc NumericColumn, numDocs uint32) {
	t.Helper()
	bounds := probeBounds(rng, nc, numDocs)
	all := make([]uint32, numDocs)
	for i := range all {
		all[i] = uint32(i)
	}
	for range 300 {
		lo, hi := bounds[rng.IntN(len(bounds))], bounds[rng.IntN(len(bounds))]
		incLo, incHi := rng.IntN(2) == 0, rng.IntN(2) == 0
		want := bruteRange(nc, numDocs, lo, hi, incLo, incHi)
		desc := fmt.Sprintf("[%v, %v] inc %v %v", lo, hi, incLo, incHi)
		if got := nc.Range(lo, hi, incLo, incHi); !got.Equals(want) {
			t.Fatalf("Range%s: %d documents, want %d", desc, got.GetCardinality(), want.GetCardinality())
		}
		if got := nc.Count(lo, hi, incLo, incHi); got != want.GetCardinality() {
			t.Fatalf("Count%s = %d, want %d", desc, got, want.GetCardinality())
		}
		docs := all
		if rng.IntN(2) == 0 {
			docs = nil
			for _, d := range all {
				if rng.IntN(3) == 0 {
					docs = append(docs, d)
				}
			}
		}
		got := nc.Filter(docs, lo, hi, incLo, incHi, nil)
		var wantF []uint32
		for _, d := range docs {
			if want.Contains(d) {
				wantF = append(wantF, d)
			}
		}
		if !slices.Equal(got, wantF) {
			t.Fatalf("Filter%s: %d documents, want %d", desc, len(got), len(wantF))
		}
		if hi < lo {
			continue
		}
		for _, descending := range []bool{false, true} {
			seen := roaring.New()
			last := math.Inf(-1)
			if descending {
				last = math.Inf(1)
			}
			nc.EachBlock(lo, hi, descending, func(b PointBlock) bool {
				bmin, bmax, docs := b.Min, b.Max, b.Docs(nil)
				if len(docs) != b.Count {
					t.Fatalf("EachBlock%s: a block of %d documents says %d", desc, len(docs), b.Count)
				}
				if bmin > bmax || (!descending && bmin < last) || (descending && bmax > last) {
					t.Fatalf("EachBlock%s desc %v: block [%v, %v] out of order after %v", desc, descending, bmin, bmax, last)
				}
				if descending {
					last = bmin
				} else {
					last = bmax
				}
				for _, d := range docs {
					v, ok := nc.Value(d)
					if !ok || v < bmin || v > bmax {
						t.Fatalf("EachBlock%s: document %d (%v) outside its block [%v, %v]", desc, d, v, bmin, bmax)
					}
					seen.Add(d)
				}
				return true
			})
			if inclusive := bruteRange(nc, numDocs, lo, hi, true, true); !roaring.AndNot(inclusive, seen).IsEmpty() {
				t.Fatalf("EachBlock%s desc %v missed documents in range", desc, descending)
			}
		}
	}
}
