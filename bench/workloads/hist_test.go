package workloads

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestHistogramBucketsRoundTrip(t *testing.T) {
	for i := range histLen {
		lo, hi := lowest(i), highest(i)
		if lo > hi {
			t.Fatalf("counter %d: lowest %d > highest %d", i, lo, hi)
		}
		if index(lo) != i || index(hi) != i {
			t.Fatalf("counter %d: [%d, %d] indexes to %d and %d", i, lo, hi, index(lo), index(hi))
		}
		if i > 0 && lowest(i) != highest(i-1)+1 {
			t.Fatalf("counter %d starts at %d, but %d ends at %d: a gap", i, lowest(i), i-1, highest(i-1))
		}
	}
	if highest(histLen-1) != MaxValue {
		t.Fatalf("the last counter ends at %d, want %d", highest(histLen-1), MaxValue)
	}
}

func TestHistogramExactBelow2048(t *testing.T) {
	h := NewHistogram()
	for v := int64(1); v <= 1000; v++ {
		h.Record(v)
	}
	for _, c := range []struct {
		q    float64
		want int64
	}{{0, 1}, {0.5, 500}, {0.99, 990}, {0.999, 999}, {1, 1000}} {
		if got := h.Quantile(c.q); got != c.want {
			t.Errorf("Quantile(%v) = %d, want %d", c.q, got, c.want)
		}
	}
	if h.Count() != 1000 || h.Min() != 1 || h.Max() != 1000 || h.Mean() != 500.5 {
		t.Errorf("count %d min %d max %d mean %v", h.Count(), h.Min(), h.Max(), h.Mean())
	}
}

// TestHistogramRelativeError checks every quantile of a wide random sample against
// the exact sorted answer: within 1/1024 relative.
func TestHistogramRelativeError(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	h := NewHistogram()
	vals := make([]int64, 0, 100_000)
	for range cap(vals) {
		v := int64(math.Exp(r.Float64()*28)) + 1 // 1 ns .. ~24 min, log-uniform
		vals = append(vals, v)
		h.Record(v)
	}
	slices.Sort(vals)
	for _, q := range []float64{0.01, 0.1, 0.5, 0.9, 0.99, 0.999, 0.9999} {
		exact := vals[int(math.Ceil(q*float64(len(vals))))-1]
		got := h.Quantile(q)
		if got < exact || float64(got-exact) > float64(exact)/1024+1 {
			t.Errorf("Quantile(%v) = %d, exact %d: outside 1/1024", q, got, exact)
		}
	}
}

func TestHistogramMergeEqualsCombined(t *testing.T) {
	a, b, all := NewHistogram(), NewHistogram(), NewHistogram()
	r := rand.New(rand.NewPCG(3, 4))
	for i := range 10_000 {
		v := r.Int64N(50_000_000)
		all.Record(v)
		if i%3 == 0 {
			a.Record(v)
		} else {
			b.Record(v)
		}
	}
	a.Merge(b)
	a.Merge(nil)
	a.Merge(NewHistogram())
	if a.Summary() != all.Summary() {
		t.Fatalf("merged %+v, combined %+v", a.Summary(), all.Summary())
	}
}

func TestHistogramClampsAndEmpty(t *testing.T) {
	h := NewHistogram()
	if s := h.Summary(); s != (Summary{}) {
		t.Fatalf("empty summary %+v", s)
	}
	h.Record(-5)
	h.Record(MaxValue * 4)
	if h.Min() != 0 || h.Max() != MaxValue || h.Quantile(1) != MaxValue {
		t.Fatalf("min %d max %d q1 %d", h.Min(), h.Max(), h.Quantile(1))
	}
}

func TestSummaryMicroseconds(t *testing.T) {
	h := NewHistogram()
	h.Record(1500) // 1.5 µs
	if s := h.Summary(); s.P50 != 1.5 || s.Max != 1.5 || s.Count != 1 {
		t.Fatalf("summary %+v", s)
	}
}
