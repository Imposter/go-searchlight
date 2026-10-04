package workloads

import (
	"math"
	"math/bits"
	"time"
)

// Histogram is an HDR-style latency histogram over nanoseconds: values below 2048 are
// counted exactly, and above that each power of two is split into 1024 linear
// sub-buckets, so a recorded value is reported within 1/1024 (about 0.1%) of itself.
// It covers 1 ns to 2^42 ns (73 minutes); a larger value is counted as the largest.
// It is not safe for concurrent use: give each worker its own and [Histogram.Merge]
// them.
type Histogram struct {
	counts   []int64
	total    int64
	min, max int64
	sum      float64
}

const (
	// subBits is how many of a value's top bits a bucket keeps: 11, so each power
	// of two above 2^11 has 2^10 sub-buckets.
	subBits = 11
	subN    = 1 << subBits // 2048: values below it are exact
	subHalf = subN / 2     // 1024 sub-buckets per power of two above it
	// maxBits bounds the values counted: up to 2^42 ns.
	maxBits = 42
	// histLen is the number of counters: the exact range, then (maxBits-subBits)
	// powers of two of subHalf sub-buckets each.
	histLen = subN + (maxBits-subBits)*subHalf
	// MaxValue is the largest value a Histogram counts apart.
	MaxValue = int64(1)<<maxBits - 1
)

// NewHistogram returns an empty histogram.
func NewHistogram() *Histogram {
	return &Histogram{counts: make([]int64, histLen), min: math.MaxInt64}
}

// index is v's counter.
func index(v int64) int {
	if v < subN {
		return int(v)
	}
	shift := bits.Len64(uint64(v)) - subBits // >= 1
	return subN + (shift-1)*subHalf + int(v>>uint(shift)) - subHalf
}

// lowest and highest are the smallest and largest values counter i holds.
func lowest(i int) int64 {
	if i < subN {
		return int64(i)
	}
	j := i - subN
	shift := j/subHalf + 1
	return int64(j%subHalf+subHalf) << uint(shift)
}

func highest(i int) int64 {
	if i < subN {
		return int64(i)
	}
	shift := (i-subN)/subHalf + 1
	return lowest(i) + int64(1)<<uint(shift) - 1
}

// Record counts one value in nanoseconds; a negative value counts as 0.
func (h *Histogram) Record(v int64) {
	v = min(max(v, 0), MaxValue)
	h.counts[index(v)]++
	h.total++
	h.sum += float64(v)
	h.min = min(h.min, v)
	h.max = max(h.max, v)
}

// RecordDuration counts one duration.
func (h *Histogram) RecordDuration(d time.Duration) { h.Record(int64(d)) }

// Merge adds o's counts to h.
func (h *Histogram) Merge(o *Histogram) {
	if o == nil || o.total == 0 {
		return
	}
	for i, c := range o.counts {
		h.counts[i] += c
	}
	h.total += o.total
	h.sum += o.sum
	h.min = min(h.min, o.min)
	h.max = max(h.max, o.max)
}

// Count is how many values were recorded.
func (h *Histogram) Count() int64 { return h.total }

// Min and Max are the smallest and largest values recorded (0 when empty).
func (h *Histogram) Min() int64 {
	if h.total == 0 {
		return 0
	}
	return h.min
}

// Max is the largest value recorded (0 when empty).
func (h *Histogram) Max() int64 { return h.max }

// Mean is the values' mean (0 when empty).
func (h *Histogram) Mean() float64 {
	if h.total == 0 {
		return 0
	}
	return h.sum / float64(h.total)
}

// Quantile returns the value at quantile q (0..1): the highest value of the counter
// holding the ceil(q*count)-th smallest value, capped at Max, as HdrHistogram's
// getValueAtPercentile reports it. 0 when empty.
func (h *Histogram) Quantile(q float64) int64 {
	if h.total == 0 {
		return 0
	}
	q = min(max(q, 0), 1)
	rank := max(int64(math.Ceil(q*float64(h.total))), 1)
	var seen int64
	for i, c := range h.counts {
		seen += c
		if seen >= rank {
			return min(max(highest(i), h.Min()), h.max)
		}
	}
	return h.max
}

// Summary is a histogram's headline numbers in microseconds.
type Summary struct {
	Count int64   `json:"count"`
	Min   float64 `json:"min_us"`
	Mean  float64 `json:"mean_us"`
	P50   float64 `json:"p50_us"`
	P90   float64 `json:"p90_us"`
	P99   float64 `json:"p99_us"`
	P999  float64 `json:"p999_us"`
	Max   float64 `json:"max_us"`
}

// Summary returns h's headline numbers.
func (h *Histogram) Summary() Summary {
	us := func(ns int64) float64 { return float64(ns) / 1e3 }
	return Summary{
		Count: h.total,
		Min:   us(h.Min()),
		Mean:  h.Mean() / 1e3,
		P50:   us(h.Quantile(0.50)),
		P90:   us(h.Quantile(0.90)),
		P99:   us(h.Quantile(0.99)),
		P999:  us(h.Quantile(0.999)),
		Max:   us(h.max),
	}
}
