package search

import (
	"math"
	"slices"
)

// TDigest is a percentiles or percentile_ranks aggregation's partial: a merging
// t-digest (Dunning and Ertl, "Computing extremely accurate quantiles using t-digests",
// 2019) with the k1 (arcsine) scale function, exact while small.
//
// It holds centroids, each a mean and a count, sorted by mean. While the values it has
// seen hold at most Compression distinct values, each centroid is one of them with its
// count, nothing is approximated, and Quantile and Rank are exact. Past that it is
// approximate (Approx): the centroids are merged so that each spans at most one unit
// of the scale function, which keeps between Compression/2 and Compression+2 of them,
// small near the tails and wide in the middle, so the extreme percentiles are the
// sharpest. Min and Max stay exact either way.
//
// Digests of the same compression merge: exact ones losslessly (their values and
// counts summed), any other by merging the centroids of both and compressing them
// again, whose error is that of one digest over all the values, as the paper shows for
// the merging digest.
type TDigest struct {
	// Compression is the digest's compression: the most distinct values it holds
	// exactly, and about the most centroids it keeps once approximate.
	Compression float64 `json:"compression"`
	// Means and Counts are the centroids, by ascending mean: strictly ascending, each
	// count at least 1.
	Means  []float64 `json:"means,omitempty"`
	Counts []int64   `json:"counts,omitempty"`
	// Min and Max are the least and greatest values seen.
	Min float64 `json:"min"`
	Max float64 `json:"max"`
	// Approx is set once the digest holds more distinct values than it keeps exactly.
	Approx bool `json:"approx,omitempty"`

	buf []float64 // values added since the last flush
}

// NewTDigest returns an empty digest of the given compression.
func NewTDigest(compression float64) *TDigest {
	return &TDigest{Compression: compression}
}

// exactLimit is the most distinct values the digest holds exactly.
func (t *TDigest) exactLimit() int {
	return max(1, int(math.Ceil(t.Compression)))
}

// bufferLimit is how many added values the digest buffers before it flushes them.
func (t *TDigest) bufferLimit() int {
	return max(64, 4*t.exactLimit())
}

// maxCentroids bounds the centroids a well-formed digest holds: its exact limit when
// exact; once compressed, each pair of neighbours spans more than one of the scale
// function's Compression/2 units.
func (t *TDigest) maxCentroids() int {
	return 2*t.exactLimit() + 4
}

// empty reports whether the digest has seen no value.
func (t *TDigest) empty() bool { return len(t.Means) == 0 && len(t.buf) == 0 }

// Add records value v; -0 is 0.
func (t *TDigest) Add(v float64) {
	v = positiveZero(v)
	if t.empty() || v < t.Min {
		t.Min = v
	}
	if t.empty() || v > t.Max {
		t.Max = v
	}
	t.buf = append(t.buf, v)
	if len(t.buf) >= t.bufferLimit() {
		t.flush()
	}
}

// Count is how many values the digest has seen.
func (t *TDigest) Count() int64 {
	n := int64(len(t.buf))
	for _, c := range t.Counts {
		n += c
	}
	return n
}

// seal flushes the buffered values into the centroids (the transportable form).
func (t *TDigest) seal() { t.flush() }

// flush merges the buffered values into the centroids.
func (t *TDigest) flush() {
	if len(t.buf) == 0 {
		return
	}
	slices.Sort(t.buf)
	means := make([]float64, 0, len(t.buf))
	counts := make([]int64, 0, len(t.buf))
	for _, v := range t.buf {
		if k := len(means); k > 0 && means[k-1] == v {
			counts[k-1]++
			continue
		}
		means = append(means, v)
		counts = append(counts, 1)
	}
	t.buf = t.buf[:0]
	t.absorb(means, counts, false)
}

// Merge adds o's values to t, leaving o as it is. Both must have the same compression.
func (t *TDigest) Merge(o *TDigest) {
	if o == nil || o.empty() {
		return
	}
	t.flush()
	means, counts := o.Means, o.Counts
	if len(o.buf) > 0 {
		c := &TDigest{Compression: o.Compression, Means: o.Means, Counts: o.Counts, Approx: o.Approx, buf: slices.Clone(o.buf)}
		c.flush()
		means, counts = c.Means, c.Counts
	}
	if t.empty() {
		t.Min, t.Max = o.Min, o.Max
	} else {
		t.Min, t.Max = min(t.Min, o.Min), max(t.Max, o.Max)
	}
	t.absorb(means, counts, o.Approx)
}

// absorb merges the sorted, distinct centroids means and counts into t's, then
// compresses them once they pass the exact limit (or when either side is approximate
// already).
func (t *TDigest) absorb(means []float64, counts []int64, approx bool) {
	m := make([]float64, 0, len(t.Means)+len(means))
	c := make([]int64, 0, len(t.Means)+len(means))
	i, j := 0, 0
	for i < len(t.Means) || j < len(means) {
		var mean float64
		var count int64
		switch {
		case j == len(means) || (i < len(t.Means) && t.Means[i] < means[j]):
			mean, count = t.Means[i], t.Counts[i]
			i++
		case i == len(t.Means) || means[j] < t.Means[i]:
			mean, count = means[j], counts[j]
			j++
		default:
			mean, count = t.Means[i], t.Counts[i]+counts[j]
			i++
			j++
		}
		m = append(m, mean)
		c = append(c, count)
	}
	t.Means, t.Counts = m, c
	t.Approx = t.Approx || approx
	if t.Approx || len(t.Means) > t.exactLimit() {
		t.Approx = true
		t.compress()
	}
}

// k1 is the scale function: q in [0, 1] to [-δ/4, δ/4], steep at the tails.
func (t *TDigest) k1(q float64) float64 {
	return t.Compression / (2 * math.Pi) * math.Asin(2*q-1)
}

// k1Inverse is k1's inverse.
func (t *TDigest) k1Inverse(k float64) float64 {
	if k >= t.Compression/4 {
		return 1
	}
	return (math.Sin(k*2*math.Pi/t.Compression) + 1) / 2
}

// compress merges neighbouring centroids while the merged one spans at most one unit
// of the scale function.
func (t *TDigest) compress() {
	if len(t.Means) < 2 {
		return
	}
	total := float64(t.Count())
	means := make([]float64, 0, min(len(t.Means), t.maxCentroids()))
	counts := make([]int64, 0, cap(means))
	// emit appends a merged centroid, within the values (a weighted mean rounds), and
	// folds it into the one before when rounding left it no greater.
	emit := func(mean float64, count int64) {
		mean = min(max(mean, t.Min), t.Max)
		if k := len(means); k > 0 && mean <= means[k-1] {
			counts[k-1] += count
			return
		}
		means, counts = append(means, mean), append(counts, count)
	}
	mean, count := t.Means[0], t.Counts[0]
	before := 0.0
	limit := total * t.k1Inverse(t.k1(0)+1)
	for i := 1; i < len(t.Means); i++ {
		next := t.Counts[i]
		if before+float64(count+next) <= limit {
			count += next
			mean += (t.Means[i] - mean) * float64(next) / float64(count)
			continue
		}
		emit(mean, count)
		before += float64(count)
		limit = total * t.k1Inverse(t.k1(before/total)+1)
		mean, count = t.Means[i], next
	}
	emit(mean, count)
	t.Means, t.Counts = means, counts
}

// Quantile is the value below which fraction q (0 to 1) of the values fall; false
// with no values. Exact, it interpolates linearly between the two values of the
// ranks q*(n-1) falls between, as Elasticsearch's exact digest does; approximate, it
// interpolates between centroids, a centroid of one value exactly at it.
func (t *TDigest) Quantile(q float64) (float64, bool) {
	t.flush()
	if len(t.Means) == 0 {
		return 0, false
	}
	q = min(max(q, 0), 1)
	if !t.Approx {
		return t.exactQuantile(q), true
	}
	return t.approxQuantile(q), true
}

func (t *TDigest) exactQuantile(q float64) float64 {
	n := t.Count()
	r := q * float64(n-1)
	lo := int64(math.Floor(r))
	at := func(rank int64) float64 {
		var seen int64
		for i, c := range t.Counts {
			if seen += c; rank < seen {
				return t.Means[i]
			}
		}
		return t.Max
	}
	a := at(lo)
	if frac := r - float64(lo); frac > 0 {
		b := at(lo + 1)
		return a + frac*(b-a)
	}
	return a
}

func (t *TDigest) approxQuantile(q float64) float64 {
	m, c := t.Means, t.Counts
	k := len(m)
	total := float64(t.Count())
	index := q * total
	w := func(i int) float64 { return float64(c[i]) }
	if index < 1 {
		return t.Min
	}
	if w(0) > 1 && index < w(0)/2 {
		return t.Min + (index-1)/(w(0)/2-1)*(m[0]-t.Min)
	}
	if index > total-1 {
		return t.Max
	}
	if w(k-1) > 1 && total-index <= w(k-1)/2 {
		return t.Max - (total-index-1)/(w(k-1)/2-1)*(t.Max-m[k-1])
	}
	soFar := w(0) / 2
	for i := 0; i < k-1; i++ {
		dw := (w(i) + w(i+1)) / 2
		if soFar+dw > index {
			leftUnit := 0.0
			if c[i] == 1 {
				if index-soFar < 0.5 {
					return m[i]
				}
				leftUnit = 0.5
			}
			rightUnit := 0.0
			if c[i+1] == 1 {
				if soFar+dw-index <= 0.5 {
					return m[i+1]
				}
				rightUnit = 0.5
			}
			z1 := index - soFar - leftUnit
			z2 := soFar + dw - index - rightUnit
			return weightedAverage(m[i], z2, m[i+1], z1)
		}
		soFar += dw
	}
	return m[k-1]
}

// weightedAverage is x1 and x2 weighted w1 and w2, within them.
func weightedAverage(x1, w1, x2, w2 float64) float64 {
	if w1+w2 <= 0 {
		return (x1 + x2) / 2
	}
	return min(max((x1*w1+x2*w2)/(w1+w2), min(x1, x2)), max(x1, x2))
}

// Rank is the fraction (0 to 1) of the values below x, plus half those equal to it,
// as Elasticsearch's percentile_ranks counts them; false with no values. Exact while
// the digest is; approximate, it interpolates between centroids.
func (t *TDigest) Rank(x float64) (float64, bool) {
	t.flush()
	if len(t.Means) == 0 {
		return 0, false
	}
	total := float64(t.Count())
	if !t.Approx {
		var below, equal int64
		for i, mean := range t.Means {
			switch {
			case mean < x:
				below += t.Counts[i]
			case mean == x:
				equal = t.Counts[i]
			}
		}
		return (float64(below) + float64(equal)/2) / total, true
	}
	return t.approxRank(x, total), true
}

func (t *TDigest) approxRank(x, total float64) float64 {
	m, c := t.Means, t.Counts
	k := len(m)
	w := func(i int) float64 { return float64(c[i]) }
	switch {
	case x < t.Min:
		return 0
	case x > t.Max:
		return 1
	case t.Min == t.Max:
		return 0.5
	case x < m[0]:
		if x == t.Min {
			return 0.5 / total
		}
		return (1 + (x-t.Min)/(m[0]-t.Min)*(w(0)/2-1)) / total
	case x > m[k-1]:
		if x == t.Max {
			return 1 - 0.5/total
		}
		return 1 - (1+(t.Max-x)/(t.Max-m[k-1])*(w(k-1)/2-1))/total
	}
	soFar := 0.0
	for i := range k {
		if m[i] == x {
			return (soFar + w(i)/2) / total
		}
		if i+1 < k && x < m[i+1] {
			lo, hi := soFar+w(i)/2, soFar+w(i)+w(i+1)/2
			if c[i] == 1 {
				lo = soFar + 1
			}
			if c[i+1] == 1 {
				hi = soFar + w(i)
			}
			return (lo + (hi-lo)*(x-m[i])/(m[i+1]-m[i])) / total
		}
		soFar += w(i)
	}
	return 1 - 0.5/total
}

// wellFormed reports whether t is a digest of compression as this package builds one:
// centroids strictly ascending, finite, within Min and Max, each counted, no more of
// them than its form holds, and nothing buffered.
func (t *TDigest) wellFormed(compression float64) bool {
	if t.Compression != compression || len(t.Means) == 0 || len(t.Means) != len(t.Counts) || len(t.buf) > 0 {
		return false
	}
	if !t.Approx && len(t.Means) > t.exactLimit() || len(t.Means) > t.maxCentroids() {
		return false
	}
	if math.IsInf(t.Min, 0) || math.IsNaN(t.Min) || math.IsInf(t.Max, 0) || math.IsNaN(t.Max) || t.Min > t.Max {
		return false
	}
	var total int64
	for i, mean := range t.Means {
		if t.Counts[i] < 1 || math.IsNaN(mean) || mean < t.Min || mean > t.Max || (i > 0 && mean <= t.Means[i-1]) {
			return false
		}
		if total += t.Counts[i]; total < 0 {
			return false
		}
	}
	return t.Approx || (t.Means[0] == t.Min && t.Means[len(t.Means)-1] == t.Max)
}
