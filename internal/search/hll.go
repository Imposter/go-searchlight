package search

import (
	"math"
	"math/bits"
	"slices"
)

// Sketch is a cardinality aggregation's partial: HyperLogLog++ (Heule et al.), exact
// while small. It keeps every distinct 64-bit hash until 2^(P-2) of them (so small
// counts are exact, a hash collision aside), then switches to 2^P registers, estimated
// with Ertl's improved estimator ("New cardinality estimation algorithms for
// HyperLogLog sketches", 2017), which needs no bias tables and is accurate across the
// whole range. Sketches of the same precision merge losslessly: a set union, or a
// register-wise maximum.
type Sketch struct {
	// P is the precision: 2^P registers.
	P uint8 `json:"p"`
	// Hashes are the distinct hashes seen, sorted, while the sketch is exact.
	Hashes []uint64 `json:"hashes,omitempty"`
	// Registers are the HyperLogLog registers once it is not.
	Registers []uint8 `json:"registers,omitempty"`

	set map[uint64]struct{} // Hashes, while adding
}

// NewSketch returns an empty sketch of precision p.
func NewSketch(p uint8) *Sketch {
	return &Sketch{P: p}
}

func (s *Sketch) exactLimit() int {
	return max(16, 1<<(s.P-2))
}

// Add records one value's hash.
func (s *Sketch) Add(h uint64) {
	if s.Registers != nil {
		s.addRegister(h)
		return
	}
	if s.set == nil {
		s.set = make(map[uint64]struct{}, len(s.Hashes)+16)
		for _, x := range s.Hashes {
			s.set[x] = struct{}{}
		}
		s.Hashes = nil
	}
	s.set[h] = struct{}{}
	if len(s.set) > s.exactLimit() {
		s.densify()
	}
}

func (s *Sketch) densify() {
	s.Registers = make([]uint8, 1<<s.P)
	for h := range s.set {
		s.addRegister(h)
	}
	for _, h := range s.Hashes {
		s.addRegister(h)
	}
	s.set, s.Hashes = nil, nil
}

func (s *Sketch) addRegister(h uint64) {
	idx := h >> (64 - s.P)
	q := 64 - int(s.P)
	rho := bits.LeadingZeros64(h<<s.P) + 1
	if rho > q+1 {
		rho = q + 1
	}
	if uint8(rho) > s.Registers[idx] { //nolint:gosec // at most 61
		s.Registers[idx] = uint8(rho) //nolint:gosec // at most 61
	}
}

// seal turns the adding set into the sorted Hashes (the transportable form).
func (s *Sketch) seal() {
	if s.set == nil {
		return
	}
	s.Hashes = make([]uint64, 0, len(s.set))
	for h := range s.set {
		s.Hashes = append(s.Hashes, h)
	}
	slices.Sort(s.Hashes)
	s.set = nil
}

// Merge adds o's values to s. Both must have the same precision.
func (s *Sketch) Merge(o *Sketch) {
	if o == nil {
		return
	}
	if o.Registers != nil {
		if s.Registers == nil {
			s.seal()
			hashes := s.Hashes
			s.Registers = slices.Clone(o.Registers)
			s.Hashes = nil
			for _, h := range hashes {
				s.addRegister(h)
			}
			return
		}
		for i, r := range o.Registers {
			if r > s.Registers[i] {
				s.Registers[i] = r
			}
		}
		return
	}
	for _, h := range o.Hashes {
		s.Add(h)
	}
	for h := range o.set {
		s.Add(h)
	}
}

// Estimate returns the number of distinct values: exact while the sketch is.
func (s *Sketch) Estimate() int64 {
	if s.Registers == nil {
		if s.set != nil {
			return int64(len(s.set))
		}
		return int64(len(s.Hashes))
	}
	m := float64(len(s.Registers))
	q := 64 - int(s.P)
	var c [66]int
	for _, r := range s.Registers {
		c[r]++
	}
	z := m * hllTau(1-float64(c[q+1])/m)
	for k := q; k >= 1; k-- {
		z = 0.5 * (z + float64(c[k]))
	}
	z += m * hllSigma(float64(c[0])/m)
	alpha := 1 / (2 * math.Ln2)
	return int64(math.Round(alpha * m * m / z))
}

func hllSigma(x float64) float64 {
	if x == 1 {
		return math.Inf(1)
	}
	y, z := 1.0, x
	for {
		x *= x
		prev := z
		z += x * y
		y += y
		if z == prev {
			return z
		}
	}
}

func hllTau(x float64) float64 {
	if x == 0 || x == 1 {
		return 0
	}
	y, z := 1.0, 1-x
	for {
		x = math.Sqrt(x)
		prev := z
		y *= 0.5
		z -= (1 - x) * (1 - x) * y
		if z == prev {
			return z / 3
		}
	}
}

// Hashing: FNV-1a with the murmur3 finalizer, fixed so sketches from different nodes
// agree.

const (
	fnvOffset = 14695981039346656037
	fnvPrime  = 1099511628211
)

func hashString(s string) uint64 {
	h := uint64(fnvOffset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime
	}
	return fmix64(h)
}

func hashFloat(f float64) uint64 {
	if f == 0 {
		f = 0 // -0 and 0 are one value
	}
	b := math.Float64bits(f)
	h := uint64(fnvOffset)
	for i := range 8 {
		h ^= (b >> (8 * i)) & 0xff
		h *= fnvPrime
	}
	return fmix64(h ^ 0x9e3779b97f4a7c15)
}

func fmix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}
