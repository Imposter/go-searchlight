package search

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"testing"
)

// TestTopK: topK's first k are the k least, sorted, and the rest are the others, over
// random lists with many ties and every k.
func TestTopK(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 9))
	for range 300 {
		n := rng.IntN(300)
		xs := make([]int, n)
		span := 1 + rng.IntN(n+1)
		for i := range xs {
			xs[i] = rng.IntN(span)
		}
		want := slices.Sorted(slices.Values(xs))
		for _, k := range []int{0, 1, rng.IntN(n + 1), n / 2, n - 1, n, n + 3} {
			got := slices.Clone(xs)
			topK(got, k, cmp.Compare[int])
			k := max(0, min(k, n))
			if !slices.Equal(got[:k], want[:k]) {
				t.Fatalf("n %d k %d: first %v, want %v", n, k, got[:k], want[:k])
			}
			if !slices.Equal(slices.Sorted(slices.Values(got)), want) {
				t.Fatalf("n %d k %d: not a permutation", n, k)
			}
		}
	}
}
