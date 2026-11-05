package search

import "slices"

// topK reorders xs so that its first k elements are its k least by cmp, sorted; the
// others follow in no particular order. It selects (quickselect, median-of-three
// pivots, three-way partitions) before sorting the k alone, so keeping shard_size
// terms of many costs time linear in the terms rather than a sort of them all. A
// selection that keeps failing to shrink falls back to sorting what is left.
func topK[T any](xs []T, k int, cmp func(a, b T) int) {
	if k <= 0 {
		return
	}
	if k >= len(xs) {
		slices.SortFunc(xs, cmp)
		return
	}
	lo, hi := 0, len(xs)
	for rounds := 0; hi-lo > 32; rounds++ {
		if rounds == 64 {
			break
		}
		p := median3(xs[lo], xs[lo+(hi-lo)/2], xs[hi-1], cmp)
		// xs[lo:lt] < p, xs[lt:i] == p, xs[gt:hi] > p.
		lt, i, gt := lo, lo, hi
		for i < gt {
			switch c := cmp(xs[i], p); {
			case c < 0:
				xs[lt], xs[i] = xs[i], xs[lt]
				lt++
				i++
			case c > 0:
				gt--
				xs[i], xs[gt] = xs[gt], xs[i]
			default:
				i++
			}
		}
		switch {
		case k < lt:
			hi = lt
		case k > gt:
			lo = gt
		default:
			lo = hi // k falls among the pivot's equals: xs[:k] are the least
		}
	}
	if lo < hi {
		slices.SortFunc(xs[lo:hi], cmp)
	}
	slices.SortFunc(xs[:k], cmp)
}

func median3[T any](a, b, c T, cmp func(a, b T) int) T {
	if cmp(a, b) > 0 {
		a, b = b, a
	}
	if cmp(b, c) > 0 {
		b = c
		if cmp(a, b) > 0 {
			b = a
		}
	}
	return b
}
