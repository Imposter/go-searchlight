package search

import (
	"math"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
)

// within fails t unless f returns within a few seconds: the fills below hung forever
// when a float64 bucket index stopped advancing.
func within(t *testing.T, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); f() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("did not return")
	}
}

func sparseHistogram(keys ...float64) *AggResult {
	res := &AggResult{Type: AggHistogram}
	for _, k := range keys {
		res.Buckets = append(res.Buckets, &Bucket{Key: k, DocCount: 1})
	}
	return res
}

// A histogram's empty buckets are filled in only while each bucket index (a count of
// intervals from offset) is a float64 exactly, at most 2^53 either side of 0, and
// neighbouring keys stay apart: past that the fill would never end, or key buckets
// on top of each other, so the histogram stays sparse and says truncated.
func TestFillHistogramStopsPastExactIndices(t *testing.T) {
	const e = float64(maxExactIndex)
	for _, tc := range []struct {
		name             string
		interval, offset float64
		keys             []float64
		want             int // buckets after the fill; 0: left sparse and truncated
	}{
		{"normal", 1, 0, []float64{0, 5}, 6},
		{"offset", 2, 0.5, []float64{-3.5, 4.5}, 5},
		{"negative", 1, 0, []float64{-7, -2}, 6},
		{"up to 2^53", 1, 0, []float64{e - 2, e}, 3},
		{"down to -2^53", 1, 0, []float64{-e, -e + 2}, 3},
		{"past 2^53", 1, 0, []float64{e, e + 2}, 0},
		{"1e16", 1, 0, []float64{1e16, 1e16 + 2}, 0},
		{"-1e16", 1, 0, []float64{-1e16 - 2, -1e16}, 0},
		{"1e16 over a coarse interval", 1e8, 0, []float64{1e16, 1e16 + 3e8}, 4},
		{"huge key over a tiny interval", 1e-300, 0, []float64{1e300, 2e300}, 0},
		{"huge offset beside the interval", 1, 1e20, []float64{1e20, 1e20 + 16384}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, typ := range []string{AggHistogram, AggDateHistogram} {
				spec := &aggSpec{typ: typ, interval: tc.interval, offset: tc.offset}
				res := sparseHistogram(tc.keys...)
				var added int
				within(t, func() { added = fillHistogram(res, spec, MaxBuckets) })
				if tc.want == 0 {
					if !res.Truncated || added != 0 || len(res.Buckets) != len(tc.keys) {
						t.Fatalf("%s: %d buckets, %d added, truncated %v; want %d, sparse and truncated", typ, len(res.Buckets), added, res.Truncated, len(tc.keys))
					}
					continue
				}
				if res.Truncated || len(res.Buckets) != tc.want || added != tc.want-len(tc.keys) {
					t.Fatalf("%s: %d buckets, %d added, truncated %v; want %d filled", typ, len(res.Buckets), added, res.Truncated, tc.want)
				}
				for i, b := range res.Buckets {
					if want := tc.keys[0] + float64(i)*tc.interval; b.Key != want {
						t.Fatalf("%s: bucket %d keyed %v, want %v", typ, i, b.Key, want)
					}
				}
			}
		})
	}
}

// A histogram and a fixed-interval date_histogram over values whose bucket indices
// pass 2^53 answer promptly, with their real buckets and truncated, rather than hang
// the reduce.
func TestHistogramPastExactIndicesAnswers(t *testing.T) {
	c := newCluster(t, 1)
	c.upsert("a", `{"price":1e16,"created":8e15}`)
	c.upsert("b", `{"price":10000000000000002,"created":8000000000000001}`)
	c.refreshAll()
	for name, agg := range map[string]Agg{
		"histogram":      {Type: AggHistogram, Field: "price", Interval: 1},
		"date_histogram": {Type: AggDateHistogram, Field: "created", Interval: 0.5},
	} {
		var resp *Response
		var err error
		within(t, func() { resp, err = c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"h": agg}}) })
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		a := resp.Aggs["h"]
		if !a.Truncated || len(a.Buckets) != 2 || a.Buckets[0].DocCount != 1 || a.Buckets[1].DocCount != 1 {
			t.Fatalf("%s: %d buckets, truncated %v; want the 2 real ones, truncated", name, len(a.Buckets), a.Truncated)
		}
	}
}

// Any two histogram keys fill in promptly, to strictly increasing keys that keep both
// real buckets, or stay sparse.
func FuzzFillHistogram(f *testing.F) {
	f.Add(0.0, 5.0, 1.0, 0.0)
	f.Add(1e16, 1e16+2, 1.0, 0.0)
	f.Add(-1e16-2, -1e16, 1.0, 0.0)
	f.Add(float64(maxExactIndex-2), float64(maxExactIndex), 1.0, 0.0)
	f.Add(1e300, 2e300, 1e-300, 0.0)
	f.Add(1e20, 1e20+16384, 1.0, 1e20)
	f.Add(3.0, 1e6, 0.25, 0.1)
	f.Add(1e16, 1.0000000000000002e16, 3.333333333333333, -88.0) // the grid recomputed starts below the first key
	f.Fuzz(func(t *testing.T, a, b, interval, offset float64) {
		if !(interval > 0) || math.IsInf(interval, 0) || math.IsNaN(offset) || math.IsInf(offset, 0) {
			return
		}
		spec := &aggSpec{typ: AggHistogram, interval: interval, offset: offset}
		lo, ok1 := bucketKey(spec, a)
		hi, ok2 := bucketKey(spec, b)
		if !ok1 || !ok2 || !(lo < hi) {
			return
		}
		res := sparseHistogram(lo, hi)
		added := -1
		within(t, func() { added = fillHistogram(res, spec, 1024) })
		if added == 0 {
			return
		}
		if len(res.Buckets) > 1026 || res.Truncated || res.Buckets[0].Key != lo || res.Buckets[len(res.Buckets)-1].Key != hi {
			t.Fatalf("%v..%v: %d buckets, truncated %v", lo, hi, len(res.Buckets), res.Truncated)
		}
		for i := 1; i < len(res.Buckets); i++ {
			k, _ := res.Buckets[i].Key.(float64)
			prev, _ := res.Buckets[i-1].Key.(float64)
			if !(k > prev) {
				t.Fatalf("keys %v and %v out of order", prev, k)
			}
		}
	})
}
