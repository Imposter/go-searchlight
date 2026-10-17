package search

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"testing"
)

// legacyAgg is a result as the encoding before the appenders wrote it: through
// encoding/json, a bucket a map of its members and sub-aggregations.
func legacyAgg(a *AggResult) any {
	switch a.Type {
	case AggStats:
		return struct {
			Count int64    `json:"count"`
			Min   *float64 `json:"min"`
			Max   *float64 `json:"max"`
			Avg   *float64 `json:"avg"`
			Sum   float64  `json:"sum"`
		}{a.Count, a.Min, a.Max, a.Avg, a.Sum}
	case AggCardinality:
		return struct {
			Value int64 `json:"value"`
		}{a.Value}
	case AggMin, AggMax:
		return struct {
			Value *float64 `json:"value"`
		}{a.Number}
	case AggFilter, AggGlobal:
		return legacyInline(a.DocCount, a.Aggs)
	case AggFilters:
		keyed := map[string]map[string]any{}
		for _, b := range a.Buckets {
			key, _ := b.Key.(string)
			keyed[key] = legacyInline(b.DocCount, b.Aggs)
		}
		return struct {
			Buckets map[string]map[string]any `json:"buckets"`
		}{keyed}
	}
	bs := []any{}
	for _, b := range a.Buckets {
		bs = append(bs, legacyBucket(b))
	}
	if a.Type == AggTerms {
		return struct {
			Error     int64 `json:"doc_count_error_upper_bound"`
			Other     int64 `json:"sum_other_doc_count"`
			Buckets   []any `json:"buckets"`
			Truncated bool  `json:"truncated,omitempty"`
		}{a.DocCountErrorUpperBound, a.SumOtherDocCount, bs, a.Truncated}
	}
	return struct {
		Buckets   []any `json:"buckets"`
		Truncated bool  `json:"truncated,omitempty"`
	}{bs, a.Truncated}
}

func legacyInline(docCount int64, aggs map[string]*AggResult) map[string]any {
	m := map[string]any{}
	for name, sub := range aggs {
		m[name] = legacyAgg(sub)
	}
	m["doc_count"] = docCount
	return m
}

func legacyBucket(b *Bucket) map[string]any {
	m := map[string]any{}
	for name, sub := range b.Aggs {
		m[name] = legacyAgg(sub)
	}
	m["key"] = b.Key
	if b.KeyAsString != "" {
		m["key_as_string"] = b.KeyAsString
	}
	if b.From != nil {
		m["from"] = *b.From
	}
	if b.To != nil {
		m["to"] = *b.To
	}
	m["doc_count"] = b.DocCount
	if b.DocCountErrorUpperBound != 0 {
		m["doc_count_error_upper_bound"] = b.DocCountErrorUpperBound
	}
	return m
}

func randomFloat(rng *rand.Rand) float64 {
	return pick(rng, []float64{0, 1, -1, 2.5, 1e-7, 1.5e-9, 1e21, 3.25e22, -4e-300, 123456789012, 0.1, 1.7e12, 99.99})
}

func randomText(rng *rand.Rand) string {
	return pick(rng, []string{"", "a", "key", "nike air", `q"uote`, `back\slash`, "<b>&amp;", "café", "\x01ctl\n", "line sep", "\xff\xfe", "doc_count", "z"})
}

func randomResult(rng *rand.Rand, depth int) *AggResult {
	num := func() *float64 {
		if rng.IntN(4) == 0 {
			return nil
		}
		return ptr(randomFloat(rng))
	}
	subs := func() map[string]*AggResult {
		if depth > 1 || rng.IntN(2) == 0 {
			return nil
		}
		m := map[string]*AggResult{}
		for range 1 + rng.IntN(3) {
			m[pick(rng, []string{"a", "m", "zz", "doc_counts", "key2", "from_", "é", "<x>"})] = randomResult(rng, depth+1)
		}
		return m
	}
	typ := pick(rng, []string{AggStats, AggCardinality, AggMin, AggMax, AggFilter, AggGlobal, AggFilters, AggTerms, AggRange, AggHistogram, AggDateHistogram})
	if depth > 1 {
		typ = pick(rng, []string{AggStats, AggCardinality, AggMin, AggMax})
	}
	a := &AggResult{
		Type: typ, Count: rng.Int64N(100), Sum: randomFloat(rng), Min: num(), Max: num(), Avg: num(),
		Value: rng.Int64N(1000), Number: num(), DocCount: rng.Int64N(50), Aggs: subs(), Truncated: rng.IntN(3) == 0,
		DocCountErrorUpperBound: rng.Int64N(3), SumOtherDocCount: rng.Int64N(9),
	}
	for range rng.IntN(4) {
		b := &Bucket{DocCount: rng.Int64N(20), DocCountErrorUpperBound: rng.Int64N(2), Aggs: subs()}
		switch rng.IntN(4) {
		case 0:
			b.Key = randomFloat(rng)
		case 1:
			b.Key = rng.IntN(2) == 0
		default:
			b.Key = randomText(rng)
		}
		if typ == AggFilters {
			b.Key = fmt.Sprintf("k%d", len(a.Buckets))
		}
		if rng.IntN(3) == 0 {
			b.KeyAsString = randomText(rng)
		}
		if rng.IntN(3) == 0 {
			b.From, b.To = num(), num()
		}
		a.Buckets = append(a.Buckets, b)
	}
	return a
}

// TestAggJSONEqualsEncodingJSON: results written by the appenders are byte for byte
// what encoding/json wrote of them through maps: member order, escaping, numbers.
func TestAggJSONEqualsEncodingJSON(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 11))
	for range 3000 {
		r := map[string]*AggResult{"x": randomResult(rng, 0)}
		got, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		want, err := json.Marshal(map[string]any{"x": legacyAgg(r["x"])})
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("got  %s\nwant %s", got, want)
		}
	}
}

// BenchmarkAggJSON: a terms result of 100 buckets, each with stats and min, written
// as the response does, and as encoding/json wrote it through maps before.
func BenchmarkAggJSON(b *testing.B) {
	res := &AggResult{Type: AggTerms}
	for i := range 100 {
		lo, hi, avg := 1.0, float64(i), 3.5
		res.Buckets = append(res.Buckets, &Bucket{Key: fmt.Sprintf("item-%06d", i), DocCount: int64(1000 - i), Aggs: map[string]*AggResult{
			"s": {Type: AggStats, Count: 9, Min: &lo, Max: &hi, Avg: &avg, Sum: 31.5},
			"m": {Type: AggMin, Number: &lo},
		}})
	}
	for name, r := range map[string]any{"appenders": map[string]*AggResult{"items": res}, "maps": map[string]any{"items": legacyAgg(res)}} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, err := json.Marshal(r); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
	b.Run("append", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := res.appendJSON(nil); err != nil {
				b.Fatal(err)
			}
		}
	})
}
