package search

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// exactQuantile is the linear interpolation between the ranks q*(n-1) falls between,
// over sorted values: what an exact digest answers.
func exactQuantile(sorted []float64, q float64) float64 {
	r := q * float64(len(sorted)-1)
	lo := int(math.Floor(r))
	a := sorted[lo]
	if frac := r - float64(lo); frac > 0 {
		return a + frac*(sorted[lo+1]-a)
	}
	return a
}

// exactRank is the fraction of sorted below x, plus half those equal to it.
func exactRank(sorted []float64, x float64) float64 {
	below, _ := slices.BinarySearch(sorted, x)
	end := below
	for end < len(sorted) && sorted[end] == x {
		end++
	}
	return (float64(below) + float64(end-below)/2) / float64(len(sorted))
}

// rankError is how far q is from the ranks estimate v holds among sorted: 0 when some
// copy of v sits at rank q, or when v falls between two neighbouring values (which an
// interpolation between them may answer) and q is within their ranks.
func rankError(sorted []float64, v, q float64) float64 {
	n := float64(len(sorted))
	lo, found := slices.BinarySearch(sorted, v)
	hi := lo
	for hi < len(sorted) && sorted[hi] == v {
		hi++
	}
	if !found {
		if lo > 0 {
			lo, _ = slices.BinarySearch(sorted, sorted[lo-1])
		}
		if hi < len(sorted) {
			next := sorted[hi]
			for hi < len(sorted) && sorted[hi] == next {
				hi++
			}
		}
	}
	from, to := float64(lo)/n, float64(hi)/n
	switch {
	case q < from:
		return from - q
	case q > to:
		return q - to
	}
	return 0
}

// tailTolerance and midTolerance bound a compression-100 digest's rank error: the
// percents at or beyond 1 and 99, and those between.
const (
	tailTolerance = 0.002
	midTolerance  = 0.01
)

var accuracyQuantiles = []float64{0, 0.001, 0.01, 0.05, 0.1, 0.25, 0.333, 0.5, 0.666, 0.75, 0.9, 0.95, 0.99, 0.999, 1}

func tolerance(q float64) float64 {
	if q <= 0.01 || q >= 0.99 {
		return tailTolerance
	}
	return midTolerance
}

// distributions are value generators for the accuracy tests: smooth, skewed, heavy
// tailed, and prices in cents with many repeats.
var distributions = map[string]func(rng *rand.Rand) float64{
	"uniform":     func(rng *rand.Rand) float64 { return rng.Float64() * 1000 },
	"normal":      func(rng *rand.Rand) float64 { return 500 + 100*rng.NormFloat64() },
	"exponential": func(rng *rand.Rand) float64 { return 50 * rng.ExpFloat64() },
	"lognormal":   func(rng *rand.Rand) float64 { return math.Exp(3 + 1.5*rng.NormFloat64()) },
	"prices":      func(rng *rand.Rand) float64 { return math.Round(math.Exp(3+rng.NormFloat64())*100) / 100 },
}

func digestOf(compression float64, values []float64) *TDigest {
	td := NewTDigest(compression)
	for _, v := range values {
		td.Add(v)
	}
	td.seal()
	return td
}

// checkDigest reports a digest's worst rank errors, of Quantile and of Rank, against
// sorted, and fails past the tolerances.
func checkDigest(t *testing.T, name string, td *TDigest, sorted []float64) (worstTail, worstMid, worstRank float64) {
	t.Helper()
	for _, q := range accuracyQuantiles {
		v, ok := td.Quantile(q)
		if !ok {
			t.Fatalf("%s: no quantile %g", name, q)
		}
		e := rankError(sorted, v, q)
		if q <= 0.01 || q >= 0.99 {
			worstTail = max(worstTail, e)
		} else {
			worstMid = max(worstMid, e)
		}
		if e > tolerance(q) {
			t.Errorf("%s: quantile %g is %g, %g ranks off (tolerance %g)", name, q, v, e, tolerance(q))
		}
		if q == 0 && v != sorted[0] || q == 1 && v != sorted[len(sorted)-1] {
			t.Errorf("%s: quantile %g is %g, want the exact %g", name, q, v, exactQuantile(sorted, q))
		}
	}
	for _, q := range accuracyQuantiles {
		x := exactQuantile(sorted, q)
		got, _ := td.Rank(x)
		e := rankError(sorted, x, got) // within the ranks x's copies hold

		worstRank = max(worstRank, e)
		if e > tolerance(q) {
			t.Errorf("%s: rank of %g is %g, want %g", name, x, got, exactRank(sorted, x))
		}
	}
	return worstTail, worstMid, worstRank
}

// TestTDigestAccuracy: over 100,000 values of skewed, heavy-tailed and repetitive
// distributions, a compression-100 digest's percentiles are within 0.2% of a rank at
// and beyond the 1st and 99th, within 1% between, and min and max exact; its ranks are
// within the same of the exact ones.
func TestTDigestAccuracy(t *testing.T) {
	seed := testSeed(t)
	n := 100_000
	if longMode() {
		n = 1_000_000
	}
	for _, name := range slices.Sorted(maps.Keys(distributions)) {
		rng := rand.New(rand.NewPCG(seed, uint64(len(name))))
		values := make([]float64, n)
		for i := range values {
			values[i] = distributions[name](rng)
		}
		td := digestOf(DefaultCompression, values)
		sorted := slices.Sorted(slices.Values(values))
		if !td.Approx || len(td.Means) > td.maxCentroids() || !td.wellFormed(DefaultCompression) {
			t.Fatalf("%s: approx %v with %d centroids, well formed %v", name, td.Approx, len(td.Means), td.wellFormed(DefaultCompression))
		}
		tail, mid, rank := checkDigest(t, name, td, sorted)
		t.Logf("%s: %d values, %d centroids: worst rank error %.5f at the tails, %.5f between, %.5f of a rank", name, n, len(td.Means), tail, mid, rank)
	}
}

// TestTDigestExactWhileSmall: while the values hold at most the compression's distinct
// values, however many times each repeats, the digest is exact: its percentiles are the
// linear interpolation between ranks, its ranks the exact mid-ranks.
func TestTDigestExactWhileSmall(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x7d))
	for _, tc := range []struct {
		distinct, n int
		compression float64
	}{{1, 1, 100}, {1, 50, 100}, {2, 3, 100}, {100, 100, 100}, {80, 20_000, 100}, {100, 50_000, 100}, {10, 5_000, 10}, {500, 10_000, 500}} {
		name := fmt.Sprintf("%d distinct of %d at %g", tc.distinct, tc.n, tc.compression)
		pool := make([]float64, tc.distinct)
		for i := range pool {
			pool[i] = math.Round(rng.NormFloat64()*1e4) / 100
		}
		slices.Sort(pool)
		pool = slices.Compact(pool)
		values := make([]float64, tc.n)
		for i := range values {
			values[i] = pool[i%len(pool)]
			if i >= len(pool) {
				values[i] = pick(rng, pool)
			}
		}
		td := digestOf(tc.compression, values)
		if td.Approx {
			t.Fatalf("%s: approximate", name)
		}
		sorted := slices.Sorted(slices.Values(values))
		for _, q := range append(slices.Clone(accuracyQuantiles), 0.123, 0.5005) {
			if got, _ := td.Quantile(q); got != exactQuantile(sorted, q) {
				t.Errorf("%s: quantile %g is %g, want %g", name, q, got, exactQuantile(sorted, q))
			}
		}
		for _, x := range append(slices.Clone(pool), pool[0]-1, pool[len(pool)-1]+1, (pool[0]+pool[len(pool)-1])/2) {
			if got, _ := td.Rank(x); got != exactRank(sorted, x) {
				t.Errorf("%s: rank of %g is %g, want %g", name, x, got, exactRank(sorted, x))
			}
		}
	}
	more := make([]float64, 101)
	for i := range more {
		more[i] = float64(i)
	}
	if !digestOf(100, more).Approx {
		t.Error("101 distinct values at compression 100 kept exact")
	}
	empty := NewTDigest(100)
	if _, ok := empty.Quantile(0.5); ok {
		t.Error("an empty digest has a median")
	}
	if _, ok := empty.Rank(1); ok {
		t.Error("an empty digest has a rank")
	}
}

// TestTDigestMergeEquivalence: digests of the parts of a set of values, merged in any
// order, are the digest of the whole: exactly while exact, within the tolerances
// once approximate; merging leaves its argument as it was, and a digest crosses JSON
// (the wire and the shared request cache) as it is.
func TestTDigestMergeEquivalence(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x3e))
	for _, tc := range []struct {
		name   string
		n      int
		values func() float64
	}{
		{"exact", 30_000, func() float64 { return float64(rng.IntN(90)) }},
		{"approximate", 60_000, func() float64 { return 50 * rng.ExpFloat64() }},
		{"crossing", 5_000, func() float64 { return float64(rng.IntN(150)) }},
	} {
		values := make([]float64, tc.n)
		for i := range values {
			values[i] = tc.values()
		}
		sorted := slices.Sorted(slices.Values(values))
		whole := digestOf(DefaultCompression, values)
		for _, parts := range []int{2, 3, 7, 16, 64} {
			name := fmt.Sprintf("%s in %d parts", tc.name, parts)
			split := make([][]float64, parts)
			for _, v := range values {
				k := rng.IntN(parts)
				split[k] = append(split[k], v)
			}
			digests := make([]*TDigest, parts)
			for k := range split {
				digests[k] = digestOf(DefaultCompression, split[k])
			}
			merged := NewTDigest(DefaultCompression)
			for _, k := range rng.Perm(parts) {
				before, _ := json.Marshal(digests[k])
				merged.Merge(digests[k])
				if after, _ := json.Marshal(digests[k]); !bytes.Equal(after, before) {
					t.Fatalf("%s: Merge changed its argument", name)
				}
			}
			raw, err := json.Marshal(merged)
			if err != nil {
				t.Fatal(err)
			}
			var back TDigest
			if err := json.Unmarshal(raw, &back); err != nil {
				t.Fatal(err)
			}
			if !back.wellFormed(DefaultCompression) || back.Count() != int64(tc.n) {
				t.Fatalf("%s: after JSON, well formed %v, count %d", name, back.wellFormed(DefaultCompression), back.Count())
			}
			if merged.Approx != whole.Approx && tc.name != "crossing" {
				t.Fatalf("%s: merged approx %v, whole %v", name, merged.Approx, whole.Approx)
			}
			for _, q := range accuracyQuantiles {
				got, _ := back.Quantile(q)
				want, _ := whole.Quantile(q)
				if !whole.Approx && got != want {
					t.Errorf("%s: quantile %g merged %g, whole %g", name, q, got, want)
				}
			}
			if whole.Approx {
				tail, mid, rank := checkDigest(t, name, &back, sorted)
				if parts == 64 {
					t.Logf("%s: worst rank error %.5f at the tails, %.5f between, %.5f of a rank", name, tail, mid, rank)
				}
			}
		}
	}
}

// percentileDocs is a cluster of shards over documents of integer prices below 100
// and dates over a few days (few enough distinct values to be exact), refreshed in
// several segments, with deletes.
func percentileDocs(t *testing.T, rng *rand.Rand, shards, n int) *cluster {
	t.Helper()
	c := newCluster(t, shards)
	brands := []string{"acme", "bolt", "core", "dyna"}
	for i := range n {
		var members []string
		if rng.IntN(10) > 0 {
			members = append(members, `"price":`+strconv.Itoa(rng.IntN(100)))
		}
		if rng.IntN(4) > 0 {
			members = append(members, `"created":`+strconv.FormatInt(1_700_000_000_000+rng.Int64N(40)*3_600_000, 10))
		}
		members = append(members, `"brand":`+jsonString(pick(rng, brands)), `"active":`+strconv.FormatBool(rng.IntN(2) == 0))
		c.upsert(fmt.Sprintf("d%05d", i), "{"+strings.Join(members, ",")+"}")
		if i%(n/4) == n/4-1 {
			c.refreshAll()
		}
	}
	for range n / 20 {
		c.remove(fmt.Sprintf("d%05d", rng.IntN(n)))
	}
	c.refreshAll()
	return c
}

// numbers are docs' values of field, sorted.
func numbers(docs []*schema.Doc, field string) []float64 {
	var out []float64
	for _, d := range docs {
		for _, v := range aggValues(d, field) {
			if x, ok := v.(float64); ok {
				out = append(out, x)
			}
		}
	}
	slices.Sort(out)
	return out
}

// checkPercentiles compares a percentiles or percentile_ranks result with the exact
// answer over sorted.
func checkPercentiles(t *testing.T, name string, res *AggResult, keys, sorted []float64, ranks bool) {
	t.Helper()
	if res == nil || !slices.Equal(res.Keys, keys) || len(res.Percentiles) != len(keys) {
		t.Fatalf("%s: %+v, want keys %v", name, res, keys)
	}
	for i, k := range keys {
		got := res.Percentiles[i]
		if len(sorted) == 0 {
			if got != nil {
				t.Errorf("%s: %g is %g with no values", name, k, *got)
			}
			continue
		}
		var want float64
		if ranks {
			want = exactRank(sorted, k) * 100
		} else {
			want = exactQuantile(sorted, k/100)
		}
		if got == nil || *got != want {
			t.Errorf("%s: %g is %v, want %g", name, k, deref(got), want)
		}
	}
}

// TestPercentilesEqualBruteForce: percentiles and percentile_ranks, top-level and under
// terms (collected depth or breadth first), histogram, range, filter, filters and global, over several shards of several
// segments each, collected whole and in parts, answer exactly what the values say while
// each bucket holds at most the compression's distinct values.
func TestPercentilesEqualBruteForce(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x9c))
	c := percentileDocs(t, rng, 3, 2400)
	defer func() { aggChunkMin, aggChunk = 1<<15, 1<<16 }()
	pcts := []float64{0, 1, 5, 25, 50, 75, 95, 99, 99.9, 100}
	values := []float64{-1, 0, 10, 49.5, 50, 99, 1000}
	pct := func(field string) Agg { return Agg{Type: AggPercentiles, Field: field, Percents: pcts} }
	ranks := Agg{Type: AggPercentileRanks, Field: "price", Values: values}
	subs := map[string]Agg{"p": pct("price"), "r": ranks, "d": {Type: AggPercentiles, Field: "created"}}
	aggs := map[string]Agg{
		"p":       pct("price"),
		"r":       ranks,
		"dates":   {Type: AggPercentiles, Field: "created"},
		"brands":  {Type: AggTerms, Field: "brand", Aggs: subs},
		"later":   {Type: AggTerms, Field: "brand", CollectMode: CollectBreadthFirst, Aggs: subs},
		"hist":    {Type: AggHistogram, Field: "price", Interval: 25, Aggs: subs},
		"bands":   {Type: AggRange, Field: "price", Ranges: []Range{{To: ptr(30.0)}, {From: ptr(30.0)}}, Aggs: subs},
		"active":  {Type: AggFilter, Filter: mustParse(t, `{"field":"active","op":"eq","value":true}`), Aggs: map[string]Agg{"p": pct("price"), "t": {Type: AggTerms, Field: "brand", Aggs: subs}}},
		"buckets": {Type: AggFilters, Filters: map[string]query.Node{"cheap": mustParse(t, `{"field":"price","op":"lt","value":20}`)}, OtherBucketKey: "rest", Aggs: subs},
		"all":     {Type: AggGlobal, Aggs: subs},
	}
	queries := []string{`{"all":[]}`, `{"field":"price","op":"gte","value":40}`, `{"field":"brand","op":"eq","value":"acme"}`, `{"field":"price","op":"lt","value":0}`}
	for _, chunked := range []bool{false, true} {
		if chunked {
			aggChunkMin, aggChunk = 1, 1+rng.Uint32N(200)
		} else {
			aggChunkMin, aggChunk = 1<<15, 1<<16
		}
		for _, raw := range queries {
			q := mustParse(t, raw)
			got, err := c.search(&Request{Query: q, Aggs: aggs, TrackTotal: TrackTotalAll})
			if err != nil {
				t.Fatalf("%s: %v", raw, err)
			}
			hits := brute(c.docs(), q, nil)
			name := fmt.Sprintf("%s (chunked %v)", raw, chunked)
			checkSubs := func(where string, res map[string]*AggResult, docs []*schema.Doc) {
				t.Helper()
				checkPercentiles(t, name+" "+where+".p", res["p"], pcts, numbers(docs, "price"), false)
				checkPercentiles(t, name+" "+where+".r", res["r"], values, numbers(docs, "price"), true)
				checkPercentiles(t, name+" "+where+".d", res["d"], DefaultPercents, numbers(docs, "created"), false)
			}
			checkPercentiles(t, name+" p", got.Aggs["p"], pcts, numbers(hits, "price"), false)
			checkPercentiles(t, name+" r", got.Aggs["r"], values, numbers(hits, "price"), true)
			checkPercentiles(t, name+" dates", got.Aggs["dates"], DefaultPercents, numbers(hits, "created"), false)
			for _, b := range got.Aggs["brands"].Buckets {
				var in []*schema.Doc
				for _, d := range hits {
					if slices.Contains(aggValues(d, "brand"), b.Key) {
						in = append(in, d)
					}
				}
				checkSubs(fmt.Sprintf("brands.%v", b.Key), b.Aggs, in)
			}
			for _, b := range got.Aggs["later"].Buckets {
				var in []*schema.Doc
				for _, d := range hits {
					if slices.Contains(aggValues(d, "brand"), b.Key) {
						in = append(in, d)
					}
				}
				checkSubs(fmt.Sprintf("later.%v", b.Key), b.Aggs, in)
			}
			for _, b := range got.Aggs["hist"].Buckets {
				k, _ := b.Key.(float64)
				var in []*schema.Doc
				for _, d := range hits {
					if xs := numbers([]*schema.Doc{d}, "price"); len(xs) > 0 && xs[0] >= k && xs[0] < k+25 {
						in = append(in, d)
					}
				}
				checkSubs(fmt.Sprintf("hist.%g", k), b.Aggs, in)
			}
			for i, b := range got.Aggs["bands"].Buckets {
				var in []*schema.Doc
				for _, d := range hits {
					if xs := numbers([]*schema.Doc{d}, "price"); len(xs) > 0 && (xs[0] < 30) == (i == 0) {
						in = append(in, d)
					}
				}
				checkSubs(fmt.Sprintf("bands.%d", i), b.Aggs, in)
			}
			active := brute(hits, aggs["active"].Filter, nil)
			checkPercentiles(t, name+" active.p", got.Aggs["active"].Aggs["p"], pcts, numbers(active, "price"), false)
			for _, b := range got.Aggs["active"].Aggs["t"].Buckets {
				var in []*schema.Doc
				for _, d := range active {
					if slices.Contains(aggValues(d, "brand"), b.Key) {
						in = append(in, d)
					}
				}
				checkSubs(fmt.Sprintf("active.t.%v", b.Key), b.Aggs, in)
			}
			cheap := brute(hits, aggs["buckets"].Filters["cheap"], nil)
			var rest []*schema.Doc
			for _, d := range hits {
				if !slices.Contains(cheap, d) {
					rest = append(rest, d)
				}
			}
			for _, b := range got.Aggs["buckets"].Buckets {
				if b.Key == "cheap" {
					checkSubs("buckets.cheap", b.Aggs, cheap)
				} else {
					checkSubs("buckets.rest", b.Aggs, rest)
				}
			}
			checkSubs("all", got.Aggs["all"].Aggs, c.docs())
		}
	}
}

// TestPercentilesAcrossShards: one set of documents searched as one shard and as five
// answers alike: exactly while exact, and within the tolerances of the exact answer,
// and of each other, once approximate.
func TestPercentilesAcrossShards(t *testing.T) {
	seed := testSeed(t)
	n := 12_000
	bodies := make([]string, n)
	rng := rand.New(rand.NewPCG(seed, seed^0x51))
	for i := range bodies {
		bodies[i] = fmt.Sprintf(`{"price":%s,"created":%d,"brand":%q}`,
			strconv.FormatFloat(math.Round(rng.ExpFloat64()*5000)/100, 'f', -1, 64), 1_700_000_000_000+rng.Int64N(90), pick(rng, []string{"a", "b"}))
	}
	pcts := []float64{0, 1, 5, 25, 50, 75, 95, 99, 99.9, 100}
	aggs := map[string]Agg{
		"p":     {Type: AggPercentiles, Field: "price", Percents: pcts},
		"wide":  {Type: AggPercentiles, Field: "price", Percents: pcts, Compression: 400},
		"exact": {Type: AggPercentiles, Field: "created", Percents: pcts},
		"t":     {Type: AggTerms, Field: "brand", Aggs: map[string]Agg{"p": {Type: AggPercentiles, Field: "price", Percents: pcts}}},
	}
	results := map[int]*Response{}
	var docs []*schema.Doc
	for _, shards := range []int{1, 5} {
		c := newCluster(t, shards)
		for i, body := range bodies {
			c.upsert(fmt.Sprintf("d%05d", i), body)
			if i%3000 == 2999 {
				c.refreshAll()
			}
		}
		c.refreshAll()
		docs = c.docs()
		got, err := c.search(&Request{Query: &query.All{}, Aggs: aggs})
		if err != nil {
			t.Fatal(err)
		}
		results[shards] = got
	}
	prices := numbers(docs, "price")
	one, five := results[1], results[5]
	for _, name := range []string{"p", "wide"} {
		for i, q := range pcts {
			a, b := *one.Aggs[name].Percentiles[i], *five.Aggs[name].Percentiles[i]
			if ea, eb := rankError(prices, a, q/100), rankError(prices, b, q/100); ea > tolerance(q/100) || eb > tolerance(q/100) {
				t.Errorf("%s at %g: one shard %g (%g ranks off), five %g (%g ranks off)", name, q, a, ea, b, eb)
			}
			if d := math.Abs(exactRank(prices, a) - exactRank(prices, b)); d > tolerance(q/100) {
				t.Errorf("%s at %g: one shard %g and five %g are %g ranks apart", name, q, a, b, d)
			}
		}
	}
	checkPercentiles(t, "one shard's exact", one.Aggs["exact"], pcts, numbers(docs, "created"), false)
	checkPercentiles(t, "five shards' exact", five.Aggs["exact"], pcts, numbers(docs, "created"), false)
	for _, b := range five.Aggs["t"].Buckets {
		var in []*schema.Doc
		for _, d := range docs {
			if slices.Contains(aggValues(d, "brand"), b.Key) {
				in = append(in, d)
			}
		}
		sorted := numbers(in, "price")
		for i, q := range pcts {
			if e := rankError(sorted, *b.Aggs["p"].Percentiles[i], q/100); e > tolerance(q/100) {
				t.Errorf("t.%v at %g: %g ranks off", b.Key, q, e)
			}
		}
	}
}

// TestNestedPercentilesEqualBruteForce: percentiles and percentile_ranks on a nested
// field's sub-field read every chosen object's value (where), at the top level, under
// filter and filters, and under a terms aggregation on a sub-field of the same nested
// field (the bucket's chosen objects holding its value).
func TestNestedPercentilesEqualBruteForce(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x4b))
	c := variantCluster(t, rng, 900)
	docs := c.docs()
	pcts := []float64{0, 10, 50, 90, 100}
	values := []float64{0, 5, 15, 29}
	for range 30 {
		whereRaw := ""
		if rng.IntN(3) > 0 {
			whereRaw = randomInnerJSON(rng, 0)
		}
		whereNode, where := compileWhere(t, whereRaw)
		p := Agg{Type: AggPercentiles, Field: "_v.price", Percents: pcts, Where: whereNode}
		r := Agg{Type: AggPercentileRanks, Field: "_v.price", Values: values, Where: whereNode}
		cheap := mustParse(t, `{"field":"price","op":"lt","value":50}`)
		aggs := map[string]Agg{
			"p":     p,
			"r":     r,
			"in":    {Type: AggFilter, Filter: cheap, Aggs: map[string]Agg{"p": p}},
			"many":  {Type: AggFilters, Filters: map[string]query.Node{"cheap": cheap}, Aggs: map[string]Agg{"r": r}},
			"sizes": {Type: AggTerms, Field: "_v.size", Size: 100, Where: whereNode, Aggs: map[string]Agg{"p": {Type: AggPercentiles, Field: "_v.price", Percents: pcts}}},
		}
		got, err := c.search(&Request{Query: &query.All{}, Aggs: aggs})
		if err != nil {
			t.Fatalf("where %s: %v", whereRaw, err)
		}
		prices := func(in []*schema.Doc, size any) []float64 {
			var out []float64
			for _, d := range in {
				chosen(d, where, func(o *schema.Object) {
					if size != nil && !slices.Contains(keysOf(o, "_v.size"), size) {
						return
					}
					if v := o.Fields["_v.price"].Number; v != nil {
						out = append(out, positiveZero(*v))
					}
				})
			}
			slices.Sort(out)
			return out
		}
		name := "where " + whereRaw
		checkPercentiles(t, name+" p", got.Aggs["p"], pcts, prices(docs, nil), false)
		checkPercentiles(t, name+" r", got.Aggs["r"], values, prices(docs, nil), true)
		cheapDocs := brute(docs, cheap, nil)
		checkPercentiles(t, name+" in.p", got.Aggs["in"].Aggs["p"], pcts, prices(cheapDocs, nil), false)
		checkPercentiles(t, name+" many.cheap.r", got.Aggs["many"].Buckets[0].Aggs["r"], values, prices(cheapDocs, nil), true)
		for _, b := range got.Aggs["sizes"].Buckets {
			checkPercentiles(t, fmt.Sprintf("%s sizes.%v.p", name, b.Key), b.Aggs["p"], pcts, prices(docs, b.Key), false)
		}
	}
}

// TestPercentilesProblems: percents outside 0 to 100, given twice, too many or none,
// a compression out of bounds, unknown settings, a field of another type, a
// percentile_ranks without values and sub-aggregations under either are each a problem
// at their loc, and every percentiles aggregation counts towards MaxAggs.
func TestPercentilesProblems(t *testing.T) {
	c := newCluster(t, 1)
	c.upsert("a", `{"price":3,"brand":"x"}`)
	c.refreshAll()
	many := make([]string, MaxPercents+1)
	for i := range many {
		many[i] = strconv.FormatFloat(float64(i)*0.5, 'f', -1, 64)
	}
	for raw, want := range map[string]string{
		`{"percentiles":{"field":"price","percents":[50,101]}}`:                          "aggs.p.percentiles.percents.1",
		`{"percentiles":{"field":"price","percents":[-1]}}`:                              "aggs.p.percentiles.percents.0",
		`{"percentiles":{"field":"price","percents":[50,25,50]}}`:                        "aggs.p.percentiles.percents",
		`{"percentiles":{"field":"price","percents":[]}}`:                                "aggs.p.percentiles.percents",
		`{"percentiles":{"field":"price","percents":"50"}}`:                              "aggs.p.percentiles.percents",
		`{"percentiles":{"field":"price","percents":[` + strings.Join(many, ",") + `]}}`: "aggs.p.percentiles.percents",
		`{"percentiles":{"field":"price","tdigest":{"compression":5}}}`:                  "aggs.p.percentiles.tdigest.compression",
		`{"percentiles":{"field":"price","tdigest":{"compression":5000}}}`:               "aggs.p.percentiles.tdigest.compression",
		`{"percentiles":{"field":"price","tdigest":{"execution_hint":"x"}}}`:             "aggs.p.percentiles.tdigest.execution_hint",
		`{"percentiles":{"field":"price","tdigest":100}}`:                                "aggs.p.percentiles.tdigest",
		`{"percentiles":{"field":"price","keyed":"yes"}}`:                                "aggs.p.percentiles.keyed",
		`{"percentiles":{"field":"price","hdr":{}}}`:                                     "aggs.p.percentiles.hdr",
		`{"percentiles":{"field":"price","values":[1]}}`:                                 "aggs.p.percentiles.values",
		`{"percentiles":{"field":"brand"}}`:                                              "aggs.p.percentiles.field",
		`{"percentiles":{}}`:                                                             "aggs.p.percentiles.field",
		`{"percentile_ranks":{"field":"price"}}`:                                         "aggs.p.percentile_ranks.values",
		`{"percentile_ranks":{"field":"price","values":[1,1]}}`:                          "aggs.p.percentile_ranks.values",
		`{"percentile_ranks":{"field":"price","percents":[1]}}`:                          "aggs.p.percentile_ranks.percents",
		`{"percentiles":{"field":"price","where":{"all":[]}}}`:                           "aggs.p.percentiles.where",
		`{"percentiles":{"field":"price"},"aggs":{"s":{"stats":{"field":"price"}}}}`:     "aggs.p.aggs",
	} {
		body := `{"size":0,"aggs":{"p":` + raw + `}}`
		r, ps := ParseRequest([]byte(body))
		if len(ps) == 0 {
			_, err := c.search(r)
			var re *RequestError
			if !asRequestError(err, &re) {
				t.Errorf("%s: %v, want a request error", raw, err)
				continue
			}
			ps = re.Problems
		}
		if len(ps) == 0 || ps[0].Loc != want {
			t.Errorf("%s: problems %v, want one at %s", raw, ps, want)
		}
	}
	for name, a := range map[string]Agg{
		"a compression past the bounds": {Type: AggPercentiles, Field: "price", Compression: MaxCompression + 1},
		"a NaN percent":                 {Type: AggPercentiles, Field: "price", Percents: []float64{math.NaN()}},
		"no values":                     {Type: AggPercentileRanks, Field: "price"},
		"an infinite value":             {Type: AggPercentileRanks, Field: "price", Values: []float64{math.Inf(1)}},
		"a nested field's sub-field under terms": {Type: AggTerms, Field: "brand", Aggs: map[string]Agg{
			"p": {Type: AggPercentiles, Field: "_v.price"},
		}},
	} {
		var re *RequestError
		if _, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"p": a}}); !asRequestError(err, &re) {
			t.Errorf("%s: %v, want a request error", name, err)
		}
	}
	aggs := map[string]Agg{}
	for i := range MaxAggs + 1 {
		aggs[fmt.Sprintf("p%d", i)] = Agg{Type: AggPercentiles, Field: "price"}
	}
	var re *RequestError
	if _, err := c.search(&Request{Query: &query.All{}, Aggs: aggs}); !asRequestError(err, &re) || re.Problems[0].Loc != "aggs" {
		t.Errorf("%d percentiles aggregations: %v, want a problem at aggs", MaxAggs+1, err)
	}
	r := parseReq(t, `{"size":0,"aggs":{"p":{"percentiles":{"field":"price","percents":[99,50],"keyed":false,"tdigest":{"compression":250}}},
		"r":{"percentile_ranks":{"field":"price","values":[7,3]}}}}`)
	if p := r.Aggs["p"]; !slices.Equal(p.Percents, []float64{99, 50}) || !p.Unkeyed || p.Compression != 250 || !slices.Equal(r.Aggs["r"].Values, []float64{7, 3}) {
		t.Errorf("parsed %+v", r.Aggs)
	}
}

// TestPercentilesJSON: keyed, a result is an object by each key as Elasticsearch
// writes it ("50.0"), unkeyed a list of {key, value}; a key with no value is null.
func TestPercentilesJSON(t *testing.T) {
	c := newCluster(t, 2)
	for i, p := range []int{10, 20, 30, 40} {
		c.upsert(fmt.Sprintf("d%d", i), fmt.Sprintf(`{"price":%d}`, p))
	}
	c.refreshAll()
	for raw, want := range map[string]string{
		`{"percentiles":{"field":"price"}}`:                                           `{"values":{"1.0":10.3,"5.0":11.5,"25.0":17.5,"50.0":25,"75.0":32.5,"95.0":38.5,"99.0":39.7}}`,
		`{"percentiles":{"field":"price","percents":[50,0.0001,99.9],"keyed":false}}`: `{"values":[{"key":0.0001,"value":10.00003},{"key":50,"value":25},{"key":99.9,"value":39.97}]}`,
		`{"percentiles":{"field":"price","percents":[0.0001,100]}}`:                   `{"values":{"1.0E-4":10.00003,"100.0":40}}`,
		`{"percentile_ranks":{"field":"price","values":[20,25,1500000000000]}}`:       `{"values":{"20.0":37.5,"25.0":50,"1.5E12":100}}`,
		`{"percentiles":{"field":"created","percents":[50]}}`:                         `{"values":{"50.0":null}}`,
		`{"percentile_ranks":{"field":"created","values":[5],"keyed":false}}`:         `{"values":[{"key":5,"value":null}]}`,
	} {
		r := parseReq(t, `{"size":0,"aggs":{"p":`+raw+`}}`)
		got, err := c.search(r)
		if err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		js, err := json.Marshal(got.Aggs["p"])
		if err != nil {
			t.Fatal(err)
		}
		if !jsonEquivalent(t, js, want) {
			t.Errorf("%s: %s, want %s", raw, js, want)
		}
		if !json.Valid(js) {
			t.Errorf("%s: invalid JSON %s", raw, js)
		}
	}
	for f, want := range map[float64]string{0: "0.0", 50: "50.0", 99.9: "99.9", 0.001: "0.001", 0.0001: "1.0E-4", 1.25e-5: "1.25E-5", 1e7: "1.0E7", 1.5e12: "1.5E12", -3: "-3.0", 9_999_999: "9999999.0"} {
		if got := string(appendJavaDouble(nil, f)); got != want {
			t.Errorf("appendJavaDouble(%g) = %s, want %s", f, got, want)
		}
	}
}

// jsonEquivalent reports whether got and want are the same JSON: numbers compared
// within 1e-9, the order of an object's members aside.
func jsonEquivalent(t *testing.T, got []byte, want string) bool {
	t.Helper()
	var a, b any
	if json.Unmarshal(got, &a) != nil || json.Unmarshal([]byte(want), &b) != nil {
		return false
	}
	var same func(x, y any) bool
	same = func(x, y any) bool {
		switch x := x.(type) {
		case map[string]any:
			ym, ok := y.(map[string]any)
			if !ok || len(x) != len(ym) {
				return false
			}
			for k, v := range x {
				if w, ok := ym[k]; !ok || !same(v, w) {
					return false
				}
			}
			return true
		case []any:
			ys, ok := y.([]any)
			if !ok || len(x) != len(ys) {
				return false
			}
			for i := range x {
				if !same(x[i], ys[i]) {
					return false
				}
			}
			return true
		case float64:
			yf, ok := y.(float64)
			return ok && math.Abs(x-yf) <= 1e-9*max(1, math.Abs(yf))
		}
		return x == y
	}
	return same(a, b)
}

// TestPercentilesRequestCache: the request cache's key holds a percentiles
// aggregation's percents, compression and keyed, and a percentile_ranks' values; a
// cached digest decodes only well formed and of its request's compression, and only
// for a percentiles or percentile_ranks aggregation.
func TestPercentilesRequestCache(t *testing.T) {
	base := `{"size":0,"aggs":{"p":{"percentiles":{"field":"price"}}}}`
	kb, ok := requestCacheKey(parseReq(t, base))
	if !ok {
		t.Fatal("a percentiles search is not cacheable")
	}
	keys := map[string]bool{string(kb): true}
	for _, raw := range []string{
		`{"size":0,"aggs":{"p":{"percentiles":{"field":"price","percents":[50]}}}}`,
		`{"size":0,"aggs":{"p":{"percentiles":{"field":"price","percents":[50,99]}}}}`,
		`{"size":0,"aggs":{"p":{"percentiles":{"field":"price","keyed":false}}}}`,
		`{"size":0,"aggs":{"p":{"percentiles":{"field":"price","tdigest":{"compression":200}}}}}`,
		`{"size":0,"aggs":{"p":{"percentile_ranks":{"field":"price","values":[5]}}}}`,
		`{"size":0,"aggs":{"p":{"percentile_ranks":{"field":"price","values":[6]}}}}`,
		`{"size":0,"aggs":{"p":{"stats":{"field":"price"}}}}`,
	} {
		k, ok := requestCacheKey(parseReq(t, raw))
		if !ok || keys[string(k)] {
			t.Errorf("%s: key the same as another's (cacheable %v)", raw, ok)
		}
		keys[string(k)] = true
	}
	if k, _ := requestCacheKey(parseReq(t, `{"size":0,"aggs":{"p":{"percentiles":{"field":"price","keyed":true}}}}`)); !bytes.Equal(k, kb) {
		t.Error("keyed true, the default, changed the key")
	}

	c := newCluster(t, 1)
	for i := range 300 {
		c.upsert(fmt.Sprintf("d%d", i), fmt.Sprintf(`{"price":%d,"brand":"b%d"}`, i, i%3))
	}
	c.refreshAll()
	req := parseReq(t, `{"size":0,"aggs":{"p":{"percentiles":{"field":"price"}},"t":{"terms":{"field":"brand"},"aggs":{"r":{"percentile_ranks":{"field":"price","values":[50]}}}}}}`)
	g := c.shards[0].Acquire()
	res, err := ExecuteShard(t.Context(), g, req)
	g.Release()
	if err != nil {
		t.Fatal(err)
	}
	if d := res.Aggs["p"].Digest; d == nil || !d.Approx {
		t.Fatalf("300 distinct prices: digest %+v", d)
	}
	enc, err := encodeCachedResult(res, 8<<20)
	if err != nil {
		t.Fatal(err)
	}
	dec, err := decodeCachedResult(enc, 8<<20, req)
	if err != nil || !sameResult(t, res, dec) {
		t.Fatalf("a percentiles result did not survive the cache: %v", err)
	}
	if resultBytes(res) < 8*int64(len(res.Aggs["p"].Digest.Means)) {
		t.Error("resultBytes leaves the digest out")
	}

	small := parseReq(t, `{"size":0,"aggs":{"p":{"percentiles":{"field":"price"}},"s":{"stats":{"field":"price"}}}}`)
	ok1 := `{"total":3,"total_relation":"eq","aggs":{"p":{"type":"percentiles","digest":{"compression":100,"means":[1,2,4],"counts":[1,3,1],"min":1,"max":4}},"s":{"type":"stats","stats":{"count":1,"sum":1,"min":1,"max":1}}}}`
	if _, err := decodeCachedResult(encodeJSONValue(t, ok1), 4096, small); err != nil {
		t.Fatalf("a valid digest: %v", err)
	}
	for name, js := range map[string]string{
		"unsorted":            strings.Replace(ok1, `[1,2,4]`, `[1,4,2]`, 1),
		"repeated":            strings.Replace(ok1, `[1,2,4]`, `[1,2,2]`, 1),
		"a zero count":        strings.Replace(ok1, `[1,3,1]`, `[1,0,1]`, 1),
		"short counts":        strings.Replace(ok1, `[1,3,1]`, `[1,3]`, 1),
		"another compression": strings.Replace(ok1, `"compression":100`, `"compression":200`, 1),
		"outside min":         strings.Replace(ok1, `"min":1,"max":4}`, `"min":2,"max":4}`, 1),
		"exact past min":      strings.Replace(ok1, `"min":1,"max":4}`, `"min":0,"max":4}`, 1),
		"no centroids":        strings.Replace(ok1, `"means":[1,2,4],"counts":[1,3,1],`, ``, 1),
		"on stats":            strings.Replace(ok1, `"stats":{"count":1,"sum":1,"min":1,"max":1}`, `"digest":{"compression":100,"means":[1],"counts":[1],"min":1,"max":1}`, 1),
		"with a value":        strings.Replace(ok1, `"type":"percentiles",`, `"type":"percentiles","value":1,`, 1),
		"too many exact":      strings.Replace(ok1, `"compression":100,"means":[1,2,4],"counts":[1,3,1],"min":1,"max":4`, exactDigestJSON(101), 1),
	} {
		if res, err := decodeCachedResult(encodeJSONValue(t, js), 1<<20, small); err == nil {
			t.Errorf("%s: decoded %+v", name, res.Aggs["p"])
		}
	}
}

// exactDigestJSON is an exact compression-100 digest's members holding n distinct
// values.
func exactDigestJSON(n int) string {
	means, counts := make([]string, n), make([]string, n)
	for i := range n {
		means[i], counts[i] = strconv.Itoa(i), "1"
	}
	return fmt.Sprintf(`"compression":100,"means":[%s],"counts":[%s],"min":0,"max":%d`, strings.Join(means, ","), strings.Join(counts, ","), n-1)
}
