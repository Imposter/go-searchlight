package search

import (
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// aggValues returns a document's values of field for aggregating, as the engine reads
// them: a text (keyword, text, _id), each entry (keyword_list), a number (number,
// date) or a bool.
func aggValues(d *schema.Doc, field string) []any {
	v, ok := d.Fields[field]
	if !ok || !v.Present {
		return nil
	}
	switch fieldType(testMapping, field) {
	case schema.Keyword, schema.Text:
		if v.Text != nil {
			return []any{*v.Text}
		}
	case schema.KeywordList:
		out := make([]any, len(v.Entries))
		for i, e := range v.Entries {
			out[i] = e
		}
		return out
	case schema.Number, schema.Date:
		if v.Number != nil {
			x := *v.Number
			if x == 0 {
				x = 0
			}
			return []any{x}
		}
	case schema.Bool:
		if v.Bool != nil {
			return []any{*v.Bool}
		}
	}
	return nil
}

// bruteAgg computes a over docs, as the reduce should answer with exact shard sizes.
func bruteAgg(t *testing.T, a Agg, docs []*schema.Doc) *AggResult {
	t.Helper()
	spec := prepareAgg("x", a, testMapping, "aggs.x", false, new(problems))
	if spec == nil {
		t.Fatalf("agg %+v refused", a)
	}
	return bruteSpec(spec, docs)
}

func bruteSpec(spec *aggSpec, docs []*schema.Doc) *AggResult {
	res := &AggResult{Type: spec.typ}
	subs := func(b *Bucket, in []*schema.Doc) {
		if len(spec.subs) == 0 {
			return
		}
		b.Aggs = map[string]*AggResult{}
		for _, sub := range spec.subs {
			b.Aggs[sub.name] = bruteSpec(sub, in)
		}
	}
	switch spec.typ {
	case AggStats:
		for _, d := range docs {
			for _, v := range aggValues(d, spec.field) {
				x, ok := v.(float64)
				if !ok {
					continue
				}
				if res.Count == 0 || x < *res.Min {
					res.Min = ptr(x)
				}
				if res.Count == 0 || x > *res.Max {
					res.Max = ptr(x)
				}
				res.Count++
				res.Sum += x
			}
		}
		if res.Count > 0 {
			res.Avg = ptr(res.Sum / float64(res.Count))
		}
	case AggCardinality:
		seen := map[any]bool{}
		for _, d := range docs {
			for _, v := range aggValues(d, spec.field) {
				seen[v] = true
			}
		}
		res.Value = int64(len(seen))
	case AggTerms:
		in := map[any][]*schema.Doc{}
		var total int64
		for _, d := range docs {
			for _, v := range aggValues(d, spec.field) {
				in[v] = append(in[v], d)
				total++
			}
		}
		var bs []*Bucket
		for k, ds := range in {
			if int64(len(ds)) >= int64(spec.minDoc) {
				b := &Bucket{Key: k, DocCount: int64(len(ds))}
				subs(b, ds)
				bs = append(bs, b)
			}
		}
		slices.SortFunc(bs, func(a, b *Bucket) int {
			if a.DocCount != b.DocCount {
				return int(b.DocCount - a.DocCount)
			}
			return cmpValue(a.Key, b.Key, false)
		})
		if len(bs) > spec.size {
			bs = bs[:spec.size]
		}
		res.Buckets = bs
		res.SumOtherDocCount = total
		for _, b := range bs {
			res.SumOtherDocCount -= b.DocCount
		}
	case AggRange:
		for _, r := range spec.ranges {
			var in []*schema.Doc
			for _, d := range docs {
				for _, v := range aggValues(d, spec.field) {
					if x, ok := v.(float64); ok && x >= r.from && x < r.to {
						in = append(in, d)
					}
				}
			}
			b := &Bucket{Key: r.key, DocCount: int64(len(in))}
			if r.hasFrom {
				b.From = ptr(r.from)
			}
			if r.hasTo {
				b.To = ptr(r.to)
			}
			subs(b, in)
			res.Buckets = append(res.Buckets, b)
		}
	default:
		in := map[float64][]*schema.Doc{}
		for _, d := range docs {
			for _, v := range aggValues(d, spec.field) {
				x, ok := v.(float64)
				if !ok {
					continue
				}
				var k float64
				if spec.calendar != "" {
					if math.Abs(x-spec.offset) > maxDateMillis {
						continue
					}
					k = float64(bruteCalendar(time.UnixMilli(int64(math.Floor(x-spec.offset))).UTC(), spec.calendar).UnixMilli()) + spec.offset
				} else {
					k = math.Floor((x-spec.offset)/spec.interval)*spec.interval + spec.offset
				}
				in[k] = append(in[k], d)
			}
		}
		keys := make([]float64, 0, len(in))
		for k := range in {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		if spec.minDoc == 0 && len(keys) > 1 {
			var all []float64
			if spec.calendar != "" {
				for at := time.UnixMilli(int64(keys[0] - spec.offset)).UTC(); float64(at.UnixMilli())+spec.offset <= keys[len(keys)-1]; at = bruteNext(at, spec.calendar) {
					all = append(all, float64(at.UnixMilli())+spec.offset)
				}
			} else {
				for j := math.Round((keys[0] - spec.offset) / spec.interval); j*spec.interval+spec.offset <= keys[len(keys)-1]; j++ {
					all = append(all, j*spec.interval+spec.offset)
				}
			}
			keys = all
		}
		for _, k := range keys {
			if int64(len(in[k])) < int64(spec.minDoc) {
				continue
			}
			b := &Bucket{Key: k, DocCount: int64(len(in[k]))}
			if spec.typ == AggDateHistogram {
				b.KeyAsString = time.UnixMilli(int64(k)).UTC().Format("2006-01-02T15:04:05.000Z07:00")
			}
			subs(b, in[k])
			res.Buckets = append(res.Buckets, b)
		}
	}
	return res
}

// bruteCalendar floors t to its calendar bucket, spelled out independently of the
// engine's calendarFloor.
func bruteCalendar(t time.Time, unit string) time.Time {
	switch unit {
	case "minute":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, time.UTC)
	case "hour":
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
	case "day":
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	case "week":
		d := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		for d.Weekday() != time.Monday {
			d = d.AddDate(0, 0, -1)
		}
		return d
	case "month":
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	case "quarter":
		m := t.Month()
		for (m-1)%3 != 0 {
			m--
		}
		return time.Date(t.Year(), m, 1, 0, 0, 0, 0, time.UTC)
	}
	return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
}

func bruteNext(t time.Time, unit string) time.Time {
	switch unit {
	case "minute":
		return t.Add(time.Minute)
	case "hour":
		return t.Add(time.Hour)
	case "day":
		return t.AddDate(0, 0, 1)
	case "week":
		return t.AddDate(0, 0, 7)
	case "month":
		return t.AddDate(0, 1, 0)
	case "quarter":
		return t.AddDate(0, 3, 0)
	}
	return t.AddDate(1, 0, 0)
}

// diffAgg describes how got differs from want, or "".
func diffAgg(got, want *AggResult, path string) string {
	switch {
	case got == nil || want == nil:
		if got != want {
			return path + ": missing"
		}
		return ""
	case got.Type != want.Type:
		return fmt.Sprintf("%s: type %s, want %s", path, got.Type, want.Type)
	case got.Count != want.Count || !floatEq(got.Sum, want.Sum) || !ptrEq(got.Min, want.Min) || !ptrEq(got.Max, want.Max) || !ptrEq(got.Avg, want.Avg):
		return fmt.Sprintf("%s: stats %d %v %v %v %v, want %d %v %v %v %v", path,
			got.Count, got.Sum, deref(got.Min), deref(got.Max), deref(got.Avg),
			want.Count, want.Sum, deref(want.Min), deref(want.Max), deref(want.Avg))
	case got.Value != want.Value:
		return fmt.Sprintf("%s: cardinality %d, want %d", path, got.Value, want.Value)
	case got.SumOtherDocCount != want.SumOtherDocCount:
		return fmt.Sprintf("%s: sum_other_doc_count %d, want %d", path, got.SumOtherDocCount, want.SumOtherDocCount)
	case len(got.Buckets) != len(want.Buckets):
		return fmt.Sprintf("%s: %d buckets %v, want %d %v", path, len(got.Buckets), bucketKeys(got.Buckets), len(want.Buckets), bucketKeys(want.Buckets))
	}
	for i, gb := range got.Buckets {
		wb := want.Buckets[i]
		at := fmt.Sprintf("%s.%d", path, i)
		if cmpValue(gb.Key, wb.Key, false) != 0 || gb.DocCount != wb.DocCount || gb.KeyAsString != wb.KeyAsString ||
			!ptrEq(gb.From, wb.From) || !ptrEq(gb.To, wb.To) {
			return fmt.Sprintf("%s: bucket %#v %d %q, want %#v %d %q", at, gb.Key, gb.DocCount, gb.KeyAsString, wb.Key, wb.DocCount, wb.KeyAsString)
		}
		for name, w := range wb.Aggs {
			if d := diffAgg(gb.Aggs[name], w, at+"."+name); d != "" {
				return d
			}
		}
	}
	return ""
}

func bucketKeys(bs []*Bucket) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = fmt.Sprintf("%v:%d", b.Key, b.DocCount)
	}
	return out
}

func ptrEq(a, b *float64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return floatEq(*a, *b)
}

func deref(p *float64) any {
	if p == nil {
		return nil
	}
	return *p
}

func randomAgg(rng *rand.Rand) Agg {
	metric := func() Agg {
		if rng.IntN(2) == 0 {
			return Agg{Type: AggStats, Field: pick(rng, []string{"price", "created", "nope"})}
		}
		return Agg{Type: AggCardinality, Field: pick(rng, []string{"brand", "tags", "price", "active", "title", "_id"})}
	}
	var a Agg
	switch rng.IntN(7) {
	case 0:
		a = Agg{
			Type: AggTerms, Field: pick(rng, []string{"brand", "tags", "active", "price", "title", "_id", "created", "nope"}),
			Size: pick(rng, []int{0, 1, 3, 100}), ShardSize: 10_000,
		}
		if rng.IntN(3) == 0 {
			a.MinDocCount = ptr(1 + rng.IntN(3))
		}
	case 1:
		a = Agg{Type: AggRange, Field: pick(rng, []string{"price", "created"}), Ranges: []Range{
			{To: ptr(5.0)}, {From: ptr(2.5), To: ptr(50.0)}, {From: ptr(10.0), Key: "ten+"}, {From: ptr(1.7e12)},
		}}
	case 2:
		a = Agg{Type: AggHistogram, Field: "price", Interval: pick(rng, []float64{1, 2.5, 10, 33}), Offset: pick(rng, []float64{0, 0.5, -3})}
		if rng.IntN(3) == 0 {
			a.MinDocCount = ptr(rng.IntN(3))
		}
	case 3:
		a = Agg{Type: AggDateHistogram, Field: "created", Calendar: pick(rng, calendarUnits[1:])}
		if rng.IntN(3) == 0 {
			a.MinDocCount = ptr(1)
		}
	case 4:
		a = Agg{Type: AggDateHistogram, Field: "created", Interval: pick(rng, []float64{3_600_000, 86_400_000 * 7}), Offset: pick(rng, []float64{0, 3_600_000})}
	default:
		return metric()
	}
	if rng.IntN(2) == 0 {
		a.Aggs = map[string]Agg{"m": metric()}
		if rng.IntN(2) == 0 {
			a.Aggs["n"] = metric()
		}
	}
	return a
}

// TestAggregationsEqualBruteForce: every aggregation, with metric sub-aggregations,
// over random queries on several shards (shard_size past every term, so exact),
// equals the aggregation computed over the matching documents directly.
func TestAggregationsEqualBruteForce(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0xa66))
	c := newCluster(t, 3)
	rounds, queries := 4, 60
	if longMode() {
		rounds, queries = 12, 300
	}
	for round := range rounds {
		for range 70 {
			c.upsert(pick(rng, ids)+pick(rng, []string{"", "-1", "-2", "-3"}), randomDoc(rng))
		}
		for range 15 {
			c.remove(pick(rng, ids))
		}
		c.refreshAll()
		if round%2 == 1 {
			c.merge(rng.IntN(3), 1)
		}
		for range queries {
			q, raw := randomQuery(t, rng)
			if rng.IntN(3) == 0 {
				q, raw = &query.All{}, "all"
			}
			aggs := map[string]Agg{"a": randomAgg(rng), "b": randomAgg(rng)}
			if rng.IntN(2) == 0 {
				aggChunkMin, aggChunk = 1, 1+rng.Uint32N(40) // segments collected in parts
			}
			got, err := c.search(&Request{Query: q, Aggs: aggs})
			aggChunkMin, aggChunk = 1<<15, 1<<16
			if err != nil {
				t.Fatalf("%s %+v: %v", raw, aggs, err)
			}
			matched := brute(c.docs(), q, nil)
			for name, a := range aggs {
				want := bruteAgg(t, a, matched)
				if d := diffAgg(got.Aggs[name], want, name); d != "" {
					t.Fatalf("round %d, query %s, agg %+v (subs %+v):\n%s", round, raw, a, a.Aggs, d)
				}
			}
		}
	}
}

// TestTermsShardSizeErrorBounds: with a small shard_size over several shards, every
// returned count is a lower bound within its error bound of the truth, and the total
// error bound covers every term left out.
func TestTermsShardSizeErrorBounds(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x7e57))
	c := newCluster(t, 4)
	brands := make([]string, 40)
	for i := range brands {
		brands[i] = fmt.Sprintf("b%02d", i)
	}
	for i := range 1500 {
		// A skewed distribution, different per shard by id.
		b := brands[min(len(brands)-1, int(rng.ExpFloat64()*6))]
		c.upsert(fmt.Sprintf("d%04d", i), fmt.Sprintf(`{"brand":%q}`, b))
	}
	c.refreshAll()
	truth := map[string]int64{}
	for _, d := range c.docs() {
		truth[*d.Fields["brand"].Text]++
	}
	for _, shardSize := range []int{1, 2, 3, 5, 8} {
		got, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"t": {Type: AggTerms, Field: "brand", Size: 5, ShardSize: shardSize}}})
		if err != nil {
			t.Fatal(err)
		}
		res := got.Aggs["t"]
		var returned int64
		for _, b := range res.Buckets {
			key, _ := b.Key.(string)
			tc := truth[key]
			if b.DocCount > tc || tc > b.DocCount+b.DocCountErrorUpperBound {
				t.Errorf("shard_size %d: %s counts %d (+%d), truth %d", shardSize, key, b.DocCount, b.DocCountErrorUpperBound, tc)
			}
			returned += b.DocCount
		}
		var all int64
		for _, n := range truth {
			all += n
		}
		if res.SumOtherDocCount != all-returned {
			t.Errorf("shard_size %d: sum_other %d, want %d", shardSize, res.SumOtherDocCount, all-returned)
		}
		// Any term with more documents than the last returned bucket plus the error
		// bound must have been returned.
		if len(res.Buckets) > 0 {
			last := res.Buckets[len(res.Buckets)-1].DocCount
			for key, n := range truth {
				if n > last+res.DocCountErrorUpperBound && !slices.ContainsFunc(res.Buckets, func(b *Bucket) bool { return b.Key == key }) {
					t.Errorf("shard_size %d: %s (%d) missing past the bound %d+%d", shardSize, key, n, last, res.DocCountErrorUpperBound)
				}
			}
		}
	}
}

func TestSketchAccuracyAndMerge(t *testing.T) {
	for _, p := range []uint8{10, 14} {
		for _, n := range []int{0, 1, 100, 4000, 5000, 50_000, 300_000} {
			whole := NewSketch(p)
			parts := []*Sketch{NewSketch(p), NewSketch(p), NewSketch(p)}
			for i := range n {
				h := hashString(fmt.Sprintf("value-%d", i))
				whole.Add(h)
				parts[i%3].Add(h)
				parts[(i+1)%3].Add(h) // overlap: merges must not double count
			}
			merged := NewSketch(p)
			for _, s := range parts {
				s.seal()
				merged.Merge(s)
			}
			est, mest := whole.Estimate(), merged.Estimate()
			if est != mest {
				t.Errorf("p=%d n=%d: merged estimate %d, whole %d", p, n, mest, est)
			}
			exact := n <= 1<<(p-2)
			tolerance := 4 * 1.04 / math.Sqrt(float64(int(1)<<p)) // four standard errors
			switch {
			case exact && est != int64(n):
				t.Errorf("p=%d n=%d: exact count %d", p, n, est)
			case !exact && math.Abs(float64(est)-float64(n)) > tolerance*float64(n):
				t.Errorf("p=%d n=%d: estimate %d off by more than %.1f%%", p, n, est, 100*tolerance)
			}
		}
	}
}

func TestCalendarFloor(t *testing.T) {
	at := time.Date(2026, 10, 3, 17, 45, 12, 0, time.UTC) // a Saturday
	for unit, want := range map[string]string{
		"minute": "2026-10-03T17:45:00Z", "hour": "2026-10-03T17:00:00Z", "day": "2026-10-03T00:00:00Z",
		"week": "2026-09-28T00:00:00Z", "month": "2026-10-01T00:00:00Z", "quarter": "2026-10-01T00:00:00Z",
		"year": "2026-01-01T00:00:00Z",
	} {
		if got := calendarFloor(at, unit).Format(time.RFC3339); got != want {
			t.Errorf("%s: %s, want %s", unit, got, want)
		}
		if got := bruteCalendar(at, unit).Format(time.RFC3339); got != want {
			t.Errorf("brute %s: %s, want %s", unit, got, want)
		}
	}
	_ = strings.TrimSpace
}
