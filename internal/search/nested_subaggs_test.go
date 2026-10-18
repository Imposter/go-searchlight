package search

import (
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// nestedBucketWant is one bucket of a terms aggregation on a sub-field, by brute force:
// its documents, and its objects (those of its documents chosen by the aggregation's
// where that hold the bucket's value).
type nestedBucketWant struct {
	docs    []*schema.Doc
	objects []*schema.Object
	owners  []*schema.Doc
}

func bruteNestedBuckets(docs []*schema.Doc, field string, where *query.Compiled) map[any]*nestedBucketWant {
	out := map[any]*nestedBucketWant{}
	for _, d := range docs {
		chosen(d, where, func(o *schema.Object) {
			for _, k := range keysOf(o, field) {
				b := out[k]
				if b == nil {
					b = &nestedBucketWant{}
					out[k] = b
				}
				if len(b.docs) == 0 || b.docs[len(b.docs)-1] != d {
					b.docs = append(b.docs, d)
				}
				b.objects = append(b.objects, o)
				b.owners = append(b.owners, d)
			}
		})
	}
	return out
}

// checkNestedSubAggs compares res, a terms aggregation on field with nestedSubAggsFor's
// sub-aggregations, with the brute force over hits.
func checkNestedSubAggs(t *testing.T, what string, res *AggResult, hits []*schema.Doc, field string, where, subWhere *query.Compiled) int {
	t.Helper()
	want := bruteNestedBuckets(hits, field, where)
	if len(res.Buckets) != len(want) {
		t.Fatalf("%s: %d buckets, want %d", what, len(res.Buckets), len(want))
	}
	for _, b := range res.Buckets {
		w := want[b.Key]
		if w == nil || b.DocCount != int64(len(w.docs)) {
			t.Fatalf("%s: bucket %v of %d documents, want %v", what, b.Key, b.DocCount, w)
		}
		var lowest *float64
		brands := map[string]bool{}
		byBrand := map[any]int64{}
		for _, d := range w.docs {
			if v := d.Fields["price"].Number; v != nil && (lowest == nil || *v < *lowest) {
				lowest = v
			}
			if v := d.Fields["brand"].Text; v != nil {
				brands[*v] = true
				byBrand[*v]++
			}
		}
		var count int64
		var sum float64
		var lo, hi, latest *float64
		bySize := map[any]int64{}
		var last *schema.Doc
		seen := map[any]bool{}
		for i, o := range w.objects {
			if v := o.Fields["_v.at"].Number; v != nil && (latest == nil || *v > *latest) {
				latest = v
			}
			if w.owners[i] != last {
				last, seen = w.owners[i], map[any]bool{}
			}
			for _, k := range keysOf(o, "_v.size") {
				if !seen[k] {
					seen[k] = true
					bySize[k]++
				}
			}
			if subWhere != nil && !subWhere.MatchObject(o) {
				continue
			}
			if v := o.Fields["_v.price"].Number; v != nil {
				count++
				sum += *v
				if lo == nil || *v < *lo {
					lo = v
				}
				if hi == nil || *v > *hi {
					hi = v
				}
			}
		}
		sub := b.Aggs
		if !ptrEq(sub["lowest"].Number, lowest) || sub["brands"].Value != int64(len(brands)) || !ptrEq(sub["latest"].Number, latest) {
			t.Fatalf("%s: bucket %v: lowest %v brands %d latest %v, want %v %d %v", what, b.Key, sub["lowest"].Number, sub["brands"].Value, sub["latest"].Number, lowest, len(brands), latest)
		}
		if st := sub["prices"]; st.Count != count || !floatEq(st.Sum, sum) || !ptrEq(st.Min, lo) || !ptrEq(st.Max, hi) {
			t.Fatalf("%s: bucket %v: prices %d %g %v %v, want %d %g %v %v", what, b.Key, st.Count, st.Sum, st.Min, st.Max, count, sum, lo, hi)
		}
		for name, counts := range map[string]map[any]int64{"by_brand": byBrand, "by_size": bySize} {
			got := map[any]int64{}
			for _, sb := range sub[name].Buckets {
				got[sb.Key] = sb.DocCount
				if sb.DocCountErrorUpperBound != 0 {
					t.Fatalf("%s: bucket %v: %s's bucket %v has an error bound", what, b.Key, name, sb.Key)
				}
			}
			if !maps.Equal(got, counts) || sub[name].SumOtherDocCount != 0 {
				t.Fatalf("%s: bucket %v: %s %v (other %d), want %v", what, b.Key, name, got, sub[name].SumOtherDocCount, counts)
			}
		}
	}
	return len(res.Buckets)
}

func nestedSubAggsFor(subWhere query.Node) map[string]Agg {
	return map[string]Agg{
		"lowest":   {Type: AggMin, Field: "price"},
		"brands":   {Type: AggCardinality, Field: "brand", Threshold: 1000},
		"latest":   {Type: AggMax, Field: "_v.at"},
		"prices":   {Type: AggStats, Field: "_v.price", Where: subWhere},
		"by_brand": {Type: AggTerms, Field: "brand", Size: 1000},
		"by_size":  {Type: AggTerms, Field: "_v.size", Size: 1000},
	}
}

// TestNestedSubAggsEqualBruteForce: a terms aggregation on a sub-field collects its
// sub-aggregations per bucket, against brute force over two shards through updates,
// deletes and merges: those on a top-level field over the bucket's documents (each
// once), those on a sub-field over the bucket's objects (the chosen ones holding its
// value, narrowed by their own where), a terms sub-aggregation exactly, each document
// once per value; at the top level and under a filter aggregation.
func TestNestedSubAggsEqualBruteForce(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x5ab5))
	c := variantCluster(t, rng, 1000)
	docs := c.docs()
	checks, buckets := 40, 0
	if longMode() {
		checks = 400
	}
	for range checks {
		raw := `{"all":[]}`
		if rng.IntN(3) > 0 {
			raw = randomNestedJSON(rng)
		}
		q, ok := parseValid(t, raw)
		if !ok {
			continue
		}
		whereRaw, subWhereRaw := "", ""
		if rng.IntN(2) == 0 {
			whereRaw = randomInnerJSON(rng, 0)
		}
		if rng.IntN(2) == 0 {
			subWhereRaw = randomInnerJSON(rng, 0)
		}
		whereNode, where := compileWhere(t, whereRaw)
		subWhereNode, subWhere := compileWhere(t, subWhereRaw)
		field := pick(rng, []string{"_v.size", "_v.color", "_v.price", "_v.stock", "_v.tags"})
		r := &Request{Query: q, TrackTotal: TrackTotalAll, Aggs: map[string]Agg{
			"t":  {Type: AggTerms, Field: field, Size: 5000, Where: whereNode, Aggs: nestedSubAggsFor(subWhereNode)},
			"in": {Type: AggFilter, Filter: &query.Leaf{Field: "price", Op: query.OpLt, Value: json.RawMessage("50")}, Aggs: map[string]Agg{"t": {Type: AggTerms, Field: field, Size: 5000, Where: whereNode, Aggs: nestedSubAggsFor(subWhereNode)}}},
		}}
		if ps := ValidateQueries(r, testMapping); len(ps) > 0 || !r.NestedSubAggs {
			t.Fatalf("%v, nested sub-aggregations %v", ps, r.NestedSubAggs)
		}
		got, err := c.search(r)
		if err != nil {
			t.Fatalf("%s on %s where %s / %s: %v", raw, field, whereRaw, subWhereRaw, err)
		}
		what := fmt.Sprintf("%s on %s where %s / %s", raw, field, whereRaw, subWhereRaw)
		hits := brute(docs, q, nil)
		buckets += checkNestedSubAggs(t, what, got.Aggs["t"], hits, field, where, subWhere)
		var cheap []*schema.Doc
		for _, d := range hits {
			if v := d.Fields["price"]; v.Number != nil && *v.Number < 50 {
				cheap = append(cheap, d)
			}
		}
		buckets += checkNestedSubAggs(t, what+" under a filter", got.Aggs["in"].Aggs["t"], cheap, field, where, subWhere)
	}
	if buckets < 100 {
		t.Fatalf("%d buckets checked: the queries match too little", buckets)
	}
}

// A terms sub-aggregation keeps its size, counting the rest in sum_other_doc_count;
// the response's 65,536 buckets count those under the buckets it holds, the shard's
// refusing past them.
func TestNestedSubTermsSizeAndBuckets(t *testing.T) {
	c := newCluster(t, 1)
	for i := range 40 {
		c.upsert(fmt.Sprintf("d%02d", i), fmt.Sprintf(`{"brand":"b%d","_v":[{"size":"s%d"},{"size":"L"}]}`, i%7, i%3))
	}
	c.refreshAll()
	got, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{
		"t": {Type: AggTerms, Field: "_v.size", Size: 10, Aggs: map[string]Agg{"b": {Type: AggTerms, Field: "brand", Size: 2}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range got.Aggs["t"].Buckets {
		sub := b.Aggs["b"]
		var listed int64
		for _, sb := range sub.Buckets {
			listed += sb.DocCount
		}
		if len(sub.Buckets) != 2 || listed+sub.SumOtherDocCount != b.DocCount {
			t.Fatalf("bucket %v (%d): %d sub-buckets, %d listed and %d other", b.Key, b.DocCount, len(sub.Buckets), listed, sub.SumOtherDocCount)
		}
	}
	big := newCluster(t, 1)
	for i := range 300 {
		big.upsert(fmt.Sprintf("d%03d", i), fmt.Sprintf(`{"brand":"b%d","_v":[{"size":"s%d"}]}`, i, i%300))
	}
	big.refreshAll()
	_, err = big.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{
		"t": {Type: AggTerms, Field: "_v.size", Size: 300, Aggs: map[string]Agg{"b": {Type: AggTerms, Field: "brand", Size: 300}}},
	}})
	if err != nil {
		t.Fatalf("300 buckets of 1: %v", err)
	}
	many := newCluster(t, 1)
	var members []string
	for i := range 300 {
		members = append(members, fmt.Sprintf(`{"size":"s%d"}`, i))
	}
	for i := range 300 {
		many.upsert(fmt.Sprintf("d%03d", i), fmt.Sprintf(`{"brand":"b%d","_v":[%s]}`, i, strings.Join(members, ",")))
	}
	many.refreshAll()
	_, err = many.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{
		"t": {Type: AggTerms, Field: "_v.size", Size: 300, Aggs: map[string]Agg{"b": {Type: AggTerms, Field: "brand", Size: 300}}},
	}})
	var re *RequestError
	if !asRequestError(err, &re) || re.Problems[0].Loc != "aggs.t.aggs.b.terms" {
		t.Fatalf("300 x 300 buckets: %v, want a refusal at aggs.t.aggs.b.terms", err)
	}
}

// The sub-aggregations a terms aggregation on a sub-field takes, and those it refuses.
func TestNestedSubAggsRefusals(t *testing.T) {
	m := &schema.Mapping{Fields: maps.Clone(testMapping.Fields)}
	m.Fields["_w"], m.Fields["_w.size"] = schema.Nested, schema.Keyword
	for raw, want := range map[string]string{
		`{"t":{"terms":{"field":"_v.size"},"aggs":{"h":{"histogram":{"field":"price","interval":1}}}}}`:                       "aggs.t.aggs.h",
		`{"t":{"terms":{"field":"_v.size"},"aggs":{"w":{"min":{"field":"_w.size"}}}}}`:                                        "aggs.t.aggs.w.min.field",
		`{"t":{"terms":{"field":"_v.size"},"aggs":{"p":{"min":{"field":"price","where":{"all":[]}}}}}}`:                       "aggs.t.aggs.p.min.where",
		`{"t":{"terms":{"field":"brand"},"aggs":{"p":{"min":{"field":"_v.price"}}}}}`:                                         "aggs.t.aggs.p",
		`{"t":{"terms":{"field":"brand"},"aggs":{"p":{"terms":{"field":"price"}}}}}`:                                          "aggs.t.aggs.p",
		`{"t":{"stats":{"field":"_v.price"},"aggs":{"p":{"min":{"field":"price"}}}}}`:                                         "aggs.t.aggs",
		`{"t":{"terms":{"field":"_v.size"},"aggs":{"p":{"terms":{"field":"brand"},"aggs":{"q":{"min":{"field":"price"}}}}}}}`: "aggs.t.aggs.p.aggs",
	} {
		r, ps := ParseRequest([]byte(`{"aggs":` + raw + `}`))
		if len(ps) == 0 {
			ps = ValidateQueries(r, m)
		}
		if len(ps) == 0 {
			_, err := prepare(r, m)
			var re *RequestError
			if asRequestError(err, &re) {
				ps = re.Problems
			}
		}
		if len(ps) == 0 || ps[0].Loc != want {
			t.Errorf("%s: %v, want a problem at %s", raw, ps, want)
		}
	}
	r := parseReq(t, `{"aggs":{"t":{"terms":{"field":"_v.size"},"aggs":{"p":{"stats":{"field":"_v.price","where":{"field":"_v.stock","op":"eq","value":true}}},"b":{"terms":{"field":"brand"}}}}}}`)
	if ps := ValidateQueries(r, m); len(ps) > 0 || !r.NestedSubAggs {
		t.Fatalf("%v, nested sub-aggregations %v", ps, r.NestedSubAggs)
	}
	if _, err := prepare(r, m); err != nil {
		t.Fatal(err)
	}
	if plain := parseReq(t, `{"aggs":{"t":{"terms":{"field":"brand"},"aggs":{"p":{"min":{"field":"price"}}}}}}`); len(ValidateQueries(plain, m)) > 0 || plain.NestedSubAggs {
		t.Fatal("a terms on a top-level field marked as nested sub-aggregations")
	}
}
