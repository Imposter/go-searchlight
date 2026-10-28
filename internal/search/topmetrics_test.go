package search

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// bruteTop is the top_metrics of docs: the size documents first by sort (those without
// a value last), then by id, and their metrics.
func bruteTop(docs []*schema.Doc, sort string, desc bool, size int, metrics []string) []TopMetric {
	ds := slices.Clone(docs)
	val := func(d *schema.Doc) any {
		if vs := aggValues(d, sort); len(vs) > 0 {
			return vs[0]
		}
		return nil
	}
	slices.SortFunc(ds, func(a, b *schema.Doc) int {
		if c := cmpValue(val(a), val(b), desc); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	out := []TopMetric{}
	for _, d := range ds[:min(size, len(ds))] {
		tm := TopMetric{Sort: []any{val(d)}, Metrics: map[string]any{}}
		for _, m := range metrics {
			var v any
			if m == IDField {
				v = d.ID
			} else if vs := aggValues(d, m); len(vs) > 0 {
				v = vs[0]
			}
			tm.Metrics[m] = v
		}
		out = append(out, tm)
	}
	return out
}

// TestTopMetricsEqualsBruteForce: a top_metrics, top-level and under terms and filter
// aggregations, answers as the brute force over every shard and segment does, ties in
// the sort value (prices repeat, and documents miss them) going to the least _id.
func TestTopMetricsEqualsBruteForce(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x709))
	c := orderedCluster(t, rng, 3, 700, 12)
	docs := c.docs()
	metrics := []string{"price", "brand", "active", IDField, "created"}
	for _, sort := range []string{"price", "created", "missing_field"} {
		for _, desc := range []bool{false, true} {
			for _, size := range []int{1, 3, MaxTopMetricsSize} {
				top := Agg{Type: AggTopMetrics, Metrics: metrics, TopSort: &SortField{Field: sort, Desc: desc}, Size: size}
				q := &query.Leaf{Field: "price", Op: query.OpGte, Value: json.RawMessage(`0`)}
				resp, err := c.search(&Request{Query: q, Aggs: map[string]Agg{
					"top":    top,
					"brands": {Type: AggTerms, Field: "brand", Size: 100, Aggs: map[string]Agg{"top": top}},
					"red":    {Type: AggFilter, Filter: &query.Leaf{Field: "tags", Op: query.OpHas, Value: json.RawMessage(`"red"`)}, Aggs: map[string]Agg{"top": top}},
				}})
				if err != nil {
					t.Fatal(err)
				}
				hits := brute(docs, q, nil)
				at := fmt.Sprintf("sort %s desc %t size %d", sort, desc, size)
				if want := bruteTop(hits, sort, desc, size, metrics); !reflect.DeepEqual(resp.Aggs["top"].Top, want) {
					t.Fatalf("%s: top\n got %v\nwant %v", at, resp.Aggs["top"].Top, want)
				}
				for _, b := range resp.Aggs["brands"].Buckets {
					var in []*schema.Doc
					for _, d := range hits {
						if vs := aggValues(d, "brand"); len(vs) > 0 && vs[0] == b.Key {
							in = append(in, d)
						}
					}
					if want := bruteTop(in, sort, desc, size, metrics); !reflect.DeepEqual(b.Aggs["top"].Top, want) {
						t.Fatalf("%s: brand %v's top\n got %v\nwant %v", at, b.Key, b.Aggs["top"].Top, want)
					}
				}
				red := brute(hits, &query.Leaf{Field: "tags", Op: query.OpHas, Value: json.RawMessage(`"red"`)}, nil)
				if want := bruteTop(red, sort, desc, size, metrics); !reflect.DeepEqual(resp.Aggs["red"].Aggs["top"].Top, want) {
					t.Fatalf("%s: red's top\n got %v\nwant %v", at, resp.Aggs["red"].Aggs["top"].Top, want)
				}
			}
		}
	}
}

// TestTopMetricsTies: documents of one sort value spread over shards and segments
// answer in _id order, whatever the order they were written in.
func TestTopMetricsTies(t *testing.T) {
	c := newCluster(t, 3)
	ids := []string{"k", "c", "x", "a", "q", "m", "b", "z"}
	for i, id := range ids {
		c.upsert(id, `{"price":5,"brand":"`+id+`"}`)
		if i%3 == 2 {
			c.refreshAll()
		}
	}
	c.upsert("first", `{"price":9,"brand":"first"}`)
	c.upsert("none", `{"brand":"none"}`)
	c.refreshAll()
	r := parseAggsBody(t, `{"size":0,"aggs":{"t":{"top_metrics":{"metrics":[{"field":"brand"},{"field":"_id"}],"sort":{"price":"desc"},"size":4}},
		"all":{"top_metrics":{"metrics":{"field":"brand"},"sort":[{"price":{"order":"asc"}}],"size":10}}}}`)
	resp, err := c.search(r)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(resp.Aggs["t"])
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"top":[{"sort":[9],"metrics":{"_id":"first","brand":"first"}},{"sort":[5],"metrics":{"_id":"a","brand":"a"}},{"sort":[5],"metrics":{"_id":"b","brand":"b"}},{"sort":[5],"metrics":{"_id":"c","brand":"c"}}]}`; string(raw) != want {
		t.Errorf("top_metrics\n got %s\nwant %s", raw, want)
	}
	var got []string
	for _, tm := range resp.Aggs["all"].Top {
		got = append(got, fmt.Sprint(tm.Metrics["brand"], tm.Sort))
	}
	if want := "a[5] b[5] c[5] k[5] m[5] q[5] x[5] z[5] first[9] none[<nil>]"; strings.Join(got, " ") != want {
		t.Errorf("ascending, the documents without a price last:\n got %s\nwant %s", strings.Join(got, " "), want)
	}
}
