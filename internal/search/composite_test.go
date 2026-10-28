package search

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// orderedCluster is a cluster of shards, each of several segments, over documents of
// a brand (of brands), some tags, a price (integral, often equal) and a date, each
// field missing from some documents.
func orderedCluster(t *testing.T, rng *rand.Rand, shards, docs, brands int) *cluster {
	t.Helper()
	c := newCluster(t, shards)
	tags := []string{"red", "green", "blue", "big", "small"}
	for i := range docs {
		var members []string
		if rng.IntN(20) > 0 {
			// Skewed, so counts differ and some brands are on few shards.
			b := min(brands-1, int(rng.ExpFloat64()*float64(brands)/4))
			members = append(members, fmt.Sprintf(`"brand":"B%03d"`, b))
		}
		if rng.IntN(10) > 0 {
			members = append(members, fmt.Sprintf(`"price":%d`, rng.IntN(50)-10))
		}
		if rng.IntN(10) > 0 {
			members = append(members, fmt.Sprintf(`"created":%d`, 1_700_000_000_000+int64(rng.IntN(90))*86_400_000+int64(rng.IntN(3))*3_600_000))
		}
		if rng.IntN(4) > 0 {
			members = append(members, fmt.Sprintf(`"active":%t`, rng.IntN(2) == 0))
		}
		var ts []string
		for _, tag := range tags {
			if rng.IntN(4) == 0 {
				ts = append(ts, fmt.Sprintf("%q", tag))
			}
		}
		if len(ts) > 0 {
			members = append(members, `"tags":[`+strings.Join(ts, ",")+`]`)
		}
		c.upsert(fmt.Sprintf("d%05d", i), "{"+strings.Join(members, ",")+"}")
		if rng.IntN(docs/6+1) == 0 {
			c.refresh(rng.IntN(shards))
		}
	}
	c.refreshAll()
	return c
}

// parseAggsBody parses a search body with aggs alone, size 0.
func parseAggsBody(t *testing.T, body string) *Request {
	t.Helper()
	r, ps := ParseRequest([]byte(body))
	if len(ps) > 0 {
		t.Fatalf("ParseRequest(%s): %v", body, ps)
	}
	return r
}

// docKey is document d's values of a composite's sources: every combination, as
// bruteComposite counts them.
func docCombos(d *schema.Doc, srcs []CompositeSource) [][]any {
	combos := [][]any{{}}
	for _, src := range srcs {
		vals := aggValues(d, src.Field)
		if src.Type != AggTerms {
			spec := &aggSpec{interval: src.Interval, offset: src.Offset, calendar: src.Calendar}
			for i, v := range vals {
				k, _ := bucketKey(spec, v.(float64)) //nolint:forcetypeassert,errcheck // numbers only
				vals[i] = k
			}
		}
		vals = slices.CompactFunc(slices.SortedFunc(slices.Values(vals), func(a, b any) int { return cmpValue(a, b, false) }), func(a, b any) bool { return a == b })
		var next [][]any
		for _, c := range combos {
			for _, v := range vals {
				next = append(next, append(slices.Clone(c), v))
			}
		}
		combos = next
	}
	return combos
}

type bruteBucket struct {
	keys []any
	docs []*schema.Doc
}

// bruteComposite is every composite bucket of docs, in order.
func bruteComposite(docs []*schema.Doc, srcs []CompositeSource) []*bruteBucket {
	byKey := map[string]*bruteBucket{}
	for _, d := range docs {
		for _, combo := range docCombos(d, srcs) {
			k := string(compositeKey(nil, combo))
			b := byKey[k]
			if b == nil {
				b = &bruteBucket{keys: combo}
				byKey[k] = b
			}
			b.docs = append(b.docs, d)
		}
	}
	out := make([]*bruteBucket, 0, len(byKey))
	for _, b := range byKey {
		out = append(out, b)
	}
	slices.SortFunc(out, func(a, b *bruteBucket) int {
		for i := range srcs {
			if c := cmpValue(a.keys[i], b.keys[i], srcs[i].Desc); c != 0 {
				return c
			}
		}
		return 0
	})
	return out
}

// TestCompositeWalk: paging a composite aggregation through after_key returns every
// bucket exactly once, in order, with its exact count and metrics, whatever the page
// size, over several shards of several segments each.
func TestCompositeWalk(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0xc0b0))
	c := orderedCluster(t, rng, 3, 900, 25)
	docs := c.docs()
	for name, srcs := range map[string][]CompositeSource{
		"brand":            {{Name: "brand", Type: AggTerms, Field: "brand"}},
		"brand desc, tags": {{Name: "b", Type: AggTerms, Field: "brand", Desc: true}, {Name: "t", Type: AggTerms, Field: "tags"}},
		"price histogram, active": {
			{Name: "p", Type: AggHistogram, Field: "price", Interval: 7, Offset: 2},
			{Name: "a", Type: AggTerms, Field: "active", Desc: true},
		},
		"week, price": {{Name: "w", Type: AggDateHistogram, Field: "created", Calendar: "week"}, {Name: "p", Type: AggTerms, Field: "price"}},
		"fixed day":   {{Name: "d", Type: AggDateHistogram, Field: "created", Interval: 86_400_000, Desc: true}},
	} {
		want := bruteComposite(docs, srcs)
		for _, size := range []int{1, 7, 64, 10_000} {
			var after map[string]any
			var got []*Bucket
			for page := 0; ; page++ {
				if page > len(want)+1 {
					t.Fatalf("%s, size %d: the walk does not end", name, size)
				}
				a := Agg{
					Type: AggComposite, Sources: srcs, Size: size, After: after,
					Aggs: map[string]Agg{"low": {Type: AggMin, Field: "price"}},
				}
				resp, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"c": a}})
				if err != nil {
					t.Fatalf("%s: %v", name, err)
				}
				res := resp.Aggs["c"]
				if len(res.Buckets) > size {
					t.Fatalf("%s, size %d: a page of %d", name, size, len(res.Buckets))
				}
				if len(res.Buckets) == 0 {
					if res.AfterKey != nil {
						t.Errorf("%s, size %d: an empty page's after_key %v", name, size, res.AfterKey)
					}
					break
				}
				got = append(got, res.Buckets...)
				// The next page starts from the after_key as a client passes it: through JSON.
				raw, err := json.Marshal(res.AfterKey)
				if err != nil {
					t.Fatal(err)
				}
				after = nil
				if err := json.Unmarshal(raw, &after); err != nil {
					t.Fatal(err)
				}
			}
			if len(got) != len(want) {
				t.Fatalf("%s, size %d: %d buckets walked, want %d", name, size, len(got), len(want))
			}
			for i, b := range got {
				key, _ := b.Key.(*CompositeKey)
				w := want[i]
				if key == nil || !slices.Equal(key.Values, w.keys) || b.DocCount != int64(len(w.docs)) {
					t.Fatalf("%s, size %d: bucket %d is %v (%d), want %v (%d)", name, size, i, key, b.DocCount, w.keys, len(w.docs))
				}
				var low *float64
				for _, d := range w.docs {
					for _, v := range aggValues(d, "price") {
						if f := v.(float64); low == nil || f < *low { //nolint:forcetypeassert,errcheck // numbers only
							low = &f
						}
					}
				}
				if !ptrEq(b.Aggs["low"].Number, low) {
					t.Fatalf("%s, size %d: bucket %v's min %v, want %v", name, size, w.keys, deref(b.Aggs["low"].Number), deref(low))
				}
			}
		}
	}
}

// TestCompositeJSON: the answer's shape, after_key first, and paging from the body.
func TestCompositeJSON(t *testing.T) {
	c := newCluster(t, 2)
	for i, b := range []string{"acme", "zeta", "acme", "beta"} {
		c.upsert(fmt.Sprintf("d%d", i), fmt.Sprintf(`{"brand":%q,"price":%d}`, b, i*10))
	}
	c.refreshAll()
	r := parseAggsBody(t, `{"size":0,"aggs":{"c":{"composite":{"size":2,"sources":[{"b":{"terms":{"field":"brand"}}},{"p":{"histogram":{"field":"price","interval":25}}}]},"aggs":{"n":{"max":{"field":"price"}}}}}}`)
	resp, err := c.search(r)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(resp.Aggs["c"])
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"after_key":{"b":"beta","p":25},"buckets":[{"doc_count":2,"key":{"b":"acme","p":0},"n":{"value":20}},{"doc_count":1,"key":{"b":"beta","p":25},"n":{"value":30}}]}`; string(raw) != want {
		t.Errorf("composite JSON\n got %s\nwant %s", raw, want)
	}
	r = parseAggsBody(t, `{"size":0,"aggs":{"c":{"composite":{"size":2,"after":{"b":"acme","p":25},"sources":[{"b":{"terms":{"field":"brand"}}},{"p":{"histogram":{"field":"price","interval":25}}}]}}}}`)
	if resp, err = c.search(r); err != nil {
		t.Fatal(err)
	}
	if raw, err = json.Marshal(resp.Aggs["c"]); err != nil {
		t.Fatal(err)
	}
	if want := `{"after_key":{"b":"zeta","p":0},"buckets":[{"doc_count":1,"key":{"b":"beta","p":25}},{"doc_count":1,"key":{"b":"zeta","p":0}}]}`; string(raw) != want {
		t.Errorf("the second page's JSON\n got %s\nwant %s", raw, want)
	}
	if got := bucketKeysJSON(t, resp.Aggs["c"]); got != `[{"b":"beta","p":25},{"b":"zeta","p":0}]` {
		t.Errorf("the second page: %s", got)
	}
}

func bucketKeysJSON(t *testing.T, res *AggResult) string {
	t.Helper()
	keys := make([]*CompositeKey, len(res.Buckets))
	for i, b := range res.Buckets {
		keys[i], _ = b.Key.(*CompositeKey)
	}
	raw, err := json.Marshal(keys)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
