package search

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

var variantFields = []string{"_v._key", "_v.size", "_v.color", "_v.price", "_v.stock", "_v.tags", "_v.at"}

var variantSizes = []string{"S", "M", "L", "l", " L ", "XL", "xl", "10", "12.5"}

// variantDoc is a product with variants: now and then no _v, an empty one, one object
// alone, a member that is no object, a variant missing sub-fields or holding one of
// another type.
func variantDoc(rng *rand.Rand, id int) string {
	members := []string{`"brand":` + jsonString(pick(rng, vocab)), `"price":` + strconv.Itoa(rng.IntN(100))}
	variant := func(k int) string {
		var fields []string
		add := func(name, value string) { fields = append(fields, jsonString(name)+":"+value) }
		add("_key", jsonString(fmt.Sprintf("SKU-%d-%d", id, k)))
		if rng.IntN(5) > 0 {
			add("size", jsonString(pick(rng, variantSizes)))
		}
		if rng.IntN(3) > 0 {
			add("color", jsonString(phrase(rng, 1+rng.IntN(3))))
		}
		switch rng.IntN(8) {
		case 0:
		case 1:
			add("price", `"cheap"`)
		default:
			add("price", strconv.Itoa(rng.IntN(30)))
		}
		switch rng.IntN(6) {
		case 0:
		case 1:
			add("stock", `"yes"`)
		default:
			add("stock", strconv.FormatBool(rng.IntN(2) == 0))
		}
		if rng.IntN(3) == 0 {
			add("tags", `[`+jsonString(pick(rng, vocab))+`,`+jsonString(pick(rng, vocab))+`]`)
		}
		if rng.IntN(3) == 0 {
			add("at", strconv.FormatInt(1_700_000_000_000+rng.Int64N(20)*86_400_000, 10))
		}
		if rng.IntN(15) == 0 {
			add("extra", `"unmapped"`)
		}
		rng.Shuffle(len(fields), func(i, j int) { fields[i], fields[j] = fields[j], fields[i] })
		return "{" + strings.Join(fields, ",") + "}"
	}
	switch rng.IntN(12) {
	case 0:
	case 1:
		members = append(members, `"_v":[]`)
	case 2:
		members = append(members, `"_v":`+variant(0))
	case 3:
		members = append(members, `"_v":"no objects"`)
	default:
		n := 1 + rng.IntN(6)
		parts := make([]string, n)
		for k := range parts {
			if rng.IntN(12) == 0 {
				parts[k] = pick(rng, []string{`"x"`, `3`, `null`, `[]`, `[{"size":"L"}]`})
				continue
			}
			parts[k] = variant(k)
		}
		members = append(members, `"_v":[`+strings.Join(parts, ",")+`]`)
	}
	rng.Shuffle(len(members), func(i, j int) { members[i], members[j] = members[j], members[i] })
	return "{" + strings.Join(members, ",") + "}"
}

// randomInnerJSON is an inner query of _v: conditions on its sub-fields, mostly with
// an op the sub-field takes, under all, any and not.
func randomInnerJSON(rng *rand.Rand, depth int) string {
	if depth >= 2 || rng.IntN(3) == 0 {
		f := pick(rng, variantFields)
		op := pick(rng, query.OpsFor(testMapping.Fields[f]))
		value := randomValue(rng, op)
		switch {
		case f == "_v.size" && (op == query.OpEq || op == query.OpNe):
			value = jsonString(pick(rng, variantSizes))
		case f == "_v.price" && query.IsDateMathOp(op) && op != query.OpBetween:
			value = strconv.Itoa(rng.IntN(30))
		}
		return fmt.Sprintf(`{"field":%q,"op":%q,"value":%s}`, f, op, value)
	}
	switch rng.IntN(4) {
	case 0:
		return `{"not":` + randomInnerJSON(rng, depth) + `}`
	case 1:
		return `{"all":[` + randomInnerJSON(rng, depth+1) + `,` + randomInnerJSON(rng, depth+1) + `]}`
	default:
		return `{"any":[` + randomInnerJSON(rng, depth+1) + `,` + randomInnerJSON(rng, depth+1) + `]}`
	}
}

func randomNestedJSON(rng *rand.Rand) string {
	inner := `{"all":[]}`
	if rng.IntN(10) > 0 {
		inner = randomInnerJSON(rng, 0)
	}
	nested := `{"nested":{"path":"_v","query":` + inner + `}}`
	switch rng.IntN(6) {
	case 0:
		return `{"not":` + nested + `}`
	case 1:
		return `{"all":[` + nested + `,{"field":"price","op":"lt","value":` + strconv.Itoa(rng.IntN(100)) + `}]}`
	case 2:
		return `{"any":[` + nested + `,{"nested":{"path":"_v","query":` + randomInnerJSON(rng, 1) + `}}]}`
	case 3:
		return `{"all":[` + nested + `,{"field":"_v","op":"exists"}]}`
	default:
		return nested
	}
}

// variantCluster is two shards of variantDocs through upserts, deletes, refreshes and
// merges, so segments hold objects of documents deleted and documents updated.
func variantCluster(t *testing.T, rng *rand.Rand, n int) *cluster {
	t.Helper()
	c := newCluster(t, 2)
	for round := range 5 {
		for i := range n / 5 {
			id := round*n/5 + i
			c.upsert(fmt.Sprintf("p%04d", id), variantDoc(rng, id))
		}
		for range n / 20 {
			id := rng.IntN(n)
			c.upsert(fmt.Sprintf("p%04d", id), variantDoc(rng, id))
		}
		for range n / 30 {
			c.remove(fmt.Sprintf("p%04d", rng.IntN(n)))
		}
		c.refreshAll()
		if round == 3 {
			c.merge(0, 1)
			c.merge(1, 2)
		}
	}
	return c
}

func parseValid(t *testing.T, raw string) (query.Node, bool) {
	t.Helper()
	q, ps := query.Parse([]byte(raw))
	if len(ps) == 0 {
		ps = query.Validate(q, testMapping)
	}
	return q, len(ps) == 0
}

// TestNestedEqualsBruteForce: nested queries (inner nots, groups, several nested nodes,
// beside and under top-level conditions) over segments holding deleted and updated
// documents' objects, through merges, answer exactly what query.Match does: as the
// query, with every candidate verified and lazily for the top hits, as a post_filter and
// as a filter aggregation.
func TestNestedEqualsBruteForce(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x4e57))
	c := variantCluster(t, rng, 1500)
	docs := c.docs()
	checks, matched := 200, 0
	if longMode() {
		checks = 2000
	}
	for range checks {
		raw := randomNestedJSON(rng)
		q, ok := parseValid(t, raw)
		if !ok {
			continue
		}
		want := docIDs(brute(docs, q, nil))
		if len(want) > 0 {
			matched++
		}
		got, err := c.search(&Request{Query: q, Size: MaxSize, TrackTotal: TrackTotalAll})
		if err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		if ids := hitIDs(got.Hits); !slices.Equal(ids, want) || got.Total != int64(len(want)) {
			t.Fatalf("%s: %d hits (total %d), want %d", raw, len(ids), got.Total, len(want))
		}
		top, err := c.search(&Request{Query: q, Size: 5, TrackTotal: TrackTotalNone})
		if err != nil {
			t.Fatalf("%s, lazily: %v", raw, err)
		}
		if ids := hitIDs(top.Hits); !slices.Equal(ids, want[:min(5, len(want))]) {
			t.Fatalf("%s, lazily: top %v, want %v", raw, ids, want[:min(5, len(want))])
		}
		filtered, err := c.search(&Request{
			Query: &query.All{}, PostFilter: q, Size: MaxSize, TrackTotal: TrackTotalAll,
			Aggs: map[string]Agg{"in": {Type: AggFilter, Filter: q}},
		})
		if err != nil {
			t.Fatalf("%s as a post_filter and filter: %v", raw, err)
		}
		if ids := hitIDs(filtered.Hits); !slices.Equal(ids, want) || filtered.Aggs["in"].DocCount != int64(len(want)) {
			t.Fatalf("%s as a post_filter and filter: %d hits, %d counted, want %d", raw, len(ids), filtered.Aggs["in"].DocCount, len(want))
		}
	}
	if matched < checks/4 {
		t.Fatalf("only %d of %d queries matched a document: the corpus is too sparse", matched, checks)
	}
}

// chosen calls fn with each object of d's _v that where (nil: any) matches.
func chosen(d *schema.Doc, where *query.Compiled, fn func(o *schema.Object)) {
	v := d.Fields["_v"]
	for k := range v.Objects {
		o := &v.Objects[k]
		if o.IsObject() && (where == nil || where.MatchObject(o)) {
			fn(o)
		}
	}
}

// keysOf is an object's values of field, as a terms aggregation keys them.
func keysOf(o *schema.Object, field string) []any {
	v, ok := o.Fields[field]
	if !ok {
		return nil
	}
	switch testMapping.Fields[field] {
	case schema.Keyword, schema.Text:
		if v.Text != nil {
			return []any{*v.Text}
		}
	case schema.Number, schema.Date:
		if v.Number != nil {
			return []any{positiveZero(*v.Number)}
		}
	case schema.Bool:
		if v.Bool != nil {
			return []any{*v.Bool}
		}
	case schema.KeywordList:
		var out []any
		for _, e := range v.Entries {
			out = append(out, e)
		}
		return out
	}
	return nil
}

// bruteTerms counts each document once per value of field among its chosen objects.
func bruteTerms(docs []*schema.Doc, field string, where *query.Compiled) map[any]int64 {
	out := map[any]int64{}
	for _, d := range docs {
		seen := map[any]bool{}
		chosen(d, where, func(o *schema.Object) {
			for _, k := range keysOf(o, field) {
				if !seen[k] {
					seen[k] = true
					out[k]++
				}
			}
		})
	}
	return out
}

func compileWhere(t *testing.T, raw string) (query.Node, *query.Compiled) {
	t.Helper()
	if raw == "" {
		return nil, nil
	}
	n, ps := query.Parse([]byte(raw))
	if len(ps) == 0 {
		ps = query.ValidateInner(n, testMapping, "_v")
	}
	if len(ps) > 0 {
		return nil, nil
	}
	return n, query.Compile(n)
}

// TestNestedAggsEqualBruteForce: terms on every kind of sub-field count each matching
// document once per value among its objects (those where chooses), and stats, min, max
// and cardinality read every chosen object's value: at the top level and under a filter
// aggregation, exactly as a brute force over query.Match does.
func TestNestedAggsEqualBruteForce(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0xa995))
	c := variantCluster(t, rng, 1200)
	docs := c.docs()
	checks := 60
	if longMode() {
		checks = 600
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
		whereRaw := ""
		if rng.IntN(2) == 0 {
			whereRaw = randomInnerJSON(rng, 0)
		}
		whereNode, where := compileWhere(t, whereRaw)
		hits := brute(docs, q, nil)
		aggs := map[string]Agg{
			"stats": {Type: AggStats, Field: "_v.price", Where: whereNode},
			"min":   {Type: AggMin, Field: "_v.at", Where: whereNode},
			"max":   {Type: AggMax, Field: "_v.price", Where: whereNode},
			"card":  {Type: AggCardinality, Field: "_v.size", Threshold: 1000, Where: whereNode},
			"in":    {Type: AggFilter, Filter: &query.Leaf{Field: "price", Op: query.OpLt, Value: json.RawMessage("50")}, Aggs: map[string]Agg{"sizes": {Type: AggTerms, Field: "_v.size", Size: 1000, Where: whereNode}}},
		}
		for _, f := range []string{"_v.size", "_v.color", "_v.price", "_v.stock", "_v.tags"} {
			aggs["terms"+strings.TrimPrefix(f, "_v.")] = Agg{Type: AggTerms, Field: f, Size: 5000, Where: whereNode}
		}
		got, err := c.search(&Request{Query: q, Aggs: aggs, TrackTotal: TrackTotalAll})
		if err != nil {
			t.Fatalf("%s where %s: %v", raw, whereRaw, err)
		}
		if len(got.Aggs) != len(aggs) {
			t.Fatalf("%s where %s: aggregations %v of %d", raw, whereRaw, slices.Sorted(maps.Keys(got.Aggs)), len(aggs))
		}
		check := func(name string, res *AggResult, want map[any]int64) {
			t.Helper()
			gotCounts := map[any]int64{}
			for _, b := range res.Buckets {
				gotCounts[b.Key] = b.DocCount
			}
			if !maps.Equal(gotCounts, want) {
				var diff []string
				for k, n := range want {
					if gotCounts[k] != n {
						diff = append(diff, fmt.Sprintf("%v: %d, want %d", k, gotCounts[k], n))
					}
				}
				for k, n := range gotCounts {
					if _, ok := want[k]; !ok {
						diff = append(diff, fmt.Sprintf("%v: %d, want none", k, n))
					}
				}
				t.Fatalf("%s where %s: %s differs: %v", raw, whereRaw, name, diff[:min(len(diff), 10)])
			}
		}
		for _, f := range []string{"_v.size", "_v.color", "_v.price", "_v.stock", "_v.tags"} {
			check("terms"+f, got.Aggs["terms"+strings.TrimPrefix(f, "_v.")], bruteTerms(hits, f, where))
		}
		var cheap []*schema.Doc
		for _, d := range hits {
			if v := d.Fields["price"]; v.Number != nil && *v.Number < 50 {
				cheap = append(cheap, d)
			}
		}
		if got.Aggs["in"].Aggs["sizes"] == nil {
			b, _ := json.Marshal(got.Aggs["in"])
			t.Fatalf("%s where %s: no sizes: %s", raw, whereRaw, b)
		}
		check("terms under a filter", got.Aggs["in"].Aggs["sizes"], bruteTerms(cheap, "_v.size", where))
		var count int64
		var sum float64
		var lo, hi *float64
		var earliest *float64
		sizes := map[string]bool{}
		for _, d := range hits {
			chosen(d, where, func(o *schema.Object) {
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
				if v := o.Fields["_v.at"].Number; v != nil && (earliest == nil || *v < *earliest) {
					earliest = v
				}
				if v := o.Fields["_v.size"].Text; v != nil {
					sizes[*v] = true
				}
			})
		}
		st := got.Aggs["stats"]
		if st.Count != count || !floatEq(st.Sum, sum) || !ptrEq(st.Min, lo) || !ptrEq(st.Max, hi) {
			t.Fatalf("%s where %s: stats %d %g %v %v, want %d %g %v %v", raw, whereRaw, st.Count, st.Sum, st.Min, st.Max, count, sum, lo, hi)
		}
		if !ptrEq(got.Aggs["max"].Number, hi) || !ptrEq(got.Aggs["min"].Number, earliest) {
			t.Fatalf("%s where %s: max %v min %v, want %v %v", raw, whereRaw, got.Aggs["max"].Number, got.Aggs["min"].Number, hi, earliest)
		}
		if got.Aggs["card"].Value != int64(len(sizes)) {
			t.Fatalf("%s where %s: cardinality %d, want %d", raw, whereRaw, got.Aggs["card"].Value, len(sizes))
		}
	}
}

// nestedSortValue is d's value for a sort on field: the least (or, when greatest, the
// greatest) among its chosen objects.
func nestedSortValue(d *schema.Doc, field string, greatest bool, where *query.Compiled) any {
	var best any
	chosen(d, where, func(o *schema.Object) {
		for _, k := range keysOf(o, field) {
			if best == nil || (greatest && cmpValue(k, best, false) > 0) || (!greatest && cmpValue(k, best, false) < 0) {
				best = k
			}
		}
	})
	return best
}

// TestNestedSortEqualsBruteForce: a sort on a sub-field orders the hits by the least
// (min) or greatest (max) value among their objects (those where chooses), a hit with
// none last, ties by id; paging with search_after walks the same order.
func TestNestedSortEqualsBruteForce(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x5027))
	c := variantCluster(t, rng, 800)
	docs := c.docs()
	checks := 40
	if longMode() {
		checks = 400
	}
	for range checks {
		raw := `{"all":[]}`
		if rng.IntN(2) == 0 {
			raw = randomNestedJSON(rng)
		}
		q, ok := parseValid(t, raw)
		if !ok {
			continue
		}
		whereRaw := ""
		if rng.IntN(2) == 0 {
			whereRaw = randomInnerJSON(rng, 0)
		}
		whereNode, where := compileWhere(t, whereRaw)
		field := pick(rng, []string{"_v.price", "_v.size", "_v.at", "_v.color"})
		desc := rng.IntN(2) == 0
		mode := pick(rng, []string{"", SortModeMin, SortModeMax})
		greatest := mode == SortModeMax || (mode == "" && desc)
		sorts := []SortField{{Field: field, Desc: desc, Mode: mode, Where: whereNode}}
		hits := brute(docs, q, nil)
		slices.SortStableFunc(hits, func(a, b *schema.Doc) int {
			if c := cmpValue(nestedSortValue(a, field, greatest, where), nestedSortValue(b, field, greatest, where), desc); c != 0 {
				return c
			}
			return cmp.Compare(a.ID, b.ID)
		})
		got, err := c.search(&Request{Query: q, Sort: sorts, Size: MaxSize, TrackTotal: TrackTotalAll})
		if err != nil {
			t.Fatalf("%s sorted on %s: %v", raw, field, err)
		}
		if ids := hitIDs(got.Hits); !slices.Equal(ids, docIDs(hits)) {
			t.Fatalf("%s sorted on %s (desc %v, mode %q, where %s): %v\nwant %v", raw, field, desc, mode, whereRaw, ids, docIDs(hits))
		}
		for i, h := range got.Hits {
			if want := nestedSortValue(hits[i], field, greatest, where); cmpValue(h.Sort[0], want, false) != 0 {
				t.Fatalf("%s: hit %s sorts by %v, want %v", raw, h.ID, h.Sort[0], want)
			}
		}
		var paged []string
		var after []any
		for {
			page, err := c.search(&Request{Query: q, Sort: sorts, Size: 37, SearchAfter: after, TrackTotal: TrackTotalNone})
			if err != nil {
				t.Fatalf("%s paged: %v", raw, err)
			}
			paged = append(paged, hitIDs(page.Hits)...)
			if page.Next == nil {
				break
			}
			after = page.Next
		}
		if !slices.Equal(paged, docIDs(hits)) {
			t.Fatalf("%s sorted on %s, paged: %d hits, want %d", raw, field, len(paged), len(hits))
		}
	}
}

// TestNestedInnerHits: each hit's inner hits are the members of its _v that satisfy
// the nested query, by offset in array order, the first size of them as objects of the
// fields asked for; a body limited by fields is limited after they are read.
func TestNestedInnerHits(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x1441))
	c := variantCluster(t, rng, 500)
	docs := c.docs()
	byID := map[string]*schema.Doc{}
	for _, d := range docs {
		byID[d.ID] = d
	}
	for range 40 {
		inner := randomInnerJSON(rng, 0)
		raw := `{"nested":{"path":"_v","query":` + inner + `,"inner_hits":{"size":2,"fields":["_v._key","_v.price"]}}}`
		q, ok := parseValid(t, raw)
		if !ok {
			continue
		}
		r := &Request{Query: q, Size: 50, Fields: []string{"brand"}}
		wide := *r
		wide.Fields = InnerHitsFields(r)
		got, err := c.search(&wide)
		if err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		if err := FillInnerHits(got.Hits, &wide, testMapping, r.Fields); err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		innerNode, _ := query.Parse([]byte(inner))
		m := query.Compile(innerNode)
		for _, h := range got.Hits {
			var body map[string]json.RawMessage
			if err := json.Unmarshal(h.Body, &body); err != nil || len(body) > 1 || (len(body) == 1 && body["brand"] == nil) {
				t.Fatalf("%s: hit %s's body %s, limited to brand", raw, h.ID, h.Body)
			}
			ih := h.InnerHits["_v"]
			var offsets []int
			objects := byID[h.ID].Fields["_v"].Objects
			for k := range objects {
				if m.MatchObject(&objects[k]) {
					offsets = append(offsets, k)
				}
			}
			if ih == nil || ih.Total != len(offsets) || !slices.Equal(ih.Offsets, offsets[:min(2, len(offsets))]) || len(ih.Objects) != len(ih.Offsets) {
				t.Fatalf("%s: hit %s's inner hits %+v, want offsets %v", raw, h.ID, ih, offsets)
			}
			for i, obj := range ih.Objects {
				var o map[string]any
				if err := json.Unmarshal(obj, &o); err != nil {
					t.Fatal(err)
				}
				id, _ := strconv.Atoi(strings.TrimPrefix(h.ID, "p"))
				if want := fmt.Sprintf("SKU-%d-%d", id, ih.Offsets[i]); o["_key"] != want {
					t.Fatalf("%s: hit %s's inner hit %s, want _key %s", raw, h.ID, obj, want)
				}
				for k := range o {
					if k != "_key" && k != "price" {
						t.Fatalf("%s: inner hit %s holds %q, not asked for", raw, obj, k)
					}
				}
			}
		}
	}
}

// The nested refusals: sub-fields outside a nested node, other fields inside one,
// inner_hits under a not, aggregations and sorts a sub-field does not take.
func TestNestedRefusals(t *testing.T) {
	c := newCluster(t, 1)
	c.upsert("a", `{"_v":[{"size":"L","price":3}]}`)
	c.refreshAll()
	for raw, want := range map[string]string{
		`{"field":"_v.size","op":"eq","value":"L"}`:                                                                "query.field",
		`{"nested":{"path":"_v","query":{"field":"brand","op":"eq","value":"x"}}}`:                                 "query.nested.query.field",
		`{"nested":{"path":"brand","query":{"field":"_v.size","op":"eq","value":"x"}}}`:                            "query.nested.path",
		`{"not":{"nested":{"path":"_v","query":{"all":[]},"inner_hits":{}}}}`:                                      "query.not.nested.inner_hits",
		`{"nested":{"path":"_v","query":{"nested":{"path":"_v","query":{"all":[]}}}}}`:                             "query.nested.query.nested",
		`{"nested":{"path":"_v","inner_hits":{"fields":["brand"]}}}`:                                               "query.nested.inner_hits.fields.0",
		`{"all":[{"nested":{"path":"_v","inner_hits":{}}},{"nested":{"path":"_v","inner_hits":{}}}]}`:              "query.all.1.nested.inner_hits",
		`{"nested":{"path":"_v","query":{"field":"_v.size","op":"lt","value":3}}}`:                                 "query.nested.query.op",
		`{"nested":{"path":"_v","query":{"all":[{"field":"_v.price","op":"lt","value":3},{"any":[{"all":[]}]}]}}}`: "query.nested.query.all.1.any.0.all",
	} {
		q, ps := query.Parse([]byte(raw))
		if len(ps) == 0 {
			ps = query.Validate(q, testMapping)
		}
		if len(ps) == 0 || ps[0].Loc != want {
			t.Errorf("%s: problems %v, want one at %s", raw, ps, want)
		}
	}
	for name, r := range map[string]*Request{
		"a histogram on a sub-field":   {Query: &query.All{}, Aggs: map[string]Agg{"h": {Type: AggHistogram, Field: "_v.price", Interval: 1}}},
		"a sub-field under terms":      {Query: &query.All{}, Aggs: map[string]Agg{"t": {Type: AggTerms, Field: "brand", Aggs: map[string]Agg{"m": {Type: AggMin, Field: "_v.price"}}}}},
		"sub-aggregations of one":      {Query: &query.All{}, Aggs: map[string]Agg{"t": {Type: AggTerms, Field: "_v.size", Aggs: map[string]Agg{"m": {Type: AggMin, Field: "price"}}}}},
		"a where on a top-level field": {Query: &query.All{}, Aggs: map[string]Agg{"t": {Type: AggTerms, Field: "brand", Where: &query.All{}}}},
		"the nested field itself":      {Query: &query.All{}, Aggs: map[string]Agg{"t": {Type: AggTerms, Field: "_v"}}},
		"a sort on the nested field":   {Query: &query.All{}, Sort: []SortField{{Field: "_v"}}},
		"a sort mode on a field":       {Query: &query.All{}, Sort: []SortField{{Field: "price", Mode: SortModeMin}}},
		"an unknown mode":              {Query: &query.All{}, Sort: []SortField{{Field: "_v.price", Mode: "avg"}}},
		"a sort on a list sub-field":   {Query: &query.All{}, Sort: []SortField{{Field: "_v.tags"}}},
	} {
		var re *RequestError
		if _, err := c.search(r); err == nil || !asRequestError(err, &re) {
			t.Errorf("%s: %v, want a request error", name, err)
		}
	}
	r := &Request{
		Query: &query.All{}, Sort: []SortField{{Field: "_v.price", Where: &query.Leaf{Field: "brand", Op: query.OpExists}}},
		Aggs: map[string]Agg{"f": {Type: AggFilter, Filter: mustParse(t, `{"nested":{"path":"_v","inner_hits":{}}}`)}},
	}
	ps := ValidateQueries(r, testMapping)
	locs := make([]string, len(ps))
	for i, p := range ps {
		locs[i] = p.Loc
	}
	if !slices.Contains(locs, "sort.0._v.price.where.field") || !slices.Contains(locs, "aggs.f.filter.nested.inner_hits") || !r.Nested {
		t.Errorf("ValidateQueries: %v, nested %v", ps, r.Nested)
	}
}

func asRequestError(err error, re **RequestError) bool {
	e, ok := err.(*RequestError) //nolint:errorlint // ExecuteShard returns it unwrapped
	*re = e
	return ok
}

// The query and the post_filter cannot ask for inner hits under one key.
func TestNestedInnerHitsKeysAcrossQueries(t *testing.T) {
	hits := mustParse(t, `{"nested":{"path":"_v","inner_hits":{}}}`)
	r := &Request{Query: hits, PostFilter: &query.All{Children: []query.Node{hits}}}
	ps := ValidateQueries(r, testMapping)
	if len(ps) != 1 || ps[0].Loc != "post_filter.all.0.nested.inner_hits" {
		t.Fatalf("problems %v", ps)
	}
	r.PostFilter = mustParse(t, `{"nested":{"path":"_v","inner_hits":{"name":"other"}}}`)
	if ps := ValidateQueries(r, testMapping); len(ps) > 0 {
		t.Fatalf("two keys: %v", ps)
	}
}

// The request cache keys a nested node by its path and inner query, and an
// aggregation by its where; a where reading now unrounded turns the cache off.
func TestRequestCacheNested(t *testing.T) {
	key := func(raw string) []byte {
		t.Helper()
		k, ok := requestCacheKey(parseReq(t, raw))
		if !ok {
			t.Fatalf("%s is not cacheable", raw)
		}
		return k
	}
	q := func(size string) string {
		return `{"size":0,"query":{"nested":{"path":"_v","query":{"field":"_v.size","op":"eq","value":"` + size + `"}}}}`
	}
	if !bytes.Equal(key(q("L")), key(q("l"))) || bytes.Equal(key(q("L")), key(q("M"))) {
		t.Fatal("nested nodes' keys")
	}
	where := func(size string) string {
		return `{"size":0,"aggs":{"t":{"terms":{"field":"_v.size","where":{"field":"_v.size","op":"eq","value":"` + size + `"}}}}}`
	}
	if bytes.Equal(key(where("L")), key(where("M"))) || bytes.Equal(key(where("L")), key(`{"size":0,"aggs":{"t":{"terms":{"field":"_v.size"}}}}`)) {
		t.Fatal("an aggregation's where is not in its key")
	}
	r := parseReq(t, `{"size":0,"aggs":{"t":{"terms":{"field":"_v.size","where":{"field":"_v.at","op":"gte","value":"now-1d"}}}}}`)
	if ps := ResolveQueries(r, time.Now()); len(ps) > 0 || !r.NoRequestCache {
		t.Fatalf("a where reading now unrounded: %v, cache off %v", ps, r.NoRequestCache)
	}
}
