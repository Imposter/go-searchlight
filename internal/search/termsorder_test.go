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

// orderSubs are the sub-aggregations the terms order tests order by.
var orderSubs = map[string]Agg{
	"low":  {Type: AggMin, Field: "price"},
	"high": {Type: AggMax, Field: "price"},
	"st":   {Type: AggStats, Field: "price"},
	"n":    {Type: AggCardinality, Field: "tags"},
	"tm":   {Type: AggTopMetrics, Metrics: []string{"price"}, TopSort: &SortField{Field: "created", Desc: true}},
	"pct":  {Type: AggPercentiles, Field: "price", Percents: []float64{50, 99.9}},
	"rk":   {Type: AggPercentileRanks, Field: "price", Values: []float64{5, 30}},
}

// bruteOrderValue is a group of documents' value for order key key, as a terms
// aggregation ordering by it reads its sub-aggregations (orderSubs).
func bruteOrderValue(key string, groupKey any, docs []*schema.Doc) any {
	var prices []float64
	for _, d := range docs {
		for _, v := range aggValues(d, "price") {
			prices = append(prices, v.(float64)) //nolint:forcetypeassert,errcheck // numbers only
		}
	}
	switch key {
	case OrderCount:
		return float64(len(docs))
	case OrderKey:
		return groupKey
	case "low", "low.value", "high", "high.value", "st.min", "st.max":
		if len(prices) == 0 {
			return nil
		}
		if strings.HasPrefix(key, "low") || key == "st.min" {
			return slices.Min(prices)
		}
		return slices.Max(prices)
	case "st.count":
		return float64(len(prices))
	case "st.sum", "st.avg":
		var sum float64
		for _, p := range prices {
			sum += p
		}
		if key == "st.sum" {
			return sum
		}
		if len(prices) == 0 {
			return nil
		}
		return sum / float64(len(prices))
	case "n":
		seen := map[any]bool{}
		for _, d := range docs {
			for _, v := range aggValues(d, "tags") {
				seen[v] = true
			}
		}
		return float64(len(seen))
	case "tm.price":
		top := bruteTop(docs, "created", true, 1, []string{"price"})
		if len(top) == 0 {
			return nil
		}
		return top[0].Metrics["price"]
	case "pct.50", "pct.99.9", "pct[99.9]", "rk.5", "rk[30.0]":
		if len(prices) == 0 {
			return nil
		}
		slices.Sort(prices)
		switch key {
		case "pct.50":
			return exactQuantile(prices, 0.5)
		case "pct.99.9", "pct[99.9]":
			return exactQuantile(prices, 0.999)
		case "rk.5":
			return exactRank(prices, 5) * 100
		}
		return exactRank(prices, 30) * 100
	}
	panic("unknown order key " + key)
}

type bruteTerm struct {
	key  any
	docs []*schema.Doc
}

// bruteTermsOrder is the size first terms of field over docs in order (then _key
// ascending).
func bruteTermsOrder(docs []*schema.Doc, field string, size int, order []TermsOrder) []*bruteTerm {
	byKey := map[any]*bruteTerm{}
	var all []*bruteTerm
	for _, d := range docs {
		for _, v := range aggValues(d, field) {
			g := byKey[v]
			if g == nil {
				g = &bruteTerm{key: v}
				byKey[v] = g
				all = append(all, g)
			}
			if len(g.docs) == 0 || g.docs[len(g.docs)-1] != d {
				g.docs = append(g.docs, d)
			}
		}
	}
	keys := append(slices.Clone(order), TermsOrder{Key: OrderKey})
	slices.SortFunc(all, func(a, b *bruteTerm) int {
		for _, k := range keys {
			if c := cmpValue(bruteOrderValue(k.Key, a.key, a.docs), bruteOrderValue(k.Key, b.key, b.docs), k.Desc); c != 0 {
				return c
			}
		}
		return 0
	})
	return all[:min(size, len(all))]
}

var testOrders = [][]TermsOrder{
	{{Key: "st.avg", Desc: true}},
	{{Key: "low"}},
	{{Key: OrderCount}},
	{{Key: OrderKey, Desc: true}},
	{{Key: OrderKey}},
	{{Key: "n", Desc: true}, {Key: OrderCount, Desc: true}},
	{{Key: "tm.price", Desc: true}},
	{{Key: OrderCount, Desc: true}, {Key: "st.sum"}},
	{{Key: "high.value"}, {Key: OrderKey, Desc: true}},
	{{Key: "st.count", Desc: true}},
	{{Key: "pct.50", Desc: true}},
	{{Key: "pct[99.9]"}, {Key: OrderCount}},
	{{Key: "rk.5", Desc: true}, {Key: "pct.99.9", Desc: true}},
	{{Key: "rk[30.0]"}},
}

// TestTermsOrderEqualsBruteForce: ordered by a sub-aggregation, by _count ascending or
// by _key, a terms aggregation over several shards of several segments answers as the
// brute force does with the default shard_size (one past every term under _count
// descending, whose bounds are the default order's), merged by term or by global ordinal,
// segments collected whole or in parts, over keyword, keyword_list, number and bool
// fields, at the top level and under a filter.
func TestTermsOrderEqualsBruteForce(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x0de4))
	c := orderedCluster(t, rng, 3, 900, 25)
	q := &query.Leaf{Field: "created", Op: query.OpExists}
	hits := brute(c.docs(), q, nil)
	for _, field := range []string{"brand", "tags", "price", "active"} {
		for _, order := range testOrders {
			at := fmt.Sprintf("%s by %v", field, order)
			a := Agg{Type: AggTerms, Field: field, Size: 5, Order: order, Aggs: orderSubs}
			if order[0].Key == OrderCount && order[0].Desc {
				a.ShardSize = 1000 // its error bounds are the default order's: past them, exact
			}
			if rng.IntN(2) == 0 {
				aggChunkMin, aggChunk = 1, 1+rng.Uint32N(40)
			}
			globalOrdsDensity = pick(rng, []uint64{0, 8, 1 << 40})
			resp, err := c.search(&Request{Query: q, Aggs: map[string]Agg{
				"t": a,
				"f": {Type: AggFilter, Filter: &query.All{}, Aggs: map[string]Agg{"t": a}},
			}})
			aggChunkMin, aggChunk, globalOrdsDensity = 1<<15, 1<<16, 8
			if err != nil {
				t.Fatalf("%s: %v", at, err)
			}
			want := bruteTermsOrder(hits, field, 5, order)
			for name, res := range map[string]*AggResult{"top": resp.Aggs["t"], "under filter": resp.Aggs["f"].Aggs["t"]} {
				if res.DocCountErrorUpperBound != 0 {
					t.Errorf("%s, %s: error bound %d with every term returned", at, name, res.DocCountErrorUpperBound)
				}
				if len(res.Buckets) != len(want) {
					t.Fatalf("%s, %s: %d buckets, want %d", at, name, len(res.Buckets), len(want))
				}
				for i, b := range res.Buckets {
					w := want[i]
					if b.Key != w.key || b.DocCount != int64(len(w.docs)) || b.DocCountErrorUpperBound != 0 {
						t.Fatalf("%s, %s: bucket %d is %v (%d, ±%d), want %v (%d)\n got %s\nwant %s", at, name, i, b.Key, b.DocCount, b.DocCountErrorUpperBound, w.key, len(w.docs), termKeys(res.Buckets), bruteKeys(want))
					}
					if low := bruteOrderValue("low", w.key, w.docs); !ptrEq(b.Aggs["low"].Number, floatPtr(low)) {
						t.Fatalf("%s, %s: bucket %v's min %v, want %v", at, name, w.key, deref(b.Aggs["low"].Number), low)
					}
				}
			}
		}
	}
}

func floatPtr(v any) *float64 {
	if f, ok := v.(float64); ok {
		return &f
	}
	return nil
}

func bruteKeys(ts []*bruteTerm) string {
	out := make([]string, len(ts))
	for i, t := range ts {
		out[i] = fmt.Sprint(t.key)
	}
	return strings.Join(out, " ")
}

// TestTermsOrderShardSize: under a shard_size that makes shards cut their lists, an
// order by _key stays exact (its error bound 0); an order by a sub-aggregation or by
// _count ascending says -1 where it cannot tell, and every bucket whose bound is 0 is
// exact; on one shard every order is exact.
func TestTermsOrderShardSize(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x5a2d))
	c := orderedCluster(t, rng, 4, 1200, 40)
	one := orderedCluster(t, rng, 1, 600, 40)
	for _, order := range testOrders {
		for _, shardSize := range []int{5, 7, 12} {
			a := Agg{Type: AggTerms, Field: "brand", Size: 5, ShardSize: shardSize, Order: order, Aggs: orderSubs}
			at := fmt.Sprintf("by %v, shard_size %d", order, shardSize)
			resp, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"t": a}})
			if err != nil {
				t.Fatal(err)
			}
			res := resp.Aggs["t"]
			truth := map[any]int64{}
			for _, g := range bruteTermsOrder(c.docs(), "brand", 1000, nil) {
				truth[g.key] = int64(len(g.docs))
			}
			byKey := order[0].Key == OrderKey
			switch {
			case byKey:
				if want := bruteTermsOrder(c.docs(), "brand", 5, order); res.DocCountErrorUpperBound != 0 || termKeys(res.Buckets) != bruteKeys(want) {
					t.Errorf("%s: %s (±%d), want %s exactly", at, termKeys(res.Buckets), res.DocCountErrorUpperBound, bruteKeys(want))
				}
			case order[0].Key == OrderCount && order[0].Desc:
				if res.DocCountErrorUpperBound < 0 {
					t.Errorf("%s: error bound %d", at, res.DocCountErrorUpperBound)
				}
			default:
				if res.DocCountErrorUpperBound != -1 {
					t.Errorf("%s: error bound %d: 40 brands over 4 shards cut at %d bound nothing", at, res.DocCountErrorUpperBound, shardSize)
				}
			}
			for _, b := range res.Buckets {
				n := truth[b.Key]
				switch e := b.DocCountErrorUpperBound; {
				case e == 0 && b.DocCount != n:
					t.Errorf("%s: %v counts %d with bound 0, truth %d", at, b.Key, b.DocCount, n)
				case e > 0 && (b.DocCount > n || n > b.DocCount+e):
					t.Errorf("%s: %v counts %d (±%d), truth %d", at, b.Key, b.DocCount, e, n)
				case e < 0 && b.DocCount > n:
					t.Errorf("%s: %v counts %d, more than its %d", at, b.Key, b.DocCount, n)
				}
			}
			resp, err = one.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"t": a}})
			if err != nil {
				t.Fatal(err)
			}
			if want := bruteTermsOrder(one.docs(), "brand", 5, order); resp.Aggs["t"].DocCountErrorUpperBound != 0 || termKeys(resp.Aggs["t"].Buckets) != bruteKeys(want) {
				t.Errorf("%s, one shard: %s (±%d), want %s", at, termKeys(resp.Aggs["t"].Buckets), resp.Aggs["t"].DocCountErrorUpperBound, bruteKeys(want))
			}
		}
	}
}

// termKeys are the buckets' keys, as bruteKeys writes them.
func termKeys(bs []*Bucket) string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = fmt.Sprint(b.Key)
	}
	return strings.Join(out, " ")
}

// TestTermsOrderJSON: order as the body gives it, an object or a list, and the default
// order unchanged.
func TestTermsOrderJSON(t *testing.T) {
	c := newCluster(t, 2)
	for i, b := range []string{"acme", "zeta", "acme", "beta", "zeta", "zeta"} {
		c.upsert(fmt.Sprintf("d%d", i), fmt.Sprintf(`{"brand":%q,"price":%d}`, b, 10-i))
	}
	c.refreshAll()
	for body, want := range map[string]string{
		`{"terms":{"field":"brand"}}`:                                                                                         "zeta acme beta",
		`{"terms":{"field":"brand","order":{"_count":"asc"}}}`:                                                                "beta acme zeta",
		`{"terms":{"field":"brand","order":{"_key":"desc"}}}`:                                                                 "zeta beta acme",
		`{"terms":{"field":"brand","order":[{"p.max":"desc"}]},"aggs":{"p":{"stats":{"field":"price"}}}}`:                     "acme zeta beta",
		`{"terms":{"field":"brand","order":[{"p":"asc"},{"_count":"desc"}]},"aggs":{"p":{"min":{"field":"price"}}}}`:          "zeta beta acme",
		`{"terms":{"field":"brand","order":{"p.50":"asc"}},"aggs":{"p":{"percentiles":{"field":"price"}}}}`:                   "zeta beta acme",
		`{"terms":{"field":"brand","order":{"p[1.0]":"desc"}},"aggs":{"p":{"percentiles":{"field":"price"}}}}`:                "acme beta zeta",
		`{"terms":{"field":"brand","order":{"r.8":"desc"}},"aggs":{"r":{"percentile_ranks":{"field":"price","values":[8]}}}}`: "beta zeta acme",
	} {
		r := parseAggsBody(t, `{"size":0,"aggs":{"t":`+body+`}}`)
		resp, err := c.search(r)
		if err != nil {
			t.Fatal(err)
		}
		if got := termKeys(resp.Aggs["t"].Buckets); got != want {
			raw, _ := json.Marshal(resp.Aggs["t"])
			t.Errorf("%s: %s (%s), want %s", body, got, raw, want)
		}
	}
}

// TestTermsOrderNested: a terms aggregation on a nested field's sub-field orders by a
// metric of its objects or of its documents.
func TestTermsOrderNested(t *testing.T) {
	c := newCluster(t, 2)
	for i, body := range []string{
		`{"price":10,"_v":[{"size":"s","price":5},{"size":"m","price":7}]}`,
		`{"price":20,"_v":[{"size":"m","price":1}]}`,
		`{"price":30,"_v":[{"size":"l","price":9},{"size":"s","price":8}]}`,
		`{"price":40,"_v":[{"size":"l","price":2}]}`,
	} {
		c.upsert(fmt.Sprintf("d%d", i), body)
	}
	c.refreshAll()
	for order, want := range map[string]string{
		`{"vmin":"asc"}`:  "m l s",
		`{"top":"desc"}`:  "l s m",
		`{"vmin":"desc"}`: "s l m",
	} {
		r := parseAggsBody(t, `{"size":0,"aggs":{"t":{"terms":{"field":"_v.size","order":`+order+`},
			"aggs":{"vmin":{"min":{"field":"_v.price"}},"top":{"max":{"field":"price"}}}}}}`)
		if problems := ValidateQueries(r, testMapping); len(problems) > 0 {
			t.Fatal(problems)
		}
		resp, err := c.search(r)
		if err != nil {
			t.Fatal(err)
		}
		if got := termKeys(resp.Aggs["t"].Buckets); got != want {
			t.Errorf("order %s: %s, want %s", order, got, want)
		}
	}
}

// TestTermsOrderPercentilesApprox: ordered by a percentile over terms whose digests are
// approximate (more distinct values than their compression, over several shards), a
// terms aggregation returns every term in the order of the values it shows, the merged
// digests', each near the term's exact percentile.
func TestTermsOrderPercentilesApprox(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x9c71))
	const brands = 6
	c := newCluster(t, 3)
	values := map[string][]float64{}
	for i := range 1800 {
		b := fmt.Sprintf("B%d", rng.IntN(brands))
		v := rng.NormFloat64()*10 + float64(len(b)+int(b[1]-'0'))*3
		values[b] = append(values[b], v)
		c.upsert(fmt.Sprintf("d%05d", i), fmt.Sprintf(`{"brand":%q,"price":%v}`, b, v))
		if rng.IntN(300) == 0 {
			c.refresh(rng.IntN(3))
		}
	}
	c.refreshAll()
	for _, desc := range []bool{false, true} {
		a := Agg{
			Type: AggTerms, Field: "brand", Size: brands, Order: []TermsOrder{{Key: "p.90", Desc: desc}},
			Aggs: map[string]Agg{"p": {Type: AggPercentiles, Field: "price", Percents: []float64{90}, Compression: 50}},
		}
		resp, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"t": a}})
		if err != nil {
			t.Fatal(err)
		}
		res := resp.Aggs["t"]
		if len(res.Buckets) != len(values) || res.DocCountErrorUpperBound != 0 {
			t.Fatalf("desc %t: %d buckets (±%d), want %d exactly", desc, len(res.Buckets), res.DocCountErrorUpperBound, len(values))
		}
		var prev float64
		for i, b := range res.Buckets {
			v := *b.Aggs["p"].Percentiles[0]
			if i > 0 && cmpValue(prev, v, desc) > 0 {
				t.Errorf("desc %t: %s (%v) sorts after %v\n%s", desc, b.Key, v, prev, termKeys(res.Buckets))
			}
			prev = v
			sorted := slices.Sorted(slices.Values(values[b.Key.(string)])) //nolint:forcetypeassert,errcheck // keyword terms
			if e := rankError(sorted, v, 0.9); e > 0.05 {
				t.Errorf("desc %t: %s's 90th percentile %v is %.3f off in rank", desc, b.Key, v, e)
			}
		}
	}
}

// TestTermsOrderPercentilesProblems: an order by a percentile names the percents (or
// values) its sub-aggregation has, and how to write one.
func TestTermsOrderPercentilesProblems(t *testing.T) {
	subs := `"aggs":{"p":{"percentiles":{"field":"price","percents":[50,99.9]}},"r":{"percentile_ranks":{"field":"price","values":[100]}}}`
	for order, want := range map[string]string{
		`{"p.75":"asc"}`:  `"p" is a percentiles of percents 50, 99.9: 75 is none of them`,
		`{"p":"asc"}`:     `"p" is a percentiles: order by one of its percents, p.<number> (p.50, or p[50])`,
		`{"p.x":"asc"}`:   `"p" is a percentiles: order by one of its percents`,
		`{"r.5":"asc"}`:   `"r" is a percentile_ranks of values 100: 5 is none of them`,
		`{"r":"asc"}`:     `"r" is a percentile_ranks: order by one of its values, r.<number> (r.100, or r[100])`,
		`{"q.50":"desc"}`: `name.50 for a percentiles`,
	} {
		ps := aggProblems(t, `{"t":{"terms":{"field":"brand","order":`+order+`},`+subs+`}}`)
		if !slices.ContainsFunc(ps, func(p query.Problem) bool {
			return p.Loc == "aggs.t.terms.order.0" && strings.Contains(p.Message, want)
		}) {
			t.Errorf("order %s: %v, want %q at aggs.t.terms.order.0", order, ps, want)
		}
	}
}
