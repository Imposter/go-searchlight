package workloads

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/Imposter/go-searchlight/bench/datasets"
	"github.com/Imposter/go-searchlight/bench/report"
)

// SearchSpec is a search workload: Searchlight search bodies cycled through in order.
type SearchSpec struct {
	Name        string
	Group       string
	Description string
	Bodies      [][]byte
	// PageDepth, when above 0, makes each iteration a search_after walk of this many
	// hits (latency per page).
	PageDepth int
}

// q builds a Searchlight query node.
type q = map[string]any

func cond(field, op string, value any) q { return q{"field": field, "op": op, "value": value} }
func allOf(children ...any) q            { return q{"all": children} }
func notOf(child any) q                  { return q{"not": child} }

func body(v map[string]any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // maps of JSON values only
	}
	return b
}

// rank draws a Zipfian rank below n, skewed to the head like real traffic.
func rank(r *rand.Rand, n int) int {
	return min(int(math.Floor(math.Pow(r.Float64(), 2.5)*float64(n))), n-1)
}

func price(r *rand.Rand) float64 {
	return math.Round(math.Exp(3.6+1.2*r.NormFloat64())/5)*5 + 5
}

func needle(r *rand.Rand) string {
	switch r.IntN(3) {
	case 0:
		return datasets.TitleAdjective(r.IntN(10)) + " " + datasets.TitleNoun(rank(r, 40))
	case 1:
		return datasets.TitleNoun(rank(r, 80))
	default:
		return datasets.Brand(rank(r, 200))
	}
}

// SearchSpecs returns the search workloads, each with variants bodies, under seed.
func SearchSpecs(seed uint64, variants, pageDepth int) []SearchSpec {
	r := rand.New(rand.NewPCG(seed, 0x5eed)) //nolint:gosec // reproducible workload values
	gen := func(f func(i int) map[string]any) [][]byte {
		out := make([][]byte, variants)
		for i := range out {
			out[i] = body(f(i))
		}
		return out
	}
	aggFilter := func(i int) any {
		if i%4 == 0 {
			return q{"all": []any{}}
		}
		return cond("source", "eq", datasets.Sources[r.IntN(len(datasets.Sources))])
	}
	specs := []SearchSpec{
		{Name: "filter_term", Group: report.GroupFilter, Description: "brand eq (keyword term)", Bodies: gen(func(int) map[string]any {
			return q{"query": cond("brand", "eq", datasets.Brand(rank(r, 400))), "size": 10}
		})},
		{Name: "filter_has", Group: report.GroupFilter, Description: "tags has_any (keyword list)", Bodies: gen(func(int) map[string]any {
			return q{"query": cond("tags", "has_any", []string{datasets.Tag(rank(r, 300)), datasets.Tag(rank(r, 300))}), "size": 10}
		})},
		{Name: "filter_range", Group: report.GroupFilter, Description: "price between (number range)", Bodies: gen(func(int) map[string]any {
			lo := price(r)
			return q{"query": cond("price", "between", []float64{lo, lo * (1.1 + r.Float64())}), "size": 10}
		})},
		{Name: "filter_date_range", Group: report.GroupFilter, Description: "updated_at gte (date range)", Bodies: gen(func(int) map[string]any {
			at := datasets.BaseTime.Add(datasets.DateSpan - time.Duration(1+r.IntN(60))*24*time.Hour)
			return q{"query": cond("updated_at", "gte", at.UnixMilli()), "size": 10}
		})},
		{Name: "filter_bool", Group: report.GroupFilter, Description: "category eq ∧ price lte ∧ in_stock ∧ ¬condition eq used", Bodies: gen(func(int) map[string]any {
			return q{"query": allOf(
				cond("category", "eq", datasets.Category(rank(r, 150))),
				cond("price", "lte", price(r)*2),
				cond("in_stock", "eq", true),
				notOf(cond("condition", "eq", "used")),
			), "size": 10}
		})},
		{Name: "filter_contains", Group: report.GroupFilter, Description: "title contains (substring)", Bodies: gen(func(int) map[string]any {
			return q{"query": cond("title", "contains", needle(r)), "size": 10}
		})},
		// description is the long text field (about 60 Zipfian words, versus title's
		// handful); every other contains workload targets title, so a regression
		// specific to long-text residuals (format 4's trigram prefilter over a much
		// longer value) went unseen until this one was added.
		{Name: "filter_contains_description", Group: report.GroupFilter, Description: "description contains (substring, long text)", Bodies: gen(func(int) map[string]any {
			return q{"query": cond("description", "contains", datasets.Word(200+rank(r, 3000))), "size": 10}
		})},
		{Name: "filter_words", Group: report.GroupFilter, Description: "title words_all / description words_any (phrases)", Bodies: gen(func(i int) map[string]any {
			if i%2 == 0 {
				return q{"query": cond("title", "words_all", []string{datasets.TitleAdjective(r.IntN(20)) + " " + datasets.TitleNoun(rank(r, 60))}), "size": 10}
			}
			return q{"query": cond("description", "words_any", []string{datasets.Word(200 + rank(r, 3000)), datasets.Word(200 + rank(r, 3000))}), "size": 10}
		})},
		{Name: "filter_prefix", Group: report.GroupFilter, Description: "sku starts_with", Bodies: gen(func(int) map[string]any {
			return q{"query": cond("sku", "starts_with", datasets.SKUPrefix(datasets.Brand(rank(r, 300)))+"-"+fmt.Sprintf("%X", r.IntN(16))), "size": 10}
		})},
		{Name: "sorted_price_top100", Group: report.GroupSorted, Description: "category eq, sort price desc, size 100", Bodies: gen(func(int) map[string]any {
			return q{
				"query": allOf(cond("category", "eq", datasets.Category(rank(r, 100))), cond("in_stock", "eq", true)),
				"sort":  []any{q{"price": "desc"}}, "size": 100,
			}
		})},
		{Name: "sorted_recent_top10", Group: report.GroupSorted, Description: "in_stock (most documents), sort updated_at desc, size 10", Bodies: gen(func(i int) map[string]any {
			query := cond("in_stock", "eq", true)
			if i%2 == 1 {
				query = cond("source", "eq", datasets.Sources[r.IntN(len(datasets.Sources))])
			}
			return q{"query": query, "sort": []any{q{"updated_at": "desc"}}, "size": 10}
		})},
		{
			Name: fmt.Sprintf("paging_search_after_%d", pageDepth), Group: report.GroupSorted, PageDepth: pageDepth,
			Description: fmt.Sprintf("in_stock, sort price asc, pages of 100 via search_after to depth %d (latency per page)", pageDepth),
			Bodies: gen(func(i int) map[string]any {
				query := cond("in_stock", "eq", true)
				if i%2 == 1 {
					query = cond("source", "eq", datasets.Sources[r.IntN(6)])
				}
				return q{"query": query, "sort": []any{"price"}, "size": 100}
			}),
		},
		{Name: "agg_terms", Group: report.GroupAggs, Description: "terms brand size 10", Bodies: gen(func(i int) map[string]any {
			return q{"query": aggFilter(i), "size": 0, "aggs": q{"brands": q{"terms": q{"field": "brand", "size": 10}}}}
		})},
		{Name: "agg_terms_stats", Group: report.GroupAggs, Description: "terms category size 10 with a stats(price) sub-aggregation", Bodies: gen(func(i int) map[string]any {
			return q{"query": aggFilter(i), "size": 0, "aggs": q{"cats": q{"terms": q{"field": "category", "size": 10}, "aggs": q{"price": q{"stats": q{"field": "price"}}}}}}
		})},
		{Name: "agg_range", Group: report.GroupAggs, Description: "range price, 6 buckets", Bodies: gen(func(i int) map[string]any {
			return q{"query": aggFilter(i), "size": 0, "aggs": q{"prices": q{"range": q{"field": "price", "ranges": []any{
				q{"key": "under-10", "to": 10},
				q{"key": "10-50", "from": 10, "to": 50},
				q{"key": "50-100", "from": 50, "to": 100},
				q{"key": "100-500", "from": 100, "to": 500},
				q{"key": "500-1000", "from": 500, "to": 1000},
				q{"key": "1000-up", "from": 1000},
			}}}}}
		})},
		{Name: "agg_histogram", Group: report.GroupAggs, Description: "histogram rating interval 0.5", Bodies: gen(func(i int) map[string]any {
			return q{"query": aggFilter(i), "size": 0, "aggs": q{"ratings": q{"histogram": q{"field": "rating", "interval": 0.5}}}}
		})},
		{Name: "agg_date_histogram", Group: report.GroupAggs, Description: "date_histogram first_seen calendar month", Bodies: gen(func(i int) map[string]any {
			return q{"query": aggFilter(i), "size": 0, "aggs": q{"months": q{"date_histogram": q{"field": "first_seen", "calendar_interval": "month"}}}}
		})},
		{Name: "agg_stats", Group: report.GroupAggs, Description: "stats price", Bodies: gen(func(i int) map[string]any {
			return q{"query": aggFilter(i), "size": 0, "aggs": q{"price": q{"stats": q{"field": "price"}}}}
		})},
		{Name: "agg_cardinality", Group: report.GroupAggs, Description: "cardinality brand", Bodies: gen(func(i int) map[string]any {
			return q{"query": aggFilter(i), "size": 0, "aggs": q{"brands": q{"cardinality": q{"field": "brand"}}}}
		})},
	}
	return specs
}

// CoverageChecks are extra cross-check searches that exercise every operator on the
// generated data, beyond what the timed workloads send. doc is a loaded document's
// body (its exact title and dates make exact-match checks hit).
func CoverageChecks(doc map[string]any) []SearchSpec {
	title, _ := doc["title"].(string)
	brand, _ := doc["brand"].(string)
	first, _ := doc["first_seen"].(string)
	at, _ := time.Parse(time.RFC3339, first)
	ms := at.UnixMilli()
	var tag string
	if tags, ok := doc["tags"].([]any); ok && len(tags) > 0 {
		tag, _ = tags[0].(string)
	}
	checks := map[string]map[string]any{
		"eq_title_exact":   {"query": cond("title", "eq", "  "+title+" "), "size": 100},
		"ne_brand":         {"query": allOf(cond("brand", "ne", brand), cond("category", "eq", datasets.Category(3))), "size": 100, "track_total": true},
		"in_stock_values":  {"query": cond("stock", "in", []int{0, 1, 2, 3}), "size": 100, "track_total": true},
		"id_eq_in":         {"query": q{"any": []any{cond("_id", "eq", "P000000003"), cond("_id", "in", []string{"p000000005", "p000000008", "nope"})}}, "size": 100},
		"exists_false":     {"query": allOf(cond("list_price", "exists", false), cond("brand", "eq", datasets.Brand(1))), "size": 100, "track_total": true},
		"tags_empty":       {"query": allOf(cond("tags", "empty", nil), cond("source", "eq", datasets.Sources[2])), "size": 100, "track_total": true},
		"tags_nonempty":    {"query": allOf(cond("tags", "nonempty", nil), cond("brand", "eq", datasets.Brand(2))), "size": 100, "track_total": true},
		"has_all":          {"query": cond("tags", "has_all", []string{datasets.Tag(0), datasets.Tag(1)}), "size": 100, "track_total": true},
		"has_entry":        {"query": cond("tags", "has", tag), "size": 100, "track_total": true},
		"rating_bounds":    {"query": allOf(cond("rating", "gt", 2.5), cond("rating", "lte", 3), cond("reviews", "lt", 50), cond("reviews", "gte", 3)), "size": 100, "track_total": true},
		"date_between":     {"query": cond("first_seen", "between", []int64{ms - 3_600_000, ms + 3_600_000}), "size": 100, "track_total": true},
		"date_eq":          {"query": cond("first_seen", "eq", ms), "size": 100},
		"contains_short":   {"query": allOf(cond("title", "contains", "a1"), cond("brand", "eq", datasets.Brand(0))), "size": 100, "track_total": true},
		"contains_all":     {"query": cond("title", "contains_all", []string{datasets.TitleNoun(0), "pro"}), "size": 100, "track_total": true},
		"contains_any":     {"query": cond("title", "contains_any", []string{datasets.TitleNoun(5), datasets.TitleNoun(9)}), "size": 100, "track_total": true},
		"starts_with":      {"query": cond("title", "starts_with", brand+" "), "size": 100, "track_total": true},
		"words_phrase":     {"query": cond("description", "words_all", []string{"free shipping"}), "size": 100, "track_total": true},
		"similar_brand":    {"query": cond("brand", "similar", q{"text": brand + "x", "min": 0.5}), "size": 100, "track_total": true},
		"bool_false":       {"query": allOf(cond("on_sale", "eq", false), cond("in_stock", "ne", true)), "size": 100, "track_total": true},
		"nested_not_any":   {"query": allOf(q{"any": []any{cond("title", "contains", datasets.TitleNoun(1)), cond("title", "words_any", []string{datasets.TitleNoun(2)})}}, notOf(cond("tags", "has", "refurbished")), cond("price", "between", []float64{10, 200})), "size": 100, "track_total": true},
		"sort_keyword":     {"query": cond("source", "eq", datasets.Sources[0]), "sort": []any{"brand", q{"price": "desc"}}, "size": 100},
		"sort_missing":     {"query": cond("brand", "eq", datasets.Brand(0)), "sort": []any{q{"rating": "desc"}}, "size": 100},
		"sort_id_desc":     {"query": cond("category", "eq", datasets.Category(0)), "sort": []any{q{"_id": "desc"}}, "size": 50},
		"agg_terms_tags":   {"size": 0, "aggs": q{"tags": q{"terms": q{"field": "tags", "size": 20}}}},
		"agg_terms_bool":   {"size": 0, "aggs": q{"b": q{"terms": q{"field": "in_stock"}}}},
		"agg_range_nokey":  {"size": 0, "aggs": q{"r": q{"range": q{"field": "price", "ranges": []any{q{"to": 25}, q{"from": 25}}}}}},
		"agg_hist_cardsub": {"size": 0, "aggs": q{"h": q{"histogram": q{"field": "price", "interval": 250}, "aggs": q{"c": q{"cardinality": q{"field": "brand"}}}}}},
		"agg_date_fixed":   {"size": 0, "aggs": q{"d": q{"date_histogram": q{"field": "updated_at", "fixed_interval": "7d", "min_doc_count": 1}}}},
		"agg_stats_empty":  {"query": cond("price", "lt", 0), "size": 0, "aggs": q{"s": q{"stats": q{"field": "price"}}}},
	}
	out := make([]SearchSpec, 0, len(checks))
	for _, name := range sortedKeys(checks) {
		out = append(out, SearchSpec{Name: "coverage_" + name, Bodies: [][]byte{body(checks[name])}})
	}
	return out
}
