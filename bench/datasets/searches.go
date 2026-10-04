package datasets

import (
	"encoding/json"
	"math"
	"math/rand/v2"
	"strconv"
)

// SavedSearch is one saved query, shaped like a scrape-bot saved search: a few
// conditions on keywords, prices and the title, sometimes with a not.
type SavedSearch struct {
	ID    string         `json:"id"`
	Query map[string]any `json:"query"`
	Meta  map[string]any `json:"meta"`
}

// SearchID returns saved search i's id: "q" and nine digits.
func SearchID(i int64) string { return string(appendID(nil, 'q', i)) }

func leaf(field, op string, value any) map[string]any {
	return map[string]any{"field": field, "op": op, "value": value}
}

func all(children ...any) map[string]any { return map[string]any{"all": children} }
func anyOf(children ...any) map[string]any {
	return map[string]any{"any": children}
}
func not(child any) map[string]any { return map[string]any{"not": child} }

// roundPrice is a price a person would type: a whole number of dollars, rounded to a
// "nice" step.
func roundPrice(p float64) float64 {
	switch {
	case p < 20:
		return math.Max(5, math.Round(p))
	case p < 200:
		return math.Round(p/5) * 5
	case p < 2000:
		return math.Round(p/50) * 50
	default:
		return math.Round(p/500) * 500
	}
}

func pricePoint(r *rand.Rand) float64 {
	return roundPrice(math.Exp(3.6 + 1.2*r.NormFloat64()))
}

// titleNeedle is a contains needle a person types for a title: a noun, an adjective
// and noun, or a brand (always three or more characters).
func titleNeedle(r *rand.Rand) string {
	switch r.IntN(4) {
	case 0:
		return TitleAdjective(zAd.Sample(r)) + " " + TitleNoun(zNouns.Sample(r))
	case 1:
		return Brand(zBrands.Sample(r))
	default:
		return TitleNoun(zNouns.Sample(r))
	}
}

// Search returns saved search i under seed.
func Search(seed uint64, i int64) SavedSearch {
	samplers()
	r := rng(seed, streamSearches, i)
	var q map[string]any
	switch k := r.IntN(100); {
	case k < 25:
		q = all(leaf("brand", "eq", Brand(zBrands.Sample(r))), leaf("price", "lte", pricePoint(r)))
	case k < 40:
		q = all(leaf("category", "eq", Category(zCats.Sample(r))),
			leaf("tags", "has_any", []string{Tag(zTags.Sample(r)), Tag(zTags.Sample(r))}))
	case k < 55:
		lo := pricePoint(r)
		q = all(leaf("title", "contains", titleNeedle(r)), leaf("price", "between", []float64{lo, roundPrice(lo * (1.5 + r.Float64()*3))}))
	case k < 65:
		q = all(leaf("title", "words_all", []string{TitleAdjective(zAd.Sample(r)) + " " + TitleNoun(zNouns.Sample(r))}),
			leaf("in_stock", "eq", true))
	case k < 75:
		q = all(leaf("brand", "in", []string{Brand(zBrands.Sample(r)), Brand(zBrands.Sample(r)), Brand(zBrands.Sample(r))}),
			not(leaf("condition", "eq", "used")))
	case k < 85:
		q = all(leaf("source", "eq", Sources[zSource.Sample(r)]), leaf("rating", "gte", float64(3+r.IntN(2))),
			not(leaf("tags", "has", "refurbished")))
	case k < 90:
		q = all(anyOf(leaf("title", "contains", titleNeedle(r)), leaf("title", "contains", titleNeedle(r))),
			leaf("on_sale", "eq", true))
	case k < 95:
		q = all(leaf("title", "contains_any", []string{TitleNoun(zNouns.Sample(r)), TitleNoun(zNouns.Sample(r))}),
			leaf("category", "eq", Category(zCats.Sample(r))), leaf("price", "lt", pricePoint(r)))
	default:
		q = all(leaf("brand", "eq", Brand(zBrands.Sample(r))),
			leaf("title", "words_any", []string{TitleNoun(zNouns.Sample(r)), TitleAdjective(zAd.Sample(r))}))
	}
	return SavedSearch{
		ID:    SearchID(i),
		Query: q,
		Meta:  map[string]any{"search_id": i, "name": "saved search " + strconv.FormatInt(i, 10)},
	}
}

// AppendSearchLine appends saved search i as one NDJSON line.
func AppendSearchLine(dst []byte, seed uint64, i int64) ([]byte, error) {
	b, err := json.Marshal(Search(seed, i))
	if err != nil {
		return dst, err
	}
	dst = append(dst, b...)
	return append(dst, '\n'), nil
}
