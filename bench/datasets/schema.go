// Package datasets generates the benchmark's data: scrape-bot-shaped product listings
// (spec section 16: keywords, lists, numbers, dates, long text) and saved searches
// shaped like scrape-bot's (keyword eq/in, number ranges, contains/words on the title,
// some nots), streamed to NDJSON.
//
// Every value is a pure function of (seed, ordinal): document i is the same whoever
// generates it, in whatever order, so a 10M-document file is written by parallel
// workers with constant memory, and a workload can regenerate any document (a
// percolation sample, a write's new body) without reading the file.
package datasets

import "slices"

// FieldType is a Searchlight field type (spec section 3).
type FieldType string

// The field types.
const (
	Keyword     FieldType = "keyword"
	Text        FieldType = "text"
	KeywordList FieldType = "keyword_list"
	Number      FieldType = "number"
	Bool        FieldType = "bool"
	Date        FieldType = "date"
)

// Field is one mapped field, with what the Elasticsearch mapping needs to know to give
// it the cheapest structures that still answer every query the benchmark sends.
type Field struct {
	Name string
	Type FieldType
	// Substring marks a keyword or text field queried with contains: Elasticsearch
	// gets a 3-gram subfield for it (Searchlight indexes 3-grams of every keyword and
	// text field; Elasticsearch gets them only where the workloads need them).
	Substring bool
	// DocValues marks a keyword or text field that is sorted, aggregated or read by
	// a script (similar): Elasticsearch keeps doc values only for those.
	DocValues bool
}

// Products is the product listing's mapping: 18 fields.
var Products = []Field{
	{Name: "title", Type: Text, Substring: true},
	{Name: "description", Type: Text},
	{Name: "brand", Type: Keyword, DocValues: true},
	{Name: "category", Type: Keyword, DocValues: true},
	{Name: "tags", Type: KeywordList, DocValues: true},
	{Name: "source", Type: Keyword, DocValues: true},
	{Name: "condition", Type: Keyword, DocValues: true},
	{Name: "sku", Type: Keyword},
	{Name: "url", Type: Keyword},
	{Name: "price", Type: Number},
	{Name: "list_price", Type: Number},
	{Name: "rating", Type: Number},
	{Name: "reviews", Type: Number},
	{Name: "stock", Type: Number},
	{Name: "in_stock", Type: Bool},
	{Name: "on_sale", Type: Bool},
	{Name: "first_seen", Type: Date},
	{Name: "updated_at", Type: Date},
}

// FieldByName returns the field named name in fields.
func FieldByName(fields []Field, name string) (Field, bool) {
	i := slices.IndexFunc(fields, func(f Field) bool { return f.Name == name })
	if i < 0 {
		return Field{}, false
	}
	return fields[i], true
}

// Types returns each field's type by name.
func Types(fields []Field) map[string]FieldType {
	out := make(map[string]FieldType, len(fields))
	for _, f := range fields {
		out[f.Name] = f.Type
	}
	return out
}

// SearchlightMapping returns the body of PUT /indexes/{i}: the mapping, strict, and the
// settings.
func SearchlightMapping(fields []Field, shards int, refresh string) map[string]any {
	m := make(map[string]string, len(fields))
	for _, f := range fields {
		m[f.Name] = string(f.Type)
	}
	settings := map[string]any{"shards": max(shards, 1)}
	if refresh != "" {
		settings["refresh_interval"] = refresh
	}
	return map[string]any{
		"mapping":  map[string]any{"dynamic": "strict", "fields": m},
		"settings": settings,
	}
}
