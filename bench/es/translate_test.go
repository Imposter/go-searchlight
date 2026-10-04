package es

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/bench/datasets"
	"github.com/Imposter/go-searchlight/internal/query"
)

var update = flag.Bool("update", false, "rewrite the golden files in testdata")

// golden compares v, as indented JSON, with testdata/name.json.
func golden(t *testing.T, name string, v any) {
	t.Helper()
	got, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	path := filepath.Join("testdata", name+".json")
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run go test ./bench/es -update to write it)", err)
	}
	if !bytes.Equal(bytes.ReplaceAll(want, []byte("\r\n"), []byte("\n")), got) {
		t.Errorf("%s differs from its golden file:\n%s", name, got)
	}
}

// testFields is the product mapping, plus a keyword field without a 3-gram subfield
// or doc values.
var testFields = append(append([]datasets.Field(nil), datasets.Products...), datasets.Field{Name: "plain", Type: datasets.Keyword})

// queryCases cover every operator, on each field type it applies to, and the
// constant folding of values nothing can match.
var queryCases = []struct{ name, query string }{
	{"all_root_empty", `{"all": []}`},
	{"eq_keyword", `{"field": "brand", "op": "eq", "value": "  ACME   Tools "}`},
	{"eq_text", `{"field": "title", "op": "eq", "value": "Acme Drill"}`},
	{"eq_number", `{"field": "price", "op": "eq", "value": 19.99}`},
	{"eq_date", `{"field": "first_seen", "op": "eq", "value": 1735689600000}`},
	{"eq_bool", `{"field": "in_stock", "op": "eq", "value": true}`},
	{"eq_wrong_kind", `{"field": "price", "op": "eq", "value": "cheap"}`},
	{"eq_id", `{"field": "_id", "op": "eq", "value": "P000000007"}`},
	{"ne_keyword", `{"field": "condition", "op": "ne", "value": "used"}`},
	{"ne_wrong_kind", `{"field": "in_stock", "op": "ne", "value": 1}`},
	{"in_keyword", `{"field": "brand", "op": "in", "value": ["Acme", "acme", "Globex", 3]}`},
	{"in_number", `{"field": "stock", "op": "in", "value": [0, 1, 2]}`},
	{"in_id", `{"field": "_id", "op": "in", "value": ["p1", "p2"]}`},
	{"in_nothing_matches", `{"field": "price", "op": "in", "value": ["a", "b"]}`},
	{"lt", `{"field": "price", "op": "lt", "value": 100}`},
	{"lte", `{"field": "rating", "op": "lte", "value": 4.5}`},
	{"gt", `{"field": "reviews", "op": "gt", "value": 10}`},
	{"gte_date", `{"field": "updated_at", "op": "gte", "value": 1735689600000}`},
	{"between", `{"field": "price", "op": "between", "value": [10, 50]}`},
	{"between_empty", `{"field": "price", "op": "between", "value": [50, 10]}`},
	{"exists_true", `{"field": "list_price", "op": "exists", "value": true}`},
	{"exists_false", `{"field": "list_price", "op": "exists", "value": false}`},
	{"exists_bare", `{"field": "tags", "op": "exists"}`},
	{"contains_trigram", `{"field": "title", "op": "contains", "value": "Cordless  Drill"}`},
	{"contains_short", `{"field": "title", "op": "contains", "value": "x2"}`},
	{"contains_no_subfield", `{"field": "plain", "op": "contains", "value": "a*b?c"}`},
	{"contains_any", `{"field": "title", "op": "contains_any", "value": ["drill", "saw", "drill"]}`},
	{"contains_all", `{"field": "title", "op": "contains_all", "value": ["acme", "pro"]}`},
	{"starts_with", `{"field": "sku", "op": "starts_with", "value": "ACM-"}`},
	{"words_all", `{"field": "title", "op": "words_all", "value": ["Heavy-Duty drill", "pro"]}`},
	{"words_all_no_word", `{"field": "title", "op": "words_all", "value": ["drill", "--"]}`},
	{"words_any", `{"field": "description", "op": "words_any", "value": ["cordless", "...", "free shipping"]}`},
	{"similar", `{"field": "brand", "op": "similar", "value": {"text": "Akme", "min": 0.3}}`},
	{"has", `{"field": "tags", "op": "has", "value": "Sale"}`},
	{"has_any", `{"field": "tags", "op": "has_any", "value": ["sale", "new"]}`},
	{"has_all", `{"field": "tags", "op": "has_all", "value": ["sale", "new"]}`},
	{"empty", `{"field": "tags", "op": "empty"}`},
	{"nonempty", `{"field": "tags", "op": "nonempty"}`},
	{"not", `{"not": {"field": "brand", "op": "eq", "value": "Acme"}}`},
	{"any", `{"any": [{"field": "brand", "op": "eq", "value": "Acme"}, {"field": "price", "op": "lt", "value": 5}]}`},
	{"nested", `{"all": [{"any": [{"field": "title", "op": "contains", "value": "drill"}, {"field": "title", "op": "words_any", "value": ["saw"]}]}, {"not": {"field": "tags", "op": "has", "value": "refurbished"}}, {"field": "price", "op": "between", "value": [10, 200]}]}`},
	{"fold_all_with_never", `{"all": [{"field": "brand", "op": "eq", "value": "Acme"}, {"field": "price", "op": "eq", "value": "x"}]}`},
	{"fold_any_with_always", `{"any": [{"field": "brand", "op": "eq", "value": "Acme"}, {"field": "price", "op": "ne", "value": "x"}]}`},
}

func TestTranslateQueryGolden(t *testing.T) {
	tr := NewTranslator(testFields)
	got := map[string]any{}
	for _, c := range queryCases {
		q, err := tr.ParseQuery([]byte(c.query))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		got[c.name] = map[string]any{"searchlight": json.RawMessage(c.query), "elasticsearch": q}
	}
	golden(t, "queries", got)
}

// TestEveryOpTranslated checks that the golden cases cover every Searchlight operator.
func TestEveryOpTranslated(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range queryCases {
		n, ps := query.Parse([]byte(c.query))
		if len(ps) > 0 {
			t.Fatalf("%s: %v", c.name, ps)
		}
		query.Walk(n, func(_ string, node query.Node) bool {
			if l, ok := node.(*query.Leaf); ok {
				seen[l.Op] = true
			}
			return true
		})
	}
	for _, op := range query.Ops {
		if !seen[op] {
			t.Errorf("no golden case translates %s", op)
		}
	}
}

func TestTranslateRefusals(t *testing.T) {
	tr := NewTranslator(testFields)
	for _, q := range []string{
		`{"field": "nope", "op": "eq", "value": 1}`,
		`{"field": "price", "op": "contains", "value": "1"}`,
		`{"field": "_id", "op": "starts_with", "value": "p"}`,
		`{"field": "title", "op": "similar", "value": {"text": "drill", "min": 0.5}}`, // no doc values
		`{"bogus": 1}`,
	} {
		if _, err := tr.ParseQuery([]byte(q)); err == nil {
			t.Errorf("%s was translated; want an error", q)
		}
	}
}

var searchCases = []struct{ name, body string }{
	{"default", `{}`},
	{"sorted_paged", `{"query": {"field": "brand", "op": "eq", "value": "Acme"}, "sort": [{"price": "desc"}, "updated_at"], "size": 100, "search_after": [19.99, 1735689600000, "p000000001"], "track_total": true}`},
	{"sort_by_id", `{"sort": [{"_id": "desc"}, "price"], "size": 5, "track_total": false}`},
	{"fields_timeout", `{"fields": ["title", "price"], "timeout": "250ms", "track_total": 500, "size": 0}`},
	{"agg_terms", `{"size": 0, "aggs": {"brands": {"terms": {"field": "brand", "size": 20, "min_doc_count": 2}, "aggs": {"price": {"stats": {"field": "price"}}}}}}`},
	{"agg_range", `{"size": 0, "aggs": {"prices": {"range": {"field": "price", "ranges": [{"to": 10}, {"from": 10, "to": 100, "key": "mid"}, {"from": 100}]}}}}`},
	{"agg_histogram", `{"size": 0, "aggs": {"h": {"histogram": {"field": "rating", "interval": 0.5, "offset": 0.25}}}}`},
	{"agg_date_histogram_calendar", `{"size": 0, "aggs": {"d": {"date_histogram": {"field": "first_seen", "calendar_interval": "month"}}}}`},
	{"agg_date_histogram_fixed", `{"size": 0, "aggs": {"d": {"date_histogram": {"field": "updated_at", "fixed_interval": "12h", "offset": "-1h", "min_doc_count": 1}}}}`},
	{"agg_stats", `{"size": 0, "aggs": {"s": {"stats": {"field": "price"}}}}`},
	{"agg_cardinality", `{"size": 0, "aggs": {"c": {"cardinality": {"field": "brand"}}, "c8": {"cardinality": {"field": "category", "precision": 8}}}}`},
}

func TestTranslateSearchGolden(t *testing.T) {
	tr := NewTranslator(testFields)
	got := map[string]any{}
	for _, c := range searchCases {
		body, err := tr.Search([]byte(c.body))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		got[c.name] = map[string]any{"searchlight": json.RawMessage(c.body), "elasticsearch": body}
	}
	golden(t, "searches", got)
}

func TestTranslateSearchRefusals(t *testing.T) {
	tr := NewTranslator(testFields)
	for _, b := range []string{
		`{"sort": ["title"]}`, // no doc values
		`{"aggs": {"x": {"terms": {"field": "description"}}}}`, // no doc values
		`{"aggs": {"x": {"stats": {"field": "nope"}}}}`,
		`{"size": -1}`,
	} {
		if _, err := tr.Search([]byte(b)); err == nil {
			t.Errorf("%s was translated; want an error", b)
		}
	}
}

func TestMappingsGolden(t *testing.T) {
	golden(t, "mapping", map[string]any{
		"documents":  IndexBody(datasets.Products, 2),
		"percolator": PercolatorIndexBody(datasets.Products, 1),
	})
}

func TestPercolatorDocAndBody(t *testing.T) {
	tr := NewTranslator(datasets.Products)
	s := datasets.Search(1, 0)
	raw, err := json.Marshal(s.Query)
	if err != nil {
		t.Fatal(err)
	}
	meta, err := json.Marshal(s.Meta)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := tr.PercolatorDoc(s.ID, raw, meta)
	if err != nil {
		t.Fatal(err)
	}
	golden(t, "percolator", map[string]any{
		"saved_search": s,
		"document":     doc,
		"percolate":    PercolateBody([]json.RawMessage{json.RawMessage(`{"title": "Acme drill"}`)}, 10_000),
	})
}

// TestSavedSearchesTranslate checks every generated saved search translates.
func TestSavedSearchesTranslate(t *testing.T) {
	tr := NewTranslator(datasets.Products)
	for i := range int64(2000) {
		s := datasets.Search(3, i)
		raw, _ := json.Marshal(s.Query)
		if _, err := tr.ParseQuery(raw); err != nil {
			t.Fatalf("saved search %d: %v", i, err)
		}
	}
}

func TestCaveatsComplete(t *testing.T) {
	areas := map[string]bool{}
	for _, c := range Caveats {
		if c.Area == "" || c.Difference == "" || c.Choice == "" || c.Benchmark == "" {
			t.Errorf("caveat %+v is incomplete", c)
		}
		areas[c.Area] = true
	}
	for _, want := range []string{"contains, contains_any, contains_all", "similar", "words_all, words_any", "text normalization", "cardinality", "percolation"} {
		if !areas[want] {
			t.Errorf("no caveat for %s", want)
		}
	}
	if !strings.Contains(similarScript, "params.min") {
		t.Fatal("similar script lost its threshold")
	}
}
