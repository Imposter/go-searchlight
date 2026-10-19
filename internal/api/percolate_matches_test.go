package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const variantMapping = `{"mapping": {"fields": {"brand": "keyword", "_v": {"type": "nested", "fields": {"size": "keyword", "price": "number", "stock": "bool"}}}}, "settings": {"shards": 3}}`

func randomVariantDoc(rng *rand.Rand) string {
	n := rng.IntN(5)
	vs := make([]string, n)
	for i := range vs {
		vs[i] = fmt.Sprintf(`{"size": %q, "price": %d, "stock": %v}`, []string{"S", "M", "L"}[rng.IntN(3)], rng.IntN(20), rng.IntN(2) == 0)
	}
	return fmt.Sprintf(`{"brand": %q, "_v": [%s]}`, []string{"acme", "zeta"}[rng.IntN(2)], strings.Join(vs, ","))
}

func randomInnerCondition(rng *rand.Rand) string {
	switch rng.IntN(4) {
	case 0:
		return fmt.Sprintf(`{"field": "_v.size", "op": "eq", "value": %q}`, []string{"S", "M", "L"}[rng.IntN(3)])
	case 1:
		return fmt.Sprintf(`{"field": "_v.price", "op": "lt", "value": %d}`, rng.IntN(20))
	case 2:
		return fmt.Sprintf(`{"field": "_v.stock", "op": "eq", "value": %v}`, rng.IntN(2) == 0)
	}
	return fmt.Sprintf(`{"not": {"field": "_v.price", "op": "gte", "value": %d}}`, rng.IntN(20))
}

// randomVariantQuery is a saved query of one to three nested nodes, each asking for
// inner hits under one of two names (so some merge), beside a brand condition now and
// then, under all or any.
func randomVariantQuery(rng *rand.Rand) string {
	var parts []string
	for range 1 + rng.IntN(3) {
		inner := `{"all": [` + randomInnerCondition(rng) + `, ` + randomInnerCondition(rng) + `]}`
		hits := fmt.Sprintf(`{"name": %q, "size": %d}`, []string{"a", "b"}[rng.IntN(2)], rng.IntN(4))
		parts = append(parts, `{"nested": {"path": "_v", "query": `+inner+`, "inner_hits": `+hits+`}}`)
	}
	if rng.IntN(3) == 0 {
		parts = append(parts, fmt.Sprintf(`{"field": "brand", "op": "eq", "value": %q}`, []string{"acme", "zeta"}[rng.IntN(2)]))
	}
	return `{"` + []string{"all", "any"}[rng.IntN(2)] + `": [` + strings.Join(parts, ", ") + `]}`
}

// TestPercolateMatchesEqualSearch: with percolate_inner_hits, each upserted document's
// matches name exactly its matching saved queries, each with its version and the inner
// hits a search of that saved query reports for the document (offsets and totals, by
// key); _percolate of the stored ids reports the same, with each document's _seq; a
// search with seq_no reports the same seqs.
func TestPercolateMatchesEqualSearch(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 15))
	e := newEnv(t, envOpts{})
	e.createIndex("pm", variantMapping)
	queries := map[string]string{}
	var lines []string
	for i := range 30 {
		id := fmt.Sprintf("q%02d", i)
		queries[id] = randomVariantQuery(rng)
		lines = append(lines, fmt.Sprintf(`{"query_upsert": {"id": %q, "version": %d}}`, id, 10+i), `{"query": `+queries[id]+`}`)
	}
	for i := range 25 {
		lines = append(lines, fmt.Sprintf(`{"upsert": {"id": "d%02d"}}`, i), randomVariantDoc(rng))
	}
	r := e.must(http.StatusOK, "POST", "/indexes/pm/_bulk?refresh=wait_for&percolate=true&percolate_inner_hits=true", ndjson(lines...))
	if r["percolated"] != true {
		t.Fatalf("not percolated: %v", r)
	}
	type match struct {
		ID        string `json:"id"`
		Version   int64  `json:"version"`
		InnerHits map[string]struct {
			Total   int   `json:"total"`
			Offsets []int `json:"offsets"`
		} `json:"inner_hits"`
	}
	type item struct {
		ID      string   `json:"id"`
		Seq     int64    `json:"seq"`
		Queries []string `json:"queries"`
		Matches []match  `json:"matches"`
	}
	raw, _ := json.Marshal(r["items"])
	var items []item
	if err := json.Unmarshal(raw, &items); err != nil {
		t.Fatal(err)
	}
	seqs := map[string]int64{}
	checked := 0
	for _, it := range items[30:] {
		seqs[it.ID] = it.Seq
		var matchIDs []string
		for _, m := range it.Matches {
			matchIDs = append(matchIDs, m.ID)
		}
		if !slices.Equal(matchIDs, it.Queries) && (len(matchIDs) > 0 || len(it.Queries) > 0) {
			t.Fatalf("%s: matches %v, queries %v", it.ID, matchIDs, it.Queries)
		}
		for id, q := range queries {
			s := e.must(http.StatusOK, "POST", "/indexes/pm/_search", fmt.Sprintf(`{"query": {"all": [%s, {"field": "_id", "op": "eq", "value": %q}]}, "size": 1, "fields": ["brand"]}`, q, it.ID))
			hits := s["hits"].([]any) //nolint:forcetypeassert,errcheck // the shape
			k := slices.IndexFunc(it.Matches, func(m match) bool { return m.ID == id })
			if (len(hits) == 1) != (k >= 0) {
				t.Fatalf("%s %s: a search hits it %v, the percolation matches it %v", it.ID, id, len(hits) == 1, k >= 0)
			}
			if k < 0 {
				continue
			}
			checked++
			m := it.Matches[k]
			var want struct {
				InnerHits map[string]struct {
					Total   int   `json:"total"`
					Offsets []int `json:"offsets"`
				} `json:"inner_hits"`
			}
			hraw, _ := json.Marshal(hits[0])
			if err := json.Unmarshal(hraw, &want); err != nil {
				t.Fatal(err)
			}
			n, err := strconv.Atoi(strings.TrimPrefix(id, "q"))
			if err != nil {
				t.Fatal(err)
			}
			wantVersion := int64(n)
			if m.Version != wantVersion+10 || len(m.InnerHits) != len(want.InnerHits) {
				t.Fatalf("%s %s: version %d, inner hits %v; want %d, %v", it.ID, id, m.Version, m.InnerHits, wantVersion+10, want.InnerHits)
			}
			for key, w := range want.InnerHits {
				g := m.InnerHits[key]
				if g.Total != w.Total || !slices.Equal(g.Offsets, w.Offsets) {
					t.Fatalf("%s %s: inner hits %s %+v, the search's %+v", it.ID, id, key, g, w)
				}
			}
		}
	}
	if checked < 20 {
		t.Fatalf("%d matches checked: the random queries match too little", checked)
	}
	var ids []string
	for i := range 25 {
		ids = append(ids, fmt.Sprintf("%q", fmt.Sprintf("d%02d", i)))
	}
	p := e.must(http.StatusOK, "POST", "/indexes/pm/_percolate?percolate_inner_hits=true", `{"ids": [`+strings.Join(ids, ",")+`, "missing"]}`)
	raw, _ = json.Marshal(p["results"])
	var results []item
	if err := json.Unmarshal(raw, &results); err != nil {
		t.Fatal(err)
	}
	for i, res := range results[:25] {
		want := items[30+i]
		a, _ := json.Marshal(res.Matches)
		b, _ := json.Marshal(want.Matches)
		if !bytes.Equal(a, b) {
			t.Fatalf("_percolate of %s: %s, the bulk's %s", want.ID, a, b)
		}
	}
	if missing := p["results"].([]any)[25].(map[string]any); fmt.Sprint(missing["matches"]) != "[]" || missing["_seq"] != nil { //nolint:forcetypeassert,errcheck // the shape
		t.Fatalf("a missing id: %v", missing)
	}
	raw, _ = json.Marshal(p["results"])
	var stored []struct {
		ID  string `json:"id"`
		Seq int64  `json:"_seq"`
	}
	_ = json.Unmarshal(raw, &stored)
	s := e.must(http.StatusOK, "POST", "/indexes/pm/_search", `{"size": 100, "seq_no": true, "fields": ["brand"]}`)
	for _, h := range s["hits"].([]any) { //nolint:forcetypeassert,errcheck // the shape
		h := h.(map[string]any)                     //nolint:forcetypeassert,errcheck // the shape
		id := h["id"].(string)                      //nolint:forcetypeassert,errcheck // the shape
		if int64(h["_seq"].(float64)) != seqs[id] { //nolint:forcetypeassert,errcheck // the shape
			t.Fatalf("search hit %s: _seq %v, written at %d", id, h["_seq"], seqs[id])
		}
	}
	for _, st := range stored[:25] {
		if st.Seq != seqs[st.ID] {
			t.Fatalf("_percolate of %s: _seq %d, written at %d", st.ID, st.Seq, seqs[st.ID])
		}
	}
	if p := e.must(http.StatusOK, "POST", "/indexes/pm/_percolate?percolate_matches=true", `{"docs": [{"brand": "zzz"}]}`); fmt.Sprint(p["results"]) != "[map[found:true matches:[] queries:[]]]" {
		t.Fatalf("a document matching nothing: %v", p)
	}
	e.problem(e.do("POST", "/indexes/pm/_bulk?percolate_matches=true", ndjson(`{"delete": {"id": "x"}}`)), 400, "invalid_request")
}
