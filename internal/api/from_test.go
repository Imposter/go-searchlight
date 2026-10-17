package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3filter"

	"github.com/Imposter/go-searchlight/internal/config"
	"github.com/Imposter/go-searchlight/internal/search"
)

// TestFromPaging: from pages a sorted search over shards as Elasticsearch's does, each
// page the slice of the whole result, its next a cursor search_after carries on from;
// the total and the aggregations are the whole search's; and a from past the
// max_result_window, below 0 or with search_after is a 400 at from.
func TestFromPaging(t *testing.T) {
	if config.MaxResultWindowLimit != search.MaxResultWindow {
		t.Fatalf("max_result_window is at most %d, and a shard takes at most %d", config.MaxResultWindowLimit, search.MaxResultWindow)
	}
	e := newEnv(t, envOpts{cfg: func(c *config.Config) { c.MaxResultWindow = 60 }})
	e.createIndex("fp", `{"mapping": {"dynamic": "strict", "fields": {"brand": "keyword", "price": "number"}}, "settings": {"shards": 3}}`)
	lines := make([]string, 0, 140)
	for i := range 70 {
		lines = append(lines, fmt.Sprintf(`{"upsert": {"id": "d%02d"}}`, i), fmt.Sprintf(`{"brand": "b%d", "price": %d}`, i%3, i%11))
	}
	e.must(http.StatusOK, "POST", "/indexes/fp/_bulk?refresh=wait_for", ndjson(lines...))
	const sort = `"sort": [{"price": "desc"}], "post_filter": {"field": "brand", "op": "ne", "value": "b2"}`
	full := ids(e.must(http.StatusOK, "POST", "/indexes/fp/_search", `{`+sort+`, "size": 60}`))
	if len(full) != 47 {
		t.Fatalf("the whole result holds %d hits, want 47", len(full))
	}
	var walked []string
	for from := 0; from < len(full)+5; from += 9 {
		r := e.must(http.StatusOK, "POST", "/indexes/fp/_search", fmt.Sprintf(`{%s, "size": 9, "from": %d, "track_total": true,
			"aggs": {"brands": {"terms": {"field": "brand"}}}}`, sort, from))
		page := ids(r)
		if want := full[min(from, len(full)):min(from+9, len(full))]; !slices.Equal(page, want) {
			t.Fatalf("from %d: %v, want %v", from, page, want)
		}
		if totalOf(r) != len(full) {
			t.Fatalf("from %d: total %d, want %d", from, totalOf(r), len(full))
		}
		buckets := r["aggs"].(map[string]any)["brands"].(map[string]any)["buckets"].([]any) //nolint:forcetypeassert,errcheck // the shape
		if len(buckets) != 3 {
			t.Fatalf("from %d: brands %v, want all three, the post_filter aside", from, buckets)
		}
		walked = append(walked, page...)
		if r["next"] == nil {
			continue
		}
		next, err := json.Marshal(r["next"])
		if err != nil {
			t.Fatal(err)
		}
		after := ids(e.must(http.StatusOK, "POST", "/indexes/fp/_search", fmt.Sprintf(`{%s, "size": 9, "search_after": %s}`, sort, next)))
		if want := full[min(from+9, len(full)):min(from+18, len(full))]; !slices.Equal(after, want) {
			t.Fatalf("search_after from %d's next: %v, want %v", from, after, want)
		}
	}
	if !slices.Equal(walked, full) {
		t.Fatalf("the pages by from walk %v, want %v", walked, full)
	}
	e.must(http.StatusOK, "POST", "/indexes/fp/_search", `{"from": 50, "size": 10}`)
	e.must(http.StatusOK, "POST", "/indexes/fp/_search", `{"from": 0, "search_after": ["d01"]}`)
	for _, tc := range []struct{ body, loc, says string }{
		{`{"from": 51, "size": 10}`, "from", "search_after"},
		{`{"size": 61}`, "size", "max_result_window"},
		{`{"from": -1}`, "from", "whole number"},
		{`{"from": 2, "search_after": ["d01"]}`, "from", "search_after"},
	} {
		p := e.problem(e.do("POST", "/indexes/fp/_search", tc.body), http.StatusBadRequest, "invalid_request")
		if !slices.Contains(locs(p), tc.loc) || !strings.Contains(fmt.Sprint(p), tc.says) {
			t.Errorf("%s: %v, want a problem at %s naming %s", tc.body, p, tc.loc, tc.says)
		}
	}
}

// TestSearchRequestSchemaTakesFrom: api/openapi.yaml's SearchRequest takes a from of 0
// or more, alone or (as 0) with search_after, and refuses a negative one.
func TestSearchRequestSchemaTakesFrom(t *testing.T) {
	router, err := specRouter()
	if err != nil {
		t.Fatal(err)
	}
	check := func(body string) error {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "/indexes/fp/_search", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		route, params, err := router.FindRoute(req)
		if err != nil {
			t.Fatal(err)
		}
		return openapi3filter.ValidateRequest(context.Background(), &openapi3filter.RequestValidationInput{
			Request: req, PathParams: params, Route: route,
			Options: &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc},
		})
	}
	for _, body := range []string{`{"from": 50, "size": 10}`, `{"from": 0, "search_after": ["d01"]}`, `{"from": 9000}`} {
		if err := check(body); err != nil {
			t.Errorf("%s: %v", body, err)
		}
	}
	if err := check(`{"from": -1}`); err == nil {
		t.Errorf("a negative from passes the schema")
	}
}
