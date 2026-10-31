package api_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3filter"
)

// validateSearchRequest checks body against api/openapi.yaml's SearchRequest.
func validateSearchRequest(t *testing.T, body string) error {
	t.Helper()
	router, err := specRouter()
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "/indexes/as/_search", strings.NewReader(body))
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

// TestAggregationSchemas: api/openapi.yaml's Aggregation takes a search of every
// aggregation type and setting, which the server answers with a response its
// AggregationResult describes (the env checks every response); and it refuses what the
// server refuses as a 400.
func TestAggregationSchemas(t *testing.T) {
	e := newEnv(t, envOpts{})
	e.createIndex("as", `{"mapping": {"dynamic": "strict", "fields": {"brand": "keyword", "tags": "keyword_list", "price": "number", "created": "date", "active": "bool",
		"_v": {"type": "nested", "fields": {"size": "keyword", "price": "number"}}}}, "settings": {"shards": 2}}`)
	lines := make([]string, 0, 120)
	for i := range 60 {
		lines = append(lines, fmt.Sprintf(`{"upsert": {"id": "d%02d"}}`, i),
			fmt.Sprintf(`{"brand": "b%d", "tags": ["t%d", "x"], "price": %d, "created": %d, "active": %t, "_v": [{"size": "s%d", "price": %d}]}`,
				i%4, i%3, i, 1_761_000_000_000+int64(i)*3_600_000, i%2 == 0, i%3, i%7))
	}
	e.must(http.StatusOK, "POST", "/indexes/as/_bulk?refresh=wait_for", ndjson(lines...))
	metrics := `"st": {"stats": {"field": "price"}}, "lo": {"min": {"field": "price"}}, "hi": {"max": {"field": "created"}},
		"n": {"cardinality": {"field": "tags", "precision": 12}}, "n2": {"cardinality": {"field": "brand", "threshold": 3}},
		"p": {"percentiles": {"field": "price", "percents": [50, 99.9], "tdigest": {"compression": 50}}},
		"pl": {"percentiles": {"field": "price", "keyed": false}},
		"r": {"percentile_ranks": {"field": "price", "values": [10, 30], "keyed": true}},
		"tm": {"top_metrics": {"metrics": [{"field": "price"}, {"field": "brand"}, {"field": "_id"}, {"field": "active"}], "sort": {"created": "desc"}, "size": 2}}`
	for _, aggs := range []string{
		`{"t": {"terms": {"field": "brand", "size": 3, "shard_size": 5, "min_doc_count": 1, "include": ["b1", "b2", "b3"], "exclude": "b3", "collect_mode": "depth_first",
			"order": [{"p.50": "desc"}, {"_count": "asc"}]}, "aggs": {` + metrics + `}}}`,
		`{"t": {"terms": {"field": "brand", "order": {"p[99.9]": "asc"}}, "aggs": {` + metrics + `}}}`,
		`{"t": {"terms": {"field": "tags", "order": {"r.30": "desc"}}, "aggs": {` + metrics + `}}}`,
		`{"t": {"terms": {"field": "price", "order": {"_key": "desc"}, "size": 2}, "aggs": {"tm": {"top_metrics": {"metrics": {"field": "price"}, "sort": "created"}}}}}`,
		`{"t": {"terms": {"field": "brand", "order": {"tm1.price": "desc"}}, "aggs": {"tm1": {"top_metrics": {"metrics": {"field": "price"}, "sort": [{"created": {"order": "desc"}}]}}}}}`,
		`{"t": {"terms": {"field": "tags", "collect_mode": "breadth_first", "order": {"_key": "asc"}}, "aggs": {` + metrics + `}}}`,
		`{"r": {"range": {"field": "price", "ranges": [{"to": 10}, {"from": 10, "to": 30, "key": "mid"}, {"from": 30, "to": null}]}, "aggs": {` + metrics + `}}}`,
		`{"h": {"histogram": {"field": "price", "interval": 25, "offset": 5, "min_doc_count": 0}, "aggs": {` + metrics + `}}}`,
		`{"d": {"date_histogram": {"field": "created", "calendar_interval": "day", "offset": "-6h"}, "aggs": {` + metrics + `}}}`,
		`{"d": {"date_histogram": {"field": "created", "fixed_interval": "12h", "min_doc_count": 1}}, "d2": {"date_histogram": {"field": "created", "interval": 86400000, "offset": 3600000}}}`,
		`{"c": {"composite": {"size": 3, "sources": [{"b": {"terms": {"field": "brand", "order": "desc"}}}, {"p": {"histogram": {"field": "price", "interval": 20, "offset": 1}}},
			{"d": {"date_histogram": {"field": "created", "calendar_interval": "day", "order": "asc"}}}, {"i": {"terms": {"field": "_id"}}}, {"a": {"terms": {"field": "active"}}}]},
			"aggs": {` + metrics + `}}}`,
		`{"c": {"composite": {"sources": [{"b": {"terms": {"field": "brand"}}}], "after": {"b": "b1"}}}}`,
		`{"c": {"composite": {"sources": [{"b": {"terms": {"field": "brand"}}}], "after": {"b": "zzz"}}}}`,
		`{"c": {"composite": {"sources": [{"d": {"date_histogram": {"field": "created", "fixed_interval": 86400000, "offset": "1h"}}}]}}}`,
		`{` + metrics + `}`,
		`{"f": {"filter": {"field": "price", "op": "lt", "value": 30}, "aggs": {"t": {"terms": {"field": "brand"}, "aggs": {` + metrics + `}}, ` + metrics + `}}}`,
		`{"fs": {"filters": {"filters": {"cheap": {"field": "price", "op": "lt", "value": 10}, "dear": {"field": "price", "op": "gte", "value": 50}}, "other_bucket_key": "rest"},
			"aggs": {"tm": {"top_metrics": {"metrics": {"field": "price"}, "sort": "price"}}, "h": {"histogram": {"field": "price", "interval": 10}}}}}`,
		`{"g": {"global": {}, "aggs": {"c": {"cardinality": {"field": "brand"}}, "r": {"range": {"field": "price", "ranges": [{"from": 0}]}}}}}`,
		`{"v": {"terms": {"field": "_v.size", "where": {"field": "_v.price", "op": "gt", "value": 1}, "order": {"vp.50": "asc"}},
			"aggs": {"vp": {"percentiles": {"field": "_v.price"}}, "vs": {"stats": {"field": "_v.price", "where": {"field": "_v.price", "op": "lt", "value": 5}}}, "vt": {"terms": {"field": "_v.size"}}}},
			"vm": {"min": {"field": "_v.price", "where": {"field": "_v.size", "op": "eq", "value": "s1"}}}}`,
	} {
		for _, key := range []string{"aggs", "aggregations"} {
			body := `{"size": 0, "` + key + `": ` + aggs + `}`
			if err := validateSearchRequest(t, body); err != nil {
				t.Errorf("%s: the request does not match api/openapi.yaml: %v", body, err)
			}
			e.must(http.StatusOK, "POST", "/indexes/as/_search", body)
		}
	}
	for _, aggs := range []string{
		`{"x": {}}`,
		`{"x": {"median": {"field": "price"}}}`,
		`{"x": {"terms": {"field": "brand"}, "stats": {"field": "price"}}}`,
		`{"x": {"terms": {}}}`,
		`{"x": {"terms": {"field": "brand", "nope": 1}}}`,
		`{"x": {"terms": {"field": "brand", "size": 0}}}`,
		`{"x": {"terms": {"field": "brand", "collect_mode": "sideways"}}}`,
		`{"x": {"terms": {"field": "brand", "order": {"_count": "up"}}}}`,
		`{"x": {"terms": {"field": "brand", "order": {"_count": "asc", "_key": "asc"}}}}`,
		`{"x": {"terms": {"field": "brand", "order": "_count"}}}`,
		`{"x": {"terms": {"field": "brand", "include": {"partition": 0, "num_partitions": 2}}}}`,
		`{"x": {"range": {"field": "price"}}}`,
		`{"x": {"range": {"field": "price", "ranges": [{"from": 1, "upto": 2}]}}}`,
		`{"x": {"histogram": {"field": "price"}}}`,
		`{"x": {"date_histogram": {"field": "created", "calendar_interval": "fortnight"}}}`,
		`{"x": {"cardinality": {"field": "brand", "threshold": 0}}}`,
		`{"x": {"cardinality": {"field": "brand", "precision": 30}}}`,
		`{"x": {"percentiles": {"field": "price", "percents": [101]}}}`,
		`{"x": {"percentiles": {"field": "price", "tdigest": {"compression": 5}}}}`,
		`{"x": {"percentiles": {"field": "price", "keyed": "yes"}}}`,
		`{"x": {"percentile_ranks": {"field": "price"}}}`,
		`{"x": {"top_metrics": {"metrics": {"field": "price"}}}}`,
		`{"x": {"top_metrics": {"metrics": {"field": "price"}, "sort": "price", "size": 11}}}`,
		`{"x": {"top_metrics": {"metrics": {"name": "price"}, "sort": "price"}}}`,
		`{"x": {"top_metrics": {"field": "price", "metrics": {"field": "price"}, "sort": "price"}}}`,
		`{"x": {"composite": {}}}`,
		`{"x": {"composite": {"size": 10001, "sources": [{"b": {"terms": {"field": "brand"}}}]}}}`,
		`{"x": {"composite": {"sources": [{"b": {"range": {"field": "price"}}}]}}}`,
		`{"x": {"composite": {"sources": [{"b": {"terms": {"field": "brand", "missing_bucket": true}}}]}}}`,
		`{"x": {"composite": {"sources": [{"b": {"terms": {"field": "brand"}}, "c": {"terms": {"field": "brand"}}}]}}}`,
		`{"x": {"composite": {"sources": [{"b": {"terms": {"field": "brand"}}}], "sort": "x"}}}`,
		`{"x": {"filters": {"filters": [{"all": []}]}}}`,
		`{"x": {"filters": {"filters": {"a": {"all": []}}, "other_bucket_key": ""}}}`,
		`{"x": {"global": {"field": "brand"}}}`,
		`{"x": {"filter": {"field": "price", "op": "near", "value": 1}}}`,
	} {
		body := `{"size": 0, "aggs": ` + aggs + `}`
		if err := validateSearchRequest(t, body); err == nil {
			t.Errorf("%s passes api/openapi.yaml", body)
		}
		e.must(http.StatusBadRequest, "POST", "/indexes/as/_search", body)
	}
}

// TestAggregationResultSchemaIsStrict: a search response whose aggregation results are
// off every shape AggregationResult lists fails the response check.
func TestAggregationResultSchemaIsStrict(t *testing.T) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://x/indexes/as/_search", http.NoBody)
	if err != nil {
		t.Fatal(err)
	}
	h := http.Header{"Content-Type": []string{"application/json"}}
	check := func(aggs string) error {
		body := `{"took_ms":1,"timed_out":false,"total":{"value":0,"relation":"eq"},"hits":[],"next":null,"aggs":` + aggs + `}`
		return validateResponse(req, &http.Response{StatusCode: http.StatusOK, Header: h}, []byte(body))
	}
	for _, aggs := range []string{
		`{"t":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":"a","doc_count":1,"p":{"values":{"50.0":1}}}]}}`,
		`{"c":{"after_key":{"b":"a"},"buckets":[{"key":{"b":"a"},"doc_count":1,"tm":{"top":[{"sort":[1],"metrics":{"price":null}}]}}]}}`,
		`{"f":{"doc_count":3,"s":{"count":0,"min":null,"max":null,"avg":null,"sum":0}},"fs":{"buckets":{"a":{"doc_count":1}}}}`,
		`{"v":{"value":null},"p":{"values":[{"key":50,"value":null}]}}`,
	} {
		if err := check(aggs); err != nil {
			t.Errorf("%s: %v", aggs, err)
		}
	}
	for _, aggs := range []string{
		`{"x":{"bogus":1}}`,
		`{"x":{"value":"high"}}`,
		`{"t":{"buckets":[{"key":"a"}]}}`,
		`{"t":{"buckets":[{"key":"a","doc_count":1,"sub":{"bogus":1}}]}}`,
		`{"p":{"values":[{"key":50}]}}`,
		`{"tm":{"top":[{"metrics":{}}]}}`,
		`{"f":{"doc_count":1,"s":{"count":1}}}`,
	} {
		if err := check(aggs); err == nil {
			t.Errorf("%s passes AggregationResult", aggs)
		}
	}
}
