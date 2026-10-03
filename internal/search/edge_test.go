package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/query"
)

func mustParse(t testing.TB, raw string) query.Node {
	t.Helper()
	n, ps := query.Parse([]byte(raw))
	if len(ps) > 0 {
		t.Fatalf("Parse(%s): %v", raw, ps)
	}
	return n
}

// checkQuery runs raw on c and compares it with brute force (ids, in _id order).
func checkQuery(t *testing.T, c *cluster, raw string) []string {
	t.Helper()
	q := mustParse(t, raw)
	want := docIDs(brute(c.docs(), q, nil))
	got, err := c.search(&Request{Query: q, Size: MaxSize, TrackTotal: TrackTotalAll})
	if err != nil {
		t.Fatalf("%s: %v", raw, err)
	}
	if ids := hitIDs(got.Hits); !slices.Equal(ids, want) {
		t.Fatalf("%s:\n got %q\nwant %q", raw, ids, want)
	}
	return want
}

// Values too long for grams are candidates for every contains, starts_with and similar.
func TestTruncatedValuesAreCandidates(t *testing.T) {
	c := newCluster(t, 1)
	long := strings.Repeat("abab ", 300) + "needle tail"
	c.upsert("long", fmt.Sprintf(`{"desc":%q}`, long))
	c.upsert("short", `{"desc":"abab"}`)
	c.upsert("other", `{"desc":"zzz"}`)
	c.upsert("repeat", fmt.Sprintf(`{"desc":%q}`, strings.Repeat("abcdefgh ", 150)))
	c.refreshAll()
	cases := map[string][]string{
		`{"field":"desc","op":"contains","value":"needle tail"}`:                     {"long"},
		`{"field":"desc","op":"contains","value":"abab"}`:                            {"long", "short"},
		`{"field":"desc","op":"contains_all","value":["abab","tail"]}`:               {"long"},
		`{"field":"desc","op":"starts_with","value":"abab abab"}`:                    {"long"},
		`{"field":"desc","op":"similar","value":{"text":"abab","min":0.25}}`:         {"long", "short"},
		`{"not":{"field":"desc","op":"similar","value":{"text":"abab","min":0.25}}}`: {"other", "repeat"},
		`{"field":"desc","op":"similar","value":{"text":"ABCDEFGH","min":0.9}}`:      {"repeat"},
	}
	for raw, want := range cases {
		if got := checkQuery(t, c, raw); !slices.Equal(got, want) {
			t.Errorf("%s: %q, want %q", raw, got, want)
		}
	}
}

// similar's trigram prefilter finds text whose casefolded spelling differs from its
// lowercased one (Cherokee folds to capitals, lowercases to small letters).
func TestSimilarPrefilterSpellings(t *testing.T) {
	c := newCluster(t, 1)
	c.upsert("cherokee", `{"title":"ꭰꭱꭲꭳ"}`)
	c.upsert("capitals", `{"title":"ᎠᎡᎢᎣ"}`)
	c.upsert("latin", `{"title":"Straße Café"}`)
	c.refreshAll()
	if got := checkQuery(t, c, `{"field":"title","op":"similar","value":{"text":"ꭰꭱꭲꭳ","min":0.9}}`); len(got) != 2 {
		t.Errorf("similar Cherokee: %q, want both spellings", got)
	}
	checkQuery(t, c, `{"field":"title","op":"similar","value":{"text":"strasse cafe","min":0.3}}`)
	checkQuery(t, c, `{"field":"title","op":"similar","value":{"text":"STRASSE","min":0.9}}`)
}

// lowerTable stops at lowerTableMax: nothing past it changes case.
func TestLowerTableCoversEveryCase(t *testing.T) {
	if testing.Short() {
		t.Skip("walks every code point")
	}
	for r := rune(lowerTableMax + 1); r <= 0x10FFFF; r++ {
		s := string(r)
		if f := analysis.Normalize(s); f != s {
			t.Fatalf("U+%04X folds to %q", r, f)
		}
		if l := analysis.Lower(s); l != s {
			t.Fatalf("U+%04X lowers to %q", r, l)
		}
	}
}

// A starts_with over more values than maxPrefixTerms falls back to grams and a check.
func TestStartsWithManyValues(t *testing.T) {
	c := newCluster(t, 1)
	for i := range maxPrefixTerms + 200 {
		c.upsert(fmt.Sprintf("d%05d", i), fmt.Sprintf(`{"brand":"prefix value %05d"}`, i))
	}
	c.upsert("x", `{"brand":"other prefix value"}`)
	c.refreshAll()
	if got := checkQuery(t, c, `{"field":"brand","op":"starts_with","value":"prefix val"}`); len(got) != maxPrefixTerms+200 {
		t.Errorf("starts_with: %d hits", len(got))
	}
	checkQuery(t, c, `{"field":"brand","op":"starts_with","value":"prefix value 0001"}`)
}

// _id queries compare the id normalized, as a keyword; hits carry the exact id.
func TestIDQueriesNormalize(t *testing.T) {
	c := newCluster(t, 2)
	for _, id := range []string{"A", "a", "Straße", "STRASSE", "b"} {
		c.upsert(id, `{}`)
	}
	c.refreshAll()
	if got := checkQuery(t, c, `{"field":"_id","op":"eq","value":"a"}`); !slices.Equal(got, []string{"A", "a"}) {
		t.Errorf("eq a: %q", got)
	}
	checkQuery(t, c, `{"field":"_id","op":"contains","value":"ss"}`)
	checkQuery(t, c, `{"field":"_id","op":"starts_with","value":"str"}`)
	checkQuery(t, c, `{"field":"_id","op":"in","value":["b","straße"]}`)
}

func TestTimeoutReturnsPartialResults(t *testing.T) {
	c := newCluster(t, 1)
	for i := range 50 {
		c.upsert(fmt.Sprintf("d%d", i), `{"title":"x"}`)
	}
	c.refreshAll()
	g := c.shards[0].Acquire()
	defer g.Release()
	// Request.Timeout: either the search beat it, or it reports a partial result.
	res, err := ExecuteShard(context.Background(), g, &Request{Query: &query.All{}, Size: 10, Timeout: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	if res.TimedOut != (res.TotalRelation == RelationGte) || (!res.TimedOut && res.Total != 50) {
		t.Fatalf("timeout: timed out %v, %d %s", res.TimedOut, res.Total, res.TotalRelation)
	}
	// A deadline already past: deterministic.
	past, cancelPast := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelPast()
	res, err = ExecuteShard(past, g, &Request{Query: &query.All{}, Size: 10})
	if err != nil {
		t.Fatal(err)
	}
	if !res.TimedOut || res.TotalRelation != RelationGte {
		t.Fatalf("timed out %v, relation %s", res.TimedOut, res.TotalRelation)
	}
	resp := Reduce([]*ShardResult{res}, &Request{Size: 10})
	if !resp.TimedOut || resp.TotalRelation != RelationGte {
		t.Fatalf("reduced: timed out %v, relation %s", resp.TimedOut, resp.TotalRelation)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ExecuteShard(ctx, g, &Request{Query: &query.All{}, Size: 10}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
}

func TestFieldsLimitBodies(t *testing.T) {
	c := newCluster(t, 1)
	c.upsert("a", `{"title":"t","brand":"b","price":3,"title":"t2"}`)
	c.refreshAll()
	resp, err := c.search(&Request{Query: &query.All{}, Size: 1, Fields: []string{"price", "title", "nope", "price"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(resp.Hits[0].Body); got != `{"price":3,"title":"t2"}` {
		t.Fatalf("body %s", got)
	}
}

func TestRequestErrors(t *testing.T) {
	c := newCluster(t, 1)
	c.refreshAll()
	g := c.shards[0].Acquire()
	defer g.Release()
	bad := []*Request{
		{Query: &query.All{}, Size: -1},
		{Query: &query.All{}, Sort: []SortField{{Field: "tags"}}},
		{Query: &query.All{}, Sort: []SortField{{Field: "price"}}, SearchAfter: []any{1.0}},
		{Query: &query.All{}, Sort: []SortField{{Field: "price"}}, SearchAfter: []any{"x", "id"}},
		{Query: &query.All{}, Aggs: map[string]Agg{"a": {Type: AggStats, Field: "brand"}}},
		{Query: &query.All{}, Aggs: map[string]Agg{"a": {Type: "median", Field: "price"}}},
		{Query: &query.All{}, Aggs: map[string]Agg{"a": {Type: AggTerms, Field: "brand", Aggs: map[string]Agg{"b": {Type: AggTerms, Field: "brand"}}}}},
		{Query: &query.All{}, Aggs: map[string]Agg{"a": {Type: AggHistogram, Field: "price"}}},
		{Query: &query.All{}, Aggs: map[string]Agg{"a": {Type: AggCardinality, Field: "price", Precision: 30}}},
	}
	for i, r := range bad {
		var re *RequestError
		if _, err := ExecuteShard(context.Background(), g, r); !errors.As(err, &re) {
			t.Errorf("request %d: %v, want a RequestError", i, err)
		}
	}
}

func TestParseRequest(t *testing.T) {
	r, ps := ParseRequest([]byte(`{
		"query": {"field": "brand", "op": "eq", "value": "acme"},
		"sort": ["price", {"brand": "desc"}, {"created": {"order": "asc"}}],
		"size": 20, "search_after": [1.5, "b", null, "id-1"], "track_total": true,
		"fields": ["title"], "timeout": "250ms",
		"aggs": {
			"brands": {"terms": {"field": "brand", "size": 5, "shard_size": 50}, "aggs": {"p": {"stats": {"field": "price"}}}},
			"prices": {"range": {"field": "price", "ranges": [{"to": 10}, {"from": 10, "key": "big"}]}},
			"h": {"histogram": {"field": "price", "interval": 5}},
			"d": {"date_histogram": {"field": "created", "calendar_interval": "month"}},
			"f": {"date_histogram": {"field": "created", "fixed_interval": "1d"}},
			"u": {"cardinality": {"field": "brand", "precision": 12}}
		}
	}`))
	if len(ps) > 0 {
		t.Fatalf("problems: %v", ps)
	}
	if r.Size != 20 || r.TrackTotal != TrackTotalAll || r.Timeout != 250*time.Millisecond || len(r.Fields) != 1 {
		t.Fatalf("request %+v", r)
	}
	if want := []SortField{{Field: "price"}, {Field: "brand", Desc: true}, {Field: "created"}}; !slices.Equal(r.Sort, want) {
		t.Fatalf("sort %+v", r.Sort)
	}
	if !slices.Equal(r.SearchAfter, []any{1.5, "b", nil, "id-1"}) {
		t.Fatalf("search_after %#v", r.SearchAfter)
	}
	if a := r.Aggs["brands"]; a.Type != AggTerms || a.Size != 5 || a.ShardSize != 50 || a.Aggs["p"].Type != AggStats {
		t.Fatalf("brands %+v", a)
	}
	if a := r.Aggs["prices"]; len(a.Ranges) != 2 || *a.Ranges[0].To != 10 || a.Ranges[1].Key != "big" {
		t.Fatalf("prices %+v", a)
	}
	if a := r.Aggs["f"]; a.Interval != 86_400_000 {
		t.Fatalf("fixed interval %v", a.Interval)
	}
	_, ps = ParseRequest([]byte(`{"size": -1, "sort": [{"a": "up"}], "track_total": 0, "bogus": 1,
		"aggs": {"x": {"terms": {"field": "a", "colour": 1}}, "y": {"terms": {}, "stats": {}}},
		"query": {"field": "a", "op": "nope"}}`))
	locs := make([]string, len(ps))
	for i, p := range ps {
		locs[i] = p.Loc
	}
	for _, want := range []string{"size", "sort.0.a", "track_total", "bogus", "aggs.x.terms.colour", "aggs.y", "query.op"} {
		if !slices.Contains(locs, want) {
			t.Errorf("no problem at %s (got %q)", want, locs)
		}
	}
}

func TestResultsMarshal(t *testing.T) {
	c := newCluster(t, 1)
	c.upsert("a", `{"brand":"x","price":1,"created":1700000000000}`)
	c.upsert("b", `{"brand":"y","price":3}`)
	c.refreshAll()
	r := &Request{Query: &query.All{}, Aggs: map[string]Agg{
		"t": {Type: AggTerms, Field: "brand", Aggs: map[string]Agg{"s": {Type: AggStats, Field: "price"}}},
		"r": {Type: AggRange, Field: "price", Ranges: []Range{{To: ptr(2.0)}}},
		"d": {Type: AggDateHistogram, Field: "created", Calendar: "day"},
		"c": {Type: AggCardinality, Field: "brand"},
		"e": {Type: AggStats, Field: "nothing"},
	}}
	resp, err := c.search(r)
	if err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(resp.Aggs)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"c":{"value":2}`,
		`"buckets":[{"doc_count":1,"key":"*-2","to":2}]`,
		`{"doc_count":1,"key":"x","s":{"count":1,"min":1,"max":1,"avg":1,"sum":1}}`,
		`"key_as_string":"2023-11-14T00:00:00.000Z"`,
		`"e":{"count":0,"min":null,"max":null,"avg":null,"sum":0}`,
		`"doc_count_error_upper_bound":0,"sum_other_doc_count":0`,
	} {
		if !strings.Contains(string(b), want) {
			t.Errorf("aggs JSON lacks %s:\n%s", want, b)
		}
	}
}

func ptr[T any](v T) *T { return &v }
