package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
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

// Lazy verification: candidates are verified only as the top hits and the total
// need them, and the total stays exact up to TrackTotal, a lower bound past it.
func TestLazyVerificationTotals(t *testing.T) {
	for _, batchAfter := range []uint64{2048, 2} {
		t.Run(fmt.Sprintf("batch after %d", batchAfter), func(t *testing.T) {
			lazyBatchAfter = batchAfter
			defer func() { lazyBatchAfter = 2048 }()
			lazyTotals(t, batchAfter)
		})
	}
}

func lazyTotals(t *testing.T, tag uint64) {
	c := newCluster(t, 1)
	for i := range 60 {
		title := "abc bcd filler" // every gram of "abcd", never "abcd": a candidate only
		if i < 20 {               // the matches come first, so counting reaches TrackTotal early
			title = "xabcdx"
		}
		c.upsert(fmt.Sprintf("d%02d", i), fmt.Sprintf(`{"title":%q,"price":%d}`, title, i))
	}
	c.refreshAll()
	countChunk = 3 // count in small steps, so stopping early is visible
	defer func() { countChunk = 4096 }()
	for _, tc := range []struct {
		track    int
		total    int64
		relation string
	}{
		{20, 20, RelationEq},
		{21, 20, RelationEq},
		{19, 19, RelationGte},
		{1, 1, RelationGte},
		{TrackTotalAll, 20, RelationEq},
		{0, 20, RelationEq},
	} {
		// A condition of its own each time, so the filter cache never serves it and
		// every request verifies lazily (even when the test runs again in one process).
		leafUsage.reset()
		q := mustParse(t, fmt.Sprintf(`{"field":"title","op":"contains_any","value":["abcd","unique-%d-%d"]}`, tc.track, tag))
		resp, err := c.search(&Request{Query: q, Size: 3, Sort: []SortField{{Field: "price"}}, TrackTotal: tc.track})
		if err != nil {
			t.Fatal(err)
		}
		if resp.Total != tc.total || resp.TotalRelation != tc.relation {
			t.Errorf("track %d: total %d %s, want %d %s", tc.track, resp.Total, resp.TotalRelation, tc.total, tc.relation)
		}
		if ids := hitIDs(resp.Hits); !slices.Equal(ids, []string{"d00", "d01", "d02"}) {
			t.Errorf("track %d: hits %q", tc.track, ids)
		}
	}
	// No total: only what the top hits need is verified.
	g := c.shards[0].Acquire()
	defer g.Release()
	// A condition no earlier request used, so the filter cache stays out of it.
	leafUsage.reset()
	fresh := mustParse(t, fmt.Sprintf(`{"field":"title","op":"contains_any","value":["abcd","lazy-test-unique-%d"]}`, tag))
	res, err := ExecuteShard(context.Background(), g, &Request{Query: fresh, Size: 1, Sort: []SortField{{Field: "price", Desc: true}}, TrackTotal: TrackTotalNone})
	if err != nil {
		t.Fatal(err)
	}
	if tag > 2 && (res.Scanned >= 60 || res.TotalRelation != RelationGte) || len(res.Hits) != 1 || res.Hits[0].ID != "d19" || res.Total > 20 {
		t.Errorf("track none: scanned %d, %d %s, hits %v", res.Scanned, res.Total, res.TotalRelation, hitIDs(res.Hits))
	}
	if r, ps := ParseRequest([]byte(`{"track_total": false}`)); len(ps) > 0 || r.TrackTotal != TrackTotalNone {
		t.Errorf("track_total false: %v %v", r, ps)
	}
}

// openShard opens one shard with mapping m (tests that change mappings).
func openShard(t *testing.T, m *schema.Mapping) *shard.Shard {
	t.Helper()
	s, err := shard.Open(context.Background(), t.TempDir(), m, shard.Options{
		RefreshInterval: -1, DisableMerges: true, Logger: quiet, FilterCache: shard.NewFilterCache(1<<20, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

func put(t *testing.T, s *shard.Shard, m *schema.Mapping, seq int64, id, body string) {
	t.Helper()
	d, _, err := schema.Analyze(m, id, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(context.Background(), []shard.Change{{Seq: seq, Kind: shard.Upsert, Doc: &d}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func searchIDs(t *testing.T, s *shard.Shard, raw string) []string {
	t.Helper()
	g := s.Acquire()
	defer g.Release()
	res, err := ExecuteShard(context.Background(), g, &Request{Query: mustParse(t, raw), Size: 100, TrackTotal: TrackTotalAll})
	if err != nil {
		t.Fatal(err)
	}
	return hitIDs(res.Hits)
}

// A phrase's residual reads the document as it was indexed, whatever the mapping
// has become: (B) a field the generation's mapping no longer holds (dynamic false),
// (C) a strict mapping that now refuses another member of the body.
func TestPhraseResidualIgnoresMappingDrift(t *testing.T) {
	phrase := `{"field":"notes","op":"words_all","value":"red blue"}`
	t.Run("dynamic false", func(t *testing.T) {
		indexed := &schema.Mapping{Dynamic: schema.DynamicFalse, Fields: map[string]schema.FieldType{"notes": schema.Text}}
		s := openShard(t, indexed)
		put(t, s, indexed, 1, "a", `{"notes":"Red blue green"}`)
		put(t, s, indexed, 2, "b", `{"notes":"blue red"}`)
		s.SetMapping(&schema.Mapping{Dynamic: schema.DynamicFalse, Fields: map[string]schema.FieldType{"other": schema.Text}})
		put(t, s, indexed, 3, "c", `{"other":"x"}`) // a new generation, with the new mapping
		if got := searchIDs(t, s, phrase); !slices.Equal(got, []string{"a"}) {
			t.Fatalf("phrase: %q, want [a]", got)
		}
	})
	t.Run("strict", func(t *testing.T) {
		indexed := &schema.Mapping{Dynamic: schema.DynamicStrict, Fields: map[string]schema.FieldType{"notes": schema.Text, "extra": schema.Keyword}}
		s := openShard(t, indexed)
		put(t, s, indexed, 1, "a", `{"notes":"red blue","extra":"e"}`)
		put(t, s, indexed, 2, "b", `{"notes":"red  , blue!","extra":"e"}`)
		put(t, s, indexed, 3, "c", `{"notes":"blue red","extra":"e"}`)
		s.SetMapping(&schema.Mapping{Dynamic: schema.DynamicStrict, Fields: map[string]schema.FieldType{"notes": schema.Text}})
		put(t, s, indexed, 4, "d", `{"notes":"x"}`)
		if got := searchIDs(t, s, phrase); !slices.Equal(got, []string{"a", "b"}) {
			t.Fatalf("phrase: %q, want [a b]", got)
		}
	})
}

// Two words_* conditions that normalize alike but have different words are cached
// apart: the second does not get the first's bitmap.
func TestWordsCacheKeysKeepPhrases(t *testing.T) {
	c := newCluster(t, 1)
	c.upsert("ss", `{"title":"ss x"}`)
	c.upsert("acute", `{"title":"s\u015b x"}`)
	c.refreshAll()
	for range 3 { // past the second sighting, so both are cached
		checkQuery(t, c, `{"field":"title","op":"words_all","value":"\u00df\u0301 x"}`)
		checkQuery(t, c, `{"field":"title","op":"words_all","value":"s\u015b x"}`)
	}
}

func TestHistogramBucketLimit(t *testing.T) {
	c := newCluster(t, 1)
	for i := range 3 {
		c.upsert(fmt.Sprintf("d%d", i), fmt.Sprintf(`{"price":%d}`, i*int(1e6)))
	}
	c.refreshAll()
	r := &Request{Query: &query.All{}, Aggs: map[string]Agg{"h": {Type: AggHistogram, Field: "price", Interval: 1}}}
	resp, err := c.search(r)
	if err != nil {
		t.Fatal(err)
	}
	if h := resp.Aggs["h"]; len(h.Buckets) != 3 || h.Truncated {
		t.Fatalf("filling past MaxBuckets: %d buckets, truncated %v", len(h.Buckets), h.Truncated)
	}
	for i := range MaxBuckets + 1 {
		c.upsert(fmt.Sprintf("e%d", i), fmt.Sprintf(`{"price":%d}`, i))
	}
	c.refreshAll()
	var re *RequestError
	if _, err := c.search(r); !errors.As(err, &re) {
		t.Fatalf("more than MaxBuckets: %v, want a RequestError", err)
	}
}

func TestNoBodiesThenFetch(t *testing.T) {
	c := newCluster(t, 1)
	c.upsert("a", `{"title":"one","price":1}`)
	c.upsert("b", `{"title":"two","price":2}`)
	c.refreshAll()
	g := c.shards[0].Acquire()
	defer g.Release()
	res, err := ExecuteShard(context.Background(), g, &Request{Query: &query.All{}, Size: 2, NoBodies: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range res.Hits {
		if h.Body != nil || h.Ref == nil {
			t.Fatalf("query phase hit %+v", h)
		}
	}
	if err := FetchShard(context.Background(), g, res.Hits, []string{"price"}); err != nil {
		t.Fatal(err)
	}
	if got := string(res.Hits[0].Body) + string(res.Hits[1].Body); got != `{"price":1}{"price":2}` {
		t.Fatalf("fetched %s", got)
	}
	stale := []Hit{{ID: "a", Ref: &HitRef{Segment: "gone", Ord: 0}}}
	if err := FetchShard(context.Background(), g, stale, nil); !errors.Is(err, ErrStaleHit) {
		t.Fatalf("stale ref: %v", err)
	}
}

func TestRunParallelRecoversPanics(t *testing.T) {
	var ran atomic.Int64
	err := runParallel(8, func(i int) {
		ran.Add(1)
		if i == 5 {
			panic("boom")
		}
	})
	if err == nil || !strings.Contains(err.Error(), "boom") || ran.Load() != 8 {
		t.Fatalf("err %v, ran %d", err, ran.Load())
	}
}

func TestRankCacheDropsClosedSegments(t *testing.T) {
	c := newCluster(t, 1)
	for i := range 20 {
		c.upsert(fmt.Sprintf("d%02d", i), `{"price":1}`)
	}
	c.refreshAll()
	g := c.shards[0].Acquire()
	seg := g.Segments[0]
	ranksFor(seg.ID, seg.Reader)
	g.Release()
	if rankCache.get(seg.ID) == nil {
		t.Fatal("rank array not cached")
	}
	c.upsert("d99", `{"price":1}`)
	c.refreshAll()
	c.merge(0, 1) // the old segment is merged away and closed
	g = c.shards[0].Acquire()
	ranksFor(g.Segments[0].ID, g.Segments[0].Reader) // a put sweeps closed segments
	g.Release()
	deadline := time.Now().Add(5 * time.Second)
	for rankCache.get(seg.ID) != nil {
		if time.Now().After(deadline) {
			t.Fatal("a closed segment's rank array is still cached")
		}
		time.Sleep(10 * time.Millisecond)
		g = c.shards[0].Acquire()
		rankCache.put("probe", g.Segments[0].Reader, nil)
		g.Release()
	}
}

func TestUsageCountsOncePerRequest(t *testing.T) {
	var u usageSketch
	for range 3 {
		if u.seen("k", 1) {
			t.Fatal("one request saw its own counts")
		}
	}
	if !u.seen("k", 2) {
		t.Fatal("a second request did not see the first")
	}
	u.halve()
	u.halve()
	if u.seen("k", 3) {
		t.Fatal("halving did not fade the count")
	}
}
