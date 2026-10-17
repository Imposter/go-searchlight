package search

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
)

// spotDoc is a document of up to maxPoints points over [0, 1000) on both axes, a
// price, and now and then a date-axis point list.
func spotDoc(rng *rand.Rand, maxPoints int) string {
	pairs := make([]string, rng.IntN(maxPoints+1))
	for i := range pairs {
		pairs[i] = fmt.Sprintf("[%d,%d]", rng.IntN(1000), rng.IntN(1000))
	}
	members := []string{`"spot":[` + strings.Join(pairs, ",") + `]`, `"price":` + strconv.Itoa(rng.IntN(100))}
	if rng.IntN(3) == 0 {
		members = append(members, fmt.Sprintf(`"when":[[%d,%d]]`, 1_700_000_000_000+rng.Int64N(30)*86_400_000, rng.IntN(10)))
	}
	if rng.IntN(15) == 0 {
		members = members[1:]
	}
	return "{" + strings.Join(members, ",") + "}"
}

// spotBox is a box over spotDoc's coordinates: one-sided bounds most often (x above
// a threshold, y above or below a cut), sometimes two-sided or one axis alone.
func spotBox(rng *rand.Rand) string {
	end := func(keys ...string) string { return fmt.Sprintf("%q:%d", pick(rng, keys), rng.IntN(1001)) }
	axis := func() string {
		switch rng.IntN(4) {
		case 0:
			return "{" + end("gt", "gte") + "," + end("lt", "lte") + "}"
		case 1:
			return "{" + end("lt", "lte") + "}"
		default:
			return "{" + end("gt", "gte") + "}"
		}
	}
	switch rng.IntN(6) {
	case 0:
		return `{"x":` + axis() + `}`
	case 1:
		return `{"y":` + axis() + `}`
	default:
		return `{"x":` + axis() + `,"y":` + axis() + `}`
	}
}

// TestBoxEqualsBruteForce: box queries over segments whose point trees have many
// leaves, on two shards, through updates, deletes, refreshes and merges, alone, negated,
// beside other conditions, as a post_filter and a filter aggregation, and again from
// the filter cache, answer exactly what query.Match does.
func TestBoxEqualsBruteForce(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0xb0c5))
	c := newCluster(t, 2)
	n := 2000
	for round := range 5 {
		for i := range n / 5 {
			c.upsert(fmt.Sprintf("s%04d", round*n/5+i), spotDoc(rng, 12))
		}
		for range 40 {
			c.upsert(fmt.Sprintf("s%04d", rng.IntN(n)), spotDoc(rng, 12))
		}
		for range 30 {
			c.remove(fmt.Sprintf("s%04d", rng.IntN(n)))
		}
		c.refreshAll()
		if round == 3 {
			c.merge(0, 1)
			c.merge(1, 2)
		}
	}
	docs := c.docs()
	matched := 0
	checks := 150
	if longMode() {
		checks = 1500
	}
	for range checks {
		box := `{"field":"spot","op":"box","value":` + spotBox(rng) + `}`
		for _, raw := range []string{
			box,
			`{"not":` + box + `}`,
			`{"all":[` + box + `,{"field":"price","op":"lt","value":` + strconv.Itoa(rng.IntN(100)) + `}]}`,
			`{"any":[` + box + `,{"field":"when","op":"box","value":{"x":{"gte":1701000000000},"y":{"lt":3}}}]}`,
		} {
			q, ps := query.Parse([]byte(raw))
			if len(ps) == 0 {
				ps = query.Validate(q, testMapping)
			}
			if len(ps) > 0 {
				continue // two-sided bounds drawn reversed
			}
			want := docIDs(brute(docs, q, nil))
			if raw == box && len(want) > 0 {
				matched++
			}
			for pass := range 2 {
				got, err := c.search(&Request{Query: q, Size: MaxSize, TrackTotal: TrackTotalAll})
				if err != nil {
					t.Fatalf("%s: %v", raw, err)
				}
				if ids := hitIDs(got.Hits); !slices.Equal(ids, want) {
					t.Fatalf("pass %d: %s: %d hits, want %d", pass, raw, len(ids), len(want))
				}
			}
			filtered, err := c.search(&Request{
				Query: &query.All{}, PostFilter: q, Size: MaxSize, TrackTotal: TrackTotalAll,
				Aggs: map[string]Agg{"in": {Type: AggFilter, Filter: q}},
			})
			if err != nil {
				t.Fatalf("%s as a post_filter and filter: %v", raw, err)
			}
			if ids := hitIDs(filtered.Hits); !slices.Equal(ids, want) {
				t.Fatalf("%s as a post_filter: %d hits, want %d", raw, len(ids), len(want))
			}
			if got := filtered.Aggs["in"].DocCount; got != int64(len(want)) {
				t.Fatalf("%s as a filter aggregation: %d documents, want %d", raw, got, len(want))
			}
		}
	}
	if matched < checks/3 {
		t.Fatalf("only %d of %d boxes matched a document: the corpus is too sparse", matched, checks)
	}
}

// Point fields take no aggregation and no sort: each is refused at its loc.
func TestPointFieldRefusesAggsAndSort(t *testing.T) {
	c := newCluster(t, 1)
	c.upsert("a", `{"spot":[1,2]}`)
	c.refreshAll()
	for loc, r := range map[string]*Request{
		"aggs.t.terms.field":       {Query: &query.All{}, Aggs: map[string]Agg{"t": {Type: AggTerms, Field: "spot"}}},
		"aggs.s.stats.field":       {Query: &query.All{}, Aggs: map[string]Agg{"s": {Type: AggStats, Field: "when"}}},
		"aggs.c.cardinality.field": {Query: &query.All{}, Aggs: map[string]Agg{"c": {Type: AggCardinality, Field: "spot"}}},
		"aggs.f.aggs.m.min.field": {Query: &query.All{}, Aggs: map[string]Agg{"f": {Type: AggGlobal, Aggs: map[string]Agg{
			"m": {Type: AggMin, Field: "spot"},
		}}}},
		"sort.0": {Query: &query.All{}, Sort: []SortField{{Field: "spot"}}, Size: 1},
	} {
		_, err := c.search(r)
		var re *RequestError
		if !errors.As(err, &re) || len(re.Problems) == 0 || re.Problems[0].Loc != loc || !strings.Contains(re.Problems[0].Message, "point field") {
			t.Errorf("%s: %v", loc, err)
		}
	}
}

// The request cache keys a box by its bounds once date math is resolved: two boxes
// apart are two keys, two spellings of one box one key; a box reading now unrounded
// is never cached, a rounded one hits until its rounding moves on.
func TestRequestCacheBox(t *testing.T) {
	key := func(raw string) []byte {
		t.Helper()
		r := parseReq(t, `{"size":0,"query":{"field":"spot","op":"box","value":`+raw+`}}`)
		k, ok := requestCacheKey(r)
		if !ok {
			t.Fatalf("%s is not cacheable", raw)
		}
		return k
	}
	base := key(`{"x":{"gte":5},"y":{"lt":7}}`)
	if !bytes.Equal(key(`{"y":{"lt":7.0},"x":{"gte":5}}`), base) {
		t.Fatal("one box spelled two ways has two keys")
	}
	for _, other := range []string{`{"x":{"gt":5},"y":{"lt":7}}`, `{"x":{"gte":5},"y":{"lte":7}}`, `{"x":{"gte":6},"y":{"lt":7}}`, `{"y":{"gte":5},"x":{"lt":7}}`, `{"x":{"gte":5}}`} {
		if bytes.Equal(key(other), base) {
			t.Errorf("%s has the key of another box", other)
		}
	}

	s := newRCShard(t)
	for i := range 40 {
		s.put(fmt.Sprintf("p%d", i), fmt.Sprintf(`{"when":[[%d,%d]],"spot":[[%d,%d],[%d,%d]]}`,
			1_700_000_000_000+int64(i)*86_400_000, i%5, i, i%7, i%9, i))
	}
	s.refresh()
	c := NewRequestCache(RequestCacheOptions{MaxBytes: 8 << 20})
	at := func(raw string, now time.Time) *Request {
		r := parseReq(t, raw)
		if ps := ResolveQueries(r, now); len(ps) > 0 {
			t.Fatal(ps)
		}
		return r
	}
	day := time.UnixMilli(1_700_000_000_000 + 20*86_400_000).UTC()
	rounded := `{"size":0,"query":{"field":"when","op":"box","value":{"x":{"gte":"now-7d/d"},"y":{"gte":2}}}}`
	first := s.search(c, at(rounded, day))
	second := s.search(c, at(rounded, day.Add(time.Minute)))
	wantStats(t, c, 1, 1)
	if !sameResult(t, first, second) || first.Total == 0 {
		t.Fatalf("a cached box: %+v and %+v", first, second)
	}
	s.search(c, at(rounded, day.Add(48*time.Hour)))
	wantStats(t, c, 1, 2)
	exact := `{"size":0,"query":{"field":"when","op":"box","value":{"x":{"gte":"now-7d"}}}}`
	if r := at(exact, day); !r.NoRequestCache {
		t.Fatal("a box reading now unrounded left the request cache on")
	}
	if r := at(`{"size":0,"aggs":{"f":{"filter":{"field":"when","op":"box","value":{"x":{"lt":"now"}}}}}}`, day); !r.NoRequestCache {
		t.Fatal("a filter's box reading now unrounded left the request cache on")
	}
	s.search(c, at(`{"size":0,"query":{"field":"spot","op":"box","value":{"x":{"gte":3}}}}`, day))
	s.search(c, at(`{"size":0,"query":{"field":"spot","op":"box","value":{"x":{"gte":4}}}}`, day))
	wantStats(t, c, 1, 4)
}
