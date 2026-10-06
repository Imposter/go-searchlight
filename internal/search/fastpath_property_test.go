package search

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// Every range, sort and aggregation fast path against brute force over the visible
// documents, on segments large enough to have many point blocks.

const revBase = int64(1_700_000_000_000)

func revDay(i int64) int64 { return revBase - revBase%86_400_000 + i*86_400_000 }

func revPrice(rng *rand.Rand, round int) (string, bool) {
	switch k := rng.IntN(20); {
	case k == 0 || k == 1:
		return "", false
	case k == 2 && round%2 == 1:
		return pick(rng, []string{"-0", "0", "-0.0", "0.0"}), true // float mode, -0 beside +0
	case k == 3:
		return pick(rng, []string{"-1.5", "1.5", "-7", "1e-300", "-1e-300"}), true
	case k < 10:
		return strconv.Itoa(rng.IntN(30)), true // long runs of ties: whole blocks in one bucket
	default:
		return strconv.FormatFloat(math.Round(math.Exp(rng.NormFloat64()*1.5+3)*100)/100, 'f', -1, 64), true
	}
}

func revCreated(rng *rand.Rand) (string, bool) {
	switch k := rng.IntN(10); {
	case k == 0:
		return "", false
	case k < 4: // millisecond edges of days and months
		d := revDay(int64(rng.IntN(400)))
		if rng.IntN(2) == 0 {
			t := time.UnixMilli(d).UTC()
			d = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC).UnixMilli()
		}
		return strconv.FormatInt(d+int64(rng.IntN(3)-1), 10), true
	case k < 6:
		return strconv.FormatInt(revDay(int64(rng.IntN(40))), 10), true // ties
	default:
		return strconv.FormatInt(revBase+rng.Int64N(400*86_400_000), 10), true
	}
}

func revDoc(rng *rand.Rand, round int) string {
	body := map[string]string{}
	if v, ok := revPrice(rng, round); ok {
		body["price"] = v
	}
	if v, ok := revCreated(rng); ok {
		body["created"] = v
	}
	if round != 2 || rng.IntN(10) == 0 { // round 2's segment has almost no brand
		switch k := rng.IntN(40); {
		case k == 0:
		case k == 1:
			body["brand"] = "7" // a number in a keyword field
		case k == 2:
			body["brand"] = "true"
		default:
			body["brand"] = strconv.Quote(fmt.Sprintf("b%03d", int(rng.ExpFloat64()*40)%400+round*3))
		}
	}
	if rng.IntN(4) > 0 {
		n := rng.IntN(4)
		tags := make([]string, n)
		for i := range tags {
			tags[i] = strconv.Quote(fmt.Sprintf("t%02d", rng.IntN(60)))
		}
		body["tags"] = "[" + join(tags) + "]"
	}
	body["title"] = strconv.Quote(pick(rng, []string{"red cup", "blue cup", "red pot", "green pan"}) + fmt.Sprintf(" %d", rng.IntN(50)))
	if rng.IntN(3) > 0 {
		body["active"] = pick(rng, []string{"true", "false"})
	}
	var parts []string
	for _, k := range slices.Sorted(func(yield func(string) bool) {
		for k := range body {
			if !yield(k) {
				return
			}
		}
	}) {
		parts = append(parts, strconv.Quote(k)+":"+body[k])
	}
	return "{" + join(parts) + "}"
}

func join(xs []string) string {
	out := ""
	for i, x := range xs {
		if i > 0 {
			out += ","
		}
		out += x
	}
	return out
}

// revCorpus builds shards of several segments of a few thousand documents each, with
// overwrites (deletes in older segments), plain deletes and the odd merge.
func revCorpus(t *testing.T, rng *rand.Rand, shards, rounds, perRound int) *cluster {
	t.Helper()
	c := newCluster(t, shards)
	for round := range rounds {
		for range perRound {
			c.upsert(fmt.Sprintf("r%05d", rng.IntN(perRound*rounds)), revDoc(rng, round))
		}
		for range perRound / 20 {
			c.remove(fmt.Sprintf("r%05d", rng.IntN(perRound*rounds)))
		}
		c.refreshAll()
		if round == rounds-2 {
			c.merge(0, 2)
		}
	}
	return c
}

func revValues(docs []*schema.Doc, field string) []float64 {
	var out []float64
	for _, d := range docs {
		if v, ok := d.Fields[field]; ok && v.Present && v.Number != nil {
			out = append(out, *v.Number)
		}
	}
	return out
}

func revBound(rng *rand.Rand, vals []float64) float64 {
	v := pick(rng, vals)
	switch rng.IntN(6) {
	case 0:
		return math.Nextafter(v, math.Inf(1))
	case 1:
		return math.Nextafter(v, math.Inf(-1))
	case 2:
		return pick(rng, []float64{0, math.Copysign(0, -1), -1.5, 1e-300})
	case 3:
		return v + 0.5
	default:
		return v
	}
}

func numJSON(v float64) string {
	if v == 0 && math.Signbit(v) {
		return "-0"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func revRangeLeaf(rng *rand.Rand, field string, vals []float64) string {
	op := pick(rng, []string{"lt", "lte", "gt", "gte", "between", "eq", "ne"})
	if op == "between" {
		a, b := revBound(rng, vals), revBound(rng, vals)
		if a > b && rng.IntN(4) > 0 {
			a, b = b, a
		}
		return fmt.Sprintf(`{"field":%q,"op":"between","value":[%s,%s]}`, field, numJSON(a), numJSON(b))
	}
	return fmt.Sprintf(`{"field":%q,"op":%q,"value":%s}`, field, op, numJSON(revBound(rng, vals)))
}

func TestRangeFiltersEveryPath(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x14c1))
	c := revCorpus(t, rng, 2, 5, 3000)
	docs := c.docs()
	vals := map[string][]float64{"price": revValues(docs, "price"), "created": revValues(docs, "created")}
	defer func() { docValuesFactor, scanChunk = defaultDocValuesFactor, 1<<16 }()
	for i := range cases(400) {
		field := pick(rng, []string{"price", "created"})
		raw := revRangeLeaf(rng, field, vals[field])
		switch rng.IntN(5) {
		case 0:
			raw = fmt.Sprintf(`{"all":[{"field":"brand","op":"eq","value":"b%03d"},%s]}`, rng.IntN(20), raw)
		case 1:
			raw = fmt.Sprintf(`{"any":[{"field":"tags","op":"has","value":"t%02d"},%s]}`, rng.IntN(60), raw)
		case 2:
			raw = fmt.Sprintf(`{"not":%s}`, raw)
		case 3:
			raw = fmt.Sprintf(`{"all":[%s,%s]}`, raw, revRangeLeaf(rng, "price", vals["price"]))
		}
		q := mustParse(t, raw)
		want := docIDs(brute(docs, q, nil))
		for _, f := range []float64{0, 0.5, 16, 1e12} {
			docValuesFactor = f
			scanChunk = pick(rng, []uint32{64, 1000, 1 << 16})
			for rep := range 3 { // the third sighting is admitted to the filter cache
				got, err := c.search(&Request{Query: q, Size: MaxSize, TrackTotal: TrackTotalAll})
				if err != nil {
					t.Fatalf("%s: %v", raw, err)
				}
				if ids := hitIDs(got.Hits); !slices.Equal(ids, want) || got.Total != int64(len(want)) {
					t.Fatalf("query %d factor %v rep %d %s: %d hits (total %d), want %d", i, f, rep, raw, len(ids), got.Total, len(want))
				}
			}
		}
	}
}

func TestRangeNonFiniteBounds(t *testing.T) {
	c := newCluster(t, 1)
	for i := range 300 {
		c.upsert(fmt.Sprintf("n%03d", i), fmt.Sprintf(`{"price":%d}`, i-150))
	}
	c.refreshAll()
	for _, raw := range []string{
		`{"field":"price","op":"gt","value":1e999}`,
		`{"field":"price","op":"lt","value":1e999}`,
		`{"field":"price","op":"gte","value":-1e999}`,
		`{"field":"price","op":"between","value":[-1e999,1e999]}`,
		`{"field":"price","op":"between","value":[5,-5]}`,
	} {
		n, ps := query.Parse([]byte(raw))
		if len(ps) > 0 {
			t.Logf("%s refused: %v", raw, ps[0].Message)
			continue
		}
		want := docIDs(brute(c.docs(), n, nil))
		got, err := c.search(&Request{Query: n, Size: MaxSize, TrackTotal: TrackTotalAll})
		if err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		if ids := hitIDs(got.Hits); !slices.Equal(ids, want) {
			t.Fatalf("%s: %d hits, want %d", raw, len(ids), len(want))
		}
		t.Logf("%s accepted: %d hits", raw, len(want))
	}
}

func TestSortWalkEveryPath(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x14c2))
	c := revCorpus(t, rng, 2, 5, 3000)
	docs := c.docs()
	vals := revValues(docs, "price")
	defer func() { sortWalkFactor = 2 }()
	for i := range cases(300) {
		field := pick(rng, []string{"price", "created"})
		sorts := []SortField{{Field: field, Desc: rng.IntN(2) == 0}}
		if rng.IntN(3) == 0 {
			sorts = append(sorts, SortField{Field: pick(rng, []string{"brand", "active", "price"}), Desc: rng.IntN(2) == 0})
		}
		raw := `{"all":[]}`
		switch rng.IntN(4) {
		case 0:
			raw = fmt.Sprintf(`{"field":"brand","op":"ne","value":"b%03d"}`, rng.IntN(5))
		case 1:
			raw = fmt.Sprintf(`{"field":"title","op":"contains","value":"%s"}`, pick(rng, []string{"red", "cup 1", "pan"}))
		case 2:
			raw = revRangeLeaf(rng, "price", vals)
		}
		q := mustParse(t, raw)
		want := brute(docs, q, sorts)
		size := pick(rng, []int{1, 10, 100, 1000})
		var after []any
		start := 0
		if rng.IntN(2) == 0 && len(want) > 0 {
			start = rng.IntN(len(want))
			after = expectedSort(want[start], sorts)
			start++
		}
		wantIDs := docIDs(want[min(start, len(want)):min(start+size, len(want))])
		track := pick(rng, []int{0, TrackTotalAll, TrackTotalNone, 5})
		var totals []int64
		for _, factor := range []uint64{0, 2, 1 << 40} {
			sortWalkFactor = factor
			got, err := c.search(&Request{Query: q, Sort: sorts, Size: size, SearchAfter: after, TrackTotal: track})
			if err != nil {
				t.Fatal(err)
			}
			if ids := hitIDs(got.Hits); !slices.Equal(ids, wantIDs) {
				t.Fatalf("case %d walk factor %d, %s sort %+v after %v size %d: got %d hits, want %d\n got %q\nwant %q", i, factor, raw, sorts, after, size, len(ids), len(wantIDs), head(ids), head(wantIDs))
			}
			for j, h := range got.Hits {
				if w := expectedSort(want[start+j], sorts); !slices.EqualFunc(h.Sort, w, func(a, b any) bool { return cmpValue(a, b, false) == 0 }) {
					t.Fatalf("case %d: hit %d sort %v, want %v", i, j, h.Sort, w)
				}
			}
			if track == TrackTotalAll && got.Total != int64(len(want)) {
				t.Fatalf("case %d: total %d, want %d", i, got.Total, len(want))
			}
			totals = append(totals, got.Total)
		}
		if track == TrackTotalAll && (totals[0] != totals[1] || totals[1] != totals[2]) {
			t.Fatalf("case %d: totals differ by walk: %v", i, totals)
		}
	}
}

// cases is n, or a fifth of it in short mode.
func cases(n int) int {
	if testing.Short() {
		return n / 5
	}
	return n
}

func head(xs []string) []string { return xs[:min(len(xs), 8)] }

// TestSearchAfterAcrossMerge walks a whole sorted result in pages while the
// shards refresh and merge (with no writes): the walk equals brute force exactly.
func TestSearchAfterAcrossMerge(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x14c3))
	c := revCorpus(t, rng, 2, 4, 2500)
	docs := c.docs()
	defer func() { sortWalkFactor = 2 }()
	for _, sorts := range [][]SortField{
		{{Field: "price"}},
		{{Field: "price", Desc: true}},
		{{Field: "created", Desc: true}, {Field: "brand"}},
		{{Field: "price"}, {Field: "created", Desc: true}},
	} {
		want := docIDs(brute(docs, &query.All{}, sorts))
		var got []string
		var after []any
		for page := 0; ; page++ {
			sortWalkFactor = pick(rng, []uint64{0, 2, 1 << 40})
			resp, err := c.search(&Request{Query: &query.All{}, Sort: sorts, Size: 97, SearchAfter: after})
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, hitIDs(resp.Hits)...)
			if resp.Next == nil {
				break
			}
			after = resp.Next
			if page%7 == 3 {
				c.merge(rng.IntN(2), 1+rng.IntN(2))
			}
		}
		if !slices.Equal(got, want) {
			for i := range min(len(got), len(want)) {
				if got[i] != want[i] {
					t.Fatalf("sort %+v: walk differs at %d: %s vs %s (%d vs %d)", sorts, i, got[i], want[i], len(got), len(want))
				}
			}
			t.Fatalf("sort %+v: %d hits, want %d", sorts, len(got), len(want))
		}
	}
}

func revAgg(rng *rand.Rand, vals []float64) Agg {
	metric := func() Agg {
		if rng.IntN(2) == 0 {
			return Agg{Type: AggStats, Field: pick(rng, []string{"price", "created"})}
		}
		return Agg{Type: AggCardinality, Field: pick(rng, []string{"brand", "tags", "title", "active"})}
	}
	var a Agg
	switch rng.IntN(8) {
	case 0, 1:
		a = Agg{Type: AggTerms, Field: pick(rng, []string{"brand", "tags", "title", "active", "price"}), Size: pick(rng, []int{1, 5, 50}), ShardSize: 1_000_000}
		if rng.IntN(3) == 0 {
			a.MinDocCount = ptr(1 + rng.IntN(20))
		}
	case 2:
		var rs []Range
		for range 1 + rng.IntN(5) {
			r := Range{}
			if rng.IntN(5) > 0 {
				r.From = ptr(revBound(rng, vals))
			}
			if rng.IntN(5) > 0 {
				r.To = ptr(revBound(rng, vals))
			}
			rs = append(rs, r)
		}
		a = Agg{Type: AggRange, Field: "price", Ranges: rs}
	case 3:
		a = Agg{Type: AggHistogram, Field: "price", Interval: pick(rng, []float64{0.5, 1, 3, 7.25, 100}), Offset: pick(rng, []float64{0, 0.25, -3, math.Copysign(0, -1)})}
		if rng.IntN(3) == 0 {
			a.MinDocCount = ptr(1)
		}
	case 4:
		a = Agg{Type: AggDateHistogram, Field: "created", Calendar: pick(rng, calendarUnits[2:]), Offset: pick(rng, []float64{0, 0, 3_600_000, -1})}
		if rng.IntN(2) == 0 {
			a.MinDocCount = ptr(1)
		}
	case 5:
		a = Agg{Type: AggDateHistogram, Field: "created", Interval: pick(rng, []float64{86_400_000, 7 * 86_400_000, 1000 * 86_400_000}), Offset: pick(rng, []float64{0, 1, -1})}
		a.MinDocCount = ptr(1)
	default:
		return metric()
	}
	if rng.IntN(3) == 0 {
		a.Aggs = map[string]Agg{"m": metric()}
	}
	return a
}

func TestAggregationsEveryPath(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x14c4))
	c := revCorpus(t, rng, 2, 5, 3000)
	docs := c.docs()
	vals := revValues(docs, "price")
	defer func() { aggChunkMin, aggChunk = 1<<15, 1<<16 }()
	for i := range cases(300) {
		raw := `{"all":[]}`
		switch rng.IntN(4) {
		case 0:
			raw = fmt.Sprintf(`{"field":"brand","op":"ne","value":"b%03d"}`, rng.IntN(3)) // dense, a few not hit
		case 1:
			raw = fmt.Sprintf(`{"field":"brand","op":"eq","value":"b%03d"}`, rng.IntN(10))
		case 2:
			raw = revRangeLeaf(rng, "price", vals)
		}
		q := mustParse(t, raw)
		aggs := map[string]Agg{"a": revAgg(rng, vals), "b": revAgg(rng, vals)}
		matched := brute(docs, q, nil)
		for _, chunk := range []uint32{0, 1 + rng.Uint32N(3000)} {
			if chunk > 0 {
				aggChunkMin, aggChunk = 1, chunk
			}
			globalOrdsDensity = pick(rng, []uint64{0, 8, 1 << 40})
			got, err := c.search(&Request{Query: q, Aggs: aggs})
			aggChunkMin, aggChunk, globalOrdsDensity = 1<<15, 1<<16, 8
			if err != nil {
				t.Fatalf("%s %+v: %v", raw, aggs, err)
			}
			for name, a := range aggs {
				if d := diffAgg(got.Aggs[name], bruteAgg(t, a, matched), name); d != "" {
					t.Fatalf("case %d chunk %d, %s, agg %+v:\n%s", i, chunk, raw, a, d)
				}
			}
		}
	}
}

// TestOrdinalsAcrossChanges: terms and cardinality stay exact as the segment list
// changes (refresh, delete, merge), with the global-ordinals cache warm in between.
func TestOrdinalsAcrossChanges(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x14c5))
	c := revCorpus(t, rng, 1, 3, 2000)
	check := func(step string) {
		t.Helper()
		docs := c.docs()
		for _, a := range []Agg{
			{Type: AggTerms, Field: "brand", Size: 20, ShardSize: 1_000_000},
			{Type: AggTerms, Field: "tags", Size: 70, ShardSize: 1_000_000, MinDocCount: ptr(2)},
			{Type: AggCardinality, Field: "brand"},
			{Type: AggCardinality, Field: "tags"},
			{Type: AggTerms, Field: "brand", Size: 5, ShardSize: 1_000_000, Aggs: map[string]Agg{"c": {Type: AggCardinality, Field: "tags"}}},
		} {
			for _, raw := range []string{`{"all":[]}`, `{"field":"active","op":"eq","value":true}`} {
				q := mustParse(t, raw)
				got, err := c.search(&Request{Query: q, Aggs: map[string]Agg{"x": a}})
				if err != nil {
					t.Fatal(err)
				}
				if d := diffAgg(got.Aggs["x"], bruteAgg(t, a, brute(docs, q, nil)), "x"); d != "" {
					t.Fatalf("%s: %+v over %s:\n%s", step, a, raw, d)
				}
			}
		}
	}
	check("start")
	for step := range 12 {
		switch step % 4 {
		case 0: // a brand that never existed, and every doc of b000 deleted
			c.upsert(fmt.Sprintf("new%d", step), fmt.Sprintf(`{"brand":"a-new-%d","tags":["zz%d"]}`, step, step))
			for _, d := range c.docs() {
				if v := d.Fields["brand"]; v.Text != nil && *v.Text == "b000" {
					c.remove(d.ID)
				}
			}
		case 1:
			for range 50 {
				c.remove(fmt.Sprintf("r%05d", rng.IntN(6000)))
			}
		case 2:
			for range 300 {
				c.upsert(fmt.Sprintf("r%05d", rng.IntN(6000)), revDoc(rng, step))
			}
		}
		c.refreshAll()
		if step%4 == 3 {
			c.merge(0, 1+rng.IntN(2))
		}
		check(fmt.Sprintf("step %d", step))
	}
}

// TestOrdinalsConcurrent: concurrent first uses and evictions of global ordinals
// (a cache of a few bytes) on a fixed index answer exactly; then readers race a writer
// that refreshes and merges (run with -race).
func TestOrdinalsConcurrent(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x14c6))
	c := revCorpus(t, rng, 2, 3, 2000)
	docs := c.docs()
	globalOrdsCacheBytes, globalOrdsDensity = 4<<10, 1<<40
	defer func() {
		globalOrdsCacheBytes, globalOrdsDensity = 256<<20, 8
		aggChunkMin, aggChunk = 1<<15, 1<<16
	}()
	aggChunkMin, aggChunk = 1, 700
	aggs := []Agg{
		{Type: AggTerms, Field: "brand", Size: 10, ShardSize: 1_000_000},
		{Type: AggTerms, Field: "tags", Size: 10, ShardSize: 1_000_000},
		{Type: AggCardinality, Field: "brand"},
		{Type: AggCardinality, Field: "title"},
		{Type: AggHistogram, Field: "price", Interval: 5, Aggs: map[string]Agg{"c": {Type: AggCardinality, Field: "tags"}}},
	}
	want := make([]*AggResult, len(aggs))
	for i, a := range aggs {
		want[i] = bruteAgg(t, a, docs)
	}
	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for w := range 8 {
		wg.Go(func() {
			for k := range 30 {
				i := (w + k) % len(aggs)
				got, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"x": aggs[i]}})
				if err != nil {
					errs <- err.Error()
					return
				}
				if d := diffAgg(got.Aggs["x"], want[i], "x"); d != "" {
					errs <- d
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	stop := make(chan struct{})
	var mu sync.Mutex
	wg.Go(func() {
		wrng := rand.New(rand.NewPCG(seed, 3))
		for {
			select {
			case <-stop:
				return
			default:
			}
			mu.Lock()
			c.upsert(fmt.Sprintf("r%05d", wrng.IntN(6000)), revDoc(wrng, 1))
			c.refresh(wrng.IntN(2))
			if wrng.IntN(5) == 0 {
				c.merge(wrng.IntN(2), 1)
			}
			mu.Unlock()
		}
	})
	for w := range 6 {
		wg.Go(func() {
			for k := range 40 {
				var parts []*ShardResult
				for _, s := range c.shards {
					g := s.Acquire()
					res, err := ExecuteShard(context.Background(), g, &Request{Query: &query.All{}, Aggs: map[string]Agg{"x": aggs[(w+k)%len(aggs)]}})
					g.Release()
					if err != nil {
						t.Error(err)
						return
					}
					parts = append(parts, res)
				}
				Reduce(parts, &Request{Aggs: map[string]Agg{"x": aggs[(w+k)%len(aggs)]}})
			}
		})
	}
	lowest := int64(0)
	for range 1000 {
		globalOrdsCache.mu.Lock()
		lowest = min(lowest, globalOrdsCache.bytes)
		globalOrdsCache.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	close(stop)
	wg.Wait()
	globalOrdsCache.mu.Lock()
	var held int64
	for el := globalOrdsCache.lru.Front(); el != nil; el = el.Next() {
		held += el.Value.(*ordsEntry).charged
	}
	t.Logf("cache: accounted %d bytes, entries hold %d bytes, %d entries; lowest accounted %d", globalOrdsCache.bytes, held, globalOrdsCache.lru.Len(), lowest)
	if lowest < 0 || globalOrdsCache.bytes != held {
		t.Errorf("cache accounting drifted: accounted %d, held %d, lowest %d", globalOrdsCache.bytes, held, lowest)
	}
	for el := globalOrdsCache.lru.Front(); el != nil; {
		next := el.Next()
		globalOrdsCache.remove(el)
		el = next
	}
	globalOrdsCache.bytes = 0
	globalOrdsCache.mu.Unlock()
}

// TestCardinalityPrecisions: past the exact limit the engine's estimate equals a
// sketch of the brute-force distinct values at every precision, and is within the
// HyperLogLog++ error of the truth.
func TestCardinalityPrecisions(t *testing.T) {
	c := newCluster(t, 2)
	for round := range 3 {
		for i := range 4000 {
			c.upsert(fmt.Sprintf("h%d-%d", round, i), fmt.Sprintf(`{"brand":"v%d","tags":["a%d","b%d"],"price":%d}`, round*3000+i, i, i%777, round*3000+i))
		}
		c.refreshAll()
	}
	for _, field := range []string{"brand", "tags", "price"} {
		seen := map[any]bool{}
		for _, d := range c.docs() {
			for _, v := range aggValues(d, field) {
				seen[v] = true
			}
		}
		for _, p := range []int{4, 8, 12, 14, 16, 18} {
			got, err := c.search(&Request{Query: &query.All{}, Aggs: map[string]Agg{"n": {Type: AggCardinality, Field: field, Precision: p}}})
			if err != nil {
				t.Fatal(err)
			}
			sk := NewSketch(uint8(p))
			for v := range seen {
				switch x := v.(type) {
				case string:
					sk.Add(hashString(x) | 1)
				case float64:
					sk.Add(hashFloat(x))
				}
			}
			est := got.Aggs["n"].Value
			rel := math.Abs(float64(est)-float64(len(seen))) / float64(len(seen))
			t.Logf("%s p=%d: estimate %d of %d (%.2f%%), sketch %d", field, p, est, len(seen), rel*100, sk.Estimate())
			if est != sk.Estimate() {
				t.Errorf("%s p=%d: %d, a sketch of the values says %d", field, p, est, sk.Estimate())
			}
		}
	}
}

// TestTimeoutStopsParts: a timed-out aggregation over a segment collected in
// parts answers (timed out) promptly.
func TestTimeoutStopsParts(t *testing.T) {
	rng := rand.New(rand.NewPCG(9, 9))
	c := revCorpus(t, rng, 1, 2, 4000)
	aggChunkMin, aggChunk = 1, 64
	defer func() { aggChunkMin, aggChunk = 1<<15, 1<<16 }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	g := c.shards[0].Acquire()
	defer g.Release()
	_, err := ExecuteShard(ctx, g, &Request{Query: &query.All{}, Aggs: map[string]Agg{"h": {Type: AggHistogram, Field: "price", Interval: 1, Aggs: map[string]Agg{"s": {Type: AggStats, Field: "price"}}}}})
	t.Logf("cancelled: %v", err)
	res, err := ExecuteShard(context.Background(), g, &Request{Query: &query.All{}, Timeout: time.Nanosecond, Aggs: map[string]Agg{"h": {Type: AggHistogram, Field: "price", Interval: 1, Aggs: map[string]Agg{"s": {Type: AggStats, Field: "price"}}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("timed out: %v", res.TimedOut)
}

// TestSparseOrdinalsBuildNothing: a cardinality whose hits hold few of a field's
// terms (one document of 300k unique values, over 11 segments) merges by term: it
// builds no global ordinals, allocates in proportion to its hits, and answers within a
// short deadline; a dense one builds them once and later requests reuse them.
func TestSparseOrdinalsBuildNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("slow")
	}
	c := newCluster(t, 1)
	n := 0
	for range 10 {
		for range 30_000 {
			c.upsert(fmt.Sprintf("u%07d", n), fmt.Sprintf(`{"brand":"sku-%07d"}`, n))
			n++
		}
		c.refreshAll()
	}
	c.upsert("x", `{"brand":"zzz"}`)
	c.refreshAll()
	sparse := &Request{Query: mustParse(t, `{"field":"brand","op":"eq","value":"zzz"}`), Timeout: 50 * time.Millisecond,
		Aggs: map[string]Agg{"n": {Type: AggCardinality, Field: "brand"}}}
	var ms runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&ms)
	before := ms.TotalAlloc
	start := time.Now()
	got, err := c.search(sparse)
	took := time.Since(start)
	runtime.ReadMemStats(&ms)
	if err != nil {
		t.Fatal(err)
	}
	if got.Aggs["n"].Value != 1 || got.TimedOut {
		t.Fatalf("cardinality %d (timed out %v), want 1", got.Aggs["n"].Value, got.TimedOut)
	}
	if alloc := ms.TotalAlloc - before; alloc > 4<<20 {
		t.Errorf("a one-hit cardinality allocated %.1f MiB", float64(alloc)/(1<<20))
	}
	g := c.shards[0].Acquire()
	key := ordsKey(g, "brand", false)
	g.Release()
	if globalOrdsCache.cached(key) != nil {
		t.Fatal("a one-hit cardinality built global ordinals")
	}
	t.Logf("one hit of %d unique terms: %v", n+1, took)
	dense := &Request{Query: &query.All{}, Aggs: map[string]Agg{"n": {Type: AggCardinality, Field: "brand", Precision: 18}}}
	if _, err := c.search(dense); err != nil {
		t.Fatal(err)
	}
	if globalOrdsCache.cached(key) == nil {
		t.Fatal("a match-all cardinality built no global ordinals")
	}
}

func TestRangeDateStringBounds(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 5))
	c := revCorpus(t, rng, 1, 2, 2000)
	docs := c.docs()
	for _, d := range []int64{revDay(10), revDay(10) - 1, revDay(10) + 1, revDay(200)} {
		s := time.UnixMilli(d).UTC().Format("2006-01-02T15:04:05.000Z07:00")
		for _, op := range []string{"lt", "lte", "gt", "gte"} {
			raw := fmt.Sprintf(`{"field":"created","op":%q,"value":%q}`, op, s)
			n, ps := query.Parse([]byte(raw))
			if len(ps) > 0 {
				t.Logf("%s refused: %s", raw, ps[0].Message)
				continue
			}
			want := docIDs(brute(docs, n, nil))
			for _, f := range []float64{0, 0.5, 1e12} {
				docValuesFactor = f
				got, err := c.search(&Request{Query: n, Size: MaxSize, TrackTotal: TrackTotalAll})
				if err != nil {
					t.Fatal(err)
				}
				if ids := hitIDs(got.Hits); !slices.Equal(ids, want) {
					t.Fatalf("%s factor %v: %d hits, want %d", raw, f, len(ids), len(want))
				}
			}
			docValuesFactor = defaultDocValuesFactor
			t.Logf("%s: %d hits", raw, len(want))
		}
	}
}

// TestShardSizeBoundsManySegments: TestTermsShardSizeErrorBounds over shards of
// several segments (the global-ordinal cut), with deletes, keyword and list fields.
func TestShardSizeBoundsManySegments(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x14c7))
	c := newCluster(t, 3)
	for round := range 4 {
		for i := range 800 {
			b := fmt.Sprintf("b%02d", min(39, int(rng.ExpFloat64()*6)+round))
			c.upsert(fmt.Sprintf("d%04d", rng.IntN(2500)+i%2), fmt.Sprintf(`{"brand":%q,"tags":["%s","x%d"]}`, b, b, rng.IntN(9)))
		}
		for range 60 {
			c.remove(fmt.Sprintf("d%04d", rng.IntN(2500)))
		}
		c.refreshAll()
	}
	for _, field := range []string{"brand", "tags"} {
		for _, raw := range []string{`{"all":[]}`, `{"field":"tags","op":"has","value":"x3"}`} {
			q := mustParse(t, raw)
			truth := map[string]int64{}
			for _, d := range brute(c.docs(), q, nil) {
				for _, v := range aggValues(d, field) {
					truth[v.(string)]++
				}
			}
			var all int64
			for _, n := range truth {
				all += n
			}
			for _, shardSize := range []int{1, 2, 3, 5, 8, 1000} {
				got, err := c.search(&Request{Query: q, Aggs: map[string]Agg{"t": {Type: AggTerms, Field: field, Size: 5, ShardSize: shardSize}}})
				if err != nil {
					t.Fatal(err)
				}
				res := got.Aggs["t"]
				var returned int64
				for _, b := range res.Buckets {
					key, _ := b.Key.(string)
					tc := truth[key]
					if b.DocCount > tc || tc > b.DocCount+b.DocCountErrorUpperBound {
						t.Errorf("%s %s shard_size %d: %s counts %d (+%d), truth %d", field, raw, shardSize, key, b.DocCount, b.DocCountErrorUpperBound, tc)
					}
					returned += b.DocCount
				}
				if res.SumOtherDocCount != all-returned {
					t.Errorf("%s %s shard_size %d: sum_other %d, want %d", field, raw, shardSize, res.SumOtherDocCount, all-returned)
				}
				if shardSize == 1000 && res.DocCountErrorUpperBound != 0 {
					t.Errorf("%s: shard_size past every term, error bound %d", field, res.DocCountErrorUpperBound)
				}
				if len(res.Buckets) > 0 {
					last := res.Buckets[len(res.Buckets)-1].DocCount
					for key, n := range truth {
						if n > last+res.DocCountErrorUpperBound && !slices.ContainsFunc(res.Buckets, func(b *Bucket) bool { return b.Key == key }) {
							t.Errorf("%s shard_size %d: %s (%d) missing past the bound %d+%d", field, shardSize, key, n, last, res.DocCountErrorUpperBound)
						}
					}
				}
			}
		}
	}
}
