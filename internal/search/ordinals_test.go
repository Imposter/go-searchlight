package search

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/testtier"
)

// clearOrdsCache empties the global ordinals cache once no build runs.
func clearOrdsCache(t testing.TB) {
	t.Helper()
	waitOrdsIdle(t)
	c := globalOrdsCache
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.lru.Front(); el != nil; {
		next := el.Next()
		c.remove(el)
		el = next
	}
}

// ordsDiff compares derived global ordinals with a full build over the same
// generation: the same dictionary (each ordinal's hash and term), maps and document
// frequencies.
func ordsDiff(g *shard.Generation, field string, entries bool, got, want *globalOrds) string {
	if !slices.Equal(got.ids, want.ids) {
		return fmt.Sprintf("ids %v, want %v", got.ids, want.ids)
	}
	if len(got.hashes) != len(want.hashes) {
		return fmt.Sprintf("%d global ordinals, want %d", len(got.hashes), len(want.hashes))
	}
	for gl := range want.hashes {
		if got.hashes[gl] != want.hashes[gl] {
			return fmt.Sprintf("ordinal %d: hash %x, want %x", gl, got.hashes[gl], want.hashes[gl])
		}
		if a, b := got.term(g, field, uint32(gl), entries), want.term(g, field, uint32(gl), entries); a != b {
			return fmt.Sprintf("ordinal %d: term %q, want %q", gl, a, b)
		}
	}
	for i := range want.segs {
		if !slices.Equal(got.segs[i], want.segs[i]) {
			return fmt.Sprintf("segment %d: map %v, want %v", i, got.segs[i], want.segs[i])
		}
		if !slices.Equal(got.dfs[i], want.dfs[i]) {
			return fmt.Sprintf("segment %d: document frequencies %v, want %v", i, got.dfs[i], want.dfs[i])
		}
	}
	return ""
}

// ordDoc is a document whose keyword, list and text values come from a vocabulary
// that grows with step, so each refresh brings new terms and old ones.
func ordDoc(rng *rand.Rand, step int) string {
	vocab := 20 + step*15
	word := func() string { return "w" + strconv.Itoa(rng.IntN(vocab)) }
	body := map[string]string{}
	if rng.IntN(8) > 0 {
		body["brand"] = strconv.Quote(word())
	}
	if rng.IntN(4) > 0 {
		tags := make([]string, rng.IntN(4))
		for i := range tags {
			tags[i] = strconv.Quote(word())
		}
		body["tags"] = "[" + join(tags) + "]"
	}
	if rng.IntN(3) > 0 {
		body["title"] = strconv.Quote(word() + " " + word())
	}
	parts := make([]string, 0, len(body))
	for _, k := range slices.Sorted(maps.Keys(body)) {
		parts = append(parts, strconv.Quote(k)+":"+body[k])
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// TestOrdinalsIncrementalMatchesFull: over random sequences of refreshes (added
// segments), overwrites and deletes (segments that lose terms), merges (segments
// removed and one added) and segments emptied, the global ordinals derived from any
// earlier list's equal a full build's, for keyword, list and text fields, and the
// terms and cardinality aggregations they answer with derivations forced equal the
// brute-force ones.
func TestOrdinalsIncrementalMatchesFull(t *testing.T) {
	seed := testSeed(t)
	rng := rand.New(rand.NewPCG(seed, seed^0x0d5))
	clearOrdsCache(t)
	defer func(factor float64, density uint64) {
		ordsIncrementalFactor, globalOrdsDensity = factor, density
		clearOrdsCache(t)
	}(ordsIncrementalFactor, globalOrdsDensity)
	steps := testtier.Pick(25, 60)
	if longMode() {
		steps = 300
	}
	c := newCluster(t, 1)
	s := c.shards[0]
	fields := []struct {
		name    string
		entries bool
	}{{"brand", false}, {"tags", true}, {"title", false}}
	type snapshot struct {
		ords map[string]*globalOrds
	}
	var history []snapshot
	ids := 0
	for step := range steps {
		switch op := rng.IntN(10); {
		case op < 6 || step < 2:
			for range 1 + rng.IntN(60) {
				c.upsert(fmt.Sprintf("d%04d", rng.IntN(ids+40)), ordDoc(rng, step))
				ids++
			}
			for range rng.IntN(10) {
				c.remove(fmt.Sprintf("d%04d", rng.IntN(ids+1)))
			}
			c.refreshAll()
		case op < 8:
			c.merge(0, 1+rng.IntN(3))
		default:
			all := rng.IntN(2) == 0 // empty the index now and then, or most of it
			for _, d := range c.docs() {
				if all || rng.IntN(3) > 0 {
					c.remove(d.ID)
				}
			}
			c.refreshAll()
		}
		g := s.Acquire()
		snap := snapshot{ords: map[string]*globalOrds{}}
		for _, f := range fields {
			full := buildGlobalOrds(context.Background(), g, f.name, f.entries)
			snap.ords[f.name] = full
			for _, k := range []int{len(history) - 1, rng.IntN(len(history) + 1)} {
				if k < 0 || k >= len(history) {
					continue
				}
				got := deriveGlobalOrds(context.Background(), history[k].ords[f.name], g, f.name, f.entries)
				if d := ordsDiff(g, f.name, f.entries, got, full); d != "" {
					g.Release()
					t.Fatalf("step %d, %s derived from step %d: %s", step, f.name, k, d)
				}
			}
		}
		g.Release()
		history = append(history, snap)
	}

	// The aggregations, with every build derived where any base is cached, after each
	// of a run of refreshes warmed by WarmOrdinals.
	ordsIncrementalFactor, globalOrdsDensity = 1e18, 1<<40
	aggs := map[string]Agg{
		"brands": {Type: AggTerms, Field: "brand", Size: 1000, ShardSize: 1_000_000},
		"tags":   {Type: AggTerms, Field: "tags", Size: 1000, ShardSize: 1_000_000},
		"nb":     {Type: AggCardinality, Field: "brand"},
		"nt":     {Type: AggCardinality, Field: "tags"},
		"nw":     {Type: AggCardinality, Field: "title"},
	}
	for step := range testtier.Pick(8, 20) {
		for range 1 + rng.IntN(40) {
			c.upsert(fmt.Sprintf("d%04d", rng.IntN(ids+40)), ordDoc(rng, steps+step))
			ids++
		}
		c.refreshAll()
		if step%5 == 4 {
			c.merge(0, 2)
		}
		g := s.Acquire()
		WarmOrdinals(g)
		g.Release()
		got, err := c.search(&Request{Query: &query.All{}, Aggs: aggs})
		if err != nil {
			t.Fatal(err)
		}
		docs := c.docs()
		for name, a := range aggs {
			if d := diffAgg(got.Aggs[name], bruteAgg(t, a, docs), name); d != "" {
				t.Fatalf("step %d: %s", step, d)
			}
		}
	}
}

// TestOrdinalsBuildOutlivesDeadline: a build the asking search's deadline cuts short
// goes on, detached, and is cached for the next search; searches asking together
// share one build.
func TestOrdinalsBuildOutlivesDeadline(t *testing.T) {
	clearOrdsCache(t)
	defer clearOrdsCache(t)
	c := newCluster(t, 1)
	for round := range 3 {
		for i := range 3000 {
			c.upsert(fmt.Sprintf("o%d-%d", round, i), fmt.Sprintf(`{"brand":"v%d","tags":["a%d","b%d"]}`, round*3000+i, i, i%97))
		}
		c.refreshAll()
	}
	g := c.shards[0].Acquire()
	defer g.Release()
	expired, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if ords := globalOrdinals(expired, g, "brand", false); ords != nil {
		t.Fatal("a search past its deadline got global ordinals it cannot have waited for")
	}
	waitOrdsIdle(t)
	got := globalOrdsCache.cached(context.Background(), ordsKey(g, "brand", false))
	if got == nil {
		t.Fatal("the build cut short by the search's deadline was not cached")
	}
	if d := ordsDiff(g, "brand", false, got, buildGlobalOrds(context.Background(), g, "brand", false)); d != "" {
		t.Fatal(d)
	}

	clearOrdsCache(t)
	results := make(chan *globalOrds, 8)
	for range cap(results) {
		go func() { results <- globalOrdinals(context.Background(), g, "tags", true) }()
	}
	first := <-results
	for range cap(results) - 1 {
		if o := <-results; o != first {
			t.Fatal("searches asking together got different builds")
		}
	}

	// A search over the cached ordinals, then one whose segments use them.
	defer func(d uint64) { globalOrdsDensity = d }(globalOrdsDensity)
	globalOrdsDensity = 1 << 40
	res, err := ExecuteShard(context.Background(), g, &Request{Query: &query.All{}, Aggs: map[string]Agg{"n": {Type: AggCardinality, Field: "tags"}}})
	if err != nil || res.TimedOut {
		t.Fatalf("search: %v, timed out %v", err, res != nil && res.TimedOut)
	}
}

// TestOrdinalsWarm: after a refresh, the fields searches used are built over the new
// list in the background (derived from the old); fields not used recently are not.
func TestOrdinalsWarm(t *testing.T) {
	clearOrdsCache(t)
	defer clearOrdsCache(t)
	defer func(w time.Duration) { ordsWarmWindow = w }(ordsWarmWindow)
	c := newCluster(t, 1)
	s := c.shards[0]
	for i := range 2000 {
		c.upsert(fmt.Sprintf("w%d", i), fmt.Sprintf(`{"brand":"v%d","tags":["t%d"]}`, i, i%50))
	}
	c.refreshAll()
	g := s.Acquire()
	if globalOrdinals(context.Background(), g, "brand", false) == nil || globalOrdinals(context.Background(), g, "tags", true) == nil {
		t.Fatal("no build")
	}
	g.Release()
	globalOrdsCache.mu.Lock()
	el := globalOrdsCache.items[ordsKey(g, "tags", true).id()]
	el.Value.(*ordsEntry).used = time.Now().Add(-time.Hour).UnixNano() //nolint:forcetypeassert,errcheck // only entries
	globalOrdsCache.mu.Unlock()

	for i := range 50 {
		c.upsert(fmt.Sprintf("x%d", i), fmt.Sprintf(`{"brand":"n%d","tags":["n%d"]}`, i, i))
	}
	c.refreshAll()
	g = s.Acquire()
	defer g.Release()
	WarmOrdinals(g)
	waitOrdsIdle(t)
	if globalOrdsCache.cached(context.Background(), ordsKey(g, "brand", false)) == nil {
		t.Error("the field a search used was not warmed")
	}
	if globalOrdsCache.cached(context.Background(), ordsKey(g, "tags", true)) != nil {
		t.Error("a field unused for an hour was warmed")
	}
}

// bulkLoad applies docs to s in batches, refreshing every perSegment of them.
func bulkLoad(t testing.TB, s *shard.Shard, n, perSegment int, body func(i int) string) {
	t.Helper()
	var batch []shard.Change
	flush := func() {
		if err := s.Apply(context.Background(), batch); err != nil {
			t.Fatal(err)
		}
		batch = batch[:0]
	}
	for i := range n {
		d, _, err := schema.Analyze(testMapping, fmt.Sprintf("b%07d", i), []byte(body(i)))
		if err != nil {
			t.Fatal(err)
		}
		batch = append(batch, shard.Change{Seq: int64(i + 1), Kind: shard.Upsert, Doc: &d})
		if len(batch) == 1000 {
			flush()
		}
		if (i+1)%perSegment == 0 {
			flush()
			if err := s.Refresh(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
	}
	flush()
	if err := s.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// BenchmarkOrdinalsAfterRefresh: a field's global ordinals over a shard of a few
// large segments and one refresh's small segment, built in full or derived from the
// list before the refresh.
func BenchmarkOrdinalsAfterRefresh(b *testing.B) {
	c := newCluster(b, 1)
	s := c.shards[0]
	const docs = 300_000
	bulkLoad(b, s, docs, docs/5, func(i int) string {
		return fmt.Sprintf(`{"brand":"b%07d","tags":["t%07d","u%06d"]}`, i, i, i/3)
	})
	g := s.Acquire()
	base := buildGlobalOrds(context.Background(), g, "brand", false)
	baseTags := buildGlobalOrds(context.Background(), g, "tags", true)
	g.Release()
	var batch []shard.Change
	for i := range 2000 {
		d, _, err := schema.Analyze(testMapping, fmt.Sprintf("r%05d", i), fmt.Appendf(nil, `{"brand":"b%07d","tags":["n%05d"]}`, i*150, i))
		if err != nil {
			b.Fatal(err)
		}
		batch = append(batch, shard.Change{Seq: int64(docs + 1 + i), Kind: shard.Upsert, Doc: &d})
	}
	if err := s.Apply(context.Background(), batch); err != nil {
		b.Fatal(err)
	}
	if err := s.Refresh(context.Background()); err != nil {
		b.Fatal(err)
	}
	g = s.Acquire()
	defer g.Release()
	for _, f := range []struct {
		name    string
		entries bool
		base    *globalOrds
	}{{"brand", false, base}, {"tags", true, baseTags}} {
		b.Run(f.name+"/full", func(b *testing.B) {
			for b.Loop() {
				buildGlobalOrds(context.Background(), g, f.name, f.entries)
			}
		})
		b.Run(f.name+"/incremental", func(b *testing.B) {
			for b.Loop() {
				deriveGlobalOrds(context.Background(), f.base, g, f.name, f.entries)
			}
		})
	}
}
