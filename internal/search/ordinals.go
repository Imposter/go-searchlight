package search

import (
	"bytes"
	"cmp"
	"container/heap"
	"container/list"
	"context"
	"log/slog"
	"math"
	"math/bits"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// Global ordinals.
//
// A keyword or list field's terms aggregation and cardinality count by ordinal: each
// segment counts its documents per local ordinal (an index into its own sorted
// dictionary) and hands the counts, or the ordinals it saw, to the shard. The shard
// merges them one of two ways:
//
//   - by global ordinal, an index into the sorted union of every segment's
//     dictionary: counts are added up in one array, a terms aggregation's cut (most
//     documents first, then by key) is made on numbers, only the buckets it keeps have
//     their terms read, and a cardinality hashes each term once, from a hash kept per
//     global ordinal;
//   - by term: each segment's ordinals with documents have their terms read and are
//     merged by key.
//
// Global ordinals cost a merge of every segment's whole dictionary, so they are built
// only when the request's ordinals with documents are dense enough among the
// dictionaries' terms (globalOrdsDensity) to pay for it, and kept for later requests in
// an LRU bounded by bytes (Elasticsearch's global ordinals are kept the same way, per
// reader). Segments never change, so an entry is valid for as long as its segment list
// is current. A refresh or merge makes a new list: its ordinals are derived from the
// old list's where that is cheaper than a merge (Incremental global ordinals), built in
// the background for the fields searches used recently (WarmOrdinals), and a build a
// search's deadline cut short goes on and is cached for the next (The cache and its
// builds).

// globalOrdsDensity: the shard builds global ordinals when the ordinals with
// documents, over every segment, are at least 1/globalOrdsDensity of the segments'
// terms. A variable so tests can force either merge.
var globalOrdsDensity uint64 = 8

// globalOrds maps one field's per-segment ordinals onto global ones.
type globalOrds struct {
	// ids are the segment IDs of the list built over, in generation order.
	ids []string
	// segs holds, per segment of the generation, each local ordinal's global one;
	// local and global ordinals ascend together.
	segs [][]uint32
	// dfs holds, per segment, each local ordinal's document frequency.
	dfs [][]uint32
	// hashes is each global ordinal's term hash (never 0), where the term was first
	// found (a segment and its local ordinal), to read it back.
	hashes []uint64
	where  []ordRef
	size   int64
}

type ordRef struct{ seg, ord uint32 }

func (g *globalOrds) local(seg int, gl uint32) (uint32, bool) {
	m := g.segs[seg]
	i, ok := slices.BinarySearch(m, gl)
	return uint32(i), ok //nolint:gosec // a segment holds at most 2^32 terms
}

// loadOrds looks up the cached global ordinals of the fields p's aggregations count by
// ordinal (top-level terms, those under a filter, filters or global aggregation
// included, and every cardinality), building none.
func loadOrds(ctx context.Context, g *shard.Generation, p *prepared) {
	var want func(spec *aggSpec, top bool)
	want = func(spec *aggSpec, top bool) {
		if entries, ok := ordKind(spec.ftype); ok && (spec.typ == AggCardinality || (top && spec.typ == AggTerms)) {
			k := ordField{spec.field, entries}
			if _, seen := p.ords[k]; !seen {
				if p.ords == nil {
					p.ords = map[ordField]*globalOrds{}
				}
				p.ords[k] = globalOrdsCache.cached(ctx, ordsKey(g, spec.field, entries))
			}
		}
		for _, sub := range spec.subs {
			want(sub, spec.wrapper())
		}
	}
	for _, spec := range p.aggs {
		want(spec, true)
	}
}

// ordKind says which dictionary a field's ordinals index: a keyword (or text) column's
// values, or a list's entries.
func ordKind(t schema.FieldType) (entries, ok bool) {
	switch t {
	case schema.Keyword, schema.Text:
		return false, true
	case schema.KeywordList:
		return true, true
	}
	return false, false
}

func ordTermKind(entries bool) segment.TermKind {
	if entries {
		return kindEntry
	}
	return kindValue
}

// numTerms is how many terms segment r's column of field holds.
func numTerms(r *segment.Reader, field string, entries bool) uint32 {
	if entries {
		return r.Entries(field).NumTerms()
	}
	return r.Keywords(field).NumTerms()
}

// ordSource is one segment's dictionary in a k-way merge.
type ordSource struct {
	seg int
	cur *segment.TermCursor
	pos uint32
}

type ordHeap []*ordSource

func (h ordHeap) Len() int { return len(h) }
func (h ordHeap) Less(i, j int) bool {
	if c := bytes.Compare(h[i].cur.Term(), h[j].cur.Term()); c != 0 {
		return c < 0
	}
	return h[i].seg < h[j].seg
}
func (h ordHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *ordHeap) Push(x any)   { *h = append(*h, x.(*ordSource)) } //nolint:forcetypeassert,errcheck // only sources
func (h *ordHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}

// ordsBuildCheck is how many terms a build merges between checks of its context.
const ordsBuildCheck = 4096

// buildGlobalOrds merges the field's dictionaries of every segment of g, reading each
// term in place: nil when ctx ends first.
func buildGlobalOrds(ctx context.Context, g *shard.Generation, field string, entries bool) *globalOrds {
	n := len(g.Segments)
	out := &globalOrds{ids: segmentIDs(g), segs: make([][]uint32, n), dfs: make([][]uint32, n)}
	h := make(ordHeap, 0, n)
	for i := range g.Segments {
		r := g.Segments[i].Reader
		if numTerms(r, field, entries) == 0 {
			continue
		}
		cur := r.Cursor(field, ordTermKind(entries))
		out.segs[i] = make([]uint32, cur.Len())
		out.dfs[i] = make([]uint32, cur.Len())
		out.size += int64(cur.Len()) * 8
		if cur.Next() {
			h = append(h, &ordSource{seg: i, cur: cur})
		}
	}
	heap.Init(&h)
	var last []byte
	for merged := 0; h.Len() > 0; merged++ {
		if merged%ordsBuildCheck == 0 && ctx.Err() != nil {
			return nil
		}
		src := h[0]
		term := src.cur.Term()
		if len(out.hashes) == 0 || !bytes.Equal(term, last) {
			last = append(last[:0], term...)
			out.hashes = append(out.hashes, hashString(term)|1)
			out.where = append(out.where, ordRef{seg: uint32(src.seg), ord: src.pos}) //nolint:gosec // a generation's segment count
		}
		if src.pos < uint32(len(out.segs[src.seg])) { //nolint:gosec // a segment holds at most 2^32 terms
			out.segs[src.seg][src.pos] = uint32(len(out.hashes) - 1) //nolint:gosec // at most 2^32 terms
			out.dfs[src.seg][src.pos] = src.cur.DocFreq()
		}
		src.pos++
		if src.cur.Next() {
			heap.Fix(&h, 0)
		} else {
			heap.Pop(&h)
		}
	}
	out.size += int64(len(out.hashes)) * 16
	return out
}

// segmentIDs lists g's segment IDs in generation order.
func segmentIDs(g *shard.Generation) []string {
	ids := make([]string, len(g.Segments))
	for i := range g.Segments {
		ids[i] = g.Segments[i].ID
	}
	return ids
}

// Incremental global ordinals.
//
// A refresh adds a small segment to the list, and a merge swaps a few segments for
// one; the field's global ordinals over the new list are derived from those over the
// old one rather than merged again from every dictionary (deriveGlobalOrds). The
// derived ordinals are exactly what a full build makes, the same sorted, dense union:
//
//   - a segment both lists hold keeps its local-to-global map, renumbered only when
//     terms are added or dropped (every number shifts by a count, so the map stays
//     ascending);
//   - a removed segment's map is dropped, and the terms it alone held with it: each
//     term's reference (where) moves to a segment still listed, and a term no listed
//     segment holds leaves the dictionary;
//   - an added segment's terms are found in the old dictionary in one ascending pass
//     that gallops from each term's place to the next one's, reading the old
//     dictionary's terms in place: a term found takes that global ordinal, one not found
//     is inserted at its place, and the terms inserted shift the ones after them.
//
// Deriving reads about 2·log2(G/m)+1 of the old dictionary's terms for each of an
// added segment's m terms (G: the old dictionary's), each decoded from its block's
// start, against a full build's heap merge of every segment's whole dictionary:
// worthDeriving picks the cheaper. A refresh's small segment is derived; a merge into
// a segment holding much of the field is built again in full.

// ordsIncrementalFactor weighs a derivation's term reads against a full build's
// terms merged: a derivation is chosen while its estimated reads are at most this
// many times the full build's terms. 0 disables derivations. A variable so tests can
// force either.
var ordsIncrementalFactor = 4.0

// noSeg marks a global ordinal no listed segment holds while a derivation runs.
const noSeg = ^uint32(0)

// worthDeriving reports whether g's ordinals of field are cheaper derived from base
// (over another list sharing segments with g's) than built in full.
func worthDeriving(base *globalOrds, g *shard.Generation, field string, entries bool) bool {
	if ordsIncrementalFactor <= 0 {
		return false
	}
	have := make(map[string]bool, len(base.ids))
	for _, id := range base.ids {
		have[id] = true
	}
	var added, all uint64
	shared := false
	for i := range g.Segments {
		n := uint64(numTerms(g.Segments[i].Reader, field, entries))
		all += n
		if have[g.Segments[i].ID] {
			shared = true
		} else {
			added += n
		}
	}
	if !shared {
		return false
	}
	if added == 0 {
		return true
	}
	reads := float64(added) * (2*math.Log2(float64(len(base.hashes))/float64(added)+1) + 1) * segment.TermsPerBlock / 2
	return reads <= ordsIncrementalFactor*float64(all)
}

// termCol reads one segment's terms of a field by local ordinal.
type termCol struct {
	kc segment.KeywordColumn
	mc segment.MultiColumn
}

func columnOf(r *segment.Reader, field string, entries bool) termCol {
	if entries {
		return termCol{mc: r.Entries(field)}
	}
	return termCol{kc: r.Keywords(field)}
}

func (c termCol) appendTerm(dst []byte, ord uint32) []byte {
	if c.mc.Exists() {
		return c.mc.AppendTerm(dst, ord)
	}
	return c.kc.AppendTerm(dst, ord)
}

// freshTerm is a term a derivation inserts: its place among the old dictionary's
// terms still held (how many of them sort before it), and where it was first found.
type freshTerm struct {
	gap   uint32
	term  string
	first ordRef
}

// gallop returns the least p in [lo, n) for which less(p) is false (n when none):
// an exponential search from lo, then a binary one.
func gallop(lo, n int, less func(p int) bool) int {
	hi, step := lo, 1
	for hi < n && less(hi) {
		lo = hi + 1
		hi = lo + step
		step *= 2
	}
	hi = min(hi, n)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if less(mid) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// deriveGlobalOrds makes g's global ordinals of field from base's, over a segment list
// sharing segments with g's (see Incremental global ordinals): exactly those
// buildGlobalOrds makes, nil when ctx ends first. base is never changed, and the maps
// of the segments both hold are shared when no term is added or dropped.
func deriveGlobalOrds(ctx context.Context, base *globalOrds, g *shard.Generation, field string, entries bool) *globalOrds {
	n := len(g.Segments)
	out := &globalOrds{ids: segmentIDs(g), segs: make([][]uint32, n), dfs: make([][]uint32, n)}
	at := make(map[string]int, n)
	for j, id := range out.ids {
		at[id] = j
	}
	newOf := make([]int, len(base.ids))
	removed := false
	inBase := make(map[string]bool, len(base.ids))
	for i, id := range base.ids {
		inBase[id] = true
		j, ok := at[id]
		if !ok {
			j, removed = -1, true
		}
		newOf[i] = j
	}
	var added []int
	for j, id := range out.ids {
		if !inBase[id] {
			added = append(added, j)
		}
	}

	where := make([]ordRef, len(base.where))
	for gl, w := range base.where {
		if j := newOf[w.seg]; j >= 0 {
			where[gl] = ordRef{seg: uint32(j), ord: w.ord} //nolint:gosec // a generation's segment count
		} else {
			where[gl] = ordRef{seg: noSeg}
		}
	}
	var live []uint32 // the base ordinals some listed segment still holds, when not all are
	if removed {
		for i, m := range base.segs {
			j := newOf[i]
			if j < 0 {
				continue
			}
			for o, gl := range m {
				if where[gl].seg == noSeg {
					where[gl] = ordRef{seg: uint32(j), ord: uint32(o)} //nolint:gosec // a segment holds at most 2^32 terms
				}
			}
		}
		dead := 0
		for _, w := range where {
			if w.seg == noSeg {
				dead++
			}
		}
		if dead > 0 {
			live = make([]uint32, 0, len(where)-dead)
			for gl, w := range where {
				if w.seg != noSeg {
					live = append(live, uint32(gl))
				}
			}
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	nLive := len(where)
	if live != nil {
		nLive = len(live)
	}
	baseOrd := func(p int) uint32 {
		if live == nil {
			return uint32(p) //nolint:gosec // at most 2^32 terms
		}
		return live[p]
	}
	cols := make([]termCol, n)
	for j := range g.Segments {
		cols[j] = columnOf(g.Segments[j].Reader, field, entries)
	}
	var buf []byte
	termOf := func(p int) []byte {
		w := where[baseOrd(p)]
		buf = cols[w.seg].appendTerm(buf[:0], w.ord)
		return buf
	}

	// Each added segment's local ordinals: a base ordinal (>= 0), or -(k+1) for fresh[k].
	var fresh []freshTerm
	locals := make([][]int64, len(added))
	walked := 0
	for a, j := range added {
		r := g.Segments[j].Reader
		if numTerms(r, field, entries) == 0 {
			continue
		}
		cur := r.Cursor(field, ordTermKind(entries))
		local := make([]int64, cur.Len())
		dfs := make([]uint32, cur.Len())
		lo := 0
		for o := 0; o < len(local) && cur.Next(); o++ {
			if walked%ordsBuildCheck == 0 && ctx.Err() != nil {
				return nil
			}
			walked++
			t := cur.Term()
			dfs[o] = cur.DocFreq()
			p := gallop(lo, nLive, func(p int) bool { return bytes.Compare(termOf(p), t) < 0 })
			lo = p
			if p < nLive && bytes.Equal(termOf(p), t) {
				local[o] = int64(baseOrd(p))
				continue
			}
			local[o] = -int64(len(fresh)) - 1
			fresh = append(fresh, freshTerm{gap: uint32(p), term: string(t), first: ordRef{seg: uint32(j), ord: uint32(o)}}) //nolint:gosec // bounded by the dictionaries' sizes
		}
		locals[a], out.dfs[j] = local, dfs
	}

	// The inserted terms in dictionary order, each once: rank is each fresh term's
	// global ordinal, its place plus the inserted terms before it.
	order := make([]int, len(fresh))
	for k := range order {
		order[k] = k
	}
	slices.SortStableFunc(order, func(a, b int) int {
		if fa, fb := &fresh[a], &fresh[b]; fa.gap != fb.gap {
			return cmp.Compare(fa.gap, fb.gap)
		}
		return strings.Compare(fresh[a].term, fresh[b].term)
	})
	rank := make([]uint32, len(fresh))
	uniq := make([]int, 0, len(fresh))
	for i, k := range order {
		if i > 0 {
			if prev := order[i-1]; fresh[k].gap == fresh[prev].gap && fresh[k].term == fresh[prev].term {
				rank[k] = rank[prev]
				continue
			}
		}
		rank[k] = fresh[k].gap + uint32(len(uniq)) //nolint:gosec // at most 2^32 terms
		uniq = append(uniq, k)
	}

	identity := live == nil && len(uniq) == 0
	var remap []uint32
	if identity {
		out.hashes, out.where = base.hashes, where
		for i, m := range base.segs {
			if j := newOf[i]; j >= 0 {
				out.segs[j], out.dfs[j] = m, base.dfs[i]
			}
		}
	} else {
		total := nLive + len(uniq)
		remap = make([]uint32, len(base.hashes))
		out.hashes = make([]uint64, 0, total)
		out.where = make([]ordRef, 0, total)
		emitFresh := func(k int) {
			out.hashes = append(out.hashes, hashString(fresh[k].term)|1)
			out.where = append(out.where, fresh[k].first)
		}
		k := 0
		for p := range nLive {
			for k < len(uniq) && int(fresh[uniq[k]].gap) <= p {
				emitFresh(uniq[k])
				k++
			}
			gl := baseOrd(p)
			remap[gl] = uint32(len(out.hashes)) //nolint:gosec // at most 2^32 terms
			out.hashes = append(out.hashes, base.hashes[gl])
			out.where = append(out.where, where[gl])
		}
		for ; k < len(uniq); k++ {
			emitFresh(uniq[k])
		}
		if ctx.Err() != nil {
			return nil
		}
		for i, m := range base.segs {
			j := newOf[i]
			if j < 0 || m == nil {
				continue
			}
			nm := make([]uint32, len(m))
			for o, gl := range m {
				nm[o] = remap[gl]
			}
			out.segs[j], out.dfs[j] = nm, base.dfs[i]
		}
	}
	for a, j := range added {
		local := locals[a]
		if local == nil {
			continue
		}
		m := make([]uint32, len(local))
		for o, v := range local {
			switch {
			case v < 0:
				m[o] = rank[-v-1]
			case identity:
				m[o] = uint32(v) //nolint:gosec // a base ordinal
			default:
				m[o] = remap[v]
			}
		}
		out.segs[j] = m
	}
	for _, m := range out.segs {
		out.size += int64(len(m)) * 8
	}
	out.size += int64(len(out.hashes)) * 16
	return out
}

// term reads global ordinal gl's term of field from a segment of gen that holds it.
func (g *globalOrds) term(gen *shard.Generation, field string, gl uint32, entries bool) string {
	at := g.where[gl]
	r := gen.Segments[at.seg].Reader
	if entries {
		return r.Entries(field).Term(at.ord)
	}
	return r.Keywords(field).Term(at.ord)
}

// The cache and its builds.
//
// Builds run on their own goroutine, detached from the search that asked (they hold
// their own reference on the generation, and stop only when its shard shuts down),
// once per segment list however many searches ask at the same time: a search waits for
// the build until its deadline, and one cut short leaves the build running, so the
// next search finds it cached. A build derives the ordinals from the cached ones of an
// overlapping list when that is cheaper (see Incremental global ordinals).
//
// After each refresh or merge, WarmOrdinals (shard.Options.Published) starts the
// builds of the fields searches used over the shard within ordsWarmWindow, at most
// ordsWarmers at once across the process: the first search after a refresh finds them
// built or building. A search that waits on a queued warm build lets it start at once.

// globalOrdsCacheBytes bounds the global ordinals kept across searches. A variable so
// tests can make it small.
var globalOrdsCacheBytes int64 = 256 << 20

// ordsWarmWindow is how recently a search must have used a field's global ordinals
// for a refresh or merge to rebuild them in the background.
var ordsWarmWindow = 10 * time.Minute

// ordsWarmers bounds the warm builds running at once, across shards.
var ordsWarmers = make(chan struct{}, max(1, runtime.GOMAXPROCS(0)/4))

// ordsBuilding counts the builds in flight, for tests to wait on.
var ordsBuilding atomic.Int64

// ordsCache is the LRU of global ordinals, by field, kind and segment list.
type ordsCache struct {
	mu    sync.Mutex
	lru   list.List // of *ordsEntry, most recent first
	items map[string]*list.Element
	bytes int64
}

// ordsEntry is one cached build. done closes once ords is set (nil: the build was
// cancelled, and the entry already removed); charged is what the entry adds to the
// cache's bytes, used when a search last used it (Unix nanoseconds), hurried whether a
// search waits on it, started whether its build has begun: all set and read under the
// cache's lock.
type ordsEntry struct {
	key     ordsCacheKey
	gen     uint64 // the generation built over (Generation.Gen)
	warm    bool   // started by WarmOrdinals, not a search
	done    chan struct{}
	ords    *globalOrds
	charged int64
	used    int64
	hurried bool
	started bool
	// urgent closes when a search waits on a warm build: it starts without a slot.
	urgent chan struct{}
	cancel context.CancelFunc
}

// ordsCacheKey names a build: a field's values or entries over one segment list.
type ordsCacheKey struct {
	field    string
	entries  bool
	segments string // the segment IDs, NUL-separated, in generation order
}

var globalOrdsCache = &ordsCache{items: map[string]*list.Element{}}

func ordsKey(g *shard.Generation, field string, entries bool) ordsCacheKey {
	return ordsCacheKey{field: field, entries: entries, segments: strings.Join(segmentIDs(g), "\x00")}
}

func (k ordsCacheKey) id() string {
	kind := "k"
	if k.entries {
		kind = "l"
	}
	return k.field + "\x00" + kind + "\x00" + k.segments
}

// cached returns key's finished build, or nil (none, or one still building).
func (c *ordsCache) cached(ctx context.Context, key ordsCacheKey) *globalOrds {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key.id()]
	if !ok {
		return nil
	}
	e := el.Value.(*ordsEntry) //nolint:forcetypeassert,errcheck // only entries
	select {
	case <-e.done:
		c.lru.MoveToFront(el)
		e.used = time.Now().UnixNano()
		ordsMetrics().lookup(ctx, "hit")
		return e.ords
	default:
		return nil
	}
}

// globalOrdinals returns field's global ordinals over g's segments, from the cache or
// built (once, however many searches ask at the same time; the others wait for it).
// Nil when ctx ends first: the build goes on, and is cached when done.
func globalOrdinals(ctx context.Context, g *shard.Generation, field string, entries bool) *globalOrds {
	key := ordsKey(g, field, entries)
	c := globalOrdsCache
	c.mu.Lock()
	e, result := c.entry(key, g, false, time.Now().UnixNano()) //nolint:contextcheck // the build outlives the search: it runs for its shard's lifetime
	select {
	case <-e.done:
	default:
		e.hurry()
	}
	c.mu.Unlock()
	ordsMetrics().lookup(ctx, result)
	select {
	case <-e.done:
		return e.ords
	case <-ctx.Done():
		return nil
	}
}

// entry returns key's entry, starting its build over g when there is none: with
// whether it was finished (hit), building (shared) or started (miss). used is when a
// search used it (0 for a warm). The caller holds c.mu.
func (c *ordsCache) entry(key ordsCacheKey, g *shard.Generation, warm bool, used int64) (*ordsEntry, string) {
	if el, ok := c.items[key.id()]; ok {
		c.lru.MoveToFront(el)
		e := el.Value.(*ordsEntry) //nolint:forcetypeassert,errcheck // only entries
		e.used = max(e.used, used)
		select {
		case <-e.done:
			return e, "hit"
		default:
			return e, "shared"
		}
	}
	e := &ordsEntry{key: key, gen: g.Gen(), warm: warm, done: make(chan struct{}), used: used, urgent: make(chan struct{})}
	el := c.lru.PushFront(e)
	c.items[key.id()] = el
	c.start(el, e, g)
	return e, "miss"
}

// hurry lets a queued warm build start at once: a search waits on it. The caller holds
// the cache's lock.
func (e *ordsEntry) hurry() {
	if !e.hurried {
		e.hurried = true
		close(e.urgent)
	}
}

// start runs e's build over g on its own goroutine, holding its own reference on g.
// The caller holds c.mu.
func (c *ordsCache) start(el *list.Element, e *ordsEntry, g *shard.Generation) {
	if !g.Retain() {
		c.remove(el)
		close(e.done)
		return
	}
	ctx, cancel := context.WithCancel(g.Context())
	e.cancel = cancel
	ordsBuilding.Add(1)
	go func() {
		var ords *globalOrds
		defer func() {
			if r := recover(); r != nil {
				slog.Default().Error("search: recovered a panic building global ordinals", slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
				ords = nil
			}
			cancel()
			c.finish(el, e, ords)
			g.Release()
			ordsBuilding.Add(-1)
		}()
		if e.warm {
			select {
			case ordsWarmers <- struct{}{}:
				defer func() { <-ordsWarmers }()
			case <-e.urgent:
			case <-ctx.Done():
				return
			}
		}
		c.mu.Lock()
		e.started = true
		c.mu.Unlock()
		ords = c.build(ctx, e, g)
	}()
}

// build makes e's ordinals over g: derived from the cached build that shares the most
// segments with g's list when worthDeriving, else in full. Nil when ctx ends first.
func (c *ordsCache) build(ctx context.Context, e *ordsEntry, g *shard.Generation) *globalOrds {
	base := c.baseFor(e.key)
	kind, trigger := "full", "search"
	if e.warm {
		trigger = "warm"
	}
	start := time.Now()
	var ords *globalOrds
	if base != nil && worthDeriving(base, g, e.key.field, e.key.entries) {
		kind = "incremental"
		ords = deriveGlobalOrds(ctx, base, g, e.key.field, e.key.entries)
	} else {
		ords = buildGlobalOrds(ctx, g, e.key.field, e.key.entries)
	}
	ordsMetrics().build(ctx, kind, trigger, ords != nil, time.Since(start))
	return ords
}

// baseFor returns the finished build of key's field and kind whose segment list shares
// the most segments with key's, or nil.
func (c *ordsCache) baseFor(key ordsCacheKey) *globalOrds {
	segs := map[string]bool{}
	for _, id := range strings.Split(key.segments, "\x00") {
		segs[id] = true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var best *globalOrds
	bestShared := 0
	for el := c.lru.Front(); el != nil; el = el.Next() {
		e := el.Value.(*ordsEntry) //nolint:forcetypeassert,errcheck // only entries
		if e.ords == nil || e.key.field != key.field || e.key.entries != key.entries {
			continue
		}
		shared := 0
		for _, id := range e.ords.ids {
			if segs[id] {
				shared++
			}
		}
		if shared > bestShared {
			best, bestShared = e.ords, shared
		}
	}
	return best
}

// finish records e's build and closes done. A finished build is cached unless its
// entry went meanwhile, or a build over a later generation of the same shard (a list
// sharing segments, of a higher Gen) is cached already: then it serves only the
// searches that waited for it. Caching it drops the shard's earlier lists' builds.
func (c *ordsCache) finish(el *list.Element, e *ordsEntry, ords *globalOrds) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e.ords = ords
	switch {
	case c.items[e.key.id()] != el:
	case ords == nil || c.supersede(e, el):
		c.remove(el)
	default:
		e.charged = ords.size
		c.bytes += e.charged
		for c.bytes > globalOrdsCacheBytes && c.lru.Len() > 1 {
			c.remove(c.lru.Back())
		}
	}
	close(e.done)
}

// supersede removes the finished entries of e's field and kind over an earlier
// generation of the same shard (a segment list sharing a segment with e's, of a lower
// Gen), which e's list replaced, and inherits the latest use among them: or, when one
// is of a later generation, removes none and reports that e is stale. The caller holds
// c.mu.
func (c *ordsCache) supersede(e *ordsEntry, keep *list.Element) bool {
	mine := map[string]bool{}
	for _, id := range e.ords.ids {
		mine[id] = true
	}
	var stale []*list.Element
	for el := c.lru.Front(); el != nil; el = el.Next() {
		o := el.Value.(*ordsEntry) //nolint:forcetypeassert,errcheck // only entries
		if el == keep || o.ords == nil || o.key.field != e.key.field || o.key.entries != e.key.entries ||
			!slices.ContainsFunc(o.ords.ids, func(id string) bool { return mine[id] }) {
			continue
		}
		if o.gen > e.gen {
			return true
		}
		stale = append(stale, el)
	}
	for _, el := range stale {
		o := el.Value.(*ordsEntry) //nolint:forcetypeassert,errcheck // only entries
		e.used = max(e.used, o.used)
		c.remove(el)
	}
	return false
}

// remove drops el; the caller holds c.mu.
func (c *ordsCache) remove(el *list.Element) {
	e := el.Value.(*ordsEntry) //nolint:forcetypeassert,errcheck // only entries
	c.lru.Remove(el)
	delete(c.items, e.key.id())
	c.bytes -= e.charged
	e.charged = 0
}

// WarmOrdinals starts background builds, over g's segment list, of the global
// ordinals searches used over g's shard (the cached builds over a list sharing a
// segment with g's) within ordsWarmWindow, so the first search after a refresh or
// merge finds them built or building. A warm build still queued for an earlier list
// of the same shard is cancelled; one already running finishes, and the next warm
// derives from it. It returns at once: install it as shard.Options.Published.
func WarmOrdinals(g *shard.Generation) {
	if g == nil || len(g.Segments) == 0 {
		return
	}
	segs := make(map[string]bool, len(g.Segments))
	for i := range g.Segments {
		segs[g.Segments[i].ID] = true
	}
	overlaps := func(ids []string) bool { return slices.ContainsFunc(ids, func(id string) bool { return segs[id] }) }
	type fieldKind struct {
		field   string
		entries bool
	}
	since := time.Now().Add(-ordsWarmWindow).UnixNano()
	c := globalOrdsCache
	c.mu.Lock()
	defer c.mu.Unlock()
	want := map[fieldKind]int64{}
	for el := c.lru.Front(); el != nil; el = el.Next() {
		e := el.Value.(*ordsEntry) //nolint:forcetypeassert,errcheck // only entries
		if e.ords != nil && e.used >= since && overlaps(e.ords.ids) {
			k := fieldKind{e.key.field, e.key.entries}
			want[k] = max(want[k], e.used)
		}
	}
	for k, used := range want {
		key := ordsKey(g, k.field, k.entries)
		if _, ok := c.items[key.id()]; ok {
			continue
		}
		for el := c.lru.Front(); el != nil; el = el.Next() {
			e := el.Value.(*ordsEntry) //nolint:forcetypeassert,errcheck // only entries
			if e.warm && !e.hurried && !e.started && e.gen < g.Gen() && e.key.field == k.field && e.key.entries == k.entries &&
				overlaps(strings.Split(e.key.segments, "\x00")) {
				e.cancel()
			}
		}
		c.entry(key, g, true, used)
	}
}

// ordsInstruments are the global ordinals metrics.
type ordsInstruments struct {
	builds   metric.Int64Counter
	duration metric.Float64Histogram
	lookups  metric.Int64Counter
}

func (in *ordsInstruments) lookup(ctx context.Context, result string) {
	in.lookups.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(attribute.String("result", result))))
}

func (in *ordsInstruments) build(ctx context.Context, kind, trigger string, done bool, d time.Duration) {
	result := "done"
	if !done {
		result = "cancelled"
	}
	ctx = context.WithoutCancel(ctx)
	in.builds.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("kind", kind), attribute.String("trigger", trigger), attribute.String("result", result))))
	in.duration.Record(ctx, d.Seconds(), metric.WithAttributeSet(attribute.NewSet(
		attribute.String("kind", kind), attribute.String("trigger", trigger))))
}

// ordsMetrics come from the global meter provider, as the search metrics do.
var ordsMetrics = sync.OnceValue(func() *ordsInstruments {
	in := telemetry.NewInstruments(otel.Meter(telemetry.ScopeName))
	oi := &ordsInstruments{
		builds:   in.Counter(telemetry.MetricSearchOrdinalsBuilds),
		duration: in.Histogram(telemetry.MetricSearchOrdinalsDuration),
		lookups:  in.Counter(telemetry.MetricSearchOrdinalsLookups),
	}
	if err := in.Err(); err != nil {
		slog.Error("global ordinals metrics unavailable", slog.Any("error", err))
	}
	return oi
})

// ordPartial is one segment's ordinal-keyed partial of a terms aggregation (counts and
// sub-aggregation collectors by local ordinal) or a cardinality (the local ordinals
// seen), which the shard merges by global ordinal or by term.
type ordPartial struct {
	seg    int
	counts []int64
	subs   [][]collector
	seen   []uint64
}

// used is how many of the partial's local ordinals have documents.
func (p *ordPartial) used() uint64 {
	var n uint64
	if p.seen != nil {
		for _, w := range p.seen {
			n += uint64(bits.OnesCount64(w)) //nolint:gosec // at most 64
		}
		return n
	}
	for _, c := range p.counts {
		if c > 0 {
			n++
		}
	}
	return n
}

// finishOrds merges the segments' ordinal partials of spec into dst: by global
// ordinal when the ordinals cached at prepare, or built now because the partials are
// dense enough, are there; by term otherwise. When ctx ends while the build runs, the
// shard's partial of spec is left empty and the search cut short (prepared.cut): the
// build goes on, for the next search.
func finishOrds(ctx context.Context, dst *AggPartial, parts []*ordPartial, spec *aggSpec, g *shard.Generation, p *prepared) {
	entries, _ := ordKind(spec.ftype)
	ords := p.ords[ordField{spec.field, entries}]
	if ords == nil {
		var used, total uint64
		for _, part := range parts {
			used += part.used()
		}
		for i := range g.Segments {
			total += uint64(numTerms(g.Segments[i].Reader, spec.field, entries))
		}
		if used*globalOrdsDensity >= total {
			ords = globalOrdinals(ctx, g, spec.field, entries)
			if ords == nil && ctx.Err() != nil {
				p.cut.Store(true)
				mergePartial(dst, emptyPartial(spec), spec)
				return
			}
		}
	}
	switch {
	case ords != nil && spec.typ == AggTerms:
		finishTerms(dst, parts, spec, g, ords, entries)
	case ords != nil:
		finishCardinality(dst, parts, spec, ords)
	case spec.typ == AggTerms:
		finishTermsByKey(dst, parts, spec, g, entries)
	default:
		finishCardinalityByKey(dst, parts, spec, g, entries)
	}
}

// finishTerms merges the segments' ordinal partials of a terms aggregation into dst:
// counts added up by global ordinal, then the shard's cut to shard_size (most
// documents first, then by key), whose terms alone are read.
func finishTerms(dst *AggPartial, parts []*ordPartial, spec *aggSpec, g *shard.Generation, ords *globalOrds, entries bool) {
	counts := make([]int64, len(ords.hashes))
	for _, p := range parts {
		m := ords.segs[p.seg]
		for o, n := range p.counts {
			if n > 0 {
				counts[m[o]] += n
			}
		}
	}
	kept := make([]uint32, 0, 64)
	for gl, n := range counts {
		if n > 0 {
			kept = append(kept, uint32(gl))
		}
	}
	slices.SortFunc(kept, func(a, b uint32) int {
		if counts[a] != counts[b] {
			if counts[a] > counts[b] {
				return -1
			}
			return 1
		}
		return int(a) - int(b)
	})
	out := &AggPartial{Type: AggTerms}
	if len(kept) > spec.shardSize {
		for _, gl := range kept[spec.shardSize:] {
			out.OtherDocCount += counts[gl]
		}
		kept = kept[:spec.shardSize]
		out.DocCountError = counts[kept[len(kept)-1]]
	}
	for _, gl := range kept {
		b := &BucketPartial{Key: ords.term(g, spec.field, gl, entries), DocCount: counts[gl]}
		if len(spec.subs) > 0 {
			b.Aggs = emptySubs(spec)
			for _, p := range parts {
				if p.subs == nil {
					continue
				}
				if o, ok := ords.local(p.seg, gl); ok && p.subs[o] != nil {
					mergeSubs(b.Aggs, spec, p.subs[o])
				}
			}
		}
		out.Buckets = append(out.Buckets, b)
	}
	mergePartial(dst, out, spec)
}

// finishTermsByKey merges the segments' ordinal partials of a terms aggregation into
// dst by term: each segment's ordinals with documents read and merged by key; the
// shard's cut is cutShard's.
func finishTermsByKey(dst *AggPartial, parts []*ordPartial, spec *aggSpec, g *shard.Generation, entries bool) {
	byKey := map[string]*BucketPartial{}
	out := &AggPartial{Type: AggTerms}
	for _, p := range parts {
		var ords []uint32
		for o, n := range p.counts {
			if n > 0 {
				ords = append(ords, uint32(o))
			}
		}
		for i, term := range segmentTerms(g.Segments[p.seg].Reader, spec.field, entries, ords) {
			o := ords[i]
			b := byKey[term]
			if b == nil {
				b = &BucketPartial{Key: term}
				if len(spec.subs) > 0 {
					b.Aggs = emptySubs(spec)
				}
				byKey[term] = b
				out.Buckets = append(out.Buckets, b)
			}
			b.DocCount += p.counts[o]
			if len(p.subs) > int(o) && p.subs[o] != nil {
				mergeSubs(b.Aggs, spec, p.subs[o])
			}
		}
	}
	mergePartial(dst, out, spec)
}

// finishCardinality merges the segments' seen ordinals into dst's sketch: each global
// ordinal seen anywhere hashed once, from the kept hashes.
func finishCardinality(dst *AggPartial, parts []*ordPartial, spec *aggSpec, ords *globalOrds) {
	seen := make([]uint64, (len(ords.hashes)+63)/64)
	for _, p := range parts {
		m := ords.segs[p.seg]
		for w, word := range p.seen {
			for word != 0 {
				o := w*64 + bits.TrailingZeros64(word)
				word &= word - 1
				gl := m[o]
				seen[gl/64] |= 1 << (gl % 64)
			}
		}
	}
	hashes := make([]uint64, 0, 64)
	for w, word := range seen {
		for word != 0 {
			gl := w*64 + bits.TrailingZeros64(word)
			word &= word - 1
			hashes = append(hashes, ords.hashes[gl])
		}
	}
	slices.Sort(hashes)
	mergePartial(dst, &AggPartial{Type: AggCardinality, Sketch: sketchOf(spec.precision, slices.Compact(hashes))}, spec)
}

// finishCardinalityByKey merges the segments' seen ordinals into dst's sketch by
// term: each segment's seen terms read and hashed.
func finishCardinalityByKey(dst *AggPartial, parts []*ordPartial, spec *aggSpec, g *shard.Generation, entries bool) {
	var hashes []uint64
	for _, p := range parts {
		var ords []uint32
		for w, word := range p.seen {
			for word != 0 {
				ords = append(ords, uint32(w*64+bits.TrailingZeros64(word))) //nolint:gosec // a segment holds at most 2^32 terms
				word &= word - 1
			}
		}
		for _, term := range segmentTerms(g.Segments[p.seg].Reader, spec.field, entries, ords) {
			hashes = append(hashes, hashString(term)|1)
		}
	}
	slices.Sort(hashes)
	mergePartial(dst, &AggPartial{Type: AggCardinality, Sketch: sketchOf(spec.precision, slices.Compact(hashes))}, spec)
}

// segmentTerms reads the terms of ords (ascending local ordinals) of field in r: one
// by one when they are few, else in one walk of the dictionary.
func segmentTerms(r *segment.Reader, field string, entries bool, ords []uint32) []string {
	out := make([]string, 0, len(ords))
	if len(ords) == 0 {
		return out
	}
	if uint64(len(ords))*segment.TermsPerBlock < uint64(numTerms(r, field, entries)) {
		for _, o := range ords {
			if entries {
				out = append(out, r.Entries(field).Term(o))
			} else {
				out = append(out, r.Keywords(field).Term(o))
			}
		}
		return out
	}
	cur := r.Cursor(field, ordTermKind(entries))
	k := 0
	for o := uint32(0); k < len(ords) && cur.Next(); o++ {
		if o == ords[k] {
			out = append(out, string(cur.Term()))
			k++
		}
	}
	return out
}

func emptySubs(spec *aggSpec) map[string]*AggPartial {
	out := make(map[string]*AggPartial, len(spec.subs))
	for _, sub := range spec.subs {
		out[sub.name] = emptyPartial(sub)
	}
	return out
}

func mergeSubs(dst map[string]*AggPartial, spec *aggSpec, subs []collector) {
	for i, sub := range spec.subs {
		mergePartial(dst[sub.name], subs[i].partial(), sub)
	}
}

func emptyPartial(spec *aggSpec) *AggPartial {
	p := &AggPartial{Type: spec.typ}
	switch spec.typ {
	case AggStats:
		p.Stats = &StatsPartial{}
	case AggCardinality:
		p.Sketch = NewSketch(spec.precision)
	}
	return p
}
