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
// The cache keeps, per shard and field, a line: the latest build cached, the build in
// flight, and when a search last used the field's ordinals over the shard. A line has
// at most one build in flight. A build over a newer generation supersedes the line's
// older one: a build still queued is cancelled, and one already running finishes and
// is cached first, so the newer derives from it (see Incremental global ordinals). A
// build over a generation older than the line's newest is refused (its searches merge
// by term).
//
// Builds run on their own goroutine, detached from the search that asked (they hold
// their own reference on the generation, which the shard's shutdown cancels and waits
// for), once however many searches ask at the same time, and every one of them, a
// search's or a warm's, takes a slot of ordsBuilders first: at most that many run at
// once across the process, and a search's build is served before warm builds queued
// earlier. A search never waits on a build still queued, and on a running one only
// briefly (globalOrdinals): it merges by term meanwhile, exactly; the build goes on,
// and the next search finds it cached. A shard's shutdown cancels its builds and
// purges its lines.
//
// After each refresh or merge, WarmOrdinals (shard.Options.Published) starts the
// builds of the fields searches used over the shard within ordsWarmWindow, so the
// first search after a refresh finds them built or building.

// globalOrdsCacheBytes bounds the global ordinals kept across searches. A variable so
// tests can make it small.
var globalOrdsCacheBytes int64 = 256 << 20

// ordsWarmWindow is how recently a search must have used a field's global ordinals
// for a refresh or merge to rebuild them in the background.
var ordsWarmWindow = 10 * time.Minute

// ordsBuilders bounds the builds running at once, across shards: a quarter of the
// CPUs, at least one.
var ordsBuilders = newBuildPool(max(1, runtime.GOMAXPROCS(0)/4))

// ordsStartGrace is how long a search waits for a build to take its slot: one not
// running by then is left to the background, and the search merges by term.
var ordsStartGrace = 5 * time.Millisecond

// ordsRunningWait bounds how long a search waits for a running build before it
// merges by term: about what the merge by term of a dense field over a large shard
// costs, so waiting longer never pays. A variable so tests can change it.
var ordsRunningWait = 250 * time.Millisecond

// buildPool is a counting semaphore whose waiters are served searches first: a
// search's build (or a warm one a search joined) takes the next free slot before any
// warm build queued earlier, so warm builds never hold a search back.
type buildPool struct {
	mu      sync.Mutex
	free    int
	waiters []*poolWaiter
}

// poolWaiter is one build waiting for a slot; urgent (a search's) is read and set
// under the pool's lock.
type poolWaiter struct {
	ch     chan struct{}
	urgent bool
}

func newBuildPool(n int) *buildPool { return &buildPool{free: n} }

// acquire takes a slot for w, waiting its turn: false when ctx ends first.
func (p *buildPool) acquire(ctx context.Context, w *poolWaiter) bool {
	p.mu.Lock()
	if p.free > 0 {
		p.free--
		p.mu.Unlock()
		return true
	}
	p.waiters = append(p.waiters, w)
	p.mu.Unlock()
	select {
	case <-w.ch:
		return true
	case <-ctx.Done():
		p.mu.Lock()
		if i := slices.Index(p.waiters, w); i >= 0 {
			p.waiters = slices.Delete(p.waiters, i, i+1)
			p.mu.Unlock()
			return false
		}
		p.mu.Unlock()
		p.release() // granted as ctx ended: pass it on
		return false
	}
}

// tryAcquire takes a free slot, if any, without waiting.
func (p *buildPool) tryAcquire() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.free == 0 {
		return false
	}
	p.free--
	return true
}

// release gives a slot back: to the first urgent waiter, else the first waiter.
func (p *buildPool) release() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.waiters) == 0 {
		p.free++
		return
	}
	i := slices.IndexFunc(p.waiters, func(w *poolWaiter) bool { return w.urgent })
	if i < 0 {
		i = 0
	}
	w := p.waiters[i]
	p.waiters = slices.Delete(p.waiters, i, i+1)
	close(w.ch)
}

// promote makes w urgent: a search joined its build.
func (p *buildPool) promote(w *poolWaiter) {
	p.mu.Lock()
	w.urgent = true
	p.mu.Unlock()
}

// ordsBuilding counts the builds in flight, for tests to wait on. A build leaves it
// before it releases its generation.
var ordsBuilding atomic.Int64

// ordsCache is the LRU of global ordinals, by shard, field, kind and segment list.
type ordsCache struct {
	mu    sync.Mutex
	lru   list.List // of *ordsEntry, most recent first
	items map[ordsCacheKey]*list.Element
	bytes int64
	// lines are each shard's fields' lines.
	lines map[*shard.Shard]map[ordsField]*ordsLine
}

// ordsField is one field's values or list entries.
type ordsField struct {
	field   string
	entries bool
}

// ordsLine is one shard's field's builds: the latest cached (nil: none), the one in
// flight (nil: none), and when a search last used them (Unix nanoseconds).
type ordsLine struct {
	cached   *list.Element
	building *ordsEntry
	used     int64
}

// ordsEntry is one build. done closes once ords is set (nil: the build was cancelled
// or failed, and the entry already removed); charged is what the entry adds to the
// cache's bytes, started whether the build has begun (taken its slot): both set and
// read under the cache's lock.
type ordsEntry struct {
	key     ordsCacheKey
	line    *ordsLine
	gen     uint64 // the generation built over (Generation.Gen)
	warm    bool   // started by WarmOrdinals, not a search
	done    chan struct{}
	ords    *globalOrds
	charged int64
	started bool
	cancel  context.CancelFunc
	// waiter is the build's place in ordsBuilders' queue; running closes once the
	// build has its slot.
	waiter  *poolWaiter
	running chan struct{}
}

// ordsCacheKey names a build: a shard's field's values or entries over one segment
// list.
type ordsCacheKey struct {
	shard    *shard.Shard
	field    string
	entries  bool
	segments string // the segment IDs, NUL-separated, in generation order
}

var globalOrdsCache = &ordsCache{items: map[ordsCacheKey]*list.Element{}, lines: map[*shard.Shard]map[ordsField]*ordsLine{}}

func ordsKey(g *shard.Generation, field string, entries bool) ordsCacheKey {
	return ordsCacheKey{shard: g.Shard(), field: field, entries: entries, segments: strings.Join(segmentIDs(g), "\x00")}
}

func entryOf(el *list.Element) *ordsEntry {
	return el.Value.(*ordsEntry) //nolint:forcetypeassert,errcheck // only entries
}

func (e *ordsEntry) finished() bool {
	select {
	case <-e.done:
		return true
	default:
		return false
	}
}

// purge drops a closed shard's lines and cached builds; its builds in flight are
// cancelled by the shutdown and removed as they end.
func (c *ordsCache) purge(sh *shard.Shard) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ln := range c.lines[sh] {
		if ln.cached != nil {
			c.remove(ln.cached)
		}
	}
	delete(c.lines, sh)
}

// line returns key's line, made when missing (the first of g's shard purges them all
// when the shard shuts down). The caller holds c.mu.
func (c *ordsCache) line(key ordsCacheKey, g *shard.Generation) *ordsLine {
	fields := c.lines[key.shard]
	if fields == nil {
		fields = map[ordsField]*ordsLine{}
		c.lines[key.shard] = fields
		if key.shard != nil {
			sh := key.shard
			context.AfterFunc(g.Context(), func() { c.purge(sh) })
		}
	}
	f := ordsField{key.field, key.entries}
	ln := fields[f]
	if ln == nil {
		ln = &ordsLine{}
		fields[f] = ln
	}
	return ln
}

// prune drops key's line once it holds nothing and was not used within the warm
// window. The caller holds c.mu.
func (c *ordsCache) prune(key ordsCacheKey, ln *ordsLine) {
	if ln.cached != nil || ln.building != nil || ln.used >= time.Now().Add(-ordsWarmWindow).UnixNano() {
		return
	}
	fields := c.lines[key.shard]
	delete(fields, ordsField{key.field, key.entries})
	if len(fields) == 0 {
		delete(c.lines, key.shard)
	}
}

// cached returns key's finished build, or nil (none, or one still building), as a
// search prepares.
func (c *ordsCache) cached(ctx context.Context, key ordsCacheKey) *globalOrds {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok || !entryOf(el).finished() {
		ordsMetrics().lookup(ctx, "absent")
		return nil
	}
	c.lru.MoveToFront(el)
	e := entryOf(el)
	e.line.used = time.Now().UnixNano()
	ordsMetrics().lookup(ctx, "hit")
	return e.ords
}

// globalOrdinals returns field's global ordinals over g's segments, from the cache or
// built (once, however many searches ask at the same time). A search waits only for a
// build that runs: one not holding a slot within ordsStartGrace (queued behind
// others) is left to the background at once, and a running one is waited for at most
// ordsRunningWait. Nil then, when ctx ends first, or when the shard's line holds a
// build over a newer generation: the caller merges by term, and the build goes on and
// is cached when done.
func globalOrdinals(ctx context.Context, g *shard.Generation, field string, entries bool) *globalOrds {
	key := ordsKey(g, field, entries)
	c := globalOrdsCache
	c.mu.Lock()
	e, result := c.entry(key, g, false, time.Now().UnixNano()) //nolint:contextcheck // the build outlives the search: it runs for its shard's lifetime
	if e != nil && !e.started {
		ordsBuilders.promote(e.waiter)
	}
	c.mu.Unlock()
	ordsMetrics().lookup(ctx, result)
	if e == nil {
		return nil
	}
	select {
	case <-e.done:
		return e.ords
	default:
	}
	start := time.NewTimer(ordsStartGrace)
	defer start.Stop()
	select {
	case <-e.running:
	case <-e.done:
		return e.ords
	case <-start.C:
		ordsMetrics().lookup(ctx, "queued")
		return nil
	case <-ctx.Done():
		return nil
	}
	wait := time.NewTimer(ordsRunningWait)
	defer wait.Stop()
	select {
	case <-e.done:
		return e.ords
	case <-wait.C:
		return nil
	case <-ctx.Done():
		return nil
	}
}

// entry returns key's entry, starting its build over g when there is none: with
// whether it was finished (hit), building (shared), started (miss) or refused (stale:
// nil, the line has a newer generation's). used is when a search used it (0 for a
// warm). The caller holds c.mu.
func (c *ordsCache) entry(key ordsCacheKey, g *shard.Generation, warm bool, used int64) (*ordsEntry, string) {
	ln := c.line(key, g)
	ln.used = max(ln.used, used)
	if el, ok := c.items[key]; ok {
		c.lru.MoveToFront(el)
		if entryOf(el).finished() {
			return entryOf(el), "hit"
		}
		return entryOf(el), "shared"
	}
	if ln.cached != nil && entryOf(ln.cached).gen > g.Gen() {
		return nil, "stale"
	}
	prev := ln.building
	if prev != nil {
		if prev.gen > g.Gen() {
			return nil, "stale"
		}
		if !prev.started {
			prev.cancel()
		}
	}
	e := &ordsEntry{
		key: key, line: ln, gen: g.Gen(), warm: warm, done: make(chan struct{}),
		waiter: &poolWaiter{ch: make(chan struct{}), urgent: !warm}, running: make(chan struct{}),
	}
	el := c.lru.PushFront(e)
	c.items[key] = el
	ln.building = e
	c.start(el, e, g, prev)
	return e, "miss"
}

// start runs e's build over g on its own goroutine, holding g as background work of
// its shard: after prev (the line's older build, still running) ends, once a slot of
// ordsBuilders is free. The caller holds c.mu.
func (c *ordsCache) start(el *list.Element, e *ordsEntry, g *shard.Generation, prev *ordsEntry) {
	ctx, cancel := context.WithCancel(g.Context())
	e.cancel = cancel
	release, ok := g.Background()
	if !ok {
		cancel()
		c.drop(el, e)
		return
	}
	ordsBuilding.Add(1)
	go func() {
		var ords *globalOrds
		started := false
		defer func() {
			if r := recover(); r != nil {
				slog.Default().Error("search: recovered a panic building global ordinals", slog.Any("panic", r), slog.String("stack", string(debug.Stack())))
				ords = nil
				ordsMetrics().build(ctx, "unknown", e.trigger(), "panic", 0)
			} else if !started {
				result := "superseded"
				if g.Context().Err() != nil {
					result = "shutdown"
				}
				ordsMetrics().build(ctx, "unstarted", e.trigger(), result, 0)
			}
			cancel()
			c.finish(el, e, ords)
			// Counted out before the generation is let go: a shard's Close, which waits
			// for release, then never returns with the build still counted.
			ordsBuilding.Add(-1)
			release()
		}()
		if prev != nil {
			select {
			case <-prev.done:
			case <-ctx.Done():
				return
			}
		}
		if !ordsBuilders.acquire(ctx, e.waiter) {
			return
		}
		defer ordsBuilders.release()
		c.mu.Lock()
		e.started = true
		c.mu.Unlock()
		close(e.running)
		started = true
		ords = c.build(ctx, e, g)
	}()
}

func (e *ordsEntry) trigger() string {
	if e.warm {
		return "warm"
	}
	return "search"
}

// drop removes e, whose build never started, and closes done. The caller holds c.mu.
func (c *ordsCache) drop(el *list.Element, e *ordsEntry) {
	if e.line.building == e {
		e.line.building = nil
	}
	if c.items[e.key] == el {
		c.remove(el)
	}
	close(e.done)
	c.prune(e.key, e.line)
}

// build makes e's ordinals over g: derived from the line's cached build when
// worthDeriving, else in full. Nil when ctx ends first.
func (c *ordsCache) build(ctx context.Context, e *ordsEntry, g *shard.Generation) *globalOrds {
	c.mu.Lock()
	var base *globalOrds
	if e.line.cached != nil {
		base = entryOf(e.line.cached).ords
	}
	c.mu.Unlock()
	kind := "full"
	start := time.Now()
	var ords *globalOrds
	if base != nil && worthDeriving(base, g, e.key.field, e.key.entries) {
		kind = "incremental"
		ords = deriveGlobalOrds(ctx, base, g, e.key.field, e.key.entries)
	} else {
		ords = buildGlobalOrds(ctx, g, e.key.field, e.key.entries)
	}
	result := "done"
	if ords == nil {
		result = "cancelled"
	}
	ordsMetrics().build(ctx, kind, e.trigger(), result, time.Since(start))
	return ords
}

// finish records e's build and closes done. A finished build becomes its line's
// cached one, replacing (and dropping) the older, unless the line has a newer
// generation's cached: then it serves only the searches that waited for it.
func (c *ordsCache) finish(el *list.Element, e *ordsEntry, ords *globalOrds) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e.ords = ords
	ln := e.line
	if ln.building == e {
		ln.building = nil
	}
	switch {
	case c.items[e.key] != el:
	case ords == nil || (ln.cached != nil && entryOf(ln.cached).gen > e.gen) ||
		c.lines[e.key.shard][ordsField{e.key.field, e.key.entries}] != ln: // purged: its shard closed
		c.remove(el)
	default:
		if ln.cached != nil {
			c.remove(ln.cached)
		}
		ln.cached = el
		e.charged = ords.size
		c.bytes += e.charged
		for it := c.lru.Back(); it != nil && c.bytes > globalOrdsCacheBytes; {
			prev := it.Prev()
			if o := entryOf(it); it != el && o.ords != nil {
				c.remove(it)
				c.prune(o.key, o.line)
			}
			it = prev
		}
	}
	close(e.done)
	c.prune(e.key, ln)
}

// remove drops el, leaving its line for the caller to prune; the caller holds c.mu.
func (c *ordsCache) remove(el *list.Element) {
	e := entryOf(el)
	c.lru.Remove(el)
	delete(c.items, e.key)
	c.bytes -= e.charged
	e.charged = 0
	if e.line.cached == el {
		e.line.cached = nil
	}
}

// WarmOrdinals starts background builds, over g's segment list, of the global
// ordinals searches used over g's shard within ordsWarmWindow, so the first search
// after a refresh or merge finds them built or building. A build still queued for an
// earlier list of the shard is cancelled; one already running finishes, and the new
// one derives from it. It returns at once: install it as shard.Options.Published.
func WarmOrdinals(g *shard.Generation) {
	if g == nil || g.Shard() == nil || len(g.Segments) == 0 {
		return
	}
	since := time.Now().Add(-ordsWarmWindow).UnixNano()
	c := globalOrdsCache
	c.mu.Lock()
	defer c.mu.Unlock()
	for f, ln := range c.lines[g.Shard()] {
		if ln.used < since {
			continue
		}
		key := ordsKey(g, f.field, f.entries)
		if _, ok := c.items[key]; !ok {
			c.entry(key, g, true, 0)
		}
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

// build counts one build: kind full or incremental, with its duration; unstarted (one
// superseded or cancelled before it began) or unknown (one that panicked), without.
func (in *ordsInstruments) build(ctx context.Context, kind, trigger, result string, d time.Duration) {
	ctx = context.WithoutCancel(ctx)
	in.builds.Add(ctx, 1, metric.WithAttributeSet(attribute.NewSet(
		attribute.String("kind", kind), attribute.String("trigger", trigger), attribute.String("result", result))))
	if kind == "full" || kind == "incremental" {
		in.duration.Record(ctx, d.Seconds(), metric.WithAttributeSet(attribute.NewSet(
			attribute.String("kind", kind), attribute.String("trigger", trigger))))
	}
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
// dense enough, are there; by term otherwise, exactly too. A search never waits on a
// build queued for a slot, and on a running one only briefly (see globalOrdinals),
// then merges by term; the build goes on, for the next search. Only when ctx ends
// before the merge by term is done is the shard's partial of spec left empty and the
// search cut short (prepared.cut).
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
		}
	}
	switch {
	case ords != nil && spec.typ == AggTerms:
		finishTerms(dst, parts, spec, g, ords, entries)
	case ords != nil:
		finishCardinality(dst, parts, spec, ords)
	default:
		var done bool
		if spec.typ == AggTerms {
			done = finishTermsByKey(ctx, dst, parts, spec, g, entries)
		} else {
			done = finishCardinalityByKey(ctx, dst, parts, spec, g, entries)
		}
		if !done {
			p.cut.Store(true)
			mergePartial(dst, emptyPartial(spec), spec)
		}
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
// shard's cut is cutShard's. False, merging nothing, when ctx ends first (checked
// before each segment's partial).
func finishTermsByKey(ctx context.Context, dst *AggPartial, parts []*ordPartial, spec *aggSpec, g *shard.Generation, entries bool) bool {
	byKey := map[string]*BucketPartial{}
	out := &AggPartial{Type: AggTerms}
	for _, p := range parts {
		if ctx.Err() != nil {
			return false
		}
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
	return true
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
// term: each segment's seen terms read and hashed. False, merging nothing, when ctx
// ends first (checked before each segment's partial).
func finishCardinalityByKey(ctx context.Context, dst *AggPartial, parts []*ordPartial, spec *aggSpec, g *shard.Generation, entries bool) bool {
	var hashes []uint64
	for _, p := range parts {
		if ctx.Err() != nil {
			return false
		}
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
	return true
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
