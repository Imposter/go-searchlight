package search

import (
	"bytes"
	"container/heap"
	"container/list"
	"context"
	"math/bits"
	"slices"
	"strings"
	"sync"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
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
// dictionaries' terms (globalOrdsDensity) to pay for it, within the request's deadline,
// and kept for later requests in an LRU bounded by bytes (Elasticsearch's global
// ordinals are kept the same way, per reader). Segments never change, so an entry is
// valid for as long as its segment list is current: a refresh or merge makes a new
// list, whose first dense request builds a new entry and drops the stale ones.

// globalOrdsDensity: the shard builds global ordinals when the ordinals with
// documents, over every segment, are at least 1/globalOrdsDensity of the segments'
// terms. A variable so tests can force either merge.
var globalOrdsDensity uint64 = 8

// globalOrds maps one field's per-segment ordinals onto global ones.
type globalOrds struct {
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
// ordinal (top-level terms, and every cardinality), building none.
func loadOrds(g *shard.Generation, p *prepared) {
	var want func(spec *aggSpec, top bool)
	want = func(spec *aggSpec, top bool) {
		if entries, ok := ordKind(spec.ftype); ok && (spec.typ == AggCardinality || (top && spec.typ == AggTerms)) {
			k := ordField{spec.field, entries}
			if _, seen := p.ords[k]; !seen {
				if p.ords == nil {
					p.ords = map[ordField]*globalOrds{}
				}
				p.ords[k] = globalOrdsCache.cached(ordsKey(g, spec.field, entries))
			}
		}
		for _, sub := range spec.subs {
			want(sub, false)
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
	if c := strings.Compare(string(h[i].cur.Term()), string(h[j].cur.Term())); c != 0 {
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
	out := &globalOrds{segs: make([][]uint32, n), dfs: make([][]uint32, n)}
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

// term reads global ordinal gl's term of field from a segment of gen that holds it.
func (g *globalOrds) term(gen *shard.Generation, field string, gl uint32, entries bool) string {
	at := g.where[gl]
	r := gen.Segments[at.seg].Reader
	if entries {
		return r.Entries(field).Term(at.ord)
	}
	return r.Keywords(field).Term(at.ord)
}

// globalOrdsCacheBytes bounds the global ordinals kept across searches. A variable so
// tests can make it small.
var globalOrdsCacheBytes int64 = 256 << 20

// ordsCache is the LRU of global ordinals, by field, kind and segment list.
type ordsCache struct {
	mu    sync.Mutex
	lru   list.List // of *ordsEntry, most recent first
	items map[string]*list.Element
	bytes int64
}

// ordsEntry is one cached build. done closes once ords is set (nil: the build was
// abandoned, and the entry already removed); charged is what the entry adds to the
// cache's bytes, set and read under the cache's lock.
type ordsEntry struct {
	key     ordsCacheKey
	done    chan struct{}
	ords    *globalOrds
	charged int64
}

// ordsCacheKey names a build: a field's values or entries over one segment list.
type ordsCacheKey struct {
	field    string
	entries  bool
	segments string // the segment IDs, NUL-separated, in generation order
}

var globalOrdsCache = &ordsCache{items: map[string]*list.Element{}}

func ordsKey(g *shard.Generation, field string, entries bool) ordsCacheKey {
	var b strings.Builder
	for i := range g.Segments {
		if i > 0 {
			b.WriteByte(0)
		}
		b.WriteString(g.Segments[i].ID)
	}
	return ordsCacheKey{field: field, entries: entries, segments: b.String()}
}

func (k ordsCacheKey) id() string {
	kind := "k"
	if k.entries {
		kind = "l"
	}
	return k.field + "\x00" + kind + "\x00" + k.segments
}

// cached returns key's finished build, or nil (none, or one still building).
func (c *ordsCache) cached(key ordsCacheKey) *globalOrds {
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
		return e.ords
	default:
		return nil
	}
}

// globalOrdinals returns field's global ordinals over g's segments, from the cache or
// built (once, however many searches ask at the same time; the others wait for it, or
// for ctx). Nil when ctx ends first: an abandoned build is not cached.
func globalOrdinals(ctx context.Context, g *shard.Generation, field string, entries bool) *globalOrds {
	key := ordsKey(g, field, entries)
	id := key.id()
	c := globalOrdsCache
	c.mu.Lock()
	el, ok := c.items[id]
	if ok {
		c.lru.MoveToFront(el)
		e := el.Value.(*ordsEntry) //nolint:forcetypeassert,errcheck // only entries
		c.mu.Unlock()
		select {
		case <-e.done:
			return e.ords
		case <-ctx.Done():
			return nil
		}
	}
	e := &ordsEntry{key: key, done: make(chan struct{})}
	el = c.lru.PushFront(e)
	c.items[id] = el
	c.mu.Unlock()

	ords := buildGlobalOrds(ctx, g, field, entries)
	c.mu.Lock()
	e.ords = ords
	switch {
	case c.items[id] != el:
	case ords == nil:
		c.remove(el)
	default:
		e.charged = ords.size
		c.bytes += e.charged
		c.dropStale(key, el)
		for c.bytes > globalOrdsCacheBytes && c.lru.Len() > 1 {
			c.remove(c.lru.Back())
		}
	}
	close(e.done)
	c.mu.Unlock()
	return ords
}

// dropStale removes the finished entries of key's field and kind whose segment list
// shares a segment with key's but is another: the same shard's earlier lists, which
// its current one replaced. The caller holds c.mu.
func (c *ordsCache) dropStale(key ordsCacheKey, keep *list.Element) {
	segs := strings.Split(key.segments, "\x00")
	for el := c.lru.Front(); el != nil; {
		next := el.Next()
		e := el.Value.(*ordsEntry) //nolint:forcetypeassert,errcheck // only entries
		if el != keep && e.ords != nil && e.key.field == key.field && e.key.entries == key.entries &&
			slices.ContainsFunc(strings.Split(e.key.segments, "\x00"), func(s string) bool { return slices.Contains(segs, s) }) {
			c.remove(el)
		}
		el = next
	}
}

// remove drops el; the caller holds c.mu.
func (c *ordsCache) remove(el *list.Element) {
	e := el.Value.(*ordsEntry) //nolint:forcetypeassert,errcheck // only entries
	c.lru.Remove(el)
	delete(c.items, e.key.id())
	c.bytes -= e.charged
	e.charged = 0
}

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
// ordinal when the ordinals cached at prepare, or built now within ctx because the
// partials are dense enough, are there; by term otherwise.
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
