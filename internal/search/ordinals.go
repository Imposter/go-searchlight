package search

import (
	"container/heap"
	"container/list"
	"math/bits"
	"slices"
	"strings"
	"sync"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// Global ordinals.
//
// A keyword or list field's terms aggregation and cardinality count by ordinal: each
// segment counts its documents per local ordinal (an index into its own sorted
// dictionary), and the shard adds those counts up by global ordinal, an index into
// the sorted union of every segment's dictionary. Global ordinals order as the terms
// do, so a terms aggregation's cut (most documents first, then by key) is made on
// numbers, and only the buckets it keeps have their terms read. A cardinality sets
// one bit per global ordinal and hashes each term once, from a hash kept per global
// ordinal.
//
// The mapping is built on first use for a field and a generation's segment list, and
// kept in an LRU bounded by bytes (Elasticsearch's global ordinals are kept the same
// way, per reader): segments never change, so an entry is valid for as long as its
// segment list is current, and a new list (a refresh or merge) builds a new one.

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

// local returns segment seg's local ordinal for global ordinal gl, if it has the term.
func (g *globalOrds) local(seg int, gl uint32) (uint32, bool) {
	m := g.segs[seg]
	i, ok := slices.BinarySearch(m, gl)
	return uint32(i), ok //nolint:gosec // a segment holds at most 2^32 terms
}

// loadOrds fetches the global ordinals p's aggregations count by.
func loadOrds(g *shard.Generation, p *prepared) {
	var want func(spec *aggSpec, top bool)
	want = func(spec *aggSpec, top bool) {
		if entries, ok := ordKind(spec.ftype); ok && (spec.typ == AggCardinality || (top && spec.typ == AggTerms)) {
			k := ordField{spec.field, entries}
			if p.ords == nil {
				p.ords = map[ordField]*globalOrds{}
			}
			if p.ords[k] == nil {
				p.ords[k] = globalOrdinals(g, spec.field, entries)
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

// ordSource is one segment's ordered term source for a field.
type ordSource struct {
	seg   int
	terms []string
	pos   int
}

type ordHeap []*ordSource

func (h ordHeap) Len() int { return len(h) }
func (h ordHeap) Less(i, j int) bool {
	if c := strings.Compare(h[i].terms[h[i].pos], h[j].terms[h[j].pos]); c != 0 {
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

// buildGlobalOrds merges the field's dictionaries of every segment of g.
func buildGlobalOrds(g *shard.Generation, field string, entries bool) *globalOrds {
	n := len(g.Segments)
	out := &globalOrds{segs: make([][]uint32, n), dfs: make([][]uint32, n)}
	kind := kindValue
	if entries {
		kind = kindEntry
	}
	h := make(ordHeap, 0, n)
	for i := range g.Segments {
		r := g.Segments[i].Reader
		var count uint32
		if entries {
			count = r.Entries(field).NumTerms()
		} else {
			count = r.Keywords(field).NumTerms()
		}
		if count == 0 {
			continue
		}
		src := &ordSource{seg: i, terms: make([]string, 0, count)}
		dfs := make([]uint32, 0, count)
		r.Terms(field, kind, "", func(term string, df uint32) bool {
			src.terms = append(src.terms, term)
			dfs = append(dfs, df)
			return true
		})
		out.segs[i] = make([]uint32, len(src.terms))
		out.dfs[i] = dfs
		out.size += int64(len(src.terms)) * 8
		if len(src.terms) > 0 {
			h = append(h, src)
		}
	}
	heap.Init(&h)
	var last string
	for h.Len() > 0 {
		src := h[0]
		term := src.terms[src.pos]
		if len(out.hashes) == 0 || term != last {
			last = term
			out.hashes = append(out.hashes, hashString(term)|1)
			out.where = append(out.where, ordRef{seg: uint32(src.seg), ord: uint32(src.pos)}) //nolint:gosec // both below 2^32
		}
		out.segs[src.seg][src.pos] = uint32(len(out.hashes) - 1) //nolint:gosec // at most 2^32 terms
		src.pos++
		if src.pos == len(src.terms) {
			heap.Pop(&h)
		} else {
			heap.Fix(&h, 0)
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

type ordsEntry struct {
	key  string
	once sync.Once
	ords *globalOrds
}

var globalOrdsCache = &ordsCache{items: map[string]*list.Element{}}

// globalOrdinals returns field's global ordinals over g's segments, from the cache or
// built (once, however many searches ask at the same time).
func globalOrdinals(g *shard.Generation, field string, entries bool) *globalOrds {
	var b strings.Builder
	b.WriteString(field)
	if entries {
		b.WriteString("\x00l")
	} else {
		b.WriteString("\x00k")
	}
	for i := range g.Segments {
		b.WriteByte(0)
		b.WriteString(g.Segments[i].ID)
	}
	key := b.String()
	c := globalOrdsCache
	c.mu.Lock()
	el, ok := c.items[key]
	if ok {
		c.lru.MoveToFront(el)
	} else {
		el = c.lru.PushFront(&ordsEntry{key: key})
		c.items[key] = el
	}
	e := el.Value.(*ordsEntry) //nolint:forcetypeassert,errcheck // only entries
	c.mu.Unlock()
	built := false
	e.once.Do(func() {
		e.ords = buildGlobalOrds(g, field, entries)
		built = true
	})
	if built {
		c.mu.Lock()
		if _, ok := c.items[key]; ok {
			c.bytes += e.ords.size
			for c.bytes > globalOrdsCacheBytes && c.lru.Len() > 1 {
				c.remove(c.lru.Back())
			}
		}
		c.mu.Unlock()
	}
	return e.ords
}

// remove drops el; the caller holds c.mu.
func (c *ordsCache) remove(el *list.Element) {
	e := el.Value.(*ordsEntry) //nolint:forcetypeassert,errcheck // only entries
	c.lru.Remove(el)
	delete(c.items, e.key)
	if e.ords != nil {
		c.bytes -= e.ords.size
	}
}

// ordPartial is one segment's ordinal-keyed partial of a terms aggregation (counts and
// sub-aggregation collectors by local ordinal) or a cardinality (the local ordinals
// seen), which the shard merges by global ordinal.
type ordPartial struct {
	seg    int
	counts []int64
	subs   [][]collector
	seen   []uint64
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
			b.Aggs = make(map[string]*AggPartial, len(spec.subs))
			for _, sub := range spec.subs {
				b.Aggs[sub.name] = emptyPartial(sub)
			}
			for _, p := range parts {
				if p.subs == nil {
					continue
				}
				o, ok := ords.local(p.seg, gl)
				if !ok || p.subs[o] == nil {
					continue
				}
				for i, sub := range spec.subs {
					mergePartial(b.Aggs[sub.name], p.subs[o][i].partial(), sub)
				}
			}
		}
		out.Buckets = append(out.Buckets, b)
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

// emptyPartial is spec's partial over no documents.
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
