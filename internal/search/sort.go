package search

import (
	"cmp"
	"container/heap"
	"container/list"
	"math"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/segment"
)

// Sorting.
//
// Within a segment a sort key reads segment-local values that order exactly as the
// global ones do: a number as itself, a keyword as twice its ordinal in the segment's
// sorted dictionary, a bool as 0 or 1, and the exact id as twice its rank among the
// segment's ids (the IDS dictionary, in byte order: [rankCache]). A search_after value
// maps into the same space, a keyword or id not in the segment as an odd number just
// below the next one, so comparing with it never ties wrongly. A document missing a
// key sorts after every document with one, ascending or descending.
//
// Each segment keeps its own top k in a heap, reading only the first key per document
// unless that key ties with the heap's worst; the id tie-breaker (and so the rank
// array) is only computed for documents that tie on every other key. Early
// termination:
//
//   - a number sort walks the point index in windows from the cursor outward, sized
//     from the column's stats to hold about k matches, and stops once the heap is full:
//     every document past a window is worse than everything in it;
//   - an ascending _id sort (the default) over a dense hit set walks the segment's id
//     dictionary from the cursor and stops at k hits.
//
// The shard merges the segments' tops by global values: numbers, terms, and the
// exact ids, read from the stored records only when everything else ties.

// part is one segment-local sort key value.
type part struct {
	miss bool
	v    float64
}

// cmpPart orders a and b for one key: missing last, then by value (reversed when desc).
func cmpPart(a, b part, desc bool) int {
	if a.miss || b.miss {
		switch {
		case a.miss && b.miss:
			return 0
		case a.miss:
			return 1
		default:
			return -1
		}
	}
	c := cmp.Compare(a.v, b.v)
	if desc {
		return -c
	}
	return c
}

// sortCol reads one sort key's values in a segment.
type sortCol struct {
	spec sortSpec
	nc   segment.NumericColumn
	kc   segment.KeywordColumn
	t, f *roaring.Bitmap
}

// segSorter is one segment's sort state.
type segSorter struct {
	s     *segExec
	cols  []sortCol
	idCol int // index of the sortID key
	// after holds search_after per key, segment-local; afterDone marks the ones
	// computed (an id's needs the rank array, so it is computed on first use).
	after     []part
	afterDone []bool
	rank      []uint32
}

func newSegSorter(s *segExec) *segSorter {
	ss := &segSorter{s: s, idCol: -1}
	for i, spec := range s.p.sorts {
		c := sortCol{spec: spec}
		switch spec.kind {
		case sortNumber:
			c.nc = s.r.Numbers(spec.field)
		case sortKeyword:
			c.kc = s.r.Keywords(spec.field)
		case sortBool:
			c.t = s.r.Postings(spec.field, kindValue, "true")
			c.f = s.r.Postings(spec.field, kindValue, "false")
		case sortID:
			ss.idCol = i
		}
		ss.cols = append(ss.cols, c)
	}
	if s.p.after != nil {
		ss.after = make([]part, len(ss.cols))
		ss.afterDone = make([]bool, len(ss.cols))
	}
	return ss
}

// key reads document d's value for key i.
func (ss *segSorter) key(d uint32, i int) part {
	c := &ss.cols[i]
	switch c.spec.kind {
	case sortNumber:
		v, ok := c.nc.Value(d)
		return part{miss: !ok, v: v}
	case sortKeyword:
		o, ok := c.kc.Ord(d)
		return part{miss: !ok, v: 2 * float64(o)}
	case sortBool:
		switch {
		case c.t.Contains(d):
			return part{v: 1}
		case c.f.Contains(d):
			return part{v: 0}
		}
		return part{miss: true}
	case sortID:
		return part{v: 2 * float64(ss.ranks()[d])}
	default:
		return part{miss: true}
	}
}

// ranks returns the segment's rank array, building it (and caching it) on first use.
func (ss *segSorter) ranks() []uint32 {
	if ss.rank == nil {
		ss.rank = ranksFor(ss.s.sv.ID, ss.s.r)
	}
	return ss.rank
}

// afterPart returns search_after's value for key i, segment-local.
func (ss *segSorter) afterPart(i int) part {
	if ss.afterDone[i] {
		return ss.after[i]
	}
	v := ss.s.p.after[i]
	c := &ss.cols[i]
	var p part
	switch {
	case v == nil:
		p.miss = true
	case c.spec.kind == sortNumber:
		p.v = v.(float64) //nolint:errcheck,forcetypeassert // prepareAfter typed it
	case c.spec.kind == sortBool:
		if v.(bool) { //nolint:errcheck,forcetypeassert // prepareAfter typed it
			p.v = 1
		}
	case c.spec.kind == sortKeyword:
		term := v.(string) //nolint:errcheck,forcetypeassert // prepareAfter typed it
		if !c.kc.Exists() {
			p.v = math.Inf(1) // no document has a value: every one is missing, after it
			break
		}
		if o, ok := c.kc.Lookup(term); ok {
			p.v = 2 * float64(o)
			break
		}
		n := c.kc.NumTerms()
		ip := uint32(sortSearch(int(n), func(o int) bool { return c.kc.Term(uint32(o)) > term })) //nolint:gosec // o < n
		p.v = 2*float64(ip) - 1
	case c.spec.kind == sortID:
		id := v.(string) //nolint:errcheck,forcetypeassert // prepareAfter typed it
		rank := ss.ranks()
		p.v = 2*float64(len(rank)) - 1
		ss.s.r.IDsFrom(id, func(got []byte, ord uint32) bool {
			if string(got) == id {
				p.v = 2 * float64(rank[ord])
			} else {
				p.v = 2*float64(rank[ord]) - 1
			}
			return false
		})
	default:
		p.v = math.Inf(1)
	}
	ss.after[i], ss.afterDone[i] = p, true
	return p
}

// sortSearch is sort.Search, kept local for clarity.
func sortSearch(n int, f func(int) bool) int {
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if !f(mid) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// afterOK reports whether document d (whose first key is k0) sorts after the cursor.
func (ss *segSorter) afterOK(d uint32, k0 part) bool {
	for i := range ss.cols {
		var k part
		if i == 0 {
			k = k0
		} else {
			k = ss.key(d, i)
		}
		switch c := cmpPart(k, ss.afterPart(i), ss.cols[i].spec.desc); {
		case c > 0:
			return true
		case c < 0:
			return false
		}
	}
	return false // the cursor's own document
}

// entry is one document in a segment's top-k heap: its keys but the id tie-breaker,
// which is read from the rank array only on a tie.
type entry struct {
	ord  uint32
	keys []part
}

// topHeap is a max-heap of the k best entries: the worst on top.
type topHeap struct {
	ss      *segSorter
	entries []entry
}

func (h *topHeap) Len() int           { return len(h.entries) }
func (h *topHeap) Less(i, j int) bool { return h.ss.compare(&h.entries[i], &h.entries[j]) > 0 }
func (h *topHeap) Swap(i, j int)      { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }
func (h *topHeap) Push(x any)         { h.entries = append(h.entries, x.(entry)) } //nolint:forcetypeassert,errcheck // only entries
func (h *topHeap) Pop() any {
	last := h.entries[len(h.entries)-1]
	h.entries = h.entries[:len(h.entries)-1]
	return last
}

// compare orders two entries of this segment: by every key, then by rank.
func (ss *segSorter) compare(a, b *entry) int {
	for i := range ss.cols {
		if i == ss.idCol {
			r := ss.ranks()
			c := cmp.Compare(r[a.ord], r[b.ord])
			if ss.cols[i].spec.desc {
				c = -c
			}
			return c
		}
		if c := cmpPart(a.keys[i], b.keys[i], ss.cols[i].spec.desc); c != 0 {
			return c
		}
	}
	return cmp.Compare(a.ord, b.ord)
}

// fill sets e's keys for document d, the id key aside.
func (ss *segSorter) fill(e *entry, d uint32, k0 part) {
	e.ord = d
	if cap(e.keys) < len(ss.cols) {
		e.keys = make([]part, len(ss.cols))
	}
	e.keys = e.keys[:len(ss.cols)]
	for i := range ss.cols {
		switch i {
		case 0:
			e.keys[i] = k0
		case ss.idCol:
			e.keys[i] = part{} // compare reads the rank array
		default:
			e.keys[i] = ss.key(d, i)
		}
	}
}

// collect offers the documents of docs to the heap of at most k.
func (ss *segSorter) collect(h *topHeap, docs *roaring.Bitmap, k int) {
	if docs.IsEmpty() {
		return
	}
	s := ss.s
	var probe entry
	it := docs.ManyIterator()
	buf := make([]uint32, 512)
	for {
		n := it.NextMany(buf)
		if n == 0 {
			return
		}
		if s.checkCtx() {
			return
		}
		for _, d := range buf[:n] {
			k0 := ss.key(d, 0)
			if ss.after != nil && !ss.afterOK(d, k0) {
				continue
			}
			if len(h.entries) == k {
				top := &h.entries[0]
				c := cmpPart(k0, top.keys[0], ss.cols[0].spec.desc)
				if c > 0 {
					continue
				}
				if c == 0 {
					ss.fill(&probe, d, k0)
					if ss.compare(&probe, top) >= 0 {
						continue
					}
				}
				// It would enter: only now is a maybe candidate verified.
				if !s.accept(d) {
					continue
				}
				// Replace the worst in place, reusing its keys.
				ss.fill(top, d, k0)
				heap.Fix(h, 0)
				continue
			}
			if !s.accept(d) {
				continue
			}
			var e entry
			ss.fill(&e, d, k0)
			heap.Push(h, e)
		}
	}
}

// topK returns the segment's best k hits among hits, best first. Under lazy
// verification hits are the candidates, and a maybe one is verified only when it
// would enter the heap (or the id walk): Lucene's two-phase top-k.
func (s *segExec) topK(hits *roaring.Bitmap, k int) []segHit {
	if k <= 0 || hits.IsEmpty() {
		return nil
	}
	ss := newSegSorter(s)
	first := ss.cols[0].spec
	if first.kind == sortID && !first.desc &&
		hits.GetCardinality()*16 >= uint64(s.n) {
		return s.topByIDWalk(hits, k)
	}
	h := &topHeap{ss: ss, entries: make([]entry, 0, k)}
	if first.kind == sortNumber {
		s.collectNumberWindows(ss, h, hits, k)
	} else {
		ss.collect(h, hits, k)
	}
	if s.err != nil {
		return nil
	}
	out := make([]entry, len(h.entries))
	for i := len(out) - 1; i >= 0; i-- {
		out[i] = heap.Pop(h).(entry) //nolint:forcetypeassert,errcheck // only entries
	}
	return s.toSegHits(ss, out)
}

// collectNumberWindows feeds the heap from windows of the point index, outward from
// the cursor, until the heap is full; whatever is left (documents past the last
// window, and those missing the value) is offered last, only if it is not.
func (s *segExec) collectNumberWindows(ss *segSorter, h *topHeap, hits *roaring.Bitmap, k int) {
	c := &ss.cols[0]
	st := c.nc.Stats()
	if st.Count == 0 {
		ss.collect(h, hits, k)
		return
	}
	desc := c.spec.desc
	// The cursor's value bounds the first window; a cursor past every value (missing)
	// leaves only missing documents.
	edge := st.Min
	if desc {
		edge = st.Max
	}
	if ss.after != nil {
		ap := ss.afterPart(0)
		if ap.miss {
			ss.collect(h, hits, k)
			return
		}
		if desc {
			edge = min(edge, ap.v)
		} else {
			edge = max(edge, ap.v)
		}
	}
	span := st.Max - edge
	if desc {
		span = edge - st.Min
	}
	density := float64(hits.GetCardinality()) / float64(max(s.n, 1))
	// Start narrow (a skewed column holds many values near its edge) and widen fast.
	frac := float64(k) / (8 * density * float64(st.Count))
	if density == 0 || frac >= 1 || span <= 0 || math.IsInf(span, 0) {
		ss.collect(h, hits, k)
		return
	}
	seen := roaring.New()
	width := span * frac
	from, inclusive := edge, true
	for {
		var to float64
		var window *roaring.Bitmap
		if desc {
			to = from - width
			if to <= st.Min {
				to = negInf
			}
			window = c.nc.Range(to, from, true, inclusive)
		} else {
			to = from + width
			if to >= st.Max {
				to = posInf
			}
			window = c.nc.Range(from, to, inclusive, true)
		}
		window.And(hits)
		ss.collect(h, window, k)
		if s.err != nil {
			return
		}
		seen.Or(window)
		if len(h.entries) == k || math.IsInf(to, 0) {
			break
		}
		from, inclusive = to, false
		width *= 4
	}
	if len(h.entries) < k {
		rest := roaring.AndNot(hits, seen)
		ss.collect(h, rest, k)
	}
}

// topByIDWalk walks the segment's ids in order from the cursor, keeping the first k
// hits: the default sort's early termination.
func (s *segExec) topByIDWalk(hits *roaring.Bitmap, k int) []segHit {
	from := ""
	if s.p.after != nil {
		from = s.p.after[0].(string) //nolint:forcetypeassert,errcheck // prepareAfter typed it
	}
	out := make([]segHit, 0, k)
	i := 0
	s.r.IDsFrom(from, func(id []byte, ord uint32) bool {
		i++
		if i%checkEvery == 0 && s.checkCtx() {
			return false
		}
		if !hits.Contains(ord) || (s.p.after != nil && string(id) == from) || !s.accept(ord) {
			return true
		}
		out = append(out, segHit{seg: s.seg, ord: ord, id: string(id), hasID: true, vals: []any{nil}})
		return len(out) < k
	})
	if s.err != nil {
		return nil
	}
	return out
}

// segHit is one of a segment's top hits, with its global sort values.
type segHit struct {
	seg   int
	ord   uint32
	vals  []any // per sort key; an id key's value is id, read on demand
	id    string
	hasID bool
}

func (s *segExec) toSegHits(ss *segSorter, es []entry) []segHit {
	out := make([]segHit, len(es))
	for i := range es {
		e := &es[i]
		vals := make([]any, len(ss.cols))
		for j := range ss.cols {
			c := &ss.cols[j]
			k := e.keys[j]
			if k.miss || c.spec.kind == sortID || c.spec.kind == sortNone {
				continue
			}
			switch c.spec.kind {
			case sortNumber:
				vals[j] = k.v
			case sortKeyword:
				vals[j] = c.kc.Term(uint32(k.v / 2))
			case sortBool:
				vals[j] = k.v == 1
			}
		}
		out[i] = segHit{seg: s.seg, ord: e.ord, vals: vals}
		if ss.rank != nil {
			// The rank array was built: the id is one dictionary lookup away, no
			// stored record to decompress.
			out[i].id, out[i].hasID = s.r.IDAt(ss.rank[e.ord])
		}
	}
	return out
}

// cmpValue orders two global sort values of one key: nil (missing) last, then by
// value, reversed when desc.
func cmpValue(a, b any, desc bool) int {
	if a == nil || b == nil {
		switch {
		case a == nil && b == nil:
			return 0
		case a == nil:
			return 1
		default:
			return -1
		}
	}
	var c int
	switch x := a.(type) {
	case float64:
		y, _ := toFloat(b)
		c = cmp.Compare(x, y)
	case string:
		y, _ := b.(string)
		c = strings.Compare(x, y)
	case bool:
		y, _ := b.(bool)
		switch {
		case x == y:
		case !x:
			c = -1
		default:
			c = 1
		}
	default:
		xf, _ := toFloat(a)
		yf, _ := toFloat(b)
		c = cmp.Compare(xf, yf)
	}
	if desc {
		return -c
	}
	return c
}

// The rank cache.

// rankCacheBytes bounds the rank arrays kept (4 bytes per document). A segment of more
// than rankCacheBytes/4 documents (64M) is never cached: its rank array is rebuilt for
// each search that needs it.
const rankCacheBytes = 256 << 20

// rankLRU caches each segment's rank array: ord -> the rank of its id among the
// segment's ids. Segments never change, so an entry never goes stale; an entry whose
// segment has been closed for good is dropped at the next put.
type rankLRU struct {
	mu    sync.Mutex
	lru   list.List // of *rankEntry, most recent first
	items map[string]*list.Element
	bytes int64
}

type rankEntry struct {
	seg    string
	reader *segment.Reader
	ranks  []uint32
}

var rankCache = &rankLRU{items: map[string]*list.Element{}}

func (c *rankLRU) get(seg string) []uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[seg]; ok {
		c.lru.MoveToFront(el)
		return el.Value.(*rankEntry).ranks //nolint:forcetypeassert,errcheck // only rankEntries
	}
	return nil
}

func (c *rankLRU) put(seg string, r *segment.Reader, ranks []uint32) {
	size := int64(len(ranks)) * 4
	if size > rankCacheBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for el := c.lru.Front(); el != nil; {
		next := el.Next()
		if e := el.Value.(*rankEntry); e.reader.Closed() { //nolint:forcetypeassert,errcheck // only rankEntries
			c.remove(el)
		}
		el = next
	}
	if _, ok := c.items[seg]; ok {
		return
	}
	c.items[seg] = c.lru.PushFront(&rankEntry{seg: seg, reader: r, ranks: ranks})
	c.bytes += size
	for c.bytes > rankCacheBytes {
		c.remove(c.lru.Back())
	}
}

func (c *rankLRU) remove(el *list.Element) {
	e := el.Value.(*rankEntry) //nolint:forcetypeassert,errcheck // only rankEntries
	c.lru.Remove(el)
	delete(c.items, e.seg)
	c.bytes -= int64(len(e.ranks)) * 4
}

// ranksFor returns segment seg's rank array, from the cache or built by one walk of
// its id dictionary.
func ranksFor(seg string, r *segment.Reader) []uint32 {
	if ranks := rankCache.get(seg); ranks != nil {
		return ranks
	}
	ranks := make([]uint32, r.NumDocs())
	var next uint32
	r.IDsFrom("", func(_ []byte, ord uint32) bool {
		if ord < uint32(len(ranks)) { //nolint:gosec // NumDocs is a uint32
			ranks[ord] = next
		}
		next++
		return true
	})
	rankCache.put(seg, r, ranks)
	return ranks
}
