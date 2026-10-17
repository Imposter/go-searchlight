package search

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// Aggregations.
//
// Each segment collects every aggregation over its hits into a partial; the shard
// merges its segments' partials (exactly: by key) and cuts a terms aggregation to
// shard_size; the reduce merges the shards' partials and finishes them into results.
//
// terms across shards is Elasticsearch's: each shard returns its shard_size best terms
// (most documents first, then by key), so a term outside some shard's list is counted
// only by the shards that returned it. Each shard that cut its list reports the
// count of its last returned term as its error (no term it left out has more); the
// response's doc_count_error_upper_bound is their sum, and each bucket's own bound is
// the sum over the shards that did not return it. A bucket's doc_count is therefore a
// lower bound, exact when its bound is 0; with one shard, or shard_size at least the
// number of distinct terms, every count is exact.
//
// A filter, filters or global aggregation is a few buckets of documents: per segment,
// the hits its filter matches (the plan's, as the query's: filter cache and residual
// checks alike), or the live documents for global. Its partial holds each bucket, in
// its spec's key order, with the partials of the aggregations under it collected over
// the bucket's documents as top-level ones are over the hits; every level merges
// those as it merges top-level partials, so a terms aggregation under a filter is cut
// and bounded as one at the top.

// AggPartial is one aggregation's partial result: a segment's, or a shard's.
type AggPartial struct {
	Type    string           `json:"type"`
	Buckets []*BucketPartial `json:"buckets,omitempty"`
	// DocCountError (terms) is the most documents a term the shard left out of its
	// cut list can have: the count of its last returned term, or 0 when it cut none.
	DocCountError int64 `json:"doc_count_error,omitempty"`
	// OtherDocCount (terms) counts the documents of the terms left out.
	OtherDocCount int64         `json:"other_doc_count,omitempty"`
	Stats         *StatsPartial `json:"stats,omitempty"`
	Sketch        *Sketch       `json:"sketch,omitempty"`
	// Value is a min or max aggregation's least or greatest value, nil with none.
	Value *float64 `json:"value,omitempty"`
	// Distinct (a cardinality with a threshold) are distinct value hashes, sorted: all
	// of them while fewer than the threshold, else threshold of them.
	Distinct []uint64 `json:"distinct,omitempty"`

	// ords are a segment's partials by local ordinal (a keyword or list field's terms
	// or cardinality, one per part of the segment collected apart), which the shard
	// merges by global ordinal into the fields above.
	ords []*ordPartial
}

// BucketPartial is one bucket of a partial: its key (a string, float64 or bool; a
// range bucket's index as a float64; a filters bucket's key, nil for filter and
// global), documents, and sub-aggregation partials.
type BucketPartial struct {
	Key      any                    `json:"key"`
	DocCount int64                  `json:"doc_count"`
	Aggs     map[string]*AggPartial `json:"aggs,omitempty"`
}

// StatsPartial is a stats partial: Min and Max are meaningful only when Count > 0.
type StatsPartial struct {
	Count int64   `json:"count"`
	Sum   float64 `json:"sum"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
}

// add counts v; -0 is 0, as min and max and every bucket key take it.
func (s *StatsPartial) add(v float64) {
	v = positiveZero(v)
	if s.Count == 0 || v < s.Min {
		s.Min = v
	}
	if s.Count == 0 || v > s.Max {
		s.Max = v
	}
	s.Count++
	s.Sum += v
}

func (s *StatsPartial) merge(o *StatsPartial) {
	if o == nil || o.Count == 0 {
		return
	}
	if s.Count == 0 {
		*s = *o
		return
	}
	s.Count += o.Count
	s.Sum += o.Sum
	s.Min = min(s.Min, o.Min)
	s.Max = max(s.Max, o.Max)
}

// srcKind is how a field's values are read for aggregating.
type srcKind uint8

const (
	srcNone srcKind = iota
	srcKeyword
	srcList
	srcNumber
	srcBool
)

// fieldSrc reads one field's values in one segment.
type fieldSrc struct {
	r     *segment.Reader
	field string
	seg   int
	kind  srcKind
	kc    segment.KeywordColumn
	mc    segment.MultiColumn
	nc    segment.NumericColumn
	t, f  *roaring.Bitmap
	// ords are the field's cached global ordinals, when a keyword or list field has
	// them: this segment's local-to-global map, and every global ordinal's hash.
	ords   *globalOrds
	global []uint32
	// hashes memoizes each local ordinal's value hash when there are no global
	// ordinals (0: not yet).
	hashes []uint64
	buf    []uint32
}

func (s *segExec) source(field string, t schema.FieldType) *fieldSrc {
	key := field + "\x00" + t.String()
	if fs, ok := s.sources[key]; ok {
		return fs
	}
	fs := &fieldSrc{r: s.r, field: field, seg: s.seg}
	switch t {
	case schema.Keyword, schema.Text:
		if kc := s.r.Keywords(field); kc.Exists() {
			fs.kind, fs.kc = srcKeyword, kc
		}
	case schema.KeywordList:
		if mc := s.r.Entries(field); mc.Exists() {
			fs.kind, fs.mc = srcList, mc
		}
	}
	if list, ok := ordKind(t); ok && (fs.kind == srcKeyword || fs.kind == srcList) {
		if g := s.p.ords[ordField{field, list}]; g != nil {
			fs.ords, fs.global = g, g.segs[s.seg]
		}
	}
	switch t {
	case schema.Number, schema.Date:
		if nc := s.r.Numbers(field); nc.Exists() {
			fs.kind, fs.nc = srcNumber, nc
		}
	case schema.Bool:
		fs.kind = srcBool
		fs.t, fs.f = s.r.Postings(field, kindValue, "true"), s.r.Postings(field, kindValue, "false")
	}
	s.sources[key] = fs
	return fs
}

func (fs *fieldSrc) number(d uint32) (float64, bool) {
	if fs.kind != srcNumber {
		return 0, false
	}
	return fs.nc.Value(d)
}

func (fs *fieldSrc) boolOf(d uint32) (bool, bool) {
	switch {
	case fs.t.Contains(d):
		return true, true
	case fs.f.Contains(d):
		return false, true
	}
	return false, false
}

func (fs *fieldSrc) ordHash(o uint32) uint64 {
	if fs.ords != nil {
		return fs.ords.hashes[fs.global[o]]
	}
	if fs.hashes == nil {
		fs.hashes = make([]uint64, fs.numTerms())
	}
	h := fs.hashes[o]
	if h == 0 {
		if fs.kind == srcList {
			h = hashString(fs.mc.Term(o)) | 1
		} else {
			h = hashString(fs.kc.Term(o)) | 1
		}
		fs.hashes[o] = h
	}
	return h
}

// numTerms is how many terms the field's keyword or list column holds.
func (fs *fieldSrc) numTerms() uint32 {
	switch fs.kind {
	case srcKeyword:
		return fs.kc.NumTerms()
	case srcList:
		return fs.mc.NumTerms()
	}
	return 0
}

// ordinal reports whether the field counts by ordinal: a keyword or list column.
func (fs *fieldSrc) ordinal() bool { return fs.kind == srcKeyword || fs.kind == srcList }

// eachHash calls fn with the hash of each of document d's values.
func (fs *fieldSrc) eachHash(d uint32, fn func(uint64)) {
	switch fs.kind {
	case srcKeyword:
		if o, ok := fs.kc.Ord(d); ok {
			fn(fs.ordHash(o))
		}
	case srcList:
		fs.buf = fs.mc.Ords(d, fs.buf[:0])
		for _, o := range fs.buf {
			fn(fs.ordHash(o))
		}
	case srcNumber:
		if v, ok := fs.nc.Value(d); ok {
			fn(hashFloat(v))
		}
	case srcBool:
		if b, ok := fs.boolOf(d); ok {
			fn(hashString(boolTerm(b)) | 1)
		}
	}
}

// collector gathers one aggregation over a segment's documents.
type collector interface {
	collect(d uint32)
	partial() *AggPartial
}

// newCollector makes spec's collector for this segment; top is true for a top-level
// aggregation (one instance per segment), false for a sub-aggregation (one per bucket).
func (s *segExec) newCollector(spec *aggSpec, top bool) collector {
	src := s.source(spec.field, spec.ftype)
	switch spec.typ {
	case AggStats:
		return &statsColl{src: src}
	case AggMin, AggMax:
		return &extremeColl{src: src, max: spec.typ == AggMax}
	case AggCardinality:
		c := &cardColl{s: s, src: src, limit: spec.threshold, reached: spec.reached}
		switch {
		case top && src.ordinal():
			c.seen = make([]uint64, (src.numTerms()+63)/64)
		case c.limit > 0:
			c.set = map[uint64]struct{}{}
		default:
			c.sketch = NewSketch(spec.precision)
		}
		return c
	case AggTerms:
		c := &termsColl{s: s, spec: spec, src: src, deferred: deferSubs(spec, src)}
		if s.lazyTerms(spec, src) {
			c.lazy = &lazyFilter{f: spec.terms, src: src, seen: map[uint32]bool{}}
		} else {
			c.allow = s.termMask(spec, src)
		}
		return c
	case AggRange:
		return &rangeColl{s: s, spec: spec, src: src, counts: make([]int64, len(spec.ranges)), subs: make([][]collector, len(spec.ranges))}
	default:
		c := &histColl{s: s, spec: spec, src: src}
		if src.kind == srcNumber {
			c.b = newBucketer(spec, src.nc.Stats())
		}
		if c.b != nil {
			c.slots = make([]histBucket, c.b.n)
		} else {
			c.buckets = map[float64]*histBucket{}
		}
		return c
	}
}

func (s *segExec) newSubs(spec *aggSpec) []collector {
	if len(spec.subs) == 0 {
		return nil
	}
	out := make([]collector, len(spec.subs))
	for i, sub := range spec.subs {
		out[i] = s.newCollector(sub, false)
	}
	return out
}

func subPartials(spec *aggSpec, subs []collector) map[string]*AggPartial {
	if len(subs) == 0 {
		return nil
	}
	out := make(map[string]*AggPartial, len(subs))
	for i, sub := range spec.subs {
		out[sub.name] = subs[i].partial()
	}
	return out
}

func collectSubs(subs []collector, d uint32) {
	for _, c := range subs {
		c.collect(d)
	}
}

type statsColl struct {
	src *fieldSrc
	st  StatsPartial
}

func (c *statsColl) collect(d uint32) {
	if v, ok := c.src.number(d); ok {
		c.st.add(v)
	}
}

func (c *statsColl) partial() *AggPartial {
	st := c.st
	return &AggPartial{Type: AggStats, Stats: &st}
}

// extremeColl keeps a min or max aggregation's least or greatest value; -0 is 0.
type extremeColl struct {
	src *fieldSrc
	max bool
	has bool
	v   float64
}

func (c *extremeColl) collect(d uint32) {
	if v, ok := c.src.number(d); ok {
		c.add(v)
	}
}

func (c *extremeColl) add(v float64) {
	v = positiveZero(v)
	if !c.has || (c.max && v > c.v) || (!c.max && v < c.v) {
		c.v, c.has = v, true
	}
}

// countDense finds the extreme of a dense hit set from the point index: its blocks in
// value order from the end sought, each block's documents in value order, until the
// first block holding a hit, whose hits hold the extreme.
func (c *extremeColl) countDense(notHit *roaring.Bitmap) bool {
	if c.src.kind != srcNumber {
		return false
	}
	nc := c.src.nc
	var docs []uint32
	nc.EachBlock(negInf, posInf, c.max, func(pb segment.PointBlock) bool {
		docs = pb.Docs(docs[:0])
		for _, d := range docs {
			if !notHit.Contains(d) {
				if v, ok := nc.Value(d); ok {
					c.add(v)
				}
			}
		}
		return !c.has
	})
	return true
}

func (c *extremeColl) partial() *AggPartial {
	p := &AggPartial{Type: AggMin}
	if c.max {
		p.Type = AggMax
	}
	if c.has {
		v := c.v
		p.Value = &v
	}
	return p
}

// mergeExtreme keeps in *dst the least (or, with greatest, the greatest) of *dst and
// src; either may be nil, for no value.
func mergeExtreme(dst **float64, src *float64, greatest bool) {
	switch {
	case src == nil:
	case *dst == nil:
		v := *src
		*dst = &v
	case (greatest && *src > **dst) || (!greatest && *src < **dst):
		**dst = *src
	}
}

// cardColl counts a cardinality's distinct values: by local ordinal for a top-level
// keyword or list field (seen), else by hash, into a sketch or, with a threshold, an
// exact set of at most threshold hashes. With a threshold it stops once it has seen
// that many values, or once another segment of the shard has (reached).
type cardColl struct {
	s      *segExec
	src    *fieldSrc
	sketch *Sketch
	seen   []uint64
	// limit is the spec's threshold (0: none); n counts the values seen, by ordinal or
	// in set.
	limit   int
	n       int
	set     map[uint64]struct{}
	reached *atomic.Bool
}

func (c *cardColl) collect(d uint32) {
	if c.full() {
		return
	}
	if c.seen != nil {
		switch c.src.kind {
		case srcKeyword:
			if o, ok := c.src.kc.Ord(d); ok {
				c.mark(o)
			}
		case srcList:
			c.src.buf = c.src.mc.Ords(d, c.src.buf[:0])
			for _, o := range c.src.buf {
				c.mark(o)
			}
		}
		return
	}
	if c.limit > 0 {
		c.src.eachHash(d, c.addHash)
		return
	}
	c.src.eachHash(d, c.sketch.Add)
}

// mark records local ordinal o as seen.
func (c *cardColl) mark(o uint32) {
	if w, bit := &c.seen[o/64], uint64(1)<<(o%64); *w&bit == 0 {
		*w |= bit
		c.count()
	}
}

func (c *cardColl) addHash(h uint64) {
	if _, ok := c.set[h]; !ok && len(c.set) < c.limit {
		c.set[h] = struct{}{}
		c.count()
	}
}

// count notes one more value seen, and the threshold when it is reached.
func (c *cardColl) count() {
	c.n++
	if c.limit > 0 && c.n >= c.limit && c.reached != nil {
		c.reached.Store(true)
	}
}

// full reports whether a cardinality with a threshold has nothing left to count: it
// has seen threshold values, or another segment of the shard has.
func (c *cardColl) full() bool {
	return c.limit > 0 && (c.n >= c.limit || (c.reached != nil && c.reached.Load()))
}

// done lets a segment's collection drop the collector once it is full.
func (c *cardColl) done() bool { return c.full() }

// countDense marks the ordinals of a dense hit set from the dictionary's document
// frequencies, minus the documents not hit. With a threshold it first marks the
// ordinals held by more documents than are not hit, which some hit must hold, and
// stops there when they are threshold or more.
func (c *cardColl) countDense(notHit *roaring.Bitmap) bool {
	if c.seen == nil {
		return false
	}
	if c.limit > 0 {
		miss := notHit.GetCardinality()
		if fs := c.src; fs.ords != nil {
			for o, df := range fs.ords.dfs[fs.seg] {
				if c.full() {
					return true
				}
				if uint64(df) > miss {
					c.mark(uint32(o))
				}
			}
		} else {
			cur := fs.r.Cursor(fs.field, ordTermKind(fs.kind == srcList))
			for o := uint32(0); o < fs.numTerms() && !c.full() && cur.Next(); o++ {
				if uint64(cur.DocFreq()) > miss {
					c.mark(o)
				}
			}
		}
		if c.full() {
			return true
		}
	}
	counts := c.src.denseCounts(notHit)
	for o, n := range counts {
		if n > 0 {
			c.mark(uint32(o))
		}
	}
	return true
}

func (c *cardColl) partial() *AggPartial {
	switch {
	case c.seen != nil:
		return &AggPartial{Type: AggCardinality, ords: []*ordPartial{{seg: c.s.seg, seen: c.seen}}}
	case c.limit > 0:
		hashes := make([]uint64, 0, len(c.set))
		for h := range c.set {
			hashes = append(hashes, h)
		}
		slices.Sort(hashes)
		return &AggPartial{Type: AggCardinality, Distinct: hashes}
	}
	c.sketch.seal()
	return &AggPartial{Type: AggCardinality, Sketch: c.sketch}
}

// distinctPartial is a cardinality's partial of hashes, sorted and distinct (and taken
// over): an exact set of at most its threshold of them when it has one, else a sketch.
func distinctPartial(spec *aggSpec, hashes []uint64) *AggPartial {
	if spec.threshold > 0 {
		return &AggPartial{Type: AggCardinality, Distinct: hashes[:min(len(hashes), spec.threshold)]}
	}
	return &AggPartial{Type: AggCardinality, Sketch: sketchOf(spec.precision, hashes)}
}

// denseCounts is each local ordinal's documents among a dense hit set: its document
// frequency, minus the documents not hit that hold it.
func (fs *fieldSrc) denseCounts(notHit *roaring.Bitmap) []int64 {
	counts := make([]int64, fs.numTerms())
	if fs.ords != nil {
		for o, df := range fs.ords.dfs[fs.seg] {
			counts[o] = int64(df)
		}
	} else {
		cur := fs.r.Cursor(fs.field, ordTermKind(fs.kind == srcList))
		for o := 0; o < len(counts) && cur.Next(); o++ {
			counts[o] = int64(cur.DocFreq())
		}
	}
	it := notHit.ManyIterator()
	buf := make([]uint32, 512)
	for {
		n := it.NextMany(buf)
		if n == 0 {
			return counts
		}
		for _, d := range buf[:n] {
			switch fs.kind {
			case srcKeyword:
				if o, ok := fs.kc.Ord(d); ok {
					counts[o]--
				}
			case srcList:
				fs.buf = fs.mc.Ords(d, fs.buf[:0])
				for _, o := range fs.buf {
					counts[o]--
				}
			}
		}
	}
}

// listedCounts is denseCounts for the ordinals m lists alone: each one's document
// frequency, minus the documents not hit that hold it.
func (fs *fieldSrc) listedCounts(m *termMask, notHit *roaring.Bitmap) []int64 {
	counts := make([]int64, fs.numTerms())
	for i, o := range m.ords {
		counts[o] = int64(m.dfs[i])
	}
	it := notHit.ManyIterator()
	buf := make([]uint32, 512)
	for {
		n := it.NextMany(buf)
		if n == 0 {
			return counts
		}
		for _, d := range buf[:n] {
			switch fs.kind {
			case srcKeyword:
				if o, ok := fs.kc.Ord(d); ok && m.has(o) {
					counts[o]--
				}
			case srcList:
				fs.buf = fs.mc.Ords(d, fs.buf[:0])
				for _, o := range fs.buf {
					if m.has(o) {
						counts[o]--
					}
				}
			}
		}
	}
}

// termsColl counts documents per value; -0 and 0 are one value, in one bucket.
type termsColl struct {
	s    *segExec
	spec *aggSpec
	src  *fieldSrc

	counts  []int64       // by ordinal (keyword, list) or 0/1 (bool)
	subs    [][]collector // by ordinal, made on first use
	numbers map[float64]*histBucket
	// deferred is set when the sub-aggregations wait for the shard's cut (breadth
	// first, see deferSubs): the collector counts alone, and docs are the documents it
	// counts, read again once the shard knows the buckets it keeps.
	deferred bool
	docs     *roaring.Bitmap
	// pool, for a part of a segment collected apart, lends counts shared with the
	// segment's other parts (collectChunked).
	pool *countPool
	// allow is the filter of include and exclude over the ordinals (or bool keys), or
	// lazy decides it ordinal by ordinal (few hits over a large dictionary): both nil
	// without them, and for a number field, whose values are checked directly.
	allow *termMask
	lazy  *lazyFilter
}

// deferSubs reports whether a terms aggregation over src collects its
// sub-aggregations breadth first: counting first, then, once the shard has cut its
// list, reading the hits again for the buckets it keeps alone (Elasticsearch's
// collect_mode breadth_first). Only a keyword or list field defers, as its terms are
// counted by ordinal; with collect_mode unset, when the segment's field holds more
// distinct values than shard_size, so most of the buckets collected depth first would
// be dropped.
func deferSubs(spec *aggSpec, src *fieldSrc) bool {
	if len(spec.subs) == 0 || !src.ordinal() {
		return false
	}
	switch spec.collect {
	case CollectDepthFirst:
		return false
	case CollectBreadthFirst:
		return true
	}
	return uint64(src.numTerms()) > uint64(spec.shardSize) //nolint:gosec // shardSize is positive
}

// deferTo gives a deferred terms collector the documents it collects.
func deferTo(c collector, docs *roaring.Bitmap) {
	if tc, ok := c.(*termsColl); ok && tc.deferred {
		tc.docs = docs
	}
}

func (c *termsColl) allowed(o uint32) bool {
	switch {
	case c.allow != nil:
		return c.allow.has(o)
	case c.lazy != nil:
		return c.lazy.allows(o)
	}
	return true
}

func (c *termsColl) slot(o uint32) {
	if c.counts == nil {
		n := uint32(2)
		if c.src.ordinal() {
			n = c.src.numTerms()
		}
		if c.pool != nil {
			c.counts = c.pool.get()
		} else {
			c.counts = make([]int64, n)
		}
		if len(c.spec.subs) > 0 && !c.deferred {
			c.subs = make([][]collector, n)
		}
	}
	c.counts[o]++
}

func (c *termsColl) withSubs(o, d uint32) {
	if c.subs == nil {
		return
	}
	if c.subs[o] == nil {
		c.subs[o] = c.s.newSubs(c.spec)
	}
	collectSubs(c.subs[o], d)
}

func (c *termsColl) collect(d uint32) {
	switch c.src.kind {
	case srcKeyword:
		if o, ok := c.src.kc.Ord(d); ok && c.allowed(o) {
			c.slot(o)
			c.withSubs(o, d)
		}
	case srcList:
		c.src.buf = c.src.mc.Ords(d, c.src.buf[:0])
		for _, o := range c.src.buf {
			if c.allowed(o) {
				c.slot(o)
				c.withSubs(o, d)
			}
		}
	case srcBool:
		if b, ok := c.src.boolOf(d); ok {
			o := uint32(0)
			if b {
				o = 1
			}
			if c.allowed(o) {
				c.slot(o)
				c.withSubs(o, d)
			}
		}
	case srcNumber:
		if v, ok := c.src.nc.Value(d); ok {
			v = positiveZero(v)
			if c.numbers == nil {
				c.numbers = map[float64]*histBucket{}
			}
			b := c.numbers[v]
			if b == nil {
				if c.spec.terms != nil && !c.spec.terms.number(v) {
					return
				}
				b = &histBucket{subs: c.s.newSubs(c.spec)}
				c.numbers[v] = b
			}
			b.count++
			collectSubs(b.subs, d)
		}
	}
}

// countDense counts a dense hit set (no sub-aggregations, or deferred ones) from the
// dictionary's document frequencies, minus the documents not hit: cheaper than reading
// every hit's value when most of the segment matches.
func (c *termsColl) countDense(notHit *roaring.Bitmap) bool {
	if (len(c.spec.subs) > 0 && !c.deferred) || !c.src.ordinal() || c.lazy != nil {
		return false
	}
	if c.allow != nil && c.allow.listed {
		c.counts = c.src.listedCounts(c.allow, notHit)
		return true
	}
	c.counts = c.src.denseCounts(notHit)
	if c.allow != nil {
		for o := range c.counts {
			if !c.allowed(uint32(o)) {
				c.counts[o] = 0
			}
		}
	}
	return true
}

func (c *termsColl) partial() *AggPartial {
	p := &AggPartial{Type: AggTerms}
	if c.pool != nil {
		if c.counts != nil {
			c.pool.put(c.counts)
		}
		return p
	}
	switch {
	case c.src.kind == srcNumber:
		for v, b := range c.numbers {
			p.Buckets = append(p.Buckets, &BucketPartial{Key: v, DocCount: b.count, Aggs: subPartials(c.spec, b.subs)})
		}
	case c.src.ordinal():
		op := &ordPartial{seg: c.s.seg, counts: c.counts, subs: c.subs}
		if c.deferred && c.counts != nil {
			op.deferred = &deferredPart{s: c.s, docs: c.docs}
		}
		p.ords = []*ordPartial{op}
	default:
		for o, n := range c.counts {
			if n == 0 {
				continue
			}
			var subs []collector
			if c.subs != nil {
				subs = c.subs[o]
			}
			if subs == nil && len(c.spec.subs) > 0 {
				subs = c.s.newSubs(c.spec)
			}
			p.Buckets = append(p.Buckets, &BucketPartial{Key: o == 1, DocCount: n, Aggs: subPartials(c.spec, subs)})
		}
	}
	return p
}

// rangeColl counts documents per range: From inclusive, To exclusive.
type rangeColl struct {
	s      *segExec
	spec   *aggSpec
	src    *fieldSrc
	counts []int64
	subs   [][]collector
}

func (c *rangeColl) collect(d uint32) {
	v, ok := c.src.number(d)
	if !ok {
		return
	}
	for i := range c.spec.ranges {
		r := &c.spec.ranges[i]
		if v >= r.from && v < r.to {
			c.counts[i]++
			if len(c.spec.subs) > 0 {
				if c.subs[i] == nil {
					c.subs[i] = c.s.newSubs(c.spec)
				}
				collectSubs(c.subs[i], d)
			}
		}
	}
}

// countDense counts a dense hit set (no sub-aggregations) from the point index: each
// range's documents, minus the documents not hit.
func (c *rangeColl) countDense(notHit *roaring.Bitmap) bool {
	if len(c.spec.subs) > 0 || c.src.kind != srcNumber {
		return false
	}
	nc := c.src.nc
	for i := range c.spec.ranges {
		r := &c.spec.ranges[i]
		c.counts[i] = int64(nc.Count(r.from, r.to, true, false)) //nolint:gosec // at most 2^32 documents
	}
	eachValue(nc, notHit, func(v float64) {
		for i := range c.spec.ranges {
			if r := &c.spec.ranges[i]; v >= r.from && v < r.to {
				c.counts[i]--
			}
		}
	})
	return true
}

func eachValue(nc segment.NumericColumn, docs *roaring.Bitmap, fn func(v float64)) {
	it := docs.ManyIterator()
	buf := make([]uint32, 512)
	for {
		n := it.NextMany(buf)
		if n == 0 {
			return
		}
		for _, d := range buf[:n] {
			if v, ok := nc.Value(d); ok {
				fn(v)
			}
		}
	}
}

func (c *rangeColl) partial() *AggPartial {
	p := &AggPartial{Type: AggRange}
	for i := range c.spec.ranges {
		subs := c.subs[i]
		if subs == nil && len(c.spec.subs) > 0 {
			subs = c.s.newSubs(c.spec)
		}
		p.Buckets = append(p.Buckets, &BucketPartial{Key: float64(i), DocCount: c.counts[i], Aggs: subPartials(c.spec, subs)})
	}
	return p
}

type histBucket struct {
	count int64
	subs  []collector
}

// histColl counts documents per histogram bucket: by slot when the segment's values
// span few enough buckets for a bucketer, else in a map by key. A value bucketKey
// cannot place (its key overflows, or a calendar cannot date it) refuses the search
// as too many buckets rather than leave a counted document out of every bucket.
type histColl struct {
	s       *segExec
	spec    *aggSpec
	src     *fieldSrc
	b       *bucketer
	slots   []histBucket
	buckets map[float64]*histBucket
}

func (c *histColl) collect(d uint32) {
	v, ok := c.src.number(d)
	if !ok {
		return
	}
	if c.b != nil {
		i, ok := c.b.slot(v)
		if !ok {
			return
		}
		b := &c.slots[i]
		b.count++
		if len(c.spec.subs) > 0 {
			if b.subs == nil {
				b.subs = c.s.newSubs(c.spec)
			}
			collectSubs(b.subs, d)
		}
		return
	}
	key, ok := bucketKey(c.spec, v)
	if !ok {
		c.refuse()
		return
	}
	b := c.buckets[key]
	if b == nil {
		if c.s.buckets.Add(1) > MaxBuckets {
			c.refuse()
			return
		}
		b = &histBucket{subs: c.s.newSubs(c.spec)}
		c.buckets[key] = b
	}
	b.count++
	collectSubs(b.subs, d)
}

// refuse fails the segment as too many buckets, once.
func (c *histColl) refuse() {
	if c.s.err == nil {
		c.s.fail(tooManyBuckets(c.spec))
	}
}

// countDense counts a dense hit set (no sub-aggregations) from the point index: a walk
// of its blocks in value order, adding a block whose least and greatest values share a
// bucket whole and reading the values of the few that straddle a bucket's edge, then
// the documents not hit are taken off.
func (c *histColl) countDense(notHit *roaring.Bitmap) bool {
	if c.b == nil || len(c.spec.subs) > 0 {
		return false
	}
	nc := c.src.nc
	var docs []uint32
	nc.EachBlock(negInf, posInf, false, func(pb segment.PointBlock) bool {
		i, ok := c.b.slot(pb.Min)
		j, okMax := c.b.slot(pb.Max)
		if ok && okMax && i == j {
			c.slots[i].count += int64(pb.Count)
			return true
		}
		docs = pb.Docs(docs[:0])
		for _, d := range docs {
			if v, ok := nc.Value(d); ok {
				if k, ok := c.b.slot(v); ok {
					c.slots[k].count++
				}
			}
		}
		return true
	})
	eachValue(nc, notHit, func(v float64) {
		if k, ok := c.b.slot(v); ok {
			c.slots[k].count--
		}
	})
	return true
}

func (c *histColl) partial() *AggPartial {
	p := &AggPartial{Type: c.spec.typ}
	for i := range c.slots {
		if b := &c.slots[i]; b.count > 0 {
			if b.subs == nil && len(c.spec.subs) > 0 {
				b.subs = c.s.newSubs(c.spec)
			}
			p.Buckets = append(p.Buckets, &BucketPartial{Key: c.b.key(i), DocCount: b.count, Aggs: subPartials(c.spec, b.subs)})
		}
	}
	for key, b := range c.buckets {
		p.Buckets = append(p.Buckets, &BucketPartial{Key: key, DocCount: b.count, Aggs: subPartials(c.spec, b.subs)})
	}
	return p
}

// maxSlots bounds the buckets a bucketer spans: a histogram over a wider spread of
// values collects into a map.
const maxSlots = 4096

// bucketer places one segment's values into a histogram's buckets by index: the
// buckets between the column's least and greatest value, each value's the one
// bucketKey gives it.
type bucketer struct {
	spec *aggSpec
	// first is a fixed interval's first bucket, as a count of intervals from offset;
	// starts are a calendar's bucket starts (milliseconds, before offset), ascending.
	first  float64
	starts []int64
	n      int
}

// newBucketer returns the bucketer of spec over values within st, or nil when they
// span more than maxSlots buckets.
func newBucketer(spec *aggSpec, st segment.Stats) *bucketer {
	if st.Count == 0 {
		return nil
	}
	if spec.calendar == "" {
		lo := math.Floor((st.Min - spec.offset) / spec.interval)
		hi := math.Floor((st.Max - spec.offset) / spec.interval)
		if !(hi-lo < maxSlots) || math.IsInf(lo, 0) {
			return nil
		}
		b := &bucketer{spec: spec, first: lo, n: int(hi-lo) + 1}
		if k := b.key(b.n - 1); math.IsInf(k, 0) || math.IsNaN(k) || math.IsInf(b.key(0), 0) {
			return nil
		}
		return b
	}
	lo, hi := st.Min-spec.offset, st.Max-spec.offset
	if math.Abs(lo) > maxDateMillis || math.Abs(hi) > maxDateMillis {
		return nil
	}
	end := int64(math.Floor(hi))
	b := &bucketer{spec: spec}
	for t := calendarFloor(time.UnixMilli(int64(math.Floor(lo))).UTC(), spec.calendar); t.UnixMilli() <= end; t = calendarFloor(calendarNext(t, spec.calendar), spec.calendar) {
		if len(b.starts) == maxSlots {
			return nil
		}
		b.starts = append(b.starts, t.UnixMilli())
	}
	b.n = len(b.starts)
	return b
}

// slot is v's bucket; false where bucketKey places it in none.
func (b *bucketer) slot(v float64) (int, bool) {
	if b.starts == nil {
		i := math.Floor((v-b.spec.offset)/b.spec.interval) - b.first
		if !(i >= 0 && i < float64(b.n)) {
			return 0, false
		}
		return int(i), true
	}
	ms := v - b.spec.offset
	if math.Abs(ms) > maxDateMillis {
		return 0, false
	}
	m := int64(math.Floor(ms))
	i := sortSearch(len(b.starts), func(i int) bool { return b.starts[i] > m }) - 1
	return i, i >= 0
}

// key is slot i's bucket key, as bucketKey computes it.
func (b *bucketer) key(i int) float64 {
	if b.starts == nil {
		return (b.first+float64(i))*b.spec.interval + b.spec.offset
	}
	return float64(b.starts[i]) + b.spec.offset
}

// maxDateMillis bounds the values a calendar interval buckets: JavaScript's (and
// Elasticsearch's) date range, 100 million days either side of 1970.
const maxDateMillis = 8.64e15

// bucketKey is the start of v's histogram bucket; false for a value a calendar
// interval cannot place.
func bucketKey(spec *aggSpec, v float64) (float64, bool) {
	if spec.calendar == "" {
		k := math.Floor((v-spec.offset)/spec.interval)*spec.interval + spec.offset
		return k, !math.IsInf(k, 0) && !math.IsNaN(k)
	}
	ms := v - spec.offset
	if math.Abs(ms) > maxDateMillis {
		return 0, false
	}
	t := time.UnixMilli(int64(math.Floor(ms))).UTC()
	return float64(calendarFloor(t, spec.calendar).UnixMilli()) + spec.offset, true
}

func calendarFloor(t time.Time, unit string) time.Time {
	y, m, d := t.Date()
	switch unit {
	case "minute":
		return t.Truncate(time.Minute)
	case "hour":
		return t.Truncate(time.Hour)
	case "day":
		return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	case "week":
		day := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
		return day.AddDate(0, 0, -((int(day.Weekday()) + 6) % 7))
	case "month":
		return time.Date(y, m, 1, 0, 0, 0, 0, time.UTC)
	case "quarter":
		return time.Date(y, ((m-1)/3)*3+1, 1, 0, 0, 0, 0, time.UTC)
	default:
		return time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC)
	}
}

func calendarNext(t time.Time, unit string) time.Time {
	switch unit {
	case "minute":
		return t.Add(time.Minute)
	case "hour":
		return t.Add(time.Hour)
	case "day":
		return t.AddDate(0, 0, 1)
	case "week":
		return t.AddDate(0, 0, 7)
	case "month":
		return t.AddDate(0, 1, 0)
	case "quarter":
		return t.AddDate(0, 3, 0)
	default:
		return t.AddDate(1, 0, 0)
	}
}

// denseCounter is a collector that can count a dense hit set from the index (every
// document's values) minus the documents not hit, cheaper than reading every hit's;
// countDense reports false when it cannot (sub-aggregations, say).
type denseCounter interface {
	countDense(notHit *roaring.Bitmap) bool
}

// aggregate collects every aggregation over a segment's hits; live are the segment's
// live documents, which a global aggregation counts.
func (s *segExec) aggregate(hits, live *roaring.Bitmap) map[string]*AggPartial {
	if len(s.p.aggs) == 0 {
		return nil
	}
	return s.aggregateSpecs(s.p.aggs, hits, live)
}

// aggregateSpecs collects specs over hits, as top-level aggregations: the request's
// over its hits, or a filter, filters or global aggregation's over a bucket's
// documents. A stats aggregation over every document of a segment with no deletes is
// the number column's own stats.
func (s *segExec) aggregateSpecs(specs []*aggSpec, hits, live *roaring.Bitmap) map[string]*AggPartial {
	if len(specs) == 0 {
		return nil
	}
	colls := make([]collector, len(specs))
	var perDoc []int
	var notHit *roaring.Bitmap
	s.collecting = hits.GetCardinality()
	dense := 2*hits.GetCardinality() >= uint64(s.n)
	for i, spec := range specs {
		if spec.wrapper() {
			continue
		}
		c := s.newCollector(spec, true)
		deferTo(c, hits)
		colls[i] = c
		if dc, ok := c.(denseCounter); ok && dense {
			if notHit == nil {
				notHit = s.flip(hits)
			}
			if dc.countDense(notHit) {
				continue
			}
		}
		if sc, ok := c.(*statsColl); ok && s.sv.Deletes.IsEmpty() && hits.GetCardinality() == uint64(s.n) && sc.src.kind == srcNumber {
			st := sc.src.nc.Stats()
			if st.Count > 0 {
				sc.st = StatsPartial{Count: int64(st.Count), Sum: st.Sum, Min: positiveZero(st.Min), Max: positiveZero(st.Max)}
			}
			continue
		}
		perDoc = append(perDoc, i)
	}
	out := make(map[string]*AggPartial, len(colls))
	if len(perDoc) > 0 && hits.GetCardinality() >= aggChunkMin && hits.Maximum()/aggChunk > hits.Minimum()/aggChunk {
		chunked := make([]*aggSpec, len(perDoc))
		for j, i := range perDoc {
			chunked[j] = specs[i]
		}
		for j, p := range s.collectChunked(hits, chunked) {
			out[chunked[j].name] = p
		}
		perDoc = nil
	}
	if len(perDoc) > 0 {
		cs := make([]collector, len(perDoc))
		for j, i := range perDoc {
			cs[j] = colls[i]
		}
		s.collect(hits, cs)
	}
	if s.err != nil {
		return nil
	}
	for i, spec := range specs {
		switch {
		case spec.wrapper():
			out[spec.name] = s.bucketPartial(spec, hits, live)
		case out[spec.name] == nil:
			out[spec.name] = colls[i].partial()
		}
	}
	if s.err != nil {
		return nil
	}
	return out
}

// bucketPartial collects a filter, filters or global aggregation over a segment: each
// bucket's documents (the hits a filter matches, the hits no filter matches for
// filters' other bucket, the live documents for global) counted, with the
// aggregations under it collected over them.
func (s *segExec) bucketPartial(spec *aggSpec, hits, live *roaring.Bitmap) *AggPartial {
	p := &AggPartial{Type: spec.typ, Buckets: make([]*BucketPartial, 0, len(spec.keys))}
	add := func(key any, docs *roaring.Bitmap) {
		p.Buckets = append(p.Buckets, &BucketPartial{Key: key, DocCount: card(docs), Aggs: s.aggregateSpecs(spec.subs, docs, live)})
	}
	if spec.typ == AggGlobal {
		add(nil, live)
		return p
	}
	var matched []*roaring.Bitmap
	for i, f := range spec.filters {
		docs := s.matches(f, hits)
		add(spec.keys[i], docs)
		if spec.hasOther {
			matched = append(matched, docs)
		}
	}
	if spec.hasOther {
		add(spec.keys[len(spec.keys)-1], roaring.AndNot(hits, roaring.FastOr(matched...)))
	}
	return p
}

// doner is a collector that can be done before its documents are: a cardinality
// with a threshold, once it has seen threshold values.
type doner interface {
	done() bool
}

// collect collects docs into cs, dropping each collector once it is done.
func (s *segExec) collect(docs *roaring.Bitmap, cs []collector) {
	it := docs.ManyIterator()
	buf := make([]uint32, 512)
	cs = slices.Clone(cs)
	for {
		cs = slices.DeleteFunc(cs, func(c collector) bool {
			d, ok := c.(doner)
			return ok && d.done()
		})
		n := it.NextMany(buf)
		if n == 0 || len(cs) == 0 || s.checkCtx() {
			return
		}
		for _, d := range buf[:n] {
			for _, c := range cs {
				c.collect(d)
			}
		}
	}
}

// aggChunkMin is the fewest hits a segment's per-document aggregation is split for:
// each stretch of aggChunk ordinals is collected apart on the search pool, and the
// parts' partials merged. Variables so tests can split small segments.
var (
	aggChunkMin uint64 = 1 << 15
	aggChunk           = scanChunk
)

// collectChunked collects the aggregations specs over hits in parts, in parallel, and
// returns each one's partial, merged.
func (s *segExec) collectChunked(hits *roaring.Bitmap, specs []*aggSpec) []*AggPartial {
	first, last := hits.Minimum()/aggChunk, hits.Maximum()/aggChunk
	chunks := int(last-first) + 1
	pools := make([]*countPool, len(specs))
	for j, spec := range specs {
		if spec.typ != AggTerms {
			continue
		}
		if src := s.source(spec.field, spec.ftype); src.ordinal() && (len(spec.subs) == 0 || deferSubs(spec, src)) {
			pools[j] = &countPool{n: int(src.numTerms())}
		}
	}
	parts := make([][]*AggPartial, chunks)
	errs := make([]error, chunks)
	err := runParallel(chunks, func(c int) {
		lo := uint64(first+uint32(c)) * uint64(aggChunk) //nolint:gosec // c < chunks
		window := roaring.New()
		window.AddRange(lo, lo+uint64(aggChunk))
		docs := roaring.And(hits, window)
		cs := s.fork()
		colls := make([]collector, len(specs))
		for j, spec := range specs {
			colls[j] = cs.newCollector(spec, true)
			if tc, ok := colls[j].(*termsColl); ok && pools[j] != nil {
				tc.pool = pools[j]
			}
		}
		cs.collect(docs, colls)
		if cs.err != nil {
			errs[c] = cs.err
			return
		}
		parts[c] = make([]*AggPartial, len(specs))
		for j := range colls {
			parts[c][j] = colls[j].partial()
		}
	})
	if err == nil {
		err = errors.Join(errs...)
	}
	if err != nil {
		if s.err == nil {
			s.err = err
		}
		return nil
	}
	out := make([]*AggPartial, len(specs))
	for j, spec := range specs {
		merged := &AggPartial{Type: spec.typ}
		if pools[j] != nil {
			op := &ordPartial{seg: s.seg, counts: pools[j].sum()}
			if op.counts != nil && len(spec.subs) > 0 {
				op.deferred = &deferredPart{s: s, docs: hits}
			}
			merged.ords = []*ordPartial{op}
			out[j] = merged
			continue
		}
		for c := range parts {
			if p := parts[c][j]; p.ords != nil {
				merged.ords = append(merged.ords, p.ords...)
			} else {
				mergePartial(merged, p, spec)
			}
		}
		out[j] = merged
	}
	return out
}

// countPool lends the parts of one segment collected apart (collectChunked) the
// count arrays of a terms aggregation by ordinal: one per part collected at once,
// each counting the parts it is lent to, summed into one once every part is done.
// Counting needs no atomics and the segment no array per part.
type countPool struct {
	n    int
	mu   sync.Mutex
	free [][]int64
	all  [][]int64
}

func (p *countPool) get() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	if k := len(p.free); k > 0 {
		a := p.free[k-1]
		p.free = p.free[:k-1]
		return a
	}
	a := make([]int64, p.n)
	p.all = append(p.all, a)
	return a
}

func (p *countPool) put(a []int64) {
	p.mu.Lock()
	p.free = append(p.free, a)
	p.mu.Unlock()
}

// sum adds every array lent into the first and returns it: nil when none was.
func (p *countPool) sum() []int64 {
	if len(p.all) == 0 {
		return nil
	}
	out := p.all[0]
	for _, a := range p.all[1:] {
		for o, n := range a {
			out[o] += n
		}
	}
	return out
}

// fork is a copy of s for one part of a segment collected apart, with field sources
// and an error of its own.
func (s *segExec) fork() *segExec {
	cs := *s
	cs.sources = map[string]*fieldSrc{}
	cs.err = nil
	return &cs
}

// mergePartial adds src into dst (the same aggregation).
func mergePartial(dst, src *AggPartial, spec *aggSpec) {
	if src == nil {
		return
	}
	switch spec.typ {
	case AggStats:
		if dst.Stats == nil {
			dst.Stats = &StatsPartial{}
		}
		dst.Stats.merge(src.Stats)
	case AggMin, AggMax:
		mergeExtreme(&dst.Value, src.Value, spec.typ == AggMax)
	case AggCardinality:
		if spec.threshold > 0 {
			u := unionSorted(dst.Distinct, src.Distinct)
			dst.Distinct = u[:min(len(u), spec.threshold)]
			return
		}
		if dst.Sketch == nil {
			dst.Sketch = NewSketch(spec.precision)
		}
		dst.Sketch.Merge(src.Sketch)
		dst.Sketch.seal()
	default:
		dst.DocCountError += src.DocCountError
		dst.OtherDocCount += src.OtherDocCount
		if spec.typ == AggRange {
			if len(dst.Buckets) == 0 {
				for _, b := range src.Buckets {
					dst.Buckets = append(dst.Buckets, cloneBucket(b, spec))
				}
				return
			}
			for i, b := range src.Buckets {
				if i < len(dst.Buckets) {
					mergeBucket(dst.Buckets[i], b, spec)
				}
			}
			return
		}
		index := make(map[any]*BucketPartial, len(dst.Buckets))
		for _, b := range dst.Buckets {
			index[b.Key] = b
		}
		for _, b := range src.Buckets {
			if have := index[b.Key]; have != nil {
				mergeBucket(have, b, spec)
				continue
			}
			nb := cloneBucket(b, spec)
			index[b.Key] = nb
			dst.Buckets = append(dst.Buckets, nb)
		}
	}
}

func cloneBucket(b *BucketPartial, spec *aggSpec) *BucketPartial {
	nb := &BucketPartial{Key: b.Key, DocCount: b.DocCount}
	if len(spec.subs) > 0 {
		nb.Aggs = make(map[string]*AggPartial, len(spec.subs))
		for _, sub := range spec.subs {
			p := &AggPartial{Type: sub.typ}
			mergePartial(p, b.Aggs[sub.name], sub)
			nb.Aggs[sub.name] = p
		}
	}
	return nb
}

func mergeBucket(dst, src *BucketPartial, spec *aggSpec) {
	dst.DocCount += src.DocCount
	for _, sub := range spec.subs {
		if dst.Aggs == nil {
			dst.Aggs = map[string]*AggPartial{}
		}
		p := dst.Aggs[sub.name]
		if p == nil {
			p = &AggPartial{Type: sub.typ}
			dst.Aggs[sub.name] = p
		}
		mergePartial(p, src.Aggs[sub.name], sub)
	}
}

// cmpTerms orders terms buckets: most documents first, then by key.
func cmpTerms(a, b *BucketPartial) int {
	if a.DocCount != b.DocCount {
		if a.DocCount > b.DocCount {
			return -1
		}
		return 1
	}
	return cmpValue(a.Key, b.Key, false)
}

// cutShard finishes a shard's partial: a terms list sorted and cut to shard_size (with
// its error bound), a histogram sorted by key.
func cutShard(p *AggPartial, spec *aggSpec) {
	switch spec.typ {
	case AggTerms:
		topK(p.Buckets, spec.shardSize, cmpTerms)
		if len(p.Buckets) > spec.shardSize {
			for _, b := range p.Buckets[spec.shardSize:] {
				p.OtherDocCount += b.DocCount
			}
			p.Buckets = p.Buckets[:spec.shardSize]
			p.DocCountError = p.Buckets[len(p.Buckets)-1].DocCount
		}
	case AggHistogram, AggDateHistogram:
		slices.SortFunc(p.Buckets, func(a, b *BucketPartial) int { return cmpValue(a.Key, b.Key, false) })
	}
}

// mergeShard merges the segments' partials of specs (one map per segment searched)
// into the shard's: by key, exactly; ordinal partials by global ordinal or by term
// (finishOrds); and a filter, filters or global aggregation bucket by bucket, the
// aggregations under each merged as top-level ones are. Its error is the first
// finishOrds returned, whose aggregation is left empty; the others are merged still.
func mergeShard(ctx context.Context, specs []*aggSpec, segs []map[string]*AggPartial, g *shard.Generation, p *prepared) (map[string]*AggPartial, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	var first error
	keep := func(err error) {
		if first == nil {
			first = err
		}
	}
	out := make(map[string]*AggPartial, len(specs))
	for _, spec := range specs {
		dst := &AggPartial{Type: spec.typ}
		out[spec.name] = dst
		if spec.wrapper() {
			for i, key := range spec.keys {
				b := &BucketPartial{Key: key}
				subs := make([]map[string]*AggPartial, 0, len(segs))
				for _, m := range segs {
					if part := m[spec.name]; part != nil && i < len(part.Buckets) {
						b.DocCount += part.Buckets[i].DocCount
						subs = append(subs, part.Buckets[i].Aggs)
					}
				}
				var err error
				b.Aggs, err = mergeShard(ctx, spec.subs, subs, g, p)
				keep(err)
				dst.Buckets = append(dst.Buckets, b)
			}
			continue
		}
		var ords []*ordPartial
		for _, m := range segs {
			part := m[spec.name]
			if part != nil && part.ords != nil {
				ords = append(ords, part.ords...)
				continue
			}
			mergePartial(dst, part, spec)
		}
		if len(ords) > 0 {
			keep(finishOrds(ctx, dst, ords, spec, g, p))
		}
	}
	return out, first
}

// cutShardAggs finishes the shard's partials of specs with cutShard, the aggregations
// under a filter, filters or global aggregation's buckets too, and adds the buckets
// the response can hold of them to *buckets: it returns the aggregation at which they
// pass MaxBuckets, or nil. A filter or global aggregation counts its one bucket, a
// filters aggregation each of its buckets.
func cutShardAggs(specs []*aggSpec, aggs map[string]*AggPartial, buckets *int) *aggSpec {
	for _, spec := range specs {
		p := aggs[spec.name]
		cutShard(p, spec)
		if *buckets += responseBuckets(p, spec); *buckets > MaxBuckets {
			return spec
		}
		if spec.wrapper() {
			for _, b := range p.Buckets {
				if over := cutShardAggs(spec.subs, b.Aggs, buckets); over != nil {
					return over
				}
			}
		}
	}
	return nil
}

// responseBuckets is how many of a shard partial's buckets the response can hold: a
// terms aggregation's at most its size (the rest of its shard_size over-fetch only
// sharpens the counts across shards), every other aggregation's all of them.
func responseBuckets(p *AggPartial, spec *aggSpec) int {
	if spec.typ == AggTerms {
		return min(len(p.Buckets), spec.size)
	}
	return len(p.Buckets)
}

// AggResult is one aggregation's result.
type AggResult struct {
	Type string
	// Buckets are a bucket aggregation's buckets.
	Buckets []*Bucket
	// DocCountErrorUpperBound and SumOtherDocCount are a terms aggregation's: the most
	// documents a term left out could have, and the documents of terms left out.
	DocCountErrorUpperBound int64
	SumOtherDocCount        int64
	// Count, Sum, Min, Max and Avg are stats' (Min, Max and Avg nil with no value).
	Count         int64
	Sum           float64
	Min, Max, Avg *float64
	// Value is a cardinality's estimate; with a threshold, the exact number of
	// distinct values when it is below the threshold, else the threshold.
	Value int64
	// Number is a min or max aggregation's value: nil when no document has one.
	Number *float64
	// DocCount and Aggs are a filter or global aggregation's one bucket: its documents
	// and the results of the aggregations under it. A filters aggregation's buckets
	// are Buckets, keyed by filter.
	DocCount int64
	Aggs     map[string]*AggResult
	// Truncated is set on a bucket aggregation cut, or a histogram left without its
	// empty buckets, to keep the response within MaxBuckets buckets.
	Truncated bool
}

// Bucket is one bucket of a result.
type Bucket struct {
	// Key is a terms bucket's value (string, float64 or bool), a range's name, a
	// histogram bucket's start (milliseconds, for a date_histogram), or a filters
	// bucket's key.
	Key any
	// KeyAsString is a date_histogram bucket's start in RFC 3339.
	KeyAsString string
	// From and To are a range bucket's bounds.
	From, To *float64
	DocCount int64
	// DocCountErrorUpperBound is a terms bucket's: how many more documents it may
	// have (shards that did not return it).
	DocCountErrorUpperBound int64
	// Aggs are the sub-aggregations' results.
	Aggs map[string]*AggResult
}

// MarshalJSON writes the result in Elasticsearch's shape.
func (a *AggResult) MarshalJSON() ([]byte, error) {
	return a.appendJSON(make([]byte, 0, 64))
}

// appendJSON appends the result's JSON to dst, as encoding/json writes it (members of
// a bucket in name order, as a map's), without building maps or reflecting.
func (a *AggResult) appendJSON(dst []byte) ([]byte, error) {
	switch a.Type {
	case AggStats:
		dst = strconv.AppendInt(append(dst, `{"count":`...), a.Count, 10)
		dst = appendJSONNumber(append(dst, `,"min":`...), a.Min)
		dst = appendJSONNumber(append(dst, `,"max":`...), a.Max)
		dst = appendJSONNumber(append(dst, `,"avg":`...), a.Avg)
		dst = appendJSONFloat(append(dst, `,"sum":`...), a.Sum)
		return append(dst, '}'), nil
	case AggCardinality:
		return append(strconv.AppendInt(append(dst, `{"value":`...), a.Value, 10), '}'), nil
	case AggMin, AggMax:
		return append(appendJSONNumber(append(dst, `{"value":`...), a.Number), '}'), nil
	case AggFilter, AggGlobal:
		return appendInlineBucket(dst, a.DocCount, a.Aggs)
	case AggFilters:
		bs := slices.Clone(a.Buckets)
		key := func(b *Bucket) string { k, _ := b.Key.(string); return k }
		slices.SortStableFunc(bs, func(x, y *Bucket) int { return strings.Compare(key(x), key(y)) })
		dst = append(dst, `{"buckets":{`...)
		for i, b := range bs {
			if i > 0 && key(b) == key(bs[i-1]) {
				continue // a map keeps one bucket of a key
			}
			if i > 0 {
				dst = append(dst, ',')
			}
			var err error
			if dst, err = appendJSONString(dst, key(b)); err != nil {
				return nil, err
			}
			if dst, err = appendInlineBucket(append(dst, ':'), b.DocCount, b.Aggs); err != nil {
				return nil, err
			}
		}
		return append(dst, "}}"...), nil
	}
	if a.Type == AggTerms {
		dst = strconv.AppendInt(append(dst, `{"doc_count_error_upper_bound":`...), a.DocCountErrorUpperBound, 10)
		dst = strconv.AppendInt(append(dst, `,"sum_other_doc_count":`...), a.SumOtherDocCount, 10)
		dst = append(dst, `,"buckets":[`...)
	} else {
		dst = append(dst, `{"buckets":[`...)
	}
	for i, b := range a.Buckets {
		if i > 0 {
			dst = append(dst, ',')
		}
		var err error
		if dst, err = b.appendJSON(dst); err != nil {
			return nil, err
		}
	}
	dst = append(dst, ']')
	if a.Truncated {
		dst = append(dst, `,"truncated":true`...)
	}
	return append(dst, '}'), nil
}

// appendInlineBucket appends a filter, filters or global bucket as Elasticsearch writes
// it: its doc_count, and the aggregations under it inline, by name, in name order.
func appendInlineBucket(dst []byte, docCount int64, aggs map[string]*AggResult) ([]byte, error) {
	names := make([]string, 0, len(aggs)+1)
	for name := range aggs {
		if name != "doc_count" {
			names = append(names, name)
		}
	}
	names = append(names, "doc_count")
	slices.Sort(names)
	dst = append(dst, '{')
	for i, name := range names {
		if i > 0 {
			dst = append(dst, ',')
		}
		var err error
		if dst, err = appendJSONString(dst, name); err != nil {
			return nil, err
		}
		dst = append(dst, ':')
		if name == "doc_count" {
			dst = strconv.AppendInt(dst, docCount, 10)
		} else if dst, err = appendJSONAgg(dst, aggs[name]); err != nil {
			return nil, err
		}
	}
	return append(dst, '}'), nil
}

// MarshalJSON writes the bucket with its sub-aggregations inline, by name.
func (b *Bucket) MarshalJSON() ([]byte, error) {
	return b.appendJSON(make([]byte, 0, 64))
}

// bucketMembers are a bucket's own members, in name order.
var bucketMembers = []string{"doc_count", "doc_count_error_upper_bound", "from", "key", "key_as_string", "to"}

// appendJSON appends the bucket's JSON to dst: its members and its sub-aggregations
// together in name order, as encoding/json writes a map, without building one.
func (b *Bucket) appendJSON(dst []byte) ([]byte, error) {
	names := make([]string, 0, len(b.Aggs))
	for name := range b.Aggs {
		if !slices.Contains(bucketMembers, name) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	dst = append(dst, '{')
	first := true
	member := func(name string) {
		if !first {
			dst = append(dst, ',')
		}
		first = false
		dst = append(append(append(dst, '"'), name...), `":`...)
	}
	var err error
	write := func(name string) {
		if err != nil {
			return
		}
		switch name {
		case "doc_count":
			member(name)
			dst = strconv.AppendInt(dst, b.DocCount, 10)
		case "doc_count_error_upper_bound":
			if b.DocCountErrorUpperBound != 0 {
				member(name)
				dst = strconv.AppendInt(dst, b.DocCountErrorUpperBound, 10)
			}
		case "from":
			if b.From != nil {
				member(name)
				dst = appendJSONFloat(dst, *b.From)
			}
		case "key":
			member(name)
			dst, err = appendJSONValue(dst, b.Key)
		case "key_as_string":
			if b.KeyAsString != "" {
				member(name)
				dst, err = appendJSONString(dst, b.KeyAsString)
			}
		case "to":
			if b.To != nil {
				member(name)
				dst = appendJSONFloat(dst, *b.To)
			}
		}
	}
	// Merge the members and the sub-aggregations' names, both sorted.
	i := 0
	for _, m := range bucketMembers {
		for ; i < len(names) && names[i] < m && err == nil; i++ {
			if !first {
				dst = append(dst, ',')
			}
			first = false
			if dst, err = appendJSONString(dst, names[i]); err == nil {
				dst, err = appendJSONAgg(append(dst, ':'), b.Aggs[names[i]])
			}
		}
		write(m)
	}
	for ; i < len(names) && err == nil; i++ {
		if !first {
			dst = append(dst, ',')
		}
		first = false
		if dst, err = appendJSONString(dst, names[i]); err == nil {
			dst, err = appendJSONAgg(append(dst, ':'), b.Aggs[names[i]])
		}
	}
	if err != nil {
		return nil, err
	}
	return append(dst, '}'), nil
}

// appendJSONAgg appends a sub-aggregation's result, null for none.
func appendJSONAgg(dst []byte, a *AggResult) ([]byte, error) {
	if a == nil {
		return append(dst, "null"...), nil
	}
	return a.appendJSON(dst)
}

// appendJSONValue appends a bucket key: a string, float64 or bool as encoding/json
// writes it, anything else through it.
func appendJSONValue(dst []byte, v any) ([]byte, error) {
	switch v := v.(type) {
	case string:
		return appendJSONString(dst, v)
	case float64:
		if math.IsInf(v, 0) || math.IsNaN(v) {
			break
		}
		return appendJSONFloat(dst, v), nil
	case bool:
		return strconv.AppendBool(dst, v), nil
	case nil:
		return append(dst, "null"...), nil
	}
	raw, err := json.Marshal(v)
	return append(dst, raw...), err
}

// appendJSONString appends s quoted as encoding/json quotes it: printable ASCII other
// than " \ < > & as it is, anything else through encoding/json.
func appendJSONString(dst []byte, s string) ([]byte, error) {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c > 0x7e || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			raw, err := json.Marshal(s)
			return append(dst, raw...), err
		}
	}
	return append(append(append(dst, '"'), s...), '"'), nil
}

// appendJSONNumber appends *f, or null for nil.
func appendJSONNumber(dst []byte, f *float64) []byte {
	if f == nil {
		return append(dst, "null"...)
	}
	return appendJSONFloat(dst, *f)
}

// appendJSONFloat appends a finite f as encoding/json writes a float64: positional
// between 1e-6 and 1e21, else with an exponent of at least two digits' worth.
func appendJSONFloat(dst []byte, f float64) []byte {
	if f == math.Trunc(f) && math.Abs(f) < 1e15 && !(f == 0 && math.Signbit(f)) {
		return strconv.AppendInt(dst, int64(f), 10) // as 'f' writes it, without the search for digits
	}
	format := byte('f')
	if abs := math.Abs(f); abs != 0 && (abs < 1e-6 || abs >= 1e21) {
		format = 'e'
	}
	dst = strconv.AppendFloat(dst, f, format, -1, 64)
	if format == 'e' {
		// e-09 is e-9, as encoding/json cleans it up.
		if n := len(dst); n >= 4 && dst[n-4] == 'e' && dst[n-3] == '-' && dst[n-2] == '0' {
			dst[n-2] = dst[n-1]
			dst = dst[:n-1]
		}
	}
	return dst
}
