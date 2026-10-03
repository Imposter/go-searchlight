package search

import (
	"encoding/json"
	"math"
	"math/bits"
	"slices"
	"time"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
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
}

// BucketPartial is one bucket of a partial: its key (a string, float64 or bool; a
// range bucket's index as a float64), documents, and sub-aggregation partials.
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

func (s *StatsPartial) add(v float64) {
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
	kind srcKind
	kc   segment.KeywordColumn
	mc   segment.MultiColumn
	nc   segment.NumericColumn
	t, f *roaring.Bitmap
	// hashes memoizes each ordinal's value hash (0: not yet).
	hashes []uint64
	buf    []uint32
}

func (s *segExec) source(field string, t schema.FieldType) *fieldSrc {
	key := field + "\x00" + t.String()
	if fs, ok := s.sources[key]; ok {
		return fs
	}
	fs := &fieldSrc{}
	switch t {
	case schema.Keyword, schema.Text:
		if kc := s.r.Keywords(field); kc.Exists() {
			fs.kind, fs.kc = srcKeyword, kc
		}
	case schema.KeywordList:
		if mc := s.r.Entries(field); mc.Exists() {
			fs.kind, fs.mc = srcList, mc
		}
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

func (fs *fieldSrc) ordHash(o uint32, term func(uint32) string, n uint32) uint64 {
	if fs.hashes == nil {
		fs.hashes = make([]uint64, n)
	}
	h := fs.hashes[o]
	if h == 0 {
		h = hashString(term(o)) | 1 // never 0, the "not yet" mark
		fs.hashes[o] = h
	}
	return h
}

// eachHash calls fn with the hash of each of document d's values.
func (fs *fieldSrc) eachHash(d uint32, fn func(uint64)) {
	switch fs.kind {
	case srcKeyword:
		if o, ok := fs.kc.Ord(d); ok {
			fn(fs.ordHash(o, fs.kc.Term, fs.kc.NumTerms()))
		}
	case srcList:
		fs.buf = fs.mc.Ords(d, fs.buf[:0])
		for _, o := range fs.buf {
			fn(fs.ordHash(o, fs.mc.Term, fs.mc.NumTerms()))
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
	case AggCardinality:
		c := &cardColl{src: src, sketch: NewSketch(spec.precision)}
		if top && (src.kind == srcKeyword || src.kind == srcList) {
			n := src.kc.NumTerms()
			if src.kind == srcList {
				n = src.mc.NumTerms()
			}
			c.seen = make([]uint64, (n+63)/64)
		}
		return c
	case AggTerms:
		return &termsColl{s: s, spec: spec, src: src}
	case AggRange:
		return &rangeColl{s: s, spec: spec, src: src, counts: make([]int64, len(spec.ranges)), subs: make([][]collector, len(spec.ranges))}
	default: // histograms
		return &histColl{s: s, spec: spec, src: src, buckets: map[float64]*histBucket{}}
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

type cardColl struct {
	src    *fieldSrc
	sketch *Sketch
	seen   []uint64 // top level over ordinals: hashed once each in partial
}

func (c *cardColl) collect(d uint32) {
	if c.seen != nil {
		switch c.src.kind {
		case srcKeyword:
			if o, ok := c.src.kc.Ord(d); ok {
				c.seen[o/64] |= 1 << (o % 64)
			}
		case srcList:
			c.src.buf = c.src.mc.Ords(d, c.src.buf[:0])
			for _, o := range c.src.buf {
				c.seen[o/64] |= 1 << (o % 64)
			}
		}
		return
	}
	c.src.eachHash(d, c.sketch.Add)
}

func (c *cardColl) partial() *AggPartial {
	if c.seen != nil {
		term := c.src.kc.Term
		if c.src.kind == srcList {
			term = c.src.mc.Term
		}
		for w, word := range c.seen {
			for word != 0 {
				b := uint32(w*64) + uint32(bits.TrailingZeros64(word)) //nolint:gosec // ordinals are uint32
				word &= word - 1
				c.sketch.Add(hashString(term(b)) | 1)
			}
		}
		c.seen = nil
	}
	c.sketch.seal()
	return &AggPartial{Type: AggCardinality, Sketch: c.sketch}
}

// termsColl counts documents per value.
type termsColl struct {
	s    *segExec
	spec *aggSpec
	src  *fieldSrc

	counts  []int64       // by ordinal (keyword, list) or 0/1 (bool)
	subs    [][]collector // by ordinal, made on first use
	numbers map[float64]*histBucket
}

func (c *termsColl) slot(o uint32) {
	if c.counts == nil {
		n := uint32(2)
		switch c.src.kind {
		case srcKeyword:
			n = c.src.kc.NumTerms()
		case srcList:
			n = c.src.mc.NumTerms()
		}
		c.counts = make([]int64, n)
		if len(c.spec.subs) > 0 {
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
		if o, ok := c.src.kc.Ord(d); ok {
			c.slot(o)
			c.withSubs(o, d)
		}
	case srcList:
		c.src.buf = c.src.mc.Ords(d, c.src.buf[:0])
		for _, o := range c.src.buf {
			c.slot(o)
			c.withSubs(o, d)
		}
	case srcBool:
		if b, ok := c.src.boolOf(d); ok {
			o := uint32(0)
			if b {
				o = 1
			}
			c.slot(o)
			c.withSubs(o, d)
		}
	case srcNumber:
		if v, ok := c.src.nc.Value(d); ok {
			if v == 0 {
				v = 0 // one bucket for -0 and 0
			}
			if c.numbers == nil {
				c.numbers = map[float64]*histBucket{}
			}
			b := c.numbers[v]
			if b == nil {
				b = &histBucket{subs: c.s.newSubs(c.spec)}
				c.numbers[v] = b
			}
			b.count++
			collectSubs(b.subs, d)
		}
	}
}

// countDense counts a dense hit set (no sub-aggregations) from the dictionary's
// document frequencies, minus the documents not hit: cheaper than reading every hit's
// value when most of the segment matches.
func (c *termsColl) countDense(notHit *roaring.Bitmap) bool {
	if len(c.spec.subs) > 0 {
		return false
	}
	var kind segment.TermKind
	switch c.src.kind {
	case srcKeyword:
		kind = kindValue
	case srcList:
		kind = kindEntry
	default:
		return false
	}
	n := c.src.kc.NumTerms()
	if c.src.kind == srcList {
		n = c.src.mc.NumTerms()
	}
	c.counts = make([]int64, n)
	var o uint32
	c.s.r.Terms(c.spec.field, kind, "", func(_ string, df uint32) bool {
		if o < n {
			c.counts[o] = int64(df)
		}
		o++
		return true
	})
	it := notHit.Iterator()
	for it.HasNext() {
		d := it.Next()
		switch c.src.kind {
		case srcKeyword:
			if o, ok := c.src.kc.Ord(d); ok {
				c.counts[o]--
			}
		case srcList:
			c.src.buf = c.src.mc.Ords(d, c.src.buf[:0])
			for _, o := range c.src.buf {
				c.counts[o]--
			}
		}
	}
	return true
}

func (c *termsColl) partial() *AggPartial {
	p := &AggPartial{Type: AggTerms}
	if c.src.kind == srcNumber {
		for v, b := range c.numbers {
			p.Buckets = append(p.Buckets, &BucketPartial{Key: v, DocCount: b.count, Aggs: subPartials(c.spec, b.subs)})
		}
		return p
	}
	// The ordinals to return: every one with documents, or, when this segment is the
	// shard's only one (so its counts are the shard's), just the shard_size best by
	// count then ordinal, which is key order: the shard's cut, made before any term
	// is read, so a field of unique values materializes shard_size strings, not
	// millions. (Several segments still merge by key: global ordinals are Task 14's.)
	ords := make([]uint32, 0, 64)
	for o, n := range c.counts {
		if n > 0 {
			ords = append(ords, uint32(o))
		}
	}
	if c.s.p.segments == 1 && len(ords) > c.spec.shardSize {
		slices.SortFunc(ords, func(a, b uint32) int {
			if c.counts[a] != c.counts[b] {
				return int(c.counts[b] - c.counts[a])
			}
			return int(a) - int(b)
		})
		for _, o := range ords[c.spec.shardSize:] {
			p.OtherDocCount += c.counts[o]
		}
		ords = ords[:c.spec.shardSize]
		p.DocCountError = c.counts[ords[len(ords)-1]]
		slices.Sort(ords)
	}
	keys := c.keys(ords)
	for i, o := range ords {
		var subs []collector
		if c.subs != nil {
			subs = c.subs[o]
		}
		if subs == nil && len(c.spec.subs) > 0 {
			subs = c.s.newSubs(c.spec)
		}
		p.Buckets = append(p.Buckets, &BucketPartial{Key: keys[i], DocCount: c.counts[o], Aggs: subPartials(c.spec, subs)})
	}
	return p
}

// keys returns the values of ords (ascending): a keyword column's in one walk of its
// dictionary, each block decoded once.
func (c *termsColl) keys(ords []uint32) []any {
	out := make([]any, len(ords))
	switch c.src.kind {
	case srcKeyword:
		k := 0
		for k < len(ords) {
			from := k
			end := (ords[k]/segment.TermsPerBlock + 1) * segment.TermsPerBlock
			c.src.kc.EachTerm(ords[k], func(o uint32, term []byte) bool {
				if o == ords[k] {
					out[k] = string(term)
					k++
				}
				return k < len(ords) && ords[k] < end
			})
			if k == from {
				out[k] = c.src.kc.Term(ords[k])
				k++
			}
		}
	case srcList:
		for i, o := range ords {
			out[i] = c.src.mc.Term(o)
		}
	default:
		for i, o := range ords {
			out[i] = o == 1
		}
	}
	return out
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

// histColl counts documents per histogram bucket.
type histColl struct {
	s       *segExec
	spec    *aggSpec
	src     *fieldSrc
	buckets map[float64]*histBucket
}

func (c *histColl) collect(d uint32) {
	v, ok := c.src.number(d)
	if !ok {
		return
	}
	key, ok := bucketKey(c.spec, v)
	if !ok {
		return
	}
	b := c.buckets[key]
	if b == nil {
		if len(c.buckets) == MaxBuckets {
			if c.s.err == nil {
				c.s.fail(checkBuckets(&AggPartial{Buckets: make([]*BucketPartial, MaxBuckets+1)}, c.spec))
			}
			return
		}
		b = &histBucket{subs: c.s.newSubs(c.spec)}
		c.buckets[key] = b
	}
	b.count++
	collectSubs(b.subs, d)
}

func (c *histColl) partial() *AggPartial {
	p := &AggPartial{Type: c.spec.typ}
	for key, b := range c.buckets {
		p.Buckets = append(p.Buckets, &BucketPartial{Key: key, DocCount: b.count, Aggs: subPartials(c.spec, b.subs)})
	}
	return p
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
	default: // year
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

// aggregate collects every aggregation over a segment's hits.
func (s *segExec) aggregate(hits *roaring.Bitmap) map[string]*AggPartial {
	if len(s.p.aggs) == 0 {
		return nil
	}
	colls := make([]collector, len(s.p.aggs))
	var perDoc []collector
	var notHit *roaring.Bitmap
	dense := 2*hits.GetCardinality() >= uint64(s.n)
	for i, spec := range s.p.aggs {
		c := s.newCollector(spec, true)
		colls[i] = c
		if tc, ok := c.(*termsColl); ok && dense {
			if notHit == nil {
				notHit = s.flip(hits)
			}
			if tc.countDense(notHit) {
				continue
			}
		}
		if sc, ok := c.(*statsColl); ok && s.sv.Deletes.IsEmpty() && hits.GetCardinality() == uint64(s.n) && sc.src.kind == srcNumber {
			// Every document matched: the column's own stats are the answer.
			st := sc.src.nc.Stats()
			if st.Count > 0 {
				sc.st = StatsPartial{Count: int64(st.Count), Sum: st.Sum, Min: st.Min, Max: st.Max}
			}
			continue
		}
		perDoc = append(perDoc, c)
	}
	if len(perDoc) > 0 {
		it := hits.ManyIterator()
		buf := make([]uint32, 512)
		for {
			n := it.NextMany(buf)
			if n == 0 || s.checkCtx() {
				break
			}
			for _, d := range buf[:n] {
				for _, c := range perDoc {
					c.collect(d)
				}
			}
		}
	}
	if s.err != nil {
		return nil
	}
	out := make(map[string]*AggPartial, len(colls))
	for i, spec := range s.p.aggs {
		out[spec.name] = colls[i].partial()
	}
	return out
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
	case AggCardinality:
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

// sortTerms orders terms buckets: most documents first, then by key.
func sortTerms(bs []*BucketPartial) {
	slices.SortFunc(bs, func(a, b *BucketPartial) int {
		if a.DocCount != b.DocCount {
			if a.DocCount > b.DocCount {
				return -1
			}
			return 1
		}
		return cmpValue(a.Key, b.Key, false)
	})
}

// cutShard finishes a shard's partial: a terms list sorted and cut to shard_size (with
// its error bound), a histogram sorted by key.
func cutShard(p *AggPartial, spec *aggSpec) {
	switch spec.typ {
	case AggTerms:
		sortTerms(p.Buckets)
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
	// Value is a cardinality's estimate.
	Value int64
	// Truncated is set on a histogram cut at MaxBuckets buckets.
	Truncated bool
}

// Bucket is one bucket of a result.
type Bucket struct {
	// Key is a terms bucket's value (string, float64 or bool), a range's name, or a
	// histogram bucket's start (milliseconds, for a date_histogram).
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
	switch a.Type {
	case AggStats:
		return json.Marshal(struct {
			Count int64    `json:"count"`
			Min   *float64 `json:"min"`
			Max   *float64 `json:"max"`
			Avg   *float64 `json:"avg"`
			Sum   float64  `json:"sum"`
		}{a.Count, a.Min, a.Max, a.Avg, a.Sum})
	case AggCardinality:
		return json.Marshal(struct {
			Value int64 `json:"value"`
		}{a.Value})
	case AggTerms:
		return json.Marshal(struct {
			Error   int64     `json:"doc_count_error_upper_bound"`
			Other   int64     `json:"sum_other_doc_count"`
			Buckets []*Bucket `json:"buckets"`
		}{a.DocCountErrorUpperBound, a.SumOtherDocCount, nonNil(a.Buckets)})
	default:
		return json.Marshal(struct {
			Buckets   []*Bucket `json:"buckets"`
			Truncated bool      `json:"truncated,omitempty"`
		}{nonNil(a.Buckets), a.Truncated})
	}
}

func nonNil(bs []*Bucket) []*Bucket {
	if bs == nil {
		return []*Bucket{}
	}
	return bs
}

// MarshalJSON writes the bucket with its sub-aggregations inline, by name.
func (b *Bucket) MarshalJSON() ([]byte, error) {
	m := make(map[string]any, 6+len(b.Aggs))
	for name, sub := range b.Aggs {
		m[name] = sub
	}
	m["key"] = b.Key
	if b.KeyAsString != "" {
		m["key_as_string"] = b.KeyAsString
	}
	if b.From != nil {
		m["from"] = *b.From
	}
	if b.To != nil {
		m["to"] = *b.To
	}
	m["doc_count"] = b.DocCount
	if b.DocCountErrorUpperBound != 0 {
		m["doc_count_error_upper_bound"] = b.DocCountErrorUpperBound
	}
	return json.Marshal(m)
}
