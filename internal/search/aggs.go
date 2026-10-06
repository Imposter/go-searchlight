package search

import (
	"encoding/json"
	"math"
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

	// ords is a segment's partial by local ordinal (a keyword or list field's terms or
	// cardinality), which the shard merges by global ordinal into the fields above.
	ords *ordPartial
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
	// ords are the field's global ordinals (keyword and list fields): this segment's
	// local-to-global map, document frequencies, and every global ordinal's hash.
	ords   *globalOrds
	global []uint32
	dfs    []uint32
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
	}
	if list, ok := ordKind(t); ok && (fs.kind == srcKeyword || fs.kind == srcList) {
		if g := s.p.ords[ordField{field, list}]; g != nil {
			fs.ords, fs.global, fs.dfs = g, g.segs[s.seg], g.dfs[s.seg]
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

// ordHash is local ordinal o's value hash.
func (fs *fieldSrc) ordHash(o uint32) uint64 {
	return fs.ords.hashes[fs.global[o]]
}

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
	case AggCardinality:
		c := &cardColl{s: s, src: src, sketch: NewSketch(spec.precision)}
		if top && src.ords != nil {
			c.seen = make([]uint64, (len(src.global)+63)/64)
		}
		return c
	case AggTerms:
		return &termsColl{s: s, spec: spec, src: src}
	case AggRange:
		return &rangeColl{s: s, spec: spec, src: src, counts: make([]int64, len(spec.ranges)), subs: make([][]collector, len(spec.ranges))}
	default: // histograms
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

type cardColl struct {
	s      *segExec
	src    *fieldSrc
	sketch *Sketch
	seen   []uint64 // top level over local ordinals, merged by global ordinal
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

// countDense marks the ordinals of a dense hit set from the dictionary's document
// frequencies, minus the documents not hit.
func (c *cardColl) countDense(notHit *roaring.Bitmap) bool {
	if c.seen == nil {
		return false
	}
	counts := c.src.denseCounts(notHit)
	for o, n := range counts {
		if n > 0 {
			c.seen[o/64] |= 1 << (o % 64)
		}
	}
	return true
}

func (c *cardColl) partial() *AggPartial {
	if c.seen != nil {
		return &AggPartial{Type: AggCardinality, ords: &ordPartial{seg: c.s.seg, seen: c.seen}}
	}
	c.sketch.seal()
	return &AggPartial{Type: AggCardinality, Sketch: c.sketch}
}

// denseCounts is each local ordinal's documents among a dense hit set: its document
// frequency, minus the documents not hit that hold it.
func (fs *fieldSrc) denseCounts(notHit *roaring.Bitmap) []int64 {
	counts := make([]int64, len(fs.dfs))
	for o, df := range fs.dfs {
		counts[o] = int64(df)
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
		if c.src.ords != nil {
			n = uint32(len(c.src.global)) //nolint:gosec // a segment holds at most 2^32 terms
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
	if len(c.spec.subs) > 0 || c.src.ords == nil {
		return false
	}
	c.counts = c.src.denseCounts(notHit)
	return true
}

func (c *termsColl) partial() *AggPartial {
	p := &AggPartial{Type: AggTerms}
	switch {
	case c.src.kind == srcNumber:
		for v, b := range c.numbers {
			p.Buckets = append(p.Buckets, &BucketPartial{Key: v, DocCount: b.count, Aggs: subPartials(c.spec, b.subs)})
		}
	case c.src.ords != nil:
		p.ords = &ordPartial{seg: c.s.seg, counts: c.counts, subs: c.subs}
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

// eachValue calls fn with the value of each document of docs that has one.
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
// span few enough buckets for a bucketer, else in a map by key.
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

// denseCounter is a collector that can count a dense hit set from the index (every
// document's values) minus the documents not hit, cheaper than reading every hit's;
// countDense reports false when it cannot (sub-aggregations, say).
type denseCounter interface {
	countDense(notHit *roaring.Bitmap) bool
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
		if dc, ok := c.(denseCounter); ok && dense {
			if notHit == nil {
				notHit = s.flip(hits)
			}
			if dc.countDense(notHit) {
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
