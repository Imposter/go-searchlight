package search

import (
	"container/heap"
	"context"
	"maps"
	"math"
	"slices"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// Reduce merges shard results (from ExecuteShard with the same r) into one response:
// the best r.Size hits over every shard by the request's sort (ties: _id, then the
// shards' order), the summed total cut at TrackTotal, and the aggregations merged and
// finished. A nil result (a shard that failed) is skipped.
func Reduce(rs []*ShardResult, r *Request) *Response {
	return ReduceContext(context.Background(), rs, r)
}

// ReduceContext is Reduce under ctx's trace: its span is a child of the request's.
func ReduceContext(ctx context.Context, rs []*ShardResult, r *Request) *Response {
	start := time.Now()
	ctx, span := otel.Tracer(telemetry.ScopeName).Start(ctx, "search.reduce", trace.WithAttributes(
		attribute.String(telemetry.KeyIndex, r.Index), attribute.Int("shards", len(rs))))
	defer span.End()
	sorts := prepareSorts(r.Sort, nil, new(problems))
	var specs []*aggSpec
	for _, name := range slices.Sorted(maps.Keys(r.Aggs)) {
		if s := prepareAgg(name, r.Aggs[name], nil, "aggs."+name, false, new(problems)); s != nil {
			specs = append(specs, s)
		}
	}
	resp := &Response{TotalRelation: RelationEq}
	var live []*ShardResult
	for _, sr := range rs {
		if sr == nil {
			continue
		}
		live = append(live, sr)
		resp.Total += sr.Total
		if sr.TotalRelation == RelationGte {
			resp.TotalRelation = RelationGte
		}
		resp.TimedOut = resp.TimedOut || sr.TimedOut
	}
	track := int64(r.TrackTotal)
	switch r.TrackTotal {
	case 0:
		track = DefaultTrackTotal
	case TrackTotalNone:
		track = -1 // no bound to cut at: the total is what the shards confirmed
	}
	if track >= 0 && resp.Total > track {
		resp.Total, resp.TotalRelation = track, RelationGte
	}
	resp.Hits = mergeHits(live, sorts, r.Size)
	if r.Size > 0 && len(resp.Hits) == r.Size {
		resp.Next = slices.Clone(resp.Hits[len(resp.Hits)-1].Sort)
	}
	if len(specs) > 0 {
		resp.Aggs = make(map[string]*AggResult, len(specs))
		for _, spec := range specs {
			parts := make([]*AggPartial, 0, len(live))
			for _, sr := range live {
				if p := sr.Aggs[spec.name]; p != nil {
					parts = append(parts, p)
				}
			}
			resp.Aggs[spec.name] = reduceAgg(parts, spec)
		}
	}
	instruments().recordPhase(ctx, r.Index, "reduce", time.Since(start))
	span.SetAttributes(attribute.Int64("total", resp.Total), attribute.Int("hits", len(resp.Hits)))
	return resp
}

// hitMerge is a k-way merge of the shards' sorted hits.
type hitMerge struct {
	sorts []sortSpec
	lists [][]Hit
	pos   []int
	order []int
}

func (m *hitMerge) cmp(a, b int) int {
	ha, hb := &m.lists[a][m.pos[a]], &m.lists[b][m.pos[b]]
	for j, spec := range m.sorts {
		var va, vb any
		if j < len(ha.Sort) {
			va = ha.Sort[j]
		}
		if j < len(hb.Sort) {
			vb = hb.Sort[j]
		}
		if c := cmpValue(va, vb, spec.desc); c != 0 {
			return c
		}
	}
	return a - b
}

func (m *hitMerge) Len() int           { return len(m.order) }
func (m *hitMerge) Less(i, j int) bool { return m.cmp(m.order[i], m.order[j]) < 0 }
func (m *hitMerge) Swap(i, j int)      { m.order[i], m.order[j] = m.order[j], m.order[i] }
func (m *hitMerge) Push(x any)         { m.order = append(m.order, x.(int)) } //nolint:forcetypeassert,errcheck // only ints
func (m *hitMerge) Pop() any {
	last := m.order[len(m.order)-1]
	m.order = m.order[:len(m.order)-1]
	return last
}

func mergeHits(rs []*ShardResult, sorts []sortSpec, size int) []Hit {
	if size <= 0 {
		return nil
	}
	m := &hitMerge{sorts: sorts}
	for _, sr := range rs {
		if len(sr.Hits) > 0 {
			m.order = append(m.order, len(m.lists))
			m.lists = append(m.lists, sr.Hits)
		}
	}
	m.pos = make([]int, len(m.lists))
	heap.Init(m)
	var out []Hit
	for len(out) < size && m.Len() > 0 {
		i := m.order[0]
		out = append(out, m.lists[i][m.pos[i]])
		m.pos[i]++
		if m.pos[i] == len(m.lists[i]) {
			heap.Pop(m)
		} else {
			heap.Fix(m, 0)
		}
	}
	return out
}

// reduceAgg merges the shards' partials of one aggregation and finishes them.
func reduceAgg(parts []*AggPartial, spec *aggSpec) *AggResult {
	if spec.typ == AggTerms {
		return reduceTerms(parts, spec)
	}
	merged := &AggPartial{Type: spec.typ}
	for _, p := range parts {
		mergePartial(merged, p, spec)
	}
	return finish(merged, spec)
}

// reduceTerms merges terms lists, tracking which shards returned each term for its
// error bound (see aggs.go).
func reduceTerms(parts []*AggPartial, spec *aggSpec) *AggResult {
	type acc struct {
		b    *BucketPartial
		seen []bool
	}
	accs := map[any]*acc{}
	var all []*acc
	res := &AggResult{Type: AggTerms}
	var docs int64
	for i, p := range parts {
		res.DocCountErrorUpperBound += p.DocCountError
		docs += p.OtherDocCount
		for _, b := range p.Buckets {
			docs += b.DocCount
			a := accs[b.Key]
			if a == nil {
				a = &acc{b: cloneBucket(b, spec), seen: make([]bool, len(parts))}
				accs[b.Key] = a
				all = append(all, a)
			} else {
				mergeBucket(a.b, b, spec)
			}
			a.seen[i] = true
		}
	}
	buckets := make([]*BucketPartial, 0, len(all))
	errs := make(map[*BucketPartial]int64, len(all))
	for _, a := range all {
		var e int64
		for i, p := range parts {
			if !a.seen[i] {
				e += p.DocCountError
			}
		}
		errs[a.b] = e
		if a.b.DocCount >= int64(spec.minDoc) {
			buckets = append(buckets, a.b)
		}
	}
	sortTerms(buckets)
	if len(buckets) > spec.size {
		buckets = buckets[:spec.size]
	}
	var returned int64
	for _, b := range buckets {
		returned += b.DocCount
		out := finishBucket(b, spec)
		out.DocCountErrorUpperBound = errs[b]
		res.Buckets = append(res.Buckets, out)
	}
	res.SumOtherDocCount = docs - returned
	return res
}

func finishBucket(b *BucketPartial, spec *aggSpec) *Bucket {
	out := &Bucket{Key: b.Key, DocCount: b.DocCount}
	if len(spec.subs) > 0 {
		out.Aggs = make(map[string]*AggResult, len(spec.subs))
		for _, sub := range spec.subs {
			p := b.Aggs[sub.name]
			if p == nil {
				p = &AggPartial{Type: sub.typ}
			}
			out.Aggs[sub.name] = finish(p, sub)
		}
	}
	return out
}

// finish turns a merged partial into its result.
func finish(p *AggPartial, spec *aggSpec) *AggResult {
	res := &AggResult{Type: spec.typ}
	switch spec.typ {
	case AggStats:
		st := p.Stats
		if st == nil {
			st = &StatsPartial{}
		}
		res.Count, res.Sum = st.Count, st.Sum
		if st.Count > 0 {
			lo, hi, avg := st.Min, st.Max, st.Sum/float64(st.Count)
			res.Min, res.Max, res.Avg = &lo, &hi, &avg
		}
	case AggCardinality:
		if p.Sketch != nil {
			res.Value = p.Sketch.Estimate()
		}
	case AggRange:
		for i, r := range spec.ranges {
			var b *BucketPartial
			if i < len(p.Buckets) {
				b = p.Buckets[i]
			} else {
				b = &BucketPartial{}
			}
			out := finishBucket(b, spec)
			out.Key = r.key
			if r.hasFrom {
				from := r.from
				out.From = &from
			}
			if r.hasTo {
				to := r.to
				out.To = &to
			}
			res.Buckets = append(res.Buckets, out)
		}
	case AggTerms:
		return reduceTerms([]*AggPartial{p}, spec)
	default: // histograms
		bs := slices.Clone(p.Buckets)
		slices.SortFunc(bs, func(a, b *BucketPartial) int { return cmpValue(a.Key, b.Key, false) })
		if spec.minDoc <= 0 && len(bs) > 1 {
			bs = fillHistogram(bs, spec)
		}
		for _, b := range bs {
			if b.DocCount < int64(spec.minDoc) {
				continue
			}
			if len(res.Buckets) == MaxBuckets {
				// Each shard refused more than MaxBuckets; together they can still pass
				// it: keep the first MaxBuckets, and say so.
				res.Truncated = true
				break
			}
			out := finishBucket(b, spec)
			if spec.typ == AggDateHistogram {
				if k, ok := b.Key.(float64); ok && math.Abs(k) <= maxDateMillis {
					out.KeyAsString = time.UnixMilli(int64(k)).UTC().Format("2006-01-02T15:04:05.000Z07:00")
				}
			}
			res.Buckets = append(res.Buckets, out)
		}
	}
	return res
}

// fillHistogram adds the empty buckets between the first and the last (min_doc_count
// 0, Elasticsearch's default), unless that would pass MaxBuckets.
func fillHistogram(bs []*BucketPartial, spec *aggSpec) []*BucketPartial {
	first, ok1 := bs[0].Key.(float64)
	last, ok2 := bs[len(bs)-1].Key.(float64)
	if !ok1 || !ok2 {
		return bs
	}
	have := make(map[float64]*BucketPartial, len(bs))
	for _, b := range bs {
		k, _ := b.Key.(float64)
		have[k] = b
	}
	var keys []float64
	if spec.calendar == "" {
		j0 := math.Round((first - spec.offset) / spec.interval)
		j1 := math.Round((last - spec.offset) / spec.interval)
		if j1-j0+1 > MaxBuckets {
			return bs
		}
		for j := j0; j <= j1; j++ {
			keys = append(keys, j*spec.interval+spec.offset)
		}
	} else {
		t := time.UnixMilli(int64(first - spec.offset)).UTC()
		end := time.UnixMilli(int64(last - spec.offset)).UTC()
		for !t.After(end) {
			if len(keys) == MaxBuckets {
				return bs
			}
			keys = append(keys, float64(t.UnixMilli())+spec.offset)
			t = calendarNext(t, spec.calendar)
		}
	}
	out := make([]*BucketPartial, 0, len(keys))
	for _, k := range keys {
		if b := have[k]; b != nil {
			out = append(out, b)
			delete(have, k)
			continue
		}
		out = append(out, &BucketPartial{Key: k})
	}
	if len(have) > 0 {
		// A key off the grid (float rounding): keep every real bucket rather than lose one.
		return bs
	}
	return out
}
