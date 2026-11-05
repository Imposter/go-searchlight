package search

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// A top_metrics aggregation reads its metrics from the documents first by its sort, as
// Elasticsearch's does: {"metrics": [{"field": "price"}], "sort": {"changed_at":
// "desc"}, "size": 1}. Each segment keeps its size best documents (a bounded insertion,
// the size being at most MaxTopMetricsSize), each shard and the reduce merge those
// lists and keep the size best again: exact at every level, as the best of a union is
// the best of its parts' best. A document without the sort field's value sorts after
// every document with one; equal sort values go to the least _id (exact, byte order),
// so the answer is the same however the documents are spread over segments and shards.

// TopPartial is one document a top_metrics partial holds: its sort value (a float64,
// nil when it has none), its id, and its metrics' values in the aggregation's order (a
// float64, string or bool; nil for none).
type TopPartial struct {
	Sort    any    `json:"sort"`
	ID      string `json:"id"`
	Metrics []any  `json:"metrics"`
}

// TopMetric is one document of a top_metrics result: its sort value, and its metrics'
// values by field (nil for a document without one).
type TopMetric struct {
	Sort    []any
	Metrics map[string]any
}

// topSpec is a top_metrics aggregation, checked.
type topSpec struct {
	sort     string
	sortType schema.FieldType
	desc     bool
	metrics  []string
	types    []schema.FieldType
	size     int
}

// parseTopMetrics reads a top_metrics aggregation's settings: {"metrics": {"field": f}
// or a list of them, "sort": a sort key as a search's sort takes one, "size": n}.
func parseTopMetrics(a *Agg, raw json.RawMessage, loc string, ps *problems) {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err != nil {
		ps.add(loc, `a top_metrics aggregation's settings are an object: {"metrics": [{"field": "price"}], "sort": {"date": "desc"}, "size": 1}`)
		return
	}
	for _, key := range slices.Sorted(maps.Keys(body)) {
		val := body[key]
		at := loc + "." + key
		switch key {
		case "metrics":
			var items []json.RawMessage
			if !bytes.HasPrefix(bytes.TrimSpace(val), []byte("[")) {
				items = []json.RawMessage{val}
			} else if json.Unmarshal(val, &items) != nil {
				ps.add(at, `metrics is {"field": name}, or a list of them`)
				continue
			}
			for i, item := range items {
				var m struct {
					Field string `json:"field"`
				}
				dec := json.NewDecoder(bytes.NewReader(item))
				dec.DisallowUnknownFields()
				if dec.Decode(&m) != nil || m.Field == "" {
					ps.add(fmt.Sprintf("%s.%d", at, i), `a metric is {"field": name}`)
					continue
				}
				a.Metrics = append(a.Metrics, m.Field)
			}
		case "sort":
			var local problems
			sorts := parseSort(val, &local)
			for _, p := range local {
				ps.add(loc+"."+p.Loc, "%s", p.Message)
			}
			switch {
			case len(local) > 0:
			case len(sorts) != 1:
				ps.add(at, "a top_metrics sorts by one key, a number or date field")
			default:
				a.TopSort = &sorts[0]
			}
		case "size":
			n, ok := intOf(val)
			if !ok || n < 1 {
				ps.add(at, "size is a whole number of at least 1")
				continue
			}
			a.Size = n
		default:
			ps.add(at, "unknown key: top_metrics takes metrics, sort and size")
		}
	}
}

// prepareTopMetrics checks top_metrics aggregation a, at at, against m into s.top. With
// no mapping (the reduce) every field passes.
func prepareTopMetrics(s *aggSpec, a Agg, m *schema.Mapping, at string, ps *problems) {
	t := &topSpec{size: a.Size}
	if t.size == 0 {
		t.size = 1
	}
	if t.size < 1 || t.size > MaxTopMetricsSize {
		ps.add(at+".size", "a top_metrics size is from 1 to %d", MaxTopMetricsSize)
	}
	if f := a.TopSort; f == nil {
		ps.add(at+".sort", "a top_metrics needs a sort: one number or date field")
	} else {
		t.sort, t.desc, t.sortType = f.Field, f.Desc, fieldType(m, f.Field)
		loc := at + ".sort." + f.Field
		_, sub := m.NestedPath(f.Field)
		switch {
		case f.Mode != "" || f.Where != nil:
			ps.add(loc, "a top_metrics sorts by a top-level field: mode and where sort by a nested field's sub-field")
		case f.Field == IDField:
			ps.add(loc, "a top_metrics sorts by a number or date field: equal values go to the least _id already")
		case sub:
			ps.add(loc, "%q is a nested field's sub-field: a top_metrics sorts by a top-level number or date field", f.Field)
		case t.sortType != 0 && t.sortType != schema.Number && t.sortType != schema.Date:
			ps.add(loc, "%q is a %s: a top_metrics sorts by a number or date field", f.Field, t.sortType)
		}
	}
	switch {
	case len(a.Metrics) == 0:
		ps.add(at+".metrics", "a top_metrics needs metrics: the fields it reads")
	case len(a.Metrics) > MaxTopMetrics:
		ps.add(at+".metrics", "a top_metrics reads at most %d metrics", MaxTopMetrics)
	}
	for i, f := range a.Metrics {
		loc := fmt.Sprintf("%s.metrics.%d", at, i)
		ft := fieldType(m, f)
		_, sub := m.NestedPath(f)
		switch {
		case slices.Index(a.Metrics, f) < i:
			ps.add(loc, "%q is already a metric: a field is read once", f)
		case sub:
			ps.add(loc, "%q is a nested field's sub-field: a top_metrics reads top-level fields", f)
		case ft == schema.KeywordList || ft == schema.Nested || ft.IsPoint():
			ps.add(loc, "%q is a %s: a top_metrics reads one value per document, of a number, date, keyword, text or bool field (or _id)", f, ft)
		}
		t.metrics, t.types = append(t.metrics, f), append(t.types, ft)
	}
	s.top = t
}

// topColl keeps a segment's (or a bucket's) best documents for a top_metrics: at most
// its size, best first.
type topColl struct {
	s     *segExec
	spec  *aggSpec
	src   *fieldSrc
	best  []topCand
	ranks []uint32
}

// topCand is a document kept: its sort value, when it has one, and its ordinal.
type topCand struct {
	v   float64
	has bool
	d   uint32
}

func (s *segExec) newTopColl(spec *aggSpec) *topColl {
	return &topColl{s: s, spec: spec, src: s.source(spec.top.sort, spec.top.sortType), best: make([]topCand, 0, spec.top.size)}
}

// cmp orders candidates best first: by sort value (none last), then by id.
func (c *topColl) cmp(a, b topCand) int {
	switch {
	case a.has != b.has:
		if a.has {
			return -1
		}
		return 1
	case a.has && a.v != b.v:
		if c.spec.top.desc {
			return cmp.Compare(b.v, a.v)
		}
		return cmp.Compare(a.v, b.v)
	}
	if c.ranks == nil {
		c.ranks = ranksFor(c.s.sv.ID, c.s.r)
	}
	return cmp.Compare(c.ranks[a.d], c.ranks[b.d])
}

func (c *topColl) collect(d uint32) {
	v, has := c.src.number(d)
	cand := topCand{v: positiveZero(v), has: has, d: d}
	n := len(c.best)
	if n == c.spec.top.size && c.cmp(cand, c.best[n-1]) >= 0 {
		return
	}
	i := sortSearch(n, func(i int) bool { return c.cmp(cand, c.best[i]) < 0 })
	if n < c.spec.top.size {
		c.best = append(c.best, topCand{})
	}
	copy(c.best[i+1:], c.best[i:])
	c.best[i] = cand
}

func (c *topColl) partial() *AggPartial {
	p := &AggPartial{Type: AggTopMetrics}
	if len(c.best) == 0 {
		return p
	}
	t := c.spec.top
	srcs := make([]*fieldSrc, len(t.metrics))
	for i, f := range t.metrics {
		if f != IDField {
			srcs[i] = c.s.source(f, t.types[i])
		}
	}
	p.Top = make([]*TopPartial, 0, len(c.best))
	for _, cand := range c.best {
		id, err := c.s.r.ID(cand.d)
		if err != nil {
			c.s.fail(err)
			return &AggPartial{Type: AggTopMetrics}
		}
		tp := &TopPartial{ID: id, Metrics: make([]any, len(t.metrics))}
		if cand.has {
			tp.Sort = cand.v
		}
		for i, f := range t.metrics {
			if f == IDField {
				tp.Metrics[i] = id
				continue
			}
			tp.Metrics[i] = srcs[i].value(cand.d)
		}
		p.Top = append(p.Top, tp)
	}
	return p
}

// value is document d's one value of the field as a top_metrics reads it: a float64
// (number, date), a string (keyword, text; normalized as aggregations read it), a bool,
// or nil for none.
func (fs *fieldSrc) value(d uint32) any {
	switch fs.kind {
	case srcNumber:
		if v, ok := fs.nc.Value(d); ok {
			return positiveZero(v)
		}
	case srcKeyword:
		if o, ok := fs.kc.Ord(d); ok {
			return fs.kc.Term(o)
		}
	case srcBool:
		if b, ok := fs.boolOf(d); ok {
			return b
		}
	}
	return nil
}

// cmpTop orders top_metrics documents best first: by sort value (none last), then by
// id.
func cmpTop(a, b *TopPartial, desc bool) int {
	if c := cmpValue(a.Sort, b.Sort, desc); c != 0 {
		return c
	}
	return strings.Compare(a.ID, b.ID)
}

// mergeTop is the best size of a and b's documents (each best first), in a new list:
// neither is changed.
func mergeTop(a, b []*TopPartial, spec *aggSpec) []*TopPartial {
	if len(b) == 0 {
		return a
	}
	out := make([]*TopPartial, 0, min(len(a)+len(b), spec.top.size))
	for len(out) < spec.top.size && (len(a) > 0 || len(b) > 0) {
		if len(b) == 0 || (len(a) > 0 && cmpTop(a[0], b[0], spec.top.desc) <= 0) {
			out, a = append(out, a[0]), a[1:]
		} else {
			out, b = append(out, b[0]), b[1:]
		}
	}
	return out
}

// finishTop turns a merged top_metrics partial into its result's documents.
func finishTop(p *AggPartial, spec *aggSpec) []TopMetric {
	out := make([]TopMetric, 0, len(p.Top))
	for _, tp := range p.Top {
		tm := TopMetric{Sort: []any{tp.Sort}, Metrics: make(map[string]any, len(spec.top.metrics))}
		for i, f := range spec.top.metrics {
			if i < len(tp.Metrics) {
				tm.Metrics[f] = tp.Metrics[i]
			} else {
				tm.Metrics[f] = nil
			}
		}
		out = append(out, tm)
	}
	return out
}

// appendTopJSON appends a top_metrics result as Elasticsearch writes it: {"top":
// [{"sort": [v], "metrics": {"field": v, ...}}, ...]}, the metrics in name order.
func appendTopJSON(dst []byte, top []TopMetric) ([]byte, error) {
	dst = append(dst, `{"top":[`...)
	var err error
	for i, tm := range top {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = append(dst, `{"sort":[`...)
		for j, v := range tm.Sort {
			if j > 0 {
				dst = append(dst, ',')
			}
			if dst, err = appendJSONValue(dst, v); err != nil {
				return nil, err
			}
		}
		dst = append(dst, `],"metrics":{`...)
		for j, name := range slices.Sorted(maps.Keys(tm.Metrics)) {
			if j > 0 {
				dst = append(dst, ',')
			}
			if dst, err = appendJSONString(dst, name); err != nil {
				return nil, err
			}
			if dst, err = appendJSONValue(append(dst, ':'), tm.Metrics[name]); err != nil {
				return nil, err
			}
		}
		dst = append(dst, "}}"...)
	}
	return append(dst, "]}"...), nil
}

// validTop checks a top_metrics partial of a, from a cache: at most its size of
// documents, each of its metrics, sorted best first.
func validTop(p *AggPartial, a *Agg) error {
	size := a.Size
	if size == 0 {
		size = 1
	}
	if p.Buckets != nil || p.Stats != nil || p.Sketch != nil || p.Value != nil || p.Distinct != nil || len(p.Top) > size {
		return fmt.Errorf("top_metrics shape")
	}
	desc := a.TopSort != nil && a.TopSort.Desc
	for i, tp := range p.Top {
		if tp == nil || len(tp.Metrics) != len(a.Metrics) {
			return fmt.Errorf("top_metrics document")
		}
		switch tp.Sort.(type) {
		case nil, float64:
		default:
			return fmt.Errorf("top_metrics sort value")
		}
		for _, v := range tp.Metrics {
			switch v.(type) {
			case nil, float64, string, bool:
			default:
				return fmt.Errorf("top_metrics metric value")
			}
		}
		if i > 0 && cmpTop(p.Top[i-1], tp, desc) >= 0 {
			return fmt.Errorf("top_metrics documents out of order")
		}
	}
	return nil
}
