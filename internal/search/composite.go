package search

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// A composite aggregation pages through every combination of its sources' values, as
// Elasticsearch's does: {"sources": [{"brand": {"terms": {"field": "brand"}}}, {"day":
// {"date_histogram": {"field": "at", "calendar_interval": "day"}}}], "size": 100,
// "after": {"brand": "acme", "day": 1700000000000}}. Its buckets are ordered by their
// keys, source by source (each ascending unless its order is desc), and a page is the
// size buckets after "after" (from the first without it); the response's after_key is
// the page's last bucket's key, which the next page passes as "after".
//
// It is exact at every level: each segment keeps the size least keys after "after" it
// has seen (once it holds twice that many it drops all but the size least, and from
// then on any key past the last it kept), each shard and the reduce merge those and
// keep the size least again. A key one part dropped had size keys before it in that
// part alone, so it cannot be among the size least of the union; every key kept is
// counted by every part that holds it. A document without a value for some source is in
// no bucket (Elasticsearch's missing_bucket false); a keyword_list document is in one
// bucket per combination of its values.

// CompositeSource is one source of a composite aggregation's keys (Agg.Sources): Name
// keys its value in each bucket's key; Type is AggTerms (a value of Field: a keyword,
// text, keyword_list, number, date or bool field, or _id), AggHistogram (Field's number
// in buckets of Interval from Offset) or AggDateHistogram (a date in buckets of
// Calendar, or of Interval milliseconds, from Offset); Desc orders it descending.
type CompositeSource struct {
	Name     string
	Type     string
	Field    string
	Desc     bool    `json:",omitempty"`
	Interval float64 `json:",omitempty"`
	Offset   float64 `json:",omitempty"`
	Calendar string  `json:",omitempty"`
}

// CompositeKey is a composite bucket's key, or a result's after_key: each source's
// value (a string, float64 or bool) by its name, in the sources' order.
type CompositeKey struct {
	Names  []string
	Values []any
}

// MarshalJSON writes the key as an object, its members in the sources' order.
func (k *CompositeKey) MarshalJSON() ([]byte, error) { return k.appendJSON(nil) }

func (k *CompositeKey) appendJSON(dst []byte) ([]byte, error) {
	dst = append(dst, '{')
	var err error
	for i, name := range k.Names {
		if i > 0 {
			dst = append(dst, ',')
		}
		if dst, err = appendJSONString(dst, name); err != nil {
			return nil, err
		}
		if dst, err = appendJSONValue(append(dst, ':'), k.Values[i]); err != nil {
			return nil, err
		}
	}
	return append(dst, '}'), nil
}

// compositeSpec is a composite aggregation, checked.
type compositeSpec struct {
	sources []compSource
	size    int
	// after is the page's start, typed per source; nil from the first bucket.
	after []any
}

// compSource is one source, checked: hist is a histogram or date_histogram source's
// buckets, as bucketKey reads them.
type compSource struct {
	name  string
	typ   string
	field string
	ftype schema.FieldType
	desc  bool
	hist  *aggSpec
}

// compositeKeys are the settings each source type of a composite takes beside order.
var compositeKeys = map[string][]string{
	AggTerms:         {"field"},
	AggHistogram:     {"field", "interval", "offset"},
	AggDateHistogram: {"field", "fixed_interval", "calendar_interval", "interval", "offset"},
}

// parseComposite reads a composite aggregation's settings: {"sources": [{name: {type:
// {...}}}, ...], "size": n, "after": {name: value, ...}}.
func parseComposite(a *Agg, raw json.RawMessage, loc string, ps *problems) {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err != nil {
		ps.add(loc, `a composite aggregation's settings are an object: {"sources": [{"name": {"terms": {"field": "brand"}}}], "size": 10, "after": {...}}`)
		return
	}
	for _, key := range slices.Sorted(maps.Keys(body)) {
		val := body[key]
		at := loc + "." + key
		switch key {
		case "sources":
			var items []json.RawMessage
			if json.Unmarshal(val, &items) != nil {
				ps.add(at, `sources is a list of {"name": {"terms"|"histogram"|"date_histogram": {...}}}`)
				continue
			}
			for i, item := range items {
				if src, ok := parseCompositeSource(item, fmt.Sprintf("%s.%d", at, i), ps); ok {
					a.Sources = append(a.Sources, src)
				}
			}
		case "size":
			n, ok := intOf(val)
			if !ok || n < 1 {
				ps.add(at, "size is a whole number of at least 1")
				continue
			}
			a.Size = n
		case "after":
			var after map[string]any
			if decodeNumbers(val, &after) != nil {
				ps.add(at, "after is the previous page's after_key: an object of each source's value by name")
				continue
			}
			for name, v := range after {
				if n, ok := v.(json.Number); ok {
					f, err := n.Float64()
					if err != nil {
						ps.add(at+"."+name, "not a number a float64 holds")
						continue
					}
					after[name] = f
				}
			}
			a.After = after
		default:
			ps.add(at, "unknown key: composite takes sources, size and after")
		}
	}
}

// parseCompositeSource reads one source: {name: {type: {"field": ..., "order": ...,
// ...}}}.
func parseCompositeSource(raw json.RawMessage, loc string, ps *problems) (CompositeSource, bool) {
	var named map[string]json.RawMessage
	if json.Unmarshal(raw, &named) != nil || len(named) != 1 {
		ps.add(loc, `a source is {"name": {"terms"|"histogram"|"date_histogram": {...}}}`)
		return CompositeSource{}, false
	}
	var src CompositeSource
	var typed map[string]json.RawMessage
	for name, v := range named {
		src.Name = name
		loc += "." + name
		if name == "" || json.Unmarshal(v, &typed) != nil || len(typed) != 1 {
			ps.add(loc, `a source is named, and holds one of terms, histogram and date_histogram`)
			return src, false
		}
	}
	var settings json.RawMessage
	for typ, v := range typed {
		src.Type, settings = typ, v
		loc += "." + typ
	}
	allowed, ok := compositeKeys[src.Type]
	if !ok {
		ps.add(loc, "a composite source is terms, histogram or date_histogram")
		return src, false
	}
	var body map[string]json.RawMessage
	if json.Unmarshal(settings, &body) != nil {
		ps.add(loc, "a source's settings are an object")
		return src, false
	}
	rest := make(map[string]json.RawMessage, len(body))
	for _, key := range slices.Sorted(maps.Keys(body)) {
		val := body[key]
		switch {
		case key == "order":
			var dir string
			if json.Unmarshal(val, &dir) != nil || (dir != "asc" && dir != "desc") {
				ps.add(loc+".order", `order is "asc" or "desc"`)
				continue
			}
			src.Desc = dir == "desc"
		case slices.Contains(allowed, key):
			rest[key] = val
		default:
			ps.add(loc+"."+key, "unknown key: a %s source takes %s and order", src.Type, joinList(allowed))
		}
	}
	tmp := Agg{Type: src.Type}
	restRaw, err := json.Marshal(rest)
	if err != nil {
		ps.add(loc, "a source's settings are an object")
		return src, false
	}
	parseAggBody(&tmp, restRaw, loc, ps)
	src.Field, src.Interval, src.Offset, src.Calendar = tmp.Field, tmp.Interval, tmp.Offset, tmp.Calendar
	return src, true
}

func joinList(xs []string) string {
	var b bytes.Buffer
	for i, x := range xs {
		switch {
		case i == 0:
		case i == len(xs)-1:
			b.WriteString(" and ")
		default:
			b.WriteString(", ")
		}
		b.WriteString(x)
	}
	return b.String()
}

// prepareComposite checks composite aggregation a, at at, against m into s.comp. With no
// mapping (the reduce) neither the fields nor after's values are checked.
func prepareComposite(s *aggSpec, a Agg, m *schema.Mapping, at string, ps *problems) {
	c := &compositeSpec{size: a.Size}
	if c.size == 0 {
		c.size = DefaultCompositeSize
	}
	if c.size < 1 || c.size > MaxCompositeSize {
		ps.add(at+".size", "a composite page holds from 1 to %d buckets: page with after", MaxCompositeSize)
	}
	switch {
	case len(a.Sources) == 0:
		ps.add(at+".sources", "a composite aggregation needs sources")
	case len(a.Sources) > MaxCompositeSources:
		ps.add(at+".sources", "a composite aggregation holds at most %d sources", MaxCompositeSources)
	}
	for i, src := range a.Sources {
		loc := fmt.Sprintf("%s.sources.%d.%s.%s", at, i, src.Name, src.Type)
		if j := slices.IndexFunc(a.Sources[:i], func(o CompositeSource) bool { return o.Name == src.Name }); j >= 0 {
			ps.add(fmt.Sprintf("%s.sources.%d", at, i), "%q is source %d's name already: each source names its value in the bucket keys", src.Name, j)
			continue
		}
		if src.Name == "" {
			ps.add(fmt.Sprintf("%s.sources.%d", at, i), "a source needs a name")
			continue
		}
		cs := compSource{name: src.Name, typ: src.Type, field: src.Field, ftype: fieldType(m, src.Field), desc: src.Desc}
		_, sub := m.NestedPath(src.Field)
		numeric := cs.ftype == 0 || cs.ftype == schema.Number || cs.ftype == schema.Date
		switch {
		case src.Field == "":
			ps.add(loc+".field", "a source needs a field")
		case sub:
			ps.add(loc+".field", "%q is a nested field's sub-field: a composite reads top-level fields", src.Field)
		case cs.ftype.IsPoint() || cs.ftype == schema.Nested:
			ps.add(loc+".field", "%q is a %s: a composite source reads a keyword, text, keyword_list, number, date or bool field", src.Field, cs.ftype)
		case src.Type != AggTerms && !numeric:
			ps.add(loc+".field", "%q is a %s: %s takes a number or date field", src.Field, cs.ftype, src.Type)
		}
		switch src.Type {
		case AggTerms:
		case AggHistogram:
			if !(src.Interval > 0) || math.IsInf(src.Interval, 0) {
				ps.add(loc+".interval", "a histogram source needs an interval above 0")
			}
			cs.hist = &aggSpec{typ: src.Type, interval: src.Interval, offset: src.Offset}
		case AggDateHistogram:
			switch {
			case src.Calendar != "" && src.Interval != 0:
				ps.add(loc, "a date_histogram source takes calendar_interval or fixed_interval, not both")
			case src.Calendar != "":
				if !slices.Contains(calendarUnits, src.Calendar) {
					ps.add(loc+".calendar_interval", "calendar_interval is one of minute, hour, day, week, month, quarter, year")
				}
			case src.Interval > 0 && !math.IsInf(src.Interval, 0):
			default:
				ps.add(loc, "a date_histogram source needs calendar_interval or fixed_interval")
			}
			cs.hist = &aggSpec{typ: src.Type, interval: src.Interval, offset: src.Offset, calendar: src.Calendar}
		default:
			ps.add(loc, "a composite source is terms, histogram or date_histogram")
		}
		c.sources = append(c.sources, cs)
	}
	if a.After != nil {
		c.after = prepareCompositeAfter(a.After, c.sources, m != nil, at+".after", ps)
	}
	s.comp = c
}

// prepareCompositeAfter orders after's values by source, and when check is set checks
// each is of its source's keys: a string for terms on a keyword, text, keyword_list
// field or _id, a bool for terms on a bool field, a number otherwise.
func prepareCompositeAfter(after map[string]any, srcs []compSource, check bool, loc string, ps *problems) []any {
	out := make([]any, len(srcs))
	for i, src := range srcs {
		v, ok := after[src.name]
		at := loc + "." + src.name
		if !ok {
			ps.add(at, "after names every source: pass the previous page's after_key as it is")
			continue
		}
		typed := !check
		switch {
		case !check:
		case src.typ == AggTerms && (src.ftype == schema.Keyword || src.ftype == schema.Text || src.ftype == schema.KeywordList):
			_, typed = v.(string)
		case src.typ == AggTerms && src.ftype == schema.Bool:
			_, typed = v.(bool)
		default:
			_, typed = v.(float64)
		}
		if !typed {
			ps.add(at, "%q's after value is not one of its keys: pass the previous page's after_key as it is", src.name)
			continue
		}
		out[i] = v
	}
	for name := range after {
		if !slices.ContainsFunc(srcs, func(s compSource) bool { return s.name == name }) {
			ps.add(loc+"."+name, "%q names no source", name)
		}
	}
	return out
}

// cmpComposite orders composite keys source by source.
func (c *compositeSpec) cmp(a, b []any) int {
	for i := range c.sources {
		if r := cmpValue(a[i], b[i], c.sources[i].desc); r != 0 {
			return r
		}
	}
	return 0
}

// compositeKey encodes a key's values as one string, by which partials merge: their
// JSON array.
func compositeKey(dst []byte, vals []any) []byte {
	dst = append(dst, '[')
	for i, v := range vals {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst, _ = appendJSONValue(dst, v) // strings, finite float64s and bools only
	}
	return append(dst, ']')
}

// compBucket is one composite bucket in one segment: its key, documents (the last one
// counted plus 1 in last, so a document with repeated combinations counts once) and
// sub-aggregations' collectors.
type compBucket struct {
	key   string
	keys  []any
	count int64
	last  uint32
	subs  []collector
}

// compColl collects a composite aggregation over a segment's documents: the buckets of
// their keys after the page's start, at most twice its size before it prunes them (see
// the type's comment above).
type compColl struct {
	s       *segExec
	spec    *aggSpec
	srcs    []*fieldSrc
	buckets map[string]*compBucket
	// ceiling, once pruned, is the last key kept: any key past it is dropped.
	ceiling []any
	vals    [][]any
	tuple   []any
	buf     []byte
	ords    []uint32
}

func (s *segExec) newCompColl(spec *aggSpec) *compColl {
	c := &compColl{s: s, spec: spec, buckets: map[string]*compBucket{}}
	n := len(spec.comp.sources)
	c.srcs, c.vals, c.tuple = make([]*fieldSrc, n), make([][]any, n), make([]any, n)
	for i, src := range spec.comp.sources {
		c.srcs[i] = s.source(src.field, src.ftype)
	}
	return c
}

func (c *compColl) collect(d uint32) {
	for i := range c.srcs {
		c.vals[i] = c.values(i, d, c.vals[i][:0])
		if len(c.vals[i]) == 0 {
			return
		}
	}
	c.combine(0, d)
}

// values appends document d's values of source i.
func (c *compColl) values(i int, d uint32, out []any) []any {
	src, fs := &c.spec.comp.sources[i], c.srcs[i]
	switch fs.kind {
	case srcKeyword:
		if o, ok := fs.kc.Ord(d); ok {
			out = append(out, fs.kc.Term(o))
		}
	case srcList:
		c.ords = fs.mc.Ords(d, c.ords[:0])
		for _, o := range c.ords {
			out = append(out, fs.mc.Term(o))
		}
	case srcBool:
		if b, ok := fs.boolOf(d); ok {
			out = append(out, b)
		}
	case srcNumber:
		v, ok := fs.nc.Value(d)
		if !ok {
			break
		}
		v = positiveZero(v)
		if src.hist != nil {
			k, ok := bucketKey(src.hist, v)
			if !ok {
				if c.s.err == nil {
					c.s.fail(tooManyBuckets(c.spec))
				}
				break
			}
			v = positiveZero(k)
		}
		out = append(out, v)
	}
	return out
}

// combine adds document d to the bucket of every combination of its values from source
// i on.
func (c *compColl) combine(i int, d uint32) {
	if i == len(c.vals) {
		c.add(d)
		return
	}
	for _, v := range c.vals[i] {
		c.tuple[i] = v
		c.combine(i+1, d)
	}
}

func (c *compColl) add(d uint32) {
	comp := c.spec.comp
	if comp.after != nil && comp.cmp(c.tuple, comp.after) <= 0 {
		return
	}
	if c.ceiling != nil && comp.cmp(c.tuple, c.ceiling) > 0 {
		return
	}
	c.buf = compositeKey(c.buf[:0], c.tuple)
	b := c.buckets[string(c.buf)]
	if b == nil {
		key := string(c.buf)
		b = &compBucket{key: key, keys: slices.Clone(c.tuple), subs: c.s.newSubs(c.spec)}
		c.buckets[key] = b
		if len(c.buckets) > 2*comp.size+16 {
			c.prune()
			if c.buckets[key] == nil {
				return
			}
		}
	}
	if b.last == d+1 {
		return
	}
	b.last = d + 1
	b.count++
	collectSubs(b.subs, d)
}

// prune keeps the size least buckets, and from then on drops any key past the last.
func (c *compColl) prune() {
	comp := c.spec.comp
	all := slices.Collect(maps.Values(c.buckets))
	topK(all, comp.size, func(a, b *compBucket) int { return comp.cmp(a.keys, b.keys) })
	for _, b := range all[comp.size:] {
		delete(c.buckets, b.key)
	}
	c.ceiling = all[comp.size-1].keys
}

func (c *compColl) partial() *AggPartial {
	p := &AggPartial{Type: AggComposite, Buckets: make([]*BucketPartial, 0, len(c.buckets))}
	for _, b := range c.buckets {
		p.Buckets = append(p.Buckets, &BucketPartial{Key: b.key, Keys: b.keys, DocCount: b.count, Aggs: subPartials(c.spec, b.subs)})
	}
	return p
}

// cutComposite sorts a composite's buckets by key and keeps its size least.
func cutComposite(bs []*BucketPartial, spec *aggSpec) []*BucketPartial {
	comp := spec.comp
	topK(bs, comp.size, func(a, b *BucketPartial) int { return comp.cmp(a.Keys, b.Keys) })
	return bs[:min(len(bs), comp.size)]
}

// finishComposite turns a merged composite partial into its result: its page of
// buckets, each keyed by source name, and the page's after_key.
func finishComposite(p *AggPartial, spec *aggSpec) *AggResult {
	res := &AggResult{Type: AggComposite}
	names := make([]string, len(spec.comp.sources))
	for i, src := range spec.comp.sources {
		names[i] = src.name
	}
	for _, b := range cutComposite(slices.Clone(p.Buckets), spec) {
		out := finishBucket(b, spec)
		out.Key = &CompositeKey{Names: names, Values: b.Keys}
		res.Buckets = append(res.Buckets, out)
	}
	res.AfterKey = lastCompositeKey(res.Buckets)
	return res
}

// lastCompositeKey is the key of the last of a composite's buckets, nil with none.
func lastCompositeKey(bs []*Bucket) *CompositeKey {
	if len(bs) == 0 {
		return nil
	}
	k, _ := bs[len(bs)-1].Key.(*CompositeKey)
	return k
}

// validComposite checks a composite partial of a, from a cache: at most its size of
// buckets, each keyed by one value per source, its Key their encoding.
func validComposite(p *AggPartial, a *Agg) error {
	size := a.Size
	if size == 0 {
		size = DefaultCompositeSize
	}
	if len(p.Buckets) > size {
		return fmt.Errorf("composite page past its size")
	}
	var buf []byte
	for _, b := range p.Buckets {
		if b == nil || len(b.Keys) != len(a.Sources) {
			return fmt.Errorf("composite bucket key")
		}
		for _, v := range b.Keys {
			switch v := v.(type) {
			case string, bool:
			case float64:
				if math.IsInf(v, 0) || math.IsNaN(v) {
					return fmt.Errorf("composite bucket key")
				}
			default:
				return fmt.Errorf("composite bucket key")
			}
		}
		buf = compositeKey(buf[:0], b.Keys)
		if k, ok := b.Key.(string); !ok || k != string(buf) {
			return fmt.Errorf("composite bucket key encoding")
		}
	}
	return nil
}
