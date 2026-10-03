package search

import (
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"strconv"
	"sync/atomic"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// prepared is a request checked against a mapping and compiled for execution: built
// once per shard search, read-only afterwards (segments share it).
type prepared struct {
	req     *Request
	mapping *schema.Mapping
	root    *pnode
	// sorts are the request's sort keys, then the _id tie-breaker (unless a sort key
	// is _id, after which nothing can tie): a total order.
	sorts []sortSpec
	// after is SearchAfter, typed per sort key; nil when not paging.
	after []any
	size  int
	// need is how many matches the shard must confirm before it may stop verifying
	// candidates (TrackTotal+1: one past proves "more than"); -1: every one; 0: none.
	need   int64
	aggs   []*aggSpec
	fields []string
	// segments is how many segments the shard searches.
	segments int
	// confirmed counts the matches the shard's segments have confirmed so far, sure
	// or verified: shared, so segments stop counting together.
	confirmed atomic.Int64
}

// sortKind is how a sort key reads its values.
type sortKind uint8

const (
	sortNone    sortKind = iota // an unmapped field: every document is missing it
	sortNumber                  // number and date: the number column
	sortKeyword                 // keyword and text: the keyword column's ordinals
	sortBool                    // bool: the true and false postings
	sortID                      // the exact document id
)

type sortSpec struct {
	field string
	desc  bool
	kind  sortKind
}

// aggSpec is one aggregation, checked.
type aggSpec struct {
	name      string
	typ       string
	field     string
	ftype     schema.FieldType // 0: not in the mapping
	size      int
	shardSize int
	minDoc    int
	ranges    []rangeSpec
	interval  float64
	offset    float64
	calendar  string
	precision uint8
	subs      []*aggSpec
}

type rangeSpec struct {
	key      string
	from, to float64 // -Inf and +Inf when open
	hasFrom  bool
	hasTo    bool
}

func fieldType(m *schema.Mapping, field string) schema.FieldType {
	if field == IDField {
		return schema.Keyword
	}
	if m == nil {
		return 0
	}
	return m.Fields[field]
}

// prepare checks r against m and compiles it.
func prepare(r *Request, m *schema.Mapping) (*prepared, error) {
	var ps problems
	p := &prepared{req: r, mapping: m, size: r.Size, fields: r.Fields}
	if r.Size < 0 || r.Size > MaxSize {
		ps.add("size", "size is from 0 to %d", MaxSize)
	}
	switch {
	case r.TrackTotal == 0:
		p.need = DefaultTrackTotal + 1
	case r.TrackTotal == TrackTotalNone:
		p.need = 0
	case r.TrackTotal < 0:
		p.need = -1
	default:
		p.need = int64(r.TrackTotal) + 1
	}
	p.sorts = prepareSorts(r.Sort, m, &ps)
	if len(r.SearchAfter) > 0 {
		p.after = prepareAfter(r.SearchAfter, p.sorts, &ps)
	}
	for _, name := range slices.Sorted(maps.Keys(r.Aggs)) {
		if s := prepareAgg(name, r.Aggs[name], m, "aggs."+name, false, &ps); s != nil {
			p.aggs = append(p.aggs, s)
		}
	}
	if len(ps) > 0 {
		return nil, &RequestError{Problems: ps}
	}
	p.root = compileNode(r.Query, requestID(r))
	return p, nil
}

func prepareSorts(fields []SortField, m *schema.Mapping, ps *problems) []sortSpec {
	out := make([]sortSpec, 0, len(fields)+1)
	for i, f := range fields {
		spec := sortSpec{field: f.Field, desc: f.Desc}
		switch f.Field {
		case IDField:
			spec.kind = sortID
		default:
			switch fieldType(m, f.Field) {
			case schema.Number, schema.Date:
				spec.kind = sortNumber
			case schema.Keyword, schema.Text:
				spec.kind = sortKeyword
			case schema.Bool:
				spec.kind = sortBool
			case schema.KeywordList:
				ps.add(fmt.Sprintf("sort.%d", i), "cannot sort on %q: a keyword_list has many values", f.Field)
				continue
			default:
				spec.kind = sortNone
			}
		}
		out = append(out, spec)
		if spec.kind == sortID {
			return out // ids are unique: nothing after can tie
		}
	}
	return append(out, sortSpec{field: IDField, kind: sortID})
}

// prepareAfter types search_after's values per sort key: a float64, string or bool,
// or nil for a document missing the key.
func prepareAfter(after []any, sorts []sortSpec, ps *problems) []any {
	if len(after) != len(sorts) {
		ps.add("search_after", "search_after holds %d values, one per sort key and the _id tie-breaker (a page's next)", len(sorts))
		return nil
	}
	out := make([]any, len(after))
	for i, v := range after {
		loc := fmt.Sprintf("search_after.%d", i)
		s := sorts[i]
		if v == nil {
			if s.kind == sortID {
				ps.add(loc, "an _id is text")
			}
			continue
		}
		switch s.kind {
		case sortNumber:
			f, ok := toFloat(v)
			if !ok {
				ps.add(loc, "%q sorts by number: a number or null", s.field)
				continue
			}
			out[i] = f
		case sortKeyword, sortID:
			text, ok := v.(string)
			if !ok {
				ps.add(loc, "%q sorts by text: text or null", s.field)
				continue
			}
			out[i] = text
		case sortBool:
			b, ok := v.(bool)
			if !ok {
				ps.add(loc, "%q sorts by bool: true, false or null", s.field)
				continue
			}
			out[i] = b
		default:
			ps.add(loc, "%q is not in the mapping: every document misses it, so null", s.field)
		}
	}
	return out
}

// toFloat reads a finite number given as any Go number or json.Number.
func toFloat(v any) (float64, bool) {
	var f float64
	switch x := v.(type) {
	case float64:
		f = x
	case float32:
		f = float64(x)
	case int:
		f = float64(x)
	case int64:
		f = float64(x)
	case int32:
		f = float64(x)
	case uint64:
		f = float64(x)
	case uint32:
		f = float64(x)
	case json.Number:
		var err error
		if f, err = strconv.ParseFloat(string(x), 64); err != nil {
			return 0, false
		}
	default:
		return 0, false
	}
	return f, !math.IsNaN(f) && !math.IsInf(f, 0)
}

var calendarUnits = []string{"minute", "hour", "day", "week", "month", "quarter", "year"}

func prepareAgg(name string, a Agg, m *schema.Mapping, loc string, sub bool, ps *problems) *aggSpec {
	s := &aggSpec{name: name, typ: a.Type, field: a.Field, ftype: fieldType(m, a.Field)}
	at := loc + "." + a.Type
	if a.Field == "" {
		ps.add(at+".field", "an aggregation needs a field")
		return nil
	}
	isMetric := a.Type == AggStats || a.Type == AggCardinality
	if sub && !isMetric {
		ps.add(loc, "a sub-aggregation is a metric: stats or cardinality")
		return nil
	}
	numeric := s.ftype == 0 || s.ftype == schema.Number || s.ftype == schema.Date
	switch a.Type {
	case AggTerms:
		s.size = a.Size
		if s.size <= 0 {
			s.size = DefaultTermsSize
		}
		if s.size > MaxBuckets {
			ps.add(at+".size", "size is at most %d", MaxBuckets)
		}
		s.shardSize = a.ShardSize
		if s.shardSize <= 0 {
			s.shardSize = s.size*3/2 + 10
		}
		s.shardSize = max(s.shardSize, s.size)
		s.minDoc = 1
		if a.MinDocCount != nil {
			if *a.MinDocCount < 1 {
				ps.add(at+".min_doc_count", "a terms aggregation's min_doc_count is at least 1")
			}
			s.minDoc = *a.MinDocCount
		}
	case AggRange:
		if !numeric {
			ps.add(at+".field", "%q is a %s: range takes a number or date field", a.Field, s.ftype)
		}
		if len(a.Ranges) == 0 {
			ps.add(at+".ranges", "a range aggregation needs ranges")
		}
		for _, r := range a.Ranges {
			rs := rangeSpec{key: r.Key, from: math.Inf(-1), to: math.Inf(1)}
			if r.From != nil {
				rs.from, rs.hasFrom = *r.From, true
			}
			if r.To != nil {
				rs.to, rs.hasTo = *r.To, true
			}
			if rs.key == "" {
				rs.key = rangeKey(rs)
			}
			s.ranges = append(s.ranges, rs)
		}
	case AggHistogram:
		if !numeric {
			ps.add(at+".field", "%q is a %s: histogram takes a number or date field", a.Field, s.ftype)
		}
		if a.Interval <= 0 || math.IsInf(a.Interval, 0) || math.IsNaN(a.Interval) {
			ps.add(at+".interval", "a histogram needs an interval above 0")
		}
		s.interval, s.offset = a.Interval, a.Offset
		if a.MinDocCount != nil {
			s.minDoc = *a.MinDocCount
		}
	case AggDateHistogram:
		if !numeric {
			ps.add(at+".field", "%q is a %s: date_histogram takes a date or number field", a.Field, s.ftype)
		}
		switch {
		case a.Calendar != "" && a.Interval != 0:
			ps.add(at, "a date_histogram takes calendar_interval or fixed_interval, not both")
		case a.Calendar != "":
			if !slices.Contains(calendarUnits, a.Calendar) {
				ps.add(at+".calendar_interval", "calendar_interval is one of minute, hour, day, week, month, quarter, year")
			}
			s.calendar = a.Calendar
		case a.Interval > 0 && !math.IsInf(a.Interval, 0):
			s.interval = a.Interval
		default:
			ps.add(at, "a date_histogram needs calendar_interval or fixed_interval")
		}
		s.offset = a.Offset
		if a.MinDocCount != nil {
			s.minDoc = *a.MinDocCount
		}
	case AggStats:
		if !numeric {
			ps.add(at+".field", "%q is a %s: stats takes a number or date field", a.Field, s.ftype)
		}
	case AggCardinality:
		p := a.Precision
		if p == 0 {
			p = DefaultPrecision
		}
		if p < MinPrecision || p > MaxPrecision {
			ps.add(at+".precision", "precision is from %d to %d", MinPrecision, MaxPrecision)
		}
		s.precision = uint8(min(max(p, MinPrecision), MaxPrecision))
	default:
		ps.add(loc, "unknown aggregation type %q: use terms, range, histogram, date_histogram, stats or cardinality", a.Type)
		return nil
	}
	if len(a.Aggs) > 0 {
		if isMetric || sub {
			ps.add(loc+".aggs", "only a bucket aggregation holds sub-aggregations, one level deep")
		} else {
			for _, subName := range slices.Sorted(maps.Keys(a.Aggs)) {
				if ss := prepareAgg(subName, a.Aggs[subName], m, loc+".aggs."+subName, true, ps); ss != nil {
					s.subs = append(s.subs, ss)
				}
			}
		}
	}
	return s
}

// rangeKey names a range bucket as Elasticsearch does: "from-to", * for an open end.
func rangeKey(r rangeSpec) string {
	from, to := "*", "*"
	if r.hasFrom {
		from = numberKey(r.from)
	}
	if r.hasTo {
		to = numberKey(r.to)
	}
	return from + "-" + to
}

func numberKey(f float64) string {
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// requestID identifies a request while it runs (its address): the shards of one
// request count a leaf's use once between them.
func requestID(r *Request) uintptr {
	return reflect.ValueOf(r).Pointer()
}

// checkBuckets refuses a shard's histogram past MaxBuckets, as Elasticsearch refuses
// a search with too many buckets.
func checkBuckets(p *AggPartial, spec *aggSpec) error {
	if (spec.typ == AggHistogram || spec.typ == AggDateHistogram) && len(p.Buckets) > MaxBuckets {
		return &RequestError{Problems: []query.Problem{{
			Loc:     "aggs." + spec.name + "." + spec.typ,
			Message: fmt.Sprintf("more than %d buckets: widen the interval or narrow the query", MaxBuckets),
		}}}
	}
	return nil
}
