package search

import (
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"strconv"
	"sync/atomic"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// prepared is a request checked against a mapping and compiled for execution: built
// once per shard search, read-only afterwards (segments share it).
type prepared struct {
	req     *Request
	mapping *schema.Mapping
	root    *pnode
	// post is the post_filter, compiled, when the request has aggregations, which see
	// root's matches; without them root is the query and the post_filter together.
	post *pnode
	// sorts are the request's sort keys, then the _id tie-breaker (unless a sort key
	// is _id, after which nothing can tie): a total order.
	sorts []sortSpec
	// after is SearchAfter, typed per sort key; nil when not paging.
	after []any
	// size is how many hits the shard returns: From+Size, the page and every hit
	// before it.
	size int
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
	// ords are the global ordinals of the keyword and list fields the aggregations
	// count by ordinal: top-level terms, and every cardinality.
	ords map[ordField]*globalOrds
	// cut is set when the shard's merge of the segments' partials ran out of time and
	// left aggregations empty: the result is timed out.
	cut atomic.Bool
	// failed is the first error of the merge that is not its context's (a replay's
	// panic, say), which fails the shard.
	failed atomic.Pointer[error]
}

// ordField names one field's global ordinals: its values, or its list entries.
type ordField struct {
	field string
	list  bool
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
	// nested is a sort on a nested field's sub-field: by the least or greatest value
	// among each document's objects (nestCol).
	nested *nestedSort
}

// nestedSort is a sort key on a sub-field of nested field path: by the greatest value
// among a document's objects when max, else the least, of the objects its where
// chooses (whereNode, compiled into where; every object without one).
type nestedSort struct {
	path      string
	max       bool
	whereNode query.Node
	where     *pnode
}

// aggSpec is one aggregation, checked.
type aggSpec struct {
	name string
	// loc is where the request gives the aggregation ("aggs.brands").
	loc       string
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
	// threshold is a cardinality's threshold (0: none), and reached is set once a
	// segment of the shard has seen that many values, so the others stop: shared by
	// a top-level cardinality's segments, or one under a filter or global aggregation
	// (nil under a filters aggregation's buckets, or a bucket aggregation's).
	threshold int
	reached   *atomic.Bool
	// terms is a terms aggregation's include and exclude, compiled; include and exclude
	// are them as given.
	terms            *termsFilter
	include, exclude *TermsFilter
	// collect is a terms aggregation's collect mode as given: "", CollectDepthFirst
	// or CollectBreadthFirst (see deferSubs).
	collect string
	// order is a terms aggregation's order resolved (termsorder.go): nil for the
	// default, _count descending.
	order *termsOrder
	// top is a top_metrics aggregation's settings, comp a composite's.
	top  *topSpec
	comp *compositeSpec
	subs []*aggSpec
	// keys are a filter, filters or global aggregation's buckets' keys, in order: nil
	// alone for filter and global; filters' keys sorted, then its other bucket's.
	// queries are the filters of the buckets that have one, in keys' order, and
	// filters those compiled (by prepare alone: the reduce needs none).
	keys     []any
	queries  []query.Node
	filters  []*pnode
	hasOther bool
	// nested is the nested field an aggregation on a sub-field collects the objects of,
	// "" for any other; where is its where, which chooses them, and wherePlan that
	// compiled (nil: every object).
	nested    string
	where     query.Node
	wherePlan *pnode
}

// wrapper reports whether the aggregation is a filter, filters or global one.
func (s *aggSpec) wrapper() bool { return singleBucket(s.typ) }

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
	if r.From != 0 {
		switch {
		case r.From < 0 || r.From > MaxResultWindow-max(r.Size, 0):
			ps.add("from", "from is at least 0, and from + size at most %d", MaxResultWindow)
		case len(r.SearchAfter) > 0:
			ps.add("from", "from must be 0 with search_after")
		case r.Size > 0:
			p.size += r.From
		}
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
	if len(r.Fields) > MaxFields {
		ps.add("fields", "fields: at most %d names", MaxFields)
	}
	if n := countAggs(r.Aggs); n > MaxAggs {
		ps.add(aggsLoc(r.aggsKey), "a search holds at most %d aggregations, sub-aggregations included and those under a filters aggregation counted once per bucket; this one holds %d", MaxAggs, n)
	} else {
		for _, name := range slices.Sorted(maps.Keys(r.Aggs)) {
			if s := prepareAgg(name, r.Aggs[name], m, aggsLoc(r.aggsKey)+"."+name, &ps); s != nil {
				p.aggs = append(p.aggs, s)
			}
		}
	}
	if len(ps) > 0 {
		return nil, &RequestError{Problems: ps}
	}
	id := requestID(r)
	switch {
	case r.PostFilter == nil:
		p.root = compileNode(r.Query, id)
	case len(p.aggs) == 0:
		p.root = compileNode(&query.All{Children: []query.Node{r.Query, r.PostFilter}}, id)
	default:
		p.root = compileNode(r.Query, id)
		p.post = compileNode(r.PostFilter, id)
	}
	compileFilters(p.aggs, id)
	for i := range p.sorts {
		if ns := p.sorts[i].nested; ns != nil && ns.whereNode != nil {
			ns.where = compileNode(ns.whereNode, id)
		}
	}
	shareThresholds(p.aggs)
	return p, nil
}

// shareThresholds gives each cardinality with a threshold among specs, and under their
// filter and global aggregations, the flag its segments share (aggSpec.reached). Those
// under a filters aggregation are collected once per bucket, and those under a bucket
// aggregation once per bucket too, so they share none.
func shareThresholds(specs []*aggSpec) {
	for _, s := range specs {
		switch {
		case s.typ == AggCardinality && s.threshold > 0:
			s.reached = new(atomic.Bool)
		case s.typ == AggFilter || s.typ == AggGlobal:
			shareThresholds(s.subs)
		}
	}
}

// compileFilters compiles the filters of specs' filter and filters aggregations, and
// the where of each aggregation on a sub-field.
func compileFilters(specs []*aggSpec, req uint64) {
	for _, s := range specs {
		for _, q := range s.queries {
			s.filters = append(s.filters, compileNode(q, req))
		}
		if s.where != nil {
			s.wherePlan = compileNode(s.where, req)
		}
		compileFilters(s.subs, req)
	}
}

// countAggs counts aggs and their sub-aggregations, those of a filters aggregation
// once per bucket (each key, and the other bucket), as each bucket collects them
// apart: what bounds the collectors a segment builds.
func countAggs(aggs map[string]Agg) int {
	n := len(aggs)
	for name := range aggs {
		a := aggs[name]
		buckets := 1
		if a.Type == AggFilters {
			buckets = max(1, len(a.Filters))
			if a.OtherBucketKey != "" {
				buckets++
			}
		}
		n += buckets * countAggs(a.Aggs)
	}
	return n
}

// prepareSorts types the sort keys and adds the _id tie-breaker. It refuses more than
// MaxSortKeys keys, a field sorted on twice, and any key after _id (ids are unique, so
// nothing after one can tie).
func prepareSorts(fields []SortField, m *schema.Mapping, ps *problems) []sortSpec {
	if len(fields) > MaxSortKeys {
		ps.add(fmt.Sprintf("sort.%d", MaxSortKeys), "a search sorts by at most %d keys", MaxSortKeys)
		fields = fields[:MaxSortKeys]
	}
	out := make([]sortSpec, 0, len(fields)+1)
	for i, f := range fields {
		if i > 0 && fields[i-1].Field == IDField {
			ps.add(fmt.Sprintf("sort.%d", i), "nothing sorts after _id: ids are unique, so no key after one can tie")
			return out
		}
		if j := slices.IndexFunc(fields[:i], func(g SortField) bool { return g.Field == f.Field }); j >= 0 {
			ps.add(fmt.Sprintf("sort.%d", i), "%q is already sort key %d: a field sorts once", f.Field, j)
			continue
		}
		spec := sortSpec{field: f.Field, desc: f.Desc}
		loc := fmt.Sprintf("sort.%d", i)
		t := fieldType(m, f.Field)
		path, sub := m.NestedPath(f.Field)
		switch {
		case t.IsPoint():
			ps.add(loc, "cannot sort on %q: a point field has many points of two coordinates", f.Field)
			continue
		case t == schema.Nested:
			ps.add(loc, "cannot sort on %q: a nested field sorts by one of its sub-fields (%s.name)", f.Field, f.Field)
			continue
		case m != nil && !sub && (f.Mode != "" || f.Where != nil):
			ps.add(loc, "mode and where sort by a nested field's sub-field, and %q is none", f.Field)
			continue
		case sub && f.Mode != "" && f.Mode != SortModeMin && f.Mode != SortModeMax:
			ps.add(loc+"."+f.Field+".mode", "mode is %q or %q", SortModeMin, SortModeMax)
			continue
		case sub && t != schema.Number && t != schema.Date && t != schema.Keyword && t != schema.Text:
			ps.add(loc, "cannot sort on %q: a sub-field sorts by number, date, keyword or text, not %s", f.Field, t)
			continue
		case sub:
			spec.nested = &nestedSort{path: path, max: f.Mode == SortModeMax || (f.Mode == "" && f.Desc), whereNode: f.Where}
		}
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
	}
	if len(out) > 0 && out[len(out)-1].kind == sortID {
		return out
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

// toFloat reads a finite number given as any Go number or json.Number, as
// [analysis.Number] does.
func toFloat(v any) (float64, bool) { return analysis.Number(v) }

var calendarUnits = []string{"minute", "hour", "day", "week", "month", "quarter", "year"}

// prepareAgg checks top-level aggregation a, given at loc, against m, and compiles it
// (its filters aside: see compileFilters).
func prepareAgg(name string, a Agg, m *schema.Mapping, loc string, ps *problems) *aggSpec {
	return prepareAggUnder(name, a, m, loc, "", "", ps)
}

// prepareAggUnder is prepareAgg for an aggregation under a terms aggregation on a
// sub-field of nested field under ("" under any other, or at the top level): a metric
// or a terms aggregation, on a top-level field (the bucket's documents) or on one of
// under's sub-fields (the bucket's objects).
func prepareAggUnder(name string, a Agg, m *schema.Mapping, loc, parent, under string, ps *problems) *aggSpec {
	s := &aggSpec{name: name, loc: loc, typ: a.Type, field: a.Field, ftype: fieldType(m, a.Field)}
	at := loc + "." + a.Type
	wrapper := singleBucket(a.Type)
	if a.Field == "" && !wrapper && a.Type != AggTopMetrics && a.Type != AggComposite {
		ps.add(at+".field", "an aggregation needs a field")
		return nil
	}
	if s.ftype.IsPoint() && !wrapper {
		ps.add(at+".field", "%q is a point field: no aggregation takes one (filter on it with box, in a filter aggregation)", a.Field)
		return nil
	}
	if s.ftype == schema.Nested && !wrapper {
		ps.add(at+".field", "%q is a nested field: aggregate one of its sub-fields (%s.name)", a.Field, a.Field)
		return nil
	}
	if path, sub := m.NestedPath(a.Field); sub && !wrapper {
		switch {
		case a.Type != AggTerms && a.Type != AggStats && a.Type != AggMin && a.Type != AggMax && a.Type != AggCardinality:
			ps.add(at+".field", "%q is a nested field's sub-field: terms, stats, min, max and cardinality aggregate one, %s does not", a.Field, a.Type)
			return nil
		case under != "" && path != under:
			ps.add(at+".field", "under a terms aggregation on a sub-field of %q, an aggregation reads %q's sub-fields (the bucket's objects) or top-level fields (its documents): %q is a sub-field of %q", under, under, a.Field, path)
			return nil
		case under == "" && parent != "" && !singleBucket(parent):
			ps.add(loc, "an aggregation on a sub-field is top-level, under filter, filters or global, or under a terms aggregation on a sub-field of its nested field: a bucket's metrics aggregate its documents' own fields")
			return nil
		case len(a.Aggs) > 0 && a.Type != AggTerms:
			ps.add(loc+"."+aggsLoc(a.aggsKey), "only a terms aggregation on a sub-field holds sub-aggregations")
			return nil
		}
		s.nested, s.where = path, a.Where
	} else if a.Where != nil && m != nil {
		ps.add(at+".where", "where chooses a nested field's objects: %q is no sub-field of one", a.Field)
		return nil
	}
	isMetric := metricType(a.Type)
	switch {
	case parent == "":
	case a.Type == AggComposite:
		ps.add(loc, "composite is a top-level aggregation only: its pages walk the search's matches")
		return nil
	case a.Type == AggGlobal:
		ps.add(loc, "global is a top-level aggregation only: it counts every document, whatever holds it")
		return nil
	case singleBucket(parent) && wrapper:
		ps.add(loc, "%s does not nest under %s: filter, filters and global hold terms, range, histogram, date_histogram, stats, cardinality, min and max", a.Type, parent)
		return nil
	case !singleBucket(parent) && a.Type == AggTerms && (under != "" || m == nil):
	case !singleBucket(parent) && !isMetric:
		ps.add(loc, "a sub-aggregation is a metric (stats, cardinality, min, max or top_metrics), or a terms aggregation under a terms aggregation on a sub-field")
		return nil
	}
	numeric := s.ftype == 0 || s.ftype == schema.Number || s.ftype == schema.Date
	if a.Type != AggTerms && (a.Include != nil || a.Exclude != nil) {
		key := "include"
		if a.Include == nil {
			key = "exclude"
		}
		ps.add(at+"."+key, "include and exclude choose a terms aggregation's values: %s takes neither", a.Type)
	}
	switch {
	case a.CollectMode == "":
	case a.Type != AggTerms:
		ps.add(at+".collect_mode", "collect_mode is a terms aggregation's: %s takes none", a.Type)
	case a.CollectMode != CollectDepthFirst && a.CollectMode != CollectBreadthFirst:
		ps.add(at+".collect_mode", "collect_mode is %q or %q", CollectDepthFirst, CollectBreadthFirst)
	}
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
		s.shardSize = min(max(s.shardSize, s.size), MaxBuckets)
		s.minDoc = 1
		if a.MinDocCount != nil {
			if *a.MinDocCount < 1 {
				ps.add(at+".min_doc_count", "a terms aggregation's min_doc_count is at least 1")
			}
			s.minDoc = *a.MinDocCount
		}
		s.include, s.exclude, s.collect = a.Include, a.Exclude, a.CollectMode
		s.terms = compileTermsFilter(a.Include, a.Exclude, s.ftype, at, ps)
	case AggRange:
		if !numeric {
			ps.add(at+".field", "%q is a %s: range takes a number or date field", a.Field, s.ftype)
		}
		if len(a.Ranges) == 0 {
			ps.add(at+".ranges", "a range aggregation needs ranges")
		}
		if len(a.Ranges) > MaxRanges {
			ps.add(at+".ranges", "a range aggregation holds at most %d ranges", MaxRanges)
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
	case AggStats, AggMin, AggMax:
		if !numeric {
			ps.add(at+".field", "%q is a %s: %s takes a number or date field", a.Field, s.ftype, a.Type)
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
		switch {
		case a.Threshold == 0:
		case a.Threshold < 0 || a.Threshold > MaxThreshold:
			ps.add(at+".threshold", "threshold is from 1 to %d", MaxThreshold)
		case a.Precision != 0:
			ps.add(at+".threshold", "a cardinality takes precision or threshold, not both: a threshold counts exactly")
		default:
			s.threshold = a.Threshold
		}
	case AggFilter:
		s.keys, s.queries = []any{nil}, []query.Node{a.Filter}
	case AggFilters:
		switch {
		case len(a.Filters) == 0:
			ps.add(at+".filters", "a filters aggregation needs filters")
		case len(a.Filters) > MaxFilters:
			ps.add(at+".filters", "a filters aggregation holds at most %d filters", MaxFilters)
		}
		for _, key := range slices.Sorted(maps.Keys(a.Filters)) {
			s.keys = append(s.keys, key)
			s.queries = append(s.queries, a.Filters[key])
		}
		if a.OtherBucketKey != "" {
			if _, taken := a.Filters[a.OtherBucketKey]; taken {
				ps.add(at+".other_bucket_key", "other_bucket_key %q is a filter's key already", a.OtherBucketKey)
			}
			s.keys, s.hasOther = append(s.keys, a.OtherBucketKey), true
		}
	case AggGlobal:
		s.keys = []any{nil}
	case AggTopMetrics:
		prepareTopMetrics(s, a, m, at, ps)
	case AggComposite:
		prepareComposite(s, a, m, at, ps)
	default:
		ps.add(loc, "unknown aggregation type %q: use %s", a.Type, aggTypeList)
		return nil
	}
	if len(a.Aggs) > 0 {
		if isMetric || (parent != "" && !singleBucket(parent)) {
			ps.add(loc+"."+aggsLoc(a.aggsKey), "only a bucket aggregation holds sub-aggregations: metrics, one level deep, or under filter, filters and global any aggregation but those")
		} else {
			for _, subName := range slices.Sorted(maps.Keys(a.Aggs)) {
				at := loc + "." + aggsLoc(a.aggsKey) + "." + subName
				if slices.Contains(reservedNames(a.Type), subName) {
					ps.add(at, "%q is a member of the %s bucket the sub-aggregation is written in: name it otherwise", subName, a.Type)
					continue
				}
				if ss := prepareAggUnder(subName, a.Aggs[subName], m, at, a.Type, nestedTermsPath(s), ps); ss != nil {
					s.subs = append(s.subs, ss)
				}
			}
		}
	}
	switch {
	case a.Type == AggTerms:
		prepareTermsOrder(s, a, at, ps)
	case len(a.Order) > 0:
		ps.add(at+".order", "order orders a terms aggregation's buckets: %s takes none", a.Type)
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

// requestID identifies a request while it runs: the shards of one request count a
// leaf's use once between them. A parsed request has a serial of its own; any other
// is known by its address, which a later request may reuse once it is collected, so
// a serial is never confused with an address.
func requestID(r *Request) uint64 {
	if r.serial != 0 {
		return r.serial
	}
	return uint64(reflect.ValueOf(r).Pointer()) | 1<<63
}

// tooManyBuckets refuses a search whose aggregations pass MaxBuckets buckets together
// at spec's, as Elasticsearch refuses a search with too many buckets.
func tooManyBuckets(spec *aggSpec) error {
	return &RequestError{Problems: []query.Problem{{
		Loc:     spec.loc + "." + spec.typ,
		Message: fmt.Sprintf("the aggregations hold more than %d buckets: widen the interval, lower a size or narrow the query", MaxBuckets),
	}}}
}

// reservedNames are the members a bucket of an aggregation of type typ is written with,
// which a sub-aggregation, written inline beside them, cannot be named.
func reservedNames(typ string) []string {
	if singleBucket(typ) {
		return []string{"doc_count"}
	}
	return []string{"key", "key_as_string", "from", "to", "doc_count", "doc_count_error_upper_bound"}
}

// nestedTermsPath is the nested field s, a terms aggregation on one of its sub-fields,
// reads the objects of; "" for any other aggregation.
func nestedTermsPath(s *aggSpec) string {
	if s.typ == AggTerms {
		return s.nested
	}
	return ""
}
