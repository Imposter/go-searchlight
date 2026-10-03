// Package search runs forward searches over a shard's generation (spec section 6):
// a cost-based plan of roaring bitmap operations with residual checks where the index
// cannot decide alone, sorting with search_after paging, partial aggregations per
// shard, and the reduce that merges shard results into one response.
//
// Results are exact: a shard's hits are precisely the live documents for which
// query.Compile(n).Match(doc) holds, and aggregations count exactly those documents
// (a terms aggregation over several shards aside, whose shard_size over-fetch is
// reported with an error bound, as Elasticsearch does).
package search

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// Total relations.
const (
	// RelationEq is an exact total.
	RelationEq = "eq"
	// RelationGte is a lower bound: more documents matched than the total says (past
	// Request.TrackTotal, or a search that timed out).
	RelationGte = "gte"
)

// Limits and defaults.
const (
	// DefaultTrackTotal is how far totals are exact when Request.TrackTotal is 0, as
	// Elasticsearch's track_total_hits defaults.
	DefaultTrackTotal = 10_000
	// TrackTotalAll asks for an exact total however many documents match.
	TrackTotalAll = -1
	// TrackTotalNone asks for no total (track_total: false): the shards verify only
	// what the top hits need, and Total is whatever lower bound that gave.
	TrackTotalNone = -2
	// MaxSize is the most hits one request returns.
	MaxSize = 10_000
	// MaxBuckets bounds the buckets of one response (Elasticsearch's search.max_buckets).
	// A histogram's empty buckets are filled in only while the total stays under it.
	MaxBuckets = 65_536
	// DefaultTermsSize is a terms aggregation's bucket count when Size is 0.
	DefaultTermsSize = 10
	// DefaultPrecision is a cardinality aggregation's HyperLogLog++ precision when
	// Precision is 0: 2^14 registers, a standard error of about 0.8%.
	DefaultPrecision = 14
	// MinPrecision and MaxPrecision bound a cardinality aggregation's precision.
	MinPrecision = 4
	MaxPrecision = 18
)

// IDField is the pseudo-field of the document id. Sorting on it sorts by the exact id;
// querying it compares the id normalized, as a keyword.
const IDField = schema.IDField

// The aggregation types.
const (
	AggTerms         = "terms"
	AggRange         = "range"
	AggHistogram     = "histogram"
	AggDateHistogram = "date_histogram"
	AggStats         = "stats"
	AggCardinality   = "cardinality"
)

// Request is one search (spec section 5).
type Request struct {
	// Query selects the documents; nil matches none, &query.All{} every one.
	Query query.Node
	// Sort orders the hits, then the document id breaks ties (ascending, exact): the
	// order is total, which is what makes search_after paging stable. Empty sorts by
	// _id alone. A document without a sort field's value sorts after every document
	// with one, ascending or descending.
	Sort []SortField
	// Size is how many hits to return, at most MaxSize; 0 returns none (a count, or
	// aggregations only).
	Size int
	// SearchAfter resumes after a hit: pass the previous page's Response.Next (the
	// last hit's Sort). Each page runs on the generation current when it runs, so
	// paging under concurrent writes is stable in this sense: a document that is
	// live and unchanged for the whole walk appears exactly once, in order; a
	// document deleted before its page is not returned; and a document written
	// during the walk appears (at its new sort position) only if that position is
	// after the cursor when its page runs, so an update can move a document onto a
	// page already served (seen twice) or behind the cursor (skipped).
	SearchAfter []any
	// TrackTotal is how far Total is exact: a count past it is reported as
	// TrackTotal with RelationGte. 0 means DefaultTrackTotal; TrackTotalAll always
	// counts exactly; TrackTotalNone does not count past what the hits need.
	TrackTotal int
	// Aggs are the aggregations to compute over every matching document, by name.
	Aggs map[string]Agg
	// Fields, when set, limits each hit's body to these top-level members.
	Fields []string
	// Timeout bounds the search; past it the shards answer with what they have,
	// TimedOut set. 0 means no bound but the context's.
	Timeout time.Duration
	// Index labels the search's spans, logs and metrics.
	Index string
	// NoBodies leaves the hits' bodies out (each keeps its Ref): the query phase of a
	// query-then-fetch, whose fetch phase is [FetchShard].
	NoBodies bool
}

// SortField is one sort key: a field (or IDField), ascending unless Desc.
type SortField struct {
	Field string
	Desc  bool
}

// Agg is one aggregation. Bucket aggregations (terms, range, histogram,
// date_histogram) may hold metric sub-aggregations (stats, cardinality) in Aggs, one
// level deep.
//
// Text values aggregate as they are indexed and compared: normalized (casefolded,
// whitespace folded). That includes the _id field, a normalized keyword for queries
// and aggregations alike (the parity rules), so terms and cardinality over _id count
// ids that differ only in case as one; sorting by _id, by contrast, uses the exact id.
type Agg struct {
	// Type is one of the Agg* constants.
	Type  string
	Field string

	// Size is how many terms buckets to return: DefaultTermsSize when 0.
	Size int
	// ShardSize is how many terms buckets each shard returns: Size*3/2+10 when 0.
	// More is more exact across shards (see Response.Aggs).
	ShardSize int
	// MinDocCount drops buckets with fewer documents. terms defaults to 1 (and takes
	// at least 1); histograms default to 0, which fills the empty buckets between the
	// first and the last.
	MinDocCount *int

	// Ranges are a range aggregation's buckets: From inclusive, To exclusive, either
	// open when nil.
	Ranges []Range

	// Interval is a histogram's bucket width, or a date_histogram's fixed interval in
	// milliseconds; Offset shifts the buckets' starts.
	Interval float64
	Offset   float64
	// Calendar is a date_histogram's calendar interval (UTC): minute, hour, day,
	// week (from Monday), month, quarter or year. Set it or Interval, not both.
	Calendar string

	// Precision is a cardinality aggregation's HyperLogLog++ precision (MinPrecision
	// to MaxPrecision): DefaultPrecision when 0. Counts are exact while fewer than
	// 2^(Precision-2) distinct values are seen.
	Precision int

	// Aggs are the sub-aggregations computed per bucket.
	Aggs map[string]Agg
}

// Range is one bucket of a range aggregation.
type Range struct {
	// Key names the bucket; "from-to" (with * for an open end) when empty.
	Key      string
	From, To *float64
}

// RequestError is a request ExecuteShard refuses, with every problem found.
type RequestError struct {
	Problems []query.Problem
}

func (e *RequestError) Error() string {
	parts := make([]string, len(e.Problems))
	for i, p := range e.Problems {
		parts[i] = p.String()
	}
	return "search: invalid request: " + strings.Join(parts, "; ")
}

// problems collects request problems.
type problems []query.Problem

func (ps *problems) add(loc, format string, args ...any) {
	*ps = append(*ps, query.Problem{Loc: loc, Message: fmt.Sprintf(format, args...)})
}

// ParseRequest reads a search request body: {query, sort, size, search_after,
// track_total, aggs, fields, timeout}, Elasticsearch style:
//
//   - sort: ["price", {"price": "desc"}, {"price": {"order": "desc"}}];
//   - track_total: true (exact) or a count;
//   - aggs: {"name": {"terms": {"field": "brand", "size": 10}, "aggs": {...}}};
//   - timeout: "250ms" (any Go duration, or d for days) or milliseconds.
//
// A missing query matches every document. It returns every problem found, each with
// its loc ("sort.1", "aggs.by_brand.terms.size"); the query's are [query.Parse]'s.
// Fields, sort and aggregation types are checked against the mapping by ExecuteShard.
func ParseRequest(raw []byte) (*Request, []query.Problem) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var top map[string]json.RawMessage
	if err := dec.Decode(&top); err != nil {
		return nil, []query.Problem{{Loc: "body", Message: "the request is not a JSON object: " + err.Error()}}
	}
	r := &Request{Query: &query.All{}}
	var ps problems
	for _, key := range slices.Sorted(maps.Keys(top)) {
		val := top[key]
		switch key {
		case "query":
			n, qps := query.Parse(val)
			ps = append(ps, qps...)
			if n != nil {
				r.Query = n
			}
		case "sort":
			r.Sort = parseSort(val, &ps)
		case "size":
			if n, ok := intOf(val); ok && n >= 0 && n <= MaxSize {
				r.Size = n
			} else {
				ps.add("size", "size is a whole number from 0 to %d", MaxSize)
			}
		case "search_after":
			var after []any
			if err := decodeNumbers(val, &after); err != nil {
				ps.add("search_after", "search_after is a list of sort values")
				continue
			}
			for i, v := range after {
				if n, ok := v.(json.Number); ok {
					f, err := n.Float64()
					if err != nil {
						ps.add(fmt.Sprintf("search_after.%d", i), "not a number a float64 holds")
						continue
					}
					after[i] = f
				}
			}
			r.SearchAfter = after
		case "track_total":
			switch string(bytes.TrimSpace(val)) {
			case "true":
				r.TrackTotal = TrackTotalAll
			case "false":
				r.TrackTotal = TrackTotalNone
			default:
				if n, ok := intOf(val); ok && n >= 1 {
					r.TrackTotal = n
				} else {
					ps.add("track_total", "track_total is true, false or a count of at least 1")
				}
			}
		case "aggs", "aggregations":
			r.Aggs = parseAggs(val, key, 0, &ps)
		case "fields":
			var fields []string
			if err := json.Unmarshal(val, &fields); err != nil {
				ps.add("fields", "fields is a list of field names")
				continue
			}
			r.Fields = fields
		case "timeout":
			d, ok := parseTimeout(val)
			if !ok {
				ps.add("timeout", `timeout is a duration ("250ms", "2s") or milliseconds`)
				continue
			}
			r.Timeout = d
		default:
			ps.add(key, "unknown key: a search holds query, sort, size, search_after, track_total, aggs, fields and timeout")
		}
	}
	if len(ps) > 0 {
		return nil, ps
	}
	return r, nil
}

func decodeNumbers(raw json.RawMessage, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return dec.Decode(v)
}

// intOf reads a JSON whole number.
func intOf(raw json.RawMessage) (int, bool) {
	var n json.Number
	if err := decodeNumbers(raw, &n); err != nil {
		return 0, false
	}
	i, err := strconv.ParseInt(string(n), 10, 32)
	if err != nil {
		f, ferr := n.Float64()
		if ferr != nil || f != math.Trunc(f) || math.Abs(f) > math.MaxInt32 {
			return 0, false
		}
		return int(f), true
	}
	return int(i), true
}

func floatOf(raw json.RawMessage) (float64, bool) {
	var n json.Number
	if err := decodeNumbers(raw, &n); err != nil {
		return 0, false
	}
	f, err := n.Float64()
	if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
		return 0, false
	}
	return f, true
}

func parseSort(raw json.RawMessage, ps *problems) []SortField {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		// A single sort key, as Elasticsearch also takes it.
		items = []json.RawMessage{raw}
	}
	out := make([]SortField, 0, len(items))
	for i, item := range items {
		loc := fmt.Sprintf("sort.%d", i)
		var name string
		if json.Unmarshal(item, &name) == nil {
			out = append(out, SortField{Field: name})
			continue
		}
		var obj map[string]json.RawMessage
		if json.Unmarshal(item, &obj) != nil || len(obj) != 1 {
			ps.add(loc, `a sort key is a field name or {"field": "asc"|"desc"}`)
			continue
		}
		for field, spec := range obj {
			var order string
			if json.Unmarshal(spec, &order) != nil {
				var o struct {
					Order string `json:"order"`
				}
				dec := json.NewDecoder(bytes.NewReader(spec))
				dec.DisallowUnknownFields()
				if dec.Decode(&o) != nil {
					ps.add(loc+"."+field, `a sort order is "asc", "desc" or {"order": ...}`)
					continue
				}
				order = o.Order
			}
			switch order {
			case "asc", "":
				out = append(out, SortField{Field: field})
			case "desc":
				out = append(out, SortField{Field: field, Desc: true})
			default:
				ps.add(loc+"."+field, `a sort order is "asc" or "desc"`)
			}
		}
	}
	return out
}

// parseTimeout reads "250ms", "2s", "1m", "1d" or a number of milliseconds.
func parseTimeout(raw json.RawMessage) (time.Duration, bool) {
	if ms, ok := floatOf(raw); ok {
		if ms < 0 || ms > maxTimeoutMillis {
			return 0, false
		}
		return time.Duration(ms * float64(time.Millisecond)), true
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return 0, false
	}
	d, ok := parseDuration(s)
	return d, ok && d >= 0
}

// maxTimeoutMillis bounds a timeout in milliseconds: a year, far inside a Duration.
const maxTimeoutMillis = 365 * 24 * 3600 * 1000

// parseDuration reads a Go duration, or a whole number of days ("2d").
func parseDuration(s string) (time.Duration, bool) {
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.ParseFloat(days, 64)
		if err != nil || n < 0 || n > 365 {
			return 0, false
		}
		return time.Duration(n * float64(24*time.Hour)), true
	}
	d, err := time.ParseDuration(s)
	return d, err == nil
}

func parseAggs(raw json.RawMessage, loc string, depth int, ps *problems) map[string]Agg {
	var named map[string]json.RawMessage
	if err := json.Unmarshal(raw, &named); err != nil {
		ps.add(loc, "aggs is an object of named aggregations")
		return nil
	}
	out := make(map[string]Agg, len(named))
	for _, name := range slices.Sorted(maps.Keys(named)) {
		at := loc + "." + name
		if name == "" {
			ps.add(at, "an aggregation needs a name")
			continue
		}
		var body map[string]json.RawMessage
		if err := json.Unmarshal(named[name], &body); err != nil {
			ps.add(at, "an aggregation is an object: {type: {...}, aggs: {...}}")
			continue
		}
		var a Agg
		typed := 0
		for _, key := range slices.Sorted(maps.Keys(body)) {
			val := body[key]
			switch key {
			case "aggs", "aggregations":
				if depth > 0 {
					ps.add(at+"."+key, "sub-aggregations nest one level deep")
					continue
				}
				a.Aggs = parseAggs(val, at+"."+key, depth+1, ps)
			case AggTerms, AggRange, AggHistogram, AggDateHistogram, AggStats, AggCardinality:
				typed++
				a.Type = key
				parseAggBody(&a, val, at+"."+key, ps)
			default:
				ps.add(at+"."+key, "unknown aggregation type: use terms, range, histogram, date_histogram, stats or cardinality")
			}
		}
		if typed != 1 {
			ps.add(at, "an aggregation holds exactly one type")
			continue
		}
		out[name] = a
	}
	return out
}

func parseAggBody(a *Agg, raw json.RawMessage, loc string, ps *problems) {
	var body map[string]json.RawMessage
	if err := json.Unmarshal(raw, &body); err != nil {
		ps.add(loc, "an aggregation's settings are an object")
		return
	}
	allowed := map[string][]string{
		AggTerms:         {"field", "size", "shard_size", "min_doc_count"},
		AggRange:         {"field", "ranges"},
		AggHistogram:     {"field", "interval", "offset", "min_doc_count"},
		AggDateHistogram: {"field", "fixed_interval", "calendar_interval", "interval", "offset", "min_doc_count"},
		AggStats:         {"field"},
		AggCardinality:   {"field", "precision"},
	}[a.Type]
	for _, key := range slices.Sorted(maps.Keys(body)) {
		val := body[key]
		at := loc + "." + key
		if !slices.Contains(allowed, key) {
			ps.add(at, "unknown key: %s takes %s", a.Type, strings.Join(allowed, ", "))
			continue
		}
		switch key {
		case "field":
			if json.Unmarshal(val, &a.Field) != nil || a.Field == "" {
				ps.add(at, "field is a field name")
			}
		case "size", "shard_size", "precision":
			n, ok := intOf(val)
			if !ok || n < 1 {
				ps.add(at, "%s is a whole number of at least 1", key)
				continue
			}
			switch key {
			case "size":
				a.Size = n
			case "shard_size":
				a.ShardSize = n
			default:
				a.Precision = n
			}
		case "min_doc_count":
			n, ok := intOf(val)
			if !ok || n < 0 {
				ps.add(at, "min_doc_count is a whole number")
				continue
			}
			a.MinDocCount = &n
		case "interval":
			if a.Type == AggDateHistogram {
				d, ok := intervalMillis(val)
				if !ok {
					ps.add(at, `interval is a duration ("1h", "1d") or milliseconds`)
					continue
				}
				a.Interval = d
				continue
			}
			f, ok := floatOf(val)
			if !ok || f <= 0 {
				ps.add(at, "interval is a number above 0")
				continue
			}
			a.Interval = f
		case "fixed_interval":
			d, ok := intervalMillis(val)
			if !ok {
				ps.add(at, `fixed_interval is a duration ("1h", "1d") or milliseconds`)
				continue
			}
			a.Interval = d
		case "calendar_interval":
			if json.Unmarshal(val, &a.Calendar) != nil {
				ps.add(at, "calendar_interval is a unit name")
			}
		case "offset":
			if a.Type == AggDateHistogram {
				var s string
				if json.Unmarshal(val, &s) == nil {
					neg := strings.HasPrefix(s, "-")
					d, ok := parseDuration(strings.TrimPrefix(strings.TrimPrefix(s, "-"), "+"))
					if !ok {
						ps.add(at, "offset is a duration or milliseconds")
						continue
					}
					a.Offset = float64(d.Milliseconds())
					if neg {
						a.Offset = -a.Offset
					}
					continue
				}
			}
			f, ok := floatOf(val)
			if !ok {
				ps.add(at, "offset is a number")
				continue
			}
			a.Offset = f
		case "ranges":
			var ranges []map[string]json.RawMessage
			if json.Unmarshal(val, &ranges) != nil {
				ps.add(at, "ranges is a list of {from, to, key}")
				continue
			}
			for i, rg := range ranges {
				var out Range
				for _, k := range slices.Sorted(maps.Keys(rg)) {
					v := rg[k]
					rloc := fmt.Sprintf("%s.%d.%s", at, i, k)
					switch k {
					case "key":
						if json.Unmarshal(v, &out.Key) != nil {
							ps.add(rloc, "key is text")
						}
					case "from", "to":
						if string(v) == "null" {
							continue
						}
						f, ok := floatOf(v)
						if !ok {
							ps.add(rloc, "%s is a number", k)
							continue
						}
						if k == "from" {
							out.From = &f
						} else {
							out.To = &f
						}
					default:
						ps.add(rloc, "unknown key: a range holds from, to and key")
					}
				}
				a.Ranges = append(a.Ranges, out)
			}
		}
	}
}

// intervalMillis reads a fixed interval: a duration ("90m", "1d") or milliseconds.
func intervalMillis(raw json.RawMessage) (float64, bool) {
	if ms, ok := floatOf(raw); ok {
		return ms, ms > 0
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return 0, false
	}
	d, ok := parseDuration(s)
	if !ok || d < time.Millisecond {
		return 0, false
	}
	return float64(d.Milliseconds()), true
}

// errNoGeneration is ExecuteShard called without a generation.
var errNoGeneration = errors.New("search: no generation")
