package workloads

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"strconv"

	"github.com/Imposter/go-searchlight/internal/search"
)

// Tolerances the cross-check allows (see es.Caveats).
const (
	// statsRelTol is the relative difference allowed on stats' sum and avg (summation
	// order).
	statsRelTol = 1e-9
	// cardinalityRelTol is the relative difference allowed between the two
	// HyperLogLog++ estimates.
	cardinalityRelTol = 0.03
)

// canonBucket and canonAgg are an aggregation result in an engine-neutral form.
type canonBucket struct {
	Key   string
	Count int64
	Subs  map[string]*canonAgg
}

type canonAgg struct {
	Type     string
	Buckets  []canonBucket
	ErrBound int64
	// Stats: count, min, max, avg, sum (min/max/avg NaN when empty).
	Stats []float64
	Value float64
}

// canonAggs reads a response's aggregations (Searchlight's and Elasticsearch's share
// the shape: buckets with key and doc_count, stats' members, cardinality's value) for
// the requested specs.
func canonAggs(raw json.RawMessage, specs map[string]search.Agg) (map[string]*canonAgg, error) {
	out := make(map[string]*canonAgg, len(specs))
	if len(specs) == 0 {
		return out, nil
	}
	var objs map[string]json.RawMessage
	if err := json.Unmarshal(raw, &objs); err != nil {
		return nil, fmt.Errorf("aggregations: %w", err)
	}
	for name, spec := range specs { //nolint:gocritic // map[string]search.Agg: indexing can't avoid the copy either
		r, ok := objs[name]
		if !ok {
			return nil, fmt.Errorf("aggregation %s missing", name)
		}
		a, err := canonAgg1(r, spec)
		if err != nil {
			return nil, fmt.Errorf("aggregation %s: %w", name, err)
		}
		out[name] = a
	}
	return out, nil
}

func num(v json.RawMessage) float64 {
	var f *float64
	if json.Unmarshal(v, &f) != nil || f == nil {
		return math.NaN()
	}
	return *f
}

func canonAgg1(raw json.RawMessage, spec search.Agg) (*canonAgg, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	a := &canonAgg{Type: spec.Type}
	switch spec.Type {
	case search.AggStats:
		a.Stats = []float64{num(m["count"]), num(m["min"]), num(m["max"]), num(m["avg"]), num(m["sum"])}
		return a, nil
	case search.AggCardinality:
		a.Value = num(m["value"])
		return a, nil
	}
	if e, ok := m["doc_count_error_upper_bound"]; ok {
		a.ErrBound = int64(num(e))
	}
	var buckets []map[string]json.RawMessage
	if err := json.Unmarshal(m["buckets"], &buckets); err != nil {
		return nil, fmt.Errorf("buckets: %w", err)
	}
	for _, b := range buckets {
		cb := canonBucket{Count: int64(num(b["doc_count"])), Subs: map[string]*canonAgg{}}
		cb.Key = bucketKey(b, spec.Type)
		for sub, subSpec := range spec.Aggs { //nolint:gocritic // map[string]search.Agg: indexing can't avoid the copy either
			sr, ok := b[sub]
			if !ok {
				return nil, fmt.Errorf("bucket %s: sub-aggregation %s missing", cb.Key, sub)
			}
			sa, err := canonAgg1(sr, subSpec)
			if err != nil {
				return nil, err
			}
			cb.Subs[sub] = sa
		}
		a.Buckets = append(a.Buckets, cb)
	}
	if spec.Type == search.AggRange {
		sort.Slice(a.Buckets, func(i, j int) bool { return a.Buckets[i].Key < a.Buckets[j].Key })
	}
	return a, nil
}

// bucketKey is a bucket's key as text: a bool terms key through key_as_string
// (Elasticsearch writes 1/0), numbers in shortest form.
func bucketKey(b map[string]json.RawMessage, typ string) string {
	if typ == search.AggTerms {
		var kas string
		if json.Unmarshal(b["key_as_string"], &kas) == nil && (kas == "true" || kas == "false") {
			return kas
		}
	}
	raw := bytes.TrimSpace(b["key"])
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var bl bool
	if json.Unmarshal(raw, &bl) == nil {
		return strconv.FormatBool(bl)
	}
	var f float64
	if json.Unmarshal(raw, &f) == nil {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	return string(raw)
}

func relDiff(a, b float64) float64 {
	if math.IsNaN(a) && math.IsNaN(b) {
		return 0
	}
	if a == b {
		return 0
	}
	return math.Abs(a-b) / math.Max(math.Abs(a), math.Abs(b))
}

// diffAggs lists the differences between two canonical aggregation sets. tolerated is
// set when every difference is in a terms aggregation with a nonzero error bound.
func diffAggs(path string, sl, es map[string]*canonAgg) (problems []string, tolerated bool) {
	tolerated = true
	for _, name := range slices.Sorted(maps.Keys(sl)) {
		ps, tol := diffAgg(path+name, sl[name], es[name])
		problems = append(problems, ps...)
		tolerated = tolerated && (len(ps) == 0 || tol)
	}
	return problems, tolerated && len(problems) > 0
}

func diffAgg(path string, s, e *canonAgg) ([]string, bool) {
	if e == nil {
		return []string{path + ": missing from Elasticsearch"}, false
	}
	switch s.Type {
	case search.AggStats:
		var ps []string
		names := []string{"count", "min", "max", "avg", "sum"}
		for i, n := range names {
			tol := 0.0
			if n == "avg" || n == "sum" {
				tol = statsRelTol
			}
			if relDiff(s.Stats[i], e.Stats[i]) > tol {
				ps = append(ps, fmt.Sprintf("%s.%s: %v vs %v", path, n, s.Stats[i], e.Stats[i]))
			}
		}
		return ps, false
	case search.AggCardinality:
		if relDiff(s.Value, e.Value) > cardinalityRelTol {
			return []string{fmt.Sprintf("%s: cardinality %v vs %v (beyond %.0f%%)", path, s.Value, e.Value, cardinalityRelTol*100)}, false
		}
		return nil, false
	}
	var ps []string
	if len(s.Buckets) != len(e.Buckets) {
		ps = append(ps, fmt.Sprintf("%s: %d buckets vs %d", path, len(s.Buckets), len(e.Buckets)))
	}
	for i := range min(len(s.Buckets), len(e.Buckets)) {
		sb, eb := s.Buckets[i], e.Buckets[i]
		if sb.Key != eb.Key || sb.Count != eb.Count {
			ps = append(ps, fmt.Sprintf("%s bucket %d: %s=%d vs %s=%d", path, i, sb.Key, sb.Count, eb.Key, eb.Count))
			continue
		}
		subPs, _ := diffAggs(fmt.Sprintf("%s[%s].", path, sb.Key), sb.Subs, eb.Subs)
		ps = append(ps, subPs...)
	}
	tolerated := s.Type == search.AggTerms && (s.ErrBound > 0 || e.ErrBound > 0)
	return ps, tolerated
}

// diffSearch compares two engines' answers to one search.
func diffSearch(req *search.Request, sl, es SearchResult) (problems []string, tolerated bool) {
	if sl.Total != es.Total || sl.Relation != es.Relation {
		problems = append(problems, fmt.Sprintf("total %d (%s) vs %d (%s)", sl.Total, sl.Relation, es.Total, es.Relation))
	}
	if !slices.Equal(sl.IDs, es.IDs) {
		first := -1
		for i := range min(len(sl.IDs), len(es.IDs)) {
			if sl.IDs[i] != es.IDs[i] {
				first = i
				break
			}
		}
		if first < 0 {
			first = min(len(sl.IDs), len(es.IDs))
		}
		at := func(ids []string) string {
			if first < len(ids) {
				return ids[first]
			}
			return "(end)"
		}
		problems = append(problems, fmt.Sprintf("hits differ: %d vs %d ids, first difference at %d: %s vs %s", len(sl.IDs), len(es.IDs), first, at(sl.IDs), at(es.IDs)))
	}
	hitsDiffer := len(problems) > 0
	if len(req.Aggs) > 0 {
		sa, err := canonAggs(sl.Aggs, req.Aggs)
		if err != nil {
			return append(problems, "searchlight "+err.Error()), false
		}
		ea, err := canonAggs(es.Aggs, req.Aggs)
		if err != nil {
			return append(problems, "elasticsearch "+err.Error()), false
		}
		aps, tol := diffAggs("aggs.", sa, ea)
		problems = append(problems, aps...)
		return problems, tol && !hitsDiffer
	}
	return problems, false
}

// diffPercolate compares two engines' matches per document.
func diffPercolate(sl, es [][]string) []string {
	if len(sl) != len(es) {
		return []string{fmt.Sprintf("%d results vs %d", len(sl), len(es))}
	}
	var ps []string
	for i := range sl {
		a, b := slices.Sorted(slices.Values(sl[i])), slices.Sorted(slices.Values(es[i]))
		if !slices.Equal(a, b) {
			onlyA, onlyB := minus(a, b), minus(b, a)
			ps = append(ps, fmt.Sprintf("doc %d: %d vs %d queries; only searchlight %v, only elasticsearch %v", i, len(a), len(b), head(onlyA), head(onlyB)))
		}
	}
	return ps
}

func minus(a, b []string) []string {
	var out []string
	for _, x := range a {
		if _, found := slices.BinarySearch(b, x); !found {
			out = append(out, x)
		}
	}
	return out
}

func head(s []string) []string {
	if len(s) > 5 {
		return append(slices.Clone(s[:5]), "…")
	}
	return s
}

func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}
