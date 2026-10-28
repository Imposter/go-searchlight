package search

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// TermsOrder is one key of a terms aggregation's order (Agg.Order), as Elasticsearch's
// order takes it: Key is OrderCount, OrderKey, or a metric sub-aggregation of the terms
// aggregation by name: a min, max or cardinality by its value ("low", or "low.value"),
// a stats by one of its members ("prices.avg": count, min, max, avg or sum), a
// top_metrics of size 1 by one of its metrics ("latest.price"). A bucket without a
// metric value (a min over no value) sorts after every bucket with one, ascending or
// descending. _key ascending breaks the ties the keys leave, and nothing orders after
// _key: keys are unique.
//
// Across shards, how exact the answer is follows the first key, as in Elasticsearch:
//
//   - _count descending (the default): each shard returns its shard_size best terms,
//     and doc_count_error_upper_bound bounds what the shards left out.
//   - _key: each shard returns its first shard_size keys in that order, so a term a
//     shard left out sorts after the last it returned: the top size are exact (while
//     min_doc_count is 1; a bucket that may miss some shard's documents says
//     doc_count_error_upper_bound -1).
//   - _count ascending, or a sub-aggregation: no shard can bound what it leaves out.
//     Each shard collects every term's sub-aggregations (depth first: collect_mode
//     breadth_first is refused), orders its terms exactly and returns shard_size of
//     them, OrderedShardSize by default, so the answer is exact while no shard holds
//     more distinct values. When one does, doc_count_error_upper_bound is -1, on the
//     response and on each bucket some shard that cut its list did not return.
//
// One shard is always exact.
type TermsOrder struct {
	Key  string
	Desc bool `json:",omitempty"`
}

// The keys of a terms order that are not a sub-aggregation's.
const (
	OrderCount = "_count"
	OrderKey   = "_key"
)

// termsOrder is a terms aggregation's order resolved against its sub-aggregations: the
// keys given, then _key ascending unless the last is _key.
type termsOrder struct {
	keys []orderKey
	// subs is set when a key reads a sub-aggregation: every bucket's sub-aggregations
	// are then collected before a shard cuts its list.
	subs  bool
	bound orderBound
}

// orderBound is what a shard that cut its terms list can say of the terms it left out,
// by the order's first key.
type orderBound uint8

const (
	// boundCount (_count descending): none has more documents than its last returned
	// term, the default order's error bound.
	boundCount orderBound = iota
	// boundKey (_key): every one sorts after its last returned term.
	boundKey
	// boundNone (_count ascending, a sub-aggregation): nothing.
	boundNone
)

type orderKind uint8

const (
	orderByCount orderKind = iota
	orderByKey
	orderBySub
)

// orderKey is one key of a termsOrder: a bucket's count, its key, or a metric of its
// sub-aggregation sub (member: a stats member; field: a top_metrics metric's index).
type orderKey struct {
	kind   orderKind
	desc   bool
	sub    *aggSpec
	member string
	field  int
}

// statsMembers are the members a terms aggregation orders a stats sub-aggregation by.
var statsMembers = []string{"count", "min", "max", "avg", "sum"}

// parseTermsOrder reads a terms aggregation's order: {"key": "asc"|"desc"}, or a list of
// them for the ties the first leaves.
func parseTermsOrder(raw json.RawMessage, loc string, ps *problems) []TermsOrder {
	const shape = `order is {"_count"|"_key"|"<sub-aggregation>": "asc"|"desc"}, or a list of them`
	var items []json.RawMessage
	list := bytes.HasPrefix(bytes.TrimSpace(raw), []byte("["))
	if !list {
		items = []json.RawMessage{raw}
	} else if json.Unmarshal(raw, &items) != nil {
		ps.add(loc, shape)
		return nil
	}
	out := make([]TermsOrder, 0, len(items))
	for i, item := range items {
		at := loc
		if list {
			at = fmt.Sprintf("%s.%d", loc, i)
		}
		var obj map[string]string
		if json.Unmarshal(item, &obj) != nil || len(obj) != 1 {
			ps.add(at, shape+": one key per object")
			continue
		}
		for key, dir := range obj {
			switch dir {
			case "asc", "desc":
				out = append(out, TermsOrder{Key: key, Desc: dir == "desc"})
			default:
				ps.add(at+"."+key, `an order is "asc" or "desc"`)
			}
		}
	}
	return out
}

// prepareTermsOrder resolves terms aggregation a's order against s's sub-aggregations
// (prepared already) into s.order, and gives s the shard_size an order that bounds no
// error takes by default.
func prepareTermsOrder(s *aggSpec, a Agg, at string, ps *problems) {
	if len(a.Order) == 0 {
		return
	}
	loc := at + ".order"
	if len(a.Order) > MaxTermsOrder {
		ps.add(loc, "a terms aggregation orders by at most %d keys", MaxTermsOrder)
		return
	}
	o := &termsOrder{}
	for i, k := range a.Order {
		kloc := fmt.Sprintf("%s.%d", loc, i)
		if i > 0 && a.Order[i-1].Key == OrderKey {
			ps.add(kloc, "nothing orders after _key: keys are unique, so no later key can break a tie")
			return
		}
		if j := slices.IndexFunc(a.Order[:i], func(o TermsOrder) bool { return o.Key == k.Key }); j >= 0 {
			ps.add(kloc, "%q is already order key %d: a key orders once", k.Key, j)
			return
		}
		ok := orderKey{desc: k.Desc}
		switch k.Key {
		case OrderCount:
			ok.kind = orderByCount
		case OrderKey:
			ok.kind = orderByKey
		default:
			if !resolveOrderSub(&ok, s, k.Key, kloc, ps) {
				return
			}
			o.subs = true
		}
		o.keys = append(o.keys, ok)
	}
	if last := o.keys[len(o.keys)-1]; last.kind != orderByKey {
		o.keys = append(o.keys, orderKey{kind: orderByKey})
	}
	switch first := o.keys[0]; {
	case first.kind == orderByCount && first.desc:
		o.bound = boundCount
	case first.kind == orderByKey:
		o.bound = boundKey
	default:
		o.bound = boundNone
	}
	if o.subs && a.CollectMode == CollectBreadthFirst {
		ps.add(at+".collect_mode", "breadth_first collects the sub-aggregations of the terms a shard keeps, and an order by a sub-aggregation makes that cut by them: use depth_first, or leave collect_mode unset")
		return
	}
	if len(o.keys) == 2 && o.bound == boundCount && !o.keys[1].desc {
		return // _count descending, _key ascending: the default
	}
	s.order = o
	if o.bound == boundNone && a.ShardSize <= 0 {
		s.shardSize = min(max(OrderedShardSize, s.size), MaxBuckets)
	}
}

// resolveOrderSub resolves order key key, a sub-aggregation's metric, into ok.
func resolveOrderSub(ok *orderKey, s *aggSpec, key, loc string, ps *problems) bool {
	name, member, _ := strings.Cut(key, ".")
	i := slices.IndexFunc(s.subs, func(sub *aggSpec) bool { return sub.name == name })
	if i < 0 {
		ps.add(loc, "%q names no sub-aggregation of this terms aggregation: it orders by _count, _key, or a metric sub-aggregation (name, name.avg for a stats, name.field for a top_metrics)", key)
		return false
	}
	sub := s.subs[i]
	ok.kind, ok.sub, ok.member = orderBySub, sub, member
	switch sub.typ {
	case AggMin, AggMax, AggCardinality:
		if member != "" && member != "value" {
			ps.add(loc, "%q is a %s: order by %q (or %q)", name, sub.typ, name, name+".value")
			return false
		}
	case AggStats:
		if !slices.Contains(statsMembers, member) {
			ps.add(loc, "%q is a stats: order by one of its members, %s.%s", name, name, strings.Join(statsMembers, "|"))
			return false
		}
	case AggTopMetrics:
		f := -1
		if sub.top != nil {
			f = slices.Index(sub.top.metrics, member)
		}
		switch {
		case sub.top == nil:
			return false // refused already
		case sub.top.size != 1:
			ps.add(loc, "%q is a top_metrics of size %d: a terms aggregation orders by one of size 1", name, sub.top.size)
			return false
		case f < 0:
			ps.add(loc, "%q is a top_metrics: order by one of its metrics, %s.<field>", name, name)
			return false
		}
		ok.field = f
	default:
		ps.add(loc, "a terms aggregation orders by a metric sub-aggregation, and %q is a %s", name, sub.typ)
		return false
	}
	return true
}

// bound is what a shard's cut of s's list can say of the terms it left out.
func (s *aggSpec) bound() orderBound {
	if s.order == nil {
		return boundCount
	}
	return s.order.bound
}

// orderSubs reports whether s, a terms aggregation, orders by a sub-aggregation.
func (s *aggSpec) orderSubs() bool { return s.order != nil && s.order.subs }

// value is bucket b's value for key k: its count, key, or its sub-aggregation's metric
// (nil when it has none).
func (k *orderKey) value(b *BucketPartial) any {
	switch k.kind {
	case orderByCount:
		return float64(b.DocCount)
	case orderByKey:
		return b.Key
	}
	p := b.Aggs[k.sub.name]
	if p == nil {
		return nil
	}
	switch k.sub.typ {
	case AggMin, AggMax:
		if p.Value == nil {
			return nil
		}
		return *p.Value
	case AggCardinality:
		return float64(finish(p, k.sub).Value)
	case AggStats:
		st := p.Stats
		if st == nil {
			st = &StatsPartial{}
		}
		switch {
		case k.member == "count":
			return float64(st.Count)
		case k.member == "sum":
			return st.Sum
		case st.Count == 0:
			return nil
		case k.member == "min":
			return st.Min
		case k.member == "max":
			return st.Max
		}
		return st.Sum / float64(st.Count)
	case AggTopMetrics:
		if len(p.Top) == 0 || k.field >= len(p.Top[0].Metrics) {
			return nil
		}
		return p.Top[0].Metrics[k.field]
	}
	return nil
}

// sortTerms reorders terms buckets bs so that the first k are the first k in s's
// order, sorted, as topK does: by count, then key, without an order.
func sortTerms(bs []*BucketPartial, k int, s *aggSpec) {
	if s.order == nil {
		topK(bs, k, cmpTerms)
		return
	}
	type row struct {
		b *BucketPartial
		v []any
	}
	keys := s.order.keys
	vals := make([]any, len(bs)*len(keys))
	rows := make([]row, len(bs))
	for i, b := range bs {
		v := vals[i*len(keys) : (i+1)*len(keys) : (i+1)*len(keys)]
		for j := range keys {
			v[j] = keys[j].value(b)
		}
		rows[i] = row{b: b, v: v}
	}
	topK(rows, k, func(a, b row) int {
		for j := range keys {
			if c := cmpValue(a.v[j], b.v[j], keys[j].desc); c != 0 {
				return c
			}
		}
		return 0
	})
	for i := range rows {
		bs[i] = rows[i].b
	}
}

// ordCmp orders global ordinals by s's order of counts and keys (no sub-aggregation),
// a global ordinal's order being its term's.
func (s *aggSpec) ordCmp(counts []int64) func(a, b uint32) int {
	keys := s.order.keys
	return func(a, b uint32) int {
		for _, k := range keys {
			var c int
			if k.kind == orderByCount {
				c = cmp.Compare(counts[a], counts[b])
			} else {
				c = cmp.Compare(a, b)
			}
			if k.desc {
				c = -c
			}
			if c != 0 {
				return c
			}
		}
		return 0
	}
}

// markCut records on p, a shard's terms list just cut to shard_size, what its cut says
// of the terms it left out: under the default order the count of its last term, the
// most any of them has; under any other order only that it cut (AggPartial.Cut).
func markCut(p *AggPartial, s *aggSpec) {
	if s.bound() == boundCount {
		p.DocCountError = p.Buckets[len(p.Buckets)-1].DocCount
		return
	}
	p.Cut = true
}

// missedBound is the error bound a terms bucket b takes from part p, a shard's list
// that does not hold b's key: under _count descending p's DocCountError; under _key
// none when b sorts before p's last term (p returned every term up to it), -1
// (unknown) otherwise; under any other order -1 when p cut its list. A list p did not
// cut holds every term of its shard.
func missedBound(b *BucketPartial, p *AggPartial, s *aggSpec) int64 {
	switch {
	case s.bound() == boundCount:
		return p.DocCountError
	case !p.Cut || len(p.Buckets) == 0:
		return 0
	case s.bound() == boundKey && cmpValue(b.Key, p.Buckets[len(p.Buckets)-1].Key, s.order.keys[0].desc) < 0:
		return 0
	}
	return -1
}
