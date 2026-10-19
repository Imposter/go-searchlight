package search

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
)

// Nested fields.
//
// A segment keeps a nested field's objects in an object segment of their own
// (segment.Nested): an index whose documents are the objects. A nested node's inner
// query runs over it as any query runs over a segment, through an execution of its
// own (inner): the same candidates, residual checks and approximations, over the
// members that are objects (its allDocs, so a not inside negates per object). Its
// answer comes back to the segment's documents through each document's run of objects:
// a document surely matches when one of its objects surely does, and may when one may;
// a maybe document is decided by its maybe objects' residual checks. Documents and
// everything kept of them (ordinals, deletes, totals, sort, the filter cache) are the
// segment's own: an object is live while its document is.
//
// An aggregation on a sub-field collects the objects of the hits (those its where
// chooses), a terms aggregation counting each document once per value however many
// of its objects hold the value; a sort on one orders the hits by the least or
// greatest value among their objects (those its where chooses), a hit with none after
// every hit with one.

// nestedScopeFactor: a nested node approximates its inner query over the objects of
// the documents in question when those are fewer than 1/nestedScopeFactor of the
// segment, and over every object otherwise (expanding a large scope costs more than
// the objects outside it do).
const nestedScopeFactor = 8

// inner returns the execution over nested field path's objects, nil when the segment
// holds no member of it: shared by every nested node, aggregation and sort on the field.
func (s *segExec) inner(path string) *segExec {
	if in, ok := s.inners[path]; ok {
		return in
	}
	var in *segExec
	if nv := s.r.Nested(path); nv != nil {
		objects := nv.Objects()
		in = &segExec{
			ctx: s.ctx, p: s.p, sv: s.sv, seg: s.seg, r: objects, n: objects.NumDocs(), allDocs: objects.Present(path),
			cacheMemo:  map[*leafPlan]*roaring.Bitmap{},
			approxes:   map[*pnode]approx{},
			ordMatches: map[ordKey]bool{},
			orders:     map[*pnode][]*pnode{},
			estimates:  map[*leafPlan]estimate{},
			sources:    map[string]*fieldSrc{},
			buckets:    s.buckets,
			masks:      &termMasks{},
			parent:     s, nv: nv, path: path,
		}
	}
	if s.inners == nil {
		s.inners = map[string]*segExec{}
	}
	s.inners[path] = in
	return in
}

// adopt takes in's verifications and first error as s's own.
func (s *segExec) adopt(in *segExec) {
	s.scanned += in.scanned
	in.scanned = 0
	if in.err != nil && s.err == nil {
		s.err = in.err
	}
}

// approximateNested is a nested node's approx over scope: the documents holding an
// object its inner query surely matches, and those holding one it may.
func (s *segExec) approximateNested(np *nestedPlan, scope *roaring.Bitmap) approx {
	in := s.inner(np.path)
	if in == nil {
		return emptyApprox
	}
	objScope := in.allDocs
	if scope != s.allDocs && scope.GetCardinality()*nestedScopeFactor < uint64(s.n) {
		objScope = roaring.And(in.nv.ObjectsOf(scope), in.allDocs)
	}
	a := in.approximate(np.inner, objScope)
	s.adopt(in)
	sure := roaring.And(in.nv.Parents(a.sure), scope)
	if a.maybe.IsEmpty() {
		return approx{sure: sure, maybe: roaring.New()}
	}
	maybe := roaring.And(in.nv.Parents(a.maybe), scope)
	maybe.AndNot(sure)
	return approx{sure: sure, maybe: maybe}
}

// resolveNested returns exactly the documents of d (maybe documents of np's node) that
// hold an object np's inner query matches: their maybe objects resolved.
func (s *segExec) resolveNested(np *nestedPlan, d *roaring.Bitmap) *roaring.Bitmap {
	in := s.inner(np.path)
	if in == nil {
		return roaring.New()
	}
	a, ok := in.approxes[np.inner]
	if !ok {
		return roaring.New()
	}
	objs := roaring.And(in.nv.ObjectsOf(d), a.maybe)
	got := in.resolve(np.inner, objs)
	s.adopt(in)
	return roaring.And(in.nv.Parents(got), d)
}

// checkNested reports whether document d holds an object np's inner query matches,
// checking its objects one at a time.
func (s *segExec) checkNested(np *nestedPlan, d uint32) bool {
	in := s.inner(np.path)
	if in == nil {
		return false
	}
	defer s.adopt(in)
	lo, hi := in.nv.Range(d)
	for o := lo; o < hi; o++ {
		if in.check(np.inner, o) {
			return true
		}
	}
	return false
}

// parentBody is one document's nested field as its stored body holds it, decoded:
// the objects of a document that are checked in turn read it once.
type parentBody struct {
	set    bool
	doc    uint32
	lo, hi uint32
	value  any
}

// objectValue reads sub-field field of object obj from its document's stored body,
// as Analyze reads it: the residual's source for what an object segment does not keep.
// c holds the last document read, decoded, and is read again only for an object of
// another document.
func (s *segExec) objectValue(field string, obj uint32, c *parentBody) (any, bool, error) {
	if !c.set || obj < c.lo || obj >= c.hi {
		doc := s.nv.Parent(obj)
		if c.set && obj >= c.hi {
			doc = s.nv.ParentFrom(c.doc, obj)
		}
		body, err := s.parent.r.Stored(doc)
		if err != nil {
			return nil, false, err
		}
		value, _, err := schema.FieldValue(body, s.path)
		if err != nil {
			return nil, false, err
		}
		lo, hi := s.nv.Range(doc)
		*c = parentBody{set: true, doc: doc, lo: lo, hi: hi, value: value}
	}
	v, ok := schema.ObjectFieldValue(c.value, int(obj-c.lo), strings.TrimPrefix(field, s.path+"."))
	return v, ok, nil
}

// parentCursor finds the documents of ascending objects, each sought from the last
// one's, and only when an object passes the last one's document.
type parentCursor struct {
	nv     *segment.Nested
	doc    uint32
	hi     uint32
	looked bool
}

func (c *parentCursor) of(obj uint32) uint32 {
	if !c.looked || obj >= c.hi {
		c.doc = c.nv.ParentFrom(c.doc, obj)
		_, c.hi = c.nv.Range(c.doc)
		c.looked = true
	}
	return c.doc
}

// nestedPartial collects an aggregation on a sub-field over the objects of hits that
// its where chooses (every object without one).
func (s *segExec) nestedPartial(spec *aggSpec, hits *roaring.Bitmap) *AggPartial {
	in := s.inner(spec.nested)
	if in == nil {
		return emptyPartial(spec)
	}
	defer s.adopt(in)
	objs := roaring.And(in.nv.ObjectsOf(hits), in.allDocs)
	if spec.wherePlan != nil {
		objs = in.matches(spec.wherePlan, objs)
	}
	if in.err != nil {
		return nil
	}
	if spec.typ == AggTerms {
		return in.nestedTerms(spec, objs)
	}
	c := in.newCollector(spec, false)
	in.collect(objs, []collector{c})
	return c.partial()
}

// nestedTerms counts a terms aggregation on a sub-field over objs: each document once
// per value, however many of its objects hold it, as key buckets. Each bucket collects
// spec's sub-aggregations: one on a top-level field over the bucket's documents (each
// once, on the parent segment), one on a sub-field over the bucket's objects (those of
// objs holding the value that its own where, if any, chooses).
func (s *segExec) nestedTerms(spec *aggSpec, objs *roaring.Bitmap) *AggPartial {
	src := s.source(spec.field, spec.ftype)
	allow := s.termMask(spec, src)
	allowed := func(o uint32) bool { return allow == nil || allow.has(o) }
	subs := s.nestedSubs(spec, objs)
	if s.err != nil {
		return nil
	}
	p := &AggPartial{Type: AggTerms}
	cur := parentCursor{nv: s.nv}
	it := objs.ManyIterator()
	buf := make([]uint32, 512)
	each := func(fn func(obj, doc uint32)) {
		for {
			n := it.NextMany(buf)
			if n == 0 || s.checkCtx() {
				return
			}
			for _, o := range buf[:n] {
				fn(o, cur.of(o))
			}
		}
	}
	switch src.kind {
	case srcKeyword, srcList, srcBool:
		size := uint32(2)
		if src.ordinal() {
			size = src.numTerms()
		}
		buckets := make([]nestedBucket, size)
		var ords []uint32
		each(func(obj, doc uint32) {
			switch src.kind {
			case srcKeyword:
				if o, ok := src.kc.Ord(obj); ok && allowed(o) {
					buckets[o].add(subs, obj, doc)
				}
			case srcList:
				ords = src.mc.Ords(obj, ords[:0])
				for _, o := range ords {
					if allowed(o) {
						buckets[o].add(subs, obj, doc)
					}
				}
			default:
				if b, ok := src.boolOf(obj); ok && allowed(boolOrd(b)) {
					buckets[boolOrd(b)].add(subs, obj, doc)
				}
			}
		})
		for o := range buckets {
			b := &buckets[o]
			if b.n == 0 {
				continue
			}
			var key any
			switch src.kind {
			case srcKeyword:
				key = src.kc.Term(uint32(o))
			case srcList:
				key = src.mc.Term(uint32(o))
			default:
				key = o == 1
			}
			p.Buckets = append(p.Buckets, b.partial(key, subs))
		}
	case srcNumber:
		byValue := map[float64]*nestedBucket{}
		each(func(obj, doc uint32) {
			v, ok := src.nc.Value(obj)
			if !ok {
				return
			}
			v = positiveZero(v)
			b := byValue[v]
			if b == nil {
				if spec.terms != nil && !spec.terms.number(v) {
					return
				}
				b = &nestedBucket{}
				byValue[v] = b
			}
			b.add(subs, obj, doc)
		})
		for _, v := range slices.Sorted(maps.Keys(byValue)) {
			p.Buckets = append(p.Buckets, byValue[v].partial(v, subs))
		}
	}
	return p
}

// nestedSubSpecs are the sub-aggregations of a terms aggregation on a sub-field, in one
// segment: spec's, the objects each one on a sub-field chooses (nil: every object, or
// one on a top-level field), and the executions their collectors read.
type nestedSubSpecs struct {
	spec   *aggSpec
	chosen []*roaring.Bitmap
	inner  *segExec
}

// nestedSubs prepares spec's sub-aggregations over objs, the objects spec collects.
func (s *segExec) nestedSubs(spec *aggSpec, objs *roaring.Bitmap) *nestedSubSpecs {
	if len(spec.subs) == 0 {
		return nil
	}
	ns := &nestedSubSpecs{spec: spec, chosen: make([]*roaring.Bitmap, len(spec.subs)), inner: s}
	for i, sub := range spec.subs {
		if sub.nested != "" && sub.wherePlan != nil {
			ns.chosen[i] = s.matches(sub.wherePlan, objs)
		}
	}
	return ns
}

// collectors are one bucket's sub-aggregation collectors: on the parent segment for a
// top-level field, on the object segment for a sub-field.
func (ns *nestedSubSpecs) collectors() []collector {
	out := make([]collector, len(ns.spec.subs))
	for i, sub := range ns.spec.subs {
		ex := ns.inner
		if sub.nested == "" {
			ex = ns.inner.parent
		}
		if sub.typ == AggTerms {
			out[i] = ex.newSubTerms(sub)
			continue
		}
		out[i] = ex.newCollector(sub, false)
	}
	return out
}

// nestedBucket is one value's bucket of a terms aggregation on a sub-field in one
// segment: its documents (n, the last one counted plus 1 in last) and its
// sub-aggregations' collectors.
type nestedBucket struct {
	n    int64
	last uint32
	subs []collector
}

// add counts object obj of document doc: the document once, and the object into the
// sub-aggregations that read it.
func (b *nestedBucket) add(ns *nestedSubSpecs, obj, doc uint32) {
	first := b.last != doc+1
	if first {
		b.last = doc + 1
		b.n++
	}
	if ns == nil {
		return
	}
	if b.subs == nil {
		b.subs = ns.collectors()
	}
	for i, sub := range ns.spec.subs {
		c := b.subs[i]
		switch {
		case sub.nested == "":
			if first {
				c.collect(doc)
			}
		case ns.chosen[i] != nil && !ns.chosen[i].Contains(obj):
		default:
			if st, ok := c.(*subTerms); ok {
				st.add(obj, doc)
				continue
			}
			c.collect(obj)
		}
	}
}

func (b *nestedBucket) partial(key any, ns *nestedSubSpecs) *BucketPartial {
	out := &BucketPartial{Key: key, DocCount: b.n}
	if ns != nil {
		out.Aggs = subPartials(ns.spec, b.subs)
	}
	return out
}

// subTerms is a terms aggregation under a terms aggregation on a sub-field, in one
// segment: documents per value, each document once per value however many of its
// objects hold it, as key buckets. It is never cut on a shard, so its counts are exact
// and its error bound 0; the reduce keeps its size.
type subTerms struct {
	spec  *aggSpec
	src   *fieldSrc
	allow *termMask
	ords  map[uint32]*nestedBucket
	nums  map[float64]*nestedBucket
	buf   []uint32
}

func (s *segExec) newSubTerms(spec *aggSpec) *subTerms {
	src := s.source(spec.field, spec.ftype)
	return &subTerms{spec: spec, src: src, allow: s.termMask(spec, src), ords: map[uint32]*nestedBucket{}, nums: map[float64]*nestedBucket{}}
}

func (c *subTerms) collect(d uint32) { c.add(d, d) }

// add counts d's values (d a document, or an object of document owner) for owner.
func (c *subTerms) add(d, owner uint32) {
	count := func(o uint32) {
		if c.allow != nil && !c.allow.has(o) {
			return
		}
		b := c.ords[o]
		if b == nil {
			b = &nestedBucket{}
			c.ords[o] = b
		}
		b.add(nil, d, owner)
	}
	switch c.src.kind {
	case srcKeyword:
		if o, ok := c.src.kc.Ord(d); ok {
			count(o)
		}
	case srcList:
		c.buf = c.src.mc.Ords(d, c.buf[:0])
		for _, o := range c.buf {
			count(o)
		}
	case srcBool:
		if b, ok := c.src.boolOf(d); ok {
			count(boolOrd(b))
		}
	case srcNumber:
		v, ok := c.src.nc.Value(d)
		if !ok {
			return
		}
		v = positiveZero(v)
		b := c.nums[v]
		if b == nil {
			if c.spec.terms != nil && !c.spec.terms.number(v) {
				return
			}
			b = &nestedBucket{}
			c.nums[v] = b
		}
		b.add(nil, d, owner)
	}
}

func (c *subTerms) partial() *AggPartial {
	p := &AggPartial{Type: AggTerms}
	for _, o := range slices.Sorted(maps.Keys(c.ords)) {
		var key any
		switch c.src.kind {
		case srcKeyword:
			key = c.src.kc.Term(o)
		case srcList:
			key = c.src.mc.Term(o)
		default:
			key = o == 1
		}
		p.Buckets = append(p.Buckets, &BucketPartial{Key: key, DocCount: c.ords[o].n})
	}
	for _, v := range slices.Sorted(maps.Keys(c.nums)) {
		p.Buckets = append(p.Buckets, &BucketPartial{Key: v, DocCount: c.nums[v].n})
	}
	return p
}

func boolOrd(b bool) uint32 {
	if b {
		return 1
	}
	return 0
}

// nestCol is a sort key on a sub-field in one segment: per document, the least or
// greatest value among its objects in q (those the key's where chooses).
type nestCol struct {
	nv  *segment.Nested
	q   []uint64 // the chosen objects, a bitset
	max bool
}

// nestCol returns spec's nestCol for s, and the object segment's columns it reads;
// nil when the segment holds no member of the field (every document misses the key).
func (s *segExec) nestCol(spec sortSpec) (*nestCol, *segExec) {
	in := s.inner(spec.nested.path)
	if in == nil {
		return &nestCol{}, nil
	}
	defer s.adopt(in)
	q := in.allDocs
	if spec.nested.where != nil {
		q = in.matches(spec.nested.where, in.allDocs)
	}
	words := make([]uint64, (uint64(in.n)+63)/64)
	q.WriteDenseTo(words)
	return &nestCol{nv: in.nv, q: words, max: spec.nested.max}, in
}

// key is document d's value of c's sort key, as part: a number, or twice a keyword's
// ordinal in the object segment's dictionary; missing when no object of d holds one.
func (n *nestCol) key(c *sortCol, d uint32) part {
	best := part{miss: true}
	if n.nv == nil {
		return best
	}
	lo, hi := n.nv.Range(d)
	for o := lo; o < hi; o++ {
		if n.q[o>>6]&(1<<(o&63)) == 0 {
			continue
		}
		var v float64
		switch c.spec.kind {
		case sortNumber:
			x, ok := c.nc.Value(o)
			if !ok {
				continue
			}
			v = x
		default:
			ord, ok := c.kc.Ord(o)
			if !ok {
				continue
			}
			v = 2 * float64(ord)
		}
		if best.miss || (n.max && v > best.v) || (!n.max && v < best.v) {
			best = part{v: v}
		}
	}
	return best
}

// InnerHits are one hit's inner hits under one key: how many members of its path
// satisfy the query of a nested node asking for them under it (Total, each member
// once), and the first of them (inner_hits' size) by their offsets in the array, in
// array order, and as objects of the sub-fields asked for.
type InnerHits struct {
	Total   int               `json:"total"`
	Offsets []int             `json:"offsets"`
	Objects []json.RawMessage `json:"objects"`
}

// innerSpec is the inner hits of one key, compiled: those of every nested node asking
// for them under it (all of one path), merged.
type innerSpec struct {
	key  string
	path string
	// size is the largest of the nodes' sizes, and fields the sub-fields any of them
	// names (nil, every one, when one names none).
	size   int
	fields []string
	// matches are the nodes' queries: a member is an inner hit when it satisfies one.
	matches []*query.Compiled
}

// innerSpecs are the inner hits r's query and post_filter ask for, one per key in the
// order the keys first appear: the nodes sharing a key (one path's, [ValidateQueries])
// merged.
func innerSpecs(r *Request) []innerSpec {
	var out []innerSpec
	at := map[string]int{}
	for _, n := range []query.Node{r.Query, r.PostFilter} {
		if n == nil {
			continue
		}
		query.Walk(n, func(_ string, node query.Node) bool {
			x, ok := node.(*query.Nested)
			if !ok || x.InnerHits == nil {
				return true
			}
			h, key := x.InnerHits, x.InnerHits.Key(x.Path)
			i, seen := at[key]
			if !seen {
				at[key] = len(out)
				out = append(out, innerSpec{key: key, path: x.Path, size: h.Size, fields: slices.Clone(h.Fields), matches: []*query.Compiled{query.Compile(x.Query)}})
				return true
			}
			spec := &out[i]
			spec.size = max(spec.size, h.Size)
			spec.matches = append(spec.matches, query.Compile(x.Query))
			switch {
			case len(spec.fields) == 0:
			case len(h.Fields) == 0:
				spec.fields = nil
			default:
				for _, f := range h.Fields {
					if !slices.Contains(spec.fields, f) {
						spec.fields = append(spec.fields, f)
					}
				}
			}
			return true
		})
	}
	return out
}

// match reports whether obj satisfies one of spec's queries.
func (spec *innerSpec) match(obj *schema.Object) bool {
	for _, m := range spec.matches {
		if m.MatchObject(obj) {
			return true
		}
	}
	return false
}

// MaxInnerHitsObjects bounds the inner hits one response holds: a search's size times
// the sizes of its inner_hits together (InnerHitsObjects) is at most it, else the
// search is refused ([ValidateQueries]).
const MaxInnerHitsObjects = 10_000

// InnerHitsObjects is the most inner hit objects a response to r holds: its size times
// the sizes of the inner hits its query and post_filter ask for.
func InnerHitsObjects(r *Request) int {
	n := 0
	for _, spec := range innerSpecs(r) {
		n += spec.size
	}
	return n * max(r.Size, 0)
}

// HasInnerHits reports whether r's query or post_filter asks for inner hits.
func HasInnerHits(r *Request) bool { return len(innerSpecs(r)) > 0 }

// InnerHitsFields are the fields a search for r fetches so that [FillInnerHits] can
// read its inner hits: r.Fields and every path whose inner hits r asks for, or nil
// (whole bodies) when r.Fields is.
func InnerHitsFields(r *Request) []string {
	if len(r.Fields) == 0 {
		return nil
	}
	out := slices.Clone(r.Fields)
	for _, spec := range innerSpecs(r) {
		if !slices.Contains(out, spec.path) {
			out = append(out, spec.path)
		}
	}
	return out
}

// FillInnerHits sets each hit's inner hits, for every key r's query and post_filter
// ask for them under, from its body, read under mapping m as the hit was indexed: each
// member that satisfies the query of a nested node under the key, in array order. Each
// body is then limited to fields (the request's own, before [InnerHitsFields] widened
// them), when set. It stops at ctx's end, with its error.
func FillInnerHits(ctx context.Context, hits []Hit, r *Request, m *schema.Mapping, fields []string) error {
	specs := innerSpecs(r)
	if len(specs) == 0 {
		return nil
	}
	for i := range hits {
		if err := ctx.Err(); err != nil {
			return err
		}
		h := &hits[i]
		if h.Body == nil {
			continue
		}
		h.InnerHits = make(map[string]*InnerHits, len(specs))
		values := map[string]any{}
		objects := map[string][]schema.Object{}
		for _, spec := range specs {
			if _, ok := objects[spec.path]; !ok {
				v, _, err := schema.FieldValue(h.Body, spec.path)
				if err != nil {
					return fmt.Errorf("search: hit %q: %w", h.ID, err)
				}
				values[spec.path] = v
				objects[spec.path] = schema.AnalyzeObjects(m, spec.path, v)
			}
			ih := &InnerHits{Offsets: []int{}, Objects: []json.RawMessage{}}
			for k := range objects[spec.path] {
				if !spec.match(&objects[spec.path][k]) {
					continue
				}
				ih.Total++
				if len(ih.Offsets) >= spec.size {
					continue
				}
				obj, err := innerObject(values[spec.path], k, spec)
				if err != nil {
					return fmt.Errorf("search: hit %q: %w", h.ID, err)
				}
				ih.Offsets = append(ih.Offsets, k)
				ih.Objects = append(ih.Objects, obj)
			}
			h.InnerHits[spec.key] = ih
		}
		if len(fields) > 0 {
			body, err := filterBody(h.Body, fields)
			if err != nil {
				return fmt.Errorf("search: hit %q: %w", h.ID, err)
			}
			h.Body = body
		}
	}
	return nil
}

// innerObject is member k of a nested field's value as an inner hit shows it: the
// object, or only the sub-fields spec asks for, by their own names.
func innerObject(value any, k int, spec innerSpec) (json.RawMessage, error) {
	var members []any
	switch v := value.(type) {
	case []any:
		members = v
	case map[string]any:
		members = []any{v}
	}
	obj, _ := members[k].(map[string]any)
	out := obj
	if len(spec.fields) > 0 {
		out = make(map[string]any, len(spec.fields))
		for _, f := range spec.fields {
			name := strings.TrimPrefix(f, spec.path+".")
			if v, ok := schema.ObjectFieldValue(value, k, name); ok {
				out[name] = v
			}
		}
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(out); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

// QueryInnerHits are the inner hits a query asks for (its nested nodes' inner_hits,
// merged by key as a search merges them), compiled, for reading them off analyzed
// documents: a percolation's matches report them.
type QueryInnerHits struct {
	specs []innerSpec
}

// NewQueryInnerHits compiles the inner hits n asks for; nil when it asks for none.
func NewQueryInnerHits(n query.Node) *QueryInnerHits {
	specs := innerSpecs(&Request{Query: n})
	if len(specs) == 0 {
		return nil
	}
	return &QueryInnerHits{specs: specs}
}

// Offsets returns d's inner hits by key, as a search's hit has them but without the
// objects: how many members of the key's path satisfy the query of a nested node under
// it (Total), and the offsets of the first size of them in the array, in array order.
// d is analyzed, its nested fields' objects in d.Fields.
func (q *QueryInnerHits) Offsets(d *schema.Doc) map[string]*InnerHits {
	out := make(map[string]*InnerHits, len(q.specs))
	for i := range q.specs {
		spec := &q.specs[i]
		ih := &InnerHits{Offsets: []int{}}
		objects := d.Fields[spec.path].Objects
		for k := range objects {
			if !spec.match(&objects[k]) {
				continue
			}
			ih.Total++
			if len(ih.Offsets) < spec.size {
				ih.Offsets = append(ih.Offsets, k)
			}
		}
		out[spec.key] = ih
	}
	return out
}
