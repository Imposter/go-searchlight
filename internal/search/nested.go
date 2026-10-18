package search

import (
	"bytes"
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

// objectValue reads sub-field field of object obj from its document's stored body,
// as Analyze reads it: the residual's source for what an object segment does not keep.
func (s *segExec) objectValue(field string, obj uint32) (any, bool, error) {
	doc := s.nv.Parent(obj)
	lo, _ := s.nv.Range(doc)
	body, err := s.parent.r.Stored(doc)
	if err != nil {
		return nil, false, err
	}
	value, ok, err := schema.FieldValue(body, s.path)
	if err != nil || !ok {
		return nil, false, err
	}
	v, ok := schema.ObjectFieldValue(value, int(obj-lo), strings.TrimPrefix(field, s.path+"."))
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
// per value, however many of its objects hold it, as key buckets.
func (s *segExec) nestedTerms(spec *aggSpec, objs *roaring.Bitmap) *AggPartial {
	src := s.source(spec.field, spec.ftype)
	allow := s.termMask(spec, src)
	allowed := func(o uint32) bool { return allow == nil || allow.has(o) }
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
		counts := make([]int64, size)
		last := make([]uint32, size) // each value's last document counted, plus 1
		count := func(o, doc uint32) {
			if allowed(o) && last[o] != doc+1 {
				last[o] = doc + 1
				counts[o]++
			}
		}
		each(func(obj, doc uint32) {
			switch src.kind {
			case srcKeyword:
				if o, ok := src.kc.Ord(obj); ok {
					count(o, doc)
				}
			case srcList:
				src.buf = src.mc.Ords(obj, src.buf[:0])
				for _, o := range src.buf {
					count(o, doc)
				}
			default:
				if b, ok := src.boolOf(obj); ok {
					count(boolOrd(b), doc)
				}
			}
		})
		for o, n := range counts {
			if n == 0 {
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
			p.Buckets = append(p.Buckets, &BucketPartial{Key: key, DocCount: n})
		}
	case srcNumber:
		type tally struct {
			n    int64
			last uint32
		}
		byValue := map[float64]*tally{}
		each(func(obj, doc uint32) {
			v, ok := src.nc.Value(obj)
			if !ok {
				return
			}
			v = positiveZero(v)
			t := byValue[v]
			if t == nil {
				if spec.terms != nil && !spec.terms.number(v) {
					return
				}
				t = &tally{}
				byValue[v] = t
			}
			if t.last != doc+1 {
				t.last = doc + 1
				t.n++
			}
		})
		for _, v := range slices.Sorted(maps.Keys(byValue)) {
			p.Buckets = append(p.Buckets, &BucketPartial{Key: v, DocCount: byValue[v].n})
		}
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

// InnerHits are one hit's inner hits for one nested node: how many members of its
// path satisfy the node's query (Total), and the first of them (inner_hits' size) by
// their offsets in the array and as objects of the sub-fields asked for.
type InnerHits struct {
	Total   int               `json:"total"`
	Offsets []int             `json:"offsets"`
	Objects []json.RawMessage `json:"objects"`
}

// innerSpec is one nested node's inner hits, compiled.
type innerSpec struct {
	key    string
	path   string
	size   int
	fields []string
	match  *query.Compiled
}

// innerSpecs are the inner hits r's query and post_filter ask for.
func innerSpecs(r *Request) []innerSpec {
	var out []innerSpec
	for _, n := range []query.Node{r.Query, r.PostFilter} {
		if n == nil {
			continue
		}
		query.Walk(n, func(_ string, node query.Node) bool {
			if x, ok := node.(*query.Nested); ok && x.InnerHits != nil {
				out = append(out, innerSpec{
					key: x.InnerHits.Key(x.Path), path: x.Path, size: x.InnerHits.Size,
					fields: x.InnerHits.Fields, match: query.Compile(x.Query),
				})
			}
			return true
		})
	}
	return out
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

// FillInnerHits sets each hit's inner hits, for every nested node of r's query and
// post_filter that asks for them, from its body, read under mapping m as the hit was
// indexed: each member that satisfies the node's query, in array order. Each body is
// then limited to fields (the request's own, before [InnerHitsFields] widened them),
// when set.
func FillInnerHits(hits []Hit, r *Request, m *schema.Mapping, fields []string) error {
	specs := innerSpecs(r)
	if len(specs) == 0 {
		return nil
	}
	for i := range hits {
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
				if !spec.match.MatchObject(&objects[spec.path][k]) {
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
