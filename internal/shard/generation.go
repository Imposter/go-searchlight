package shard

import (
	"fmt"
	"path/filepath"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
)

// Generations and segment lifetimes.
//
// A Generation is an immutable snapshot: the segments and their deletes as of one
// commit. The shard publishes each through an atomic pointer and holds one reference on
// the current one; Acquire adds a reference with a compare-and-swap that only succeeds
// while the count is above zero, so a generation whose count reached zero (retired) can
// never be revived, and Release drops one. Nothing a reader touches takes a lock.
//
// Each segment is a segRef, opened once (one segment.Reader, so one mapping) and
// reference-counted by the generations that list it: a new generation takes its
// references before it is published, and a retired one drops them. A segment is
// therefore mapped for as long as any generation that exposes it is alive, from the
// first Acquire until the last Release, which is the lifetime its zero-copy bitmaps
// need. When a segRef's count reaches zero the janitor closes it (the final unmap) and,
// if a commit has since dropped it from the manifest (merged away), only then removes
// its files, which Windows refuses while they are mapped.

// segKind is a document or a query segment.
type segKind uint8

const (
	kindDocs segKind = iota
	kindQueries
)

func (k segKind) String() string {
	if k == kindQueries {
		return "query"
	}
	return "document"
}

// segRef is one open segment, shared by every generation that lists it.
type segRef struct {
	id      string
	kind    segKind
	numDocs uint32
	bytes   int64
	format  string // a query segment's builder format

	reader *segment.Reader // kindDocs
	qs     QuerySegment    // kindQueries

	// refs counts the live generations that list the segment. A segment a refresh or
	// merge has built but not yet published is at 0 and owned by that refresh or
	// merge, which closes it itself if it is never published; the generation that
	// publishes it takes its first reference.
	refs atomic.Int32
	// obsolete is set once a durable manifest no longer lists the segment: its files
	// are removed after its final close.
	obsolete atomic.Bool
}

// lookup returns the ordinal of the document or query whose id is exactly id, deleted
// or not: within one segment an id has at most one copy (a refresh writes each id
// once, a merge keeps only live copies, of which there is at most one per id, and
// segment.Build refuses duplicates).
func (r *segRef) lookup(id string) (uint32, bool) {
	if r.kind == kindDocs {
		return r.reader.Ord(id)
	}
	return r.qs.Ord(id)
}

// close releases the segment (the final unmap, for a document segment).
func (r *segRef) close() error {
	if r.kind == kindDocs {
		return r.reader.Close()
	}
	return r.qs.Close()
}

// segState is a segment as one generation sees it: with that generation's deletes.
type segState struct {
	ref     *segRef
	deletes *roaring.Bitmap // owned and never changed once published; emptyDeletes when none
	delGen  uint64          // the sidecar holding deletes; 0: none
	dirty   bool            // deletes changed in the commit being prepared: write a sidecar
}

// emptyDeletes is the deletes of every segment with none. Read-only, like every deletes
// bitmap a generation exposes.
var emptyDeletes = roaring.New()

// SegmentView is one document segment of a [Generation].
type SegmentView struct {
	// ID is the segment's name, unique for the shard's life: the [FilterCache] key.
	ID string
	// Reader is the open segment, valid until the generation's Release: so are the
	// zero-copy bitmaps its Postings, Present and Truncated return.
	Reader *segment.Reader
	// Deletes are the segment's deleted ordinals in this generation. Never nil, and
	// read-only: never modify it.
	Deletes *roaring.Bitmap
	// Base is the segment's first ordinal in the generation's ordinal space (the
	// NumDocs of every segment before it, added up).
	Base uint64
	// NumDocs is the segment's ordinals, live and deleted; Live the live ones.
	NumDocs uint32
	Live    uint32
}

// QuerySegmentView is one percolator query segment of a [Generation].
type QuerySegmentView struct {
	ID      string
	Segment QuerySegment
	// Deletes are the segment's deleted query ordinals in this generation. Never nil,
	// and read-only.
	Deletes *roaring.Bitmap
	Base    uint64
	// NumQueries is the segment's ordinals, live and deleted; Live the live ones.
	NumQueries uint32
	Live       uint32
}

// Generation is an immutable view of the shard: its segments and query segments, with
// deletes, as of one refresh or merge. Acquire one with [Shard.Acquire] and Release it
// when done; everything it exposes is valid in between, and never changes.
type Generation struct {
	refs  atomic.Int64
	shard *Shard

	gen     uint64
	seq     int64
	maxSeq  int64
	uid     string
	mapping *schema.Mapping

	// Segments are the document segments, in base-ordinal order.
	Segments []SegmentView
	// QuerySegments are the percolator's query segments.
	QuerySegments []QuerySegmentView

	docs, queries       []segState
	numDocs, numQueries uint64
}

// newGeneration builds a generation over docs and queries, taking a reference on each
// segment; the generation starts with one reference, the caller's.
func newGeneration(s *Shard, gen uint64, seq, maxSeq int64, uid string, docs, queries []segState) *Generation {
	g := &Generation{
		shard:   s,
		gen:     gen,
		seq:     seq,
		maxSeq:  maxSeq,
		uid:     uid,
		mapping: s.mapping.Load(),
		docs:    docs,
		queries: queries,
	}
	g.refs.Store(1)
	g.Segments = make([]SegmentView, len(docs))
	var base uint64
	for i := range docs {
		st := &docs[i]
		st.dirty = false
		st.ref.refs.Add(1)
		live := st.ref.numDocs - uint32(st.deletes.GetCardinality()) //nolint:gosec // deletes are ordinals below numDocs
		g.Segments[i] = SegmentView{
			ID: st.ref.id, Reader: st.ref.reader, Deletes: st.deletes,
			Base: base, NumDocs: st.ref.numDocs, Live: live,
		}
		base += uint64(st.ref.numDocs)
		g.numDocs += uint64(live)
	}
	g.QuerySegments = make([]QuerySegmentView, len(queries))
	base = 0
	for i := range queries {
		st := &queries[i]
		st.dirty = false
		st.ref.refs.Add(1)
		live := st.ref.numDocs - uint32(st.deletes.GetCardinality()) //nolint:gosec // deletes are ordinals below numDocs
		g.QuerySegments[i] = QuerySegmentView{
			ID: st.ref.id, Segment: st.ref.qs, Deletes: st.deletes,
			Base: base, NumQueries: st.ref.numDocs, Live: live,
		}
		base += uint64(st.ref.numDocs)
		g.numQueries += uint64(live)
	}
	return g
}

// tryRef adds a reference unless the generation is already retired.
func (g *Generation) tryRef() bool {
	for {
		n := g.refs.Load()
		if n <= 0 {
			return false
		}
		if g.refs.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// Release drops the reference Acquire gave. The generation, and every bitmap and
// reader taken from it, must not be used afterwards. It allocates nothing unless it
// retires the generation.
func (g *Generation) Release() {
	switch n := g.refs.Add(-1); {
	case n == 0:
		g.retireAll()
	case n < 0:
		panic("shard: Generation released more times than acquired")
	}
}

// retireAll drops the generation's references on its segments, handing any that
// reach zero to the janitor.
func (g *Generation) retireAll() {
	for _, list := range [][]segState{g.docs, g.queries} {
		for i := range list {
			if list[i].ref.refs.Add(-1) == 0 {
				g.shard.jan.retire(list[i].ref)
			}
		}
	}
}

// Gen is the commit that published the generation; it grows with every refresh and
// merge.
func (g *Generation) Gen() uint64 { return g.gen }

// Seq is the changelog position the generation covers: every change with a seq at or
// below it (this shard's, gaps included) is visible, and none above it.
func (g *Generation) Seq() int64 { return g.seq }

// MaxSeq is the newest change the generation holds: at most Seq.
func (g *Generation) MaxSeq() int64 { return g.maxSeq }

// IndexUID is the index incarnation the generation's data belongs to ("" when none was
// seen).
func (g *Generation) IndexUID() string { return g.uid }

// Mapping is the index mapping the shard had when the generation was published.
func (g *Generation) Mapping() *schema.Mapping { return g.mapping }

// NumDocs is how many live documents the generation holds.
func (g *Generation) NumDocs() uint64 { return g.numDocs }

// NumQueries is how many live saved queries the generation holds.
func (g *Generation) NumQueries() uint64 { return g.numQueries }

// FilterCache is the cache of per-segment leaf bitmaps; key it by SegmentView.ID.
func (g *Generation) FilterCache() *FilterCache { return g.shard.opts.FilterCache }

// DocFreq implements [TermStats]: how many documents hold term, deleted ones included.
func (g *Generation) DocFreq(field string, kind segment.TermKind, term string) uint64 {
	var n uint64
	for i := range g.Segments {
		n += uint64(g.Segments[i].Reader.TermFreq(field, kind, term))
	}
	return n
}

// Lookup finds the live copy of document id: its segment's index in Segments and its
// ordinal there.
func (g *Generation) Lookup(id string) (seg int, ord uint32, ok bool) {
	for i := range g.docs {
		st := &g.docs[i]
		if o, found := st.ref.lookup(id); found && !st.deletes.Contains(o) {
			return i, o, true
		}
	}
	return 0, 0, false
}

// Get returns the live document id's stored body, false when it has none.
func (g *Generation) Get(id string) ([]byte, bool, error) {
	seg, ord, ok := g.Lookup(id)
	if !ok {
		return nil, false, nil
	}
	body, err := g.Segments[seg].Reader.Stored(ord)
	if err != nil {
		return nil, false, err
	}
	return body, true, nil
}

// LookupQuery finds the live copy of saved query id: its query segment's index in
// QuerySegments and its ordinal there.
func (g *Generation) LookupQuery(id string) (seg int, ord uint32, ok bool) {
	for i := range g.queries {
		st := &g.queries[i]
		if o, found := st.ref.lookup(id); found && !st.deletes.Contains(o) {
			return i, o, true
		}
	}
	return 0, 0, false
}

// openGeneration opens every segment the manifest lists, checking each against it.
func (s *Shard) openGeneration(man *manifest) (*Generation, error) {
	var docs, queries []segState
	fail := func(err error) (*Generation, error) {
		for _, list := range [][]segState{docs, queries} {
			for i := range list {
				_ = list[i].ref.close()
			}
		}
		return nil, err
	}
	for _, ms := range man.Segments {
		r, err := segment.Open(filepath.Join(s.dir, ms.ID+segment.FileExt))
		if err != nil {
			return fail(fmt.Errorf("shard: segment %s: %w", ms.ID, err))
		}
		ref := &segRef{id: ms.ID, kind: kindDocs, numDocs: r.NumDocs(), bytes: ms.Bytes, reader: r}
		docs = append(docs, segState{ref: ref, deletes: emptyDeletes})
		if r.NumDocs() != ms.Docs {
			return fail(fmt.Errorf("shard: segment %s holds %d documents, the manifest says %d", ms.ID, r.NumDocs(), ms.Docs))
		}
		if err := s.loadDeletes(&docs[len(docs)-1], ms); err != nil {
			return fail(err)
		}
	}
	for _, ms := range man.QuerySegments {
		if ms.Format != s.opts.QueryIndex.Format() {
			return fail(fmt.Errorf("shard: query segment %s is in format %q, the query index builds %q", ms.ID, ms.Format, s.opts.QueryIndex.Format()))
		}
		qs, err := s.opts.QueryIndex.Open(s.dir, ms.ID)
		if err != nil {
			return fail(fmt.Errorf("shard: query segment %s: %w", ms.ID, err))
		}
		ref := &segRef{id: ms.ID, kind: kindQueries, numDocs: qs.NumQueries(), bytes: ms.Bytes, format: ms.Format, qs: qs}
		queries = append(queries, segState{ref: ref, deletes: emptyDeletes})
		if qs.NumQueries() != ms.Docs {
			return fail(fmt.Errorf("shard: query segment %s holds %d queries, the manifest says %d", ms.ID, qs.NumQueries(), ms.Docs))
		}
		if err := s.loadDeletes(&queries[len(queries)-1], ms); err != nil {
			return fail(err)
		}
	}
	g := newGeneration(s, man.Gen, man.Seq, man.MaxSeq, man.IndexUID, docs, queries)
	// newGeneration took the generation's references; the opener's are not needed.
	return g, nil
}

func (s *Shard) loadDeletes(st *segState, ms manifestSegment) error {
	if ms.DelGen == 0 {
		if ms.Deleted != 0 {
			return fmt.Errorf("shard: segment %s: the manifest counts %d deletes but names no sidecar", ms.ID, ms.Deleted)
		}
		return nil
	}
	del, err := segment.LoadDeletes(s.dir, ms.ID, ms.DelGen)
	if err != nil {
		return fmt.Errorf("shard: segment %s deletes: %w", ms.ID, err)
	}
	if del.GetCardinality() != uint64(ms.Deleted) {
		return fmt.Errorf("shard: segment %s: its deletes sidecar holds %d, the manifest says %d", ms.ID, del.GetCardinality(), ms.Deleted)
	}
	if !del.IsEmpty() && del.Maximum() >= st.ref.numDocs {
		return fmt.Errorf("shard: segment %s: a delete past its %d documents", ms.ID, st.ref.numDocs)
	}
	st.deletes, st.delGen = del, ms.DelGen
	return nil
}
