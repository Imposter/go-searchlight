package shard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
)

// Refresh makes every change applied so far searchable: it writes the buffer's
// documents as a new segment and its saved queries as a new query segment, masks every
// older copy of the ids the buffer touched, and publishes the new generation. It
// fsyncs nothing: the segment files are written unsynced and the deletes stay in
// memory until a flush makes them durable ([Shard.Flush]). Apply keeps going into a
// fresh buffer meanwhile, and readers keep the old generation until they release it. A
// refresh with nothing new is a no-op. When it fails before it publishes, the buffer
// is put back, so the next refresh retries it.
//
// A refresh whose buffer is empty, when only the changelog position moved ([Advance]),
// publishes the new seq at once and writes nothing.
func (s *Shard) Refresh(ctx context.Context) error {
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	cur := s.cur.Load()
	if cur == nil {
		return ErrClosed
	}

	s.mu.Lock()
	fb := s.buf
	seq, maxSeq, uid, mp := s.applied, s.maxChange, s.indexUID, s.mapState
	if fb.empty() {
		s.mu.Unlock()
		if seq == cur.seq && uid == cur.uid && mp == cur.mp {
			return nil
		}
		ctx, span := s.startSpan(ctx, "shard.refresh", attribute.Int64("seq", seq), attribute.Bool("seq_only", true))
		defer span.End()
		s.publishSeq(ctx, seq, maxSeq, uid, mp)
		return nil
	}
	s.buf = newBuffer()
	s.bufBytes.Store(0)
	s.mu.Unlock()

	ctx, span := s.startSpan(ctx, "shard.refresh",
		attribute.Int("documents", len(fb.docs)), attribute.Int("queries", len(fb.queries)), attribute.Int64("seq", seq))
	defer span.End()
	start := s.opts.Clock.Now()
	if err := s.refresh(ctx, fb, seq, maxSeq, uid, mp); err != nil {
		if !isCrash(err) {
			s.mu.Lock()
			fb.absorb(s.buf)
			s.buf = fb
			s.bufBytes.Store(fb.bytes)
			s.mu.Unlock()
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "refresh failed")
		s.inst.refreshFailures.Add(ctx, 1, s.inst.attrs)
		return err
	}
	d := s.opts.Clock.Since(start)
	s.inst.recordRefresh(ctx, d)
	s.mu.Lock()
	buffered := s.buf.size()
	s.mu.Unlock()
	s.inst.recordBuffer(ctx, buffered)
	s.log.DebugContext(ctx, "refreshed", slog.Int64("seq", seq), slog.Int("documents", len(fb.docs)),
		slog.Int("queries", len(fb.queries)), slog.Float64(telemetryDuration, float64(d.Microseconds())/1000))
	s.wakeMerges()
	return nil
}

// telemetryDuration is telemetry.KeyDuration, the log key of a duration in ms.
const telemetryDuration = "duration_ms"

// refresh writes a frozen buffer's segments and publishes them. When it fails, nothing
// of fb is visible.
func (s *Shard) refresh(ctx context.Context, fb *buffer, seq, maxSeq int64, uid string, mp *mappingState) error {
	var built []*segRef
	discard := func(err error) error {
		for _, ref := range built {
			_ = ref.close()
			if !isCrash(err) {
				s.jan.removeLater(segmentFiles(s.dir, ref.id)...)
			}
		}
		return err
	}

	if docs := fb.liveDocs(); len(docs) > 0 {
		ref, err := s.buildDocSegment(ctx, docs)
		if err != nil {
			return err
		}
		built = append(built, ref)
	}
	if err := s.hook(pointRefreshBuilt); err != nil {
		return discard(err)
	}
	if queries := fb.liveQueries(); len(queries) > 0 {
		g := s.Acquire()
		if g == nil {
			return discard(ErrClosed)
		}
		ref, err := s.buildQuerySegment(ctx, queries, g)
		g.Release()
		if err != nil {
			return discard(err)
		}
		built = append(built, ref)
	}

	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	cur := s.cur.Load()
	if cur == nil {
		return discard(ErrClosed)
	}
	docs := s.mask(cur.docs, fb.docIDs())
	queries := s.mask(cur.queries, fb.queryIDs())
	for _, ref := range built {
		st := segState{ref: ref, deletes: emptyDeletes}
		if ref.kind == kindDocs {
			docs = append(docs, st)
		} else {
			queries = append(queries, st)
		}
	}
	s.publish(ctx, docs, queries, built, seq, maxSeq, uid, mp)
	return nil
}

// buildDocSegment writes docs as a new segment and opens it.
func (s *Shard) buildDocSegment(ctx context.Context, docs []schema.Doc) (*segRef, error) {
	_, span := s.startSpan(ctx, "shard.build_segment", attribute.Int("documents", len(docs)))
	defer span.End()
	name := newSegmentName()
	// Each Build worker keeps its own per-term structures for its range of documents,
	// so a small refresh split many ways mostly multiplies that overhead.
	threads := min(s.opts.RefreshThreads, max(1, len(docs)/docsPerBuildThread))
	meta, err := segment.Build(s.dir, docs, segment.BuildOptions{Name: name, Threads: threads, NoSync: true, MarksUntyped: true})
	if err != nil {
		s.jan.removeLater(segmentFiles(s.dir, name)...)
		return nil, fmt.Errorf("shard: building a segment: %w", err)
	}
	ref, err := s.openDocSegment(meta)
	if err != nil {
		s.jan.removeLater(segmentFiles(s.dir, name)...)
		return nil, err
	}
	return ref, nil
}

// docsPerBuildThread is the fewest documents a refresh gives each Build worker.
const docsPerBuildThread = 2048

// openDocSegment opens a segment Build or Merge just wrote.
func (s *Shard) openDocSegment(meta segment.Meta) (*segRef, error) {
	info, err := os.Stat(meta.Path)
	if err != nil {
		return nil, fmt.Errorf("shard: %w", err)
	}
	r, err := segment.Open(meta.Path)
	if err != nil {
		return nil, fmt.Errorf("shard: opening segment %s: %w", meta.ID, err)
	}
	return newDocRef(meta.ID, r, info.Size(), false), nil
}

// buildQuerySegment writes queries as a new query segment and opens it.
func (s *Shard) buildQuerySegment(ctx context.Context, queries []StoredQuery, stats TermStats) (*segRef, error) {
	ctx, span := s.startSpan(ctx, "shard.build_query_segment", attribute.Int("queries", len(queries)))
	defer span.End()
	qi := s.opts.QueryIndex
	name := newSegmentName()
	size, err := qi.Build(ctx, s.dir, name, queries, stats)
	if err != nil {
		s.jan.removeLater(segmentFiles(s.dir, name)...)
		return nil, fmt.Errorf("shard: building a query segment: %w", err)
	}
	qs, err := qi.Open(s.dir, name)
	if err != nil {
		s.jan.removeLater(segmentFiles(s.dir, name)...)
		return nil, fmt.Errorf("shard: opening query segment %s: %w", name, err)
	}
	return &segRef{id: name, kind: kindQueries, numDocs: qs.NumQueries(), bytes: size, format: qi.Format(), qs: qs}, nil
}

// maskTask is one chunk of ids looked up in one segment.
type maskTask struct {
	seg    int
	lo, hi int
}

const maskChunk = 4096

// mask returns a copy of states in which every live copy of ids is deleted: the ids a
// refresh is about to write (or delete) mask their older versions. A segment whose
// deletes change gets a new bitmap (the old one belongs to published generations) and
// is marked dirty. Lookups run in parallel over segments and chunks of ids.
func (s *Shard) mask(states []segState, ids []string) []segState {
	out := slices.Clone(states)
	if len(ids) == 0 || len(out) == 0 {
		return out
	}
	var tasks []maskTask
	for i := range out {
		for lo := 0; lo < len(ids); lo += maskChunk {
			tasks = append(tasks, maskTask{seg: i, lo: lo, hi: min(lo+maskChunk, len(ids))})
		}
	}
	found := make([]*roaring.Bitmap, len(tasks))
	run := func(t int) {
		task := tasks[t]
		st := &out[task.seg]
		var add *roaring.Bitmap
		for _, id := range ids[task.lo:task.hi] {
			if ord, ok := st.ref.lookup(id); ok && !st.deletes.Contains(ord) {
				if add == nil {
					add = roaring.New()
				}
				add.Add(ord)
			}
		}
		found[t] = add
	}
	parallel(len(tasks), runtime.GOMAXPROCS(0), run)
	for t, add := range found {
		if add == nil {
			continue
		}
		st := &out[tasks[t].seg]
		if !st.dirty {
			st.deletes = st.deletes.Clone()
			st.dirty = true
		}
		st.deletes.Or(add)
	}
	return out
}

// publish makes docs and queries, built among them, the shard's visible state as a new
// generation, and numbers the deletes it changed (dirty) for it: the flush that
// persists it writes them as sidecars of that number. It writes nothing. The caller
// holds commitMu.
func (s *Shard) publish(ctx context.Context, docs, queries []segState, built []*segRef, seq, maxSeq int64, uid string, mp *mappingState) {
	s.gen++
	for _, list := range [][]segState{docs, queries} {
		for i := range list {
			if list[i].dirty {
				list[i].delGen = s.gen
			}
		}
	}
	g := newGeneration(s, s.gen, seq, maxSeq, uid, mp, docs, queries)
	s.unflushed = append(s.unflushed, built...)
	if old := s.cur.Swap(g); old != nil {
		old.Release()
	}
	s.notifyPublished()
	s.recordGeneration(ctx, g)
}

// publishSeq publishes the current segments as covering seq: a refresh with nothing
// to write.
func (s *Shard) publishSeq(ctx context.Context, seq, maxSeq int64, uid string, mp *mappingState) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	cur := s.cur.Load()
	if cur == nil {
		return
	}
	g := newGeneration(s, cur.gen, seq, maxSeq, uid, mp, slices.Clone(cur.docs), slices.Clone(cur.queries))
	s.cur.Store(g)
	cur.Release()
	s.notifyPublished()
	s.recordGeneration(ctx, g)
}

func buildManifest(gen uint64, seq, maxSeq int64, uid string, mp *mappingState, docs, queries []segState) (*manifest, error) {
	m := &manifest{
		Gen: gen, Seq: seq, MaxSeq: maxSeq, IndexUID: uid,
		Segments: make([]manifestSegment, len(docs)), QuerySegments: make([]manifestSegment, len(queries)),
	}
	if mp.m != nil {
		raw, err := json.Marshal(mp.m)
		if err != nil {
			return nil, fmt.Errorf("shard: encoding the mapping: %w", err)
		}
		m.Mapping, m.MappingVersion = raw, mp.version
	}
	for i := range docs {
		m.Segments[i] = manifestEntry(&docs[i])
	}
	for i := range queries {
		m.QuerySegments[i] = manifestEntry(&queries[i])
	}
	return m, nil
}

func manifestEntry(st *segState) manifestSegment {
	ms := manifestSegment{ID: st.ref.id, Docs: st.ref.numDocs, Bytes: st.ref.bytes, Format: st.ref.format}
	if st.delGen > 0 {
		ms.DelGen = st.delGen
		ms.Deleted = uint32(st.deletes.GetCardinality()) //nolint:gosec // deletes are ordinals of one segment
	}
	return ms
}

// newSegmentName returns a segment name unique for the shard's life (and any other's):
// 128 random bits, hex-encoded.
func newSegmentName() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand failing means the OS's RNG is broken
	}
	return hex.EncodeToString(b[:])
}

// isCrash reports whether err is a test's simulated crash.
func isCrash(err error) bool { return errors.Is(err, errSimulatedCrash) }

// syncWorkers bounds the files a flush fsyncs or writes at once.
const syncWorkers = 8

// parallel calls work(i) for every i in [0, n) on up to workers goroutines.
func parallel(n, workers int, work func(i int)) {
	workers = min(workers, n)
	if workers <= 1 {
		for i := range n {
			work(i)
		}
		return
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				work(i)
			}
		}()
	}
	wg.Wait()
}
