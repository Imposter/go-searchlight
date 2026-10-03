package shard

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
)

// Refresh makes every change applied so far searchable and durable: it writes the
// buffer's documents as a new segment and its saved queries as a new query segment,
// masks every older copy of the ids the buffer touched, commits the manifest, and
// publishes the new generation. Apply keeps going into a fresh buffer meanwhile, and
// readers keep the old generation until they release it. A refresh with nothing new
// is a no-op. When it fails before its commit, the buffer is put back, so the next
// refresh retries it.
func (s *Shard) Refresh(ctx context.Context) error {
	ctx, span := s.tr.Start(ctx, "shard.refresh")
	defer span.End()
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
	seq, maxSeq, uid := s.applied, s.maxChange, s.indexUID
	if fb.empty() && seq == cur.seq && uid == cur.uid {
		s.mu.Unlock()
		return nil
	}
	s.buf = newBuffer()
	s.mu.Unlock()

	start := time.Now()
	span.SetAttributes(attribute.Int("documents", len(fb.docs)), attribute.Int("queries", len(fb.queries)), attribute.Int64("seq", seq))
	published, err := s.refresh(ctx, fb, seq, maxSeq, uid)
	if err != nil {
		if !published && !isCrash(err) {
			s.mu.Lock()
			fb.absorb(s.buf)
			s.buf = fb
			s.mu.Unlock()
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, "refresh failed")
		return err
	}
	d := time.Since(start)
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

// refresh writes and commits a frozen buffer. published reports whether a generation
// was published (even if the shard then failed): if not, nothing of fb is visible.
func (s *Shard) refresh(ctx context.Context, fb *buffer, seq, maxSeq int64, uid string) (published bool, err error) {
	var built []*segRef
	discard := func(err error) (bool, error) {
		for _, ref := range built {
			_ = ref.close()
			if !isCrash(err) {
				s.jan.removeLater(segmentFiles(s.dir, ref.id)...)
			}
		}
		return false, err
	}

	if docs := fb.liveDocs(); len(docs) > 0 {
		ref, err := s.buildDocSegment(ctx, docs)
		if err != nil {
			return false, err
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
	published, err = s.commit(ctx, docs, queries, nil, seq, maxSeq, uid)
	if !published {
		return discard(err)
	}
	return true, err
}

// buildDocSegment writes docs as a new segment and opens it.
func (s *Shard) buildDocSegment(ctx context.Context, docs []schema.Doc) (*segRef, error) {
	_, span := s.tr.Start(ctx, "shard.build_segment", trace.WithAttributes(attribute.Int("documents", len(docs))))
	defer span.End()
	name := newSegmentName()
	// Each Build worker keeps its own per-term structures for its range of documents,
	// so a small refresh split many ways mostly multiplies that overhead.
	threads := min(s.opts.RefreshThreads, max(1, len(docs)/docsPerBuildThread))
	meta, err := segment.Build(s.dir, docs, segment.BuildOptions{Name: name, Threads: threads})
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
	return &segRef{id: meta.ID, kind: kindDocs, numDocs: r.NumDocs(), bytes: info.Size(), reader: r}, nil
}

// buildQuerySegment writes queries as a new query segment and opens it.
func (s *Shard) buildQuerySegment(ctx context.Context, queries []StoredQuery, stats TermStats) (*segRef, error) {
	ctx, span := s.tr.Start(ctx, "shard.build_query_segment", trace.WithAttributes(attribute.Int("queries", len(queries))))
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

// commit makes docs and queries (with removed no longer among them) the shard's
// durable and visible state: it writes the dirty segments' deletes sidecars and the
// manifest, then publishes the generation. The caller holds commitMu. published
// reports whether the generation was published: a failure after the manifest was
// renamed into place (its directory fsync) publishes anyway, since the manifest may
// well be durable, and fails the shard.
func (s *Shard) commit(ctx context.Context, docs, queries []segState, removed []*segRef, seq, maxSeq int64, uid string) (published bool, err error) {
	s.gen++
	gen := s.gen // never reused, even if this commit fails
	var written, obsolete []string
	fail := func(err error) (bool, error) {
		if isCrash(err) {
			s.fail(err)
		} else {
			s.jan.removeLater(written...)
		}
		return false, err
	}
	var dirty []*segState
	for _, list := range [][]segState{docs, queries} {
		for i := range list {
			if list[i].dirty {
				dirty = append(dirty, &list[i])
			}
		}
	}
	for _, st := range dirty {
		s.jan.forget(filepath.Join(s.dir, deletesName(st.ref.id, gen)))
	}
	// Each sidecar is fsynced on its own; writing them in parallel overlaps the syncs.
	sidecarErrs := make([]error, len(dirty))
	parallel(len(dirty), sidecarWriters, func(i int) {
		st := dirty[i]
		sidecarErrs[i] = segment.WriteDeletes(s.dir, st.ref.id, gen, st.deletes)
	})
	for i, st := range dirty {
		written = append(written, filepath.Join(s.dir, deletesName(st.ref.id, gen)))
		if st.delGen > 0 {
			obsolete = append(obsolete, filepath.Join(s.dir, deletesName(st.ref.id, st.delGen)))
		}
		st.delGen = gen
		if err := sidecarErrs[i]; err != nil {
			return fail(fmt.Errorf("shard: writing deletes of %s: %w", st.ref.id, err))
		}
	}
	if err := s.hook(pointCommitSidecars); err != nil {
		return fail(err)
	}
	man := buildManifest(gen, seq, maxSeq, uid, docs, queries)
	renamed, err := writeManifest(s.dir, man, s.hook, s.jan.forget)
	if err != nil {
		if !renamed {
			return fail(fmt.Errorf("shard: writing the manifest: %w", err))
		}
		if isCrash(err) {
			// The process "died" right after the rename: nothing more happens in it.
			s.fail(err)
			return false, err
		}
		err = fmt.Errorf("shard: the manifest swap may not be durable: %w", err)
		s.fail(err)
	}

	g := newGeneration(s, gen, seq, maxSeq, uid, docs, queries)
	for _, ref := range removed {
		ref.obsolete.Store(true)
	}
	old := s.cur.Swap(g)
	s.committed.Store(seq)
	if old != nil {
		old.Release()
	}
	s.jan.removeLater(obsolete...)
	s.notifyPublished()
	s.recordGeneration(ctx, g)
	return true, err
}

func buildManifest(gen uint64, seq, maxSeq int64, uid string, docs, queries []segState) *manifest {
	m := &manifest{
		Gen: gen, Seq: seq, MaxSeq: maxSeq, IndexUID: uid,
		Segments: make([]manifestSegment, len(docs)), QuerySegments: make([]manifestSegment, len(queries)),
	}
	for i := range docs {
		m.Segments[i] = manifestEntry(&docs[i])
	}
	for i := range queries {
		m.QuerySegments[i] = manifestEntry(&queries[i])
	}
	return m
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

// sidecarWriters bounds the deletes sidecars a commit writes at once.
const sidecarWriters = 8

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
