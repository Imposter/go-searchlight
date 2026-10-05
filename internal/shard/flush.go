package shard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/Imposter/go-searchlight/internal/segment"
)

// Refresh and flush.
//
// A refresh is visibility: it publishes a generation for search and fsyncs nothing.
// Its segment is written unsynced, readable through the page cache at once, and the
// deletes it changed stay in memory. A flush is durability: it makes the current
// generation durable by fsyncing the segment files no flush has synced yet, writing and
// fsyncing the deletes sidecars the generation's segments use, then writing the
// manifest (a temp file, fsynced, renamed, one directory fsync), and only then
// advancing CommittedSeq.
//
// The SQL changelog is the write-ahead log, so a crash loses nothing acknowledged: the
// copy reopens at the last flushed manifest and replays the changelog after its seq.
// CommittedSeq, which the tailer reports to the registry and the cluster prunes the
// changelog by, is always the seq of a manifest durable on disk.
//
// A flush runs every Options.FlushInterval, at Close, at every merge commit (the merge
// is durable when it returns, and its inputs can go), and before a peer [Snapshot]; a
// flush with nothing new is a no-op. Flushes run one at a time (flushMu), and a flush
// holds the commit lock only to take its generation and to record what it made
// durable: its fsyncs never hold up a refresh.
//
// A file is removed only once no durable manifest references it. A segment a merge
// dropped, or one published and merged away before any flush, is marked obsolete by
// the first flush whose manifest does not list it, and its files are removed after its
// final close, whichever comes second. A sidecar is removed by the first flush whose
// manifest does not list it. What a crash leaves (unsynced segments, the sidecars of a
// flush that never renamed its manifest) is collected at the next Open.

// Flush makes the published generation durable and advances [Shard.CommittedSeq] to
// its seq. A flush with nothing new is a no-op.
func (s *Shard) Flush(ctx context.Context) error {
	g, err := s.flush(ctx)
	if g != nil {
		g.Release()
	}
	return err
}

// flush makes the current generation durable and returns it acquired, for the caller to
// release.
func (s *Shard) flush(ctx context.Context) (*Generation, error) {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	if err := s.Err(); err != nil {
		return nil, err
	}
	s.commitMu.Lock()
	g := s.cur.Load()
	if g == nil || !g.tryRef() {
		s.commitMu.Unlock()
		return nil, ErrClosed
	}
	if g.gen == s.durableGen && g.seq == s.committed.Load() && g.uid == s.committedUID && g.mp == s.committedMap {
		s.commitMu.Unlock()
		return g, nil
	}
	pending := s.unflushed
	s.unflushed = nil
	s.commitMu.Unlock()

	ctx, span := s.startSpan(ctx, "shard.flush", attribute.Int64("seq", g.seq), attribute.Int64("gen", int64(g.gen))) //nolint:gosec // a commit counter
	defer span.End()
	start := s.opts.Clock.Now()
	sidecars, err := s.persist(ctx, g)
	if err != nil {
		if !isCrash(err) {
			s.commitMu.Lock()
			s.unflushed = append(pending, s.unflushed...)
			s.commitMu.Unlock()
		}
		g.Release()
		span.RecordError(err)
		span.SetStatus(codes.Error, "flush failed")
		s.inst.flushFailures.Add(ctx, 1, s.inst.attrs)
		return nil, err
	}
	s.retireDurable(g, pending, sidecars)
	d := s.opts.Clock.Since(start)
	s.inst.recordFlush(ctx, d)
	s.log.DebugContext(ctx, "flushed", slog.Int64("seq", g.seq), slog.Uint64("gen", g.gen),
		slog.Float64(telemetryDuration, float64(d.Microseconds())/1000))
	return g, nil
}

// persist writes g durably: its unsynced segment files fsynced, its sidecars written,
// its manifest swapped in. It returns g's sidecars with their sizes. A failure after the
// manifest's rename (its directory fsync) fails the shard: the old manifest may come
// back, so nothing is retired and CommittedSeq stays.
func (s *Shard) persist(ctx context.Context, g *Generation) (map[string]int64, error) {
	var syncs []string
	var synced []*segRef
	var writes []*segState
	sidecars := map[string]int64{}
	for _, list := range [][]segState{g.docs, g.queries} {
		for i := range list {
			st := &list[i]
			if !st.ref.synced {
				files, err := s.dataFiles(st.ref)
				if err != nil {
					return nil, err
				}
				syncs = append(syncs, files...)
				synced = append(synced, st.ref)
			}
			if st.delGen == 0 {
				continue
			}
			path := filepath.Join(s.dir, deletesName(st.ref.id, st.delGen))
			if size, ok := s.durableSidecars[path]; ok {
				sidecars[path] = size
				continue
			}
			s.jan.forget(path)
			writes = append(writes, st)
		}
	}
	errs := make([]error, len(syncs)+len(writes))
	parallel(len(errs), syncWorkers, func(i int) {
		if i < len(syncs) {
			errs[i] = segment.SyncPath(syncs[i])
			return
		}
		st := writes[i-len(syncs)]
		errs[i] = segment.WriteDeletes(s.dir, st.ref.id, st.delGen, st.deletes, segment.DeletesOptions{NoDirSync: true})
	})
	for _, st := range writes {
		s.strays[filepath.Join(s.dir, deletesName(st.ref.id, st.delGen))] = true
	}
	for i, path := range syncs {
		if errs[i] != nil {
			return nil, fmt.Errorf("shard: syncing %s: %w", filepath.Base(path), errs[i])
		}
	}
	for i, st := range writes {
		if err := errs[len(syncs)+i]; err != nil {
			return nil, fmt.Errorf("shard: writing deletes of %s: %w", st.ref.id, err)
		}
		path := filepath.Join(s.dir, deletesName(st.ref.id, st.delGen))
		var size int64
		if info, err := os.Stat(path); err == nil {
			size = info.Size()
		}
		sidecars[path] = size
	}
	for _, ref := range synced {
		ref.synced = true
	}
	if err := s.hook(pointFlushSynced); err != nil {
		return nil, err
	}
	man, err := buildManifest(g.gen, g.seq, g.maxSeq, g.uid, g.mp, g.docs, g.queries)
	if err != nil {
		return nil, err
	}
	if s.marksUntyped {
		man.UntypedMarks = untypedMarksFormat
	}
	manBytes, renamed, err := writeManifest(s.dir, man, s.hook, s.jan.forget, s.log)
	switch {
	case err == nil:
	case !renamed:
		return nil, fmt.Errorf("shard: writing the manifest: %w", err)
	case isCrash(err):
		return nil, err
	default:
		err = fmt.Errorf("shard: the manifest swap may not be durable: %w", err)
		s.fail(err)
		s.log.ErrorContext(ctx, "flush failed after the manifest swap; the copy must be reopened", slog.Any("error", err))
		return nil, err
	}
	s.manifestBytes.Store(manBytes)
	return sidecars, nil
}

// retireDurable records g as the durable state once its manifest is: CommittedSeq moves
// to its seq, segments no longer in it (merged away, whether a manifest listed them or
// only a generation published since the last flush did) become obsolete, and sidecars
// it does not list are removed. pending are the segments published since the previous
// flush. The caller holds flushMu.
func (s *Shard) retireDurable(g *Generation, pending []*segRef, sidecars map[string]int64) {
	keep := make(map[*segRef]bool, len(g.docs)+len(g.queries))
	for _, list := range [][]segState{g.docs, g.queries} {
		for i := range list {
			keep[list[i].ref] = true
		}
	}
	for ref := range s.durableSegs {
		if !keep[ref] {
			s.markObsolete(ref)
		}
	}
	for _, ref := range pending {
		if !keep[ref] && !s.durableSegs[ref] {
			s.markObsolete(ref)
		}
	}
	s.durableSegs = keep

	var obsolete []string
	var bytes int64
	for _, size := range sidecars {
		bytes += size
	}
	for path := range s.durableSidecars {
		if _, ok := sidecars[path]; !ok {
			obsolete = append(obsolete, path)
		}
	}
	for path := range s.strays {
		if _, ok := sidecars[path]; !ok {
			if _, durable := s.durableSidecars[path]; !durable {
				obsolete = append(obsolete, path)
			}
		}
	}
	clear(s.strays)
	s.durableSidecars = sidecars
	s.sidecarBytes.Store(bytes)
	s.durableGen, s.committedUID, s.committedMap = g.gen, g.uid, g.mp
	s.committed.Store(g.seq)
	s.jan.removeLater(obsolete...)
}

// markObsolete records that no durable manifest lists ref any more: its files are
// removed once it is closed, now if it already is.
func (s *Shard) markObsolete(ref *segRef) {
	ref.obsolete.Store(true)
	if ref.closed.Load() {
		s.jan.removeLater(segmentFiles(s.dir, ref.id)...)
	}
}

// dataFiles lists ref's own files: its segment file, or its query segment's files;
// never its sidecars or a temp file.
func (s *Shard) dataFiles(ref *segRef) ([]string, error) {
	if ref.kind == kindDocs {
		return []string{filepath.Join(s.dir, ref.id+segment.FileExt)}, nil
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("shard: %w", err)
	}
	var out []string
	for _, e := range entries {
		name := e.Name()
		if !e.IsDir() && strings.HasPrefix(name, ref.id+".") && !strings.HasSuffix(name, deletesExt) && !strings.HasSuffix(name, ".tmp") {
			out = append(out, filepath.Join(s.dir, name))
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("shard: query segment %s has no files", ref.id)
	}
	return out, nil
}

// flushLoop flushes every Options.FlushInterval until the shard closes.
func (s *Shard) flushLoop() {
	defer s.wg.Done()
	t := s.opts.Clock.NewTicker(s.opts.FlushInterval)
	defer t.Stop()
	warn := rateLimitedWarn{clock: s.opts.Clock, every: time.Minute}
	for {
		select {
		case <-s.bg.Done():
			return
		case <-t.C():
		}
		if s.Err() != nil {
			continue
		}
		if err := s.Flush(s.bg); err != nil && s.bg.Err() == nil && !errors.Is(err, ErrClosed) {
			if suppressed, ok := warn.allow(); ok {
				s.log.WarnContext(s.bg, "background flush failed", slog.Any("error", err), slog.Int("suppressed", suppressed))
			}
		}
	}
}
