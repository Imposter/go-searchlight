package replica

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// Fetcher brings a shard copy's files from somewhere other than SQL: a serving peer's
// segments over the internal API, or a bundle in the store's blobs (spec section 9;
// the cluster provides it). When a copy must be rebuilt, the tailer wipes its
// directory and calls Fetch, which fills dir (it exists and is empty) with a shard
// directory, segments and sidecars first and the manifest last, each fsynced (the
// manifest is the commit point: a Fetch cut short leaves a directory that opens empty),
// and reports where the copy came from: [SourcePeer] or [SourceBlob]. The copy must be
// of the index's current incarnation. The tailer opens it and replays the changelog
// from its seq. Any error, or a copy that cannot be used (another incarnation, an older
// mapping than the rebuild needs, or one the changelog has been pruned past before it
// moved), makes the tailer wipe dir again and rebuild from the store's ScanShard
// instead.
type Fetcher interface {
	Fetch(ctx context.Context, id ShardID, dir string) (source string, err error)
}

// ErrNoSource is what a Fetcher returns, wrapped, when neither a peer nor a bundle has
// a copy worth fetching (no peer serves the shard, or the one that does holds nothing
// yet, and no bundle the changelog still reaches exists): the tailer rebuilds from the
// store's snapshot, as it does after any fetch error, but this is no failure.
var ErrNoSource = errors.New("replica: no peer or bundle has a copy to fetch")

// Recovery sources, for metrics and logs. A [Fetcher] reports SourcePeer or SourceBlob.
const (
	// SourcePeer is a copy streamed from a serving peer.
	SourcePeer = "peer"
	// SourceBlob is a copy restored from a bundle in the store's blobs (sl_blobs).
	SourceBlob   = "blob"
	sourceSQL    = "sql"
	sourceReopen = "reopen"
	// sourceFetch labels a fetch before the Fetcher has said where its copy came from.
	sourceFetch = "fetch"
)

func fetchedFrom(source string) bool { return source == SourcePeer || source == SourceBlob }

// ErrWipeNeeded is returned by [Recover] for a copy it cannot resume: one whose index
// was dropped and recreated since, or one a rebuild left half-loaded. Close it, remove
// its directory, open it again empty and recover that.
var ErrWipeNeeded = errors.New("replica: the shard copy must be wiped and rebuilt")

// Recover readies sh, a shard copy opened from its directory, to tail the changelog of
// shard id, and returns the seq to tail from. A copy that has applied changes resumes
// from its seq (CommittedSeq at Open): the changelog is replayed from there. An empty
// copy is loaded from st's ScanShard snapshot, mapping included, and resumes from the
// snapshot's seq. A copy of another incarnation of the index, or one an earlier load
// left half done, is [ErrWipeNeeded]. A [Tailer] does all of this itself; Recover is
// for callers that manage the copy's directory on their own.
func Recover(ctx context.Context, sh *shard.Shard, st store.Store, id ShardID) (int64, error) {
	t := NewTailer(st, sh, id, Options{})
	if err := t.cat.load(ctx, id.Shard); err != nil {
		return 0, err
	}
	if err := sh.Err(); err != nil {
		return 0, err
	}
	if uid := sh.IndexUID(); uid != "" && uid != t.cat.meta.UID {
		return 0, fmt.Errorf("%w: the copy holds incarnation %s of index %q, which is now %s", ErrWipeNeeded, uid, id.Index, t.cat.meta.UID)
	}
	if seq := sh.AppliedSeq(); seq > 0 {
		return seq, nil
	}
	if holdsData(sh) {
		return 0, fmt.Errorf("%w: an earlier load did not finish", ErrWipeNeeded)
	}
	return t.loadSnapshot(ctx, sh)
}

// holdsData reports whether sh's current generation has any segment.
func holdsData(sh *shard.Shard) bool {
	g := sh.Acquire()
	if g == nil {
		return false
	}
	defer g.Release()
	return len(g.Segments)+len(g.QuerySegments) > 0
}

// start checks the copy Run was given against the catalogue and decides how it
// recovers: it resumes a copy that has applied changes, and asks for a rebuild of an
// empty one, one of another incarnation, and one a rebuild left half done. A failure to
// read the catalogue decides nothing: Run retries the start.
func (t *Tailer) start(ctx context.Context) error {
	if err := t.cat.load(ctx, t.id.Shard); err != nil {
		return err
	}
	t.lastCatalog = t.opts.Clock.Now()
	sh := t.Shard()
	if err := sh.Err(); err != nil {
		return err
	}
	if uid := sh.IndexUID(); uid != "" && uid != t.cat.meta.UID {
		return &rebuildError{reason: reasonIncarnation, err: fmt.Errorf("the copy holds incarnation %s, the index is %s", uid, t.cat.meta.UID)}
	}
	if seq := sh.AppliedSeq(); seq > 0 {
		t.applied.Store(seq)
		return nil
	}
	if holdsData(sh) {
		return &rebuildError{reason: reasonInterrupted, err: errors.New("the copy is committed at seq 0 but holds segments")}
	}
	return &rebuildError{reason: reasonEmpty, err: errors.New("the copy is empty")}
}

// recoverIfNeeded runs a pending reopen or rebuild.
func (t *Tailer) recoverIfNeeded(ctx context.Context) error {
	if t.needReopen {
		t.setState(StateRecovering)
		t.needReopen = false
		if err := t.reopen(ctx); err != nil {
			return err
		}
	}
	if t.needRebuild != "" {
		if g := t.gate; g != nil {
			// A load stopped at a row the copy cannot apply: a rebuild would stop
			// there again until the row is superseded.
			superseded, err := t.superseded(ctx, g)
			if err != nil {
				return err
			}
			if !superseded {
				return g
			}
		}
		// A copy that is valid but outdated keeps serving (stale) while its
		// replacement is built aside; any other is rebuilt in place, unserved.
		aside := t.canBuildAside(t.needRebuild)
		if !t.halted() {
			if aside {
				t.setState(StateRebuilding)
			} else {
				t.setState(StateRecovering)
			}
		}
		if t.needRebuild == reasonRemap {
			if err := t.debounceRemap(ctx); err != nil {
				return err
			}
		}
		rebuild := t.rebuild
		if aside {
			rebuild = t.rebuildAside
		}
		if err := rebuild(ctx, t.needRebuild); err != nil {
			var rb *rebuildError
			var halt *HaltError
			switch {
			case errors.As(err, &rb):
				return err // handle records the new reason
			case errors.As(err, &halt):
				t.gate = halt
			}
			if !aside {
				// Whatever the rebuild left behind is wiped by the next attempt.
				t.needRebuild = reasonInterrupted
			}
			return err
		}
		t.needRebuild, t.gate, t.remapSince = "", nil, time.Time{}
		t.rebuildWait = 0
		t.clearHalt()
	}
	return nil
}

// asideReasons are the rebuilds of a copy that is valid, only outdated: it can keep
// serving while its replacement is built.
var asideReasons = map[string]bool{
	reasonRemap: true, reasonMappingOrder: true, reasonMappingBehind: true, reasonPruned: true, reasonHalted: true,
}

// canBuildAside reports whether the rebuild for reason can be built aside: the copy
// is outdated rather than invalid, and its shard works and holds data. The replacement
// is fetched from a peer (the Fetcher) or loaded from the store's snapshot into a new
// directory under the copy's root while the current copy keeps serving.
func (t *Tailer) canBuildAside(reason string) bool {
	sh := t.Shard()
	if t.opts.hooks != nil && t.opts.hooks.inPlace {
		return false
	}
	return asideReasons[reason] && sh.Err() == nil && sh.AppliedSeq() > 0
}

// useFetcher reports whether a rebuild should try the Fetcher first. A fetched copy
// that made no progress before it needed rebuilding again (it was pruned past, or
// halted at the same change) is not fetched again: the peers and bundles are no better
// off.
func (t *Tailer) useFetcher() bool {
	return t.opts.Fetcher != nil && (!fetchedFrom(t.lastSource) || t.Applied() != t.recoveredAt)
}

// rebuildAside rebuilds the copy in a new directory under its root while the current
// copy keeps serving: it fetches a peer's copy there (the Fetcher) or loads the store's
// snapshot, applies the changelog after it until caught up, refreshes and flushes, then
// makes it current (CURRENT, atomically) and swaps it in. It is durable before it is
// current: the copy it replaces may have reported a seq up to the head, and the
// registry must never hold a seq the current copy could lose. Readers holding a
// generation of the old copy keep it until they release it; the old copy's files are
// removed once they have (or by the next OpenCopy). A failure, or a crash, leaves the
// old copy current and serving, and the half-built directory is removed (now, or by
// the next OpenCopy).
func (t *Tailer) rebuildAside(ctx context.Context, reason string) (err error) {
	if err := t.cat.load(ctx, t.id.Shard); err != nil {
		return err
	}
	t.lastCatalog = t.opts.Clock.Now()
	old := t.Shard()
	root := copyRoot(old.Dir())
	start := t.opts.Clock.Now()
	var sh *shard.Shard
	var dir string
	swapped := false
	defer func() {
		if !swapped && sh != nil {
			sh.Abandon()
			t.applied.Store(old.AppliedSeq())
			if ctx.Err() == nil {
				_ = os.RemoveAll(dir)
			} // stopping: left, like a crash's, for the next OpenCopy to remove
		}
	}()
	source := sourceSQL
	var asOf int64
	if t.useFetcher() {
		dir = newCopyDir(root, t.opts.Clock.Now())
		t.log.InfoContext(ctx, "fetching a replacement copy aside; the current copy serves meanwhile", slog.String("reason", reason), slog.String("dir", dir))
		fetched, ok, err := t.fetchInto(ctx, reason, dir)
		if err != nil {
			return err
		}
		if ok {
			sh, source, asOf = fetched, t.lastSource, fetched.AppliedSeq()
		}
	}
	if sh == nil {
		dir = newCopyDir(root, t.opts.Clock.Now())
		ctx, span := t.startRecoverySpan(ctx, sourceSQL, reason)
		defer func() { t.endRecovery(ctx, span, sourceSQL, reason, start, err) }()
		if sh, err = t.openShard(ctx, dir); err != nil {
			return err
		}
		t.log.InfoContext(ctx, "rebuilding the shard copy aside; the current copy serves meanwhile", slog.String("reason", reason), slog.String("dir", dir))
		if asOf, err = t.loadSnapshot(ctx, sh); err != nil {
			return err
		}
	}
	for {
		head, _, err := t.st.HeadSeq(ctx)
		if err != nil {
			return err
		}
		changes, err := t.st.ChangesAfter(ctx, t.id, sh.AppliedSeq(), t.opts.BatchSize)
		if errors.Is(err, store.ErrPruned) {
			if fetchedFrom(source) {
				t.lastSource, t.recoveredAt = source, asOf // fetch no more: the peers and bundles are no better off
			}
			return &rebuildError{reason: reasonPruned, err: err}
		}
		if err != nil {
			return err
		}
		if len(changes) > 0 {
			if err := t.applyChanges(ctx, sh, changes); err != nil {
				return err
			}
		}
		if len(changes) < t.opts.BatchSize {
			if head > sh.AppliedSeq() {
				if err := sh.Advance(head); err != nil {
					return err
				}
			}
			break
		}
	}
	if err := sh.Refresh(ctx); err != nil {
		return err
	}
	if err := sh.Flush(ctx); err != nil {
		return err
	}
	if t.opts.hooks != nil && t.opts.hooks.beforeSwap != nil {
		if err := t.opts.hooks.beforeSwap(ctx, dir); err != nil {
			return err
		}
	}
	if err := makeCurrent(root, dir); err != nil {
		return fmt.Errorf("replica: making %s current: %w", dir, err)
	}
	swapped = true
	t.swap(sh)
	old.Abandon()
	removeCopy(root, old.Dir())
	t.lastSource, t.recoveredAt, t.minMappingVersion = source, asOf, 0
	t.log.InfoContext(ctx, "shard copy rebuilt aside and swapped in", slog.String("reason", reason), slog.String("source", source),
		slog.Int64("seq", sh.AppliedSeq()), slog.Float64("duration_ms", float64(t.opts.Clock.Since(start).Microseconds())/1000))
	return nil
}

// debounceRemap holds a rebuild a mapping change asked for until mapping changes stop
// arriving for RemapDebounce (at most five such windows), so that a burst of them
// costs one rebuild, at the latest seq. Nothing is applied meanwhile: the copy never
// analyzes a document under a mapping it knows to be stale.
func (t *Tailer) debounceRemap(ctx context.Context) error {
	if t.opts.RemapDebounce < 0 {
		return nil
	}
	if t.remapSince.IsZero() {
		if err := t.cat.load(ctx, t.id.Shard); err != nil {
			return err
		}
		t.remapSince, t.remapVersion = t.opts.Clock.Now(), t.cat.meta.MappingVersion
		t.log.InfoContext(ctx, "a mapping change re-analyzes the copy; waiting for more before rebuilding it",
			slog.Duration("debounce", t.opts.RemapDebounce))
	}
	limit := t.remapSince.Add(5 * t.opts.RemapDebounce)
	for {
		t.pause(ctx, min(t.opts.RemapDebounce, max(0, t.opts.Clock.Until(limit))))
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := t.cat.load(ctx, t.id.Shard); err != nil {
			return err
		}
		v := t.cat.meta.MappingVersion
		if v <= t.remapVersion || !t.opts.Clock.Now().Before(limit) {
			return nil
		}
		t.remapVersion = v // another mapping change: wait for the burst to end
	}
}

// refreshCatalog re-reads the catalogue entry: a dropped index (or shard) stops the
// copy, a new incarnation rebuilds it.
func (t *Tailer) refreshCatalog(ctx context.Context) error {
	if err := t.cat.load(ctx, t.id.Shard); err != nil {
		return err
	}
	t.lastCatalog = t.opts.Clock.Now()
	if uid := t.Shard().IndexUID(); uid != "" && uid != t.cat.meta.UID {
		return &rebuildError{reason: reasonIncarnation, err: fmt.Errorf("index %q became incarnation %s", t.id.Index, t.cat.meta.UID)}
	}
	return nil
}

// openShard opens dir as the copy's shard. Its mapping comes from its manifest, or,
// for an empty directory, from the snapshot the rebuild loads.
func (t *Tailer) openShard(ctx context.Context, dir string) (*shard.Shard, error) {
	if t.opts.OpenShard != nil {
		return t.opts.OpenShard(ctx, dir)
	}
	return shard.Open(ctx, dir, nil, t.shardOpts)
}

// swap makes sh the copy's shard.
func (t *Tailer) swap(sh *shard.Shard) {
	t.sh.Store(sh)
	t.applied.Store(sh.AppliedSeq())
	if t.opts.OnShard != nil {
		t.opts.OnShard(sh)
	}
}

// reopen replaces a failed shard with its directory opened again; a directory that
// does not open is rebuilt.
func (t *Tailer) reopen(ctx context.Context) (err error) {
	ctx, span := t.startRecoverySpan(ctx, sourceReopen, "failed")
	start := t.opts.Clock.Now()
	defer func() { t.endRecovery(ctx, span, sourceReopen, "failed", start, err) }()
	old := t.Shard()
	dir := old.Dir()
	old.Abandon()
	sh, err := t.openShard(ctx, dir)
	if err != nil {
		return &rebuildError{reason: reasonUnopenable, err: err}
	}
	t.swap(sh)
	t.log.InfoContext(ctx, "shard copy reopened", slog.Int64("seq", sh.AppliedSeq()))
	return t.start(ctx)
}

// rebuild wipes the copy and rebuilds it: from the Fetcher when there is one, else
// (or when that fails) from the store's snapshot. A new copy with nothing to wipe is
// loaded in place.
func (t *Tailer) rebuild(ctx context.Context, reason string) (err error) {
	if err := t.setCopyState(ctx, store.CopyRecovering); err != nil {
		return err
	}
	if err := t.cat.load(ctx, t.id.Shard); err != nil {
		return err
	}
	t.lastCatalog = t.opts.Clock.Now()
	sh := t.Shard()
	fresh := reason == reasonEmpty && sh.Err() == nil && sh.AppliedSeq() == 0 && !holdsData(sh)
	useFetcher := t.useFetcher()
	if !fresh || useFetcher {
		dir := sh.Dir()
		sh.Abandon()
		if err := t.wipe(ctx, dir); err != nil {
			return err
		}
		if useFetcher {
			ok, err := t.fetch(ctx, reason, dir)
			if ok || err != nil {
				return err
			}
		}
		if sh, err = t.openShard(ctx, dir); err != nil {
			return err
		}
		t.swap(sh)
	}

	ctx, span := t.startRecoverySpan(ctx, sourceSQL, reason)
	start := t.opts.Clock.Now()
	defer func() { t.endRecovery(ctx, span, sourceSQL, reason, start, err) }()
	asOf, err := t.loadSnapshot(ctx, sh)
	if err != nil {
		return err
	}
	t.lastSource, t.recoveredAt, t.minMappingVersion = sourceSQL, asOf, 0
	t.log.InfoContext(ctx, "shard copy rebuilt from the store", slog.String("reason", reason), slog.Int64("seq", asOf),
		slog.Float64("duration_ms", float64(t.opts.Clock.Since(start).Microseconds())/1000))
	return nil
}

// fetch tries the Fetcher into the wiped dir. ok reports a usable fetched copy, now
// the tailer's shard; otherwise dir is wiped again for the snapshot.
func (t *Tailer) fetch(ctx context.Context, reason, dir string) (ok bool, err error) {
	sh, ok, err := t.fetchInto(ctx, reason, dir)
	if ok {
		t.swap(sh)
	}
	return ok, err
}

// fetchInto runs the Fetcher into dir (created when missing) and opens what it
// fetched. ok reports a usable copy: of the index's current incarnation, at a seq past
// 0, with the mapping version the rebuild needs; it is returned open, not yet the
// tailer's. Otherwise dir is wiped, and the error is nil unless ctx ended.
func (t *Tailer) fetchInto(ctx context.Context, reason, dir string) (sh *shard.Shard, ok bool, err error) {
	ctx, span := t.startRecoverySpan(ctx, sourceFetch, reason)
	start := t.opts.Clock.Now()
	source := sourceFetch
	var used error
	defer func() {
		span.SetAttributes(attribute.String("source", source))
		t.endRecovery(ctx, span, source, reason, start, used)
	}()
	if used = os.MkdirAll(dir, 0o750); used == nil {
		var from string
		from, used = t.opts.Fetcher.Fetch(ctx, t.id, dir)
		if used == nil {
			source = from
		}
	}
	if used == nil {
		if sh, used = t.openShard(ctx, dir); used == nil {
			uid, seq, mv := sh.IndexUID(), sh.AppliedSeq(), sh.MappingVersion()
			if seq > 0 && uid == t.cat.meta.UID && mv >= t.minMappingVersion {
				t.lastSource, t.recoveredAt, t.minMappingVersion = source, seq, 0
				t.log.InfoContext(ctx, "shard copy fetched", slog.String("reason", reason), slog.String("source", source), slog.Int64("seq", seq))
				return sh, true, nil
			}
			sh.Abandon()
			used = &rebuildError{reason: reasonFetchedStale, err: fmt.Errorf("the fetched copy is at seq %d of incarnation %q, mapping version %d", seq, uid, mv)}
		}
	}
	if ctx.Err() != nil {
		return nil, false, ctx.Err()
	}
	if errors.Is(used, ErrNoSource) {
		t.log.InfoContext(ctx, "no peer or bundle has a copy to fetch; rebuilding it from the store", slog.Any("reason", used))
	} else {
		t.log.WarnContext(ctx, "fetching the shard copy failed; rebuilding it from the store", slog.Any("error", used))
	}
	return nil, false, t.wipe(ctx, dir)
}

// loadSnapshot loads the store's snapshot of the shard into sh, which has applied
// nothing, then advances it to the snapshot's seq. The snapshot's first record is the
// index's mapping as of the snapshot, which every document after it is analyzed under.
// The scan holds one transaction open, so its callback only hands each record over;
// another goroutine analyzes and loads them in batches, waiting out backpressure.
func (t *Tailer) loadSnapshot(ctx context.Context, sh *shard.Shard) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	t.inst.progress.Record(ctx, 0, t.inst.with(attribute.String("source", sourceSQL)))
	records := make(chan store.Record, t.opts.BatchSize)
	type result struct {
		bytes, count int64
		mapped       bool
		err          error
	}
	done := make(chan result, 1)
	go func() {
		var r result
		r.bytes, r.count, r.mapped, r.err = t.consumeSnapshot(ctx, sh, records)
		if r.err != nil {
			cancel() // stop the scan
			for range records {
				// drain whatever the scan already handed over
			}
		}
		done <- r
	}()
	asOf, scanErr := t.st.ScanShard(ctx, t.id, func(r store.Record) error {
		select {
		case records <- r:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	close(records)
	res := <-done
	if res.err != nil {
		return 0, res.err
	}
	var ce *store.CorruptError
	if errors.As(scanErr, &ce) {
		return 0, corruptHalt(t.id, ce)
	}
	if scanErr != nil {
		return 0, scanErr
	}
	if !res.mapped {
		return 0, fmt.Errorf("%w: %s is missing from the snapshot", ErrIndexDropped, t.id.Index)
	}
	t.inst.recoveryBytes.Add(ctx, res.bytes, t.inst.with(attribute.String("source", sourceSQL)))
	if err := sh.Advance(asOf); err != nil {
		return 0, err
	}
	t.applied.Store(asOf)
	// Flush now, so a crash from here on resumes rather than reloads. A refresh or
	// flush that fails is retried in the background (the refresh keeps its buffer).
	err := sh.Refresh(ctx)
	if err == nil {
		err = sh.Flush(ctx)
	}
	if err != nil {
		if errors.Is(err, shard.ErrFailed) || errors.Is(err, shard.ErrClosed) || ctx.Err() != nil {
			return 0, err
		}
		t.log.WarnContext(ctx, "persisting the snapshot load failed; the background refresh and flush retry it", slog.Any("error", err))
	}
	t.inst.progress.Record(ctx, 1, t.inst.with(attribute.String("source", sourceSQL)))
	t.log.DebugContext(ctx, "snapshot loaded", slog.Int64("seq", asOf), slog.Int64("records", res.count), slog.Int64("bytes", res.bytes))
	return asOf, nil
}

// snapshotBatchBytes bounds the record bytes one snapshot batch holds.
const snapshotBatchBytes = 8 << 20

// consumeSnapshot loads the records the scan hands over, in batches. mapped reports
// that the snapshot carried the index's mapping (it does whenever the index exists).
func (t *Tailer) consumeSnapshot(ctx context.Context, sh *shard.Shard, records <-chan store.Record) (bytes, count int64, mapped bool, err error) {
	batch := make([]item, 0, t.opts.BatchSize)
	var batchBytes int
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := t.applyItems(ctx, sh, batch, true); err != nil {
			return err
		}
		t.inst.batchSize.Record(ctx, float64(len(batch)), t.inst.attrs)
		count += int64(len(batch))
		batch, batchBytes = batch[:0], 0
		return nil
	}
	for r := range records {
		if r.Kind == store.RecordMapping {
			mapped = true
		}
		batch = append(batch, itemFromRecord(&r))
		n := len(r.ID) + len(r.Body) + len(r.Meta)
		batchBytes += n
		bytes += int64(n)
		if len(batch) >= t.opts.BatchSize || batchBytes >= snapshotBatchBytes {
			if err := flush(); err != nil {
				return bytes, count, mapped, err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return bytes, count, mapped, err
	}
	return bytes, count, mapped, flush()
}

// wipe removes a copy's directory, discarding its manifest first so that a crash
// part way leaves a directory that opens as an empty copy. On Windows a file that is
// still mapped (a reader holding a generation of the abandoned shard) cannot be
// removed: it retries until the readers let go, or ctx ends.
func (t *Tailer) wipe(ctx context.Context, dir string) error {
	delay := 10 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := shard.Discard(dir)
		if err == nil && t.opts.hooks != nil && t.opts.hooks.afterDiscard != nil {
			t.opts.hooks.afterDiscard(ctx, dir)
			err = ctx.Err()
		}
		if err == nil {
			err = os.RemoveAll(dir)
		}
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("wiping %s: %w (last error: %w)", dir, ctx.Err(), err)
		}
		if attempt%20 == 0 {
			t.log.WarnContext(ctx, "shard directory still in use; retrying its removal", slog.String("dir", dir),
				slog.Int("attempt", attempt), slog.Any("error", err))
		}
		t.pause(ctx, delay)
		delay = min(2*delay, time.Second)
	}
}

func (t *Tailer) startRecoverySpan(ctx context.Context, source, reason string) (context.Context, trace.Span) {
	return t.tr.Start(ctx, "replica.recover", trace.WithAttributes(
		attribute.String("index", t.id.Index), attribute.Int("shard", t.id.Shard),
		attribute.String("source", source), attribute.String("reason", reason)))
}

// endRecovery records a recovery's outcome and duration.
func (t *Tailer) endRecovery(ctx context.Context, span trace.Span, source, reason string, start time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, "recovery failed")
	}
	span.End()
	t.inst.recoveries.Add(ctx, 1, t.inst.with(attribute.String("source", source), attribute.String("reason", reason),
		attribute.String("result", result)))
	t.inst.recoveryDur.Record(ctx, t.opts.Clock.Since(start).Seconds(), t.inst.with(attribute.String("source", source)))
}
