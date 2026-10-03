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
// directory, manifest and segments, of the index's current incarnation. The tailer
// opens it and replays the changelog from its seq. Any error, or a copy that cannot
// be used (another incarnation, or one the changelog has been pruned past), makes the
// tailer wipe dir again and rebuild from the store's ScanShard instead.
type Fetcher interface {
	Fetch(ctx context.Context, id ShardID, dir string) error
}

// Recovery sources, for metrics and logs.
const (
	sourceSQL    = "sql"
	sourcePeer   = "peer"
	sourceReopen = "reopen"
)

// ErrWipeNeeded is returned by [Recover] for a copy it cannot resume: one whose index
// was dropped and recreated since, or one a rebuild left half-loaded. Close it, remove
// its directory, open it again empty and recover that.
var ErrWipeNeeded = errors.New("replica: the shard copy must be wiped and rebuilt")

// Recover readies sh, a shard copy opened from its directory, to tail the changelog of
// shard id, and returns the seq to tail from. A copy that has applied changes resumes
// from its seq (CommittedSeq at Open): the changelog is replayed from there. An empty
// copy is loaded from st's ScanShard snapshot, and resumes from the snapshot's seq. A
// copy of another incarnation of the index, or one an earlier load left half done, is
// [ErrWipeNeeded]. A [Tailer] does all of this itself; Recover is for callers that
// manage the copy's directory on their own.
func Recover(ctx context.Context, sh *shard.Shard, st store.Store, id ShardID) (int64, error) {
	t := NewTailer(st, sh, id, Options{})
	if err := t.cat.load(ctx); err != nil {
		return 0, err
	}
	t.syncMapping(sh)
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
	asOf, err := t.loadSnapshot(ctx, sh)
	if err != nil {
		return 0, err
	}
	return asOf, nil
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
// empty one, one of another incarnation, and one a rebuild left half done.
func (t *Tailer) start(ctx context.Context) error {
	if err := t.cat.load(ctx); err != nil {
		return err
	}
	t.lastCatalog = time.Now()
	sh := t.Shard()
	t.syncMapping(sh)
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
		t.setState(StateRecovering)
		if err := t.rebuild(ctx, t.needRebuild); err != nil {
			var rb *rebuildError
			if errors.As(err, &rb) {
				return err // handle records the new reason
			}
			// Whatever the rebuild left behind is wiped by the next attempt.
			t.needRebuild = reasonInterrupted
			return err
		}
		t.needRebuild = ""
	}
	return nil
}

// refreshCatalog re-reads the catalogue entry: a new incarnation means a rebuild, a
// grown mapping goes to the shard.
func (t *Tailer) refreshCatalog(ctx context.Context) error {
	uid := t.cat.meta.UID
	if err := t.cat.load(ctx); err != nil {
		return err
	}
	t.lastCatalog = time.Now()
	if t.cat.meta.UID != uid {
		return &rebuildError{reason: reasonIncarnation, err: fmt.Errorf("index %q became incarnation %s", t.id.Index, t.cat.meta.UID)}
	}
	t.syncMapping(t.Shard())
	return nil
}

// openShard opens dir as the copy's shard.
func (t *Tailer) openShard(ctx context.Context, dir string) (*shard.Shard, error) {
	if t.opts.OpenShard != nil {
		return t.opts.OpenShard(ctx, dir)
	}
	return shard.Open(ctx, dir, t.cat.mapping, t.shardOpts)
}

// swap makes sh the copy's shard.
func (t *Tailer) swap(sh *shard.Shard) {
	t.syncMapping(sh)
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
	start := time.Now()
	defer func() { t.endRecovery(ctx, span, sourceReopen, start, err) }()
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
	if err := t.cat.load(ctx); err != nil {
		return err
	}
	t.lastCatalog = time.Now()
	sh := t.Shard()
	fresh := reason == reasonEmpty && sh.Err() == nil && sh.AppliedSeq() == 0 && !holdsData(sh)
	// A fetched copy the changelog was pruned past before it applied anything is
	// not fetched again: the peers are as far behind.
	useFetcher := t.opts.Fetcher != nil &&
		(reason != reasonPruned || t.lastSource != sourcePeer || t.Applied() != t.recoveredAt)
	if !fresh || useFetcher {
		dir := sh.Dir()
		sh.Abandon()
		if err := wipe(ctx, dir, t.log); err != nil {
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
	start := time.Now()
	defer func() { t.endRecovery(ctx, span, sourceSQL, start, err) }()
	asOf, err := t.loadSnapshot(ctx, sh)
	if err != nil {
		return err
	}
	t.lastSource, t.recoveredAt = sourceSQL, asOf
	t.log.InfoContext(ctx, "shard copy rebuilt from the store", slog.String("reason", reason), slog.Int64("seq", asOf),
		slog.Float64("duration_ms", float64(time.Since(start).Microseconds())/1000))
	return nil
}

// fetch tries the Fetcher into the wiped dir. ok reports a usable fetched copy, now
// the tailer's shard; otherwise dir is wiped again for the snapshot.
func (t *Tailer) fetch(ctx context.Context, reason, dir string) (ok bool, err error) {
	ctx, span := t.startRecoverySpan(ctx, sourcePeer, reason)
	start := time.Now()
	var used error
	defer func() { t.endRecovery(ctx, span, sourcePeer, start, used) }()
	if used = os.MkdirAll(dir, 0o750); used == nil { //nolint:gosec // dir is the copy's own directory, the node's choice
		used = t.opts.Fetcher.Fetch(ctx, t.id, dir)
	}
	if used == nil {
		var sh *shard.Shard
		if sh, used = t.openShard(ctx, dir); used == nil {
			uid, seq := sh.IndexUID(), sh.AppliedSeq()
			if seq > 0 && (uid == "" || uid == t.cat.meta.UID) {
				t.swap(sh)
				t.lastSource, t.recoveredAt = sourcePeer, seq
				t.log.InfoContext(ctx, "shard copy fetched", slog.String("reason", reason), slog.Int64("seq", seq))
				return true, nil
			}
			sh.Abandon()
			used = &rebuildError{reason: reasonFetchedStale, err: fmt.Errorf("the fetched copy is at seq %d of incarnation %q", seq, uid)}
		}
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	t.log.WarnContext(ctx, "fetching the shard copy failed; rebuilding it from the store", slog.Any("error", used))
	return false, wipe(ctx, dir, t.log)
}

// loadSnapshot loads the store's snapshot of the shard into sh, which has applied
// nothing, then advances it to the snapshot's seq. The scan holds one transaction
// open, so its callback only hands each record over; another goroutine analyzes and
// loads them in batches, waiting out backpressure.
func (t *Tailer) loadSnapshot(ctx context.Context, sh *shard.Shard) (int64, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	records := make(chan store.Record, t.opts.BatchSize)
	type result struct {
		bytes, count int64
		err          error
	}
	done := make(chan result, 1)
	go func() {
		var r result
		r.bytes, r.count, r.err = t.consumeSnapshot(ctx, sh, records)
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
	if scanErr != nil {
		return 0, scanErr
	}
	t.inst.recoveryBytes.Add(ctx, res.bytes, t.inst.with(attribute.String("source", sourceSQL)))
	if err := sh.Advance(asOf); err != nil {
		return 0, err
	}
	t.applied.Store(asOf)
	// Commit now, so a crash from here on resumes rather than reloads. A refresh
	// that fails keeps its buffer for the background refresh to retry.
	if err := sh.Refresh(ctx); err != nil {
		if errors.Is(err, shard.ErrFailed) || errors.Is(err, shard.ErrClosed) || ctx.Err() != nil {
			return 0, err
		}
		t.log.WarnContext(ctx, "refresh after the snapshot load failed; the background refresh retries it", slog.Any("error", err))
	}
	t.log.DebugContext(ctx, "snapshot loaded", slog.Int64("seq", asOf), slog.Int64("records", res.count), slog.Int64("bytes", res.bytes))
	return asOf, nil
}

// snapshotBatchBytes bounds the record bytes one snapshot batch holds.
const snapshotBatchBytes = 8 << 20

// consumeSnapshot loads the records the scan hands over, in batches.
func (t *Tailer) consumeSnapshot(ctx context.Context, sh *shard.Shard, records <-chan store.Record) (bytes, count int64, err error) {
	batch := make([]item, 0, t.opts.BatchSize)
	var batchBytes int
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		for i := range batch {
			if uid := batch[i].uid; uid != "" && uid != t.cat.meta.UID {
				return &rebuildError{reason: reasonIncarnation, err: fmt.Errorf("the snapshot is of incarnation %s, the copy follows %s", uid, t.cat.meta.UID)}
			}
		}
		converted, cerr := t.convert(ctx, batch)
		t.syncMapping(sh)
		if _, err := t.applyBatch(ctx, sh, converted, true); err != nil {
			return err
		}
		if cerr != nil {
			return cerr
		}
		t.inst.batchSize.Record(ctx, float64(len(batch)), t.inst.attrs)
		count += int64(len(batch))
		batch, batchBytes = batch[:0], 0
		return nil
	}
	for r := range records {
		batch = append(batch, itemFromRecord(&r))
		n := len(r.ID) + len(r.Body) + len(r.Meta)
		batchBytes += n
		bytes += int64(n)
		if len(batch) >= t.opts.BatchSize || batchBytes >= snapshotBatchBytes {
			if err := flush(); err != nil {
				return bytes, count, err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return bytes, count, err
	}
	return bytes, count, flush()
}

// wipe removes a copy's directory, discarding its manifest first so that a crash
// part way leaves a directory that opens as an empty copy. On Windows a file that is
// still mapped (a reader holding a generation of the abandoned shard) cannot be
// removed: it retries until the readers let go, or ctx ends.
func wipe(ctx context.Context, dir string, log *slog.Logger) error {
	delay := 10 * time.Millisecond
	for attempt := 1; ; attempt++ {
		err := shard.Discard(dir)
		if err == nil {
			err = os.RemoveAll(dir) //nolint:gosec // dir is the copy's own directory, the node's choice
		}
		if err == nil {
			return nil
		}
		if attempt%20 == 0 {
			log.WarnContext(ctx, "shard directory still in use; retrying its removal", slog.String("dir", dir),
				slog.Int("attempt", attempt), slog.Any("error", err))
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("wiping %s: %w (last error: %w)", dir, ctx.Err(), err)
		case <-timer.C:
		}
		delay = min(2*delay, time.Second)
	}
}

func (t *Tailer) startRecoverySpan(ctx context.Context, source, reason string) (context.Context, trace.Span) {
	return t.tr.Start(ctx, "replica.recover", trace.WithAttributes(
		attribute.String("index", t.id.Index), attribute.Int("shard", t.id.Shard),
		attribute.String("source", source), attribute.String("reason", reason)))
}

// endRecovery records a recovery's outcome and duration.
func (t *Tailer) endRecovery(ctx context.Context, span trace.Span, source string, start time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
		span.RecordError(err)
		span.SetStatus(codes.Error, "recovery failed")
	}
	span.End()
	t.inst.recoveries.Add(ctx, 1, t.inst.with(attribute.String("source", source), attribute.String("result", result)))
	t.inst.recoveryDur.Record(ctx, time.Since(start).Seconds(), t.inst.with(attribute.String("source", source)))
}
