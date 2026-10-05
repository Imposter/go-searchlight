package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
	"github.com/Imposter/go-searchlight/internal/telemetry"
)

// The methods here are the cluster's handles on the engine (package cluster): which
// copies this node hosts, what they hold, and the catalogue kept in step with the
// store.

// HostCopy opens this node's copy of spec.Copy's shard (its directory, as left by an
// earlier run, or empty) and starts its tailer, which recovers it (from a peer through
// spec.Fetcher, or from the store) and reports to the registry under the copy's epoch.
// A copy the node already hosts is left as it is: one tailer per copy directory.
func (n *Single) HostCopy(ctx context.Context, spec HostSpec) error {
	id := spec.Copy.Shard
	idx, err := n.lookup(ctx, id.Index)
	if err != nil {
		return err
	}
	if id.Shard < 0 || id.Shard >= len(idx.shards) {
		return fmt.Errorf("node: index %q has no shard %d", id.Index, id.Shard)
	}
	return n.hostCopy(ctx, idx, id.Shard, spec)
}

// Hosted reports the registry copy this node hosts for id, if any.
func (n *Single) Hosted(id store.ShardID) (store.Copy, bool) {
	n.mu.RLock()
	idx := n.indexes[id.Index]
	n.mu.RUnlock()
	if idx == nil || id.Shard < 0 || id.Shard >= len(idx.shards) {
		return store.Copy{}, false
	}
	c := idx.shards[id.Shard].local.Load()
	if c == nil || c.copy == nil {
		return store.Copy{}, false
	}
	return *c.copy, true
}

// UnhostCopy stops this node's copy cp (its shard, at its epoch): it stops serving at
// once, then its tailer stops and its shard closes (a final commit), and with wipe
// its directory is removed (in the background while readers still map its segments).
// It is a no-op when the node hosts no copy of the shard, or one of another epoch (a
// later claim, or another incarnation of the index).
func (n *Single) UnhostCopy(ctx context.Context, cp store.Copy, wipe bool) error {
	mode := unhostClose
	if wipe {
		mode = unhostWipe
	}
	return n.unhost(ctx, cp, mode)
}

// PauseCopy stops the tailer of this node's copy cp (at its epoch), whose lease lapsed
// by the local clock but is not known lost: it writes nothing more and serves no peer,
// while its shard stays open, serving this node's own last-resort reads, stale.
// ResumeCopy (the same epoch re-claimed) starts it again; UnhostCopy closes it.
func (n *Single) PauseCopy(ctx context.Context, cp store.Copy) error {
	c, idx, unlock := n.lockCopy(cp)
	if c == nil {
		return nil
	}
	defer unlock()
	if c.paused.Load() {
		return nil
	}
	c.cancel()
	<-c.done
	c.paused.Store(true)
	n.log.WarnContext(ctx, "shard copy paused: its lease lapsed", slog.String(telemetry.KeyIndex, idx.name), slog.Int(telemetry.KeyShard, cp.Shard.Shard))
	return nil
}

// ResumeCopy starts a paused copy again under cp (its lease re-claimed at the same
// epoch): a new tailer over the open shard catches it up, reporting under cp.
func (n *Single) ResumeCopy(ctx context.Context, cp store.Copy) error {
	c, idx, unlock := n.lockCopy(cp)
	if c == nil {
		return fmt.Errorf("node: no copy of %s at epoch %d to resume", cp.Shard, cp.Epoch)
	}
	defer unlock()
	if !c.paused.Load() {
		return nil
	}
	sh := c.shard()
	if sh == nil || sh.Err() != nil {
		return fmt.Errorf("node: the paused copy of %s cannot resume", cp.Shard)
	}
	copyCopy := cp
	next := &copyState{
		id:      c.id,
		slot:    c.slot,
		tailer:  n.opts.NewTailer(n.st, sh, c.id, TailerEnv{Head: n.head.Load, Fetcher: c.spec.Fetcher, Copy: &copyCopy}),
		startup: c.startup,
		spec:    c.spec,
		copy:    &copyCopy,
		perc:    c.perc,
		warm:    sh,
	}
	next.started.Store(c.started.Load())
	next.spec.Copy = cp
	n.startTailer(idx, next) //nolint:contextcheck // the tailer outlives the call that resumes the copy
	n.log.InfoContext(ctx, "shard copy resumed: its lease was claimed again", slog.String(telemetry.KeyIndex, idx.name), slog.Int(telemetry.KeyShard, cp.Shard.Shard))
	return nil
}

// Paused reports whether this node's copy of id is paused (its lease lapsed).
func (n *Single) Paused(id store.ShardID) bool {
	n.mu.RLock()
	idx := n.indexes[id.Index]
	n.mu.RUnlock()
	if idx == nil || id.Shard < 0 || id.Shard >= len(idx.shards) {
		return false
	}
	c := idx.shards[id.Shard].local.Load()
	return c != nil && c.paused.Load()
}

// lockCopy finds this node's copy of cp's shard at cp's epoch and takes its slot's
// host lock: unlock releases it. The copy is nil (and nothing locked) when the node
// hosts none at that epoch.
func (n *Single) lockCopy(cp store.Copy) (*copyState, *index, func()) {
	n.mu.RLock()
	idx := n.indexes[cp.Shard.Index]
	n.mu.RUnlock()
	if idx == nil || cp.Shard.Shard < 0 || cp.Shard.Shard >= len(idx.shards) {
		return nil, nil, nil
	}
	sl := idx.shards[cp.Shard.Shard]
	sl.host.Lock()
	c := sl.local.Load()
	if c == nil || (c.copy != nil && c.copy.Epoch != cp.Epoch) {
		sl.host.Unlock()
		return nil, nil, nil
	}
	return c, idx, sl.host.Unlock
}

// AbandonCopy stops this node's copy cp as a crash would: no final commit, the
// directory kept as it is (tests of node loss).
func (n *Single) AbandonCopy(ctx context.Context, cp store.Copy) error {
	return n.unhost(ctx, cp, unhostAbandon)
}

type unhostMode int

const (
	unhostClose unhostMode = iota
	unhostWipe
	unhostAbandon
)

func (n *Single) unhost(ctx context.Context, cp store.Copy, mode unhostMode) error {
	id := cp.Shard
	n.mu.RLock()
	idx := n.indexes[id.Index]
	n.mu.RUnlock()
	if idx == nil || id.Shard < 0 || id.Shard >= len(idx.shards) {
		return nil
	}
	sl := idx.shards[id.Shard]
	sl.host.Lock()
	defer sl.host.Unlock()
	c := sl.local.Load()
	if c == nil || (c.copy != nil && c.copy.Epoch != cp.Epoch) {
		return nil
	}
	sl.local.Store(nil)
	sl.unhostedAtNano.Store(n.clock.Now().UnixNano())
	c.cancel()
	<-c.done
	var err error
	if sh := c.shard(); sh != nil {
		if mode == unhostClose {
			err = sh.Close(ctx)
		} else {
			sh.Abandon()
		}
	}
	if mode == unhostWipe {
		n.removeLater(ctx, idx.copyRoot(id.Shard))
	}
	n.log.InfoContext(ctx, "shard copy unhosted", slog.String(telemetry.KeyIndex, id.Index), slog.Int(telemetry.KeyShard, id.Shard), slog.Int("mode", int(mode)))
	return err
}

// removeLater removes dir, retrying in the background while it is in use (Windows
// refuses to remove a file a reader still maps).
func (n *Single) removeLater(ctx context.Context, dir string) {
	if os.RemoveAll(dir) == nil {
		return
	}
	go func() {
		delay := 100 * time.Millisecond
		for range 30 {
			time.Sleep(delay) //nolint:forbidigo // waits out other processes' handles on the files: real time is the subject
			if os.RemoveAll(dir) == nil {
				return
			}
			delay = min(2*delay, 10*time.Second)
		}
		n.log.WarnContext(ctx, "could not remove an unhosted copy's directory; the next start removes it", slog.String("dir", dir))
	}()
}

// LocalCopy describes one copy this node hosts.
type LocalCopy struct {
	Info api.ShardInfo
	// Copy is its registry copy.
	Copy store.Copy
	// Serving reports that it serves reads now, to peers too (it may still be
	// stale): it serves, and its lease surely holds.
	Serving bool
	// Paused reports that its lease lapsed and its tailer is stopped: it serves only
	// this node's own last-resort reads.
	Paused bool
	// StartedUp reports that it has finished its startup recovery.
	StartedUp bool
	// Startup marks a copy the node took as it started.
	Startup bool
}

// LocalCopies describes the copies this node hosts, by index and shard.
func (n *Single) LocalCopies() []LocalCopy {
	var out []LocalCopy
	for _, idx := range n.sortedIndexes() {
		for _, c := range idx.copies() {
			lc := LocalCopy{Info: n.copyInfo(c), Serving: c.peerServing() == nil, Paused: c.paused.Load(), StartedUp: c.startedUp(), Startup: c.startup}
			if c.copy != nil {
				lc.Copy = *c.copy
			}
			out = append(out, lc)
		}
	}
	return out
}

// SegmentMajors returns the oldest and newest segment format majors of this node's
// serving copy of id ([shard.Generation.FormatMajors]): what a peer's recovery would
// get from a snapshot, without taking one.
func (n *Single) SegmentMajors(ctx context.Context, id store.ShardID) (oldest, newest int, err error) {
	idx, err := n.lookup(ctx, id.Index)
	if err != nil {
		return 0, 0, err
	}
	if id.Shard < 0 || id.Shard >= len(idx.shards) {
		return 0, 0, api.NotFound(api.CodeNotFound, "index %q has no shard %d", id.Index, id.Shard)
	}
	c := idx.shards[id.Shard].local.Load()
	if c == nil {
		return 0, 0, api.Unavailable(shard.ErrClosed, "this node holds no copy of %s", id)
	}
	if err := c.peerServing(); err != nil {
		return 0, 0, err
	}
	g := c.shard().Acquire()
	if g == nil {
		return 0, 0, api.Unavailable(shard.ErrClosed, "%s is being swapped; retry", id)
	}
	defer g.Release()
	oldest, newest = g.FormatMajors()
	return oldest, newest, nil
}

// Snapshot takes a snapshot of this node's serving copy of id, for a peer's recovery.
// Release it when done.
func (n *Single) Snapshot(ctx context.Context, id store.ShardID) (*shard.Snapshot, error) {
	idx, err := n.lookup(ctx, id.Index)
	if err != nil {
		return nil, err
	}
	if id.Shard < 0 || id.Shard >= len(idx.shards) {
		return nil, api.NotFound(api.CodeNotFound, "index %q has no shard %d", id.Index, id.Shard)
	}
	c := idx.shards[id.Shard].local.Load()
	if c == nil {
		return nil, api.Unavailable(shard.ErrClosed, "this node holds no copy of %s", id)
	}
	for range acquireTries {
		if err := c.peerServing(); err != nil {
			return nil, err
		}
		sh := c.shard()
		sn, err := sh.Snapshot(ctx)
		if errors.Is(err, shard.ErrClosed) && c.shard() != sh {
			continue // swapped by a rebuild: snapshot its replacement
		}
		if err != nil {
			return nil, api.Unavailable(err, "%s cannot be copied now", id)
		}
		if c.peerServing() == nil && c.shard() == sh {
			return sn, nil
		}
		sn.Release()
	}
	return nil, api.Unavailable(shard.ErrClosed, "%s is being swapped; retry", id)
}

// Wake asks this node's copy of id to read the changelog now, when it has not applied
// seq yet: the push hint a peer that committed a write sends.
func (n *Single) Wake(id store.ShardID, seq int64) {
	n.mu.RLock()
	idx := n.indexes[id.Index]
	n.mu.RUnlock()
	if idx == nil || id.Shard < 0 || id.Shard >= len(idx.shards) {
		return
	}
	if c := idx.shards[id.Shard].local.Load(); c != nil && c.tailer.Applied() < seq {
		c.tailer.Wake()
	}
}

// Counts returns the live documents and saved queries of this node's serving copy of
// id.
func (n *Single) Counts(ctx context.Context, id store.ShardID) (docs, queries uint64, err error) {
	t, err := n.LocalTarget(ctx, id.Index, id.Shard, 0)
	if err != nil {
		return 0, 0, err
	}
	defer t.Release()
	lt, ok := t.(*localTarget)
	if !ok {
		return 0, 0, errors.New("node: not a local target")
	}
	return lt.g.NumDocs(), lt.g.NumQueries(), nil
}

// Head returns the newest seq this node knows committed.
func (n *Single) Head() int64 { return n.head.Load() }

// NoteDB records whether the database just answered a call the cluster made: reads are
// marked stale while it does not (spec section 10).
func (n *Single) NoteDB(err error) { n.noteDB(err) }

// DBStale reports whether the database cannot be reached now.
func (n *Single) DBStale() bool { return n.stale() }

// DBAnsweredWithin reports whether the database answered within d.
func (n *Single) DBAnsweredWithin(d time.Duration) bool {
	return n.clock.Since(time.Unix(0, n.dbOK.Load())) <= d
}

// Indexes describes the indexes this node knows, by name.
func (n *Single) Indexes() []IndexView {
	list := n.sortedIndexes()
	out := make([]IndexView, 0, len(list))
	for _, idx := range list {
		st := idx.meta.Load()
		out = append(out, IndexView{Name: idx.name, UID: st.meta.UID, Shards: len(idx.shards), ReplicasPerShard: st.settings.ReplicasPerShard})
	}
	return out
}

// SyncCatalog brings this node's catalogue in step with the store's (a cluster node runs
// it periodically): it opens the indexes other nodes created, adopts the mappings and
// settings they changed, and drops (stopping and removing their copies here) the
// indexes they dropped, or dropped and recreated.
func (n *Single) SyncCatalog(ctx context.Context) error {
	metas, err := n.st.Indexes().List(ctx)
	if err != nil {
		n.noteDB(err)
		return err
	}
	n.noteDB(nil)
	inStore := map[string]store.IndexMeta{}
	for _, m := range metas {
		inStore[m.Name] = m
	}
	n.mu.RLock()
	var gone []*index
	var fresh []store.IndexMeta
	var known []*index
	for name, idx := range n.indexes {
		m, ok := inStore[name]
		switch {
		case n.reserved[name]:
		case !ok || m.UID != idx.meta.Load().meta.UID:
			gone = append(gone, idx)
		default:
			known = append(known, idx)
		}
	}
	for _, m := range metas {
		if idx := n.indexes[m.Name]; (idx == nil || idx.meta.Load().meta.UID != m.UID) && !n.reserved[m.Name] {
			fresh = append(fresh, m)
		}
	}
	n.mu.RUnlock()
	for _, idx := range gone {
		n.forget(ctx, idx)
	}
	for _, idx := range known {
		m := inStore[idx.name]
		if cur := idx.meta.Load(); m.Version > cur.meta.Version {
			if st, err := parseState(m); err == nil {
				idx.meta.CompareAndSwap(cur, st)
				n.adoptSettings(idx, st.settings)
			}
		}
	}
	var errs []error
	for _, m := range fresh {
		if _, err := n.adopt(ctx, m); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// adoptIndex opens index name when the store has it (another node created it): the
// index, or nil when the store has none. With cachedAbsence, a name the store did not
// have is not looked up again for absentTTL, unless this node creates it meanwhile.
func (n *Single) adoptIndex(ctx context.Context, name string, cachedAbsence bool) (*index, error) {
	gen, absent := n.absent.check(name)
	if absent && cachedAbsence {
		return nil, nil
	}
	m, err := n.st.Indexes().Get(ctx, name)
	if errors.Is(err, store.ErrNotFound) {
		n.absent.note(name, gen)
		return nil, nil
	}
	if err != nil {
		return nil, storeError(err, name)
	}
	return n.adopt(ctx, m)
}

// adopt opens the index m describes, unless this node opened it meanwhile.
func (n *Single) adopt(ctx context.Context, m store.IndexMeta) (*index, error) {
	n.mu.Lock()
	switch cur := n.indexes[m.Name]; {
	case n.closed:
		n.mu.Unlock()
		return nil, api.Unavailable(store.ErrClosed, "the node is shutting down")
	case n.reserved[m.Name]:
		n.mu.Unlock()
		return nil, nil
	case cur != nil && cur.meta.Load().meta.UID == m.UID:
		n.mu.Unlock()
		return cur, nil
	}
	n.reserved[m.Name] = true
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		delete(n.reserved, m.Name)
		n.mu.Unlock()
	}()
	idx, err := n.openIndex(ctx, m)
	if err != nil {
		return nil, err
	}
	n.mu.Lock()
	old := n.indexes[m.Name]
	if n.closed {
		n.mu.Unlock()
		_ = n.stopIndex(context.WithoutCancel(ctx), idx)
		return nil, api.Unavailable(store.ErrClosed, "the node is shutting down")
	}
	n.indexes[m.Name] = idx
	n.mu.Unlock()
	if old != nil {
		n.retire(ctx, old)
	}
	n.log.InfoContext(ctx, "index adopted from the catalogue", slog.String(telemetry.KeyIndex, m.Name), slog.String("uid", m.UID))
	return idx, nil
}

// forget drops an index another node dropped (or dropped and recreated) from this
// node, stopping its copies here and removing their directories.
func (n *Single) forget(ctx context.Context, idx *index) {
	n.mu.Lock()
	if n.indexes[idx.name] == idx {
		delete(n.indexes, idx.name)
	}
	n.mu.Unlock()
	n.retire(ctx, idx)
	n.log.InfoContext(ctx, "index dropped from the catalogue", slog.String(telemetry.KeyIndex, idx.name))
}

// retire stops an index no longer in the catalogue and removes its copies' files.
func (n *Single) retire(ctx context.Context, idx *index) {
	idx.dropped.Store(true)
	if err := n.stopIndex(context.WithoutCancel(ctx), idx); err != nil {
		n.log.WarnContext(ctx, "closing a dropped index's copies failed", slog.String(telemetry.KeyIndex, idx.name), slog.Any("error", err))
	}
	n.removeLater(ctx, idx.dir)
}

// reloadIfNewer re-reads idx's catalogue entry and adopts it when another node has
// changed it since this node read it: changed reports that.
func (n *Single) reloadIfNewer(ctx context.Context, idx *index) (changed bool, err error) {
	m, err := n.st.Indexes().Get(ctx, idx.name)
	if err != nil {
		return false, err
	}
	cur := idx.meta.Load()
	if m.UID != cur.meta.UID || m.Version <= cur.meta.Version {
		return false, nil
	}
	st, err := parseState(m)
	if err != nil {
		return false, err
	}
	if !idx.meta.CompareAndSwap(cur, st) {
		return true, nil // someone else adopted a newer one meanwhile
	}
	n.adoptSettings(idx, st.settings)
	return true, nil
}

// CopyDir is a copy directory this node does not host a copy from.
type CopyDir struct {
	Shard store.ShardID
	Path  string
	// UnhostedAt is when this node stopped hosting the copy; zero when it has not
	// hosted it this run (an earlier run left the directory).
	UnhostedAt time.Time
}

// UnhostedCopyDirs lists the copy directories of open indexes this node holds no copy
// from: ones an earlier run left (it did not claim the shard back), or a released or
// lost copy's.
func (n *Single) UnhostedCopyDirs() []CopyDir {
	var out []CopyDir
	for _, idx := range n.sortedIndexes() {
		for s, sl := range idx.shards {
			if sl.local.Load() != nil {
				continue
			}
			root := idx.copyRoot(s)
			if _, err := os.Stat(root); err == nil {
				d := CopyDir{Shard: sl.id, Path: root}
				if at := sl.unhostedAtNano.Load(); at > 0 {
					d.UnhostedAt = time.Unix(0, at)
				}
				out = append(out, d)
			}
		}
	}
	return out
}

// RemoveCopyDir removes this node's directory of id, unless it hosts a copy of id
// (checked under the slot's host lock, so a claim racing it opens an empty one).
func (n *Single) RemoveCopyDir(ctx context.Context, id store.ShardID) error {
	n.mu.RLock()
	idx := n.indexes[id.Index]
	n.mu.RUnlock()
	if idx == nil || id.Shard < 0 || id.Shard >= len(idx.shards) {
		return nil
	}
	sl := idx.shards[id.Shard]
	sl.host.Lock()
	defer sl.host.Unlock()
	if sl.local.Load() != nil {
		return nil
	}
	n.log.InfoContext(ctx, "removing an unused copy directory", slog.String(telemetry.KeyIndex, id.Index), slog.Int(telemetry.KeyShard, id.Shard))
	return os.RemoveAll(idx.copyRoot(id.Shard))
}
