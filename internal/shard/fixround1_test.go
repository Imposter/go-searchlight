package shard

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/segment"
)

// I1: a sidecar a crash left behind, which garbage collection could not remove at
// Open, must never be removed later once a new commit has written (and the manifest
// references) a sidecar of the same name.
func TestGCRetryNeverRemovesARecommittedSidecar(t *testing.T) {
	var crash atomic.Bool
	opts := testOptions()
	opts.hooks = &testHooks{at: func(point string) error {
		if point == pointManifestWritten && crash.CompareAndSwap(true, false) {
			return errSimulatedCrash
		}
		return nil
	}}
	h := newHarness(t, opts)
	h.upsert("a", "b", "c")
	h.refresh()
	h.del("a")
	crash.Store(true)
	if err := h.s.Refresh(context.Background()); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("Refresh = %v, want the simulated crash", err)
	}
	h.abandon() // leaves <segment>.<gen>.del, written for a manifest that never landed

	// The leftover sidecar is in use (an indexer, an antivirus scanner) while the
	// shard reopens, so garbage collection cannot remove it then.
	var inUse atomic.Bool
	inUse.Store(true)
	h.opts.hooks = &testHooks{remove: func(p string) error {
		if inUse.Load() && strings.HasSuffix(p, deletesExt) {
			return errors.New("the file is being used by another process")
		}
		return os.Remove(p)
	}}
	h.open()
	h.replayFrom(h.s.CommittedSeq())
	h.refresh() // writes a sidecar for the same segment, maybe of the same name
	inUse.Store(false)
	h.s.jan.drain()
	h.check()
	h.reopen()
	h.check()
}

// I1: a manifest.tmp garbage collection could not remove at Open stays pending; a
// retry that runs while a commit has written its own manifest.tmp, before the rename,
// must not remove it.
func TestGCRetryNeverRemovesTheManifestBeingWritten(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a")
	h.refresh()
	h.abandon()
	if err := os.WriteFile(filepath.Join(h.dir, manifestName+".tmp"), []byte("left by a crash"), 0o600); err != nil {
		t.Fatal(err)
	}
	var inUse atomic.Bool
	inUse.Store(true)
	var s *Shard
	h.opts.hooks = &testHooks{
		remove: func(p string) error {
			if inUse.Load() && strings.HasSuffix(p, ".tmp") {
				return errors.New("the file is being used by another process")
			}
			return os.Remove(p)
		},
		at: func(point string) error {
			if point == pointManifestWritten && s != nil {
				inUse.Store(false)
				s.jan.drain() // the retry lands between write and rename
			}
			return nil
		},
	}
	h.open()
	s = h.s
	if p := h.s.jan.pendingFiles(); len(p) != 1 {
		t.Fatalf("pending after Open: %v, want manifest.tmp", p)
	}
	h.upsert("b")
	h.refresh()
	h.check()
	h.reopen()
	h.check()
}

// I2: when the directory fsync after the manifest rename fails, the swap may not be
// durable: the new generation is published, but nothing the old manifest needs is
// removed and CommittedSeq stays what is known to be durable. The next Open settles it.
func TestUncertainManifestSwapKeepsTheOldManifestsFiles(t *testing.T) {
	dirSyncFailed := errors.New("fsync directory: input/output error")
	var armed atomic.Bool
	opts := testOptions()
	opts.hooks = &testHooks{at: func(point string) error {
		if point == pointManifestRenamed && armed.CompareAndSwap(true, false) {
			return dirSyncFailed
		}
		return nil
	}}
	h := newHarness(t, opts)
	h.upsert("a", "b")
	h.refresh()
	h.del("a")
	h.refresh() // a sidecar of the first commit's segment
	h.upsert("c")
	h.refresh()
	before := dirFiles(t, h.dir)
	committed := h.s.CommittedSeq()

	// A merge whose swap is uncertain.
	armed.Store(true)
	if err := h.s.ForceMerge(context.Background(), 1); !errors.Is(err, dirSyncFailed) {
		t.Fatalf("ForceMerge = %v, want the directory fsync failure", err)
	}
	if !errors.Is(h.s.Err(), ErrFailed) {
		t.Fatalf("Err() = %v, want the shard failed", h.s.Err())
	}
	g := h.s.Acquire()
	if len(g.Segments) != 1 {
		t.Fatalf("%d segments: the merged generation was not published", len(g.Segments))
	}
	if err := checkGeneration(g, h.model); err != nil {
		t.Fatal(err)
	}
	g.Release()
	h.s.jan.drain()
	for _, name := range before {
		if _, err := os.Stat(filepath.Join(h.dir, name)); err != nil {
			t.Fatalf("%s, which the old manifest needs, was removed after an uncertain swap", name)
		}
	}
	if h.s.CommittedSeq() != committed {
		t.Fatalf("CommittedSeq %d, want %d", h.s.CommittedSeq(), committed)
	}

	// A refresh's uncertain swap leaves CommittedSeq where it was.
	h.abandon()
	armed.Store(true)
	h.open()
	h.upsert("d")
	committed = h.s.CommittedSeq()
	if err := h.s.Refresh(context.Background()); !errors.Is(err, dirSyncFailed) {
		t.Fatalf("Refresh = %v", err)
	}
	if h.s.RefreshedSeq() != h.seq || h.s.CommittedSeq() != committed {
		t.Fatalf("RefreshedSeq %d CommittedSeq %d, want %d and %d", h.s.RefreshedSeq(), h.s.CommittedSeq(), h.seq, committed)
	}
	h.check()
	h.abandon()
	h.opts.hooks = nil
	h.open() // whichever manifest survived: here the new one
	h.check()
	h.waitNoOrphans()
}

// I3: Close cancels a ForceMerge in flight (here throttled by a starved I/O budget,
// under a context that never ends) and waits for it, and a ForceMerge after Close is
// refused.
func TestCloseCancelsAndWaitsForForceMerge(t *testing.T) {
	opts := testOptions()
	opts.MergeBudget = NewMergeBudget(1, 1) // one byte per second: the merge stalls
	h := newHarness(t, opts)
	for i := range 3 {
		h.upsert(fmt.Sprintf("d%d", i))
		h.refresh()
	}
	merged := make(chan error, 1)
	go func() { merged <- h.s.ForceMerge(context.Background(), 1) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		h.s.commitMu.Lock()
		inflight := h.s.inflight
		h.s.commitMu.Unlock()
		if inflight > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ForceMerge never started")
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond) // into the throttle

	closed := make(chan error, 1)
	go func() { closed <- h.s.Close(context.Background()) }()
	select {
	case err := <-merged:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, ErrClosed) {
			t.Fatalf("ForceMerge across Close = %v, want cancelled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not cancel the ForceMerge in flight")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return")
	}
	if err := h.s.ForceMerge(context.Background(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("ForceMerge after Close = %v, want ErrClosed", err)
	}
	h.s = nil
}

// I3: Close returns only after the ForceMerge it cancelled has let go of the shard.
func TestCloseWaitsForForceMerge(t *testing.T) {
	inMerge := make(chan struct{})
	release := make(chan struct{})
	opts := testOptions()
	opts.hooks = &testHooks{at: func(point string) error {
		if point == pointMergeBuilt {
			close(inMerge)
			<-release
		}
		return nil
	}}
	h := newHarness(t, opts)
	h.upsert("a")
	h.refresh()
	h.upsert("b")
	h.refresh()
	merged := make(chan error, 1)
	go func() { merged <- h.s.ForceMerge(context.Background(), 1) }()
	<-inMerge
	closed := make(chan error, 1)
	go func() { closed <- h.s.Close(context.Background()) }()
	select {
	case <-closed:
		t.Fatal("Close returned while a ForceMerge was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-merged
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	h.s = nil
}

// syncsDuring returns the fsyncs f asked for.
func syncsDuring(f func()) segment.SyncStats {
	before := segment.SyncCounts()
	f()
	after := segment.SyncCounts()
	return segment.SyncStats{Files: after.Files - before.Files, Dirs: after.Dirs - before.Dirs}
}

// I4: a refresh that only moves the seq publishes it with no fsync at all; the
// manifest catches up at the next commit, every SeqPersistInterval, and at Close. A
// refresh that writes a segment syncs each file it wrote and the directory once.
func TestSeqOnlyRefresh(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a")
	h.refresh()
	h.del("a") // a sidecar on the first segment, as well as the second segment
	h.upsert("b")
	if got := syncsDuring(h.refresh); got.Files != 3 || got.Dirs != 1 {
		t.Fatalf("a refresh with a segment and a sidecar: %+v syncs, want 3 files (segment, sidecar, manifest) and 1 directory", got)
	}
	committed := h.s.CommittedSeq()
	h.seq += 10
	if err := h.s.Advance(h.seq); err != nil {
		t.Fatal(err)
	}
	if got := syncsDuring(h.refresh); got.Files != 0 || got.Dirs != 0 {
		t.Fatalf("a seq-only refresh: %+v syncs, want none", got)
	}
	if err := h.s.WaitRefreshed(context.Background(), h.seq); err != nil {
		t.Fatal(err)
	}
	if h.s.CommittedSeq() != committed {
		t.Fatalf("CommittedSeq %d, want %d until the seq is persisted", h.s.CommittedSeq(), committed)
	}
	h.snapshots[h.seq] = h.snapshots[committed]
	h.check()
	// The next commit persists it.
	h.upsert("c")
	h.refresh()
	if h.s.CommittedSeq() != h.seq {
		t.Fatalf("CommittedSeq %d after a commit, want %d", h.s.CommittedSeq(), h.seq)
	}
	// So does Close.
	h.seq += 5
	if err := h.s.Advance(h.seq); err != nil {
		t.Fatal(err)
	}
	h.refresh()
	h.reopen()
	if h.s.CommittedSeq() != h.seq {
		t.Fatalf("CommittedSeq %d after Close, want %d", h.s.CommittedSeq(), h.seq)
	}
	h.check()
}

// I4: in the background, a seq-only move is persisted within SeqPersistInterval.
func TestSeqPersistedInTheBackground(t *testing.T) {
	opts := testOptions()
	opts.RefreshInterval = 5 * time.Millisecond
	opts.SeqPersistInterval = 20 * time.Millisecond
	h := newHarness(t, opts)
	h.upsert("a")
	if err := h.s.Advance(100); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for h.s.CommittedSeq() != 100 {
		if time.Now().After(deadline) {
			t.Fatalf("CommittedSeq %d, want 100 within the persist interval", h.s.CommittedSeq())
		}
		time.Sleep(time.Millisecond)
	}
}
