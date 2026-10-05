package shard

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Imposter/go-searchlight/internal/segment"
)

// A failed fsync is never retried: after a write-back error a later fsync may succeed
// over pages that were never written. The flush fails the shard, CommittedSeq stays
// short of the segment, and the reopened copy replays it from the changelog.
func TestFailedFsyncFailsTheShard(t *testing.T) {
	var armed atomic.Bool
	opts := testOptions()
	opts.hooks = &testHooks{sync: func(p string) error {
		if strings.HasSuffix(p, segment.FileExt) && armed.CompareAndSwap(true, false) {
			return fmt.Errorf("%w: write %s: input/output error", segment.ErrSync, p)
		}
		return nil
	}}
	h := newHarness(t, opts)
	h.upsert("a", "b")
	h.commit()
	durable := h.s.CommittedSeq()
	h.upsert("c")
	h.del("a")
	h.refresh()

	armed.Store(true)
	if err := h.s.Flush(context.Background()); !errors.Is(err, segment.ErrSync) {
		t.Fatalf("Flush = %v, want the fsync failure", err)
	}
	if !errors.Is(h.s.Err(), ErrFailed) {
		t.Fatalf("Err() = %v after a failed fsync, want the shard failed", h.s.Err())
	}
	if err := h.s.Flush(context.Background()); !errors.Is(err, ErrFailed) {
		t.Fatalf("a second Flush = %v, want the failure, not a retry", err)
	}
	if got := h.s.CommittedSeq(); got != durable {
		t.Fatalf("CommittedSeq %d, want %d", got, durable)
	}
	h.abandon()

	h.opts.hooks = nil
	h.open()
	if got := h.s.CommittedSeq(); got != durable {
		t.Fatalf("reopened at CommittedSeq %d, want %d", got, durable)
	}
	if got, ref := dirFiles(t, h.dir), referencedFiles(t, h.dir); !slices.Equal(got, ref) {
		t.Fatalf("files after reopen %v, manifest references %v", got, ref)
	}
	g := h.s.Acquire()
	if err := checkGeneration(g, h.snapshots[durable]); err != nil {
		g.Release()
		t.Fatal(err)
	}
	g.Release()
	h.replayFrom(durable)
	h.commit()
	h.check()
	h.reopen()
	h.check()
}

// A file a flush cannot open (another handle in the way) is no fsync failure: the
// flush fails, the shard does not, and the next flush persists the generation.
func TestFlushRetriesAFileItCouldNotOpen(t *testing.T) {
	var armed atomic.Bool
	opts := testOptions()
	opts.hooks = &testHooks{sync: func(p string) error {
		if armed.CompareAndSwap(true, false) {
			return fmt.Errorf("open %s: the process cannot access the file because it is being used by another process", p)
		}
		return nil
	}}
	h := newHarness(t, opts)
	h.upsert("a", "b")
	h.refresh()
	armed.Store(true)
	if err := h.s.Flush(context.Background()); err == nil || errors.Is(err, segment.ErrSync) {
		t.Fatalf("Flush = %v, want the open failure", err)
	}
	if err := h.s.Err(); err != nil {
		t.Fatalf("Err() = %v: an open failure failed the shard", err)
	}
	if h.s.CommittedSeq() != 0 {
		t.Fatalf("CommittedSeq %d after a failed flush", h.s.CommittedSeq())
	}
	h.flush()
	if h.s.CommittedSeq() != h.seq {
		t.Fatalf("CommittedSeq %d, want %d", h.s.CommittedSeq(), h.seq)
	}
	h.reopen()
	h.check()
}

// A flush whose context has ended stops waiting for its turn, and leaves the
// generation for the next flush.
func TestFlushHonoursItsContext(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a")
	h.refresh()
	h.s.flushSem <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.s.Flush(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Flush waiting for another = %v, want the context's end", err)
	}
	<-h.s.flushSem
	if err := h.s.Flush(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Flush under an ended context = %v", err)
	}
	if h.s.Err() != nil || h.s.CommittedSeq() != 0 {
		t.Fatalf("Err %v, CommittedSeq %d after cancelled flushes", h.s.Err(), h.s.CommittedSeq())
	}
	h.flush()
	if h.s.CommittedSeq() != h.seq {
		t.Fatalf("CommittedSeq %d, want %d", h.s.CommittedSeq(), h.seq)
	}
}
