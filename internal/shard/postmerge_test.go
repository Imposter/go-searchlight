package shard

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// ForceMerge(1) leaves one segment even when a background merge of some of its
// segments is in flight when it starts: that merge's output is ForceMerge's to merge
// too. (Before the fix ForceMerge only knew the segments present at its entry, so it
// returned with the in-flight merge's output beside its own, and background merges
// went on committing, and renaming the manifest, after it returned.)
func TestForceMergeIncludesMergesInFlightAtEntry(t *testing.T) {
	inMerge := make(chan struct{})
	release := make(chan struct{})
	var first atomic.Bool
	opts := testOptions()
	opts.DisableMerges = false
	opts.MergePolicy = &TieredPolicy{SegmentsPerTier: 2, MaxMergeAtOnce: 2, FloorSegmentBytes: 1}
	opts.hooks = &testHooks{at: func(point string) error {
		if point == pointMergeBuilt && first.CompareAndSwap(false, true) {
			close(inMerge) // the first (background) merge waits here
			<-release
		}
		return nil
	}}
	h := newHarness(t, opts)
	for i := range 6 {
		h.upsert(fmt.Sprintf("d%d", i))
		h.refresh()
	}
	<-inMerge
	done := make(chan error, 1)
	go func() { done <- h.s.ForceMerge(context.Background(), 1) }()
	// Let ForceMerge start: it takes its segments, then waits for the merge in flight.
	time.Sleep(50 * time.Millisecond)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	g := h.s.Acquire()
	n := len(g.Segments)
	g.Release()
	if n != 1 {
		t.Fatalf("ForceMerge(1) left %d segments", n)
	}
	h.check()
}
