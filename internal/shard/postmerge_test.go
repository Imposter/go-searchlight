package shard

import (
	"context"
	"errors"
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

// N1: a merge's output joins a running ForceMerge's segments only when every input is
// one of them; a merge that also took a segment refreshed since does not hand
// ForceMerge that new data to merge.
func TestForceEligibleNeedsAllInputs(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a")
	h.refresh()
	h.upsert("b")
	h.refresh()
	h.s.commitMu.Lock()
	cur := h.s.cur.Load()
	h.s.forceEligible = map[*segRef]bool{cur.docs[0].ref: true, cur.docs[1].ref: true}
	h.s.commitMu.Unlock()

	merged := func() *segRef {
		t.Helper()
		out, err := h.s.runMerge(context.Background(), h.planAll(kindDocs))
		if err != nil || out == nil {
			t.Fatalf("runMerge: %v, %v", out, err)
		}
		return out
	}
	// Every input eligible: the output is too.
	out := merged()
	h.s.commitMu.Lock()
	if !h.s.forceEligible[out] {
		t.Fatal("the merge of eligible segments is not eligible")
	}
	h.s.commitMu.Unlock()
	// One input refreshed after ForceMerge started: the output is not.
	h.upsert("c")
	h.refresh()
	out = merged()
	h.s.commitMu.Lock()
	defer h.s.commitMu.Unlock()
	if h.s.forceEligible[out] {
		t.Fatal("a merge that took a segment refreshed since is eligible")
	}
	h.s.forceEligible = nil
}

// N2: a ForceMerge waiting for another one ends when its context does.
func TestForceMergeWaitHonoursContext(t *testing.T) {
	inMerge := make(chan struct{})
	release := make(chan struct{})
	var first atomic.Bool
	opts := testOptions()
	opts.hooks = &testHooks{at: func(point string) error {
		if point == pointMergeBuilt && first.CompareAndSwap(false, true) {
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
	done := make(chan error, 1)
	go func() { done <- h.s.ForceMerge(context.Background(), 1) }()
	<-inMerge
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := h.s.ForceMerge(ctx, 1)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("a second ForceMerge = %v, want its deadline", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("the second ForceMerge returned after %v, not at its deadline", d)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	h.check()
}
