package shard

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/clock"
)

func segs(n int, bytes int64) []MergeCandidate {
	out := make([]MergeCandidate, n)
	for i := range out {
		out[i] = MergeCandidate{ID: fmt.Sprintf("s%03d", i), Bytes: bytes, Docs: 1000}
	}
	return out
}

// checkMerges verifies merges are disjoint, never hold a Merging segment, never exceed
// MaxMergeAtOnce, and returns how many segments they take.
func checkMerges(t *testing.T, p TieredPolicy, cands []MergeCandidate, merges [][]string) int {
	t.Helper()
	p = p.normalized()
	byID := map[string]MergeCandidate{}
	for _, c := range cands {
		byID[c.ID] = c
	}
	used := map[string]bool{}
	n := 0
	for _, m := range merges {
		if len(m) > p.MaxMergeAtOnce {
			t.Fatalf("a merge of %d segments, over MaxMergeAtOnce %d", len(m), p.MaxMergeAtOnce)
		}
		var size int64
		for _, id := range m {
			c, ok := byID[id]
			if !ok || c.Merging || used[id] {
				t.Fatalf("merge %v: %s is unknown, merging or already used", m, id)
			}
			used[id] = true
			size += c.size()
		}
		if len(m) > 1 && size > p.MaxMergedBytes {
			t.Fatalf("merge %v: %d bytes, over the %d cap", m, size, p.MaxMergedBytes)
		}
		n += len(m)
	}
	return n
}

func TestTieredPolicy(t *testing.T) {
	p := DefaultTieredPolicy()
	if p.MaxMergedBytes != 1<<30 {
		t.Fatalf("default MaxMergedBytes %d, want 1 GiB (merges are in memory)", p.MaxMergedBytes)
	}
	// Segments over half the cap (600 MiB) are left alone, however many there are.
	cands := segs(12, 1<<10)
	for i := range 12 {
		cands = append(cands, MergeCandidate{ID: fmt.Sprintf("big%d", i), Bytes: 600 << 20, Docs: 1000})
	}
	for _, mm := range p.FindMerges(cands) {
		for _, id := range mm {
			if strings.HasPrefix(id, "big") {
				t.Fatalf("merge %v takes a segment past half the 1 GiB cap", mm)
			}
		}
	}

	// Up to SegmentsPerTier small segments: nothing to do.
	if m := p.FindMerges(segs(10, 1<<10)); len(m) != 0 {
		t.Fatalf("10 small segments: merges %v", m)
	}
	// More: merge MaxMergeAtOnce of them at a time, down to the allowed count.
	cands = segs(25, 1<<10)
	m := p.FindMerges(cands)
	if n := checkMerges(t, p, cands, m); len(m) == 0 || n < 15 {
		t.Fatalf("25 small segments: merges %v", m)
	}
	for _, mm := range m {
		if len(mm) != 10 {
			t.Fatalf("merge of %d tiny segments, want full merges of 10: %v", len(mm), m)
		}
	}

	// Segments already merging are neither chosen nor counted.
	cands = segs(25, 1<<10)
	for i := range 15 {
		cands[i].Merging = true
	}
	if m := p.FindMerges(cands); len(m) != 0 {
		t.Fatalf("10 idle segments while 15 merge: merges %v", m)
	}

	// Tiers: ten 100 MiB segments and ten 1 MiB ones fit two tiers.
	cands = append(segs(10, 100<<20), segs(10, 1<<20)...)
	for i := 10; i < 20; i++ {
		cands[i].ID = fmt.Sprintf("small%d", i)
	}
	if m := p.FindMerges(cands); len(m) != 0 {
		t.Fatalf("two full tiers: merges %v", m)
	}
	// A run of similar sizes is preferred over mixing a big segment in.
	cands = append(cands, segs(15, 1<<20)...)
	for i := 20; i < len(cands); i++ {
		cands[i].ID = fmt.Sprintf("more%d", i)
	}
	m = p.FindMerges(cands)
	checkMerges(t, p, cands, m)
	if len(m) == 0 {
		t.Fatal("no merge with 35 segments")
	}
	for _, id := range m[0] {
		if id[0] == 's' && id[1] == '0' { // a 100 MiB one
			t.Fatalf("the first merge mixes in a big segment: %v", m[0])
		}
	}

	// A segment over half the cap is left alone, unless mostly deleted.
	big := MergeCandidate{ID: "big", Bytes: 3 << 30, Docs: 100}
	cands = append(segs(11, 1<<10), big)
	m = p.FindMerges(cands)
	checkMerges(t, p, cands, m)
	for _, mm := range m {
		for _, id := range mm {
			if id == "big" {
				t.Fatalf("a 3 GiB segment was merged: %v", m)
			}
		}
	}
	big.Deleted = 50
	m = p.FindMerges([]MergeCandidate{big})
	if len(m) != 1 || len(m[0]) != 1 || m[0][0] != "big" {
		t.Fatalf("a half-deleted 3 GiB segment: merges %v, want it rewritten alone", m)
	}

	// Deletes under the allowed share are left; over it, the segment is rewritten.
	cands = segs(3, 1<<20)
	cands[0].Deleted = 100 // 10%
	cands[1].Deleted = 300 // 30%
	m = p.FindMerges(cands)
	if len(m) != 1 || len(m[0]) != 1 || m[0][0] != "s001" {
		t.Fatalf("deletes reclaim: merges %v, want [[s001]]", m)
	}

	// The merged size cap holds when choosing.
	small := TieredPolicy{SegmentsPerTier: 2, MaxMergeAtOnce: 10, MaxMergedBytes: 5 << 20, FloorSegmentBytes: 1}
	cands = segs(12, 2<<20)
	m = small.FindMerges(cands)
	checkMerges(t, small, cands, m)
	for _, mm := range m {
		if len(mm) > 2 {
			t.Fatalf("merge %v exceeds the 5 MiB cap", mm)
		}
	}
	if len(m) == 0 {
		t.Fatal("no merge under a tight cap")
	}
	if m := small.FindMerges(nil); m != nil {
		t.Fatalf("no segments: %v", m)
	}
}

func TestMergeBudget(t *testing.T) {
	b := NewMergeBudget(3, 0, clock.Real{})
	ctx := context.Background()
	n, err := b.acquire(ctx, 2)
	if err != nil || n != 2 {
		t.Fatalf("acquire(2) = %d, %v", n, err)
	}
	m, err := b.acquire(ctx, 5)
	if err != nil || m != 1 {
		t.Fatalf("acquire(5) with one free = %d, %v", m, err)
	}
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, err := b.acquire(short, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquire with none free = %v", err)
	}
	b.release(n)
	b.release(m)
	if n, _ := b.acquire(ctx, 3); n != 3 {
		t.Fatalf("all tokens back: acquire(3) = %d", n)
	}

	// 10 MiB/s, by the budget's clock: each MiB after the first waits 0.1 s more.
	clk := clock.NewFake(time.Now())
	b = NewMergeBudget(1, 10<<20, clk)
	if err := b.throttle(ctx, 1<<20); err != nil {
		t.Fatalf("the first MiB waited: %v", err)
	}
	throttled := make(chan error, 1)
	go func() {
		for range 3 {
			if err := b.throttle(ctx, 1<<20); err != nil {
				throttled <- err
				return
			}
		}
		throttled <- nil
	}()
	for range 3 {
		if err := clk.BlockUntilArmed(ctx, 100*time.Millisecond); err != nil {
			t.Fatalf("a MiB at 10 MiB/s does not wait 0.1 s: %v", err)
		}
		clk.Advance(100 * time.Millisecond)
	}
	if err := <-throttled; err != nil {
		t.Fatal(err)
	}
	cancelled, cancel2 := context.WithCancel(ctx)
	cancel2()
	if err := b.throttle(cancelled, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("throttle after cancel = %v", err)
	}
	if NewMergeBudget(0, -5, clock.Real{}).Threads() != 1 {
		t.Fatal("a budget has at least one thread")
	}
}

func TestFilterCache(t *testing.T) {
	c := NewFilterCache(1<<20, nil)
	if _, ok := c.Get("s1", "k"); ok {
		t.Fatal("hit on an empty cache")
	}
	src := roaring.BitmapOf(1, 2, 3)
	got := c.Put("s1", "k", src)
	src.Add(99) // the cache holds a copy
	if bm, ok := c.Get("s1", "k"); !ok || bm != got || bm.Contains(99) || bm.GetCardinality() != 3 {
		t.Fatalf("Get after Put: %v, %v", bm, ok)
	}
	calls := 0
	compute := func() *roaring.Bitmap { calls++; return roaring.BitmapOf(7) }
	for range 3 {
		if bm := c.GetOrCompute("s2", "k", compute); !bm.Contains(7) {
			t.Fatal("GetOrCompute lost the bitmap")
		}
	}
	if calls != 1 {
		t.Fatalf("compute called %d times", calls)
	}
	st := c.Stats()
	if st.Hits != 3 || st.Misses != 2 || st.Entries != 2 {
		t.Fatalf("stats %+v, want 3 hits, 2 misses, 2 entries", st)
	}
	c.DropSegment("s1")
	if _, ok := c.Get("s1", "k"); ok {
		t.Fatal("entry of a dropped segment still cached")
	}
	if _, ok := c.Get("s2", "k"); !ok {
		t.Fatal("DropSegment dropped another segment's entry")
	}

	// Bounded by bytes: the least recently used entries go first.
	c = NewFilterCache(1<<20, nil) // 64 KiB per cache shard
	dense := roaring.New()
	dense.AddRange(0, 1<<16) // one run container after RunOptimize: tiny
	sparse := func(seed uint32) *roaring.Bitmap {
		bm := roaring.New()
		for i := range uint32(1500) {
			bm.Add(seed + i*37) // an array container of 3 KB
		}
		return bm
	}
	for i := range 2000 {
		c.Put(fmt.Sprintf("seg%d", i), "k", sparse(uint32(i)))
	}
	if st := c.Stats(); st.Bytes > 1<<20 || st.Entries >= 2000 {
		t.Fatalf("cache over budget: %+v", st)
	}
	if _, ok := c.Get("seg1999", "k"); !ok {
		t.Fatal("the most recent entry was evicted")
	}
	if _, ok := c.Get("seg0", "k"); ok {
		t.Fatal("the oldest entry survived 2000 inserts into a 1 MiB cache")
	}
	c.Put("dense", "k", dense)
	if bm, ok := c.Get("dense", "k"); !ok || bm.GetCardinality() != 1<<16 {
		t.Fatal("a run-optimized bitmap was not kept")
	}
}

// Entries of a segment the shard has closed for good are dropped.
func TestFilterCacheDroppedWithSegment(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a")
	h.refresh()
	h.upsert("b")
	h.refresh()
	g := h.s.Acquire()
	for _, sv := range g.Segments {
		g.FilterCache().GetOrCompute(sv.ID, "k", func() *roaring.Bitmap { return sv.Reader.Present("brand") })
	}
	g.Release()
	if n := h.s.opts.FilterCache.Stats().Entries; n != 2 {
		t.Fatalf("%d entries, want 2", n)
	}
	h.forceMerge(1)
	h.waitNoOrphans()
	if n := h.s.opts.FilterCache.Stats().Entries; n != 0 {
		t.Fatalf("%d entries left for merged-away segments", n)
	}
}
