package shard

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"

	"github.com/Imposter/go-searchlight/internal/clock"
	"github.com/Imposter/go-searchlight/internal/segment"
)

// Readers acquire, read and release generations nonstop while a writer applies,
// refreshes, and background merges replace segments under them. Every generation a
// reader sees must equal the model as of its Seq, every bitmap and stored document it
// reads must stay valid until its Release (including segments merged away meanwhile),
// and nothing a reader does takes a lock on the shard. Run under -race (CI) this also
// checks the publication of generations, the reference counts and the deletes bitmaps
// are free of data races.
func TestConcurrentReadersDuringRefreshAndMerge(t *testing.T) {
	opts := testOptions()
	opts.DisableMerges = false
	opts.MergePolicy = &TieredPolicy{SegmentsPerTier: 2, MaxMergeAtOnce: 3, FloorSegmentBytes: 1}
	opts.MergeBudget = NewMergeBudget(2, 0, clock.Real{})
	opts.FilterCache = NewFilterCache(1<<20, nil)
	h := newHarness(t, opts)

	var snaps sync.Map // seq -> map[string]string, stored before the refresh that publishes it
	snaps.Store(int64(0), map[string]string{})
	stop := make(chan struct{})
	errs := make(chan error, 16)
	var checks atomic.Int64
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := readOnce(h.s, &snaps); err != nil {
					errs <- err
					return
				}
				checks.Add(1)
				runtime.Gosched()
			}
		}()
	}

	rng := rand.New(rand.NewPCG(5, 7))
	maxSegments := 0
	for i := range 80 {
		for range 1 + rng.IntN(12) {
			id := fmt.Sprintf("d%02d", rng.IntN(50))
			if rng.IntN(5) == 0 {
				h.del(id)
			} else {
				h.upsert(id)
			}
		}
		snaps.Store(h.seq, maps.Clone(h.model))
		if i%2 == 1 {
			h.refresh()
		}
		if g := h.s.Acquire(); g != nil {
			maxSegments = max(maxSegments, len(g.Segments))
			g.Release()
		}
		select {
		case err := <-errs:
			t.Fatal(err)
		default:
		}
	}
	h.refresh()
	h.forceMerge(1)
	if g := h.s.Acquire(); len(g.Segments) != 1 {
		n := len(g.Segments)
		g.Release()
		t.Fatalf("ForceMerge(1) under background merges left %d segments", n)
	} else {
		g.Release()
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	h.check()
	if checks.Load() < 20 {
		t.Fatalf("readers only completed %d checks", checks.Load())
	}
	g := h.s.Acquire()
	gen := g.Gen()
	g.Release()
	t.Logf("%d reader checks over %d commits; at most %d segments at once", checks.Load(), gen, maxSegments)

	// Every segment merged away is gone once its last reader released it.
	h.waitNoOrphans()
}

// waitNoOrphans flushes, then waits for the janitor (which may be mid-drain in its own
// goroutine) to leave exactly the files the manifest references.
func (h *harness) waitNoOrphans() {
	h.t.Helper()
	if h.s.Err() == nil {
		h.flush()
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		h.s.jan.drain()
		got, want := dirFiles(h.t, h.dir), referencedFiles(h.t, h.dir)
		if slices.Equal(got, want) {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("files %v, manifest references %v", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

// readOnce acquires a generation and checks it against the model at its seq, reading
// every live document and a cached filter bitmap of every segment.
func readOnce(s *Shard, snaps *sync.Map) error {
	g := s.Acquire()
	if g == nil {
		return errors.New("Acquire returned nil on an open shard")
	}
	defer g.Release()
	want, ok := snaps.Load(g.Seq())
	if !ok {
		return fmt.Errorf("generation %d has seq %d, which no batch ended at", g.Gen(), g.Seq())
	}
	if err := checkGeneration(g, want.(map[string]string)); err != nil { //nolint:errcheck // only maps are stored
		return err
	}
	for _, sv := range g.Segments {
		bm := g.FilterCache().GetOrCompute(sv.ID, "brand=acme", func() *roaring.Bitmap {
			return sv.Reader.Postings("brand", segment.KindValue, "acme")
		})
		if bm.GetCardinality() != uint64(sv.NumDocs) {
			return fmt.Errorf("segment %s: cached brand=acme holds %d of %d documents", sv.ID, bm.GetCardinality(), sv.NumDocs)
		}
	}
	return nil
}

// A segment merged away keeps its file, and stays readable, while any generation
// listing it is held; it is removed after the final Release (when the mapping is gone,
// which Windows requires), and a removal that fails is retried until it works. Its
// sidecars, which only a durable manifest reads, go with the flush that drops them.
func TestMergedAwayFilesRemovedAfterLastRelease(t *testing.T) {
	var mu sync.Mutex
	failLeft := map[string]int{}
	attempts := map[string]int{}
	opts := testOptions()
	opts.DeleteRetry = time.Hour // retries happen when the test drains
	opts.hooks = &testHooks{remove: func(p string) error {
		mu.Lock()
		attempts[p]++
		n := failLeft[p]
		if n > 0 {
			failLeft[p] = n - 1
		}
		mu.Unlock()
		if n > 0 {
			return errors.New("the process cannot access the file because it is being used by another process")
		}
		return os.Remove(p)
	}}
	h := newHarness(t, opts)
	for i := range 3 {
		h.upsert(fmt.Sprintf("a%d", i), fmt.Sprintf("b%d", i), "shared")
		h.refresh()
		h.del(fmt.Sprintf("a%d", i))
		h.refresh() // a second deletes generation: the first sidecar is obsolete
	}
	h.waitNoOrphans() // obsolete sidecars are removed

	held := h.s.Acquire()
	var old []string
	for _, sv := range held.Segments {
		old = append(old, filepath.Join(h.dir, sv.ID+segment.FileExt))
	}
	segPath := filepath.Join(h.dir, held.Segments[0].ID+segment.FileExt)
	if runtime.GOOS == "windows" {
		// Why removal waits: Windows refuses to delete a mapped file.
		if err := os.Remove(segPath); err == nil {
			t.Fatal("removed a mapped segment file on Windows")
		}
	}
	h.forceMerge(1)
	h.s.jan.drain()
	for _, p := range old {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("%s removed while a generation still lists it: %v", p, err)
		}
	}
	if err := checkGeneration(held, h.snapshots[held.Seq()]); err != nil {
		t.Fatalf("a held generation after its segments were merged away: %v", err)
	}

	mu.Lock()
	failLeft[segPath] = 2
	mu.Unlock()
	held.Release()
	for range 3 {
		h.s.jan.drain()
	}
	for _, p := range old {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("%s still exists after the last release (stat: %v)", p, err)
		}
	}
	mu.Lock()
	if attempts[segPath] < 3 {
		t.Fatalf("%s: %d removal attempts, want 3 (two refused, then done)", segPath, attempts[segPath])
	}
	mu.Unlock()
	if p := h.s.jan.pendingFiles(); len(p) != 0 {
		t.Fatalf("pending removals left: %v", p)
	}
	if got, want := dirFiles(t, h.dir), referencedFiles(t, h.dir); !slices.Equal(got, want) {
		t.Fatalf("files %v, manifest references %v", got, want)
	}
}

// A removal that never succeeds before Close is collected by the next Open.
func TestPendingRemovalsCollectedAtOpen(t *testing.T) {
	opts := testOptions()
	opts.hooks = &testHooks{remove: func(string) error { return errors.New("in use") }}
	h := newHarness(t, opts)
	h.upsert("a")
	h.refresh()
	h.upsert("b")
	h.refresh()
	h.forceMerge(1)
	if err := h.s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got, want := dirFiles(t, h.dir), referencedFiles(t, h.dir); slices.Equal(got, want) {
		t.Fatal("expected leftovers from refused removals")
	}
	h.opts.hooks = nil
	h.open()
	if got, want := dirFiles(t, h.dir), referencedFiles(t, h.dir); !slices.Equal(got, want) {
		t.Fatalf("files %v, manifest references %v", got, want)
	}
	h.check()
}

// Background merges keep the segment count bounded under a stream of refreshes.
func TestBackgroundMergesBoundSegments(t *testing.T) {
	opts := testOptions()
	opts.DisableMerges = false
	opts.MergePolicy = &TieredPolicy{SegmentsPerTier: 3, MaxMergeAtOnce: 3, FloorSegmentBytes: 1 << 20}
	h := newHarness(t, opts)
	for i := range 20 {
		h.upsert(fmt.Sprintf("d%d", i), fmt.Sprintf("d%d", i/2))
		h.refresh()
	}
	// Let the merge loop finish what the last refreshes started.
	deadline := time.Now().Add(10 * time.Second)
	for {
		h.s.commitMu.Lock()
		inflight := h.s.inflight
		h.s.commitMu.Unlock()
		g := h.s.Acquire()
		n := len(g.Segments)
		g.Release()
		if inflight == 0 && n <= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d segments, %d merges in flight after 20 refreshes", n, inflight)
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.check()
}

// A merge waiting on, or throttled by, the budget stops when its context ends, and
// leaves nothing behind.
func TestMergeCancelledByBudget(t *testing.T) {
	opts := testOptions()
	opts.MergeBudget = NewMergeBudget(1, 1, clock.Real{}) // one byte per second
	h := newHarness(t, opts)
	for i := range 3 {
		h.upsert(fmt.Sprintf("d%d", i))
		h.refresh()
	}
	before := dirFiles(t, h.dir)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := h.s.ForceMerge(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ForceMerge under a starved budget = %v", err)
	}
	h.s.jan.drain()
	if after := dirFiles(t, h.dir); !slices.Equal(before, after) {
		t.Fatalf("a cancelled merge left files: %v, before %v", after, before)
	}
	h.check()
	if h.s.Err() != nil {
		t.Fatal(h.s.Err())
	}
}
