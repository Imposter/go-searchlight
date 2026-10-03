package shard

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
)

func TestEmptyShard(t *testing.T) {
	h := newHarness(t, testOptions())
	h.check()
	h.refresh() // nothing to do
	if g := h.s.Acquire(); g.Gen() != 0 || g.Seq() != 0 || len(g.Segments) != 0 {
		t.Fatalf("empty shard: gen %d seq %d segments %d", g.Gen(), g.Seq(), len(g.Segments))
	} else {
		g.Release()
	}
	h.reopen()
	h.check()
}

// Review Focus 2: a document updated many times, deleted, then re-added appears exactly
// once, with its latest body, whatever mix of buffer, segments and merges its versions
// went through.
func TestUpdatesAndDeletesAcrossSegmentsAndMerges(t *testing.T) {
	h := newHarness(t, testOptions())

	// Updated many times, in one buffer and across refreshes.
	for i := range 5 {
		h.upsert("a", "b", "c")
		h.upsert("a")
		if i%2 == 0 {
			h.refresh()
		}
		h.check2(t)
	}
	h.refresh()
	h.check()

	// Deleted, then re-added: in separate refreshes, in one buffer, and after a merge.
	h.del("a")
	h.refresh()
	h.check()
	h.upsert("a")
	h.refresh()
	h.check()
	h.del("b")
	h.upsert("b")
	h.del("b")
	h.upsert("b")
	h.refresh()
	h.check()
	h.forceMerge(1)
	h.check()
	h.del("c")
	h.forceMerge(1) // the buffered delete is not visible yet...
	h.check2(t)
	h.refresh() // ...until the refresh
	h.check()
	h.upsert("c")
	h.refresh()
	h.forceMerge(1)
	h.check()
	h.reopen()
	h.check()
}

// planAll reserves a merge of every segment of kind, as the merge loop would.
func (h *harness) planAll(kind segKind) mergePlan {
	h.s.commitMu.Lock()
	defer h.s.commitMu.Unlock()
	cur := h.s.cur.Load()
	list := cur.docs
	if kind == kindQueries {
		list = cur.queries
	}
	p := mergePlan{kind: kind}
	for _, st := range list {
		p.inputs = append(p.inputs, st.ref)
	}
	h.s.reserve(p)
	return p
}

// check2 checks the shard against the model as of the last refresh.
func (h *harness) check2(t *testing.T) {
	t.Helper()
	g := h.s.Acquire()
	defer g.Release()
	if err := checkGeneration(g, h.snapshots[g.Seq()]); err != nil {
		t.Fatal(err)
	}
}

// Review Focus 2: an update of a document that is in the buffer and also in two
// segments (one live copy, one masked).
func TestUpdateOfDocInBufferAndTwoSegments(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("x", "y")
	h.refresh() // segment 1: x v1
	h.upsert("x")
	h.refresh() // segment 2: x v2; v1 masked in segment 1
	h.upsert("x")
	h.upsert("x") // buffer: x v4 replaced v3
	g := h.s.Acquire()
	if len(g.Segments) != 2 {
		t.Fatalf("%d segments, want 2", len(g.Segments))
	}
	g.Release()
	h.check2(t)
	h.refresh() // segment 3: x v4; v2 masked
	h.check()
	g = h.s.Acquire()
	copies := 0
	for _, sv := range g.Segments {
		if ord, ok := sv.Reader.Ord("x"); ok {
			copies++
			if sv != g.Segments[len(g.Segments)-1] && !sv.Deletes.Contains(ord) {
				t.Fatalf("an old copy of x in %s is live", sv.ID)
			}
		}
	}
	g.Release()
	if copies != 3 {
		t.Fatalf("x is in %d segments, want 3", copies)
	}
	h.forceMerge(1)
	h.check()
	g = h.s.Acquire()
	if len(g.Segments) != 1 || g.Segments[0].NumDocs != 2 || !g.Segments[0].Deletes.IsEmpty() {
		t.Fatalf("after ForceMerge(1): %d segments, %d docs", len(g.Segments), g.Segments[0].NumDocs)
	}
	g.Release()
}

// Two ids that normalize alike ("SKU-1", "sku-1", "sku-1 " with folded space) are
// distinct documents: the shard finds each by its exact id.
func TestMixedCaseIDsAreDistinct(t *testing.T) {
	h := newHarness(t, testOptions())
	ids := []string{"SKU-1", "sku-1", "Sku-1", "ÄÖ", "äö", "a  b", "a b", "Straße", "STRASSE"}
	h.upsert(ids...)
	h.refresh()
	h.check()
	h.upsert("sku-1", "ÄÖ", "a b")
	h.del("Sku-1", "STRASSE")
	h.refresh()
	h.check()
	h.forceMerge(1)
	h.check()
	h.del("SKU-1")
	h.upsert("Sku-1")
	h.refresh()
	h.check()
	h.reopen()
	h.check()
	g := h.s.Acquire()
	defer g.Release()
	if _, _, ok := g.Lookup("SKU-1"); ok {
		t.Fatal("deleted SKU-1 found")
	}
	if _, _, ok := g.Lookup("sku-1"); !ok {
		t.Fatal("sku-1 not found")
	}
}

// Deletes and updates refreshed while a merge is running land on the merged segment.
func TestMergeCarriesDeletesMadeDuringIt(t *testing.T) {
	built := make(chan struct{})
	proceed := make(chan struct{})
	opts := testOptions()
	opts.hooks = &testHooks{at: func(point string) error {
		if point == pointMergeBuilt {
			close(built)
			<-proceed
		}
		return nil
	}}
	h := newHarness(t, opts)
	for i := range 20 {
		h.upsert(fmt.Sprintf("d%02d", i))
		if i%5 == 4 {
			h.refresh()
		}
	}
	h.del("d03") // deleted before the merge's snapshot: dropped by the merge
	h.refresh()

	errc := make(chan error, 1)
	p := h.planAll(kindDocs)
	go func() {
		_, err := h.s.runMerge(context.Background(), p)
		errc <- err
	}()
	<-built
	// While the merged segment waits to be committed: update, delete, re-add.
	h.upsert("d01", "d07", "d19")
	h.del("d02", "d11")
	h.del("d07")
	h.upsert("d11")
	h.refresh()
	h.check()
	close(proceed)
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	h.check()
	g := h.s.Acquire()
	merged := g.Segments[0]
	for _, id := range []string{"d01", "d02", "d07", "d11", "d19"} {
		ord, ok := merged.Reader.Ord(id)
		if !ok || !merged.Deletes.Contains(ord) {
			t.Errorf("%s: the merged segment's copy (ord %d, %v) is not deleted", id, ord, ok)
		}
	}
	if _, ok := merged.Reader.Ord("d03"); ok {
		t.Error("d03, deleted before the merge, is in the merged segment")
	}
	g.Release()
	h.reopen()
	h.check()
}

func TestApplyRefusals(t *testing.T) {
	h := newHarness(t, testOptions())
	big := &schema.Doc{ID: "big", Body: make([]byte, segment.MaxStoredBytes-2)}
	ok1 := analyze(t, "ok1", `{"brand":"x"}`)
	ok2 := analyze(t, "ok2", `{"brand":"x"}`)

	cases := []struct {
		name    string
		changes []Change
		pos     int
		want    error
	}{
		{"oversized document", []Change{
			{Seq: 1, Kind: Upsert, Doc: ok1},
			{Seq: 2, Kind: Upsert, Doc: big},
			{Seq: 3, Kind: Upsert, Doc: ok2},
		}, 1, ErrDocTooLarge},
		{"seq not increasing", []Change{
			{Seq: 5, Kind: Upsert, Doc: ok1},
			{Seq: 5, Kind: Delete, DocID: "ok2"},
		}, 1, ErrSeqOrder},
		{"no document", []Change{{Seq: 1, Kind: Upsert, DocID: "x"}}, 0, ErrInvalidChange},
		{"id mismatch", []Change{{Seq: 1, Kind: Upsert, DocID: "other", Doc: ok1}}, 0, ErrInvalidChange},
		{"empty delete id", []Change{{Seq: 1, Kind: Delete}}, 0, ErrInvalidChange},
		{"unknown kind", []Change{{Seq: 1, Kind: 9, DocID: "x"}}, 0, ErrInvalidChange},
		{"query without a query", []Change{{Seq: 1, Kind: QueryUpsert, QueryID: "q"}}, 0, ErrInvalidChange},
		{"oversized meta", []Change{{Seq: 1, Kind: QueryUpsert, QueryID: "q", Query: &query.All{}, Meta: make([]byte, MaxMetaBytes+1)}}, 0, ErrInvalidChange},
		{"unstorable query", []Change{{Seq: 1, Kind: QueryUpsert, QueryID: "q", Query: &query.Any{}}}, 0, ErrInvalidChange},
		{"uid mismatch", []Change{
			{Seq: 1, Kind: Upsert, Doc: ok1, IndexUID: "u1"},
			{Seq: 2, Kind: Upsert, Doc: ok2, IndexUID: "u2"},
		}, 1, ErrIndexUID},
	}
	for _, c := range cases {
		err := h.s.Apply(context.Background(), c.changes)
		var ce *ChangeError
		if !errors.As(err, &ce) || ce.Pos != c.pos || !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want a ChangeError at %d wrapping %v", c.name, err, c.pos, c.want)
		}
	}
	if !errors.Is(ErrDocTooLarge, segment.ErrDocTooLarge) {
		t.Fatal("ErrDocTooLarge is not segment.ErrDocTooLarge")
	}
	// Nothing of a refused batch was applied, and the shard flushes as ever.
	if h.s.AppliedSeq() != 0 {
		t.Fatalf("applied seq %d after refused batches", h.s.AppliedSeq())
	}
	h.upsert("after")
	h.refresh()
	h.check()
	if h.s.IndexUID() != "" {
		t.Fatalf("IndexUID %q from a refused batch", h.s.IndexUID())
	}
}

// A document just under the size limit is stored; the one bad document of a stream is
// refused alone, and the refreshes around it carry on.
func TestOversizedDocNeverBlocksFlush(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("before")
	h.seq++
	big := &schema.Doc{ID: "big", Body: bytes.Repeat([]byte{'x'}, segment.MaxStoredBytes)}
	err := h.s.Apply(context.Background(), []Change{{Seq: h.seq, Kind: Upsert, Doc: big}})
	var ce *ChangeError
	if !errors.As(err, &ce) || ce.ID != "big" || ce.Seq != h.seq || !errors.Is(err, ErrDocTooLarge) {
		t.Fatalf("Apply(oversized) = %v", err)
	}
	if !strings.Contains(err.Error(), `"big"`) {
		t.Fatalf("error %q does not name the document", err)
	}
	// The caller skips it and goes on: the next refresh succeeds.
	h.upsert("after")
	h.refresh()
	h.check()

	// Exactly at the limit is fine.
	h.seq++
	prefix := `{"brand":"Acme","pad":"`
	atLimit := prefix + strings.Repeat("y", segment.MaxStoredBytes-len("edge")-len(prefix)-len(`"}`)) + `"}`
	d := analyze(t, "edge", `{"brand":"Acme"}`)
	d.Body = []byte(atLimit) // the stored body is all that counts toward the limit
	if err := h.s.Apply(context.Background(), []Change{{Seq: h.seq, Kind: Upsert, Doc: d}}); err != nil {
		t.Fatalf("Apply(at the limit): %v", err)
	}
	h.model["edge"] = atLimit
	h.snapshots[h.seq] = maps.Clone(h.model)
	h.refresh()
	h.check()
}

func TestWaitRefreshedWithGaps(t *testing.T) {
	h := newHarness(t, testOptions())
	ctx := context.Background()
	// This shard's changes are seqs 10 and 20 of the global changelog.
	d1, d2 := analyze(t, "a", `{}`), analyze(t, "b", `{}`)
	if err := h.s.Apply(ctx, []Change{{Seq: 10, Kind: Upsert, Doc: d1}}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- h.s.WaitRefreshed(ctx, 15) }() // seq 15 belongs to another shard
	if err := h.s.Apply(ctx, []Change{{Seq: 20, Kind: Upsert, Doc: d2}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		t.Fatalf("WaitRefreshed returned %v before any refresh", err)
	case <-time.After(20 * time.Millisecond):
	}
	h.refresh()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := h.s.RefreshedSeq(); got != 20 {
		t.Fatalf("RefreshedSeq %d, want 20", got)
	}

	// Advance moves the shard past changes it has none of.
	if err := h.s.Advance(30); err != nil {
		t.Fatal(err)
	}
	h.refresh()
	if err := h.s.WaitRefreshed(ctx, 30); err != nil {
		t.Fatal(err)
	}
	// A seq-only refresh is visible at once and persisted lazily (TestSeqOnlyRefresh).
	if h.s.CommittedSeq() != 20 || h.s.RefreshedSeq() != 30 {
		t.Fatalf("CommittedSeq %d RefreshedSeq %d after Advance(30) and a refresh, want 20 and 30", h.s.CommittedSeq(), h.s.RefreshedSeq())
	}
	g := h.s.Acquire()
	if g.Seq() != 30 || g.MaxSeq() != 20 {
		t.Fatalf("Seq %d MaxSeq %d, want 30 and 20", g.Seq(), g.MaxSeq())
	}
	g.Release()

	// A wait past everything ends with its context, or with Close.
	short, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if err := h.s.WaitRefreshed(short, 99); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WaitRefreshed(99) = %v, want a deadline", err)
	}
	go func() { done <- h.s.WaitRefreshed(ctx, 99) }()
	time.Sleep(5 * time.Millisecond)
	if err := h.s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrClosed) {
		t.Fatalf("WaitRefreshed across Close = %v, want ErrClosed", err)
	}
	h.s = nil
}

func TestBackgroundRefresh(t *testing.T) {
	opts := testOptions()
	opts.RefreshInterval = 5 * time.Millisecond
	h := newHarness(t, opts)
	h.upsert("a", "b")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.s.WaitRefreshed(ctx, h.seq); err != nil {
		t.Fatal(err)
	}
	h.check()
}

// A buffer over FlushBytes refreshes early, without waiting for the interval.
func TestFlushBytesTriggersRefresh(t *testing.T) {
	opts := testOptions()
	opts.RefreshInterval = time.Hour
	opts.FlushBytes = 1
	h := newHarness(t, opts)
	h.upsert("a")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.s.WaitRefreshed(ctx, h.seq); err != nil {
		t.Fatal(err)
	}
}

func TestIndexUID(t *testing.T) {
	h := newHarness(t, testOptions())
	h.seq++
	d := analyze(t, "a", `{}`)
	if err := h.s.Apply(context.Background(), []Change{{Seq: h.seq, Kind: Upsert, Doc: d, IndexUID: "uid-1"}}); err != nil {
		t.Fatal(err)
	}
	h.model["a"] = `{}`
	if h.s.IndexUID() != "uid-1" {
		t.Fatalf("IndexUID %q", h.s.IndexUID())
	}
	h.reopen()
	if h.s.IndexUID() != "uid-1" {
		t.Fatalf("IndexUID after reopen %q, want uid-1 from the manifest", h.s.IndexUID())
	}
	h.seq++
	err := h.s.Apply(context.Background(), []Change{{Seq: h.seq, Kind: Delete, DocID: "a", IndexUID: "uid-2"}})
	if !errors.Is(err, ErrIndexUID) {
		t.Fatalf("a change of another incarnation: %v", err)
	}
	g := h.s.Acquire()
	defer g.Release()
	if g.IndexUID() != "uid-1" {
		t.Fatalf("generation IndexUID %q", g.IndexUID())
	}
}

func TestReopenKeepsSegmentsAndSeq(t *testing.T) {
	h := newHarness(t, testOptions())
	for i := range 30 {
		h.upsert(fmt.Sprintf("d%d", i%13))
		if i%4 == 3 {
			h.del(fmt.Sprintf("d%d", i%7))
			h.refresh()
		}
	}
	h.upsert("buffered") // Close refreshes it
	h.reopen()
	if h.s.CommittedSeq() != h.seq {
		t.Fatalf("CommittedSeq %d, want %d", h.s.CommittedSeq(), h.seq)
	}
	h.check()
	if got, want := dirFiles(t, h.dir), referencedFiles(t, h.dir); !slices.Equal(got, want) {
		t.Fatalf("files %v, manifest references %v", got, want)
	}
}

func TestRefreshFailureKeepsTheBuffer(t *testing.T) {
	fail := errors.New("disk full")
	var armed bool
	var mu sync.Mutex
	opts := testOptions()
	opts.hooks = &testHooks{at: func(point string) error {
		mu.Lock()
		defer mu.Unlock()
		if armed && point == pointCommitSidecars {
			return fail
		}
		return nil
	}}
	h := newHarness(t, opts)
	h.upsert("a", "b")
	h.refresh()
	h.upsert("a")
	h.del("b")
	mu.Lock()
	armed = true
	mu.Unlock()
	before := dirFiles(t, h.dir)
	if err := h.s.Refresh(context.Background()); !errors.Is(err, fail) {
		t.Fatalf("Refresh = %v, want %v", err, fail)
	}
	h.upsert("c") // applied after the failed refresh froze its buffer
	h.check2(t)
	h.s.jan.drain()
	if after := dirFiles(t, h.dir); !slices.Equal(before, after) {
		t.Fatalf("a failed refresh left files: before %v, after %v", before, after)
	}
	mu.Lock()
	armed = false
	mu.Unlock()
	h.refresh()
	h.check()
}

func TestManifestDamageRefused(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a")
	h.refresh()
	h.abandon()
	path := filepath.Join(h.dir, manifestName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-3] ^= 0x20
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = Open(context.Background(), h.dir, testMapping, testOptions())
	var me *ManifestError
	if !errors.As(err, &me) {
		t.Fatalf("Open with a damaged manifest: %v, want a ManifestError", err)
	}
}

// A segment file damaged on disk is refused at Open (checksums), never served.
func TestDamagedSegmentRefused(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a", "b")
	h.refresh()
	g := h.s.Acquire()
	id := g.Segments[0].ID
	g.Release()
	h.abandon()
	path := filepath.Join(h.dir, id+segment.FileExt)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[40] ^= 0xff
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	var ce *segment.CorruptError
	if _, err := Open(context.Background(), h.dir, testMapping, testOptions()); !errors.As(err, &ce) {
		t.Fatalf("Open with a damaged segment: %v, want a CorruptError", err)
	}
}

func TestOpenRemovesOrphans(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a", "b")
	h.refresh()
	h.del("a")
	h.refresh()
	h.abandon()
	for _, name := range []string{
		"0123456789abcdef0123456789abcdef.seg",
		"0123456789abcdef0123456789abcdef.seg.tmp",
		"0123456789abcdef0123456789abcdef.3.del",
		"manifest.tmp",
		"stray",
	} {
		if err := os.WriteFile(filepath.Join(h.dir, name), []byte("junk"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(filepath.Join(h.dir, "subdir"), 0o750); err != nil {
		t.Fatal(err)
	}
	h.open()
	h.check()
	// Every shard file the manifest does not reference is gone; a file the shard
	// did not write, and a directory, are left alone.
	if got, want := dirFiles(t, h.dir), append(referencedFiles(t, h.dir), "stray"); !slices.Equal(got, slices.Sorted(slices.Values(want))) {
		t.Fatalf("files %v, want the manifest's and stray: %v", got, want)
	}
	if _, err := os.Stat(filepath.Join(h.dir, "subdir")); err != nil {
		t.Fatal("a subdirectory was removed")
	}
}

func TestAcquireAfterCloseIsNil(t *testing.T) {
	h := newHarness(t, testOptions())
	g := h.s.Acquire()
	if err := h.s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h.s.Acquire() != nil {
		t.Fatal("Acquire after Close returned a generation")
	}
	if err := h.s.Apply(context.Background(), []Change{{Seq: 1, Kind: Delete, DocID: "x"}}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Apply after Close = %v", err)
	}
	if err := h.s.Refresh(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Refresh after Close = %v", err)
	}
	g.Release() // a generation held across Close stays valid until released
	if err := h.s.Close(context.Background()); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	h.s = nil
}

func TestAcquireReleaseAllocatesNothing(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a")
	h.refresh()
	allocs := testing.AllocsPerRun(1000, func() {
		g := h.s.Acquire()
		g.Release()
	})
	if allocs != 0 {
		t.Fatalf("Acquire+Release allocates %.1f times", allocs)
	}
}

func TestReleaseTooOftenPanics(t *testing.T) {
	h := newHarness(t, testOptions())
	g := newGeneration(h.s, 0, 0, 0, "", nil, nil)
	g.Release()
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	g.Release()
}
