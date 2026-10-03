package shard

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/segment"
)

// Saved queries follow the same upsert and delete semantics as documents, through the
// buffer, refreshes, merges and a reopen.
func TestQuerySegments(t *testing.T) {
	h := newHarness(t, testOptions())
	h.putQuery("q1", "q2", "q3")
	h.checkQueries2(t)
	h.refresh()
	h.checkQueries()
	h.putQuery("q1") // update: the old copy is masked
	h.delQuery("q2")
	h.refresh()
	h.checkQueries()
	h.putQuery("q2") // re-added
	h.delQuery("q3")
	h.putQuery("q3")
	h.delQuery("q3")
	h.refresh()
	h.checkQueries()
	g := h.s.Acquire()
	if len(g.QuerySegments) != 3 {
		t.Fatalf("%d query segments, want 3", len(g.QuerySegments))
	}
	g.Release()
	h.forceMerge(1)
	h.checkQueries()
	g = h.s.Acquire()
	if len(g.QuerySegments) != 1 || g.QuerySegments[0].NumQueries != 2 {
		t.Fatalf("after ForceMerge(1): %d query segments", len(g.QuerySegments))
	}
	g.Release()
	h.reopen()
	h.checkQueries()
	h.delQuery("q1", "q2")
	h.refresh()
	h.forceMerge(1) // wholly deleted: dropped, nothing written
	h.checkQueries()
	if g := h.s.Acquire(); len(g.QuerySegments) != 0 {
		t.Fatalf("%d query segments left after deleting every query", len(g.QuerySegments))
	} else {
		g.Release()
	}
}

func (h *harness) checkQueries2(t *testing.T) {
	t.Helper()
	g := h.s.Acquire()
	defer g.Release()
	if err := checkQueries(g, h.querySnapshots[g.Seq()]); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultQueryIndexRoundTrip(t *testing.T) {
	queries := []string{
		`{"all":[]}`,
		`{"all":[{"field":"brand","op":"eq","value":"Ac\"me"},{"not":{"field":"price","op":"gt","value":10}}]}`,
		`{"any":[{"field":"tags","op":"has_any","value":["a","ß","İ"]},{"field":"title","op":"exists"}]}`,
		`{"all":[{"field":"title","op":"similar","value":{"text":"café","min":0.4}},{"field":"_id","op":"in","value":["x","y"]}]}`,
	}
	var stored []StoredQuery
	for i, raw := range queries {
		n, problems := query.Parse([]byte(raw))
		if len(problems) > 0 {
			t.Fatalf("%s: %v", raw, problems)
		}
		q := StoredQuery{ID: fmt.Sprintf("q%d", i), Seq: int64(i + 1), Query: n}
		if i%2 == 0 {
			q.Meta = []byte(`{"owner":"x"}`)
		}
		if err := (DefaultQueryIndex{}).Check(&q); err != nil {
			t.Fatalf("Check(%s): %v", raw, err)
		}
		stored = append(stored, q)
	}
	dir := t.TempDir()
	qi := DefaultQueryIndex{}
	size, err := qi.Build(context.Background(), dir, "seg", stored, nil)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "seg"+defaultQueryExt))
	if err != nil || info.Size() != size {
		t.Fatalf("Build reported %d bytes, the file has %v (%v)", size, info, err)
	}
	seg, err := qi.Open(dir, "seg")
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	if seg.NumQueries() != uint32(len(queries)) {
		t.Fatalf("NumQueries %d", seg.NumQueries())
	}
	for i, want := range stored {
		ord, ok := seg.Ord(want.ID)
		if !ok || ord != uint32(i) {
			t.Fatalf("Ord(%s) = %d, %v", want.ID, ord, ok)
		}
		got, err := seg.Query(ord)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != want.ID || got.Seq != want.Seq || !bytes.Equal(got.Meta, want.Meta) ||
			!bytes.Equal(query.Canonical(got.Query), query.Canonical(want.Query)) {
			t.Fatalf("query %d: got %+v, want %+v", i, got, want)
		}
	}
	if _, err := seg.Query(99); err == nil {
		t.Fatal("Query(99) did not fail")
	}

	// Damage is detected.
	path := filepath.Join(dir, "seg"+defaultQueryExt)
	data, _ := os.ReadFile(path)
	data[20] ^= 1
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := qi.Open(dir, "seg"); err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("Open of a damaged query segment: %v", err)
	}
}

func TestDefaultQueryIndexCheckRefuses(t *testing.T) {
	for _, n := range []query.Node{
		&query.Any{}, // an empty any: Parse refuses it, so it cannot be stored
		&query.All{Children: []query.Node{nil}},
		&query.Leaf{Field: "x", Op: "eq", Value: []byte("{bad")},
	} {
		if err := (DefaultQueryIndex{}).Check(&StoredQuery{ID: "q", Query: n}); err == nil {
			t.Errorf("Check(%#v) accepted it", n)
		}
	}
}

// recordingIndex is a QueryIndexBuilder that wraps the default and records the stats it
// was given.
type recordingIndex struct {
	DefaultQueryIndex
	format   string
	numDocs  []uint64
	acmeFreq []uint64
}

func (r *recordingIndex) Format() string { return r.format }

func (r *recordingIndex) Build(ctx context.Context, dir, name string, queries []StoredQuery, stats TermStats) (int64, error) {
	r.numDocs = append(r.numDocs, stats.NumDocs())
	r.acmeFreq = append(r.acmeFreq, stats.DocFreq("brand", segment.KindValue, "acme"))
	return r.DefaultQueryIndex.Build(ctx, dir, name, queries, stats)
}

// The query index builder is pluggable, gets the shard's document statistics, and a
// segment it did not write is refused by format.
func TestQueryIndexBuilderPluggable(t *testing.T) {
	ri := &recordingIndex{format: "recording/1"}
	opts := testOptions()
	opts.QueryIndex = ri
	h := newHarness(t, opts)
	h.upsert("a", "b", "c")
	h.refresh()
	h.putQuery("q")
	h.refresh()
	if len(ri.numDocs) != 1 || ri.numDocs[0] != 3 || ri.acmeFreq[0] != 3 {
		t.Fatalf("stats seen by Build: numDocs %v, DocFreq(brand=acme) %v; want 3 and 3", ri.numDocs, ri.acmeFreq)
	}
	h.checkQueries()
	if err := h.s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.s = nil
	_, err := Open(context.Background(), h.dir, testMapping, testOptions()) // the default builder
	if err == nil || !strings.Contains(err.Error(), "recording/1") {
		t.Fatalf("Open with another query index format: %v", err)
	}
	h.open() // the right builder still opens it
	h.checkQueries()
}

// A failing query index Build fails the refresh without losing the buffer.
type failingIndex struct {
	DefaultQueryIndex
	fail bool
}

func (f *failingIndex) Build(ctx context.Context, dir, name string, queries []StoredQuery, stats TermStats) (int64, error) {
	if f.fail {
		// Leave a partial file, as a crash in the middle of a build would.
		_ = os.WriteFile(filepath.Join(dir, name+defaultQueryExt+".tmp"), []byte("partial"), 0o600)
		return 0, errors.New("query index build failed")
	}
	return f.DefaultQueryIndex.Build(ctx, dir, name, queries, stats)
}

func TestQueryBuildFailureKeepsBuffer(t *testing.T) {
	fi := &failingIndex{fail: true}
	opts := testOptions()
	opts.QueryIndex = fi
	h := newHarness(t, opts)
	h.upsert("a")
	h.putQuery("q")
	if err := h.s.Refresh(context.Background()); err == nil {
		t.Fatal("Refresh succeeded with a failing query index")
	}
	fi.fail = false
	h.refresh()
	h.check()
	h.checkQueries()
	h.waitNoOrphans()
}
