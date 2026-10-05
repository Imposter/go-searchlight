package shard

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// copySnapshot writes every file of sn into dir, the manifest last, as a peer recovery
// does.
func copySnapshot(t *testing.T, sn *Snapshot, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	files := sn.Files()
	if files[len(files)-1].Name != ManifestName {
		t.Fatalf("the last file is %q, not the manifest", files[len(files)-1].Name)
	}
	for _, f := range files {
		r, err := sn.Open(f.Name)
		if err != nil {
			t.Fatal(err)
		}
		b, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(b)) != f.Size {
			t.Fatalf("%s is %d bytes, listed as %d", f.Name, len(b), f.Size)
		}
		if err := os.WriteFile(filepath.Join(dir, f.Name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

// TestSnapshotOutlivesSupersededSidecars takes a snapshot of a generation with deletes
// sidecars, lets later commits supersede (and remove) those sidecars and merge its
// segments away, then copies the snapshot: the copy opens exactly as of the snapshot.
func TestSnapshotOutlivesSupersededSidecars(t *testing.T) {
	h := newHarness(t, testOptions())
	for i := range 40 {
		h.upsert(fmt.Sprintf("d%02d", i))
	}
	h.putQuery("q1", "q2")
	h.refresh()
	h.del("d01", "d02", "d03")
	h.upsert("d04", "d05")
	h.delQuery("q1")
	h.refresh() // the first segment now has a sidecar
	want := maps.Clone(h.model)
	wantQueries := maps.Clone(h.queries())
	wantSeq := h.seq

	sn, err := h.s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer sn.Release()
	if sn.Seq() != wantSeq {
		t.Fatalf("Seq %d, want %d", sn.Seq(), wantSeq)
	}
	var sidecars []string
	for _, f := range sn.Files() {
		if filepath.Ext(f.Name) == deletesExt {
			sidecars = append(sidecars, f.Name)
		}
	}
	if len(sidecars) == 0 {
		t.Fatal("the snapshot lists no deletes sidecar")
	}

	// A later commit replaces the sidecars on disk.
	h.del("d06", "d07", "d10", "d11")
	h.delQuery("q2")
	h.upsert("d08")
	h.refresh()
	h.s.jan.drain()
	for _, name := range sidecars {
		if _, err := os.Stat(filepath.Join(h.dir, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("superseded sidecar %s is still on disk (%v): the test proves nothing", name, err)
		}
	}
	// Then a merge drops the snapshot's segments from the shard.
	h.forceMerge(1)
	h.del("d09")
	h.refresh()

	dir := filepath.Join(t.TempDir(), "copy")
	copySnapshot(t, sn, dir)
	c, err := Open(context.Background(), dir, testMapping, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer c.shutdown()
	if got := c.CommittedSeq(); got != wantSeq {
		t.Fatalf("the copy is committed at %d, want %d", got, wantSeq)
	}
	g := c.Acquire()
	defer g.Release()
	if err := checkGeneration(g, want); err != nil {
		t.Fatal(err)
	}
	if err := checkQueries(g, wantQueries); err != nil {
		t.Fatal(err)
	}
	if g.IndexUID() != sn.IndexUID() || g.MappingVersion() != sn.MappingVersion() {
		t.Fatalf("copy uid %q mapping %d, snapshot %q %d", g.IndexUID(), g.MappingVersion(), sn.IndexUID(), sn.MappingVersion())
	}
}

// TestSnapshotHoldsSegmentFiles: a merge that drops the snapshot's segments leaves
// their files on disk until the snapshot is released.
func TestSnapshotHoldsSegmentFiles(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a", "b")
	h.refresh()
	h.upsert("c")
	h.refresh()
	sn, err := h.s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	h.forceMerge(1)
	for _, f := range sn.Files() {
		if f.Name == ManifestName {
			continue
		}
		if _, err := os.Stat(filepath.Join(h.dir, f.Name)); err != nil {
			t.Fatalf("%s is gone while the snapshot holds it: %v", f.Name, err)
		}
	}
	if _, err := sn.Open("nope.seg"); !errors.Is(err, ErrNoSuchFile) {
		t.Fatalf("Open of an unlisted name: %v", err)
	}
	names := make([]string, 0)
	for _, f := range sn.Files() {
		names = append(names, f.Name)
	}
	sn.Release()
	sn.Release() // idempotent
	h.refresh()
	deadline := 50
	for ; deadline > 0; deadline-- {
		gone := true
		for _, n := range names {
			if n == ManifestName {
				continue
			}
			if _, err := os.Stat(filepath.Join(h.dir, n)); err == nil {
				gone = false
			}
		}
		if gone {
			break
		}
		h.s.jan.drain()
	}
	if deadline == 0 {
		t.Fatalf("merged-away files %v outlived the snapshot's release: %v", names, dirFiles(t, h.dir))
	}
	if !slices.Contains(dirFiles(t, h.dir), ManifestName) {
		t.Fatal("no manifest")
	}
}
