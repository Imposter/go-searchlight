package shard

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/segment"
)

// copyFile copies src to dst.
func copyFile(t *testing.T, src, dst string) int64 {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		t.Fatal(err)
	}
	n, err := io.Copy(out, in)
	if err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestOpensPreviousMajorSegments: a shard whose manifest lists segments of the previous
// format major (an upgraded node's) opens them as they are, without a rebuild; whether
// they mark untyped values comes from the manifest; a merge rewrites them into the
// current major, marking only when every input did; and the result reopens.
func TestOpensPreviousMajorSegments(t *testing.T) {
	fixtures := map[string]uint32{"marked": 300, "sparse": 400}
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "unmarked", true: "marked"}[legacy], func(t *testing.T) {
			dir := t.TempDir()
			man := &manifest{Gen: 1, Seq: 7, MaxSeq: 7}
			for name, docs := range fixtures {
				id := fmt.Sprintf("%032x", docs)
				src := filepath.Join("..", "segment", "testdata", "v3", name+segment.FileExt)
				size := copyFile(t, src, filepath.Join(dir, id+segment.FileExt))
				man.Segments = append(man.Segments, manifestSegment{ID: id, Docs: docs, Bytes: size})
			}
			if legacy {
				man.UntypedMarks = untypedMarksFormat
			}
			if _, _, err := writeManifest(dir, man, func(string) error { return nil }, func(...string) {}, quietLogger); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			s, err := Open(ctx, dir, testMapping, testOptions())
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer func() { s.shutdown() }()
			g := s.Acquire()
			if len(g.Segments) != 2 {
				t.Fatalf("%d segments", len(g.Segments))
			}
			extra := uint64(0)
			for _, sv := range g.Segments {
				if sv.Reader.FormatMajor() != segment.ReadsMajor || sv.MarksUntyped != legacy {
					t.Fatalf("segment %s: format %d, marks %v", sv.ID, sv.Reader.FormatMajor(), sv.MarksUntyped)
				}
				extra += sv.Reader.Untyped("extra").GetCardinality()
			}
			if extra == 0 {
				t.Fatal("the marked fixture's marks do not read")
			}
			if body, ok, err := g.Get("m7"); err != nil || !ok || len(body) == 0 {
				t.Fatalf("a document of a previous-major segment: %v, %v", ok, err)
			}
			g.Release()

			if err := s.ForceMerge(ctx, 1); err != nil {
				t.Fatal(err)
			}
			g = s.Acquire()
			if len(g.Segments) != 1 || g.Segments[0].Reader.FormatMajor() != segment.FormatMajor || g.Segments[0].MarksUntyped != legacy {
				t.Fatalf("after the merge: %d segments, format %d, marks %v", len(g.Segments), g.Segments[0].Reader.FormatMajor(), g.Segments[0].MarksUntyped)
			}
			if got := g.Segments[0].Reader.Untyped("extra").GetCardinality(); got != extra {
				t.Fatalf("the merge kept %d marks of %d", got, extra)
			}
			g.Release()
			if err := s.Close(ctx); err != nil {
				t.Fatal(err)
			}
			s, err = Open(ctx, dir, testMapping, testOptions())
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			g = s.Acquire()
			if g.Segments[0].MarksUntyped != legacy {
				t.Fatalf("reopened: marks %v", g.Segments[0].MarksUntyped)
			}
			g.Release()
		})
	}
}

// TestPreviousMajorDeletesStayReadable: a node on this build that holds only format-3
// segments and refreshes nothing but a delete writes the sidecar stamped as format 3,
// so a snapshot of the copy - segments and sidecar alike - is all format 3: a peer that
// reads only format 3 is served it (FormatMajors) and opens every file of it.
func TestPreviousMajorDeletesStayReadable(t *testing.T) {
	dir := t.TempDir()
	man := &manifest{Gen: 1, Seq: 7, MaxSeq: 7, UntypedMarks: untypedMarksFormat}
	src := filepath.Join("..", "segment", "testdata", "v3", "marked"+segment.FileExt)
	id := fmt.Sprintf("%032x", 300)
	size := copyFile(t, src, filepath.Join(dir, id+segment.FileExt))
	man.Segments = append(man.Segments, manifestSegment{ID: id, Docs: 300, Bytes: size})
	if _, _, err := writeManifest(dir, man, func(string) error { return nil }, func(...string) {}, quietLogger); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	s, err := Open(ctx, dir, testMapping, testOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.shutdown() }()
	if err := s.Apply(ctx, []Change{{Seq: 8, Kind: Delete, DocID: "m7"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	sn, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sn.Release()
	if oldest, newest := sn.FormatMajors(); oldest != segment.ReadsMajor || newest != segment.ReadsMajor {
		t.Fatalf("FormatMajors = %d, %d, want both %d", oldest, newest, segment.ReadsMajor)
	}
	out := t.TempDir()
	sidecars := 0
	for _, f := range sn.Files() {
		r, err := sn.Open(f.Name)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(out, f.Name), data, 0o600); err != nil {
			t.Fatal(err)
		}
		if f.Name == ManifestName {
			continue
		}
		if major := binary.LittleEndian.Uint16(data[8:]); major != segment.ReadsMajor {
			t.Fatalf("%s is stamped format %d, want %d", f.Name, major, segment.ReadsMajor)
		}
		if strings.HasSuffix(f.Name, deletesExt) {
			sidecars++
		}
	}
	if sidecars != 1 {
		t.Fatalf("the snapshot carries %d sidecars, want the delete's one", sidecars)
	}
	onDisk, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range onDisk {
		if !strings.HasSuffix(e.Name(), deletesExt) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if major := binary.LittleEndian.Uint16(data[8:]); major != segment.ReadsMajor {
			t.Fatalf("the flushed sidecar %s is stamped format %d", e.Name(), major)
		}
	}
	copied, err := Open(ctx, out, testMapping, testOptions())
	if err != nil {
		t.Fatalf("opening the copy: %v", err)
	}
	defer copied.shutdown()
	g := copied.Acquire()
	defer g.Release()
	if _, ok, _ := g.Get("m7"); ok {
		t.Fatal("the copy lost the delete")
	}
	if _, ok, _ := g.Get("m8"); !ok {
		t.Fatal("the copy lost a live document")
	}
}
