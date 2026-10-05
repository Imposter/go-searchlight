package shard

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
