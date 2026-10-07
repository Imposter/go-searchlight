package segment

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestFormat4Golden: Build and Merge write, byte for byte, the format-4 files in
// testdata/v4, which the writer of format 4.0 as first released (commit 4cfd594, one
// field's dictionaries at a time) built from compatCorpora with one thread, whatever
// the number of threads now. Merging the two halves of dense wrote dense's bytes.
func TestFormat4Golden(t *testing.T) {
	corpora := compatCorpora(t)
	dir := t.TempDir()
	for _, name := range compatFixtures {
		want := readGolden4(t, name)
		docs := currentDocs(corpora[name])
		for _, threads := range []int{1, 4} {
			m, err := Build(dir, docs, BuildOptions{Threads: threads, NoSync: true, Name: fmt.Sprintf("%s-%d", name, threads)})
			if err != nil {
				t.Fatal(err)
			}
			sameBytes(t, fmt.Sprintf("%s built with %d threads", name, threads), m.Path, want)
		}
	}
	dense := currentDocs(corpora["dense"])
	halves := make([]*Reader, 2)
	for i, part := range [][]int{{0, 125}, {125, len(dense)}} {
		m, err := Build(dir, dense[part[0]:part[1]], BuildOptions{Threads: 1, NoSync: true, Name: fmt.Sprint("half-", i)})
		if err != nil {
			t.Fatal(err)
		}
		r, err := Open(m.Path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		halves[i] = r
	}
	want := readGolden4(t, "dense")
	for _, threads := range []int{1, 4} {
		m, err := Merge(dir, halves, nil, MergeOptions{Threads: threads, Name: fmt.Sprint("merged-", threads)})
		if err != nil {
			t.Fatal(err)
		}
		sameBytes(t, fmt.Sprintf("dense's halves merged with %d threads", threads), m.Path, want)
	}
}

func readGolden4(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "v4", name+FileExt))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func sameBytes(t *testing.T, what, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		at := 0
		for at < min(len(got), len(want)) && got[at] == want[at] {
			at++
		}
		t.Errorf("%s: %d bytes, want %d; first difference at byte %d", what, len(got), len(want), at)
	}
}
