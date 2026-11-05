package percolate

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/Imposter/go-searchlight/internal/shard"
)

// A segment keeps each query's client version (format 4), which Version and Query
// return; a segment of format 3, written before versions, still opens, its queries
// with none, and the index reads that format.
func TestSegmentVersions(t *testing.T) {
	dir := t.TempDir()
	queries := sampleStored(t)
	for i := range queries {
		queries[i].Version = int64(i * 7)
	}
	if _, err := (Index{}).Build(context.Background(), dir, "v", queries, nil); err != nil {
		t.Fatal(err)
	}
	qs, err := Index{}.Open(dir, "v")
	if err != nil {
		t.Fatal(err)
	}
	defer qs.Close()
	for _, q := range queries {
		ord, _ := qs.Ord(q.ID)
		got, err := qs.Query(ord)
		if err != nil || got.Version != q.Version || qs.Version(ord) != q.Version {
			t.Fatalf("query %s: version %d (%d), %v; want %d", q.ID, got.Version, qs.Version(ord), err, q.Version)
		}
	}
	if qs.Version(qs.NumQueries()) != 0 {
		t.Fatal("a version past the end")
	}

	data, err := os.ReadFile(filepath.Join(dir, "v"+FileExt))
	if err != nil {
		t.Fatal(err)
	}
	v3 := asFormat3(t, data)
	if err := os.WriteFile(filepath.Join(dir, "old"+FileExt), v3, 0o600); err != nil {
		t.Fatal(err)
	}
	old, err := Index{}.Open(dir, "old")
	if err != nil {
		t.Fatalf("a format 3 segment: %v", err)
	}
	defer old.Close()
	for _, q := range queries {
		ord, ok := old.Ord(q.ID)
		got, err := old.Query(ord)
		if !ok || err != nil || got.Version != 0 || got.Seq != q.Seq {
			t.Fatalf("format 3 query %s: %+v, %v", q.ID, got, err)
		}
	}
	var fr shard.FormatReader = Index{}
	if !fr.Reads("percolate/3") || fr.Reads("percolate/2") || FormatName != "percolate/4" {
		t.Fatal("the formats read")
	}
}

// asFormat3 rewrites a format 4 file as format 3 wrote it: without its last section.
func asFormat3(t *testing.T, data []byte) []byte {
	t.Helper()
	body := data[:len(data)-4]
	head := len(fileMagic) + 8
	rest := body[head:]
	var sections [][]byte
	for range numSections {
		n := binary.LittleEndian.Uint64(rest)
		sections = append(sections, rest[8:8+n])
		rest = rest[8+n:]
	}
	out := bytes.Clone(body[:head])
	binary.LittleEndian.PutUint32(out[len(fileMagic):], formatV3)
	binary.LittleEndian.PutUint32(out[len(fileMagic)+4:], numSectionsV3)
	for _, s := range sections[:numSectionsV3] {
		out = binary.LittleEndian.AppendUint64(out, uint64(len(s)))
		out = append(out, s...)
	}
	return binary.LittleEndian.AppendUint32(out, crc32.Checksum(out, castagnoli))
}
