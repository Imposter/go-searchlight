package segment

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// fixChecksums rewrites data's per-section CRCs and whole-file CRC in place to match its
// current bytes, so a deliberately damaged segment gets past verifyFile and exercises
// the parsers behind it - the files N4 is about, which only a checksum-valid crafted
// file can reach. A footer too damaged to locate its sections is left alone (Open then
// refuses it at the checksum, which is fine for a fuzz input too).
func fixChecksums(data []byte) {
	if len(data) < headerSize+tailSize {
		return
	}
	tail := data[len(data)-tailSize:]
	count := uint64(binary.LittleEndian.Uint32(tail))
	if count > 64 || count*sectionEntrySize > uint64(len(data)-headerSize-tailSize) {
		return
	}
	tableStart := uint64(len(data)-tailSize) - count*sectionEntrySize
	for i := range count {
		e := data[tableStart+i*sectionEntrySize:]
		off, n := binary.LittleEndian.Uint64(e[4:]), binary.LittleEndian.Uint64(e[12:])
		if off <= tableStart && n <= tableStart-off {
			binary.LittleEndian.PutUint32(e[20:], crc32c(data[off:off+n]))
		}
	}
	binary.LittleEndian.PutUint32(data[len(data)-4:], crc32c(data[:len(data)-4]))
}

// segmentLayout locates a valid segment's sections and META's stored index offset, so
// a test can damage one particular structure.
type segmentLayout struct {
	sections       map[sectionKind]sectionEntry
	storedIndexAbs uint64 // the stored block table's absolute position
}

func layoutOf(t testing.TB, data []byte) segmentLayout {
	t.Helper()
	footer, err := verifyFile("layout", data)
	if err != nil {
		t.Fatal(err)
	}
	meta := footer.sections[sectionMeta]
	rel := binary.LittleEndian.Uint64(data[meta.off+4:])
	return segmentLayout{sections: footer.sections, storedIndexAbs: footer.sections[sectionStored].off + rel}
}

// buildFile builds docs and returns the segment's bytes.
func buildFile(t testing.TB, docs []schema.Doc) []byte {
	t.Helper()
	meta, err := Build(t.TempDir(), docs, BuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(meta.Path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// openCrafted fixes data's checksums, writes it out and opens it.
func openCrafted(t *testing.T, data []byte) (*Reader, error) {
	t.Helper()
	fixChecksums(data)
	path := filepath.Join(t.TempDir(), "crafted.seg")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := Open(path)
	if err == nil {
		t.Cleanup(func() { _ = r.Close() })
	}
	return r, err
}

func wantCorrupt(t *testing.T, err error, section string) {
	t.Helper()
	var ce *CorruptError
	if !errors.As(err, &ce) {
		t.Fatalf("error = %v (%T), want *CorruptError", err, err)
	}
	if ce.Section != section {
		t.Fatalf("CorruptError section = %q, want %q (%v)", ce.Section, section, err)
	}
}

// craftStoredBlockCount is N4's stored.go case: a block count of 2^32-1 in a table with
// a few bytes after it. Open used to make([]storedBlockInfo, n) first - about 100 GiB -
// and only then find the bytes missing.
func craftStoredBlockCount(t testing.TB, data []byte) []byte {
	t.Helper()
	binary.LittleEndian.PutUint32(data[layoutOf(t, data).storedIndexAbs:], 0xFFFFFFFF)
	return data
}

func TestStoredBlockCountBoundedBeforeAllocating(t *testing.T) {
	data := craftStoredBlockCount(t, buildFile(t, testDocs(t)))
	_, err := openCrafted(t, data)
	wantCorrupt(t, err, "stored")
}

// TestStoredTableRefusesInconsistentBlocks damages one field of the first block's
// table entry at a time; each must be refused at Open.
func TestStoredTableRefusesInconsistentBlocks(t *testing.T) {
	cases := map[string]func(entry []byte){
		"offset past section":  func(e []byte) { binary.LittleEndian.PutUint64(e[0:], 1<<40) },
		"clen past section":    func(e []byte) { binary.LittleEndian.PutUint32(e[8:], 0xFFFFFFF0) },
		"rawLen over ceiling":  func(e []byte) { binary.LittleEndian.PutUint32(e[12:], maxStoredBlockRaw+1) },
		"rawLen under 2/doc":   func(e []byte) { binary.LittleEndian.PutUint32(e[12:], 1) },
		"firstOrd not zero":    func(e []byte) { binary.LittleEndian.PutUint32(e[16:], 1) },
		"empty block":          func(e []byte) { binary.LittleEndian.PutUint32(e[20:], 0) },
		"count past numDocs":   func(e []byte) { binary.LittleEndian.PutUint32(e[20:], 1000) },
		"count short of total": func(e []byte) { binary.LittleEndian.PutUint32(e[20:], 1) },
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			data := buildFile(t, testDocs(t)) // 7 documents: one block
			damage(data[layoutOf(t, data).storedIndexAbs+4:])
			_, err := openCrafted(t, data)
			wantCorrupt(t, err, "stored")
		})
	}
}

// TestStoredRawLenMismatchIsAnError: a block whose recorded rawLen disagrees with what
// it decompresses to is refused when read, with an error - never a panic, and never a
// buffer sized from the zstd frame's own (unchecked) content size.
func TestStoredRawLenMismatchIsAnError(t *testing.T) {
	for _, delta := range []int{-1, +1} {
		data := buildFile(t, testDocs(t))
		e := data[layoutOf(t, data).storedIndexAbs+4:]
		raw := binary.LittleEndian.Uint32(e[12:])
		binary.LittleEndian.PutUint32(e[12:], uint32(int(raw)+delta))
		r, err := openCrafted(t, data)
		if err != nil {
			t.Fatalf("delta %d: Open: %v", delta, err)
		}
		if _, err := r.Stored(0); err == nil {
			t.Fatalf("delta %d: Stored of a block with the wrong rawLen succeeded", delta)
		}
	}
}

func TestDocTooLargeRefused(t *testing.T) {
	doc := schema.Doc{ID: "big", Body: make([]byte, MaxStoredBytes)} // with the 3-byte id, over the limit
	_, err := Build(t.TempDir(), []schema.Doc{doc}, BuildOptions{})
	if !errors.Is(err, ErrDocTooLarge) {
		t.Fatalf("Build of a %d-byte document: err = %v, want ErrDocTooLarge", MaxStoredBytes+3, err)
	}
	doc.Body = doc.Body[:MaxStoredBytes-len(doc.ID)] // exactly at the limit: accepted
	if _, err := Build(t.TempDir(), []schema.Doc{doc}, BuildOptions{}); err != nil {
		t.Fatalf("Build of a document exactly MaxStoredBytes long: %v", err)
	}
}

// TestFormatOneRefused pins N2: a 1.x file - 1.0 (absolute offsets) or 1.1 (no stored
// rawLen, points offsets from the wrong origin) - must be refused with a VersionError,
// never parsed with the 2.x layout. The checksums are made valid, so only the version
// check stands between such a file and a misread.
func TestFormatOneRefused(t *testing.T) {
	data := buildFile(t, testDocs(t))
	if major, minor := binary.LittleEndian.Uint16(data[8:]), binary.LittleEndian.Uint16(data[10:]); major != 2 || minor != 0 {
		t.Fatalf("Build wrote format %d.%d, want 2.0", major, minor)
	}
	for _, minor := range []uint16{0, 1, 7} {
		old := append([]byte(nil), data...)
		binary.LittleEndian.PutUint16(old[8:], 1)
		binary.LittleEndian.PutUint16(old[10:], minor)
		_, err := openCrafted(t, old)
		var ve *VersionError
		if !errors.As(err, &ve) || ve.Major != 1 || ve.Minor != minor {
			t.Fatalf("Open of format 1.%d: err = %v (%T), want *VersionError{Major: 1, Minor: %d}", minor, err, err, minor)
		}
	}
}
