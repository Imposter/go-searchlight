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
	at := meta.off + 4
	if footer.major >= 4 {
		at += 4 // flags
	}
	rel := binary.LittleEndian.Uint64(data[at:])
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

// TestOlderFormatsRefused pins N2 and the 4.0 bump: a file older than the previous
// major (1.x: absolute offsets or no stored rawLen; 2.x: no IDS section) must be
// refused with a VersionError that reads as older (so the copy is rebuilt), never
// parsed with a newer layout; one newer than this build must be refused as newer. The
// checksums are made valid, so only the version check stands between such a file and
// a misread.
func TestOlderFormatsRefused(t *testing.T) {
	data := buildFile(t, testDocs(t))
	if major, minor := binary.LittleEndian.Uint16(data[8:]), binary.LittleEndian.Uint16(data[10:]); major != 4 || minor != 0 {
		t.Fatalf("Build wrote format %d.%d, want 4.0", major, minor)
	}
	for _, major := range []uint16{1, 2, 5} {
		for _, minor := range []uint16{0, 1, 7} {
			old := append([]byte(nil), data...)
			binary.LittleEndian.PutUint16(old[8:], major)
			binary.LittleEndian.PutUint16(old[10:], minor)
			_, err := openCrafted(t, old)
			var ve *VersionError
			if !errors.As(err, &ve) || ve.Major != major || ve.Minor != minor {
				t.Fatalf("Open of format %d.%d: err = %v (%T), want *VersionError{Major: %d, Minor: %d}", major, minor, err, err, major, minor)
			}
			if older := errors.Is(err, ErrOlderFormat); older != (major < ReadsMajor) || Rebuildable(err) != older {
				t.Fatalf("Open of format %d.%d: older %v, rebuildable %v", major, minor, older, Rebuildable(err))
			}
		}
	}
}

// idsTrailer returns the absolute position of the IDS section's trailer in data.
func idsTrailer(t testing.TB, data []byte) uint64 {
	t.Helper()
	sec := layoutOf(t, data).sections[sectionIDs]
	return sec.off + sec.n - 8
}

// TestIDsSectionDamageRefused: each way a checksum-valid IDS section can disagree with
// the segment is refused at Open, naming the section.
func TestIDsSectionDamageRefused(t *testing.T) {
	cases := map[string]func(t *testing.T, data []byte){
		"trailer past the section": func(t *testing.T, data []byte) {
			binary.LittleEndian.PutUint64(data[idsTrailer(t, data):], 1<<40)
		},
		"no dictionary for documents": func(t *testing.T, data []byte) {
			binary.LittleEndian.PutUint64(data[idsTrailer(t, data):], 0)
		},
		"term count differs from the documents": func(t *testing.T, data []byte) {
			dict := openValid(t, data).ids
			binary.LittleEndian.PutUint32(data[dict.base:], dict.numTerms-1)
		},
	}
	for name, damage := range cases {
		t.Run(name, func(t *testing.T) {
			data := buildFile(t, testDocs(t))
			damage(t, data)
			_, err := openCrafted(t, data)
			wantCorrupt(t, err, "ids")
		})
	}
}

// An inline ordinal past the segment's documents reads as no match, never as a
// document the segment does not have.
func TestIDsOrdinalOutOfRangeReadsAbsent(t *testing.T) {
	r, err := openData("crafted", craftIDsOrdinal(t))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	id, err := r.ID(0)
	if err != nil {
		t.Fatal(err)
	}
	if ord, ok := r.Ord(id); ok {
		t.Fatalf("Ord(%q) = %d, true for an ordinal past %d documents", id, ord, r.NumDocs())
	}
}

// TestMalformedPostingsReadEmpty: a term whose serialized postings fail checkBitmap
// (here, an array container with a repeated value) reads as empty - on every call,
// since a failure is never remembered as checked - while a well-formed term in the
// same dictionary reads correctly, first call and cached call alike.
func TestMalformedPostingsReadEmpty(t *testing.T) {
	data := buildFile(t, testDocs(t))
	valid := openValid(t, data)
	info, ok := valid.fields["brand"].dicts[KindValue].lookup([]byte("acme"))
	if !ok || info.docFreq != 2 {
		t.Fatalf("acme: %+v, %v; want a 2-document serialized bitmap", info, ok)
	}
	end := info.post.off + info.post.n // the array's last value is the region's last 2 bytes
	copy(data[end-2:end], data[end-4:end-2])
	r, err := openCrafted(t, data)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if got := r.Postings("brand", KindValue, "acme"); !got.IsEmpty() {
			t.Fatalf("malformed postings = %v, want empty", got.ToArray())
		}
	}
	if got := r.TermFreq("brand", KindValue, "acme"); got != 2 {
		t.Fatalf("TermFreq = %d, want the dictionary's 2", got)
	}
	tags := r.Postings("tags", KindEntry, "red")
	for range 2 {
		if got := r.Postings("tags", KindEntry, "red"); !got.Equals(tags) || got.GetCardinality() != 2 {
			t.Fatalf("well-formed postings = %v, want [0 1]", got.ToArray())
		}
	}
}
