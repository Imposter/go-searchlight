package segment

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/RoaringBitmap/roaring/v2"
)

// LiveDocs sidecar files hold a segment's deleted document ordinals for one generation.
// The segment file itself is never rewritten: a delete (or an update, which deletes the
// old copy) only ever adds a sidecar.
//
// A sidecar is the segment's own header and footer wrapped around one section, so it is
// checksummed and versioned exactly like a segment file.

const deletesExt = ".del"

// deletesPath returns the sidecar path for segmentID's generation gen.
func deletesPath(segmentDir, segmentID string, gen uint64) string {
	return filepath.Join(segmentDir, DeletesName(segmentID, gen))
}

// DeletesOptions configures [WriteDeletes].
type DeletesOptions struct {
	// NoDirSync is [BuildOptions.NoDirSync].
	NoDirSync bool
}

// WriteDeletes writes segmentID's generation gen deletes bitmap to segmentDir, replacing
// any sidecar already there for that generation. Write to a temp file, fsync, then
// rename, as a segment file is.
func WriteDeletes(segmentDir, segmentID string, gen uint64, deletes *roaring.Bitmap, opts DeletesOptions) error {
	path := deletesPath(segmentDir, segmentID, gen)
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if err := EncodeDeletes(f, deletes); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := SyncFile(f); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if opts.NoDirSync {
		return nil
	}
	return SyncDir(segmentDir)
}

// DeletesName is the file name of segmentID's generation gen deletes sidecar.
func DeletesName(segmentID string, gen uint64) string {
	return fmt.Sprintf("%s.%d%s", segmentID, gen, deletesExt)
}

// EncodeDeletes writes a deletes sidecar's bytes for deletes (nil: none deleted) to w:
// exactly what [WriteDeletes] puts in the file, so a copy of a shard can carry a
// sidecar from memory (a snapshot of a generation whose sidecar a later commit has
// already replaced on disk).
func EncodeDeletes(w io.Writer, deletes *roaring.Bitmap) error {
	fw := newFileWriter(w)
	fw.header()
	fw.beginSection(sectionPresence)
	var buf bytes.Buffer
	if deletes == nil {
		deletes = roaring.New()
	}
	if _, err := deletes.WriteTo(&buf); err != nil {
		return err
	}
	fw.write(buf.Bytes())
	fw.endSection()
	fw.footer()
	return fw.err
}

// LoadDeletes reads segmentID's generation gen deletes sidecar, or an empty bitmap when
// none has been written yet.
func LoadDeletes(segmentDir, segmentID string, gen uint64) (*roaring.Bitmap, error) {
	path := deletesPath(segmentDir, segmentID, gen)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return roaring.New(), nil
	}
	if err != nil {
		return nil, err
	}
	footer, err := verifyFileSections(path, data, []sectionKind{sectionPresence})
	if err != nil {
		return nil, err
	}
	sec := footer.sections[sectionPresence]
	rb := roaring.New()
	if sec.n > 0 {
		// The same check segment bitmaps get (checkBitmap): UnmarshalBinary accepts
		// container shapes that later operations on rb would panic over.
		raw := data[sec.off : sec.off+sec.n]
		if _, ok := checkBitmap(raw, 1<<32); !ok {
			return nil, &CorruptError{Path: path, Section: "presence", Reason: "malformed bitmap"}
		}
		if err := rb.UnmarshalBinary(raw); err != nil {
			return nil, &CorruptError{Path: path, Section: "presence", Reason: err.Error()}
		}
	}
	return rb, nil
}
