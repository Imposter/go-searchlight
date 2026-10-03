package segment

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
)

// castagnoli is the CRC32C table; the standard library uses the CPU's CRC instructions.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func crc32c(p []byte) uint32 { return crc32.Checksum(p, castagnoli) }

// sectionEntry is one row of the footer's section table.
type sectionEntry struct {
	kind sectionKind
	off  uint64
	n    uint64
	crc  uint32
}

// fileWriter streams a segment file: it tracks the offset, a CRC32C per section and one
// over the whole file, and keeps the first error.
type fileWriter struct {
	f        *os.File
	chunk    []byte
	off      uint64 // bytes written, including those still in chunk
	fileCRC  uint32
	secCRC   uint32
	secKind  sectionKind
	secStart uint64
	sections []sectionEntry
	err      error
}

const writeChunk = 256 << 10

func newFileWriter(f *os.File) *fileWriter {
	return &fileWriter{f: f, chunk: make([]byte, 0, writeChunk)}
}

func (w *fileWriter) flush() {
	if len(w.chunk) == 0 {
		return
	}
	w.fileCRC = crc32.Update(w.fileCRC, castagnoli, w.chunk)
	w.secCRC = crc32.Update(w.secCRC, castagnoli, w.chunk)
	if w.err == nil {
		_, w.err = w.f.Write(w.chunk)
	}
	w.chunk = w.chunk[:0]
}

func (w *fileWriter) writeByte(b byte) {
	if len(w.chunk) == cap(w.chunk) {
		w.flush()
	}
	w.chunk = append(w.chunk, b)
	w.off++
}

func (w *fileWriter) write(p []byte) {
	w.off += uint64(len(p))
	for len(p) > 0 {
		if len(w.chunk) == cap(w.chunk) {
			w.flush()
		}
		n := copy(w.chunk[len(w.chunk):cap(w.chunk)], p)
		w.chunk = w.chunk[:len(w.chunk)+n]
		p = p[n:]
	}
}

func (w *fileWriter) u32(v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	w.write(b[:])
}

func (w *fileWriter) u64(v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	w.write(b[:])
}

// beginSection starts a section at the current offset.
func (w *fileWriter) beginSection(kind sectionKind) {
	w.flush()
	w.secKind, w.secStart, w.secCRC = kind, w.off, 0
}

// endSection closes the current section and records it in the table.
func (w *fileWriter) endSection() {
	w.flush()
	w.sections = append(w.sections, sectionEntry{kind: w.secKind, off: w.secStart, n: w.off - w.secStart, crc: w.secCRC})
}

// header writes the file header.
func (w *fileWriter) header() {
	w.write(magic[:])
	var b [8]byte
	binary.LittleEndian.PutUint16(b[0:], FormatMajor)
	binary.LittleEndian.PutUint16(b[2:], FormatMinor)
	w.write(b[:])
}

// footer writes the section table and the tail, including the file's checksum.
func (w *fileWriter) footer() {
	for _, s := range w.sections {
		w.u32(uint32(s.kind))
		w.u64(s.off)
		w.u64(s.n)
		w.u32(s.crc)
	}
	w.u32(uint32(len(w.sections))) //nolint:gosec // a handful of sections
	w.write(endMagic[:])
	w.flush()
	sum := w.fileCRC
	w.u32(sum)
	w.flush()
}

// parsedFooter is a verified file's section table.
type parsedFooter struct {
	sections map[sectionKind]sectionEntry
	checksum uint32
}

// verifyFile checks a segment's header, footer and checksums, and returns its section
// table. A whole-file mismatch is reported against the first section whose own CRC
// fails, so the error names what is damaged.
func verifyFile(path string, data []byte) (parsedFooter, error) {
	return verifyFileSections(path, data, sectionOrder[:])
}

// verifyFileSections is [verifyFile], but only the sections named in required must be
// present; a sidecar file (such as a deletes file) holds just one of them.
func verifyFileSections(path string, data []byte, required []sectionKind) (parsedFooter, error) {
	corrupt := func(section, format string, args ...any) error {
		return &CorruptError{Path: path, Section: section, Reason: fmt.Sprintf(format, args...)}
	}
	if len(data) < headerSize+tailSize {
		return parsedFooter{}, corrupt("", "%d bytes is too short for a segment", len(data))
	}
	if [8]byte(data[:8]) != magic {
		return parsedFooter{}, corrupt("header", "bad magic")
	}
	major := binary.LittleEndian.Uint16(data[8:])
	minor := binary.LittleEndian.Uint16(data[10:])
	if major != FormatMajor {
		return parsedFooter{}, &VersionError{Path: path, Major: major, Minor: minor}
	}
	tail := data[len(data)-tailSize:]
	if [8]byte(tail[4:12]) != endMagic {
		return parsedFooter{}, corrupt("footer", "bad end magic (a truncated or partly written file)")
	}
	count := uint64(binary.LittleEndian.Uint32(tail))
	tableStart := uint64(len(data)-tailSize) - count*sectionEntrySize //nolint:gosec // len(data) >= headerSize+tailSize, checked above
	if count > 64 || tableStart < headerSize || tableStart > uint64(len(data)) {
		return parsedFooter{}, corrupt("footer", "bad section count %d", count)
	}
	want := binary.LittleEndian.Uint32(tail[12:])
	got := crc32c(data[:len(data)-4])
	footer := parsedFooter{sections: make(map[sectionKind]sectionEntry, count), checksum: want}
	table := data[tableStart : len(data)-tailSize]
	for i := range count {
		e := table[i*sectionEntrySize:]
		s := sectionEntry{
			kind: sectionKind(binary.LittleEndian.Uint32(e)),
			off:  binary.LittleEndian.Uint64(e[4:]),
			n:    binary.LittleEndian.Uint64(e[12:]),
			crc:  binary.LittleEndian.Uint32(e[20:]),
		}
		if s.off < headerSize || s.off > tableStart || s.n > tableStart-s.off {
			if got != want {
				break // the table itself is damaged; reported below
			}
			return parsedFooter{}, corrupt(s.kind.String(), "section runs outside the file")
		}
		footer.sections[s.kind] = s
	}
	if got != want {
		for _, kind := range sectionOrder {
			s, ok := footer.sections[kind]
			if ok && crc32c(data[s.off:s.off+s.n]) != s.crc {
				return parsedFooter{}, corrupt(kind.String(), "checksum mismatch")
			}
		}
		return parsedFooter{}, corrupt("", "file checksum mismatch: stored %08x, computed %08x", want, got)
	}
	for _, kind := range required {
		if _, ok := footer.sections[kind]; !ok {
			return parsedFooter{}, corrupt(kind.String(), "section missing")
		}
	}
	return footer, nil
}
