// Package segment is Searchlight's on-disk segment format: an immutable file of analyzed
// documents, written once ([Build], [Merge]) and read through mmap ([Open]).
//
// A segment file is a header, a run of sections and a footer:
//
//	header   magic (8 bytes), format major (u16), format minor (u16), reserved (u32)
//	TERMS    per (field, kind) term dictionaries, each one's postings interleaved with
//	         its prefix-compressed term blocks (zstd-compressed where that pays), then
//	         its sparse block index
//	DOCVALS  per field columns: numbers (frame-of-reference bit-packed), keyword
//	         ordinals, multi-valued entry ordinals
//	POINTS   per number field: documents sorted by value into blocks with min/max
//	         (BKD-lite)
//	PRESENCE per field roaring bitmaps: present docs, docs whose grams were truncated,
//	         and docs whose value the writer marked untyped
//	STORED   a zstd dictionary, then zstd blocks of a fixed number of documents
//	         (about 8 KB) holding each document's id and JSON body, then a bit-packed
//	         block table
//	IDS      the primary key: every document's exact id (as given, never normalized)
//	         to its ordinal, a term dictionary whose terms each hold one ordinal
//	         inline, then u64 (the dictionary's index offset in the section) + 1, or
//	         0 for a segment with no documents
//	META     segment flags and the field directory: names and where every structure
//	         starts
//	footer   section table (kind, offset, length, CRC32C), section count, end magic,
//	         CRC32C of every byte before it
//
// All integers are little-endian. [Open] verifies the whole-file checksum and refuses a
// file whose major version it does not read.
//
// # Versions
//
// Format 4.0 is the current layout, and the only one [Build] and [Merge] write. Open
// reads it and the major before it ([ReadsMajor], 3.0), so an upgraded node reopens its
// segments instead of rebuilding them, and merges rewrite them into the current major
// as they go. Every other major is refused with a [VersionError]: an older one is
// rebuilt ([ErrOlderFormat]), a newer one left as it is ([ErrNewerFormat]).
//
// 4.0 against 3.0:
//   - A term block starts with a flags byte; a block of long terms is stored
//     zstd-compressed when that saves at least a quarter of it.
//   - A word or gram term's postings are Elias-Fano coded instead of a roaring bitmap
//     when that is at least a fifth smaller; the low bit of the entry's postings
//     length names the codec.
//   - A point block holds only its documents; a partly covered block reads their
//     values from the number column.
//   - Stored blocks hold a fixed number of documents each, compressed against a
//     dictionary sampled from the segment's first documents, behind a bit-packed
//     block table.
//   - META carries segment flags (whether the writer marks untyped values) and a
//     per-field untyped bitmap. 3.0 overloaded the marks onto the truncated bitmap;
//     a 3.0 segment's marks are derived on read ([Reader.Untyped]).
//
// 3.0 was 2.0 plus the IDS section, without which [Reader.Ord] cannot answer. A minor
// bump is reserved for additions an older reader of the same major can safely ignore.
//
// # Offsets, and why most of them are section-relative
//
// META holds each field's entry points into TERMS, DOCVALS and POINTS (dictOff,
// keywordColOff, multiColOff, numberColOff, pointsOff) and into PRESENCE (presOff,
// truncOff, untypedOff). Every one of those is relative to its own section's
// absolute start (which the footer's section table gives), not to the file: Build and
// Merge can then build one field's whole contribution to a section - a term
// dictionary, a doc-values column, a point index - complete and self-contained, in
// its own private buffer, independently of (and in parallel with) every other field's,
// because nothing in it depends on knowing where it will land until the moment it is
// concatenated into the section, which is when its META entry is filled in. Open adds
// each section's absolute start, from the footer, to these entries exactly once, while
// parsing META (see [Reader.parseMeta]).
//
// A field's own structures can themselves bake in further offsets - a term
// dictionary's block index and each block's postings offset ([termDict.base]; see
// termdict.go), a point index's block table ([points.base]; see points.go) - and those
// are relative to that structure's own absolute start in turn (its META entry plus its
// section's start), for the same reason: so the structure is self-contained down to its
// own bytes, not just positioned independently of other fields. Open resolves each such
// base once, when it opens that structure, and every other accessor of a value derived
// from it (a postings region, a block's byte range) already works with an ordinary
// absolute mmap position and needs no further change.
//
// Doc-values columns (keyword, multi, number) and the stored-fields block table have no
// such nested offsets of their own - they are packed, contiguous data with nothing
// pointing elsewhere inside themselves - so only their one META or block-table entry
// needs this treatment.
package segment

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

// File format identification.
const (
	// FormatMajor is the segment format's major version: what Build and Merge write.
	// Open reads it and [ReadsMajor] (see the package doc comment's "Versions").
	FormatMajor = 4
	// ReadsMajor is the oldest major Open reads: the one before FormatMajor.
	ReadsMajor = FormatMajor - 1
	// FormatMinor is the segment format's minor version: additions an older reader of
	// the same major can ignore.
	FormatMinor = 0
	// FileExt is a segment file's extension.
	FileExt = ".seg"
)

// magic opens every segment file; the CR LF and SUB bytes catch text-mode mangling.
var magic = [8]byte{'S', 'L', 'S', 'E', 'G', '\r', '\n', 0x1a}

// endMagic closes every segment file, just before the file checksum.
var endMagic = [8]byte{'S', 'L', 'S', 'E', 'G', 'E', 'N', 'D'}

const (
	headerSize = 16
	// tailSize is the fixed end of the footer: section count, end magic, file CRC.
	tailSize = 4 + 8 + 4
	// sectionEntrySize is one section table entry: kind, offset, length, CRC.
	sectionEntrySize = 4 + 8 + 8 + 4
)

// sectionKind names a section in the footer's table.
type sectionKind uint32

const (
	sectionTerms sectionKind = iota + 1
	sectionDocValues
	sectionPoints
	sectionPresence
	sectionStored
	sectionMeta
	sectionIDs
)

var sectionNames = map[sectionKind]string{
	sectionTerms:     "terms",
	sectionDocValues: "docvalues",
	sectionPoints:    "points",
	sectionPresence:  "presence",
	sectionStored:    "stored",
	sectionMeta:      "meta",
	sectionIDs:       "ids",
}

func (k sectionKind) String() string {
	if name, ok := sectionNames[k]; ok {
		return name
	}
	return fmt.Sprintf("section(%d)", uint32(k))
}

// sectionOrder is the order sections are written in.
var sectionOrder = [...]sectionKind{
	sectionTerms, sectionDocValues, sectionPoints, sectionPresence, sectionStored, sectionIDs, sectionMeta,
}

// TermKind is which of a field's term dictionaries a term belongs to.
type TermKind uint8

// The term kinds.
const (
	// KindValue is a keyword or text field's whole normalized value, or a bool field's
	// TermTrue or TermFalse.
	KindValue TermKind = iota
	// KindEntry is one entry of a keyword_list field.
	KindEntry
	// KindWord is one word of a text field.
	KindWord
	// KindGram is one 3-rune substring of a keyword or text field's value.
	KindGram

	numKinds = 4
)

// The KindValue terms of a bool field.
const (
	TermTrue  = "true"
	TermFalse = "false"
)

var kindNames = [numKinds]string{"value", "entry", "word", "gram"}

func (k TermKind) String() string {
	if k < numKinds {
		return kindNames[k]
	}
	return fmt.Sprintf("TermKind(%d)", uint8(k))
}

// Valid reports whether k is one of the term kinds.
func (k TermKind) Valid() bool { return k < numKinds }

// Errors that classify why a shard copy's files do not open. A copy whose data is
// damaged or of a format this build no longer reads is rebuilt; one written in a newer
// format is refused and left as it is, never rebuilt over: a binary rolled back must
// not destroy what its successor wrote.
var (
	// ErrCorrupt marks data that fails its checksum, does not parse, or is missing
	// where a manifest lists it: a [CorruptError] matches it, and the shard and
	// percolator wrap their own damage in it.
	ErrCorrupt = errors.New("segment: corrupt")
	// ErrNewerFormat marks a file written in a format newer than this build reads.
	ErrNewerFormat = errors.New("segment: written in a newer format than this build reads")
	// ErrOlderFormat marks a file written in a format older than this build reads.
	ErrOlderFormat = errors.New("segment: written in an older format than this build reads")
)

// Rebuildable reports whether err, from opening a shard copy, means its files must be
// wiped and the copy rebuilt: they are damaged ([ErrCorrupt]) or of a format this
// build no longer reads ([ErrOlderFormat]). A newer format, and any other error (I/O,
// permissions, resources), is not.
func Rebuildable(err error) bool {
	return errors.Is(err, ErrCorrupt) || errors.Is(err, ErrOlderFormat)
}

// FormatError classifies a format version found against the one this build reads:
// [ErrNewerFormat] or [ErrOlderFormat] (nil when they are equal).
func FormatError(found, reads uint64) error {
	switch {
	case found > reads:
		return ErrNewerFormat
	case found < reads:
		return ErrOlderFormat
	}
	return nil
}

// CorruptError is a segment file that fails its checksum or does not parse.
type CorruptError struct {
	Path    string
	Section string // the damaged section, when one can be named
	Reason  string
}

func (e *CorruptError) Error() string {
	if e.Section != "" {
		return fmt.Sprintf("segment %s: corrupt %s section: %s", e.Path, e.Section, e.Reason)
	}
	return fmt.Sprintf("segment %s: corrupt: %s", e.Path, e.Reason)
}

// Unwrap makes a CorruptError match [ErrCorrupt].
func (e *CorruptError) Unwrap() error { return ErrCorrupt }

// VersionError is a segment file written by a format major this build does not read.
type VersionError struct {
	Path         string
	Major, Minor uint16
}

func (e *VersionError) Error() string {
	return fmt.Sprintf("segment %s: format %d.%d is not readable by format %d.%d, which reads majors %d and %d",
		e.Path, e.Major, e.Minor, FormatMajor, FormatMinor, ReadsMajor, FormatMajor)
}

// Unwrap makes a VersionError match [ErrNewerFormat] or [ErrOlderFormat].
func (e *VersionError) Unwrap() error {
	if e.Major < ReadsMajor {
		return ErrOlderFormat
	}
	return FormatError(uint64(e.Major), FormatMajor)
}

// readsMajor reports whether Open reads files of major.
func readsMajor(major uint16) bool { return major == FormatMajor || major == ReadsMajor }

// errShort is a structure that runs past the end of its bytes.
var errShort = errors.New("truncated structure")

// bitsFor returns how many bits hold every value in [0, max].
func bitsFor(maxValue uint64) uint8 {
	return uint8(bits.Len64(maxValue)) //nolint:gosec // at most 64
}

// packedWidth rounds a bit width the single-load unpacker cannot read (57 to 63) up to 64.
func packedWidth(b uint8) uint8 {
	if b > 56 {
		return 64
	}
	return b
}

// packedSize is the bytes n values of width b take, including the 8 bytes of padding
// that let every read be one unaligned 8-byte load.
func packedSize(n uint64, b uint8) uint64 {
	return (n*uint64(b)+7)/8 + 8
}

// unpack returns value i of a bit-packed array of width b (0..56 or 64).
func unpack(data []byte, i uint64, b uint8) uint64 {
	switch b {
	case 0:
		return 0
	case 64:
		return binary.LittleEndian.Uint64(data[i*8:])
	}
	pos := i * uint64(b)
	return binary.LittleEndian.Uint64(data[pos>>3:]) >> (pos & 7) & (1<<b - 1)
}

// packer bit-packs values of one width into a byteSink, LSB first.
type packer struct {
	out   byteSink
	acc   uint64
	nbits uint8
	width uint8
	buf   [8]byte
}

// byteSink is where packed bytes go: a section writer or an in-memory buffer.
type byteSink interface {
	writeByte(b byte)
	write(p []byte)
}

func newPacker(out byteSink, width uint8) *packer {
	return &packer{out: out, width: width}
}

func (p *packer) add(v uint64) {
	switch p.width {
	case 0:
		return
	case 64:
		binary.LittleEndian.PutUint64(p.buf[:], v)
		p.out.write(p.buf[:])
		return
	}
	p.acc |= v << p.nbits
	p.nbits += p.width
	for p.nbits >= 8 {
		p.out.writeByte(byte(p.acc)) //nolint:gosec // intentional truncation: packing one byte at a time
		p.acc >>= 8
		p.nbits -= 8
	}
}

// finish flushes the last partial byte and the 8 bytes of read padding.
func (p *packer) finish() {
	if p.nbits > 0 {
		p.out.writeByte(byte(p.acc)) //nolint:gosec // intentional truncation: packing the last partial byte
		p.acc, p.nbits = 0, 0
	}
	p.out.write(make([]byte, 8))
}

// decoder reads the varints and fixed integers of a structure, remembering the first
// overrun instead of panicking.
type decoder struct {
	b   []byte
	pos int
	err error
}

func (d *decoder) uvarint() uint64 {
	if d.err != nil {
		return 0
	}
	v, n := binary.Uvarint(d.b[d.pos:])
	if n <= 0 {
		d.err = errShort
		return 0
	}
	d.pos += n
	return v
}

func (d *decoder) u8() uint8 {
	if d.err != nil || d.pos >= len(d.b) {
		d.err = errShort
		return 0
	}
	v := d.b[d.pos]
	d.pos++
	return v
}

func (d *decoder) u32() uint32 {
	if d.err != nil || d.pos+4 > len(d.b) {
		d.err = errShort
		return 0
	}
	v := binary.LittleEndian.Uint32(d.b[d.pos:])
	d.pos += 4
	return v
}

func (d *decoder) u64() uint64 {
	if d.err != nil || d.pos+8 > len(d.b) {
		d.err = errShort
		return 0
	}
	v := binary.LittleEndian.Uint64(d.b[d.pos:])
	d.pos += 8
	return v
}

func (d *decoder) bytes(n uint64) []byte {
	if d.err != nil || d.pos > len(d.b) || n > uint64(len(d.b)-d.pos) { //nolint:gosec // d.pos <= len(d.b) is this type's invariant
		d.err = errShort
		return nil
	}
	v := d.b[d.pos : d.pos+int(n)] //nolint:gosec // n <= len(d.b)-d.pos, checked above
	d.pos += int(n)                //nolint:gosec // n <= len(d.b)-d.pos, checked above
	return v
}

// encoder appends varints and fixed integers to a byte slice.
type encoder struct{ b []byte }

func (e *encoder) uvarint(v uint64) { e.b = binary.AppendUvarint(e.b, v) }
func (e *encoder) u8(v uint8)       { e.b = append(e.b, v) }
func (e *encoder) u32(v uint32)     { e.b = binary.LittleEndian.AppendUint32(e.b, v) }
func (e *encoder) u64(v uint64)     { e.b = binary.LittleEndian.AppendUint64(e.b, v) }
func (e *encoder) bytes(p []byte) {
	e.uvarint(uint64(len(p)))
	e.b = append(e.b, p...)
}

// region is a byte range of the file: an absolute offset and a length.
type region struct{ off, n uint64 }

func (r region) slice(data []byte) ([]byte, bool) {
	if r.off > uint64(len(data)) || r.n > uint64(len(data))-r.off {
		return nil, false
	}
	return data[r.off : r.off+r.n], true
}
