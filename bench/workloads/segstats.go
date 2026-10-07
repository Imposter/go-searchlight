package workloads

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// This file reads Searchlight's on-disk segment format (internal/segment, spec §6)
// from the outside, the way any external tool reading a documented file format
// would: it duplicates the footer's fixed layout rather than importing
// internal/segment, so the harness never reaches into engine internals, only the
// stable, documented contract internal/segment's own doc comment describes ("A
// segment file is a header, a run of sections and a footer"). It does not verify
// checksums or the format version: a segment it cannot make sense of is skipped
// (segmentSectionSizes returns an error, and the caller treats that as 0 for that
// file), never fatal to the benchmark run.

// segFileExt is a segment file's extension (internal/segment.FileExt).
const segFileExt = ".seg"

// segTailSize is the footer's fixed end: section count (u32), end magic (8 bytes),
// file CRC32C (u32).
const segTailSize = 4 + 8 + 4

// segEntrySize is one section-table entry: kind (u32), offset (u64), length (u64),
// CRC32C (u32).
const segEntrySize = 4 + 8 + 8 + 4

// segSectionNames maps a section kind (internal/segment's sectionKind) to its name,
// matching internal/segment's own sectionNames exactly (spec §6's table: term
// dictionary, postings and doc values interleaved as "terms"; doc values; points;
// presence bitmaps; stored fields; the field directory; and the id dictionary).
var segSectionNames = map[uint32]string{
	1: "terms", 2: "docvalues", 3: "points", 4: "presence", 5: "stored", 6: "meta", 7: "ids",
}

// segmentSectionSizes reads one segment file's footer and returns its sections'
// byte lengths by name, summed when a kind appears more than once (it never does
// today, but the footer's table does not promise uniqueness).
func segmentSectionSizes(path string) (map[string]int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size < segTailSize {
		return nil, fmt.Errorf("%s: %d bytes is too short for a segment", path, size)
	}
	tail := make([]byte, segTailSize)
	if _, err := f.ReadAt(tail, size-segTailSize); err != nil {
		return nil, err
	}
	count := int64(binary.LittleEndian.Uint32(tail))
	tableBytes := count * segEntrySize
	if count < 0 || tableBytes < 0 || tableBytes+segTailSize > size {
		return nil, fmt.Errorf("%s: bad section count %d", path, count)
	}
	table := make([]byte, tableBytes)
	if _, err := f.ReadAt(table, size-segTailSize-tableBytes); err != nil {
		return nil, err
	}
	out := make(map[string]int64, count)
	for i := int64(0); i < count; i++ {
		e := table[i*segEntrySize:]
		kind := binary.LittleEndian.Uint32(e)
		length := binary.LittleEndian.Uint64(e[12:])
		name, ok := segSectionNames[kind]
		if !ok {
			name = fmt.Sprintf("section(%d)", kind)
		}
		out[name] += int64(length) //nolint:gosec // segment section lengths, not attacker-controlled sizes
	}
	return out, nil
}

// sectionSizes sums every .seg file's section sizes found under paths' directories
// (recursively: a node's data directory nests segments under each index and shard).
// A segment that cannot be read (removed mid-merge, or mid-write) is skipped, not
// fatal: this is a best-effort diagnostic, not a correctness check.
func sectionSizes(paths []string) (map[string]int64, error) {
	out := map[string]int64{}
	for _, root := range paths {
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				}
				return err
			}
			if !d.Type().IsRegular() || filepath.Ext(p) != segFileExt {
				return nil
			}
			sizes, err := segmentSectionSizes(p)
			if err != nil {
				return nil //nolint:nilerr // best-effort: a removed or unreadable segment is skipped, not fatal
			}
			for k, v := range sizes {
				out[k] += v
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, err
		}
	}
	return out, nil
}
