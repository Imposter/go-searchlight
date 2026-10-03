package segment

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// BuildOptions configures [Build].
type BuildOptions struct {
	// Name is the segment's file stem, without extension. A random one is generated
	// when empty. An explicit Name must be unique for as long as a segment of that
	// name might still be open anywhere in the process (or on the machine, since the
	// file is memory-mapped): Build writes to a fresh temp file and renames it into
	// place, but renaming over a file Windows currently has mapped fails with
	// "Access is denied", where POSIX would silently replace it out from under any
	// open mapping. Prefer leaving Name empty unless the caller already guarantees
	// the name is retired (for example, a merge's inputs, which by the time Merge
	// returns are no longer the live segment set).
	Name string
	// Threads bounds how many goroutines Build uses, in two separate phases:
	//
	//  1. Accumulation: 0 or 1 is sequential, one worker covering every document. A
	//     value above 1 splits the documents into that many contiguous ordinal
	//     ranges, one per worker, each building its own, independent fieldBuilder
	//     per field.
	//  2. Writing: each section (terms, doc values, points, presence) merges and
	//     writes every field's contribution in parallel, up to Threads workers -
	//     made possible by every offset a field's structures bake in being relative
	//     to that structure's own start (format.go), so one field's dictionary,
	//     column or point index can be built, complete and self-contained, in its
	//     own buffer independently of every other field's. Stored fields compress
	//     their ~16 KB blocks in parallel the same way, then concatenate.
	//
	// Both phases produce identical bytes for any Threads value: accumulation merges
	// ranges by a k-way merge of each worker's already-sorted term groups and a
	// concatenation, in range order, of its doc-values and presence data, which does
	// not depend on how many ranges there are; writing fixes up each field's section-
	// relative offsets by the field's own position within the section once its
	// buffer is placed, which does not depend on how many fields were written
	// concurrently. TestBuildThreadsByteIdentical checks this directly.
	Threads int
	// NoDirSync skips the directory fsync after the file is renamed into place: the
	// file's own bytes are still fsynced, but the rename is durable only once the
	// caller syncs the directory ([SyncDir]). A shard does that once per commit, for
	// every file it wrote, instead of once per file.
	NoDirSync bool
}

// Meta describes a written segment: enough for a shard's manifest entry.
type Meta struct {
	// ID is the segment's name, without directory or extension.
	ID string
	// Path is the segment file's full path.
	Path string
	// NumDocs is how many document ordinals the segment holds (live and deleted).
	NumDocs uint32
}

// genName returns a random segment name: a 16-byte identifier, hex-encoded.
func genName() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand failing means the OS's RNG is broken
	}
	return hex.EncodeToString(b[:])
}

// docRange is a contiguous, half-open range of document ordinals: one worker's share
// of a [Build], or one group of a [Merge]'s input readers.
type docRange struct{ start, end uint32 }

// splitRanges divides [0, numDocs) into workers contiguous, ascending, as-equal-as-
// possible ranges (the last few one document larger, if numDocs does not divide
// evenly). Always returns at least one range, even for numDocs == 0 (an empty one),
// so callers never need to special-case "no documents" separately from "one worker".
func splitRanges(numDocs uint32, workers int) []docRange {
	switch {
	case workers < 1:
		workers = 1
	case numDocs == 0:
		workers = 1
	case uint32(workers) > numDocs: //nolint:gosec // numDocs > 0 here, so this comparison is meaningful
		workers = int(numDocs)
	}
	ranges := make([]docRange, workers)
	base := numDocs / uint32(workers)
	rem := numDocs % uint32(workers)
	var start uint32
	for i := range ranges {
		size := base
		if uint32(i) < rem {
			size++
		}
		ranges[i] = docRange{start: start, end: start + size}
		start += size
	}
	return ranges
}

// Build writes docs as one immutable segment file in dir and returns its [Meta]. Each
// document's position in docs is its ordinal in the segment (0 to len(docs)-1), stable
// for the segment's life.
//
// Build writes to a temp file, fsyncs it, then renames it into place, so a reader never
// observes a partially written segment.
func Build(dir string, docs []schema.Doc, opts BuildOptions) (Meta, error) {
	if uint64(len(docs)) > math.MaxUint32 {
		return Meta{}, fmt.Errorf("segment: %d documents exceeds the uint32 ordinal space", len(docs))
	}
	numDocs := uint32(len(docs)) //nolint:gosec // checked above

	ids, err := sortedIDs(numDocs, func(ord uint32) string { return docs[ord].ID })
	if err != nil {
		return Meta{}, err
	}
	ranges := splitRanges(numDocs, opts.Threads)
	parts := buildPartsParallel(docs, ranges)
	names := unionFieldNames(parts)

	name := opts.Name
	if name == "" {
		name = genName()
	}
	path := filepath.Join(dir, name+FileExt)
	meta, err := writeSegmentParts(path, numDocs, names, parts, storedFromDocs(docs), ids, opts.Threads, nil, !opts.NoDirSync)
	if err != nil {
		return Meta{}, err
	}
	meta.ID = name
	return meta, nil
}

// buildPartsParallel builds one part (a field name to fieldBuilder map) per range,
// each in its own goroutine when there is more than one range.
func buildPartsParallel(docs []schema.Doc, ranges []docRange) []map[string]*fieldBuilder {
	parts := make([]map[string]*fieldBuilder, len(ranges))
	if len(ranges) == 1 {
		parts[0] = buildPart(docs, ranges[0])
		return parts
	}
	var wg sync.WaitGroup
	for i, rg := range ranges {
		wg.Add(1)
		go func(i int, rg docRange) {
			defer wg.Done()
			parts[i] = buildPart(docs, rg)
		}(i, rg)
	}
	wg.Wait()
	return parts
}

// buildPart accumulates rg's documents into one fieldBuilder per field they use, and
// sorts each one's term dictionaries ready for the merge-and-write pass.
func buildPart(docs []schema.Doc, rg docRange) map[string]*fieldBuilder {
	builders := make(map[string]*fieldBuilder)
	for ord := rg.start; ord < rg.end; ord++ {
		for name, v := range docs[ord].Fields {
			if !v.Present {
				continue
			}
			b, ok := builders[name]
			if !ok {
				b = newFieldBuilder()
				builders[name] = b
			}
			addValue(b, ord, v)
		}
	}
	for _, b := range builders {
		b.sortTermGroups()
	}
	return builders
}

// addValue folds one document's field value into its builder, as [Build] and
// [mergeDoc] both do (through their own, analogous per-value cases: this one reads a
// [schema.Value] directly, where Merge reads a reader's already-analyzed columns). A
// duplicate (term, doc) pair - the same word or gram occurring more than once in one
// document's text - is harmless to add more than once: [termPairs.add] collapses it
// into one posting, so there is no need to deduplicate here.
func addValue(b *fieldBuilder, ord uint32, v schema.Value) {
	b.presence.Add(ord)
	if v.GramsTruncated {
		b.truncated.Add(ord)
	}
	switch {
	case v.Text != nil:
		b.addValueTerm(ord, *v.Text)
		b.hasText = true
		if v.Words != "" {
			for _, word := range strings.Fields(v.Words) {
				b.addWordTerm(ord, word)
			}
		}
		for _, g := range v.Grams {
			b.addGramTerm(ord, g)
		}
	case v.Number != nil:
		b.numDocs = append(b.numDocs, docFloat{doc: ord, v: *v.Number})
	case v.Bool != nil:
		term := TermFalse
		if *v.Bool {
			term = TermTrue
		}
		b.addValueTerm(ord, term)
	case v.Entries != nil:
		for _, e := range v.Entries {
			b.addEntryTerm(ord, e)
		}
	}
}

// unionFieldNames returns every field name any part has a builder for, sorted: the
// field directory's order, and the order [writeSegmentParts] visits fields in.
func unionFieldNames(parts []map[string]*fieldBuilder) []string {
	set := make(map[string]bool)
	for _, m := range parts {
		for name := range m {
			set[name] = true
		}
	}
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// partsFor returns one *fieldBuilder per part for field name, substituting empty for
// any part that built nothing for it (a field some ranges' documents never used).
func partsFor(parts []map[string]*fieldBuilder, name string, empty *fieldBuilder) []*fieldBuilder {
	out := make([]*fieldBuilder, len(parts))
	for i, m := range parts {
		if b, ok := m[name]; ok {
			out[i] = b
		} else {
			out[i] = empty
		}
	}
	return out
}

// storedSource yields every live document's id and body, by ascending ordinal.
type storedSource func(add func(ord uint32, id string, body []byte) error) error

func storedFromDocs(docs []schema.Doc) storedSource {
	return func(add func(ord uint32, id string, body []byte) error) error {
		for ord := range docs {
			if err := add(uint32(ord), docs[ord].ID, docs[ord].Body); err != nil {
				return err
			}
		}
		return nil
	}
}

// runParallel calls work(i) for every i in [0, n), using up to threads goroutines (0
// or 1: plain sequential loop, no goroutines spawned at all).
func runParallel(n, threads int, work func(i int)) {
	if threads < 2 || n < 2 {
		for i := range n {
			work(i)
		}
		return
	}
	if threads > n {
		threads = n
	}
	idx := make(chan int)
	var wg sync.WaitGroup
	for range threads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				work(i)
			}
		}()
	}
	for i := range n {
		idx <- i
	}
	close(idx)
	wg.Wait()
}

// writeFieldSectionParallel writes one section's per-field contributions: writeOne(fw,
// name) builds name's whole contribution into its own private buffer fw (offset
// starting at 0, exactly as if it were the only field in the section), for every name
// in names, up to threads of those running at once; the buffers are then written into
// w, in field order (so the file is identical regardless of how many ran
// concurrently), and fixup(name, fieldBase) is called for each, with fieldBase its
// buffer's offset from the section's own start, to turn the 0-based offsets writeOne
// left in that field's fieldOutput into offsets relative to the section (format.go).
func writeFieldSectionParallel(w *fileWriter, names []string, threads int, writeOne func(fw *fileWriter, name string), fixup func(name string, fieldBase uint64)) {
	sectionStart := w.off
	bufs := make([][]byte, len(names))
	runParallel(len(names), threads, func(i int) {
		var buf bytes.Buffer
		fw := newFileWriter(&buf)
		writeOne(fw, names[i])
		// fileWriter only flushes its internal chunk to its target when the chunk
		// fills (or a section boundary asks for it) - neither of which a small
		// private buffer ever hits on its own, so without this, buf stays empty.
		fw.flush()
		bufs[i] = buf.Bytes()
	})
	for i, name := range names {
		fieldBase := w.off - sectionStart
		w.write(bufs[i])
		fixup(name, fieldBase)
	}
}

// writeSegmentParts writes every section, merging parts (by field name, in names's
// order, which must be sorted) and stored, to a fresh segment file at path. parts may
// have any length: one (a sequential build, or any Merge with one effective worker)
// or many (one per [BuildOptions.Threads] worker, or one per Merge reader group) -
// writeFieldDicts and friends treat those identically, which is what makes the file
// byte-for-byte the same either way; so does threads, the degree of parallelism the
// writing phase itself (as opposed to parts, accumulation's) uses. throttle, when not
// nil, is called before every chunk written to the file ([MergeOptions.Throttle]);
// syncDir fsyncs the directory after the rename ([BuildOptions.NoDirSync]).
func writeSegmentParts(path string, numDocs uint32, names []string, parts []map[string]*fieldBuilder, stored storedSource, ids []idOrd, threads int, throttle func(n int) error, syncDir bool) (Meta, error) {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return Meta{}, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()

	var out io.Writer = f
	if throttle != nil {
		out = throttledWriter{w: f, throttle: throttle}
	}
	w := newFileWriter(out)
	w.header()

	empty := newFieldBuilder()
	scratch := make(map[string]*fieldScratch, len(names))
	for _, name := range names {
		scratch[name] = newFieldScratch()
	}

	w.beginSection(sectionTerms)
	writeFieldSectionParallel(w, names, threads,
		func(fw *fileWriter, name string) { writeFieldDicts(fw, partsFor(parts, name, empty), scratch[name]) },
		func(name string, fieldBase uint64) {
			out := scratch[name].out
			for k := range numKinds {
				if out.dictOff[k] != 0 {
					out.dictOff[k] += fieldBase
				}
			}
		})
	w.endSection()

	w.beginSection(sectionDocValues)
	writeFieldSectionParallel(w, names, threads,
		func(fw *fileWriter, name string) {
			writeFieldDocValues(fw, partsFor(parts, name, empty), numDocs, scratch[name])
		},
		func(name string, fieldBase uint64) {
			out := scratch[name].out
			if out.keywordColOff != 0 {
				out.keywordColOff += fieldBase
			}
			if out.multiColOff != 0 {
				out.multiColOff += fieldBase
			}
			if out.numberColOff != 0 {
				out.numberColOff += fieldBase
			}
		})
	w.endSection()

	w.beginSection(sectionPoints)
	writeFieldSectionParallel(w, names, threads,
		func(fw *fileWriter, name string) { writeFieldPoints(fw, numDocs, scratch[name]) },
		func(name string, fieldBase uint64) {
			out := scratch[name].out
			if out.pointsOff != 0 {
				out.pointsOff += fieldBase
			}
		})
	w.endSection()

	w.beginSection(sectionPresence)
	writeFieldSectionParallel(w, names, threads,
		func(fw *fileWriter, name string) {
			writeFieldPresence(fw, partsFor(parts, name, empty), scratch[name].out)
		},
		func(name string, fieldBase uint64) {
			out := scratch[name].out
			out.presOff += fieldBase
			if out.truncOff != 0 {
				out.truncOff += fieldBase
			}
		})
	w.endSection()

	w.beginSection(sectionStored)
	storedIndexOff, err := writeStoredParallel(w, stored, threads)
	if err != nil {
		return Meta{}, err
	}
	w.endSection()

	w.beginSection(sectionIDs)
	writeIDs(w, ids)
	w.endSection()

	w.beginSection(sectionMeta)
	outs := make(map[string]*fieldOutput, len(names))
	for _, name := range names {
		outs[name] = scratch[name].out
	}
	writeMeta(w, numDocs, storedIndexOff, names, outs)
	w.endSection()

	w.footer()
	if w.err != nil {
		return Meta{}, w.err
	}
	if err := SyncFile(f); err != nil {
		return Meta{}, err
	}
	if err := f.Close(); err != nil {
		return Meta{}, err
	}
	ok = true
	if err := os.Rename(tmp, path); err != nil {
		return Meta{}, err
	}
	// The rename is only durable once the directory entry itself is fsynced: without
	// this, a crash can leave the directory pointing at the old file (or nothing),
	// even though the new file's own bytes were already fsynced above. A caller that
	// syncs the directory itself, once for many files, skips this one.
	if syncDir {
		if err := SyncDir(filepath.Dir(path)); err != nil {
			return Meta{}, err
		}
	}
	return Meta{Path: path, NumDocs: numDocs}, nil
}

// throttledWriter calls throttle before each write to w, failing the write with its
// error.
type throttledWriter struct {
	w        io.Writer
	throttle func(n int) error
}

func (t throttledWriter) Write(p []byte) (int, error) {
	if err := t.throttle(len(p)); err != nil {
		return 0, err
	}
	return t.w.Write(p)
}
