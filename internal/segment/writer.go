package segment

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
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
	// Threads is how many goroutines [Build] uses to accumulate documents into
	// fields: 0 or 1 is sequential, one worker covering every document. A value
	// above 1 splits the documents into that many contiguous ordinal ranges, one per
	// worker, each building its own, independent fieldBuilder per field; the result
	// is then merged per field - a k-way merge of each worker's already-sorted term
	// dictionaries, and a concatenation, in range order, of its doc-values and
	// presence data - into the single dictionary and columns the file holds. That
	// merge, not the workers, is what the plan's "done when" build-throughput target
	// is measured against ([BuildOptions.Threads] set to every core), and it is the
	// same code for one worker as for many: the bytes Build writes are identical for
	// any Threads value, because a range's own data does not depend on how many
	// other ranges there are or what order they finish in, only on the (fixed, by
	// construction) order the ranges themselves cover the documents in.
	Threads int
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

	ranges := splitRanges(numDocs, opts.Threads)
	parts := buildPartsParallel(docs, ranges)
	names := unionFieldNames(parts)

	name := opts.Name
	if name == "" {
		name = genName()
	}
	path := filepath.Join(dir, name+FileExt)
	meta, err := writeSegmentParts(path, numDocs, names, parts, storedFromDocs(docs))
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

// writeSegmentParts writes every section, merging parts (by field name, in names's
// order, which must be sorted) and stored, to a fresh segment file at path. parts may
// have any length: one (a sequential build, or any Merge with one effective worker)
// or many (one per [BuildOptions.Threads] worker, or one per Merge reader group) -
// writeFieldDicts and friends treat those identically, which is what makes the file
// byte-for-byte the same either way.
func writeSegmentParts(path string, numDocs uint32, names []string, parts []map[string]*fieldBuilder, stored storedSource) (Meta, error) {
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

	w := newFileWriter(f)
	w.header()

	empty := newFieldBuilder()
	scratch := make(map[string]*fieldScratch, len(names))
	for _, name := range names {
		scratch[name] = newFieldScratch()
	}

	w.beginSection(sectionTerms)
	for _, name := range names {
		writeFieldDicts(w, partsFor(parts, name, empty), scratch[name])
	}
	w.endSection()

	w.beginSection(sectionDocValues)
	for _, name := range names {
		writeFieldDocValues(w, partsFor(parts, name, empty), numDocs, scratch[name])
	}
	w.endSection()

	w.beginSection(sectionPoints)
	for _, name := range names {
		writeFieldPoints(w, numDocs, scratch[name])
	}
	w.endSection()

	w.beginSection(sectionPresence)
	for _, name := range names {
		writeFieldPresence(w, partsFor(parts, name, empty), scratch[name].out)
	}
	w.endSection()

	w.beginSection(sectionStored)
	sw, err := newStoredWriter(w)
	if err != nil {
		return Meta{}, err
	}
	if err := stored(func(ord uint32, id string, body []byte) error {
		sw.add(ord, id, body)
		return nil
	}); err != nil {
		_ = sw.close()
		return Meta{}, err
	}
	storedIndexOff := sw.finish()
	if err := sw.close(); err != nil {
		return Meta{}, err
	}
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
	if err := f.Sync(); err != nil {
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
	// even though the new file's own bytes were already fsynced above.
	if err := fsyncDir(filepath.Dir(path)); err != nil {
		return Meta{}, err
	}
	return Meta{Path: path, NumDocs: numDocs}, nil
}
