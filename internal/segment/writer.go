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
	// Threads is how many goroutines [Build] (and [Merge]) use for each segment's
	// per-field preparation: sorting its term pairs and choosing its numeric
	// encoding, which is where the CPU time goes for high-cardinality fields. 0 or 1
	// is sequential. The section writer itself - the part that advances the file's
	// byte offset and so must run in a fixed order - always runs single-threaded
	// afterward, in sorted field order, so the bytes Build writes are identical
	// regardless of Threads.
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

	builders := make(map[string]*fieldBuilder)
	get := func(name string) *fieldBuilder {
		b, ok := builders[name]
		if !ok {
			b = newFieldBuilder()
			builders[name] = b
		}
		return b
	}
	for ord := range docs {
		for name, v := range docs[ord].Fields {
			if !v.Present {
				continue
			}
			addValue(get(name), uint32(ord), v)
		}
	}
	names := fieldNames(builders)

	name := opts.Name
	if name == "" {
		name = genName()
	}
	path := filepath.Join(dir, name+FileExt)
	meta, err := writeSegment(path, numDocs, names, builders, storedFromDocs(docs))
	if err != nil {
		return Meta{}, err
	}
	meta.ID = name
	return meta, nil
}

// addValue folds one document's field value into its builder, as [Build] and the
// doc-major pass of [Merge] both do.
func addValue(b *fieldBuilder, ord uint32, v schema.Value) {
	b.presence.Add(ord)
	if v.GramsTruncated {
		b.truncated.Add(ord)
	}
	switch {
	case v.Text != nil:
		b.addValueTerm(ord, *v.Text)
		b.textDocs = append(b.textDocs, docString{doc: ord, s: *v.Text})
		if v.Words != "" {
			for _, word := range dedupeStrings(strings.Fields(v.Words)) {
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
		b.entriesDocs = append(b.entriesDocs, docEntries{doc: ord, es: v.Entries})
	}
}

func dedupeStrings(ss []string) []string {
	if len(ss) < 2 {
		return ss
	}
	seen := make(map[string]bool, len(ss))
	out := ss[:0]
	for _, s := range ss {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func fieldNames(builders map[string]*fieldBuilder) []string {
	names := make([]string, 0, len(builders))
	for name := range builders {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
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

// writeSegment writes every section from builders (by field name, in names's order,
// which must be sorted) and stored, to a fresh segment file at path.
func writeSegment(path string, numDocs uint32, names []string, builders map[string]*fieldBuilder, stored storedSource) (Meta, error) {
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

	outs := make(map[string]*fieldOutput, len(names))
	valueOrdsByField := make(map[string]map[string]uint32, len(names))
	entryOrdsByField := make(map[string]map[string]uint32, len(names))

	w.beginSection(sectionTerms)
	for _, name := range names {
		out := &fieldOutput{}
		outs[name] = out
		vo, eo := builders[name].writeDicts(w, out)
		valueOrdsByField[name] = vo
		entryOrdsByField[name] = eo
	}
	w.endSection()

	w.beginSection(sectionDocValues)
	for _, name := range names {
		builders[name].writeDocValues(w, numDocs, outs[name], valueOrdsByField[name], entryOrdsByField[name])
	}
	w.endSection()

	w.beginSection(sectionPoints)
	for _, name := range names {
		builders[name].writePointsSection(w, numDocs, outs[name])
	}
	w.endSection()

	w.beginSection(sectionPresence)
	for _, name := range names {
		builders[name].writePresence(w, outs[name])
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
