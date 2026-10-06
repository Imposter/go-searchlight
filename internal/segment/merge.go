package segment

import (
	"bytes"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
)

// MergeOptions configures [Merge].
type MergeOptions struct {
	// Name is the merged segment's file stem, as [BuildOptions.Name]; a random one is
	// generated when empty. The same uniqueness rule applies.
	Name string
	// Threads bounds how many goroutines Merge uses, in both its accumulation and its
	// writing phase (as [BuildOptions.Threads] does for Build). 0 means GOMAXPROCS.
	Threads int
	// Throttle, when set, is called with each chunk's size before it is written to the
	// merged file or to a temp file holding a field until its turn, and with 0 every
	// mergeCheckDocs input documents; never from two goroutines at once. A shard's I/O
	// budget sleeps in it. An error from it (a cancelled merge) stops the merge at its
	// next check - within mergeCheckDocs documents while reading the inputs, within
	// failCheckTerms terms of a dictionary, before the next field or the next stored
	// record while writing - and Merge removes its temp files and returns that error.
	Throttle func(n int) error
	// NoDirSync is [BuildOptions.NoDirSync].
	NoDirSync bool
	// MarksUntyped is [BuildOptions.MarksUntyped]: set it only when every input marks
	// untyped values (for a format-3 input, when whoever wrote it did).
	MarksUntyped bool
}

// mergeCheckDocs is how many input documents a merge reads between Throttle(0) calls.
const mergeCheckDocs = 4096

// Merge combines inputs into one new segment in dir, dropping every document that is
// not live: set in its reader's corresponding entry of deletes (which may be nil for an
// input with no deletions). Document ordinals are reassigned: input 0's live documents
// come first, in their original order, then input 1's, and so on. The result holds
// exactly the terms, postings, doc values and stored fields a fresh [Build] of those
// same live documents would, by construction (both write the same fieldBuilder data
// through the same section writers), always in the current format whatever the
// inputs'; Merge only ever reads from inputs and never rewrites them.
//
// Merge parallelizes itself up to opts.Threads (GOMAXPROCS when 0): inputs are split
// into contiguous, ascending groups of readers, one per worker, each merged into its
// own fieldBuilder per field exactly as a single-worker Merge would merge all of
// inputs; the groups are then merged with everything else [writeSegmentParts] merges
// parts with, so Merge is parallel the same way, and for the same reason, [Build] is.
//
// Merge holds no document and no posting in memory: each field's term dictionaries are
// a merge of the inputs' (already sorted) dictionaries, and stored records and ids
// stream from the inputs, in order, as the merged file is written. What it accumulates
// is per document: presence and marks, numbers, and each value and entry ordinal.
func Merge(dir string, inputs []*Reader, deletes []*roaring.Bitmap, opts MergeOptions) (Meta, error) {
	threads := opts.Threads
	if threads < 1 {
		threads = runtime.GOMAXPROCS(0)
	}
	remaps := make([][]int32, len(inputs))
	var total uint64
	for i, r := range inputs {
		del := bitmapOrEmpty(deletes, i)
		remap := make([]int32, r.numDocs)
		for ord := range remap {
			if del.Contains(uint32(ord)) {
				remap[ord] = -1
				continue
			}
			remap[ord] = int32(total)
			total++
		}
		remaps[i] = remap
	}
	if total > math.MaxUint32 {
		return Meta{}, fmt.Errorf("segment: merge produces %d documents, over the uint32 ordinal space", total)
	}

	groups := splitReaderRanges(len(inputs), threads)
	parts := make([]map[string]*fieldBuilder, len(groups))
	errs := make([]error, len(groups))
	var check func(int) error
	if opts.Throttle != nil {
		var mu sync.Mutex
		check = func(n int) error {
			mu.Lock()
			defer mu.Unlock()
			return opts.Throttle(n)
		}
	}
	mergeGroups(inputs, remaps, groups, parts, errs, check)
	for _, err := range errs {
		if err != nil {
			return Meta{}, err
		}
	}

	names := liveFieldNames(parts)
	name := opts.Name
	if name == "" {
		name = genName()
	}
	var flags uint32
	if opts.MarksUntyped {
		flags |= flagMarksUntyped
	}
	path := filepath.Join(dir, name+FileExt)
	meta, err := writeSegmentParts(path, uint32(total), flags, names, parts, readersDicts(inputs, remaps), storedFromReaders(inputs, remaps),
		idsFromReaders(inputs, remaps), threads, check, true, !opts.NoDirSync)
	if err != nil {
		return Meta{}, err
	}
	meta.ID = name
	return meta, nil
}

// liveFieldNames is [unionFieldNames] restricted to fields at least one live document
// has. mergeGroup starts a builder for every field its readers know, so a field whose
// every document was deleted still has one, but empty; a [Build] of the same live
// documents never sees that field at all, so leaving it in would add a META entry (and
// an empty presence bitmap) that a rebuild does not have.
func liveFieldNames(parts []map[string]*fieldBuilder) []string {
	names := unionFieldNames(parts)
	live := names[:0]
	for _, name := range names {
		for _, m := range parts {
			if b, ok := m[name]; ok && !b.presence.IsEmpty() {
				live = append(live, name)
				break
			}
		}
	}
	return live
}

// readerRange is a contiguous, half-open range of input-reader indices: one worker's
// share of a [Merge].
type readerRange struct{ start, end int }

// splitReaderRanges divides [0, n) readers into workers contiguous, ascending ranges
// (as [splitRanges] does for document ordinals). Always returns at least one range.
func splitReaderRanges(n, workers int) []readerRange {
	switch {
	case workers < 1:
		workers = 1
	case n == 0:
		workers = 1
	case workers > n:
		workers = n
	}
	ranges := make([]readerRange, workers)
	base, rem := n/workers, n%workers
	start := 0
	for i := range ranges {
		size := base
		if i < rem {
			size++
		}
		ranges[i] = readerRange{start: start, end: start + size}
		start += size
	}
	return ranges
}

// mergeGroups merges each of groups' readers into parts[i] (and errs[i], if it
// fails), one goroutine per group when there is more than one.
func mergeGroups(inputs []*Reader, remaps [][]int32, groups []readerRange, parts []map[string]*fieldBuilder, errs []error, check func(int) error) {
	run := func(i int, rg readerRange) {
		parts[i], errs[i] = mergeGroup(inputs[rg.start:rg.end], remaps[rg.start:rg.end], check)
	}
	if len(groups) == 1 {
		run(0, groups[0])
		return
	}
	var wg sync.WaitGroup
	for i, rg := range groups {
		wg.Add(1)
		go func(i int, rg readerRange) {
			defer wg.Done()
			run(i, rg)
		}(i, rg)
	}
	wg.Wait()
}

// mergeGroup merges readers (a contiguous slice of Merge's inputs) into one
// fieldBuilder per field they use: every per-document structure but the terms. check,
// when set, is called with 0 every mergeCheckDocs documents, and its error aborts the
// merge.
func mergeGroup(readers []*Reader, remaps [][]int32, check func(int) error) (map[string]*fieldBuilder, error) {
	fieldSet := map[string]bool{}
	for _, r := range readers {
		for name := range r.fields {
			fieldSet[name] = true
		}
	}
	builders := make(map[string]*fieldBuilder, len(fieldSet))
	for name := range fieldSet {
		builders[name] = newFieldBuilder()
	}

	seen := 0
	for ri, r := range readers {
		remap := remaps[ri]
		views := fieldViewsFor(r)
		for oldOrd := uint32(0); oldOrd < r.numDocs; oldOrd++ {
			seen++
			if check != nil && seen%mergeCheckDocs == 0 {
				if err := check(0); err != nil {
					return nil, err
				}
			}
			newOrd := remap[oldOrd]
			if newOrd < 0 {
				continue
			}
			for name, fv := range views {
				mergeDoc(builders[name], fv, oldOrd, uint32(newOrd))
			}
		}
		if check != nil {
			if err := check(0); err != nil {
				return nil, err
			}
		}
	}
	return builders, nil
}

func bitmapOrEmpty(deletes []*roaring.Bitmap, i int) *roaring.Bitmap {
	if i >= len(deletes) || deletes[i] == nil {
		return roaring.New()
	}
	return deletes[i]
}

// storedFromReaders streams every live document's stored record from inputs, in merge
// order, each input read through a cache of its own, so a merge neither evicts nor
// waits on the blocks searches are fetching hits from.
func storedFromReaders(inputs []*Reader, remaps [][]int32) storedSource {
	return func(add func(ord uint32, id string, body []byte) error) error {
		for i, r := range inputs {
			cache, err := newStoredCache(&r.stored)
			if err != nil {
				return err
			}
			for oldOrd, newOrd := range remaps[i] {
				if newOrd < 0 {
					continue
				}
				b, ok := r.stored.blockFor(uint32(oldOrd))
				if !ok {
					cache.close()
					return &CorruptError{Path: r.path, Section: "stored", Reason: "no block holds that ordinal"}
				}
				id, body, err := cache.record(r.data, b, uint32(oldOrd))
				if err != nil {
					cache.close()
					return &CorruptError{Path: r.path, Section: "stored", Reason: err.Error()}
				}
				if err := add(uint32(newOrd), id, body); err != nil {
					cache.close()
					return err
				}
			}
			cache.close()
		}
		return nil
	}
}

// idsFromReaders streams every live document's id with its merged ordinal, ascending by
// id: a merge of the inputs' IDS dictionaries, each already sorted.
func idsFromReaders(inputs []*Reader, remaps [][]int32) idSource {
	return func(yield func(id []byte, ord uint32) error) error {
		type cursor struct {
			it    *termIter
			remap []int32
			ok    bool
		}
		advance := func(c *cursor) {
			for c.ok = c.it.next(); c.ok; c.ok = c.it.next() {
				if old := c.it.info.single; c.it.info.docFreq == 1 && old < uint32(len(c.remap)) && c.remap[old] >= 0 { //nolint:gosec // remap has one entry per ordinal
					return
				}
			}
		}
		cursors := make([]*cursor, 0, len(inputs))
		for i, r := range inputs {
			if r.ids == nil {
				continue
			}
			c := &cursor{it: r.ids.iter(0), remap: remaps[i]}
			advance(c)
			cursors = append(cursors, c)
		}
		for {
			var best *cursor
			for _, c := range cursors {
				if c.ok && (best == nil || bytes.Compare(c.it.term, best.it.term) < 0) {
					best = c
				}
			}
			if best == nil {
				return nil
			}
			if err := yield(best.it.term, uint32(best.remap[best.it.info.single])); err != nil { //nolint:gosec // a live ordinal, non-negative
				return err
			}
			advance(best)
		}
	}
}

// fieldViews is one reader's one field, with its presence, truncated and untyped
// bitmaps resolved once per reader instead of once per document.
type fieldViews struct {
	fi        *fieldInfo
	presence  *roaring.Bitmap
	truncated *roaring.Bitmap
	untyped   *roaring.Bitmap
}

func fieldViewsFor(r *Reader) map[string]*fieldViews {
	out := make(map[string]*fieldViews, len(r.fields))
	for name, fi := range r.fields {
		fv := &fieldViews{fi: fi, presence: viewBitmap(r.data, fi.presRegion)}
		if r.major < 4 {
			fv.untyped, fv.truncated = fi.v3Marks, fi.v3Truncated
		} else {
			fv.truncated, fv.untyped = r.Truncated(name), r.Untyped(name)
		}
		out[name] = fv
	}
	return out
}

// mergeDoc folds one reader's document oldOrd into b under its new ordinal: its
// presence, truncated and untyped marks, its number, and whether it has text (a keyword
// value, which a Build of it would have seen as Value.Text). Terms are merged from the
// inputs' dictionaries as the file is written (readersDicts).
func mergeDoc(b *fieldBuilder, fv *fieldViews, oldOrd, newOrd uint32) {
	if !fv.presence.Contains(oldOrd) {
		return
	}
	b.presence.Add(newOrd)
	if fv.truncated.Contains(oldOrd) {
		b.truncated.Add(newOrd)
	}
	if fv.untyped.Contains(oldOrd) {
		b.untyped.Add(newOrd)
	}
	if kc := fv.fi.keywordCol; kc != nil && !b.hasText {
		_, b.hasText = (KeywordColumn{c: kc}).Ord(oldOrd)
	}
	if nc := fv.fi.numberCol; nc != nil {
		if v, ok := (NumericColumn{c: nc}).Value(oldOrd); ok {
			b.numDocs = append(b.numDocs, docFloat{doc: newOrd, v: v})
		}
	}
}

// readersDicts is [Merge]'s dictSources: each input's dictionary of the field and
// kind, in input order, remapped.
func readersDicts(inputs []*Reader, remaps [][]int32) dictSources {
	return func(name string, _ []*fieldBuilder, kind TermKind) []termSource {
		var out []termSource
		for i, r := range inputs {
			if fi := r.fields[name]; fi != nil && fi.dicts[kind] != nil {
				out = append(out, &readerSource{data: r.data, it: fi.dicts[kind].iter(0), remap: remaps[i]})
			}
		}
		return out
	}
}

// readerSource is one input's dictionary as a termSource: every term with at least one
// live document, its documents remapped to merged ordinals. A term's new ordinals are
// kept only while they keep ascending, which a merged dictionary requires: always so
// for a file this package wrote (its postings ascend, and remap preserves order), and
// enforced here so a damaged one cannot break that invariant, only lose documents.
// Documents at or past the input's own count are never visited (see [appendDocs]).
type readerSource struct {
	data  []byte
	it    *termIter
	remap []int32
	docs  []uint32
}

func (s *readerSource) next() bool {
	limit := uint32(len(s.remap)) //nolint:gosec // remap has one entry per document ordinal, a uint32
	for s.it.next() {
		s.docs = appendDocs(s.data, s.it.info, limit, s.docs[:0])
		live := s.docs[:0]
		last := int32(-1)
		for _, old := range s.docs {
			if newOrd := s.remap[old]; newOrd > last {
				live = append(live, uint32(newOrd)) //nolint:gosec // newOrd > last >= -1
				last = newOrd
			}
		}
		s.docs = live
		if len(live) > 0 {
			return true
		}
	}
	return false
}

func (s *readerSource) term() []byte                     { return s.it.term }
func (s *readerSource) appendDocs(dst []uint32) []uint32 { return append(dst, s.docs...) }
