package segment

import (
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
)

// storedRec is one merged document's id and body, indexed by its new ordinal.
type storedRec struct {
	id   string
	body []byte
}

// Merge combines inputs into one new segment in dir, dropping every document that is
// not live: set in its reader's corresponding entry of deletes (which may be nil for an
// input with no deletions). Document ordinals are reassigned: input 0's live documents
// come first, in their original order, then input 1's, and so on. The result holds
// exactly the terms, postings, doc values and stored fields a fresh [Build] of those
// same live documents would, by construction (both write the same fieldBuilder data
// through the same section writers); Merge only ever reads from inputs and never
// rewrites them.
//
// Merge has no [BuildOptions] of its own (its signature is fixed), so it parallelizes
// itself, up to GOMAXPROCS: inputs are split into contiguous, ascending groups of
// readers, one per worker, each merged into its own fieldBuilder per field exactly as
// a single-worker Merge would merge all of inputs; the groups are then merged with
// everything else [writeSegmentParts] merges parts with, so Merge is parallel the same
// way, and for the same reason, [Build] is.
func Merge(dir string, inputs []*Reader, deletes []*roaring.Bitmap) (Meta, error) {
	remaps := make([][]int32, len(inputs))
	var total uint32
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
	if uint64(total) > math.MaxUint32 {
		return Meta{}, fmt.Errorf("segment: merge produces %d documents, over the uint32 ordinal space", total)
	}

	recs := make([]storedRec, total)
	groups := splitReaderRanges(len(inputs), runtime.GOMAXPROCS(0))
	parts := make([]map[string]*fieldBuilder, len(groups))
	errs := make([]error, len(groups))
	mergeGroups(inputs, remaps, recs, groups, parts, errs)
	for _, err := range errs {
		if err != nil {
			return Meta{}, err
		}
	}

	names := unionFieldNames(parts)
	name := genName()
	path := filepath.Join(dir, name+FileExt)
	meta, err := writeSegmentParts(path, total, names, parts, storedFromSlice(func(ord uint32) (string, []byte) {
		return recs[ord].id, recs[ord].body
	}, total))
	if err != nil {
		return Meta{}, err
	}
	meta.ID = name
	return meta, nil
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
func mergeGroups(inputs []*Reader, remaps [][]int32, recs []storedRec, groups []readerRange, parts []map[string]*fieldBuilder, errs []error) {
	run := func(i int, rg readerRange) {
		parts[i], errs[i] = mergeGroup(inputs[rg.start:rg.end], remaps[rg.start:rg.end], recs)
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
// fieldBuilder per field they use, writing each live document's stored record
// straight into its slot of the shared recs (disjoint across every call: readers'
// remapped ordinals never overlap between groups), and sorts each builder's term
// dictionaries ready for the merge-and-write pass.
func mergeGroup(readers []*Reader, remaps [][]int32, recs []storedRec) (map[string]*fieldBuilder, error) {
	fieldSet := map[string]bool{}
	for _, r := range readers {
		for name := range r.fields {
			fieldSet[name] = true
		}
	}
	names := make([]string, 0, len(fieldSet))
	for name := range fieldSet {
		names = append(names, name)
	}

	builders := make(map[string]*fieldBuilder, len(names))
	for _, name := range names {
		builders[name] = newFieldBuilder()
	}

	for ri, r := range readers {
		remap := remaps[ri]
		views := fieldViewsFor(r, names)
		for oldOrd := uint32(0); oldOrd < r.numDocs; oldOrd++ {
			newOrd := remap[oldOrd]
			if newOrd < 0 {
				continue
			}
			id, body, err := r.storedRecord(oldOrd)
			if err != nil {
				return nil, err
			}
			recs[newOrd] = storedRec{id: id, body: append([]byte(nil), body...)}
			for _, name := range names {
				fv := views[name]
				if fv == nil {
					continue
				}
				mergeDoc(builders[name], fv, oldOrd, uint32(newOrd))
			}
		}
		for _, name := range names {
			fi := r.fields[name]
			if fi == nil {
				continue
			}
			mergeTermsOnly(builders[name], fi, r.data, remap, KindWord)
			mergeTermsOnly(builders[name], fi, r.data, remap, KindGram)
		}
	}
	for _, b := range builders {
		b.sortTermGroups()
	}
	return builders, nil
}

func bitmapOrEmpty(deletes []*roaring.Bitmap, i int) *roaring.Bitmap {
	if i >= len(deletes) || deletes[i] == nil {
		return roaring.New()
	}
	return deletes[i]
}

func storedFromSlice(at func(ord uint32) (string, []byte), total uint32) storedSource {
	return func(add func(ord uint32, id string, body []byte) error) error {
		for ord := range total {
			id, body := at(ord)
			if err := add(ord, id, body); err != nil {
				return err
			}
		}
		return nil
	}
}

// fieldViews is one reader's one field, with its presence and (for a bool-style field,
// with no keyword column) true/false membership bitmaps resolved once per reader
// instead of once per document.
type fieldViews struct {
	fi        *fieldInfo
	presence  *roaring.Bitmap
	truncated *roaring.Bitmap
	trueBM    *roaring.Bitmap
	falseBM   *roaring.Bitmap
}

func fieldViewsFor(r *Reader, names []string) map[string]*fieldViews {
	out := make(map[string]*fieldViews, len(names))
	for _, name := range names {
		fi := r.fields[name]
		if fi == nil {
			continue
		}
		fv := &fieldViews{fi: fi, presence: viewBitmap(r.data, fi.presRegion)}
		if fi.truncRegion.n > 0 {
			fv.truncated = viewBitmap(r.data, fi.truncRegion)
		}
		if fi.keywordCol == nil && fi.dicts[KindValue] != nil {
			if info, ok := fi.dicts[KindValue].lookup(stringBytes(TermTrue)); ok {
				fv.trueBM = bitmapAt(r.data, info)
			}
			if info, ok := fi.dicts[KindValue].lookup(stringBytes(TermFalse)); ok {
				fv.falseBM = bitmapAt(r.data, info)
			}
		}
		out[name] = fv
	}
	return out
}

// mergeDoc folds one reader's document oldOrd into b under its new ordinal, using the
// per-document accessors doc values give (fast and exact), and bool bitmap membership
// for the one kind, bool, that has no doc-values column.
func mergeDoc(b *fieldBuilder, fv *fieldViews, oldOrd, newOrd uint32) {
	if !fv.presence.Contains(oldOrd) {
		return
	}
	b.presence.Add(newOrd)
	if fv.truncated != nil && fv.truncated.Contains(oldOrd) {
		b.truncated.Add(newOrd)
	}
	fi := fv.fi
	switch {
	case fi.keywordCol != nil:
		if ord, ok := (KeywordColumn{c: fi.keywordCol}).Ord(oldOrd); ok {
			b.addValueTerm(newOrd, string(fi.dicts[KindValue].termAt(nil, ord)))
			b.hasText = true
		}
	case fv.trueBM != nil && fv.trueBM.Contains(oldOrd):
		b.addValueTerm(newOrd, TermTrue)
	case fv.falseBM != nil && fv.falseBM.Contains(oldOrd):
		b.addValueTerm(newOrd, TermFalse)
	}
	if fi.multiCol != nil {
		for _, o := range (MultiColumn{c: fi.multiCol}).Ords(oldOrd, nil) {
			b.addEntryTerm(newOrd, string(fi.dicts[KindEntry].termAt(nil, o)))
		}
	}
	if fi.numberCol != nil {
		if v, ok := (NumericColumn{c: fi.numberCol}).Value(oldOrd); ok {
			b.numDocs = append(b.numDocs, docFloat{doc: newOrd, v: v})
		}
	}
}

// mergeTermsOnly merges one kind that has no doc-values column (word, gram): term by
// term, remapping and filtering its postings.
func mergeTermsOnly(b *fieldBuilder, fi *fieldInfo, data []byte, remap []int32, kind TermKind) {
	dict := fi.dicts[kind]
	if dict == nil {
		return
	}
	it := dict.iter(0)
	for it.next() {
		term := string(it.term)
		for _, old := range appendDocs(nil, data, it.info) {
			if int(old) >= len(remap) {
				continue
			}
			newOrd := remap[old]
			if newOrd < 0 {
				continue
			}
			if kind == KindWord {
				b.addWordTerm(uint32(newOrd), term)
			} else {
				b.addGramTerm(uint32(newOrd), term)
			}
		}
	}
}
