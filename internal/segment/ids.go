package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"strings"
)

// The IDS section is the segment's primary key: each document's exact id, as given (the
// _id field holds it normalized, for queries), to its ordinal. It is one term
// dictionary in the TERMS layout whose every term has exactly one document, so each
// ordinal is inline in its term block and a lookup is the dictionary's own: a binary
// search of the block index and a scan of one block.

// DuplicateIDError is a [Build] or [Merge] whose documents hold one id twice: a segment
// holds at most one document per exact id.
type DuplicateIDError struct {
	ID string
}

func (e *DuplicateIDError) Error() string {
	return fmt.Sprintf("segment: document id %q appears more than once", e.ID)
}

// idOrd is one document's exact id and ordinal.
type idOrd struct {
	id  string
	ord uint32
}

// sortedIDs returns every document's id with its ordinal, sorted by id, or a
// *DuplicateIDError naming an id two documents share.
func sortedIDs(numDocs uint32, idAt func(ord uint32) string) ([]idOrd, error) {
	ids := make([]idOrd, numDocs)
	for ord := range numDocs {
		ids[ord] = idOrd{id: idAt(ord), ord: ord}
	}
	slices.SortFunc(ids, func(a, b idOrd) int { return strings.Compare(a.id, b.id) })
	for i := 1; i < len(ids); i++ {
		if ids[i].id == ids[i-1].id {
			return nil, &DuplicateIDError{ID: ids[i].id}
		}
	}
	return ids, nil
}

// writeIDs writes the IDS section's contents: the dictionary, then its index offset
// relative to the section's start, plus one (0: no documents). It refuses an id that
// ids yields twice with a *DuplicateIDError.
func writeIDs(w *fileWriter, ids idSource) error {
	sectionStart := w.off
	dw := newDictWriter(w, newPostingsEncoder())
	var one [1]uint32
	var prev []byte
	first := true
	err := ids(func(id []byte, ord uint32) error {
		if w.err != nil {
			return w.err
		}
		if !first && bytes.Equal(id, prev) {
			return &DuplicateIDError{ID: string(id)}
		}
		first = false
		prev = append(prev[:0], id...)
		one[0] = ord
		dw.add(id, one[:])
		return nil
	})
	if err != nil {
		return err
	}
	var trailer uint64
	if off, ok := dw.finish(); ok {
		trailer = off - sectionStart + 1
	}
	w.u64(trailer)
	return nil
}

// openIDs opens the IDS section sec: a dictionary of exactly numDocs ids, or none for
// a segment with no documents.
func (r *Reader) openIDs(sec sectionEntry) error {
	corrupt := func(why string) error { return &CorruptError{Path: r.path, Section: "ids", Reason: why} }
	if sec.n < 8 {
		return corrupt("section too short for its trailer")
	}
	rel := binary.LittleEndian.Uint64(r.data[sec.off+sec.n-8:])
	if rel == 0 {
		if r.numDocs != 0 {
			return corrupt(fmt.Sprintf("no ids for %d documents", r.numDocs))
		}
		return nil
	}
	if rel-1 >= sec.n-8 {
		return corrupt("dictionary offset outside the section")
	}
	dict, err := openDict(r.data, sec.off+rel-1)
	if err != nil {
		return corrupt(err.Error())
	}
	if dict.numTerms != r.numDocs || dict.sumDocFreq != uint64(r.numDocs) {
		return corrupt(fmt.Sprintf("%d ids (%d postings) for %d documents", dict.numTerms, dict.sumDocFreq, r.numDocs))
	}
	r.ids = dict
	return nil
}

// Ord returns the ordinal of the document whose id is exactly id (byte for byte, not
// normalized), false when the segment has none.
func (r *Reader) Ord(id string) (uint32, bool) {
	info, ok := r.ids.lookup(stringBytes(id))
	if !ok || info.docFreq != 1 || info.single >= r.numDocs {
		return 0, false
	}
	return info.single, true
}

// IDsFrom calls fn with every document id at or after from (exact ids, byte order, as
// [Reader.Ord] takes them) and its ordinal, ascending, until fn returns false or the
// ids run out: the segment's documents in id order, deleted ones included. id is a
// buffer the next call overwrites: copy it to keep it.
func (r *Reader) IDsFrom(from string, fn func(id []byte, ord uint32) bool) {
	if r.ids == nil {
		return
	}
	fb := stringBytes(from)
	block, ok := r.ids.blockFor(fb)
	if !ok {
		block = 0
	}
	it := r.ids.iter(block)
	for it.next() {
		if bytes.Compare(it.term, fb) < 0 {
			continue
		}
		if !fn(it.term, it.info.single) {
			return
		}
	}
}

// IDAt returns the i-th smallest document id (exact, byte order; deleted documents
// included), false past the last: with a rank from an [Reader.IDsFrom] walk, an id
// without reading the stored record.
func (r *Reader) IDAt(i uint32) (string, bool) {
	if r.ids == nil || i >= r.ids.numTerms {
		return "", false
	}
	return string(r.ids.termAt(nil, i)), true
}
