package percolate

import (
	"bytes"
	"encoding/binary"
	stdbits "math/bits"
	"slices"
	"unsafe"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// scratch is one worker's reusable per-document state, kept for every document the
// worker handles so that the steady state allocates nothing but results:
//
//   - the candidates, a bitmap over a segment's ranks and the list of ranks set (so it
//     clears in time proportional to them);
//   - a verdict per verification class (memoMatch or memoMiss), and the classes set;
//   - the document's values of the segment's fields, by field index, which programs
//     read, and each field's trigram keys once a similar condition has needed them;
//   - the matching ids so far, JSON string literals viewing the segments, in runs
//     sorted by id (unsorted when a run is not), and the buffers results merges into;
//   - the pair members the document holds (see probePairs).
type scratch struct {
	bits     []uint64
	cands    []uint32
	memo     []uint8
	memoSet  []uint32
	keys     []uint64
	vals     []schema.Value
	simKeys  [][]uint64
	simDone  []bool
	simSet   []int
	want     []uint64
	hits     [][]byte
	runs     []int
	unsorted bool
	buf      []byte
	merger   merger
	held     []uint32
	heldBits []uint64
	pairOps  int
	key      []byte
}

const (
	memoMatch = 1
	memoMiss  = 2
)

// fit sizes s for segments of at most n queries, entries dictionary terms and fields
// fields.
func (s *scratch) fit(n, entries uint32, fields int) {
	if words := int(entries+63) / 64; len(s.heldBits) < words {
		s.heldBits = make([]uint64, words)
	}
	if words := int(n+63) / 64; len(s.bits) < words {
		s.bits = make([]uint64, words)
	}
	if len(s.memo) < int(n) {
		s.memo = make([]uint8, n)
	}
	if len(s.vals) < fields {
		s.vals = make([]schema.Value, fields)
		s.simKeys = make([][]uint64, fields)
		s.simDone = make([]bool, fields)
	}
}

func (s *scratch) add(ord uint32) {
	w, bit := ord>>6, uint64(1)<<(ord&63)
	if s.bits[w]&bit == 0 {
		s.bits[w] |= bit
		s.cands = append(s.cands, ord)
	}
}

// addPosts adds every rank of a postings list (little-endian u32s).
func (s *scratch) addPosts(posts []byte) {
	for i := 0; i+4 <= len(posts); i += 4 {
		s.add(binary.LittleEndian.Uint32(posts[i:]))
	}
}

// addEntry adds dictionary entry e's queries (none for -1): its postings, and its
// filtered postings whose filter the document's values pass.
func (s *scratch) addEntry(seg *Segment, e int) {
	if e < 0 {
		return
	}
	s.addPosts(seg.posts(e))
	recs := seg.filteredOf(e)
	for r := 0; r+filteredSize <= len(recs); r += filteredSize {
		fb := binary.LittleEndian.Uint32(recs[r+4:])
		if passes(&s.vals[fb>>2], uint8(fb&3), recs[r+8:r+filteredSize]) {
			s.add(binary.LittleEndian.Uint32(recs[r:]))
		}
	}
}

// passes reports whether v passes a stored filter of kind (see postFilter), its 16
// bytes f.
func passes(v *schema.Value, kind uint8, f []byte) bool {
	switch kind {
	case filterRange:
		return v.Number != nil && f64(f, 0) <= *v.Number && *v.Number <= f64(f, 8)
	case filterBool:
		return v.Bool != nil && boolNumber(*v.Bool) == f64(f, 0)
	case filterTextNe:
		text := f[1 : 1+f[0]]
		return !v.Present || v.Text == nil || *v.Text != string(text)
	default:
		text := unsafe.String(unsafe.SliceData(f[1:]), int(f[0]))
		_, found := slices.BinarySearch(v.Entries, text)
		return !v.Present || !found
	}
}

// reset clears the candidates, the verdicts and the document's values (so a pooled
// scratch pins no document).
func (s *scratch) reset() {
	for _, ord := range s.cands {
		s.bits[ord>>6] = 0
	}
	s.cands = s.cands[:0]
	for _, ord := range s.memoSet {
		s.memo[ord] = 0
	}
	s.memoSet = s.memoSet[:0]
	for _, f := range s.simSet {
		s.simDone[f] = false
		s.simKeys[f] = s.simKeys[f][:0]
	}
	s.simSet = s.simSet[:0]
	clear(s.vals)
}

// inOrder puts the candidates in ascending order, so verification reads the segment's
// per-query records and programs front to back: by sorting a few, or by reading many
// back from the bitmap (words long).
func (s *scratch) inOrder(words int) {
	if len(s.cands)*16 < words {
		slices.Sort(s.cands)
		return
	}
	s.cands = appendSet(s.cands[:0], s.bits[:words])
}

// appendSet appends the positions of bits' set bits, in order.
func appendSet(dst []uint32, bits []uint64) []uint32 {
	for w, word := range bits {
		for word != 0 {
			dst = append(dst, uint32(w)<<6|uint32(stdbits.TrailingZeros64(word))) //nolint:gosec // a bit position within a u32 range
			word &= word - 1
		}
	}
	return dst
}

// collect adds to sc every query of the segment that d might match: every query
// anchored on an atom d holds, and the always-check list. sc must be fit to the
// segment and empty. The atoms are read exactly where the matcher reads them (see
// [AtomKind]), only for the kinds of atoms the segment has terms of on each field.
func (s *Segment) collect(d *schema.Doc, sc *scratch) {
	for fi := range s.fields {
		sc.vals[fi] = d.Fields[s.fields[fi].name]
	}
	for fi := range s.fields {
		f := &s.fields[fi]
		v := &sc.vals[fi]
		if !v.Present {
			continue // a missing field holds no atom: only Always queries match it
		}
		field := uint32(fi)
		k := f.kinds
		mem := k&(1<<AtomMember) != 0 // the field's atoms may be halves of pairs
		if k&(1<<AtomPresent) != 0 {
			sc.addEntry(s, s.lookup(AtomPresent, field, ""))
		}
		if v.Text != nil {
			text := *v.Text
			if k&(1<<AtomText) != 0 {
				sc.addEntry(s, s.lookup(AtomText, field, ""))
			}
			if k&(1<<AtomValue) != 0 {
				sc.addEntry(s, s.lookup(AtomValue, field, text))
			}
			if mem {
				s.addMember(sc, AtomValue, field, text)
			}
			if k&(1<<AtomGram) != 0 {
				// Every 3-rune window of the whole text: Value.Grams stops at
				// MaxGramChars (and is never filled for _id), the windows do not.
				a, b, c := -1, -1, -1
				for i := range text {
					if a >= 0 {
						sc.addEntry(s, s.lookupGram(f, field, text[a:i]))
					}
					a, b, c = b, c, i
				}
				if a >= 0 {
					sc.addEntry(s, s.lookupGram(f, field, text[a:]))
				}
			}
			if k&(1<<AtomSimKey) != 0 {
				sc.keys = analysis.AppendTrigramKeys(sc.keys[:0], text)
				for _, key := range sc.keys {
					sc.addEntry(s, s.lookupKey(field, key))
				}
			}
		}
		if v.Bool != nil {
			if k&(1<<AtomBool) != 0 {
				sc.addEntry(s, s.lookup(AtomBool, field, boolTerm(*v.Bool)))
			}
			if mem {
				s.addMember(sc, AtomBool, field, boolTerm(*v.Bool))
			}
		}
		if len(v.Entries) > 0 {
			if k&(1<<AtomNonempty) != 0 {
				sc.addEntry(s, s.lookup(AtomNonempty, field, ""))
			}
			if k&(1<<AtomEntry) != 0 || mem {
				for _, e := range v.Entries {
					if k&(1<<AtomEntry) != 0 {
						sc.addEntry(s, s.lookup(AtomEntry, field, e))
					}
					if mem {
						s.addMember(sc, AtomEntry, field, e)
					}
				}
			}
		}
		if v.Words != "" && (k&(1<<AtomWord) != 0 || mem) {
			words := v.Words
			for words != "" {
				i := 0
				for i < len(words) && words[i] != ' ' {
					i++
				}
				if i > 0 {
					if k&(1<<AtomWord) != 0 {
						sc.addEntry(s, s.lookup(AtomWord, field, words[:i]))
					}
					if mem {
						s.addMember(sc, AtomWord, field, words[:i])
					}
				}
				if i == len(words) {
					break
				}
				words = words[i+1:]
			}
		}
		if v.Number != nil && f.root >= 0 {
			s.stab(f.root, *v.Number, sc)
		}
	}
	s.probePairs(sc)
	sc.addPosts(s.always)
}

// stab adds every query whose range on the field (the tree at root) holds x.
func (s *Segment) stab(root int32, x float64, sc *scratch) {
	for nd := root; nd >= 0; {
		o := int(nd) * nodeSize
		center := f64(s.nodes, o)
		start, count := int(u32(s.nodes, o+16)), int(u32(s.nodes, o+20))
		switch {
		case x < center:
			// Every interval here holds center, so hi >= center > x: it holds x when
			// lo <= x, a prefix of the lo-ascending order.
			for r := start; r < start+count; r++ {
				p := r * recordSize
				if f64(s.byLo, p) > x {
					break
				}
				sc.add(u32(s.byLo, p+16))
			}
			nd = signed(u32(s.nodes, o+8))
		case x > center:
			for r := start; r < start+count; r++ {
				p := r * recordSize
				if f64(s.byHi, p+8) < x {
					break
				}
				sc.add(u32(s.byHi, p+16))
			}
			nd = signed(u32(s.nodes, o+12))
		default:
			for r := start; r < start+count; r++ {
				sc.add(u32(s.byLo, r*recordSize+16))
			}
			return
		}
	}
}

// run marks the hits from start on as one run sorted by id (none if empty).
func (s *scratch) run(start int) {
	if start < len(s.hits) {
		s.runs = append(s.runs, start)
	}
}

// results returns the document's hits as one JSON array (nil for none), the runs
// merged (or all sorted, when one is not), and clears the hits.
func (s *scratch) results() IDs {
	if len(s.hits) == 0 {
		s.clearHits()
		return nil
	}
	if s.unsorted {
		slices.SortFunc(s.hits, idCompare)
		s.runs = append(s.runs[:0], 0)
	}
	s.buf = s.merger.merge(s.buf[:0], s.hits, s.runs, false)
	s.clearHits()
	return IDs(bytes.Clone(s.buf))
}

// clearHits drops the hits (views into segments, not kept past the call).
func (s *scratch) clearHits() {
	clear(s.hits)
	s.hits = s.hits[:0]
	s.runs, s.unsorted = s.runs[:0], false
}

// Pair probing limits (variables so tests can force each path). A document walks the
// partner lists of the member atoms it holds when that is cheap: within pairWalkBudget
// records, or within candidateWeight records per candidate the fallback would add.
// Otherwise it adds every query any member it holds is half of (the members'
// postings), which is linear in those postings and sound with no threshold at all: a
// query anchored on a pair the document holds has the pair's halves among its
// members.
var (
	pairWalkBudget  = 4096
	candidateWeight = 32
)

// addMember records the document's atom (kind, field, term) when the segment has it
// as half of some pair.
func (s *Segment) addMember(sc *scratch, kind AtomKind, field uint32, term string) {
	sc.key = append(sc.key[:0], byte(kind))
	sc.key = append(sc.key, term...)
	e := s.findEntry(AtomMember, field, sc.key)
	if e < 0 {
		return
	}
	w, bit := e>>6, uint64(1)<<(e&63)
	if sc.heldBits[w]&bit == 0 {
		sc.heldBits[w] |= bit
		sc.held = append(sc.held, count32(e))
	}
}

// probePairs adds the queries anchored on a pair both of whose halves the document
// holds. Each pair is stored once, under one half (its owner), with the other as its
// partner: walking the partner lists of the held members, and keeping the partners
// held, finds every such pair in time linear in those lists, never quadratic in the
// members. When the lists are long (see pairWalkBudget) it adds the held members'
// postings instead. pairOps counts the records walked or postings added.
func (s *Segment) probePairs(sc *scratch) {
	if len(sc.held) >= 2 {
		walk, fallback := 0, 0
		for _, e := range sc.held {
			_, n := s.partnersOf(int(e))
			walk += n
			fallback += len(s.entryPosts(int(e))) / 4
		}
		if walk <= pairWalkBudget || walk <= candidateWeight*fallback {
			for _, e := range sc.held {
				first, n := s.partnersOf(int(e))
				for r := first; r < first+n; r++ {
					partner, posts := s.pairAt(r)
					if sc.heldBits[partner>>6]&(1<<(partner&63)) != 0 {
						sc.addPosts(posts)
					}
				}
			}
			sc.pairOps += walk
		} else {
			for _, e := range sc.held {
				sc.addPosts(s.entryPosts(int(e)))
			}
			sc.pairOps += fallback
		}
	}
	for _, e := range sc.held {
		sc.heldBits[e>>6] = 0
	}
	sc.held = sc.held[:0]
}
