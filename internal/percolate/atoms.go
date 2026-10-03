package percolate

import (
	"bytes"
	"encoding/binary"
	"slices"

	"github.com/Imposter/go-searchlight/internal/analysis"
	"github.com/Imposter/go-searchlight/internal/schema"
)

// scratch is one worker's reusable per-document state: a candidate set (a bitmap over
// a segment's query ordinals, plus the list of the ordinals set, so it is cleared in
// time proportional to the candidates), a verification memo per class, and a buffer
// for trigram keys. A worker takes one from the pool per call and keeps it for every
// document it handles, so the steady state allocates nothing but results.
type scratch struct {
	bits  []uint64
	cands []uint32
	// memo holds a class representative's verdict for the current document: 0 unknown,
	// 1 match, 2 miss; memoSet lists the ordinals set.
	memo    []uint8
	memoSet []uint32
	keys    []uint64
	// matched are one segment's matches as rank<<32|ordinal; hits are the document's
	// matching ids so far (views into the segments), and buf the bytes results
	// copies them into.
	matched []uint64
	hits    [][]byte
	buf     []byte
	// members are the document's atoms that are pair members, encoded in memberBuf;
	// key is a buffer for probe keys.
	members   []member
	memberBuf []byte
	key       []byte
}

const (
	memoMatch = 1
	memoMiss  = 2
)

// fit sizes s for a segment of n queries.
func (s *scratch) fit(n uint32) {
	if words := int(n+63) / 64; len(s.bits) < words {
		s.bits = make([]uint64, words)
	}
	if len(s.memo) < int(n) {
		s.memo = make([]uint8, n)
	}
}

func (s *scratch) add(ord uint32) {
	w, bit := ord>>6, uint64(1)<<(ord&63)
	if s.bits[w]&bit == 0 {
		s.bits[w] |= bit
		s.cands = append(s.cands, ord)
	}
}

// addPosts adds every ordinal of a postings list (little-endian u32s).
func (s *scratch) addPosts(posts []byte) {
	for i := 0; i+4 <= len(posts); i += 4 {
		s.add(binary.LittleEndian.Uint32(posts[i:]))
	}
}

// reset clears the candidate set and the memo.
func (s *scratch) reset() {
	for _, ord := range s.cands {
		s.bits[ord>>6] = 0
	}
	s.cands = s.cands[:0]
	for _, ord := range s.memoSet {
		s.memo[ord] = 0
	}
	s.memoSet = s.memoSet[:0]
}

// collect adds to sc every query of the segment that d might match: every query
// anchored on an atom d holds, and the always-check list. sc must be fit to the
// segment and empty. The atoms are read exactly where the matcher reads them (see
// [AtomKind]), only for the kinds of atoms the segment has terms of on each field.
func (s *Segment) collect(d *schema.Doc, sc *scratch) {
	for fi := range s.fields {
		f := &s.fields[fi]
		v, ok := d.Fields[f.name]
		if !ok || !v.Present {
			continue // a missing field holds no atom: only Always queries match it
		}
		field := uint32(fi)
		k := f.kinds
		mem := k&(1<<AtomMember) != 0 // the field's atoms may be halves of pairs
		if k&(1<<AtomPresent) != 0 {
			sc.addPosts(s.lookup(AtomPresent, field, ""))
		}
		if v.Text != nil {
			text := *v.Text
			if k&(1<<AtomText) != 0 {
				sc.addPosts(s.lookup(AtomText, field, ""))
			}
			if k&(1<<AtomValue) != 0 {
				sc.addPosts(s.lookup(AtomValue, field, text))
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
						sc.addPosts(s.lookup(AtomGram, field, text[a:i]))
					}
					a, b, c = b, c, i
				}
				if a >= 0 {
					sc.addPosts(s.lookup(AtomGram, field, text[a:]))
				}
			}
			if k&(1<<AtomSimKey) != 0 {
				sc.keys = analysis.AppendTrigramKeys(sc.keys[:0], text)
				for _, key := range sc.keys {
					sc.addPosts(s.lookupKey(field, key))
				}
			}
		}
		if v.Bool != nil {
			if k&(1<<AtomBool) != 0 {
				sc.addPosts(s.lookup(AtomBool, field, boolTerm(*v.Bool)))
			}
			if mem {
				s.addMember(sc, AtomBool, field, boolTerm(*v.Bool))
			}
		}
		if len(v.Entries) > 0 {
			if k&(1<<AtomNonempty) != 0 {
				sc.addPosts(s.lookup(AtomNonempty, field, ""))
			}
			if k&(1<<AtomEntry) != 0 || mem {
				for _, e := range v.Entries {
					if k&(1<<AtomEntry) != 0 {
						sc.addPosts(s.lookup(AtomEntry, field, e))
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
						sc.addPosts(s.lookup(AtomWord, field, words[:i]))
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

// results returns the document's hits as strings sharing one allocation (sorted: they
// already are, else they are sorted here), and clears the hits.
func (s *scratch) results(sorted bool) []string {
	if len(s.hits) == 0 {
		return nil
	}
	s.buf = s.buf[:0]
	for _, h := range s.hits {
		s.buf = append(s.buf, h...)
	}
	all := string(s.buf)
	out := make([]string, len(s.hits))
	off := 0
	for i, h := range s.hits {
		out[i] = all[off : off+len(h)]
		off += len(h)
	}
	s.clearHits()
	if !sorted {
		slices.Sort(out)
	}
	return out
}

// clearHits drops the hits (views into segments, not kept past the call).
func (s *scratch) clearHits() {
	clear(s.hits)
	s.hits = s.hits[:0]
}

// member is a document atom that is a member of some pair: its encoding in
// scratch.memberBuf.
type member struct{ off, n int }

// addMember records atom (kind, field, term) of the document when the segment has it
// as a member of some pair.
func (s *Segment) addMember(sc *scratch, kind AtomKind, field uint32, term string) {
	sc.key = append(sc.key[:0], byte(kind))
	sc.key = append(sc.key, term...)
	if s.lookupBytes(AtomMember, field, sc.key) == nil {
		return
	}
	off := len(sc.memberBuf)
	sc.memberBuf = appendAtom(sc.memberBuf, kind, field, term)
	sc.members = append(sc.members, member{off: off, n: len(sc.memberBuf) - off})
}

// probePairs adds the queries anchored on a pair of the document's member atoms: every
// unordered pair of distinct members, each keyed in byte order as the builder keys it.
func (s *Segment) probePairs(sc *scratch) {
	if len(sc.members) < 2 {
		sc.members, sc.memberBuf = sc.members[:0], sc.memberBuf[:0]
		return
	}
	buf := sc.memberBuf
	enc := func(m member) []byte { return buf[m.off : m.off+m.n] }
	slices.SortFunc(sc.members, func(a, b member) int { return bytes.Compare(enc(a), enc(b)) })
	sc.members = slices.CompactFunc(sc.members, func(a, b member) bool { return bytes.Equal(enc(a), enc(b)) })
	for i, a := range sc.members {
		for _, b := range sc.members[i+1:] {
			sc.key = append(append(sc.key[:0], enc(a)...), enc(b)...)
			sc.addPosts(s.lookupBytes(AtomPair, 0, sc.key))
		}
	}
	sc.members, sc.memberBuf = sc.members[:0], sc.memberBuf[:0]
}
