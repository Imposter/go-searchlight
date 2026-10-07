package percolate

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"
	"unicode/utf8"
)

// IDs are one document's matching saved-query ids, sorted, written as the JSON array
// of strings encoding/json would write for them, so a response copies them as they
// are. Nil is no match.
type IDs []byte

// Strings decodes ids.
func (ids IDs) Strings() ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal(ids, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// MergeIDs returns the union of lists (each sorted, as Percolate returns them), sorted,
// each id once; a lone list is returned as it is.
func MergeIDs(lists []IDs) (IDs, error) {
	var nonEmpty []IDs
	for _, l := range lists {
		if len(l) > 0 {
			nonEmpty = append(nonEmpty, l)
		}
	}
	switch len(nonEmpty) {
	case 0:
		return nil, nil
	case 1:
		return nonEmpty[0], nil
	}
	var lits [][]byte
	var runs []int
	for _, l := range nonEmpty {
		start := len(lits)
		var err error
		if lits, err = appendLiterals(lits, l); err != nil {
			return nil, err
		}
		if len(lits) > start {
			runs = append(runs, start)
		}
	}
	var m merger
	return IDs(m.merge(nil, lits, runs, true)), nil
}

// appendLiterals appends the string literals of the JSON array a.
func appendLiterals(dst [][]byte, a []byte) ([][]byte, error) {
	errBad := errors.New("percolate: not a JSON array of strings")
	a = bytes.TrimSpace(a)
	if len(a) < 2 || a[0] != '[' || a[len(a)-1] != ']' {
		return dst, errBad
	}
	a = bytes.TrimSpace(a[1 : len(a)-1])
	for len(a) > 0 {
		if a[0] != '"' {
			return dst, errBad
		}
		end := 1
		for end < len(a) && a[end] != '"' {
			if a[end] == '\\' {
				end++
			}
			end++
		}
		if end >= len(a) {
			return dst, errBad
		}
		dst = append(dst, a[:end+1])
		a = bytes.TrimSpace(a[end+1:])
		if len(a) > 0 {
			if a[0] != ',' {
				return dst, errBad
			}
			a = bytes.TrimSpace(a[1:])
		}
	}
	return dst, nil
}

// merger merges sorted runs of id literals, or of ranks; its buffers are reused across
// merges.
type merger struct {
	raws      [][]byte
	keys      []uint64
	pos, ends []int
	heap      []int
	heads     []runHead
}

// rankRun is one query segment's matches of a document: ranks[start:end] of the
// scratch, ascending, so sorted by id.
type rankRun struct {
	seg        *Segment
	start, end int
}

// runHead is a run's next match while runs merge: its position, and its id and that
// id's key.
type runHead struct {
	seg      *Segment
	pos, end int
	key      uint64
	raw      []byte
}

// load reads the id of the match h is at.
func (h *runHead) load(ranks []uint32) {
	h.raw = h.seg.rawIDAt(ranks[h.pos])
	if len(h.raw) >= 8 {
		h.key = binary.BigEndian.Uint64(h.raw)
	} else {
		h.key = idKey(h.raw)
	}
}

func (h *runHead) less(o *runHead) bool {
	if h.key != o.key {
		return h.key < o.key
	}
	return bytes.Compare(h.raw, o.raw) < 0
}

// below reports whether the id of the match at rank r of h's segment sorts before o's.
func (h *runHead) below(r uint32, o *runHead) bool {
	raw := h.seg.rawIDAt(r)
	var key uint64
	if len(raw) >= 8 {
		key = binary.BigEndian.Uint64(raw)
	} else {
		key = idKey(raw)
	}
	if key != o.key {
		return key < o.key
	}
	return bytes.Compare(raw, o.raw) < 0
}

// streak returns the first position after h's whose id does not sort before bound's
// (h.end when every one does), galloping: a streak of k ids costs about 2 log2(k)
// compares.
func (h *runHead) streak(ranks []uint32, bound *runHead) int {
	lo, step := h.pos, 1
	var hi int
	for {
		hi = lo + step
		if hi >= h.end {
			hi = h.end
			break
		}
		if !h.below(ranks[hi], bound) {
			break
		}
		lo, step = hi, 2*step
	}
	for hi-lo > 1 {
		mid := lo + (hi-lo)/2
		if h.below(ranks[mid], bound) {
			lo = mid
		} else {
			hi = mid
		}
	}
	return hi
}

// appendRanks appends to dst the JSON string literals of the ids of runs' ranks, each
// followed by a comma, sorted: each run is sorted by id and no id is in two runs, and
// they merge with a heap over the runs. The least run's ids below the next least
// run's are found by galloping and copied without touching the heap, so runs that
// interleave little (queries saved in about id order) cost a few compares per run, not
// one per id.
func (m *merger) appendRanks(dst []byte, ranks []uint32, runs []rankRun) []byte {
	m.heads, m.heap = m.heads[:0], m.heap[:0]
	for _, run := range runs {
		if run.start < run.end {
			h := runHead{seg: run.seg, pos: run.start, end: run.end}
			if len(runs) > 1 {
				h.load(ranks)
			}
			m.heap = append(m.heap, len(m.heads))
			m.heads = append(m.heads, h)
		}
	}
	for i := len(m.heap)/2 - 1; i >= 0; i-- {
		m.downHead(i)
	}
	for len(m.heap) > 1 {
		top := &m.heads[m.heap[0]]
		next := m.heap[1]
		if len(m.heap) > 2 && m.heads[m.heap[2]].less(&m.heads[next]) {
			next = m.heap[2]
		}
		stop := top.streak(ranks, &m.heads[next])
		for _, r := range ranks[top.pos:stop] {
			dst = append(append(dst, top.seg.idAt(r)...), ',')
		}
		top.pos = stop
		if stop == top.end {
			m.heap[0] = m.heap[len(m.heap)-1]
			m.heap = m.heap[:len(m.heap)-1]
		} else {
			top.load(ranks)
		}
		m.downHead(0)
	}
	if len(m.heap) == 1 {
		h := &m.heads[m.heap[0]]
		for _, r := range ranks[h.pos:h.end] {
			dst = append(append(dst, h.seg.idAt(r)...), ',')
		}
	}
	clear(m.heads)
	return dst
}

func (m *merger) downHead(i int) {
	h := m.heap
	for {
		c := 2*i + 1
		if c >= len(h) {
			return
		}
		if c+1 < len(h) && m.heads[h[c+1]].less(&m.heads[h[c]]) {
			c++
		}
		if !m.heads[h[c]].less(&m.heads[h[i]]) {
			return
		}
		h[i], h[c] = h[c], h[i]
		i = c
	}
}

// merge appends to dst the JSON array of the string literals lits, whose runs
// (starting at runs[i], ascending) are each sorted by the ids they spell: merged with a
// heap over the runs, in time linear in the literals times the log of the runs, and
// without repeats when dedupe is set.
func (m *merger) merge(dst []byte, lits [][]byte, runs []int, dedupe bool) []byte {
	dst = append(dst, '[')
	if len(runs) <= 1 {
		for i, l := range lits {
			if i > 0 {
				dst = append(dst, ',')
			}
			dst = append(dst, l...)
		}
		return append(dst, ']')
	}
	m.raws, m.keys = m.raws[:0], m.keys[:0]
	for _, l := range lits {
		raw, ok := plainID(l)
		if !ok {
			raw = decodeLiteral(l)
		}
		m.raws = append(m.raws, raw)
		m.keys = append(m.keys, idKey(raw))
	}
	m.pos, m.ends, m.heap = m.pos[:0], m.ends[:0], m.heap[:0]
	for i, start := range runs {
		end := len(lits)
		if i+1 < len(runs) {
			end = runs[i+1]
		}
		m.pos = append(m.pos, start)
		m.ends = append(m.ends, end)
		m.heap = append(m.heap, i)
	}
	for i := len(m.heap)/2 - 1; i >= 0; i-- {
		m.down(i)
	}
	last := -1
	for len(m.heap) > 0 {
		run := m.heap[0]
		at := m.pos[run]
		if !dedupe || last < 0 || m.compare(last, at) != 0 {
			if last >= 0 {
				dst = append(dst, ',')
			}
			dst = append(dst, lits[at]...)
			last = at
		}
		m.pos[run]++
		if m.pos[run] == m.ends[run] {
			m.heap[0] = m.heap[len(m.heap)-1]
			m.heap = m.heap[:len(m.heap)-1]
		}
		m.down(0)
	}
	clear(m.raws)
	return append(dst, ']')
}

// compare compares the ids of literals x and y.
func (m *merger) compare(x, y int) int {
	if kx, ky := m.keys[x], m.keys[y]; kx != ky {
		if kx < ky {
			return -1
		}
		return 1
	}
	return bytes.Compare(m.raws[x], m.raws[y])
}

func (m *merger) down(i int) {
	h := m.heap
	less := func(a, b int) bool { return m.compare(m.pos[h[a]], m.pos[h[b]]) < 0 }
	for {
		c := 2*i + 1
		if c >= len(h) {
			return
		}
		if c+1 < len(h) && less(c+1, c) {
			c++
		}
		if !less(c, i) {
			return
		}
		h[i], h[c] = h[c], h[i]
		i = c
	}
}

// idKey is an id's first eight bytes as a big-endian integer, zero-padded: ids order
// as their keys do when the keys differ (an id holds no NUL).
func idKey(raw []byte) uint64 {
	var b [8]byte
	copy(b[:], raw)
	return binary.BigEndian.Uint64(b[:])
}

// idCompare compares the ids two JSON string literals spell.
func idCompare(a, b []byte) int {
	ra, ok := plainID(a)
	if !ok {
		ra = decodeLiteral(a)
	}
	rb, ok := plainID(b)
	if !ok {
		rb = decodeLiteral(b)
	}
	if len(ra) >= 8 && len(rb) >= 8 {
		if x, y := binary.BigEndian.Uint64(ra), binary.BigEndian.Uint64(rb); x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return bytes.Compare(ra, rb)
}

// plainID returns the id a literal spells when it holds no escape (it is then the
// literal's inside), false when it does.
func plainID(lit []byte) ([]byte, bool) {
	inner := lit[1 : len(lit)-1]
	return inner, bytes.IndexByte(inner, '\\') < 0
}

// decodeLiteral returns the id an escaped literal spells.
func decodeLiteral(lit []byte) []byte {
	var s string
	if json.Unmarshal(lit, &s) != nil {
		return lit
	}
	return []byte(s)
}

// idLiteral is id's JSON string literal, as encoding/json writes it.
func idLiteral(id string) []byte {
	if !needsEscape(id) {
		return []byte(`"` + id + `"`)
	}
	lit, err := json.Marshal(id)
	if err != nil {
		return []byte(`""`)
	}
	return lit
}

// needsEscape reports whether encoding/json escapes anything in s: a quote, a
// backslash, a control character, an HTML-sensitive one, U+2028 or U+2029, or invalid
// UTF-8.
func needsEscape(s string) bool {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c < 0x20 || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			return true
		}
	}
	return !utf8.ValidString(s) || strings.ContainsRune(s, ' ') || strings.ContainsRune(s, ' ')
}

// isLiteralOf reports whether lit is exactly id's literal, as idLiteral writes it.
func isLiteralOf(lit, id []byte) bool {
	if !needsEscape(string(id)) {
		return len(lit) == len(id)+2 && lit[0] == '"' && lit[len(lit)-1] == '"' && bytes.Equal(lit[1:len(lit)-1], id)
	}
	return bytes.Equal(lit, idLiteral(string(id)))
}
