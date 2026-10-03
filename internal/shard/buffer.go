package shard

import (
	"github.com/Imposter/go-searchlight/internal/schema"
)

// buffer is the write buffer: the newest version of every document and saved query
// applied since the last refresh, or a tombstone for one deleted. Each id appears once,
// so a refresh's segment holds each id at most once; the order is first-applied, which
// becomes the segment's ordinal order. It is guarded by Shard.mu while it is the
// active buffer, and owned by the refresh once frozen.
type buffer struct {
	docIdx  map[string]int
	docs    []docEntry
	qIdx    map[string]int
	queries []queryEntry
	live    int   // documents and queries that are not tombstones
	bytes   int64 // estimated memory
}

type docEntry struct {
	id  string
	seq int64
	doc *schema.Doc // nil: deleted
}

type queryEntry struct {
	id  string
	seq int64
	q   *StoredQuery // nil: deleted
}

func newBuffer() *buffer {
	return &buffer{docIdx: map[string]int{}, qIdx: map[string]int{}}
}

// empty reports whether the buffer holds no change at all.
func (b *buffer) empty() bool { return len(b.docs) == 0 && len(b.queries) == 0 }

// size is how many ids the buffer holds, tombstones included.
func (b *buffer) size() int { return len(b.docs) + len(b.queries) }

// putDoc records doc as id's newest version (nil: deleted).
func (b *buffer) putDoc(id string, seq int64, doc *schema.Doc) {
	e := docEntry{id: id, seq: seq, doc: doc}
	if i, ok := b.docIdx[id]; ok {
		old := &b.docs[i]
		b.account(docBytes(old.id, old.doc), old.doc != nil, -1)
		*old = e
	} else {
		b.docIdx[id] = len(b.docs)
		b.docs = append(b.docs, e)
	}
	b.account(docBytes(id, doc), doc != nil, 1)
}

// putQuery records q as id's newest version (nil: deleted).
func (b *buffer) putQuery(id string, seq int64, q *StoredQuery) {
	e := queryEntry{id: id, seq: seq, q: q}
	if i, ok := b.qIdx[id]; ok {
		old := &b.queries[i]
		b.account(queryBytes(old.id, old.q), old.q != nil, -1)
		*old = e
	} else {
		b.qIdx[id] = len(b.queries)
		b.queries = append(b.queries, e)
	}
	b.account(queryBytes(id, q), q != nil, 1)
}

func (b *buffer) account(bytes int64, live bool, sign int) {
	b.bytes += int64(sign) * bytes
	if live {
		b.live += sign
	}
}

// absorb overlays newer's entries onto b (newer wins), for putting a frozen buffer back
// in front of the one that replaced it when its refresh fails.
func (b *buffer) absorb(newer *buffer) {
	for _, e := range newer.docs {
		b.putDoc(e.id, e.seq, e.doc)
	}
	for _, e := range newer.queries {
		b.putQuery(e.id, e.seq, e.q)
	}
}

// liveDocs returns the buffer's documents (no tombstones) for segment.Build, in buffer
// order.
func (b *buffer) liveDocs() []schema.Doc {
	out := make([]schema.Doc, 0, len(b.docs))
	for i := range b.docs {
		if d := b.docs[i].doc; d != nil {
			out = append(out, *d)
		}
	}
	return out
}

// liveQueries returns the buffer's saved queries (no tombstones), in buffer order.
func (b *buffer) liveQueries() []StoredQuery {
	out := make([]StoredQuery, 0, len(b.queries))
	for i := range b.queries {
		if q := b.queries[i].q; q != nil {
			out = append(out, *q)
		}
	}
	return out
}

// docIDs returns every document id the buffer touched: each masks its older copies.
func (b *buffer) docIDs() []string {
	out := make([]string, len(b.docs))
	for i := range b.docs {
		out[i] = b.docs[i].id
	}
	return out
}

// queryIDs returns every saved query id the buffer touched.
func (b *buffer) queryIDs() []string {
	out := make([]string, len(b.queries))
	for i := range b.queries {
		out[i] = b.queries[i].id
	}
	return out
}

// docBytes estimates the memory a buffered document holds: its id and body, and its
// analyzed fields (a map entry and value each, plus their terms).
func docBytes(id string, d *schema.Doc) int64 {
	n := int64(len(id)) + 64
	if d == nil {
		return n
	}
	n += int64(len(d.Body))
	for name, v := range d.Fields {
		n += int64(len(name)) + 96
		if v.Text != nil {
			n += int64(len(*v.Text))
		}
		n += int64(len(v.Words))
		for _, e := range v.Entries {
			n += int64(len(e)) + 16
		}
		n += int64(len(v.Grams)) * 20
	}
	return n
}

func queryBytes(id string, q *StoredQuery) int64 {
	n := int64(len(id)) + 64
	if q != nil {
		n += int64(len(q.Meta)) + 512
	}
	return n
}
