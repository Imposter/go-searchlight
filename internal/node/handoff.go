package node

import (
	"slices"
	"sync"
	"sync/atomic"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// maxHandoffBytes bounds the analyzed documents every shard's handoff on a node holds
// together, by docHeap's estimate. Past it a write's documents are not handed off, and
// the tailer analyzes them itself.
const maxHandoffBytes = 64 << 20

// The heap an analyzed document takes beyond its strings, by docHeap: each field's or
// object's Value and its map entry, and each object's map.
const (
	valueHeap  = 192
	objectHeap = 96
)

// docHeap estimates the heap d takes: its body, and per field and nested object their
// values and map entries, with their text, words and entries.
func docHeap(d *schema.Doc) int64 {
	n := int64(len(d.Body)) + int64(len(d.ID))
	var value func(v *schema.Value)
	value = func(v *schema.Value) {
		n += valueHeap + int64(len(v.Words)) + int64(len(v.Points))*16
		if v.Text != nil {
			n += int64(len(*v.Text))
		}
		for _, e := range v.Entries {
			n += int64(len(e)) + 16
		}
		for o := range v.Objects {
			n += objectHeap
			fields := v.Objects[o].Fields
			for name := range fields {
				sub := fields[name]
				n += int64(len(name))
				value(&sub)
			}
		}
	}
	for name := range d.Fields {
		v := d.Fields[name]
		n += int64(len(name))
		value(&v)
	}
	return n
}

// handoffBudget is the bytes every handoff of a node holds together.
type handoffBudget struct{ used atomic.Int64 }

// reserve takes n bytes, or reports that they do not fit.
func (b *handoffBudget) reserve(n int64) bool {
	for {
		cur := b.used.Load()
		if cur+n > maxHandoffBytes {
			return false
		}
		if b.used.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

// handoff passes the documents a write on this node analyzed to the node's copy of
// their shard, by seq, so the copy's tailer applies them without analyzing their
// bodies again: each document is analyzed once per write on the node that commits it.
// The tailer takes a document only when it is the change's (same id and body bytes)
// and was analyzed under the types the copy maps (replica.Options.Analyzed); otherwise
// it analyzes the body as before. A copy applies its shard's changes in seq order, so
// taking seq drops every document below it, and a document at or below the last seq
// taken is refused.
type handoff struct {
	budget *handoffBudget

	mu    sync.Mutex
	docs  map[int64]handedDoc
	order []int64 // the seqs docs holds, ascending
	// taken is the highest seq a tailer asked for.
	taken int64
}

// handedDoc is an analyzed document, the mapping it was analyzed under, and its heap.
type handedDoc struct {
	doc   *schema.Doc
	under *schema.Mapping
	bytes int64
}

// put offers the document committed at seq, analyzed under mapping under; it is dropped
// when the copy has asked for seq already, or the node's handoffs are full.
func (h *handoff) put(seq int64, d *schema.Doc, under *schema.Mapping) {
	size := docHeap(d)
	h.mu.Lock()
	defer h.mu.Unlock()
	if seq <= h.taken || h.budget == nil {
		return
	}
	if _, ok := h.docs[seq]; ok || !h.budget.reserve(size) {
		return
	}
	if h.docs == nil {
		h.docs = map[int64]handedDoc{}
	}
	h.docs[seq] = handedDoc{doc: d, under: under, bytes: size}
	i, _ := slices.BinarySearch(h.order, seq)
	h.order = slices.Insert(h.order, i, seq)
}

// take returns and forgets the document committed at seq and the mapping it was
// analyzed under (nil when it has none), and forgets every document below seq.
func (h *handoff) take(seq int64) (*schema.Doc, *schema.Mapping) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.taken = max(h.taken, seq)
	drop := 0
	for drop < len(h.order) && h.order[drop] < seq {
		h.forget(h.order[drop])
		drop++
	}
	h.order = h.order[drop:]
	if len(h.order) == 0 || h.order[0] != seq {
		return nil, nil
	}
	d := h.docs[seq]
	h.forget(seq)
	h.order = h.order[1:]
	return d.doc, d.under
}

// forget drops seq's document and gives back its bytes; the caller holds h.mu.
func (h *handoff) forget(seq int64) {
	h.budget.used.Add(-h.docs[seq].bytes)
	delete(h.docs, seq)
}

// clear forgets every document, as the copy goes away (its index stopped, or the copy
// unhosted): a copy hosted later starts from its own seq.
func (h *handoff) clear() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, seq := range h.order {
		h.forget(seq)
	}
	h.order = nil
}

// held is the documents h holds.
func (h *handoff) held() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.docs)
}
