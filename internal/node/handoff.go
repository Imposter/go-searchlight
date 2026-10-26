package node

import (
	"sync"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// maxHandoffBytes bounds the documents one shard's handoff holds, by their bodies'
// size times handoffAmplification (what an analyzed document takes on the heap besides
// its body). Past it a write's documents are not handed off, and the tailer analyzes
// them itself.
const (
	maxHandoffBytes      = 32 << 20
	handoffAmplification = 3
)

// handoff passes the documents a write on this node analyzed to the node's copy of
// their shard, by seq, so the copy's tailer applies them without analyzing their
// bodies again: each document is analyzed once per write on the node that commits it.
// The tailer takes a document only when it is the change's (same id and body bytes)
// and was analyzed under fields the copy maps alike (replica.Options.Analyzed);
// otherwise it analyzes the body as before. A copy applies its shard's changes in seq
// order, so documents below the last one taken are never taken: they are dropped once
// the handoff is full.
type handoff struct {
	mu    sync.Mutex
	docs  map[int64]handedDoc
	bytes int64
	// taken is the highest seq a tailer asked for.
	taken int64
}

// handedDoc is an analyzed document and the mapping it was analyzed under.
type handedDoc struct {
	doc   *schema.Doc
	under *schema.Mapping
}

func docBytes(d *schema.Doc) int64 { return int64(len(d.Body)) * handoffAmplification }

// put offers the document committed at seq, analyzed under mapping under; it is dropped
// when the copy has asked for seq already, or the handoff is full.
func (h *handoff) put(seq int64, d *schema.Doc, under *schema.Mapping) {
	size := docBytes(d)
	h.mu.Lock()
	defer h.mu.Unlock()
	if seq <= h.taken {
		return
	}
	if h.bytes+size > maxHandoffBytes {
		for s, old := range h.docs {
			if s <= h.taken {
				delete(h.docs, s)
				h.bytes -= docBytes(old.doc)
			}
		}
		if h.bytes+size > maxHandoffBytes {
			return
		}
	}
	if h.docs == nil {
		h.docs = map[int64]handedDoc{}
	}
	if old, ok := h.docs[seq]; ok {
		h.bytes -= docBytes(old.doc)
	}
	h.docs[seq] = handedDoc{doc: d, under: under}
	h.bytes += size
}

// take returns and forgets the document committed at seq and the mapping it was
// analyzed under, nil when it has none.
func (h *handoff) take(seq int64) (*schema.Doc, *schema.Mapping) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.taken = max(h.taken, seq)
	d, ok := h.docs[seq]
	if !ok {
		return nil, nil
	}
	delete(h.docs, seq)
	h.bytes -= docBytes(d.doc)
	return d.doc, d.under
}
