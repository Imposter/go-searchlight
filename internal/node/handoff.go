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
	docs  map[int64]*schema.Doc
	bytes int64
	// taken is the highest seq a tailer asked for.
	taken int64
}

func docBytes(d *schema.Doc) int64 { return int64(len(d.Body)) * handoffAmplification }

// put offers the document committed at seq; it is dropped when the handoff is full.
func (h *handoff) put(seq int64, d *schema.Doc) {
	size := docBytes(d)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.bytes+size > maxHandoffBytes {
		for s, old := range h.docs {
			if s <= h.taken {
				delete(h.docs, s)
				h.bytes -= docBytes(old)
			}
		}
		if h.bytes+size > maxHandoffBytes {
			return
		}
	}
	if h.docs == nil {
		h.docs = map[int64]*schema.Doc{}
	}
	if old, ok := h.docs[seq]; ok {
		h.bytes -= docBytes(old)
	}
	h.docs[seq] = d
	h.bytes += size
}

// take returns and forgets the document committed at seq, nil when it has none.
func (h *handoff) take(seq int64) *schema.Doc {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.taken = max(h.taken, seq)
	d, ok := h.docs[seq]
	if !ok {
		return nil
	}
	delete(h.docs, seq)
	h.bytes -= docBytes(d)
	return d
}
