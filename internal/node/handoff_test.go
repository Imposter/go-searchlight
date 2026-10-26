package node

import (
	"bytes"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// A handoff gives each document once, by its seq, and refuses one at or below the last
// seq taken; full, it drops the documents below that seq (no copy takes them any more)
// before it refuses a new one.
func TestHandoffTakesOnceAndStaysBounded(t *testing.T) {
	var h handoff
	big := &schema.Doc{ID: "a", Body: bytes.Repeat([]byte("x"), maxHandoffBytes/handoffAmplification/4)}
	h.put(1, big, nil)
	h.put(2, big, nil)
	if d, _ := h.take(1); d != big {
		t.Fatal("seq 1 is not given")
	}
	if d, _ := h.take(1); d != nil {
		t.Fatal("seq 1 is given twice")
	}
	h.put(3, big, nil)
	h.put(4, big, nil)
	h.put(5, big, nil)
	if d, _ := h.take(5); d != big {
		t.Fatal("seq 5 is not given")
	}
	h.put(6, big, nil)
	h.put(7, big, nil)
	if d, _ := h.take(6); d != big {
		t.Fatal("seq 6 is not given")
	}
	if d, _ := h.take(7); d != big {
		t.Fatal("the handoff did not make room below the last seq taken")
	}
	h.put(7, big, nil)
	if len(h.docs) != 0 || h.bytes != 0 {
		t.Fatalf("left %d documents, %d bytes", len(h.docs), h.bytes)
	}
	for seq := int64(10); seq < 20; seq++ {
		h.put(seq, big, nil)
	}
	if h.bytes > maxHandoffBytes {
		t.Fatalf("holds %d bytes, over %d", h.bytes, maxHandoffBytes)
	}
}
