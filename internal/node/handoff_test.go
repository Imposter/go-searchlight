package node

import (
	"bytes"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// A handoff gives each document once, by its seq; full, it drops the documents below
// the last seq taken (no copy takes them any more) before it refuses a new one.
func TestHandoffTakesOnceAndStaysBounded(t *testing.T) {
	var h handoff
	big := &schema.Doc{ID: "a", Body: bytes.Repeat([]byte("x"), maxHandoffBytes/handoffAmplification/4)}
	h.put(1, big)
	h.put(2, big)
	if h.take(1) != big || h.take(1) != nil {
		t.Fatal("seq 1 is not given exactly once")
	}
	h.put(3, big)
	h.put(4, big)
	h.put(5, big)
	if h.take(5) != big {
		t.Fatal("seq 5 is not given")
	}
	h.put(6, big)
	h.put(7, big)
	if h.take(6) != big || h.take(7) != big {
		t.Fatal("the handoff did not make room below the last seq taken")
	}
	if len(h.docs) != 0 || h.bytes != 0 {
		t.Fatalf("left %d documents, %d bytes", len(h.docs), h.bytes)
	}
	for seq := int64(10); seq < 20; seq++ {
		h.put(seq, big)
	}
	if h.bytes > maxHandoffBytes {
		t.Fatalf("holds %d bytes, over %d", h.bytes, maxHandoffBytes)
	}
}
