package node

import (
	"bytes"
	"testing"

	"github.com/Imposter/go-searchlight/internal/schema"
)

// A handoff gives each document once, by its seq; taking a seq forgets every document
// below it, a document at or below the last seq taken is refused, and the handoffs of
// a node hold at most maxHandoffBytes together, giving the bytes back as they go.
func TestHandoffTakesInOrderWithinItsBudget(t *testing.T) {
	var budget handoffBudget
	a, b := handoff{budget: &budget}, handoff{budget: &budget}
	doc := &schema.Doc{ID: "d", Body: []byte(`{"k":"v"}`)}
	for _, seq := range []int64{3, 1, 2, 5} {
		a.put(seq, doc, nil)
	}
	if d, _ := a.take(2); d != doc || a.held() != 2 {
		t.Fatalf("take(2) = %v, %d held; want the document and 3 and 5 held", d, a.held())
	}
	if d, _ := a.take(4); d != nil || a.held() != 1 {
		t.Fatalf("take(4) = %v, %d held; want none and 5 held", d, a.held())
	}
	a.put(4, doc, nil)
	if a.held() != 1 {
		t.Fatal("a document below the last seq taken was kept")
	}
	if d, _ := a.take(5); d != doc || a.held() != 0 || budget.used.Load() != 0 {
		t.Fatalf("take(5) = %v; %d held, %d bytes", d, a.held(), budget.used.Load())
	}

	big := &schema.Doc{ID: "big", Body: bytes.Repeat([]byte("x"), maxHandoffBytes/3)}
	a.put(10, big, nil)
	b.put(10, big, nil)
	a.put(11, big, nil)
	if a.held()+b.held() != 2 || budget.used.Load() > maxHandoffBytes {
		t.Fatalf("held %d and %d, %d bytes: the node's budget does not bound them", a.held(), b.held(), budget.used.Load())
	}
	b.take(11)
	a.put(11, big, nil)
	if a.held() != 2 {
		t.Fatal("bytes given back were not reused")
	}
	a.clear()
	if a.held() != 0 || budget.used.Load() != 0 {
		t.Fatalf("after clear: %d held, %d bytes", a.held(), budget.used.Load())
	}
}

// docHeap counts a document's fields and nested objects, not only its body.
func TestDocHeapCountsFieldsAndObjects(t *testing.T) {
	m := &schema.Mapping{Fields: map[string]schema.FieldType{"t": schema.Text, "_v": schema.Nested, "_v.s": schema.Keyword}}
	flat, _, err := schema.Analyze(m, "a", []byte(`{"t": "oak chair"}`))
	if err != nil {
		t.Fatal(err)
	}
	nested, _, err := schema.Analyze(m, "a", []byte(`{"t": "oak chair", "_v": [{"s": "l"}, {"s": "m"}, {"s": "s"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if hf, hn := docHeap(&flat), docHeap(&nested); hf <= int64(len(flat.Body)) || hn < hf+3*(objectHeap+valueHeap) {
		t.Fatalf("docHeap %d flat, %d with three objects", hf, hn)
	}
}
