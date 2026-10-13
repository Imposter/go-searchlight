package shard

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
)

// TestSeqMarkIsDurable: a mark recorded for a seq is the shard's once it applies or
// advances to that seq, survives a refresh and a flush in the manifest (seq_mark), and
// is the reopened shard's and a snapshot's. A mark recorded only after the seq was
// published still reaches the next flush; a mark for another seq is never reported.
func TestSeqMarkIsDurable(t *testing.T) {
	h := newHarness(t, testOptions())
	mark := json.RawMessage(`{"seq":3}`)
	h.s.SetMark(3, mark)
	if got := h.s.SeqMark(); got != nil {
		t.Fatalf("a mark for seq 3 at applied 0: %s", got)
	}
	h.upsert("a", "b", "c")
	if got := h.s.SeqMark(); string(got) != string(mark) {
		t.Fatalf("SeqMark at seq 3 = %s, want %s", got, mark)
	}
	h.commit()
	h.reopen()
	if got := h.s.SeqMark(); string(got) != string(mark) {
		t.Fatalf("after a reopen SeqMark = %s, want %s", got, mark)
	}

	if err := h.s.Advance(9); err != nil {
		t.Fatal(err)
	}
	h.commit()
	if got := h.s.SeqMark(); got != nil {
		t.Fatalf("at seq 9 with no mark for it: %s", got)
	}
	late := json.RawMessage(`{"seq":9}`)
	h.s.SetMark(9, late)
	h.commit()
	sn, err := h.s.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sn.Release()
	raw := sn.mem[manifestName]
	var man manifest
	if err := json.Unmarshal(raw[bytes.IndexByte(raw, '\n')+1:], &man); err != nil {
		t.Fatal(err)
	}
	if string(man.SeqMark) != string(late) {
		t.Fatalf("the snapshot's manifest carries %s, want %s", man.SeqMark, late)
	}
	h.reopen()
	if got := h.s.SeqMark(); string(got) != string(late) {
		t.Fatalf("a mark set after its seq was published: after a reopen SeqMark = %s, want %s", got, late)
	}
}
