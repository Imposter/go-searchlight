package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Imposter/go-searchlight/internal/segment"
)

// TestMaxPayloadIsTheSegmentLimit pins the store's payload bound to what a segment
// stores: a change the store accepts must always be appliable.
func TestMaxPayloadIsTheSegmentLimit(t *testing.T) {
	if MaxPayloadBytes != segment.MaxStoredBytes {
		t.Fatalf("MaxPayloadBytes = %d, segment.MaxStoredBytes = %d", MaxPayloadBytes, segment.MaxStoredBytes)
	}
}

func TestApplyGuards(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		mustCreateIndex(t, st, "g")

		// A document a segment could not store never commits, by any path.
		big := upsert("g", 0, "big", `{"t":"`+strings.Repeat("x", MaxPayloadBytes)+`"}`)
		_, _, err := st.Apply(ctx, []Change{upsert("g", 0, "ok", `{}`), big})
		var ce *ChangeError
		if !errors.As(err, &ce) || ce.Position != 1 || !errors.Is(err, ErrInvalid) {
			t.Fatalf("an oversized payload: err = %v, want a ChangeError at 1", err)
		}
		if n := countRows(t, st, "SELECT COUNT(*) FROM sl_changes"); n != 0 {
			t.Fatalf("%d changes committed with the refused one", n)
		}

		// IfExists: a delete of nothing is skipped inside the transaction (no
		// row, no seq) while the rest of the batch applies with contiguous seqs.
		_, seq := mustApply(t, st, upsert("g", 0, "there", `{}`))
		batch := []Change{
			{Index: "g", Kind: KindDelete, ID: "none", IfSeq: IfExists},
			upsert("g", 0, "x", `{}`),
			{Index: "g", Kind: KindDelete, ID: "there", IfSeq: IfExists},
			{Index: "g", Kind: KindDelete, ID: "none2", IfSeq: IfExists},
		}
		first, last, err := st.Apply(ctx, batch)
		if err != nil || first != seq+1 || last != seq+2 {
			t.Fatalf("a batch with deletes of nothing: %d..%d %v", first, last, err)
		}
		if got := []int64{batch[0].Seq, batch[1].Seq, batch[2].Seq, batch[3].Seq}; fmt.Sprint(got) != fmt.Sprint([]int64{0, seq + 1, seq + 2, 0}) {
			t.Errorf("seqs = %v", got)
		}
		if n := countRows(t, st, "SELECT COUNT(*) FROM sl_changes WHERE id IN ('none', 'none2')"); n != 0 {
			t.Errorf("%d changes written for deletes of nothing", n)
		}
		// Through the group committer, a request of only deletes of nothing gets
		// 0, 0 and fails no other request.
		gc := NewGroupCommitter(st, GroupCommitOptions{})
		defer gc.Close()
		only := []Change{{Index: "g", Kind: KindDelete, ID: "nothing", IfSeq: IfExists}}
		if f, l, err := gc.Apply(ctx, only); err != nil || f != 0 || l != 0 || only[0].Seq != 0 {
			t.Errorf("a request of a delete of nothing: %d..%d %v", f, l, err)
		}
		mixed := []Change{upsert("g", 0, "y", `{}`), {Index: "g", Kind: KindDelete, ID: "nothing", IfSeq: IfExists}}
		if f, l, err := gc.Apply(ctx, mixed); err != nil || f == 0 || f != l || mixed[0].Seq != f || mixed[1].Seq != 0 {
			t.Errorf("a mixed request: %d..%d %v %+v", f, l, err, mixed)
		}

		// An expected incarnation that is gone is a missing index.
		meta, err := st.Indexes().Get(ctx, "g")
		if err != nil {
			t.Fatal(err)
		}
		c := upsert("g", 0, "a", `{}`)
		c.IndexUID = meta.UID
		mustApply(t, st, c)
		if err := st.Indexes().Drop(ctx, "g"); err != nil {
			t.Fatal(err)
		}
		mustCreateIndex(t, st, "g")
		_, _, err = st.Apply(ctx, []Change{upsert("g", 0, "b", `{}`), c})
		var nf *IndexNotFoundError
		if !errors.As(err, &nf) || len(nf.Positions) != 1 || nf.Positions[0] != 1 || !errors.Is(err, ErrNotFound) {
			t.Fatalf("a stale incarnation: err = %v", err)
		}
	})
}
