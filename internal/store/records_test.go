package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestRecordReader(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		rr, ok := st.(RecordReader)
		if !ok {
			t.Fatalf("%T does not implement RecordReader", st)
		}
		mustCreateIndex(t, st, "rec")
		payload := func(q string) []byte {
			p, err := EncodeQueryPayload([]byte(q), []byte(`{"owner":"x"}`))
			if err != nil {
				t.Fatal(err)
			}
			return p
		}
		mustApply(t, st,
			upsert("rec", 0, "d1", `{"v":1}`),
			upsert("rec", 1, "d2", `{"v":2}`),
			Change{Index: "rec", Shard: 1, Kind: KindQueryUpsert, ID: "q2", Payload: payload(`{"all":[]}`)},
			Change{Index: "rec", Shard: 0, Kind: KindQueryUpsert, ID: "q1", Payload: payload(`{"field":"v","op":"eq","value":1}`)},
			Change{Index: "rec", Shard: 0, Kind: KindQueryUpsert, ID: "q3", Payload: payload(`{"all":[]}`)},
		)
		_, last := mustApply(t, st, upsert("rec", 0, "d1", `{"v":3}`))

		r, err := rr.GetRecord(ctx, RecordDocument, ShardID{Index: "rec"}, "d1")
		if err != nil || string(r.Body) != `{"v":3}` || r.Seq != last {
			t.Fatalf("GetRecord d1 = %+v, %v; want the latest body at seq %d", r, err, last)
		}
		if _, err := rr.GetRecord(ctx, RecordDocument, ShardID{Index: "rec"}, "d2"); !errors.Is(err, ErrNotFound) {
			t.Errorf("d2 on the wrong shard: err = %v, want ErrNotFound", err)
		}
		q, err := rr.GetRecord(ctx, RecordQuery, ShardID{Index: "rec"}, "q1")
		if err != nil || string(q.Body) != `{"field":"v","op":"eq","value":1}` || string(q.Meta) != `{"owner":"x"}` {
			t.Fatalf("GetRecord q1 = %+v, %v", q, err)
		}
		if _, err := rr.GetRecord(ctx, RecordQuery, ShardID{Index: "rec"}, "missing"); !errors.Is(err, ErrNotFound) {
			t.Errorf("missing query: err = %v, want ErrNotFound", err)
		}
		if _, err := rr.GetRecord(ctx, RecordDocument, ShardID{Index: "rec"}, ""); !errors.Is(err, ErrInvalid) {
			t.Errorf("empty id: err = %v, want ErrInvalid", err)
		}

		var ids []string
		after := ""
		for {
			page, err := rr.ListQueries(ctx, "rec", after, 2)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range page {
				ids = append(ids, fmt.Sprintf("%s@%d", r.ID, r.Shard))
			}
			if len(page) < 2 {
				break
			}
			after = page[len(page)-1].ID
		}
		if fmt.Sprint(ids) != "[q1@0 q2@1 q3@0]" {
			t.Errorf("ListQueries pages = %v", ids)
		}
		if _, err := rr.ListQueries(ctx, "rec", "", MaxListLimit+1); !errors.Is(err, ErrInvalid) {
			t.Errorf("limit over the maximum: err = %v, want ErrInvalid", err)
		}
	})
}
