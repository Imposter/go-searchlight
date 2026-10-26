package store

import (
	"context"
	"testing"
)

// DocumentHashes reads each named document's seq and the hash of its stored body, in
// the order asked, across shards and statements; an absent document, and a row whose
// seq a release before body hashes moved on (hash_seq behind seq), are not Known.
func TestDocumentHashes(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		mustCreateIndex(t, st, "h")
		mustApply(t, st,
			upsert("h", 0, "a", `{"n":1}`),
			upsert("h", 1, "b", `{"n":2}`),
			upsert("h", 0, "c", `{"n":3}`),
		)
		mustApply(t, st, upsert("h", 0, "a", `{"n":4}`), del("h", 0, "c"))
		s := engine(st)
		if _, err := s.w.ExecContext(ctx, rebind(s, "UPDATE sl_documents SET seq = seq + 100 WHERE index_name = ? AND id = ?"), "h", "b"); err != nil {
			t.Fatal(err)
		}
		keys := []DocKey{{Shard: 1, ID: "b"}, {Shard: 0, ID: "a"}, {Shard: 0, ID: "c"}, {Shard: 0, ID: "nope"}, {Shard: 0, ID: "a"}}
		rr, ok := st.(RecordReader)
		if !ok {
			t.Fatal("the store reads no records")
		}
		got, err := rr.DocumentHashes(ctx, "h", keys)
		if err != nil {
			t.Fatal(err)
		}
		want := []DocHash{{Seq: 102}, {Seq: 4, Hash: BodyHash([]byte(`{"n":4}`)), Known: true}, {}, {}, {Seq: 4, Hash: BodyHash([]byte(`{"n":4}`)), Known: true}}
		got[0].Hash = 0
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("key %v = %+v, want %+v", keys[i], got[i], want[i])
			}
		}

		many := make([]DocKey, 2*hashesPerStatement+3)
		for i := range many {
			many[i] = DocKey{Shard: i % 2, ID: "x"}
		}
		many[len(many)-1] = DocKey{Shard: 0, ID: "a"}
		got, err = rr.DocumentHashes(ctx, "h", many)
		if err != nil {
			t.Fatal(err)
		}
		if last := got[len(got)-1]; !last.Known || last.Seq != 4 {
			t.Fatalf("a key past %d keys = %+v", 2*hashesPerStatement, last)
		}
	})
}
