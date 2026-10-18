package store

import (
	"context"
	"testing"
)

// Client versions (Elasticsearch's external versioning): a versioned change applies
// only above the stored version, the target's or, while it is absent, its tombstone's;
// otherwise it is a noop that takes no seq, as the changes after it in the batch see.
// A versioned delete keeps its version as a tombstone, of an absent target too, which a
// versioned upsert replaces; unversioned writes store version 0 and leave tombstones
// be; dropping the index drops its tombstones.
func TestVersions(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		rr := st.(RecordReader) //nolint:forcetypeassert,errcheck // every store reads records
		mustCreateIndex(t, st, "ver")
		doc := func(id string, version int64) Change {
			c := upsert("ver", 0, id, `{"v":1}`)
			c.Version = version
			return c
		}
		gone := func(id string, version int64) Change {
			c := del("ver", 0, id)
			c.Version, c.IfSeq = version, IfExists
			return c
		}
		qry := func(id string, version int64) Change {
			p, err := EncodeQueryPayload([]byte(`{"all":[]}`), nil, version)
			if err != nil {
				t.Fatal(err)
			}
			return Change{Index: "ver", Kind: KindQueryUpsert, ID: id, Payload: p, Version: version}
		}
		apply := func(want string, batch ...Change) {
			t.Helper()
			if _, _, err := st.Apply(ctx, batch); err != nil {
				t.Fatal(err)
			}
			got := ""
			for _, c := range batch {
				switch {
				case c.Noop:
					got += "n"
				case c.Seq > 0:
					got += "a"
				default:
					got += "s"
				}
			}
			if got != want {
				t.Fatalf("outcomes %q, want %q (a applied, n noop, s skipped)", got, want)
			}
		}
		version := func(kind RecordKind, id string, want int64) {
			t.Helper()
			r, err := rr.GetRecord(ctx, kind, ShardID{Index: "ver"}, id)
			if err != nil || r.Version != want {
				t.Fatalf("%s: %+v, %v; want version %d", id, r, err, want)
			}
		}

		apply("aanna", doc("a", 2), doc("b", 0), doc("a", 2), doc("a", 1), doc("a", 3))
		version(RecordDocument, "a", 3)
		version(RecordDocument, "b", 0)
		apply("a", doc("b", 1))
		version(RecordDocument, "b", 1)
		apply("a", doc("b", 0))
		version(RecordDocument, "b", 0)

		apply("anna", gone("a", 5), doc("a", 4), doc("a", 5), doc("a", 6))
		version(RecordDocument, "a", 6)
		apply("aa", del("ver", 0, "a"), doc("a", 1))
		version(RecordDocument, "a", 1)

		apply("snna", gone("c", 9), doc("c", 8), gone("c", 9), doc("c", 10))
		version(RecordDocument, "c", 10)
		apply("s", gone("d", 4))
		apply("na", doc("d", 3), doc("d", 7))

		apply("anan", qry("q", 2), qry("q", 2), qry("q", 3), qry("q", 1))
		version(RecordQuery, "q", 3)
		list, err := rr.ListQueries(ctx, "ver", "", 10, true)
		if err != nil || len(list) != 1 || list[0].Version != 3 || list[0].Body != nil {
			t.Fatalf("lean list %+v, %v", list, err)
		}
		var scanned int64 = -1
		if _, err := st.ScanShard(ctx, ShardID{Index: "ver"}, func(r Record) error {
			if r.Kind == RecordQuery && r.ID == "q" {
				scanned = r.Version
			}
			return nil
		}); err != nil || scanned != 3 {
			t.Fatalf("scanned version %d, %v; want 3", scanned, err)
		}
		qdel := Change{Index: "ver", Kind: KindQueryDelete, ID: "q", Version: 4, IfSeq: IfExists}
		apply("an", qdel, qry("q", 4))

		if err := st.Indexes().Drop(ctx, "ver"); err != nil {
			t.Fatal(err)
		}
		mustCreateIndex(t, st, "ver")
		apply("a", doc("d", 1))
	})
}
