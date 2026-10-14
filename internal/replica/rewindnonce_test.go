package replica

import (
	"context"
	"fmt"
	"testing"

	"github.com/Imposter/go-searchlight/internal/store"
)

// TestIdenticalReissueIsARewind: a failover loses a change, and the change that takes
// its seq afterwards names the same document, of the same kind, in the same
// millisecond, with another body: only its nonce tells it apart, and the copy rebuilds
// and serves the new body. A reissue written by a node from before nonces (no nonce)
// is told apart by its identity, as before.
func TestIdenticalReissueIsARewind(t *testing.T) {
	for _, tc := range []struct {
		name    string
		id      string
		nonce   string
		restart bool
	}{
		{"identical, with a nonce", "x", "424242", false},
		{"identical, with a nonce, across a restart", "x", "424242", true},
		{"another document, no nonce", "y", "NULL", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forEachDialect(t, func(t *testing.T, d *db) {
				ctx := context.Background()
				st := d.open(t)
				createIndex(t, st, "w", testMapping)
				id := ShardID{Index: "w", Shard: 0}
				opts, reader := meteredOptions()
				c := newCopy(t, d.open(t), id, opts)
				c.start()
				k := mustApply(t, st, upsert("w", 0, "kept", docBody("kept", 1)))
				lost := mustApply(t, st, upsert("w", 0, "x", docBody("old", 1)))
				c.waitApplied(lost)
				changes, err := st.ChangesAfter(ctx, id, k, 1)
				if err != nil || len(changes) != 1 || changes[0].Nonce == 0 {
					t.Fatalf("the lost change: %+v %v", changes, err)
				}
				lc := changes[0]
				body := docBody("new", 2)
				if tc.restart {
					c.current().Wake()
					c.waitApplied(lost)
					if err := c.stop(); err != nil {
						t.Fatal(err)
					}
					if err := c.shard().Close(ctx); err != nil {
						t.Fatal(err)
					}
				}
				rewind(ctx, t, d, k)
				raw := rawDB(t, d)
				for _, q := range []string{
					fmt.Sprintf("INSERT INTO sl_changes (seq, index_name, shard, kind, id, payload, at, index_uid, mapping_version, nonce) VALUES (%d, 'w', 0, 'upsert', '%s', '%s', %d, '%s', %d, %s)",
						lc.Seq, tc.id, body, lc.At.UnixMilli(), lc.IndexUID, lc.MappingVersion, tc.nonce),
					fmt.Sprintf("INSERT INTO sl_documents (index_name, shard, id, body, seq) VALUES ('w', 0, '%s', '%s', %d)", tc.id, body, lc.Seq),
					fmt.Sprintf("UPDATE sl_counter SET value = %d WHERE id = 1", lc.Seq),
				} {
					if _, err := raw.ExecContext(ctx, q); err != nil {
						t.Fatalf("reissue %q: %v", q, err)
					}
				}
				if tc.restart {
					c.start()
				}
				waitMatches(t, c, st, id)
				if n := rewindCount(t, reader, "w"); n < 1 {
					t.Fatalf("%d rewinds counted", n)
				}
			})
		})
	}
}

// TestMarkKeepsTheNonce: a mark's nonce survives the manifest, so a restarted copy
// compares it.
func TestMarkKeepsTheNonce(t *testing.T) {
	m := store.ChangeMark{Seq: 3, Index: "w", Kind: store.KindUpsert, ID: "x", Nonce: 99}
	if got := decodeMark(encodeMark(m)); got.Nonce != 99 || !got.Same(m) {
		t.Fatalf("decoded %+v from %+v", got, m)
	}
}
