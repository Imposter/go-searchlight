package store

import (
	"context"
	"fmt"
	"testing"
)

// createOldIndex adds an index the way a binary from before shard sequences does: its
// sl_indexes row alone.
func createOldIndex(t *testing.T, st Store, name, settings string) string {
	t.Helper()
	s := engine(st)
	uid := randName("uid")
	q := "INSERT INTO sl_indexes (name, mapping, settings, version, created_at, mapping_version, uid) VALUES (?, '{}', ?, 1, 0, 2, ?)"
	if _, err := s.w.ExecContext(context.Background(), rebind(s, q), name, settings, uid); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	return uid
}

type shardSeqRow struct {
	index          string
	shard          int
	value          int64
	uid            string
	mappingVersion int64
}

func shardSeqRows(t *testing.T, st Store) []shardSeqRow {
	t.Helper()
	s := engine(st)
	rows, err := s.r.QueryContext(context.Background(), "SELECT index_name, shard, value, index_uid, mapping_version FROM sl_shard_seq ORDER BY index_name, shard")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []shardSeqRow
	for rows.Next() {
		var r shardSeqRow
		if err := rows.Scan(&r.index, &r.shard, &r.value, &r.uid, &r.mappingVersion); err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestMigrateShardSeq: a database at schema v6 migrates to a sequence per shard. Every
// shard of every index starts at the old global head with its index's uid and mapping
// version, whether its settings name the shard count as a number, as a string or not at
// all; every change stays readable; a seq may repeat across shards; and a rerun changes
// nothing.
func TestMigrateShardSeq(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, all := openAt(t, h, "0001_init.sql", "0002_body_codecs.sql", "0003_change_nonce.sql",
			"0004_mapping_types.sql", "0005_versions.sql", "0006_body_hash.sql")
		s := engine(st)
		uids := map[string]string{
			"one":   createOldIndex(t, st, "one", "{}"),
			"three": createOldIndex(t, st, "three", `{"shards": 3}`),
			"two":   createOldIndex(t, st, "two", `{"shards": "2"}`),
		}
		change := "INSERT INTO sl_changes (seq, index_name, shard, kind, id, payload, at, index_uid, mapping_version) VALUES (?, ?, ?, 'upsert', ?, '{}', 0, ?, 2)"
		old := []struct {
			index string
			shard int
		}{{"one", 0}, {"three", 0}, {"three", 2}, {"two", 1}, {"three", 2}}
		for i, c := range old {
			if _, err := s.w.ExecContext(ctx, rebind(s, change), i+1, c.index, c.shard, fmt.Sprint("d", i), uids[c.index]); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.w.ExecContext(ctx, "UPDATE sl_counter SET value = 5 WHERE id = 1"); err != nil {
			t.Fatal(err)
		}

		s.d.Migrations = all
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("migrate to shard sequences: %v", err)
		}
		var want []shardSeqRow
		for _, ix := range []struct {
			name   string
			shards int
		}{{"one", 1}, {"three", 3}, {"two", 2}} {
			for sh := range ix.shards {
				want = append(want, shardSeqRow{index: ix.name, shard: sh, value: 5, uid: uids[ix.name], mappingVersion: 2})
			}
		}
		if got := shardSeqRows(t, st); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("shard rows after the migration:\n got %v\nwant %v", got, want)
		}
		for i, c := range old {
			changes, err := st.ChangesAfter(ctx, ShardID{Index: c.index, Shard: c.shard}, int64(i), 1)
			if err != nil || len(changes) != 1 || changes[0].Seq != int64(i+1) || changes[0].ID != fmt.Sprint("d", i) {
				t.Errorf("change %d after the migration: %+v %v", i+1, changes, err)
			}
		}
		if _, err := s.w.ExecContext(ctx, rebind(s, change), 1, "three", 1, "again", uids["three"]); err != nil {
			t.Errorf("seq 1 on another shard: %v", err)
		}
		if _, err := s.w.ExecContext(ctx, rebind(s, change), 1, "three", 1, "twice", uids["three"]); err == nil {
			t.Error("seq 1 twice on one shard was stored")
		}

		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("migrate again: %v", err)
		}
		if got := shardSeqRows(t, st); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("shard rows after a second migration: %v", got)
		}
	})
}
