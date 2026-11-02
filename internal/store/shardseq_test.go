package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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
// all; every change stays readable; a seq may repeat across shards; a rerun changes
// nothing; and each shard's next change takes the old head's next seq.
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
		batch := []Change{upsert("one", 0, "n1", `{}`), upsert("three", 1, "n3", `{}`), upsert("two", 1, "n2", `{}`), upsert("three", 1, "n4", `{}`)}
		mustApply(t, st, batch...)
		if got := fmt.Sprint(seqsOf(batch)); got != "[6 6 6 7]" {
			t.Fatalf("the first changes after the migration took %s, want [6 6 6 7]", got)
		}
		for _, c := range batch {
			if c.IndexUID != "" {
				continue
			}
			changes, err := st.ChangesAfter(ctx, c.ShardID(), 5, 10)
			if err != nil || len(changes) == 0 || changes[0].IndexUID != uids[c.Index] || changes[0].MappingVersion != 2 {
				t.Fatalf("%s after the migration: %+v %v", c.ShardID(), changes, err)
			}
		}
	})
}

// TestRecreatedIndexStartsAboveItsPast: a drop raises the floor to the newest seq of
// any of the index's shards, so a shard of the dropped index stands there, and an
// index created under the same name (or any other) starts above every seq the dropped
// one took. A change meant for the dropped incarnation finds its index missing.
func TestRecreatedIndexStartsAboveItsPast(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		mustCreateShards(t, st, "r", 2)
		old, err := st.Indexes().Get(ctx, "r")
		if err != nil {
			t.Fatal(err)
		}
		for i := range 5 {
			mustApply(t, st, upsert("r", 0, fmt.Sprint("d", i), `{}`))
		}
		mustApply(t, st, upsert("r", 1, "e", `{}`))
		if err := st.Indexes().Drop(ctx, "r"); err != nil {
			t.Fatal(err)
		}
		if f := floorSeq(t, st); f != 5 {
			t.Fatalf("floor %d after the drop, want 5", f)
		}
		for sh := range 3 {
			if head := headSeq(t, st, ShardID{Index: "r", Shard: sh}); head != 5 {
				t.Fatalf("shard %d of the dropped index stands at %d, want 5", sh, head)
			}
		}
		if n := countRows(t, st, "SELECT COUNT(*) FROM sl_shard_seq WHERE index_name = ?", "r"); n != 0 {
			t.Fatalf("the drop left %d sequence rows", n)
		}
		mustCreateShards(t, st, "r", 3)
		mustCreateIndex(t, st, "other")
		batch := []Change{upsert("r", 0, "a", `{}`), upsert("r", 2, "b", `{}`), upsert("other", 0, "c", `{}`)}
		mustApply(t, st, batch...)
		if got := fmt.Sprint(seqsOf(batch)); got != "[6 6 6]" {
			t.Fatalf("new shards' first seqs %s, want [6 6 6]", got)
		}
		stale := upsert("r", 1, "x", `{}`)
		stale.IndexUID = old.UID
		var nf *IndexNotFoundError
		if err := st.Apply(ctx, []Change{upsert("r", 1, "ok", `{}`), stale}); !errors.As(err, &nf) || fmt.Sprint(nf.Positions) != "[1]" {
			t.Fatalf("a change for the dropped incarnation: %v", err)
		}
	})
}

// TestRemapAndDropDuringMultiShardWrites: mapping updates interleave with writers that
// commit across every shard at once, directly and through a committer with a lane per
// shard, with no deadlock, and every change of every shard follows the mapping change
// of the version it carries; a drop under the same load ends it, after which every
// write finds the index missing and nothing of it is left.
func TestRemapAndDropDuringMultiShardWrites(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		const shards = 4
		if _, err := st.Indexes().Create(ctx, IndexMeta{Name: "rd", Mapping: []byte(`{"fields":{}}`), Settings: fmt.Appendf(nil, `{"shards":%d}`, shards)}); err != nil {
			t.Fatal(err)
		}
		gc := NewGroupCommitter(st, GroupCommitOptions{MaxTransactions: 4})
		defer gc.Close()
		appliers := []Applier{st, gc}
		write := func(stop <-chan struct{}, dropped *atomic.Bool) error {
			var wg sync.WaitGroup
			errs := make(chan error, 8)
			for w := range 8 {
				wg.Go(func() {
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						batch := make([]Change, shards)
						for sh := range batch {
							batch[sh] = upsert("rd", (sh+w)%shards, fmt.Sprintf("w%d-%d-%d", w, i, sh), `{}`)
						}
						err := appliers[w%2].Apply(ctx, batch)
						var pe *PartialError
						switch {
						case err == nil:
						case dropped != nil && dropped.Load() && (errors.Is(err, ErrNotFound) || errors.As(err, &pe) && errors.Is(err, ErrNotFound)):
							return
						default:
							errs <- fmt.Errorf("writer %d: %w", w, err)
							return
						}
					}
				})
			}
			wg.Wait()
			close(errs)
			return <-errs
		}

		stop := make(chan struct{})
		done := make(chan error, 1)
		go func() { done <- write(stop, nil) }()
		for i := range 5 {
			time.Sleep(20 * time.Millisecond)
			for {
				m, err := st.Indexes().Get(ctx, "rd")
				if err != nil {
					t.Fatal(err)
				}
				m.Mapping = fmt.Appendf(nil, `{"fields":{"f%d":"keyword"}}`, i)
				if _, err = st.Indexes().Update(ctx, m); err == nil {
					break
				} else if !errors.Is(err, ErrConflict) {
					t.Fatal(err)
				}
			}
		}
		close(stop)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		var top int64
		written := map[int64]int{}
		for sh := range shards {
			version := int64(1)
			for _, c := range allChanges(t, st, ShardID{Index: "rd", Shard: sh}) {
				top = max(top, c.Seq)
				written[c.MappingVersion]++
				if c.Kind == KindMapping {
					if c.MappingVersion != version+1 {
						t.Fatalf("shard %d: mapping change %d after version %d", sh, c.MappingVersion, version)
					}
					version = c.MappingVersion
					continue
				}
				if c.MappingVersion != version {
					t.Fatalf("shard %d: seq %d carries mapping version %d, the log is at %d", sh, c.Seq, c.MappingVersion, version)
				}
			}
			if version != 6 {
				t.Fatalf("shard %d ended at mapping version %d", sh, version)
			}
		}
		for v := int64(1); v <= 5; v++ {
			if written[v] <= shards {
				t.Fatalf("only %d changes carry mapping version %d: the writers did not write across the remaps", written[v], v)
			}
		}

		var dropped atomic.Bool
		stop = make(chan struct{})
		go func() { done <- write(stop, &dropped) }()
		time.Sleep(50 * time.Millisecond)
		dropped.Store(true)
		if err := st.Indexes().Drop(ctx, "rd"); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		close(stop)
		for _, table := range []string{"sl_changes", "sl_documents", "sl_shard_seq"} {
			if n := countRows(t, st, "SELECT COUNT(*) FROM "+table+" WHERE index_name = ?", "rd"); n != 0 {
				t.Fatalf("%s keeps %d rows of the dropped index", table, n)
			}
		}
		if f := floorSeq(t, st); f <= top {
			t.Fatalf("floor %d after the drop, at or below the seq %d the index reached before it", f, top)
		}
	})
}

// TestNotifierCoalesces: announcements not yet sent keep only each shard's newest seq.
func TestNotifierCoalesces(t *testing.T) {
	n := &notifier{pending: map[ShardID]int64{}, wake: make(chan struct{}, 1)}
	a, b := ShardID{Index: "i"}, ShardID{Index: "i", Shard: 1}
	n.add(a, 3)
	n.add(b, 2)
	n.add(a, 5)
	n.add(a, 4)
	if fmt.Sprint(n.pending) != fmt.Sprint(map[ShardID]int64{a: 5, b: 2}) || len(n.wake) != 1 {
		t.Fatalf("pending %v, wake %d", n.pending, len(n.wake))
	}
}
