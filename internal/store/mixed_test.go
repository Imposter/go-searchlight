package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// legacyNode plays a node of the release before shard sequences (1.x) on a database
// migrated past them: it writes the statements that release writes, which know nothing
// of sl_shard_seq. Every write takes sl_counter row 1's lock and its next values, the
// global sequence; Create inserts the sl_indexes row alone; Drop and Update take row
// 1's lock; and the reads join sl_changes on seq alone.
type legacyNode struct {
	t *testing.T
	s *sqlStore
}

func legacy(t *testing.T, st Store) *legacyNode { return &legacyNode{t: t, s: engine(st)} }

// lockCounter is 1.x's Changelog.LockCounter.
func (l *legacyNode) lockCounter(ctx context.Context, tx *sql.Tx) (int64, error) {
	q := "SELECT value FROM sl_counter WHERE id = 1"
	if l.s.d.Name != "sqlite" {
		q += " FOR UPDATE"
	}
	var v int64
	if err := tx.QueryRowContext(ctx, q).Scan(&v); err != nil {
		return 0, fmt.Errorf("lock counter: %w", err)
	}
	return v, nil
}

// apply commits upserts of ids to one shard, as 1.x's Apply does, and returns their
// seqs.
func (l *legacyNode) apply(index string, shard int, ids ...string) ([]int64, error) {
	ctx := context.Background()
	s := l.s
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	counter, err := l.lockCounter(ctx, tx)
	if err != nil {
		return nil, err
	}
	var uid string
	var mv int64
	if err := tx.QueryRowContext(ctx, rebind(s, "SELECT uid, mapping_version FROM sl_indexes WHERE name = ?"), index).Scan(&uid, &mv); err != nil {
		return nil, fmt.Errorf("index %q: %w", index, err)
	}
	seqs := make([]int64, len(ids))
	q := rebind(s, "INSERT INTO sl_changes (seq, index_name, shard, kind, id, payload, at, index_uid, mapping_version) VALUES (?, ?, ?, 'upsert', ?, '{}', 0, ?, ?)")
	for i, id := range ids {
		seqs[i] = counter + 1 + int64(i)
		if _, err := tx.ExecContext(ctx, q, seqs[i], index, shard, id, uid, mv); err != nil {
			return nil, fmt.Errorf("1.x write at %d: %w", seqs[i], err)
		}
	}
	if _, err := tx.ExecContext(ctx, rebind(s, "UPDATE sl_counter SET value = ? WHERE id = 1"), counter+int64(len(ids))); err != nil {
		return nil, err
	}
	return seqs, tx.Commit()
}

func (l *legacyNode) mustApply(index string, shard int, ids ...string) []int64 {
	l.t.Helper()
	seqs, err := l.apply(index, shard, ids...)
	if err != nil {
		l.t.Fatalf("1.x apply to %s/%d: %v", index, shard, err)
	}
	return seqs
}

// create adds an index as 1.x's Create does: its sl_indexes row alone, at mapping
// version 1.
func (l *legacyNode) create(name string, shards int) string {
	l.t.Helper()
	s := l.s
	uid := randName("uid")
	q := "INSERT INTO sl_indexes (name, mapping, settings, version, created_at, mapping_version, uid) VALUES (?, '{}', ?, 1, 0, 1, ?)"
	if _, err := s.w.ExecContext(context.Background(), rebind(s, q), name, fmt.Sprintf(`{"shards": %d}`, shards), uid); err != nil {
		l.t.Fatalf("1.x create %s: %v", name, err)
	}
	return uid
}

// drop removes an index as 1.x's Drop does, under the counter lock: its catalogue row
// and its data, but no sequence rows, and no floor.
func (l *legacyNode) drop(name string) error {
	ctx := context.Background()
	s := l.s
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	if _, err := l.lockCounter(ctx, tx); err != nil {
		return err
	}
	for _, q := range []string{
		"DELETE FROM sl_indexes WHERE name = ?",
		"DELETE FROM sl_documents WHERE index_name = ?",
		"DELETE FROM sl_queries WHERE index_name = ?",
		"DELETE FROM sl_tombstones WHERE index_name = ?",
		"DELETE FROM sl_changes WHERE index_name = ?",
		"DELETE FROM sl_pruned WHERE index_name = ?",
		"DELETE FROM sl_shard_copies WHERE index_name = ?",
	} {
		if _, err := tx.ExecContext(ctx, rebind(s, q), name); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// remap changes an index's mapping as 1.x's Update does: under the counter lock, it
// moves sl_indexes on to the next mapping version and logs a mapping change on every
// shard at the global sequence's next values.
func (l *legacyNode) remap(name string, shards int) error {
	ctx := context.Background()
	s := l.s
	tx, err := s.w.BeginTx(ctx, s.d.ApplyTx)
	if err != nil {
		return err
	}
	defer rollback(tx)
	counter, err := l.lockCounter(ctx, tx)
	if err != nil {
		return err
	}
	var uid string
	var mv int64
	if err := tx.QueryRowContext(ctx, rebind(s, "SELECT uid, mapping_version FROM sl_indexes WHERE name = ?"), name).Scan(&uid, &mv); err != nil {
		return err
	}
	mv++
	mapping := fmt.Sprintf(`{"fields": {"f%d": "keyword"}}`, mv)
	if _, err := tx.ExecContext(ctx, rebind(s, "UPDATE sl_indexes SET mapping = ?, mapping_version = ?, version = version + 1 WHERE name = ?"), mapping, mv, name); err != nil {
		return err
	}
	q := rebind(s, "INSERT INTO sl_changes (seq, index_name, shard, kind, id, payload, at, index_uid, mapping_version) VALUES (?, ?, ?, 'mapping', '_mapping', ?, 0, ?, ?)")
	for sh := range shards {
		if _, err := tx.ExecContext(ctx, q, counter+1+int64(sh), name, sh, mapping, uid, mv); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, rebind(s, "UPDATE sl_counter SET value = ? WHERE id = 1"), counter+int64(shards)); err != nil {
		return err
	}
	return tx.Commit()
}

// errNoHead is 1.x's read of the head finding no global sequence.
var errNoHead = errors.New("1.x: no head")

// headChanges is how many changes 1.x's ReadTip and ReadCounterMark find at the head:
// they join sl_changes on seq alone, which must name one change.
func (l *legacyNode) headChanges() (int, error) {
	rows, err := l.s.r.QueryContext(context.Background(),
		"SELECT c.value, h.seq FROM sl_counter c LEFT JOIN sl_changes h ON h.seq = c.value WHERE c.id = 1")
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	n, found := 0, false
	for rows.Next() {
		var v int64
		var seq sql.NullInt64
		if err := rows.Scan(&v, &seq); err != nil {
			return 0, err
		}
		found = true
		if seq.Valid {
			n++
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if !found {
		return 0, errNoHead
	}
	return n, nil
}

// sharedSeqs lists the seqs above from that more than one change holds: 1.x's reads
// by seq cannot tell those apart.
func sharedSeqs(t *testing.T, st Store, from int64) []int64 {
	t.Helper()
	s := engine(st)
	rows, err := s.r.QueryContext(context.Background(), rebind(s, "SELECT seq FROM sl_changes WHERE seq > ? GROUP BY seq HAVING COUNT(*) > 1 ORDER BY seq"), from)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	return out
}

// openAgain opens another node's store on st's database and migrates it.
func openAgain(t *testing.T, h *harness, st Store) Store {
	t.Helper()
	url := h.url
	if h.dialect == "sqlite" {
		url = sqliteURL(engine(st).dbPath)
	}
	other, err := Open(context.Background(), url, quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	if err := other.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate another node: %v", err)
	}
	return other
}

// openMixed opens a database at schema 6 with index "mx" of two shards that a 1.x
// node created and wrote to, and migrates it to this binary's schema: the state a
// rolling upgrade is in once its first new node has started.
func openMixed(t *testing.T, h *harness) (Store, *legacyNode) {
	t.Helper()
	st, all := openAt(t, h, "0001_init.sql", "0002_body_codecs.sql", "0003_change_nonce.sql",
		"0004_mapping_types.sql", "0005_versions.sql", "0006_body_hash.sql")
	old := legacy(t, st)
	old.create("mx", 2)
	old.mustApply("mx", 0, "a", "b")
	old.mustApply("mx", 1, "c")
	engine(st).d.Migrations = all
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return st, old
}

// TestMixedVersionWritesShareOneSequence (ship blocker 1): while a 1.x node writes,
// this binary's writes take the global sequence too, so a write never lands below a
// seq a shard's tailer has passed, and never collides with a 1.x write.
func TestMixedVersionWritesShareOneSequence(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, old := openMixed(t, h)
		old.mustApply("mx", 1, "d", "e", "f")
		s0 := old.mustApply("mx", 0, "g")[0]
		page, err := st.ChangesAfter(ctx, ShardID{Index: "mx"}, 0, 100)
		if err != nil || page[len(page)-1].Seq != s0 {
			t.Fatalf("the tailer of shard 0: %+v %v", page, err)
		}
		b := []Change{upsert("mx", 0, "new0", `{}`)}
		if err := st.Apply(ctx, b); err != nil {
			t.Fatalf("apply to shard 0: %v", err)
		}
		if b[0].Seq <= s0 {
			t.Errorf("a write to shard 0 after 1.x's seq %d took seq %d, which its tailer has passed", s0, b[0].Seq)
		}
		if page, err := st.ChangesAfter(ctx, ShardID{Index: "mx"}, s0, 10); err != nil || len(page) != 1 || page[0].ID != "new0" {
			t.Errorf("the tailer after seq %d reads %+v %v, want new0", s0, page, err)
		}
		b = []Change{upsert("mx", 1, "new1", `{}`), upsert("mx", 0, "new2", `{}`)}
		if err := st.Apply(ctx, b); err != nil {
			t.Fatalf("apply to shards 1 and 0, which 1.x wrote: %v", err)
		}
		next := old.mustApply("mx", 1, "h")[0]
		if top := max(b[0].Seq, b[1].Seq); next <= top {
			t.Errorf("1.x's write after seq %d took seq %d", top, next)
		}
		if shared := sharedSeqs(t, st, 0); len(shared) > 0 {
			t.Errorf("seqs %v name more than one change", shared)
		}
	})
}

// TestMixedVersionIndexCreatedByOldNode (ship blocker 2): an index a 1.x node creates
// after the migration has no sequence rows; this binary still writes to it, and a node
// of it starting then still starts.
func TestMixedVersionIndexCreatedByOldNode(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, old := openMixed(t, h)
		old.create("late", 3)
		old.mustApply("late", 2, "x")
		b := []Change{upsert("late", 2, "y", `{}`), upsert("late", 0, "z", `{}`)}
		if err := st.Apply(ctx, b); err != nil {
			t.Fatalf("apply to an index 1.x created: %v", err)
		}
		if got := allChanges(t, st, ShardID{Index: "late", Shard: 2}); len(got) != 2 {
			t.Errorf("shard 2 of the index 1.x created holds %d changes, want 2", len(got))
		}
		other := openAgain(t, h, st)
		if err := other.Apply(ctx, []Change{upsert("late", 1, "w", `{}`)}); err != nil {
			t.Fatalf("a node started after 1.x created an index cannot write to it: %v", err)
		}
	})
}

// TestMixedVersionDropByOldNode (ship blocker 3): an index a 1.x node drops leaves no
// sequence rows behind to trip a recreate, and an index this binary creates under the
// name starts above every seq the dropped one took.
func TestMixedVersionDropByOldNode(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, old := openMixed(t, h)
		b := []Change{upsert("mx", 0, "two", `{}`)}
		mustApply(t, st, b...)
		top := old.mustApply("mx", 1, "one")[0]
		if err := old.drop("mx"); err != nil {
			t.Fatalf("1.x drop: %v", err)
		}
		if _, err := st.Indexes().Create(ctx, IndexMeta{Name: "mx", Settings: []byte(`{"shards": 2}`)}); err != nil {
			t.Fatalf("recreate after 1.x's drop: %v", err)
		}
		b = []Change{upsert("mx", 0, "again0", `{}`), upsert("mx", 1, "again1", `{}`)}
		if err := st.Apply(ctx, b); err != nil {
			t.Fatalf("apply to the recreated index: %v", err)
		}
		for _, c := range b {
			if c.Seq <= top {
				t.Errorf("%s of the recreated index took seq %d, at or below the dropped one's %d", c.ShardID(), c.Seq, top)
			}
		}
	})
}

// TestMixedVersionRemapByOldNode (ship blocker 4): a mapping change a 1.x node makes is
// what this binary's later writes carry.
func TestMixedVersionRemapByOldNode(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, old := openMixed(t, h)
		mustApply(t, st, upsert("mx", 0, "before", `{}`))
		if err := old.remap("mx", 2); err != nil {
			t.Fatalf("1.x remap: %v", err)
		}
		m, err := st.Indexes().Get(ctx, "mx")
		if err != nil {
			t.Fatal(err)
		}
		b := []Change{upsert("mx", 0, "after", `{}`)}
		mustApply(t, st, b...)
		got := allChanges(t, st, ShardID{Index: "mx"})
		if last := got[len(got)-1]; last.ID != "after" || last.MappingVersion != m.MappingVersion {
			t.Errorf("the write after 1.x's remap to mapping version %d logged %+v", m.MappingVersion, last)
		}
	})
}

// TestMixedVersionOldReadsStayUnambiguous (ship blocker 5): 1.x's reads find a change
// by its seq alone, so while one runs no two changes share a seq.
func TestMixedVersionOldReadsStayUnambiguous(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, old := openMixed(t, h)
		for i := range 3 {
			b := []Change{upsert("mx", 0, fmt.Sprint("p", i), `{}`), upsert("mx", 1, fmt.Sprint("q", i), `{}`)}
			if err := st.Apply(ctx, b); err != nil {
				t.Fatal(err)
			}
			if err := st.Apply(ctx, []Change{upsert("mx", 1, fmt.Sprint("r", i), `{}`)}); err != nil {
				t.Fatal(err)
			}
			if n, err := old.headChanges(); err != nil || n != 1 {
				t.Fatalf("1.x's read of the head finds %d changes: %v", n, err)
			}
		}
		if shared := sharedSeqs(t, st, 0); len(shared) > 0 {
			t.Errorf("seqs %v name more than one change", shared)
		}
	})
}

// TestShardSeqsSwitch: turning shard sequences on seeds a row for every shard of every
// index sl_indexes holds, whatever 1.x created, dropped or remapped meanwhile, at the
// global sequence's value with the index's current incarnation and mapping version;
// no head moves back; 1.x can no longer write or read the head; and a node that has
// not seen the switch yet writes on the shard sequences at its next commit.
func TestShardSeqsSwitch(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, old := openMixed(t, h)
		other := openAgain(t, h, st)
		mustApply(t, st, upsert("mx", 0, "n1", `{}`), upsert("mx", 1, "n2", `{}`))
		lateUID := old.create("late", 3)
		old.mustApply("late", 1, "l1")
		old.create("gone", 2)
		old.mustApply("gone", 0, "g1")
		if err := old.drop("gone"); err != nil {
			t.Fatal(err)
		}
		if err := old.remap("mx", 2); err != nil {
			t.Fatal(err)
		}
		mustApply(t, other, upsert("late", 2, "l2", `{}`))
		if on, err := st.ShardSeqs(ctx); err != nil || on {
			t.Fatalf("shard sequences before the switch: %v %v", on, err)
		}
		if rows := shardSeqRows(t, st); len(rows) != 0 {
			t.Fatalf("sequence rows before the switch: %v", rows)
		}
		before := map[ShardID]int64{}
		for _, id := range []ShardID{{Index: "mx"}, {Index: "mx", Shard: 1}, {Index: "late"}, {Index: "late", Shard: 1}, {Index: "late", Shard: 2}} {
			before[id] = headSeq(t, st, id)
		}
		global := floorSeq(t, st)

		if err := st.EnableShardSeqs(ctx); err != nil {
			t.Fatalf("switch: %v", err)
		}
		mx, err := st.Indexes().Get(ctx, "mx")
		if err != nil {
			t.Fatal(err)
		}
		want := []shardSeqRow{
			{index: "late", shard: 0, value: global, uid: lateUID, mappingVersion: 1},
			{index: "late", shard: 1, value: global, uid: lateUID, mappingVersion: 1},
			{index: "late", shard: 2, value: global, uid: lateUID, mappingVersion: 1},
			{index: "mx", shard: 0, value: global, uid: mx.UID, mappingVersion: mx.MappingVersion},
			{index: "mx", shard: 1, value: global, uid: mx.UID, mappingVersion: mx.MappingVersion},
		}
		if got := shardSeqRows(t, st); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("sequence rows after the switch:\n got %v\nwant %v", got, want)
		}
		if mx.MappingVersion != 2 {
			t.Fatalf("mx's mapping version %d after 1.x's remap", mx.MappingVersion)
		}
		if n := countRows(t, st, "SELECT COUNT(*) FROM sl_counter WHERE id = 1"); n != 0 {
			t.Error("the global sequence is still there")
		}
		if got := floorSeq(t, st); got != global {
			t.Errorf("the floor is %d, want %d", got, global)
		}
		if f, err := st.Registry().Features(ctx); err != nil || f[FeatureShardSeqs].IsZero() {
			t.Errorf("features after the switch: %v %v", f, err)
		}
		for id, seq := range before {
			if now := headSeq(t, st, id); now < seq {
				t.Errorf("%s's head moved back from %d to %d", id, seq, now)
			}
		}

		if _, err := old.apply("mx", 0, "refused"); err == nil {
			t.Error("1.x wrote after the switch")
		}
		if err := old.remap("mx", 2); err == nil {
			t.Error("1.x remapped after the switch")
		}
		if err := old.drop("mx"); err == nil {
			t.Error("1.x dropped after the switch")
		}
		if _, err := old.headChanges(); err == nil {
			t.Error("1.x read the head after the switch")
		}

		b := []Change{upsert("mx", 0, "a0", `{}`), upsert("mx", 1, "a1", `{}`)}
		if err := other.Apply(ctx, b); err != nil {
			t.Fatalf("a node that had not seen the switch: %v", err)
		}
		if b[0].Seq != global+1 || b[1].Seq != global+1 {
			t.Errorf("the first writes after the switch took %v, want both %d", seqsOf(b), global+1)
		}
		if on, err := other.ShardSeqs(ctx); err != nil || !on {
			t.Errorf("the other node after its write: %v %v", on, err)
		}
		if err := st.EnableShardSeqs(ctx); err != nil {
			t.Errorf("a second switch: %v", err)
		}
		if _, err := other.Indexes().Create(ctx, IndexMeta{Name: "after", Settings: []byte(`{"shards": 2}`)}); err != nil {
			t.Fatal(err)
		}
		if n := countRows(t, st, "SELECT COUNT(*) FROM sl_shard_seq WHERE index_name = ? AND value = ?", "after", global); n != 2 {
			t.Errorf("an index created after the switch has %d rows at the floor", n)
		}
		mustApply(t, st, upsert("late", 1, "top", `{}`), upsert("late", 1, "top2", `{}`))
		if err := st.Indexes().Drop(ctx, "late"); err != nil {
			t.Fatal(err)
		}
		if got := floorSeq(t, st); got != global+2 {
			t.Errorf("the floor after dropping late is %d, want %d", got, global+2)
		}
	})
}

// TestFreshDatabaseHasShardSeqs: a database Migrate creates has shard sequences on from
// the start.
func TestFreshDatabaseHasShardSeqs(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		st := h.open(t)
		if on, err := st.ShardSeqs(context.Background()); err != nil || !on {
			t.Fatalf("a new database: %v %v", on, err)
		}
		if n := countRows(t, st, "SELECT COUNT(*) FROM sl_counter WHERE id = 3"); n != 1 {
			t.Fatal("a new database has no floor")
		}
	})
}

// TestIndexCreatedByOldNodeAfterTheSwitch: 1.x's Create takes no lock, so a 1.x node
// left running past the switch can still create an index, without sequence rows. A
// node starting seeds them at the floor (1.x can write nothing to it).
func TestIndexCreatedByOldNodeAfterTheSwitch(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		st, old := openMixed(t, h)
		if err := st.EnableShardSeqs(context.Background()); err != nil {
			t.Fatal(err)
		}
		old.create("zombie", 2)
		other := openAgain(t, h, st)
		mustApply(t, other, upsert("zombie", 1, "z", `{}`))
		if n := countRows(t, st, "SELECT COUNT(*) FROM sl_shard_seq WHERE index_name = ?", "zombie"); n != 2 {
			t.Fatalf("%d sequence rows for zombie", n)
		}
	})
}

// TestRollingUpgradeUnderLoad: a 1.x node writes all through: while a node of this
// binary migrates the database (MySQL's key swap runs online, beside the 1.x commits),
// while two such nodes write beside it, and while shard sequences come on, after which
// its writes fail. A tailer of each shard, reading as the writes go, misses no change
// any of them committed, and no head is ever below a seq its shard holds.
func TestRollingUpgradeUnderLoad(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, all := openAt(t, h, "0001_init.sql", "0002_body_codecs.sql", "0003_change_nonce.sql",
			"0004_mapping_types.sql", "0005_versions.sql", "0006_body_hash.sql")
		const shards = 4
		old := legacy(t, st)
		old.create("load", shards)
		old.mustApply("load", 0, "seed")

		stop := make(chan struct{})
		var wg sync.WaitGroup
		var oldWrites, oldRefused atomic.Int64
		wg.Go(func() {
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := old.apply("load", i%shards, fmt.Sprint("old-", i)); err != nil {
					oldRefused.Add(1)
					if oldRefused.Load() > 20 {
						return
					}
					continue
				}
				oldWrites.Add(1)
			}
		})
		engine(st).d.Migrations = all
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("migrate beside a 1.x writer: %v", err)
		}
		other := openAgain(t, h, st)

		type seen struct {
			mu  sync.Mutex
			ids map[string]bool
		}
		tailed := make([]*seen, shards)
		tailersDone := make(chan struct{})
		var tailers sync.WaitGroup
		for sh := range shards {
			tailed[sh] = &seen{ids: map[string]bool{}}
			tailers.Go(func() {
				after := int64(0)
				finishing := false
				for {
					page, err := st.ChangesAfter(ctx, ShardID{Index: "load", Shard: sh}, after, 50)
					if err != nil {
						t.Errorf("tail %d: %v", sh, err)
						return
					}
					for _, c := range page {
						tailed[sh].mu.Lock()
						tailed[sh].ids[c.ID] = true
						tailed[sh].mu.Unlock()
						after = c.Seq
					}
					if len(page) == 0 {
						if finishing {
							return
						}
						select {
						case <-tailersDone:
							finishing = true
						default:
						}
					}
				}
			})
		}
		var newWrites atomic.Int64
		var writers sync.WaitGroup
		for w, node := range []Store{st, other} {
			writers.Go(func() {
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					b := []Change{upsert("load", i%shards, fmt.Sprintf("new%d-%d", w, i), `{}`), upsert("load", (i+1)%shards, fmt.Sprintf("new%d-%d+", w, i), `{}`)}
					if err := node.Apply(ctx, b); err != nil {
						t.Errorf("writer %d: %v", w, err)
						return
					}
					newWrites.Add(2)
				}
			})
		}
		waitFor := func(n *atomic.Int64, want int64) {
			deadline := time.Now().Add(time.Minute)
			for n.Load() < want && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
		}
		waitFor(&newWrites, 200)
		if err := other.EnableShardSeqs(ctx); err != nil {
			t.Fatalf("switch under load: %v", err)
		}
		switched := oldWrites.Load()
		waitFor(&newWrites, newWrites.Load()+200)
		waitFor(&oldRefused, 1)
		close(stop)
		wg.Wait()
		writers.Wait()
		close(tailersDone)
		tailers.Wait()

		if oldRefused.Load() == 0 {
			t.Error("1.x kept writing after the switch")
		}
		if n := oldWrites.Load(); n > switched+1 {
			t.Errorf("1.x committed %d writes after the switch", n-switched)
		}
		t.Logf("%s: 1.x committed %d writes, this binary %d", h.dialect, oldWrites.Load(), newWrites.Load())
		for sh := range shards {
			id := ShardID{Index: "load", Shard: sh}
			changes := allChanges(t, st, id)
			var missed []string
			for _, c := range changes {
				if !tailed[sh].ids[c.ID] {
					missed = append(missed, c.ID)
				}
			}
			if len(missed) > 0 {
				t.Errorf("the tailer of %s missed %d of %d changes: %v", id, len(missed), len(changes), missed[:min(len(missed), 5)])
			}
			if top := changes[len(changes)-1].Seq; headSeq(t, st, id) < top {
				t.Errorf("%s's head %d is below its change at %d", id, headSeq(t, st, id), top)
			}
		}
	})
}
