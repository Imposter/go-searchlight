package store

import (
	"context"
	"fmt"
	"io/fs"
	"testing"
	"testing/fstest"
	"time"
)

// TestChangeNonces: every change a store writes (Apply, group commit and a mapping
// change) carries a nonce of its own, and the marks Tip and ScanShard return carry
// the nonce ChangesAfter returns for the same change.
func TestChangeNonces(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		meta, err := st.Indexes().Create(ctx, IndexMeta{Name: "n"})
		if err != nil {
			t.Fatal(err)
		}
		mustApply(t, st, upsert("n", 0, "a", `{}`), upsert("n", 0, "b", `{}`))
		g := NewGroupCommitter(st, GroupCommitOptions{MaxDelay: time.Millisecond})
		defer g.Close()
		if _, _, err := g.Apply(ctx, []Change{upsert("n", 0, "c", `{}`)}); err != nil {
			t.Fatal(err)
		}
		meta.Mapping = []byte(`{"fields":{"x":"keyword"}}`)
		if _, err := st.Indexes().Update(ctx, meta); err != nil {
			t.Fatal(err)
		}
		changes, err := st.ChangesAfter(ctx, ShardID{Index: "n"}, 0, 10)
		if err != nil || len(changes) != 4 {
			t.Fatalf("changes: %v %v", changes, err)
		}
		seen := map[int64]bool{}
		for _, c := range changes {
			if c.Nonce == 0 || seen[c.Nonce] {
				t.Fatalf("change %d (%s %q) has nonce %d", c.Seq, c.Kind, c.ID, c.Nonce)
			}
			seen[c.Nonce] = true
		}
		last := changes[len(changes)-1]
		tip, err := st.Tip(ctx, changes[0].Seq)
		if err != nil || tip.HeadMark.Nonce != last.Nonce || tip.AtMark.Nonce != changes[0].Nonce {
			t.Fatalf("tip marks %+v and %+v, want nonces %d and %d (%v)", tip.HeadMark, tip.AtMark, last.Nonce, changes[0].Nonce, err)
		}
		_, mark, err := st.ScanShard(ctx, ShardID{Index: "n"}, func(Record) error { return nil })
		if err != nil || mark.Nonce != last.Nonce {
			t.Fatalf("the snapshot's mark %+v, want nonce %d (%v)", mark, last.Nonce, err)
		}
	})
}

// TestChangeMarkSameByNonce: two marks of one identity are the same change only when
// their nonces agree, or when either has none (a change written before nonces).
func TestChangeMarkSameByNonce(t *testing.T) {
	at := time.UnixMilli(1_700_000_000_000).UTC()
	base := ChangeMark{Seq: 7, Index: "i", Shard: 0, Kind: KindUpsert, ID: "a", At: at}
	withNonce := func(n int64) ChangeMark { m := base; m.Nonce = n; return m }
	for _, tc := range []struct {
		a, b ChangeMark
		same bool
	}{
		{withNonce(1), withNonce(1), true},
		{withNonce(1), withNonce(2), false},
		{withNonce(1), base, true},
		{base, withNonce(2), true},
		{base, base, true},
		{withNonce(1), ChangeMark{Seq: 7, Index: "i", Kind: KindUpsert, ID: "b", At: at, Nonce: 1}, false},
	} {
		if got := tc.a.Same(tc.b); got != tc.same {
			t.Errorf("%+v Same %+v = %v, want %v", tc.a, tc.b, got, tc.same)
		}
	}
}

// TestMigrateChangeNonce: a database at schema v2, holding changes, migrates to v3:
// the old changes read with no nonce, new ones get one, and a v2 binary's insert,
// which names no nonce column, still writes (a NULL nonce).
func TestMigrateChangeNonce(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, all := openAt(t, h, "0001_init.sql", "0002_body_codecs.sql")
		s := engine(st)
		meta, err := st.Indexes().Create(ctx, IndexMeta{Name: "old"})
		if err != nil {
			t.Fatal(err)
		}
		old := oldStatements(h.dialect)
		for i := range 3 {
			if _, err := s.w.ExecContext(ctx, rebind(s, old.change), i+1, "old", fmt.Sprint("d", i), `{}`, meta.UID); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.w.ExecContext(ctx, "UPDATE sl_counter SET value = 3 WHERE id = 1"); err != nil {
			t.Fatal(err)
		}
		s.d.Migrations = all
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("migrate to change nonces: %v", err)
		}
		_, last := mustApply(t, st, upsert("old", 0, "new", `{}`))
		if _, err := s.w.ExecContext(ctx, rebind(s, old.change), last+1, "old", "late", `{}`, meta.UID); err != nil {
			t.Fatalf("a v2 binary's insert after the migration: %v", err)
		}
		if _, err := s.w.ExecContext(ctx, rebind(s, "UPDATE sl_counter SET value = ? WHERE id = 1"), last+1); err != nil {
			t.Fatal(err)
		}
		changes, err := st.ChangesAfter(ctx, ShardID{Index: "old"}, 0, 10)
		if err != nil || len(changes) != 5 {
			t.Fatalf("changes: %v %v", changes, err)
		}
		for _, c := range changes {
			if want := c.ID == "new"; (c.Nonce != 0) != want {
				t.Errorf("change %q has nonce %d", c.ID, c.Nonce)
			}
		}
	})
}

// openAt opens a store on h migrated with the named migrations only, and returns it
// with the dialect's full migration set.
func openAt(t *testing.T, h *harness, names ...string) (Store, fs.FS) {
	t.Helper()
	st, all := openV1(t, h)
	s := engine(st)
	files := fstest.MapFS{}
	for _, name := range names {
		data, err := fs.ReadFile(all, name)
		if err != nil {
			t.Fatal(err)
		}
		files[name] = &fstest.MapFile{Data: data}
	}
	s.d.Migrations = files
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate to %v: %v", names, err)
	}
	return st, all
}
