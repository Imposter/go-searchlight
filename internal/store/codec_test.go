package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"testing"
	"testing/fstest"
)

func TestCodec(t *testing.T) {
	product := []byte(benchBody(1))
	for name, tc := range map[string]struct {
		body    []byte
		encoded bool
	}{
		"empty":          {nil, false},
		"small":          {[]byte(`{"title":"a small document"}`), false},
		"product":        {product, true},
		"repetitive":     {[]byte(`{"pad":"` + string(bytes.Repeat([]byte("ab"), 4000)) + `"}`), true},
		"incompressible": {[]byte(`{"pad":"` + noise(150) + `"}`), false},
		"leading space":  {append([]byte(" \t\r\n"), product...), true},
	} {
		t.Run(name, func(t *testing.T) {
			stored := encodeBody(tc.body)
			if got := len(stored) > 0 && stored[0] == codecZstd; got != tc.encoded {
				t.Fatalf("encoded %v, want %v (%d -> %d bytes)", got, tc.encoded, len(tc.body), len(stored))
			}
			if tc.encoded && len(stored) >= len(tc.body) {
				t.Fatalf("encoded to %d bytes from %d", len(stored), len(tc.body))
			}
			back, err := decodeBody(stored)
			if err != nil || !bytes.Equal(back, tc.body) {
				t.Fatalf("decode: %v, %q", err, back)
			}
		})
	}
	if _, err := decodeBody([]byte{codecLast, 1, 2}); !errors.Is(err, errUnknownCodec) {
		t.Fatalf("a reserved codec byte: %v", err)
	}
	stored := encodeBody(product)
	stored[len(stored)/2] ^= 0xff
	if _, err := decodeBody(stored[:len(stored)-3]); err == nil {
		t.Fatal("a truncated frame decoded")
	}
}

// TestStoredBodies: a document Apply compresses is stored compressed in both
// sl_changes and sl_documents and reads back as its JSON from ChangesAfter, ScanShard
// and GetRecord; a small one is stored as it is; and rows holding plain JSON (as every
// row did before codecs) read as they are.
func TestStoredBodies(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		mustCreateIndex(t, st, "z")
		big, small, legacy := benchBody(7), `{"title":"small"}`, benchBody(8)
		mustApply(t, st, upsert("z", 0, "big", big), upsert("z", 0, "small", small), upsert("z", 0, "legacy", legacy))
		s := engine(st)
		raw := func(q, id string) []byte {
			var b []byte
			if err := s.r.QueryRowContext(ctx, rebind(s, q), id).Scan(&b); err != nil {
				t.Fatalf("%s %s: %v", q, id, err)
			}
			return b
		}
		for _, q := range []string{"SELECT body FROM sl_documents WHERE id = ?", "SELECT payload FROM sl_changes WHERE id = ?"} {
			if b := raw(q, "big"); b[0] != codecZstd || len(b) >= len(big) {
				t.Errorf("%s: big stored as %d bytes from %d, codec %d", q, len(b), len(big), b[0])
			}
			if b := raw(q, "small"); string(b) != small {
				t.Errorf("%s: small stored as %q", q, b)
			}
		}
		var plain any = []byte(legacy)
		if h.dialect == "sqlite" {
			plain = legacy // as text, the way rows were written before codecs
		}
		for _, q := range []string{"UPDATE sl_documents SET body = ? WHERE id = ?", "UPDATE sl_changes SET payload = ? WHERE id = ?"} {
			if _, err := s.w.ExecContext(ctx, rebind(s, q), plain, "legacy"); err != nil {
				t.Fatal(err)
			}
		}
		want := map[string]string{"big": big, "small": small, "legacy": legacy}
		checkBodies(t, st, ShardID{Index: "z"}, want)
	})
}

// checkBodies checks that every read path returns want's bodies.
func checkBodies(t *testing.T, st Store, shard ShardID, want map[string]string) {
	t.Helper()
	ctx := context.Background()
	got := map[string]string{}
	changes := allChanges(t, st, shard)
	for i := range changes {
		got[changes[i].ID] = string(changes[i].Payload)
	}
	for id, body := range want {
		if got[id] != body {
			t.Errorf("ChangesAfter %s: %.60q, want %.60q", id, got[id], body)
		}
	}
	records, _ := scanAll(t, st, shard)
	for id, body := range want {
		if r := records["d:"+id]; string(r.Body) != body {
			t.Errorf("ScanShard %s: %.60q, want %.60q", id, r.Body, body)
		}
		r, err := st.(RecordReader).GetRecord(ctx, RecordDocument, shard, id) //nolint:errcheck // every store reads records
		if err != nil || string(r.Body) != body {
			t.Errorf("GetRecord %s: %v %.60q, want %.60q", id, err, r.Body, body)
		}
	}
}

// TestMigrateStoredBodies: a database at schema v1, whose bodies and payloads are
// text, migrates to stored bodies with every row reading as it did, and takes
// compressed bodies from then on.
func TestMigrateStoredBodies(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		url := h.url
		if h.dialect == "sqlite" {
			url = sqliteURL(filepath.Join(t.TempDir(), "v1.db"))
		}
		st, err := Open(ctx, url, quiet)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = st.Close() })
		s := engine(st)
		all := s.d.Migrations
		v1, err := fs.ReadFile(all, "0001_init.sql")
		if err != nil {
			t.Fatal(err)
		}
		s.d.Migrations = fstest.MapFS{"0001_init.sql": {Data: v1}}
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("migrate to v1: %v", err)
		}
		meta, err := st.Indexes().Create(ctx, IndexMeta{Name: "old"})
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]string{}
		for i := range 3 {
			id, body := fmt.Sprint("d", i), benchBody(100+i)
			want[id] = body
			if _, err := s.w.ExecContext(ctx, rebind(s, "INSERT INTO sl_changes (seq, index_name, shard, kind, id, payload, at, index_uid, mapping_version) VALUES (?, ?, 0, 'upsert', ?, ?, 0, ?, 1)"),
				i+1, "old", id, body, meta.UID); err != nil {
				t.Fatal(err)
			}
			if _, err := s.w.ExecContext(ctx, rebind(s, "INSERT INTO sl_documents (index_name, shard, id, body, seq) VALUES (?, 0, ?, ?, ?)"), "old", id, body, i+1); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.w.ExecContext(ctx, "UPDATE sl_counter SET value = 3 WHERE id = 1"); err != nil {
			t.Fatal(err)
		}

		s.d.Migrations = all
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("migrate to stored bodies: %v", err)
		}
		shard := ShardID{Index: "old"}
		checkBodies(t, st, shard, want)
		want["new"] = benchBody(200)
		mustApply(t, st, upsert("old", 0, "new", want["new"]))
		checkBodies(t, st, shard, want)
		var stored []byte
		if err := s.r.QueryRowContext(ctx, rebind(s, "SELECT body FROM sl_documents WHERE id = ?"), "new").Scan(&stored); err != nil || stored[0] != codecZstd {
			t.Fatalf("a new body after the migration: %v, codec %d", err, stored[0])
		}
	})
}
