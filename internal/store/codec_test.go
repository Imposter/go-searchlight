package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"
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
			if off := encodeBody(tc.body, false); off.z != nil || off.plain != string(tc.body) {
				t.Fatalf("compression off: %+v", off)
			}
			stored := encodeBody(tc.body, true)
			if got := stored.z != nil; got != tc.encoded {
				t.Fatalf("encoded %v, want %v (%d -> %d bytes)", got, tc.encoded, len(tc.body), stored.size())
			}
			if tc.encoded && (stored.plain != "" || stored.z[0] != codecZstd || len(stored.z) >= len(tc.body)) {
				t.Fatalf("encoded to %q and %d bytes from %d", stored.plain, len(stored.z), len(tc.body))
			}
			back, err := readBody([]byte(stored.plain), stored.z)
			if err != nil || !bytes.Equal(back, tc.body) {
				t.Fatalf("decode: %v, %q", err, back)
			}
		})
	}
	if _, err := decodeBody([]byte{0x02, 1, 2}); !errors.Is(err, errUnknownCodec) {
		t.Fatalf("a reserved codec byte: %v", err)
	}
	z := encodeBody(product, true).z
	flipped := bytes.Clone(z)
	flipped[len(flipped)/2] ^= 0xff
	if _, err := decodeBody(flipped); err == nil {
		t.Fatal("a frame with a flipped byte decoded")
	}
	if _, err := decodeBody(z[:len(z)-3]); err == nil {
		t.Fatal("a truncated frame decoded")
	}
}

// TestStoredBodies: with compression off, every body is stored as text; on, a body
// that gains is stored in the codec column and the text left empty, and a small one
// stays text. Every read path returns the JSON.
func TestStoredBodies(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		st := h.open(t)
		mustCreateIndex(t, st, "z")
		shard := ShardID{Index: "z"}
		big, small := benchBody(7), `{"title":"small"}`
		mustApply(t, st, upsert("z", 0, "off", big))
		if c := columns(t, st, "off"); c.docText != big || c.docZ != nil || c.changeText != big || c.changeZ != nil {
			t.Fatalf("compression off: %+v", c)
		}
		st.CompressBodies(true)
		mustApply(t, st, upsert("z", 0, "big", big), upsert("z", 0, "small", small))
		if c := columns(t, st, "big"); c.docText != "" || c.changeText != "" || c.docZ[0] != codecZstd || !bytes.Equal(c.docZ, c.changeZ) || len(c.docZ) >= len(big) {
			t.Errorf("big stored as %q/%q and %d/%d bytes from %d", c.docText, c.changeText, len(c.docZ), len(c.changeZ), len(big))
		}
		if c := columns(t, st, "small"); c.docText != small || c.docZ != nil || c.changeText != small || c.changeZ != nil {
			t.Errorf("small stored as %+v", c)
		}
		checkBodies(t, st, shard, map[string]string{"off": big, "big": big, "small": small})
	})
}

// TestMixedVersionWrites: during a rolling upgrade, nodes from before codecs write
// documents and changes through the v1 statements, which know nothing of the codec
// columns; a compressed row they overwrite keeps a stale body_z beside its new text,
// and the text is what every read returns. Their heartbeats leave body_codecs at 0.
func TestMixedVersionWrites(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		st.CompressBodies(true)
		meta, err := st.Indexes().Create(ctx, IndexMeta{Name: "mix"})
		if err != nil {
			t.Fatal(err)
		}
		shard := ShardID{Index: "mix"}
		want := map[string]string{"a": benchBody(1), "b": benchBody(2)}
		mustApply(t, st, upsert("mix", 0, "a", want["a"]), upsert("mix", 0, "b", want["b"]))
		if c := columns(t, st, "a"); c.docZ == nil {
			t.Fatal("not compressed")
		}
		old := oldStatements(h.dialect)
		s := engine(st)
		want["a"], want["c"] = benchBody(11), benchBody(12)
		seq := counterValue(t, st)
		for _, w := range []struct{ id, body string }{{"a", want["a"]}, {"c", want["c"]}} {
			seq++
			if _, err := s.w.ExecContext(ctx, rebind(s, old.change), seq, "mix", w.id, w.body, meta.UID); err != nil {
				t.Fatal(err)
			}
			if _, err := s.w.ExecContext(ctx, rebind(s, old.document), "mix", w.id, w.body, seq); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.w.ExecContext(ctx, rebind(s, "UPDATE sl_counter SET value = ? WHERE id = 1"), seq); err != nil {
			t.Fatal(err)
		}
		if c := columns(t, st, "a"); c.docText != want["a"] || c.docZ == nil {
			t.Fatalf("an old node's overwrite: %+v", c)
		}
		checkBodies(t, st, shard, want)
		changes := allChanges(t, st, shard)
		if got := string(changes[len(changes)-2].Payload); got != want["a"] {
			t.Fatalf("the old node's change reads %.40q", got)
		}

		if _, err := s.w.ExecContext(ctx, rebind(s, old.heartbeat), "old", "10.0.0.1:9200", "0.9", 1); err != nil {
			t.Fatal(err)
		}
		if err := st.Registry().Heartbeat(ctx, Node{ID: "new", Version: "1.0", BodyCodecs: BodyCodecs}); err != nil {
			t.Fatal(err)
		}
		nodes, err := st.Registry().Nodes(ctx)
		if err != nil || len(nodes) != 2 || nodes[0].ID != "new" || nodes[0].BodyCodecs != BodyCodecs || nodes[1].BodyCodecs != 0 {
			t.Fatalf("nodes %+v %v", nodes, err)
		}
	})
}

// v1Statements are the v1 binaries' writes of a change, a document and a heartbeat.
type v1Statements struct{ change, document, heartbeat string }

func oldStatements(dialect string) v1Statements {
	s := v1Statements{
		change:    "INSERT INTO sl_changes (seq, index_name, shard, kind, id, payload, at, index_uid, mapping_version) VALUES (?, ?, 0, 'upsert', ?, ?, 0, ?, 1)",
		document:  "INSERT INTO sl_documents (index_name, shard, id, body, seq) VALUES (?, 0, ?, ?, ?) ON CONFLICT (index_name, shard, id) DO UPDATE SET body = excluded.body, seq = excluded.seq",
		heartbeat: "INSERT INTO sl_nodes (node_id, address, version, capacity, heartbeat_at, started_at) VALUES (?, ?, ?, ?, 0, 0) ON CONFLICT (node_id) DO UPDATE SET address = excluded.address",
	}
	if dialect == "mysql" {
		s.document = "INSERT INTO sl_documents (index_name, shard, id, body, seq) VALUES (?, 0, ?, ?, ?) AS new ON DUPLICATE KEY UPDATE body = new.body, seq = new.seq"
		s.heartbeat = "INSERT INTO sl_nodes (node_id, address, version, capacity, heartbeat_at, started_at) VALUES (?, ?, ?, ?, 0, 0) AS new ON DUPLICATE KEY UPDATE address = new.address"
	}
	return s
}

// storedColumns is how a document and its newest change are stored.
type storedColumns struct {
	docText, changeText string
	docZ, changeZ       []byte
}

func columns(t *testing.T, st Store, id string) storedColumns {
	t.Helper()
	s := engine(st)
	var c storedColumns
	if err := s.r.QueryRowContext(context.Background(), rebind(s, "SELECT body, body_z FROM sl_documents WHERE id = ?"), id).Scan(&c.docText, &c.docZ); err != nil {
		t.Fatalf("document %s: %v", id, err)
	}
	q := "SELECT payload, payload_z FROM sl_changes WHERE id = ? ORDER BY seq DESC LIMIT 1"
	if err := s.r.QueryRowContext(context.Background(), rebind(s, q), id).Scan(&c.changeText, &c.changeZ); err != nil {
		t.Fatalf("change %s: %v", id, err)
	}
	return c
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

// TestMigrateBodyCodecs: a database at schema v1 migrates to v2 with every row
// reading as it did; writes stay text until compression is turned on, and the v1
// statements still write afterwards.
func TestMigrateBodyCodecs(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st, all := openV1(t, h)
		s := engine(st)
		meta, err := st.Indexes().Create(ctx, IndexMeta{Name: "old"})
		if err != nil {
			t.Fatal(err)
		}
		old := oldStatements(h.dialect)
		want := map[string]string{}
		for i := range 3 {
			id, body := fmt.Sprint("d", i), benchBody(100+i)
			want[id] = body
			if _, err := s.w.ExecContext(ctx, rebind(s, old.change), i+1, "old", id, body, meta.UID); err != nil {
				t.Fatal(err)
			}
			if _, err := s.w.ExecContext(ctx, rebind(s, old.document), "old", id, body, i+1); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := s.w.ExecContext(ctx, "UPDATE sl_counter SET value = 3 WHERE id = 1"); err != nil {
			t.Fatal(err)
		}

		s.d.Migrations = all
		if err := st.Migrate(ctx); err != nil {
			t.Fatalf("migrate to body codecs: %v", err)
		}
		shard := ShardID{Index: "old"}
		checkBodies(t, st, shard, want)
		want["plain"] = benchBody(200)
		mustApply(t, st, upsert("old", 0, "plain", want["plain"]))
		if c := columns(t, st, "plain"); c.docZ != nil || c.docText == "" {
			t.Fatalf("a body before compression is on: %+v", c)
		}
		st.CompressBodies(true)
		want["new"] = benchBody(201)
		mustApply(t, st, upsert("old", 0, "new", want["new"]))
		if c := columns(t, st, "new"); c.docZ == nil || c.docText != "" {
			t.Fatalf("a body after compression is on: %+v", c)
		}
		want["late"] = benchBody(202)
		if _, err := s.w.ExecContext(ctx, rebind(s, old.change), 6, "old", "late", want["late"], meta.UID); err != nil {
			t.Fatal(err)
		}
		if _, err := s.w.ExecContext(ctx, rebind(s, old.document), "old", "late", want["late"], 6); err != nil {
			t.Fatal(err)
		}
		if _, err := s.w.ExecContext(ctx, "UPDATE sl_counter SET value = 6 WHERE id = 1"); err != nil {
			t.Fatal(err)
		}
		checkBodies(t, st, shard, want)
	})
}

// TestMigrateRetriesLockTimeout: a migration whose DDL waits past its lock timeout
// behind a transaction using the table gives up, so it does not hold the table's
// other queries behind it, and is retried until the transaction ends.
func TestMigrateRetriesLockTimeout(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		if h.dialect == "sqlite" {
			t.Skip("SQLite's DDL takes the writer lock, which excludes other writers rather than queueing behind readers")
		}
		ctx := context.Background()
		logs := &captureHandler{}
		st, all := openV1(t, h, WithLogger(slog.New(logs)))
		s := engine(st)
		s.d.Migrations = all
		if h.dialect == "postgres" {
			s.d.Maintenance.MigrateLockTimeout = "SET LOCAL lock_timeout = '100ms'"
		} else {
			s.d.Maintenance.MigrateLockTimeout = "SET SESSION lock_wait_timeout = 1"
		}
		blocker, err := s.r.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		var n int
		if err := blocker.QueryRowContext(ctx, "SELECT COUNT(*) FROM sl_documents").Scan(&n); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- st.Migrate(ctx) }()
		time.Sleep(2500 * time.Millisecond)
		select {
		case err := <-done:
			t.Fatalf("migrated while a transaction held the table: %v", err)
		default:
		}
		if err := blocker.Commit(); err != nil {
			t.Fatal(err)
		}
		if err := <-done; err != nil {
			t.Fatalf("migrate: %v", err)
		}
		if !logs.has("a migration timed out waiting for a table lock") {
			t.Fatal("the migration never timed out")
		}
		mustCreateIndex(t, st, "after")
		mustApply(t, st, upsert("after", 0, "a", benchBody(1)))
	})
}

// openV1 opens a store on a database migrated to schema v1 only, and returns the
// dialect's every migration as well.
func openV1(t *testing.T, h *harness, opts ...Option) (Store, fs.FS) {
	t.Helper()
	url := h.url
	if h.dialect == "sqlite" {
		url = sqliteURL(filepath.Join(t.TempDir(), "v1.db"))
	}
	st, err := Open(context.Background(), url, append([]Option{quiet}, opts...)...)
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
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate to v1: %v", err)
	}
	return st, all
}

// TestCorruptBody: a stored body that does not decode is reported by every read as a
// *CorruptError naming its document and seq; ChangesAfter returns the changes before
// it with the error. Writing the document again replaces the row.
func TestCorruptBody(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		st := h.open(t)
		st.CompressBodies(true)
		mustCreateIndex(t, st, "bad")
		shard := ShardID{Index: "bad"}
		mustApply(t, st, upsert("bad", 0, "a", repetitive(1)), upsert("bad", 0, "b", repetitive(2)), upsert("bad", 0, "c", repetitive(3)))
		s := engine(st)
		unknown := []byte{0x02, 1, 2, 3}
		for _, q := range []string{"UPDATE sl_documents SET body_z = ? WHERE id = ?", "UPDATE sl_changes SET payload_z = ? WHERE id = ?"} {
			if _, err := s.w.ExecContext(ctx, rebind(s, q), unknown, "b"); err != nil {
				t.Fatal(err)
			}
		}
		page, err := st.ChangesAfter(ctx, shard, 0, 10)
		var ce *CorruptError
		if !errors.As(err, &ce) || !errors.Is(err, ErrCorrupt) || ce.ID != "b" || len(page) != 1 || page[0].ID != "a" || ce.Seq != page[0].Seq+1 {
			t.Fatalf("ChangesAfter: %d changes, %v", len(page), err)
		}
		seq := ce.Seq
		if _, err := st.ScanShard(ctx, shard, func(Record) error { return nil }); !errors.As(err, &ce) || ce.ID != "b" || ce.Seq != seq {
			t.Fatalf("ScanShard: %v", err)
		}
		if _, err := st.(RecordReader).GetRecord(ctx, RecordDocument, shard, "b"); !errors.As(err, &ce) || ce.ID != "b" || ce.Seq != seq { //nolint:errcheck // every store reads records
			t.Fatalf("GetRecord: %v", err)
		}
		mustApply(t, st, upsert("bad", 0, "b", repetitive(4)))
		r, err := st.(RecordReader).GetRecord(ctx, RecordDocument, shard, "b") //nolint:errcheck // every store reads records
		if err != nil || string(r.Body) != repetitive(4) {
			t.Fatalf("after a rewrite: %v", err)
		}
	})
}

// repetitive is a document zstd always shrinks.
func repetitive(n int) string {
	return fmt.Sprintf(`{"n":%d,"pad":%q}`, n, strings.Repeat(fmt.Sprint("word ", n, " "), 40))
}

// TestFeatures: a feature flag turned on stays on, with the time it was first turned
// on.
func TestFeatures(t *testing.T) {
	forEachDialect(t, func(t *testing.T, h *harness) {
		ctx := context.Background()
		reg := h.open(t).Registry()
		if f, err := reg.Features(ctx); err != nil || len(f) != 0 {
			t.Fatalf("features of a new store: %v %v", f, err)
		}
		if err := reg.EnableFeature(ctx, FeatureZstdBodies); err != nil {
			t.Fatal(err)
		}
		first, err := reg.Features(ctx)
		if err != nil || first[FeatureZstdBodies].IsZero() {
			t.Fatalf("features %v %v", first, err)
		}
		time.Sleep(5 * time.Millisecond)
		if err := reg.EnableFeature(ctx, FeatureZstdBodies); err != nil {
			t.Fatal(err)
		}
		again, err := reg.Features(ctx)
		if err != nil || len(again) != 1 || !again[FeatureZstdBodies].Equal(first[FeatureZstdBodies]) {
			t.Fatalf("enabled twice: %v %v", again, err)
		}
	})
}
