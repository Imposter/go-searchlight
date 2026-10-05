package sqlite

import (
	"database/sql"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestPendingLogReadsTheWALIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.db")
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=wal_autocheckpoint(0)")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("CREATE TABLE t (v BLOB)"); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		if _, err := db.Exec("INSERT INTO t VALUES (zeroblob(8000))"); err != nil {
			t.Fatal(err)
		}
	}
	if _, ok, err := readWALIndex(path + "-shm"); err != nil || !ok {
		t.Fatalf("SQLite's own wal-index was not recognised: %v", err)
	}
	pending, err := PendingLog(path)
	if err != nil || pending == 0 {
		t.Fatalf("pending %d, %v: the inserts are in the log", pending, err)
	}
	if _, err := db.Exec("PRAGMA wal_checkpoint(PASSIVE)"); err != nil {
		t.Fatal(err)
	}
	if pending, err := PendingLog(path); err != nil || pending != 0 {
		t.Fatalf("pending %d, %v after a checkpoint copied every frame", pending, err)
	}
}

func TestPendingLogFallsBackToTheLogFileSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.db")
	if err := os.WriteFile(path+"-wal", make([]byte, 12345), 0o600); err != nil {
		t.Fatal(err)
	}
	header := func(version uint32, torn bool) []byte {
		b := make([]byte, 136)
		for _, off := range []int{0, walIndexHdrSize} {
			binary.NativeEndian.PutUint32(b[off:], version)
			binary.NativeEndian.PutUint16(b[off+offPageSize:], 4096)
			binary.NativeEndian.PutUint32(b[off+offMaxFrame:], 10)
		}
		if torn {
			binary.NativeEndian.PutUint32(b[walIndexHdrSize+offMaxFrame:], 11)
		}
		binary.NativeEndian.PutUint32(b[offBackfill:], 4)
		return b
	}
	for _, tc := range []struct {
		name string
		shm  []byte
		want int64
	}{
		{"no wal-index", nil, 12345},
		{"short wal-index", make([]byte, 10), 12345},
		{"another version", header(1, false), 12345},
		{"caught mid-update", header(walIndexVersion, true), 12345},
		{"readable", header(walIndexVersion, false), 6 * 4096},
	} {
		_ = os.Remove(path + "-shm")
		if tc.shm != nil {
			if err := os.WriteFile(path+"-shm", tc.shm, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if got, err := PendingLog(path); err != nil || got != tc.want {
			t.Errorf("%s: %d, %v; want %d", tc.name, got, err, tc.want)
		}
	}
}
