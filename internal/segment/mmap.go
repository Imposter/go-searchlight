package segment

import (
	"os"
	"sync/atomic"
)

// mmapHandle is a live memory mapping of a file, behind a small platform interface so
// [Open] stays pure Go: mmap_unix.go and mmap_windows.go each implement mmapOpen with no
// cgo.
type mmapHandle interface {
	// Data is the mapping's bytes. Valid until Close.
	Data() []byte
	// Close unmaps. Idempotent.
	Close() error
	// Poison releases the file and its mapping but keeps the mapping's address range
	// reserved as no-access (unix: an anonymous PROT_NONE mmap with MAP_FIXED over
	// it; Windows: UnmapViewOfFile, then a PAGE_NOACCESS VirtualAlloc reservation of
	// the same range), so a stale reference to Data (for example a zero-copy
	// roaring.Bitmap returned by [Reader.Postings], kept past [Reader.Close]) faults
	// immediately and deterministically instead of silently reading whatever the OS
	// later maps at the same address. The file itself is let go, so it can be removed
	// or renamed over as after a real Close. Used only when built with the
	// searchlight_debug tag; it leaks the address range for the rest of the process,
	// which is fine for tests and never used in a release build.
	Poison() error
}

// mmapOpen maps f's first size bytes read-only. size 0 returns an empty, no-op mapping
// (mmap of zero bytes is not portable).
func mmapOpen(f *os.File, size int64) (mmapHandle, error) {
	if size == 0 {
		return emptyMapping{}, nil
	}
	return platformMmap(f, size)
}

type emptyMapping struct{}

func (emptyMapping) Data() []byte  { return nil }
func (emptyMapping) Close() error  { return nil }
func (emptyMapping) Poison() error { return nil }

// mapping is a reference-counted mmap: the file handle and the mapping stay open as
// long as any [Reader] holds a reference (through [Reader.Retain]), and are released
// exactly once, when the last one closes.
type mapping struct {
	f    *os.File
	h    mmapHandle
	data []byte
	refs atomic.Int32
}

func openFileMapping(path string) (*mapping, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	h, err := mmapOpen(f, info.Size())
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	m := &mapping{f: f, h: h, data: h.Data()}
	m.refs.Store(1)
	return m, nil
}

// retain adds one reference.
func (m *mapping) retain() { m.refs.Add(1) }

// release drops one reference, unmapping and closing the file when it reaches zero. A
// searchlight_debug build poisons the mapping instead of unmapping it (see
// [mmapHandle.Poison]), so a reference kept past the last Close faults instead of
// silently reading stale or reused memory; a release build always does the cheap,
// resource-reclaiming thing and unmaps for real.
func (m *mapping) release() error {
	if m.refs.Add(-1) != 0 {
		return nil
	}
	var err error
	if debugGuard {
		err = m.h.Poison()
	} else {
		err = m.h.Close()
	}
	if cerr := m.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// SyncDir fsyncs dir itself, making a prior rename, create or unlink of an entry inside
// it durable (a documented no-op on Windows, where the volume's journal does this; see
// mmap_windows.go). Shards use it after swapping their manifest.
func SyncDir(dir string) error {
	dirSyncs.Add(1)
	return fsyncDir(dir)
}

// SyncFile fsyncs f's contents, counting it in [SyncCounts].
func SyncFile(f *os.File) error {
	fileSyncs.Add(1)
	return f.Sync()
}

// SyncPath fsyncs the file at path, counting it in [SyncCounts]: the file a [Build]
// with NoSync wrote, once the caller needs it durable. It opens the file for writing,
// which Windows requires to flush a file's buffers.
func SyncPath(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := SyncFile(f); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// fileSyncs and dirSyncs count every fsync this package (or a caller through SyncFile
// and SyncDir) asked for, for measuring what a refresh or merge costs in syncs.
var fileSyncs, dirSyncs atomic.Int64

// SyncStats are the fsyncs asked for so far in this process.
type SyncStats struct {
	// Files are file fsyncs; Dirs are directory fsyncs (requested: on Windows they are
	// no-ops, see SyncDir).
	Files, Dirs int64
}

// SyncCounts returns the fsyncs asked for so far, through this package.
func SyncCounts() SyncStats { return SyncStats{Files: fileSyncs.Load(), Dirs: dirSyncs.Load()} }
