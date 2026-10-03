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

func (emptyMapping) Data() []byte { return nil }
func (emptyMapping) Close() error { return nil }

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

// release drops one reference, unmapping and closing the file when it reaches zero.
func (m *mapping) release() error {
	if m.refs.Add(-1) != 0 {
		return nil
	}
	err := m.h.Close()
	if cerr := m.f.Close(); err == nil {
		err = cerr
	}
	return err
}
