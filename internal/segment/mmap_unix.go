//go:build unix

package segment

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// unixMapping is a read-only mmap on a POSIX system (Linux, macOS, ...), through
// golang.org/x/sys/unix: a pure Go syscall wrapper, no cgo.
type unixMapping struct {
	data []byte
}

func platformMmap(f *os.File, size int64) (mmapHandle, error) {
	data, err := unix.Mmap(int(f.Fd()), 0, int(size), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	return &unixMapping{data: data}, nil
}

func (m *unixMapping) Data() []byte { return m.data }

func (m *unixMapping) Close() error {
	if m.data == nil {
		return nil
	}
	data := m.data
	m.data = nil
	return unix.Munmap(data)
}

// Poison replaces the mapping, in place and atomically, with an anonymous PROT_NONE
// mapping of the same range (mmap with MAP_FIXED): any further read faults with
// SIGSEGV, nothing else can be mapped there (the range stays reserved for the rest of
// the process), and the file's own pages are released, so the file can be removed
// (t.TempDir's cleanup) like any other. See [mmapHandle.Poison].
func (m *unixMapping) Poison() error {
	if m.data == nil {
		return nil
	}
	data := m.data
	m.data = nil
	_, err := unix.MmapPtr(-1, 0, unsafe.Pointer(unsafe.SliceData(data)), uintptr(len(data)),
		unix.PROT_NONE, unix.MAP_FIXED|unix.MAP_PRIVATE|unix.MAP_ANON)
	return err
}

// fsyncDir fsyncs dir itself, making a prior rename (or create, or unlink) of an entry
// inside it durable. Without this, a crash can leave the directory pointing at the old
// file, or at nothing, even though the new file's own bytes were already fsynced.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
