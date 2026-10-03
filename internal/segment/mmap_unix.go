//go:build unix

package segment

import (
	"os"

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

// Poison switches the mapping to PROT_NONE in place, rather than unmapping it: the
// address range stays reserved (so nothing else can be mapped there) and any further
// read faults with SIGSEGV. See [mmapHandle.Poison].
func (m *unixMapping) Poison() error {
	if m.data == nil {
		return nil
	}
	return unix.Mprotect(m.data, unix.PROT_NONE)
}

// fsyncDir fsyncs dir itself, making a prior rename (or create, or unlink) of an entry
// inside it durable. Without this, a crash can leave the directory pointing at the old
// file, or at nothing, even though the new file's own bytes were already fsynced.
func fsyncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // dir is an operator-controlled path
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
