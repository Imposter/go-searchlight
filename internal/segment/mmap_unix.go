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
