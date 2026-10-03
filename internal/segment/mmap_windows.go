//go:build windows

package segment

import (
	"fmt"
	"os"
	"reflect"
	"unsafe"

	"golang.org/x/sys/windows"
)

// windowsMapping is a read-only file mapping through golang.org/x/sys/windows: a pure Go
// syscall wrapper, no cgo.
type windowsMapping struct {
	data []byte
	h    windows.Handle
	addr uintptr
}

func platformMmap(f *os.File, size int64) (mmapHandle, error) {
	h, err := windows.CreateFileMapping(windows.Handle(f.Fd()), nil, windows.PAGE_READONLY, 0, 0, nil)
	if err != nil {
		return nil, err
	}
	addr, err := windows.MapViewOfFile(h, windows.FILE_MAP_READ, 0, 0, uintptr(size)) //nolint:gosec // size is a segment file's length, far under uintptr's range
	if err != nil {
		_ = windows.CloseHandle(h)
		return nil, err
	}
	// Building the slice through a reflect.SliceHeader, rather than unsafe.Slice, is
	// the one pattern the unsafe package documents for turning a raw OS address (not
	// itself derived from a Go pointer) into a slice; see the unsafe.Pointer rules.
	var data []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&data)) //nolint:staticcheck // sanctioned use, see above
	hdr.Data = addr
	hdr.Len = int(size)
	hdr.Cap = int(size)
	return &windowsMapping{data: data, h: h, addr: addr}, nil
}

func (m *windowsMapping) Data() []byte { return m.data }

func (m *windowsMapping) Close() error {
	if m.addr == 0 {
		return nil
	}
	addr := m.addr
	m.addr = 0
	err := windows.UnmapViewOfFile(addr)
	if cerr := windows.CloseHandle(m.h); err == nil {
		err = cerr
	}
	return err
}

// Poison releases the view and the file mapping, then reserves the view's exact
// address range again as PAGE_NOACCESS: any further read raises an access violation,
// and nothing else can be mapped there while the reservation stands (for the rest of
// the process). Unlike leaving the view mapped and VirtualProtect-ing it, this lets
// go of the file itself, so it can be deleted (t.TempDir's cleanup) or renamed over.
//
// Windows has no atomic "replace this view with a reservation", so there is a window
// between UnmapViewOfFile and VirtualAlloc in which another allocation could land in
// the range; VirtualAlloc then fails and Poison reports it, rather than claiming a
// guard it could not place. See [mmapHandle.Poison].
func (m *windowsMapping) Poison() error {
	if m.addr == 0 {
		return nil
	}
	addr := m.addr
	m.addr = 0
	err := windows.UnmapViewOfFile(addr)
	if cerr := windows.CloseHandle(m.h); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	got, err := windows.VirtualAlloc(addr, uintptr(len(m.data)), windows.MEM_RESERVE, windows.PAGE_NOACCESS)
	if err != nil {
		return fmt.Errorf("segment: poisoning %#x: reserving the unmapped range: %w", addr, err)
	}
	if got != addr {
		return fmt.Errorf("segment: poisoning %#x: reservation landed at %#x", addr, got)
	}
	return nil
}

// fsyncDir is a documented no-op on Windows: NTFS does not expose a way to fsync a
// directory handle the way POSIX does, and a rename's directory-entry durability is the
// volume's own journal's job, not the application's. See fsyncDir in mmap_unix.go for
// the real thing.
func fsyncDir(string) error { return nil }
