//go:build windows

package segment

import (
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
