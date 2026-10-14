//go:build windows

package percolate

import (
	"errors"
	"os"
	"reflect"
	"unsafe"

	"golang.org/x/sys/windows"
)

// platformMap maps f's first size bytes read-only. The slice is built through a
// reflect.SliceHeader: that is the one pattern the unsafe package documents for
// turning a raw OS address (not derived from a Go pointer) into a slice.
func platformMap(f *os.File, size int) ([]byte, func() error, error) {
	h, err := windows.CreateFileMapping(windows.Handle(f.Fd()), nil, windows.PAGE_READONLY, 0, 0, nil)
	if err != nil {
		return nil, nil, err
	}
	addr, err := windows.MapViewOfFile(h, windows.FILE_MAP_READ, 0, 0, uintptr(size))
	if err != nil {
		_ = windows.CloseHandle(h)
		return nil, nil, err
	}
	var data []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&data)) //nolint:staticcheck // the sanctioned use: see the doc comment
	hdr.Data, hdr.Len, hdr.Cap = addr, size, size
	return data, func() error {
		return errors.Join(windows.UnmapViewOfFile(addr), windows.CloseHandle(h))
	}, nil
}
