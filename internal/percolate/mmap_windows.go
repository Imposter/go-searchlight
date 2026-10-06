//go:build windows

package percolate

import (
	"errors"
	"os"
	"reflect"
	"unsafe"

	"golang.org/x/sys/windows"
)

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
	// A reflect.SliceHeader is the one pattern the unsafe package documents for turning
	// a raw OS address (not derived from a Go pointer) into a slice.
	var data []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&data)) //nolint:staticcheck // sanctioned use, see above
	hdr.Data, hdr.Len, hdr.Cap = addr, size, size
	return data, func() error {
		return errors.Join(windows.UnmapViewOfFile(addr), windows.CloseHandle(h))
	}, nil
}
