//go:build linux

package segment

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// residentBytes counts data's pages mincore reports resident. data is a mapping, so
// it starts on a page boundary.
func residentBytes(data []byte) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}
	page := os.Getpagesize()
	vec := make([]byte, (len(data)+page-1)/page)
	_, _, errno := unix.Syscall(unix.SYS_MINCORE,
		uintptr(unsafe.Pointer(unsafe.SliceData(data))), uintptr(len(data)), uintptr(unsafe.Pointer(unsafe.SliceData(vec))))
	if errno != 0 {
		return 0, errno
	}
	var n int64
	for _, v := range vec {
		if v&1 != 0 {
			n += int64(page)
		}
	}
	return min(n, int64(len(data))), nil
}
