//go:build windows

package segment

import (
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

// residentBatch is how many pages one QueryWorkingSetEx call asks about.
const residentBatch = 4096

// residentBytes counts data's pages that are in the process's working set. data is a
// mapped view, so it starts on a page boundary.
func residentBytes(data []byte) (int64, error) {
	if len(data) == 0 {
		return 0, nil
	}
	page := os.Getpagesize()
	pages := (len(data) + page - 1) / page
	infos := make([]windows.PSAPI_WORKING_SET_EX_INFORMATION, min(pages, residentBatch))
	var n int64
	for start := 0; start < pages; start += len(infos) {
		batch := infos[:min(len(infos), pages-start)]
		for i := range batch {
			batch[i] = windows.PSAPI_WORKING_SET_EX_INFORMATION{
				VirtualAddress: windows.Pointer(unsafe.Pointer(&data[(start+i)*page])),
			}
		}
		size := uint32(len(batch)) * uint32(unsafe.Sizeof(batch[0])) //nolint:gosec // at most residentBatch entries of 16 bytes
		if err := windows.QueryWorkingSetEx(windows.CurrentProcess(), uintptr(unsafe.Pointer(&batch[0])), size); err != nil {
			return 0, err
		}
		for _, info := range batch {
			if info.VirtualAttributes.Valid() {
				n += int64(page)
			}
		}
	}
	return min(n, int64(len(data))), nil
}
