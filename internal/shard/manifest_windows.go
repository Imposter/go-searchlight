//go:build windows

package shard

import (
	"errors"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

// openShared opens path for reading with every sharing mode, FILE_SHARE_DELETE
// included, so the open never stops the shard (or anyone) renaming a new manifest over
// the file: os.Open leaves FILE_SHARE_DELETE out, and while such a handle is open
// Windows refuses the rename.
func openShared(path string) (*os.File, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	h, err := windows.CreateFile(p, windows.GENERIC_READ,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(h), path), nil
}

// fileRenameInfoEx is FILE_RENAME_INFO_EX: Flags, RootDirectory, FileNameLength, then
// the name (the same layout and alignment as the C struct).
type fileRenameInfoEx struct {
	Flags          uint32
	RootDirectory  windows.Handle
	FileNameLength uint32
	FileName       [1]uint16
}

// replaceFile renames from over to with POSIX semantics (FileRenameInfoEx with
// FILE_RENAME_FLAG_POSIX_SEMANTICS): the rename succeeds even while readers that
// allow FILE_SHARE_DELETE (openShared) hold the old file, which they go on reading,
// as on Linux. os.Rename's MoveFileEx refuses whenever any handle is open on the
// target. A volume without POSIX rename support (FAT, older Windows) falls back to
// os.Rename.
func replaceFile(from, to string) error {
	err := posixRename(from, to)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, windows.ERROR_INVALID_PARAMETER), errors.Is(err, windows.ERROR_NOT_SUPPORTED),
		errors.Is(err, windows.ERROR_INVALID_FUNCTION):
		return os.Rename(from, to)
	default:
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err}
	}
}

func posixRename(from, to string) error {
	abs, err := filepath.Abs(to)
	if err != nil {
		return err
	}
	src, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return err
	}
	name, err := windows.UTF16FromString(abs)
	if err != nil {
		return err
	}
	h, err := windows.CreateFile(src, windows.DELETE|windows.SYNCHRONIZE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
		nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(h) //nolint:errcheck // a close failure after the rename changes nothing
	var info fileRenameInfoEx
	size := int(unsafe.Offsetof(info.FileName)) + len(name)*2 // name ends with its NUL
	buf := make([]byte, size)
	p := (*fileRenameInfoEx)(unsafe.Pointer(&buf[0]))
	p.Flags = windows.FILE_RENAME_REPLACE_IF_EXISTS | windows.FILE_RENAME_POSIX_SEMANTICS
	p.FileNameLength = uint32((len(name) - 1) * 2) //nolint:gosec // a path's length
	copy(unsafe.Slice(&p.FileName[0], len(name)), name)
	return windows.SetFileInformationByHandle(h, windows.FileRenameInfoEx, &buf[0], uint32(size)) //nolint:gosec // a path's length
}

// retryableIO reports whether err is another handle being in the way for now (a
// scanner, a reader that did not allow FILE_SHARE_DELETE, a rename in progress), worth
// a short retry.
func retryableIO(err error) bool {
	return errors.Is(err, windows.ERROR_SHARING_VIOLATION) || errors.Is(err, windows.ERROR_ACCESS_DENIED) ||
		errors.Is(err, windows.ERROR_LOCK_VIOLATION)
}
