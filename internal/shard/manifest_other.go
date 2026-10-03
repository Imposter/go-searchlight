//go:build !windows

package shard

import "os"

// openShared opens path for reading. A POSIX system never stops a rename over an open
// file.
func openShared(path string) (*os.File, error) { return os.Open(path) }

// replaceFile renames from over to, atomically.
func replaceFile(from, to string) error { return os.Rename(from, to) }

// retryableIO reports whether err is worth a retry: never, on a POSIX system.
func retryableIO(error) bool { return false }
