//go:build windows

package shard

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows"
)

// holdExclusive opens path with no sharing at all, as a scanner can, or as Windows
// itself does while it renames over the file, and closes it after d.
func holdExclusive(t *testing.T, path string, d time.Duration) {
	t.Helper()
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := windows.CreateFile(p, windows.GENERIC_READ, 0, nil, windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		time.Sleep(d)
		_ = windows.CloseHandle(h)
	}()
}

// The flake: reading the manifest while another handle denies sharing (here held for
// 60 ms) failed at once with ERROR_SHARING_VIOLATION; it is retried instead.
func TestReadManifestWaitsOutASharingViolation(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a")
	h.refresh()
	holdExclusive(t, filepath.Join(h.dir, manifestName), 60*time.Millisecond)
	if _, err := readManifest(h.dir, quietLogger); err != nil {
		t.Fatalf("readManifest while the manifest was held for 60 ms: %v", err)
	}
}

// The retry is bounded: a manifest held for longer than about half a second fails the
// read with the sharing violation, rather than hanging.
func TestReadManifestRetryIsBounded(t *testing.T) {
	h := newHarness(t, testOptions())
	h.upsert("a")
	h.refresh()
	holdExclusive(t, filepath.Join(h.dir, manifestName), 3*time.Second)
	start := time.Now()
	_, err := readManifest(h.dir, quietLogger)
	d := time.Since(start)
	if !errors.Is(err, windows.ERROR_SHARING_VIOLATION) {
		t.Fatalf("readManifest = %v, want the sharing violation", err)
	}
	if d < 400*time.Millisecond || d > 2*time.Second {
		t.Fatalf("gave up after %v, want about half a second", d)
	}
	time.Sleep(3 * time.Second) // let the holder go before the directory is removed
}
