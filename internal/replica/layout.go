package replica

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
)

// A shard copy lives under its root directory (the node's choice, one per copy). At
// first the shard's files are the root's own. A rebuild that can keep the copy serving
// meanwhile builds the new copy in a subdirectory of the root (copyPrefix and a
// number), then makes it current by writing its name to the root's currentFile:
// atomically (a temp file renamed over it), so a crash leaves either the old copy or
// the new one current, never neither. OpenCopy opens whatever is current and removes
// the rest: a half-built copy, or the one a swap replaced.
const (
	currentFile = "CURRENT"
	copyPrefix  = "copy-"
)

// OpenCopy opens the shard copy rooted at root: the directory root's CURRENT names, or
// root itself when there is none. Every other copy directory under root (one a crash
// left half built, or one a swap replaced) is removed first, as are the replaced
// copy's files in root once a subdirectory is current. Open copies through it: a copy
// that has been rebuilt aside does not live in root.
func OpenCopy(ctx context.Context, root string, m *schema.Mapping, opts shard.Options) (*shard.Shard, error) {
	dir, err := CopyDir(root)
	if err != nil {
		return nil, err
	}
	collectCopies(root, dir)
	return shard.Open(ctx, dir, m, opts)
}

// CopyDir returns the directory of root's current copy.
func CopyDir(root string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(root, currentFile)) //nolint:gosec // root is the copy's own directory, the node's choice
	if errors.Is(err, os.ErrNotExist) {
		return root, nil
	}
	if err != nil {
		return "", fmt.Errorf("replica: reading %s: %w", filepath.Join(root, currentFile), err)
	}
	name := strings.TrimSpace(string(raw))
	if !isCopyName(name) {
		return "", fmt.Errorf("replica: %s names %q, not a copy directory", filepath.Join(root, currentFile), name)
	}
	return filepath.Join(root, name), nil
}

// isCopyName reports whether name is one newCopyDir makes.
func isCopyName(name string) bool {
	num, ok := strings.CutPrefix(name, copyPrefix)
	if !ok || num == "" {
		return false
	}
	_, err := strconv.ParseUint(num, 10, 64)
	return err == nil
}

// copyRoot returns the root of the copy whose shard lives in dir.
func copyRoot(dir string) string {
	if isCopyName(filepath.Base(dir)) {
		return filepath.Dir(dir)
	}
	return dir
}

// newCopyDir names a fresh copy directory under root, after now.
func newCopyDir(root string, now time.Time) string {
	n := now.UnixNano()
	for {
		dir := filepath.Join(root, copyPrefix+strconv.FormatInt(n, 10))
		if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
			return dir
		}
		n++
	}
}

// makeCurrent makes dir root's current copy: a temp file renamed over CURRENT, then the
// root synced, so the swap is durable once it returns.
func makeCurrent(root, dir string) error {
	tmp := filepath.Join(root, currentFile+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.WriteString(filepath.Base(dir) + "\n")
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(root, currentFile)); err != nil {
		return err
	}
	return segment.SyncDir(root)
}

// collectCopies removes what root holds besides its current copy, dir: other copy
// directories and, when dir is a subdirectory, the files of the copy that lived in
// root itself. A file still in use (mapped on Windows) is left for the next open.
func collectCopies(root, dir string) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return
	}
	for _, e := range entries {
		name := e.Name()
		path := filepath.Join(root, name)
		switch {
		case path == dir, name == currentFile:
		case e.IsDir() && isCopyName(name):
			_ = os.RemoveAll(path) //nolint:gosec // an entry of the copy's own root, named like a copy directory
		case !e.IsDir() && dir != root:
			_ = os.Remove(path) //nolint:gosec // the replaced root copy's files, and CURRENT.tmp
		}
	}
}

// removeCopy removes a replaced copy: its directory, or, for the copy that lived in
// root itself, its files. Readers may still map its segments (Windows refuses to
// remove those), so it retries in the background for a while; whatever is left is
// removed by the next OpenCopy.
func removeCopy(root, dir string) {
	try := func() bool {
		if dir != root {
			return os.RemoveAll(dir) == nil
		}
		entries, err := os.ReadDir(root)
		if err != nil {
			return true
		}
		ok := true
		for _, e := range entries {
			if !e.IsDir() && e.Name() != currentFile {
				if err := os.Remove(filepath.Join(root, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
					ok = false
				}
			}
		}
		return ok
	}
	if try() {
		return
	}
	go func() {
		delay := 100 * time.Millisecond
		for range 20 {
			time.Sleep(delay) //nolint:forbidigo // waits out other processes' handles on the files: real time is the subject
			if try() {
				return
			}
			delay = min(2*delay, 5*time.Second)
		}
	}()
}
