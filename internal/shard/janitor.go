package shard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Imposter/go-searchlight/internal/segment"
)

// janitor closes retired segments and removes files the shard no longer needs, off the
// readers' path (Release only hands it work).
//
// Removal is deferred and retried: a merged-away segment's files are removed only after
// its final close (the last generation listing it was released, so it is unmapped), and
// a removal that fails anyway (Windows refuses to delete a file another process, an
// antivirus scanner say, has open) stays pending and is retried every
// Options.DeleteRetry, then once more at Close. What is still left then is collected at
// the next Open, which removes every file the manifest does not reference.
type janitor struct {
	s      *Shard
	remove func(path string) error

	mu      sync.Mutex
	retired []*segRef
	pending map[string]bool // files to remove
	running bool
	wake    chan struct{}
	stopCh  chan struct{}
	done    chan struct{}
}

func newJanitor(s *Shard) *janitor {
	j := &janitor{s: s, remove: os.Remove, pending: map[string]bool{}, wake: make(chan struct{}, 1)}
	if s.opts.hooks != nil && s.opts.hooks.remove != nil {
		j.remove = s.opts.hooks.remove
	}
	return j
}

func (j *janitor) start() {
	j.mu.Lock()
	j.running = true
	j.stopCh = make(chan struct{})
	j.done = make(chan struct{})
	j.mu.Unlock()
	go j.loop()
}

// stop ends the loop and does its work one last time.
func (j *janitor) stop() {
	j.mu.Lock()
	running := j.running
	j.running = false
	j.mu.Unlock()
	if running {
		close(j.stopCh)
		<-j.done
	}
	j.drain()
}

// retire hands over a segment no generation lists any more.
func (j *janitor) retire(ref *segRef) {
	j.mu.Lock()
	j.retired = append(j.retired, ref)
	running := j.running
	j.mu.Unlock()
	if running {
		wake(j.wake)
	} else {
		j.drain()
	}
}

// removeLater queues files for removal.
func (j *janitor) removeLater(paths ...string) {
	if len(paths) == 0 {
		return
	}
	j.mu.Lock()
	for _, p := range paths {
		j.pending[p] = true
	}
	running := j.running
	j.mu.Unlock()
	if running {
		wake(j.wake)
	} else {
		j.drain()
	}
}

func (j *janitor) loop() {
	defer close(j.done)
	t := time.NewTicker(j.s.opts.DeleteRetry)
	defer t.Stop()
	for {
		select {
		case <-j.stopCh:
			return
		case <-j.wake:
		case <-t.C:
		}
		j.drain()
	}
}

// drain closes every retired segment, queues the files of obsolete ones, and tries
// every pending removal once.
func (j *janitor) drain() {
	j.mu.Lock()
	retired := j.retired
	j.retired = nil
	j.mu.Unlock()
	for _, ref := range retired {
		if err := ref.close(); err != nil {
			j.s.log.Warn("closing a retired segment failed", slog.String("segment", ref.id), slog.Any("error", err))
		}
		j.s.opts.FilterCache.DropSegment(ref.id)
		if ref.obsolete.Load() {
			files := segmentFiles(j.s.dir, ref.id)
			j.mu.Lock()
			for _, p := range files {
				j.pending[p] = true
			}
			j.mu.Unlock()
		}
	}

	j.mu.Lock()
	paths := make([]string, 0, len(j.pending))
	for p := range j.pending {
		paths = append(paths, p)
	}
	j.mu.Unlock()
	for _, p := range paths {
		err := j.remove(p)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			j.s.log.Debug("file removal deferred", slog.String("file", p), slog.Any("error", err))
			continue
		}
		j.mu.Lock()
		delete(j.pending, p)
		j.mu.Unlock()
	}
}

// pendingFiles returns the files still waiting to be removed (for tests).
func (j *janitor) pendingFiles() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]string, 0, len(j.pending))
	for p := range j.pending {
		out = append(out, p)
	}
	return out
}

// segmentFiles lists every file of segment id in dir: its segment or query files and
// its deletes sidecars, all named id + "." + something.
func segmentFiles(dir, id string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), id+".") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	return out
}

// collectGarbage removes every shard file in the directory that man does not
// reference: segments, query segments and sidecars a crash left before (or after) the
// commit that would have listed (or dropped) them, and their temp files. A shard file
// is one named as the shard names its files (a segment name, 32 hex digits, then a
// dot; or manifest.tmp): anything else, and every directory, is left alone and
// reported, so a data directory pointed somewhere wrong loses nothing. A file that
// cannot be removed now is retried by the janitor.
func (s *Shard) collectGarbage(ctx context.Context, man *manifest) error {
	keep := map[string]bool{manifestName: true}
	queryIDs := map[string]bool{}
	for _, ms := range man.Segments {
		keep[ms.ID+segment.FileExt] = true
		if ms.DelGen > 0 {
			keep[deletesName(ms.ID, ms.DelGen)] = true
		}
	}
	for _, ms := range man.QuerySegments {
		queryIDs[ms.ID] = true
		if ms.DelGen > 0 {
			keep[deletesName(ms.ID, ms.DelGen)] = true
		}
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("shard: %w", err)
	}
	var removed, foreign []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || keep[name] || isQueryFile(name, queryIDs) {
			continue
		}
		if !isShardFile(name) {
			foreign = append(foreign, name)
			continue
		}
		path := filepath.Join(s.dir, name)
		if err := s.jan.remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			s.jan.mu.Lock()
			s.jan.pending[path] = true
			s.jan.mu.Unlock()
			continue
		}
		removed = append(removed, name)
	}
	if len(removed) > 0 {
		s.log.InfoContext(ctx, "removed files the manifest does not reference", slog.Any("files", removed))
	}
	if len(foreign) > 0 {
		s.log.WarnContext(ctx, "the shard directory holds files the shard did not write; left alone", slog.Any("files", foreign))
	}
	return nil
}

// segmentNameLen is the length of a segment name: 16 random bytes, hex-encoded.
const segmentNameLen = 32

// isShardFile reports whether name is one the shard writes: a segment's own file (its
// name, a dot, anything) or the manifest's temp file.
func isShardFile(name string) bool {
	if name == manifestName+".tmp" {
		return true
	}
	if len(name) <= segmentNameLen || name[segmentNameLen] != '.' {
		return false
	}
	for _, c := range name[:segmentNameLen] {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// isQueryFile reports whether name is one of a referenced query segment's own files
// (not a sidecar, which keep decides, nor a temp file).
func isQueryFile(name string, queryIDs map[string]bool) bool {
	dot := strings.IndexByte(name, '.')
	if dot <= 0 || !queryIDs[name[:dot]] {
		return false
	}
	return !strings.HasSuffix(name, deletesExt) && !strings.HasSuffix(name, ".tmp")
}

// deletesExt is segment.WriteDeletes' sidecar extension.
const deletesExt = ".del"

// deletesName is segment.WriteDeletes' sidecar file name for segment id's gen.
func deletesName(id string, gen uint64) string {
	return fmt.Sprintf("%s.%d%s", id, gen, deletesExt)
}
