package shard

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/Imposter/go-searchlight/internal/segment"
)

// janitor closes retired segments and removes files the shard no longer needs, off the
// readers' path (Release only hands it work).
//
// Removal is deferred and retried: a merged-away segment's files are removed only after
// its final close (the last generation listing it was released, so it is unmapped) and
// the flush that dropped it from the manifest (see flush.go), and a removal that fails
// anyway (Windows refuses to delete a file another process, an antivirus scanner say,
// has open) stays pending and is retried every Options.DeleteRetry, then once more at
// Close. What is still left then is collected at the next Open, which removes every
// shard file the manifest does not reference.
//
// A pending name may be written again: a deletes sidecar is named by its segment and
// generation, and manifest.tmp by nothing at all. So a flush forgets a name
// before writing it ([janitor.forget]), and forget waits out a removal of that name in
// progress (removeMu), so a retry can never remove the new file. Once the shard is
// closed the janitor forgets every pending name: another Shard may reopen the
// directory, and that Shard's garbage collection owns whatever is left.
type janitor struct {
	s      *Shard
	remove func(path string) error

	// removeMu is held across each removal and by forget, so forget returns only
	// once no removal of the forgotten names is running or can start.
	removeMu sync.Mutex

	mu      sync.Mutex
	retired []*segRef
	pending map[string]bool // files to remove
	running bool
	stopped bool
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

// stop ends the loop, does its work one last time, and forgets what is still pending.
// Segments retired after it are still closed, and an obsolete one's files removed if
// they can be, once.
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
	j.removeMu.Lock()
	j.mu.Lock()
	j.stopped = true
	clear(j.pending)
	j.mu.Unlock()
	j.removeMu.Unlock()
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

// forget drops paths from the pending removals, waiting for a removal of any of them
// already under way: call it before writing a file whose name may be pending.
func (j *janitor) forget(paths ...string) {
	j.removeMu.Lock()
	defer j.removeMu.Unlock()
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, p := range paths {
		delete(j.pending, p)
	}
}

func (j *janitor) loop() {
	defer close(j.done)
	t := j.s.opts.Clock.NewTicker(j.s.opts.DeleteRetry)
	defer t.Stop()
	for {
		select {
		case <-j.stopCh:
			return
		case <-j.wake:
		case <-t.C():
		}
		j.drain()
	}
}

// drain closes every retired segment, queues the files of obsolete ones, and tries
// every pending removal once. After stop nothing stays pending.
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
		ref.closed.Store(true)
		if ref.obsolete.Load() {
			// A segment's own name is unique for the directory's life (and no
			// manifest lists an obsolete one again), so its files are safe to remove
			// whenever this runs.
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
	stopped := j.stopped
	j.mu.Unlock()
	for _, p := range paths {
		j.removeOne(p, stopped)
	}
}

// removeOne removes p if it is still pending; on failure it stays pending, unless the
// janitor has stopped.
func (j *janitor) removeOne(p string, stopped bool) {
	j.removeMu.Lock()
	defer j.removeMu.Unlock()
	j.mu.Lock()
	still := j.pending[p]
	j.mu.Unlock()
	if !still {
		return // forgotten (about to be written again), or removed by another drain
	}
	err := j.remove(p)
	if err != nil && !errors.Is(err, os.ErrNotExist) && !stopped {
		j.s.log.Debug("file removal deferred", slog.String("file", p), slog.Any("error", err))
		return
	}
	j.mu.Lock()
	delete(j.pending, p)
	j.mu.Unlock()
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

// maxSidecarGen returns the highest generation any deletes sidecar (or its temp
// file) in dir is named for, 0 when there is none. A crash can leave a sidecar of a
// generation the manifest never reached; the next generation must not reuse its name.
func maxSidecarGen(dir string) (uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("shard: %w", err)
	}
	var top uint64
	for _, e := range entries {
		if gen, ok := sidecarGen(e.Name()); ok {
			top = max(top, gen)
		}
	}
	return top, nil
}

// sidecarGen parses <segment>.<gen>.del or <segment>.<gen>.del.tmp.
func sidecarGen(name string) (uint64, bool) {
	name = strings.TrimSuffix(name, ".tmp")
	if !isShardFile(name) || !strings.HasSuffix(name, deletesExt) {
		return 0, false
	}
	gen, err := strconv.ParseUint(name[segmentNameLen+1:len(name)-len(deletesExt)], 10, 64)
	return gen, err == nil
}

// collectGarbage removes every shard file in the directory that man does not
// reference: segments, query segments and sidecars a crash left before (or after) the
// flush that would have listed (or dropped) them, and their temp files. A shard file
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
	return len(name) > segmentNameLen && name[segmentNameLen] == '.' && isSegmentName(name[:segmentNameLen])
}

// isSegmentName reports whether name is one newSegmentName makes: 32 lowercase hex
// digits.
func isSegmentName(name string) bool {
	if len(name) != segmentNameLen {
		return false
	}
	for _, c := range name {
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
