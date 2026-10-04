package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Imposter/go-searchlight/internal/api"
	"github.com/Imposter/go-searchlight/internal/replica"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// Peer recovery (spec section 9): a copy that must be rebuilt fetches a serving peer's
// copy of the shard, file by file, then replays the changelog after it.
//
//  1. The recovering node asks a serving peer (best first, by adaptive replica
//     selection) for a snapshot: the peer holds one generation of its copy and lists
//     its files with their sizes and SHA-256 sums (shard.Snapshot: segment files,
//     deletes sidecars encoded from the held generation, and a manifest built from
//     it, last).
//  2. Each file but the manifest is streamed into a staging directory beside the copy
//     (data_dir/recovery), as name.part, and checked against its sum before it is
//     renamed to its name and fsynced. A transfer cut short resumes from the bytes
//     already written (an HTTP Range request) on the same snapshot; a file that fails
//     its sum is fetched again from the start. A file already staged with the right
//     size and sum (from an attempt that failed later, even against another peer) is
//     not fetched again, since segment file names are unique and their content never
//     changes.
//  3. Once every file is staged and checked, they are moved into the copy's (empty)
//     directory, and the manifest, fetched and checked last, is written there (a temp
//     file, fsynced, renamed, the directory fsynced): it is the copy's commit point, so
//     a recovery cut short anywhere before it leaves a directory that opens empty.
//
// The tailer then opens the copy and replays the changelog from the snapshot's seq. Any
// failure makes the tailer rebuild the copy from the store's snapshot (ScanShard)
// instead. A copy that is outdated but valid is rebuilt aside, so it keeps serving
// while its replacement is fetched (replica's aside rebuild).

// Fetch attempts and retries.
const (
	// fetchAttempts is how many times one source is tried before the next.
	fetchAttempts = 3
	// fileAttempts is how many times one file is resumed or refetched within an
	// attempt.
	fileAttempts = 4
)

// errNoSource is a recovery with no serving peer to fetch from.
var errNoSource = fmt.Errorf("cluster: no serving peer holds a copy of the shard: %w", replica.ErrNoSource)

// errEmptySource is a peer whose copy holds nothing yet.
var errEmptySource = fmt.Errorf("cluster: the peer's copy is empty: %w", replica.ErrNoSource)

// fetcher is the replica.Fetcher: peer recovery.
type fetcher struct {
	n *Node
	// bytes and resumes count what recoveries fetched and how often a file was
	// resumed (tests, benchmarks).
	bytes, resumes atomic.Int64
	// running are the shards recovering now, with the bytes each has fetched (their
	// progress, for the prune leader's stall detection).
	mu      sync.Mutex
	running map[store.ShardID]*atomic.Int64
	fetched map[store.ShardID]*atomic.Int64
	ended   map[store.ShardID]time.Time
}

// begin marks id recovering; the returned func ends it.
func (f *fetcher) begin(id store.ShardID) (*atomic.Int64, func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.running == nil {
		f.running, f.fetched = map[store.ShardID]*atomic.Int64{}, map[store.ShardID]*atomic.Int64{}
	}
	ctr := f.fetched[id]
	if ctr == nil {
		ctr = &atomic.Int64{}
		f.fetched[id] = ctr
	}
	f.running[id] = ctr
	return ctr, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.running, id)
		if f.ended == nil {
			f.ended = map[store.ShardID]time.Time{}
		}
		f.ended[id] = time.Now()
	}
}

// idleSince is when id's last recovery here ended (zero: none this run).
func (f *fetcher) idleSince(id store.ShardID) time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.ended[id]
}

// active reports whether id is recovering from a peer now.
func (f *fetcher) active(id store.ShardID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.running[id] != nil
}

// progressOf is the bytes the latest recovery attempt of id has fetched on this node.
func (f *fetcher) progressOf(id store.ShardID) int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c := f.fetched[id]; c != nil {
		return c.Load()
	}
	return 0
}

var _ replica.Fetcher = (*fetcher)(nil)

// Fetch implements replica.Fetcher: it fills dir with a serving peer's copy of id.
func (f *fetcher) Fetch(ctx context.Context, id store.ShardID, dir string) error {
	n := f.n
	cands := n.candidates(id)
	if len(cands) == 0 {
		return errNoSource
	}
	staging := n.stagingDir(id)
	progress, end := f.begin(id)
	defer end()
	ctx = context.WithValue(ctx, progressKey{}, progress)
	var errs []error
	for _, c := range cands {
		delay := 100 * time.Millisecond
		for attempt := 1; attempt <= fetchAttempts; attempt++ {
			start := time.Now()
			progress.Store(0)
			bytes, err := f.fetchFrom(ctx, c, id, dir, staging)
			if err == nil {
				_ = os.RemoveAll(staging)
				secs := time.Since(start).Seconds()
				n.log.InfoContext(ctx, "shard copy recovered from a peer", slog.String("shard", id.String()), slog.String("peer", c.node),
					slog.Int64("bytes", bytes), slog.Float64("seconds", secs), slog.Float64("mb_per_s", float64(bytes)/(1<<20)/max(secs, 1e-9)))
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			errs = append(errs, err)
			if errors.Is(err, errEmptySource) {
				break // nothing to fetch from this peer: try the next
			}
			n.log.WarnContext(ctx, "a peer recovery attempt failed", slog.String("shard", id.String()), slog.String("peer", c.node),
				slog.Int("attempt", attempt), slog.Any("error", err))
			var ae *api.Error
			if errors.As(err, &ae) && ae.Status < 500 && ae.Status != http.StatusGone && ae.Status != http.StatusTooManyRequests {
				break // the peer cannot serve this: try the next
			}
			sleep(ctx, delay)
			delay *= 2
		}
	}
	if !slices.ContainsFunc(errs, func(err error) bool { return !errors.Is(err, errEmptySource) }) {
		return errEmptySource // no peer has anything yet
	}
	return fmt.Errorf("cluster: peer recovery of %s failed: %w", id, errors.Join(errs...))
}

// stagingDir is where id's recovery stages its files: under data_dir, on the copy's
// volume, outside the copy's directory (which the tailer wipes before each fetch).
func (n *Node) stagingDir(id store.ShardID) string {
	return filepath.Join(n.cfg.DataDir, "recovery", url.PathEscape(id.Index)+"."+strconv.Itoa(id.Shard))
}

// fetchFrom recovers id from one peer into dir, through staging. It returns the bytes
// it fetched.
func (f *fetcher) fetchFrom(ctx context.Context, c candidate, id store.ShardID, dir, staging string) (int64, error) {
	n := f.n
	var snap snapshotReply
	if err := n.call(ctx, c.node, c.addr, http.MethodPost, peerPrefix+"snapshots", shardRef{Index: id.Index, Shard: id.Shard}, &snap); err != nil {
		return 0, err
	}
	defer n.background(func(bctx context.Context) {
		bctx, cancel := context.WithTimeout(bctx, 5*time.Second)
		defer cancel()
		_ = n.call(bctx, c.node, c.addr, http.MethodDelete, peerPrefix+"snapshots/"+snap.ID, nil, nil)
	})
	if snap.Seq <= 0 {
		// A copy at seq 0 holds nothing the store's snapshot would not give as well.
		return 0, errEmptySource
	}
	if len(snap.Files) == 0 || snap.Files[len(snap.Files)-1].Name != shard.ManifestName {
		return 0, fmt.Errorf("cluster: the peer's snapshot of %s does not end with its manifest", id)
	}
	for _, wf := range snap.Files {
		if !safeName(wf.Name) {
			return 0, fmt.Errorf("cluster: the peer's snapshot names a file %q", wf.Name)
		}
	}
	if err := os.MkdirAll(staging, 0o750); err != nil {
		return 0, err
	}
	keep := map[string]bool{}
	var fetched int64
	files, manifest := snap.Files[:len(snap.Files)-1], snap.Files[len(snap.Files)-1]
	for _, wf := range files {
		keep[wf.Name], keep[wf.Name+".part"] = true, true
		k, err := f.stage(ctx, c, snap.ID, staging, wf)
		fetched += k
		if err != nil {
			return fetched, err
		}
	}
	clearStaging(staging, keep)
	man, err := f.download(ctx, c, snap.ID, manifest)
	fetched += int64(len(man))
	if err != nil {
		return fetched, err
	}
	if sum := sha256.Sum256(man); hex.EncodeToString(sum[:]) != manifest.SHA256 {
		return fetched, fmt.Errorf("cluster: the manifest of %s from %s fails its checksum", id, c.node)
	}
	// Every file is staged and checked: move them in, make their names durable, then
	// commit the manifest.
	for _, wf := range files {
		if err := os.Rename(filepath.Join(staging, wf.Name), filepath.Join(dir, wf.Name)); err != nil {
			return fetched, err
		}
	}
	if err := segment.SyncDir(dir); err != nil {
		return fetched, err
	}
	if err := writeSynced(dir, shard.ManifestName, man); err != nil {
		return fetched, err
	}
	return fetched, nil
}

// safeName reports whether name is a plain file name, nothing a path could escape by.
func safeName(name string) bool {
	return name != "" && name != "." && name != ".." && filepath.Base(name) == name && !strings.ContainsAny(name, `/\:`)
}

// clearStaging removes staged files no longer wanted (from another snapshot).
func clearStaging(staging string, keep map[string]bool) {
	entries, err := os.ReadDir(staging)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !keep[e.Name()] {
			_ = os.RemoveAll(filepath.Join(staging, e.Name()))
		}
	}
}

// stage brings one file into staging, checked against its sum: kept when already
// there, else streamed into name.part, resumed from what it holds, and renamed once
// it matches. It returns the bytes it fetched.
func (f *fetcher) stage(ctx context.Context, c candidate, snapID, staging string, wf wireFile) (int64, error) {
	dst := filepath.Join(staging, wf.Name)
	if ok, _ := matches(dst, wf); ok {
		return 0, nil
	}
	part := dst + ".part"
	var fetched int64
	var last error
	for try := 1; try <= fileAttempts; try++ {
		var off int64
		if info, err := os.Stat(part); err == nil {
			off = info.Size()
		}
		if off > wf.Size {
			_ = os.Remove(part)
			off = 0
		}
		if off < wf.Size {
			if off > 0 {
				f.resumes.Add(1)
			}
			k, err := f.streamTo(ctx, c, snapID, wf.Name, part, off)
			fetched += k
			if err != nil {
				if ctx.Err() != nil {
					return fetched, ctx.Err()
				}
				last = err
				continue // resume from what arrived
			}
		}
		ok, err := matches(part, wf)
		if err != nil {
			return fetched, err
		}
		if !ok {
			_ = os.Remove(part)
			last = fmt.Errorf("cluster: %s from %s fails its checksum", wf.Name, c.node)
			continue
		}
		if err := os.Rename(part, dst); err != nil {
			return fetched, err
		}
		return fetched, nil
	}
	return fetched, last
}

// matches reports whether path holds exactly wf: its size and SHA-256.
func matches(path string, wf wireFile) (bool, error) {
	fh, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer fh.Close()
	info, err := fh.Stat()
	if err != nil || info.Size() != wf.Size {
		return false, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, fh); err != nil {
		return false, err
	}
	return hex.EncodeToString(h.Sum(nil)) == wf.SHA256, nil
}

// fileURL is a snapshot file's address on a peer.
func (n *Node) fileURL(c candidate, snapID, name string) string {
	return n.scheme + "://" + c.addr + peerPrefix + "snapshots/" + url.PathEscape(snapID) + "/files/" + url.PathEscape(name)
}

// progressKey carries a recovery's byte counter in its context.
type progressKey struct{}

// idleReader cuts a stream (cancels its request) that delivers nothing for idle.
type idleReader struct {
	r     io.Reader
	timer *time.Timer
	idle  time.Duration
	ctr   *atomic.Int64
}

func (r *idleReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if n > 0 {
		r.timer.Reset(r.idle)
		if r.ctr != nil {
			r.ctr.Add(int64(n))
		}
	}
	return n, err
}

// streamTo appends a snapshot file's bytes from off to part, and fsyncs it. It returns
// the bytes it wrote. A stream idle for PeerIdleTimeout is cut (and resumed by the
// caller).
func (f *fetcher) streamTo(ctx context.Context, c candidate, snapID, name, part string, off int64) (int64, error) {
	n := f.n
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := time.AfterFunc(n.opts.PeerIdleTimeout, cancel)
	defer timer.Stop()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.fileURL(c, snapID, name), http.NoBody)
	if err != nil {
		return 0, err
	}
	n.authorize(req)
	if off > 0 {
		req.Header.Set("Range", "bytes="+strconv.FormatInt(off, 10)+"-")
	}
	resp, err := n.client.Do(req)
	if err != nil {
		return 0, &peerError{node: c.node, err: err}
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusPartialContent && off > 0:
	case resp.StatusCode == http.StatusOK:
		off = 0 // the whole file: start over
	default:
		return 0, decodeError(resp)
	}
	flags := os.O_CREATE | os.O_WRONLY
	if off == 0 {
		flags |= os.O_TRUNC
	}
	fh, err := os.OpenFile(part, flags, 0o600)
	if err != nil {
		return 0, err
	}
	if _, err := fh.Seek(off, io.SeekStart); err != nil {
		_ = fh.Close()
		return 0, err
	}
	ctr, _ := ctx.Value(progressKey{}).(*atomic.Int64)
	k, cerr := io.Copy(fh, &idleReader{r: resp.Body, timer: timer, idle: n.opts.PeerIdleTimeout, ctr: ctr})
	f.bytes.Add(k)
	n.inst.recoveryBytes(ctx, k)
	serr := segment.SyncFile(fh)
	if err := errors.Join(cerr, serr, fh.Close()); err != nil {
		return k, &peerError{node: c.node, err: err}
	}
	return k, nil
}

// download fetches a whole (small) snapshot file into memory.
func (f *fetcher) download(ctx context.Context, c candidate, snapID string, wf wireFile) ([]byte, error) {
	n := f.n
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, n.fileURL(c, snapID, wf.Name), http.NoBody)
	if err != nil {
		return nil, err
	}
	n.authorize(req)
	resp, err := n.client.Do(req)
	if err != nil {
		return nil, &peerError{node: c.node, err: err}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, decodeError(resp)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, wf.Size+1))
	if err != nil {
		return nil, &peerError{node: c.node, err: err}
	}
	f.bytes.Add(int64(len(b)))
	return b, nil
}

// writeSynced writes data to dir/name durably: a temp file, fsynced, renamed over
// name, then the directory fsynced.
func writeSynced(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	fh, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, werr := fh.Write(data)
	serr := segment.SyncFile(fh)
	if err := errors.Join(werr, serr, fh.Close()); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	return segment.SyncDir(dir)
}

// sleep waits d or until ctx ends.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
