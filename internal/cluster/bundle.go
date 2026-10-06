package cluster

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/Imposter/go-searchlight/internal/replica"
	"github.com/Imposter/go-searchlight/internal/segment"
	"github.com/Imposter/go-searchlight/internal/shard"
	"github.com/Imposter/go-searchlight/internal/store"
)

// Recovery bundles (spec sections 8 and 9): a copy that must be rebuilt while no peer
// can serve it restores the newest bundle of its shard from the store's blobs
// (sl_blobs) before it falls back to the store's ScanShard.
//
//   - Upload. Every BundleInterval (bundle_interval; 0 uploads none), each shard's
//     serving copy on the live node with the lowest id takes a snapshot (a flush, then
//     the generation that flush made durable: shard.Snapshot, as a peer recovery does)
//     and streams it into one blob through the BlobStore, which chunks it and sums it.
//     Set bundle_interval alike on every node: a lowest-id node without it uploads
//     none.
//     A shard whose newest bundle is already at the snapshot's seq is skipped. Once the
//     new bundle is surely stored, the oldest are deleted down to BundleRetention
//     (bundle_retention). An upload whose commit's outcome is unknown
//     (store.ErrAmbiguousCommit) is judged by Stat: it counts only when the stored
//     blob's SHA-256 is the one streamed; otherwise nothing is deleted and the next
//     round uploads again.
//   - Name. bundles/<index>/<incarnation uid>/<shard>/<seq, 20 digits>: names sort by
//     seq, and a recreated index never sees its predecessor's bundles.
//   - Format. A header line "SLBUNDLE <format> <length>", then that many bytes of JSON
//     (the index, its incarnation, the shard, the seq, the mapping version and every
//     file's name, size and SHA-256, the manifest last), then the files' bytes in that
//     order.
//   - Restore. The newest bundle the changelog still reaches (ChangesAfter its seq is
//     not pruned) is streamed into staging (data_dir/recovery/<shard>/bundle), each
//     file checked against its SHA-256 and fsynced, and the blob's own SHA-256 checked
//     at its end. Only a bundle that passes every check is installed: its files move
//     into the copy's empty directory, and the manifest, written last, commits it. A
//     bundle that fails is skipped for the next older one; with none left the copy is
//     rebuilt from ScanShard.
//   - Pruning. Each retained bundle is a recovery point: the leader keeps the changelog
//     after the oldest retained bundle's seq of each shard (current incarnation). It
//     deletes, before computing the floor, the bundles pruning by age
//     (changelog_retention) has passed, which no recovery can replay from, and those
//     whose index is gone.

// Bundle format.
const (
	bundlePrefix = "bundles/"
	bundleMagic  = "SLBUNDLE"
	bundleFormat = 1
	// maxBundleHeader bounds a bundle's JSON header, read before any check.
	maxBundleHeader = 4 << 20
	// maxBundleManifest bounds the manifest, which is held in memory until the
	// bundle's checksum is verified.
	maxBundleManifest = 64 << 20
	// bundleSeqDigits pads a bundle name's seq so names sort by it.
	bundleSeqDigits = 20
)

// bundleHeader describes a bundle's files.
type bundleHeader struct {
	Index          string `json:"index"`
	UID            string `json:"uid"`
	Shard          int    `json:"shard"`
	Seq            int64  `json:"seq"`
	MappingVersion int64  `json:"mapping_version"`
	// SegmentMajor is the newest segment format major among the bundle's segments;
	// absent from a bundle written before it, whose segments are legacyReadsMajor. A
	// build that predates the field refuses the header as unknown, so a bundle never
	// reaches a node that could not open its segments.
	SegmentMajor int        `json:"segment_major,omitempty"`
	Files        []wireFile `json:"files"`
}

// bundleRef is a stored bundle, as its name describes it.
type bundleRef struct {
	name string
	id   store.ShardID
	uid  string
	seq  int64
}

// shardBundlePrefix is the name prefix of the bundles of one incarnation's shard.
func shardBundlePrefix(id store.ShardID, uid string) string {
	return bundlePrefix + url.PathEscape(id.Index) + "/" + uid + "/" + strconv.Itoa(id.Shard) + "/"
}

func bundleName(id store.ShardID, uid string, seq int64) string {
	return shardBundlePrefix(id, uid) + fmt.Sprintf("%0*d", bundleSeqDigits, seq)
}

func parseBundleName(name string) (bundleRef, bool) {
	rest, ok := strings.CutPrefix(name, bundlePrefix)
	if !ok {
		return bundleRef{}, false
	}
	parts := strings.Split(rest, "/")
	if len(parts) != 4 || parts[1] == "" {
		return bundleRef{}, false
	}
	index, err := url.PathUnescape(parts[0])
	if err != nil || index == "" {
		return bundleRef{}, false
	}
	s, err := strconv.Atoi(parts[2])
	if err != nil || s < 0 {
		return bundleRef{}, false
	}
	seq, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil || seq <= 0 {
		return bundleRef{}, false
	}
	return bundleRef{name: name, id: store.ShardID{Index: index, Shard: s}, uid: parts[1], seq: seq}, true
}

// listBundles lists the bundles whose names start with prefix, by shard and seq.
func (n *Node) listBundles(ctx context.Context, prefix string) ([]bundleRef, error) {
	infos, err := n.st.Blobs().List(ctx, prefix)
	if err != nil {
		return nil, err
	}
	out := make([]bundleRef, 0, len(infos))
	for i := range infos {
		if b, ok := parseBundleName(infos[i].Name); ok {
			out = append(out, b)
		}
	}
	slices.SortFunc(out, func(a, b bundleRef) int {
		if c := cmpShard(a.id, b.id); c != 0 {
			return c
		}
		return cmp.Compare(a.seq, b.seq)
	})
	return out, nil
}

func (n *Node) indexUIDs() map[string]string {
	out := map[string]string{}
	for _, iv := range n.Indexes() {
		out[iv.Name] = iv.UID
	}
	return out
}

// bundleLoop uploads bundles every BundleInterval.
func (n *Node) bundleLoop(ctx context.Context) {
	t := n.clock.NewTicker(n.opts.BundleInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
			n.publishBundles(ctx)
		}
	}
}

// publishBundles uploads a bundle of each shard this node is the uploader of.
func (n *Node) publishBundles(ctx context.Context) {
	uids := n.indexUIDs()
	for _, id := range n.bundleShards() {
		if err := n.publishBundle(ctx, id, uids[id.Index]); err != nil && ctx.Err() == nil {
			n.log.WarnContext(ctx, "uploading a recovery bundle failed; the next round tries again", slog.String("shard", id.String()), slog.Any("error", err))
		}
	}
}

// bundleShards are the shards this node uploads bundles of: those whose copy here
// serves peers, and whose serving copies in the registry are on no live node with a
// lower id.
func (n *Node) bundleShards() []store.ShardID {
	v := n.view.Load()
	var out []store.ShardID
	for _, l := range n.leaseList() {
		id := l.copy.Shard
		if !n.peerValid(id) {
			continue
		}
		mine, first := false, true
		for i := range v.copies[id] {
			c := &v.copies[id][i]
			if c.State != store.CopyServing || !v.usable(c) {
				continue
			}
			switch {
			case c.NodeID == n.id:
				mine = true
			case c.NodeID < n.id:
				first = false
			}
		}
		if mine && first {
			out = append(out, id)
		}
	}
	return out
}

// publishBundle uploads a bundle of this node's serving copy of id, unless the newest
// stored one is as recent, then deletes the bundles past the retention.
func (n *Node) publishBundle(ctx context.Context, id store.ShardID, uid string) error {
	if uid == "" {
		return nil
	}
	have, err := n.listBundles(ctx, shardBundlePrefix(id, uid))
	if err != nil {
		return err
	}
	sn, err := n.Snapshot(ctx, id)
	if err != nil {
		return err
	}
	defer sn.Release()
	if sn.IndexUID() != uid || sn.Seq() <= 0 || (len(have) > 0 && have[len(have)-1].seq >= sn.Seq()) {
		return nil
	}
	_, newest := sn.FormatMajors()
	hdr := bundleHeader{Index: id.Index, UID: uid, Shard: id.Shard, Seq: sn.Seq(), MappingVersion: sn.MappingVersion(), SegmentMajor: newest}
	for _, f := range sn.Files() {
		sum, err := n.sums.sum(id, sn, f)
		if err != nil {
			return err
		}
		hdr.Files = append(hdr.Files, wireFile{Name: f.Name, Size: f.Size, SHA256: sum})
	}
	name := bundleName(id, uid, sn.Seq())
	start := n.clock.Now()
	info, err := n.putBundle(ctx, name, &hdr, sn)
	if err != nil {
		return err
	}
	n.log.InfoContext(ctx, "uploaded a recovery bundle", slog.String("shard", id.String()), slog.Int64("seq", hdr.Seq),
		slog.Int64("bytes", info.Size), slog.Float64("seconds", n.clock.Since(start).Seconds()))
	if extra := len(have) + 1 - n.opts.BundleRetention; extra > 0 {
		for _, b := range have[:min(extra, len(have))] {
			if err := n.st.Blobs().Delete(ctx, b.name); err != nil {
				return fmt.Errorf("deleting the older bundle %s: %w", b.name, err)
			}
		}
	}
	return nil
}

// errBundleAbandoned stops a bundle's writer once the upload stopped reading it.
var errBundleAbandoned = errors.New("cluster: the bundle upload stopped")

// putBundle streams a bundle of sn into the blob name. It succeeds only once the blob
// is surely stored: an ambiguous commit counts when the stored blob's sum is the one
// streamed.
func (n *Node) putBundle(ctx context.Context, name string, hdr *bundleHeader, sn *shard.Snapshot) (store.BlobInfo, error) {
	pr, pw := io.Pipe()
	h := sha256.New()
	written := make(chan error, 1)
	go func() {
		err := writeBundle(io.MultiWriter(pw, h), hdr, sn)
		_ = pw.CloseWithError(err)
		written <- err
	}()
	info, err := n.st.Blobs().Put(ctx, name, pr)
	_ = pr.CloseWithError(errBundleAbandoned)
	werr := <-written
	switch {
	case errors.Is(err, store.ErrAmbiguousCommit) && werr == nil:
		got, serr := n.st.Blobs().Stat(context.WithoutCancel(ctx), name)
		if serr == nil && got.SHA256 == hex.EncodeToString(h.Sum(nil)) {
			return got, nil
		}
		return info, fmt.Errorf("the bundle %s may not have been stored: %w", name, errors.Join(err, serr))
	case err != nil:
		return info, err
	}
	return info, werr
}

// writeBundle writes the bundle of sn that hdr describes.
func writeBundle(w io.Writer, hdr *bundleHeader, sn *shard.Snapshot) error {
	body, err := json.Marshal(hdr)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "%s %d %d\n", bundleMagic, bundleFormat, len(body)); err != nil {
		return err
	}
	if _, err := w.Write(body); err != nil {
		return err
	}
	for _, f := range hdr.Files {
		if err := copySnapshotFile(w, sn, f); err != nil {
			return err
		}
	}
	return nil
}

func copySnapshotFile(w io.Writer, sn *shard.Snapshot, f wireFile) error {
	r, err := sn.Open(f.Name)
	if err != nil {
		return err
	}
	defer r.Close()
	k, err := io.Copy(w, r)
	if err != nil {
		return err
	}
	if k != f.Size {
		return fmt.Errorf("cluster: %s holds %d bytes, its snapshot listed %d", f.Name, k, f.Size)
	}
	return nil
}

// restoreBundle fills dir with the newest usable bundle of id, and reports whether it
// did. It tries the bundles newest first, down to the first one the changelog no
// longer reaches; the error joins why each it tried failed.
func (f *fetcher) restoreBundle(ctx context.Context, id store.ShardID, dir string) (bool, error) {
	n := f.n
	uid := n.indexUIDs()[id.Index]
	if uid == "" {
		return false, nil
	}
	list, err := n.listBundles(ctx, shardBundlePrefix(id, uid))
	if err != nil {
		return false, err
	}
	var errs []error
	for i := len(list) - 1; i >= 0; i-- {
		b := list[i]
		if _, err := n.st.ChangesAfter(ctx, id, b.seq, 1); errors.Is(err, store.ErrPruned) {
			break
		} else if err != nil {
			return false, err
		}
		start := n.clock.Now()
		size, err := f.restoreFrom(ctx, b, uid, dir)
		if err == nil {
			f.restored.Add(1)
			secs := n.clock.Since(start).Seconds()
			n.log.InfoContext(ctx, "shard copy restored from a recovery bundle", slog.String("shard", id.String()), slog.String("bundle", b.name),
				slog.Int64("seq", b.seq), slog.Int64("bytes", size), slog.Float64("seconds", secs))
			return true, nil
		}
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		f.rejected.Add(1)
		n.log.WarnContext(ctx, "a recovery bundle failed its checks; it is not installed", slog.String("shard", id.String()),
			slog.String("bundle", b.name), slog.Any("error", err))
		errs = append(errs, fmt.Errorf("%s: %w", b.name, err))
	}
	return false, errors.Join(errs...)
}

// restoreFrom stages bundle b, checks it whole, then installs it in dir: its files
// moved in, the manifest written last. It returns the bundle's size.
func (f *fetcher) restoreFrom(ctx context.Context, b bundleRef, uid, dir string) (int64, error) {
	n := f.n
	staging := filepath.Join(n.stagingDir(b.id), "bundle")
	if err := os.RemoveAll(staging); err != nil {
		return 0, err
	}
	if err := os.MkdirAll(staging, 0o750); err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(staging) }()
	rc, info, err := n.st.Blobs().Get(ctx, b.name)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	ctr, _ := ctx.Value(progressKey{}).(*atomic.Int64)
	r := bufio.NewReaderSize(&countingReader{r: rc, ctr: ctr, onRead: func(k int64) { n.inst.recoveryBytes(ctx, replica.SourceBlob, k) }}, 1<<20)
	hdr, err := readBundleHeader(r)
	if err != nil {
		return 0, err
	}
	if hdr.Index != b.id.Index || hdr.Shard != b.id.Shard || hdr.UID != uid || hdr.Seq != b.seq {
		return 0, fmt.Errorf("the header describes %s/%d of incarnation %s at seq %d", hdr.Index, hdr.Shard, hdr.UID, hdr.Seq)
	}
	files, manifest := hdr.Files[:len(hdr.Files)-1], hdr.Files[len(hdr.Files)-1]
	for _, wf := range files {
		if err := stageBundleFile(r, staging, wf); err != nil {
			return 0, err
		}
	}
	man, err := readBundleFile(r, manifest)
	if err != nil {
		return 0, err
	}
	// The blob's own SHA-256 is checked once its last byte is read: the bundle is
	// installed only after that.
	if rest, err := io.ReadAll(r); err != nil {
		return 0, err
	} else if len(rest) > 0 {
		return 0, fmt.Errorf("%d bytes follow the bundle's last file", len(rest))
	}
	if err := clearDir(dir); err != nil {
		return 0, err
	}
	for _, wf := range files {
		if err := os.Rename(filepath.Join(staging, wf.Name), filepath.Join(dir, wf.Name)); err != nil {
			return 0, err
		}
	}
	if err := segment.SyncDir(dir); err != nil {
		return 0, err
	}
	if err := writeSynced(dir, shard.ManifestName, man); err != nil {
		return 0, err
	}
	return info.Size, nil
}

// readBundleHeader reads and checks a bundle's header: its magic and format, and a
// file list of plain names ending with the manifest.
func readBundleHeader(r *bufio.Reader) (*bundleHeader, error) {
	line, err := r.ReadSlice('\n')
	if err != nil {
		return nil, fmt.Errorf("the bundle has no header line: %w", err)
	}
	fields := strings.Fields(string(line))
	if len(fields) != 3 || fields[0] != bundleMagic {
		return nil, errors.New("the bundle has a bad header line")
	}
	if format, err := strconv.Atoi(fields[1]); err != nil || format != bundleFormat {
		return nil, fmt.Errorf("the bundle is in format %q, this build reads %d", fields[1], bundleFormat)
	}
	size, err := strconv.Atoi(fields[2])
	if err != nil || size <= 0 || size > maxBundleHeader {
		return nil, fmt.Errorf("the bundle's header length %q is out of bounds", fields[2])
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, fmt.Errorf("the bundle's header is cut short: %w", err)
	}
	var hdr bundleHeader
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&hdr); err != nil {
		return nil, fmt.Errorf("the bundle's header: %w", err)
	}
	if major := cmp.Or(hdr.SegmentMajor, legacyReadsMajor); major > segment.FormatMajor {
		return nil, fmt.Errorf("the bundle's segments are in format %d, newer than this build reads (%d)", major, segment.FormatMajor)
	}
	if len(hdr.Files) == 0 || hdr.Files[len(hdr.Files)-1].Name != shard.ManifestName {
		return nil, errors.New("the bundle's files do not end with its manifest")
	}
	for _, wf := range hdr.Files {
		if !safeName(wf.Name) || wf.Size < 0 || len(wf.SHA256) != sha256.Size*2 {
			return nil, fmt.Errorf("the bundle lists a file %q of %d bytes", wf.Name, wf.Size)
		}
	}
	if m := hdr.Files[len(hdr.Files)-1]; m.Size > maxBundleManifest {
		return nil, fmt.Errorf("the bundle's manifest is %d bytes", m.Size)
	}
	return &hdr, nil
}

// stageBundleFile copies one file's bytes from r into staging, checks its SHA-256,
// fsyncs it and names it.
func stageBundleFile(r io.Reader, staging string, wf wireFile) error {
	part := filepath.Join(staging, wf.Name+".part")
	fh, err := os.OpenFile(part, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	h := sha256.New()
	_, cerr := io.CopyN(io.MultiWriter(fh, h), r, wf.Size)
	var serr error
	if cerr == nil {
		serr = segment.SyncFile(fh)
	}
	if err := errors.Join(cerr, serr, fh.Close()); err != nil {
		return fmt.Errorf("%s: %w", wf.Name, err)
	}
	if hex.EncodeToString(h.Sum(nil)) != wf.SHA256 {
		return fmt.Errorf("%s fails its checksum", wf.Name)
	}
	return os.Rename(part, filepath.Join(staging, wf.Name))
}

// readBundleFile reads one (small) file's bytes from r and checks its SHA-256.
func readBundleFile(r io.Reader, wf wireFile) ([]byte, error) {
	b := make([]byte, wf.Size)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, fmt.Errorf("%s: %w", wf.Name, err)
	}
	if sum := sha256.Sum256(b); hex.EncodeToString(sum[:]) != wf.SHA256 {
		return nil, fmt.Errorf("%s fails its checksum", wf.Name)
	}
	return b, nil
}

// clearDir empties dir, keeping it: a peer attempt may have moved files into it
// before it failed.
func clearDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

// countingReader counts the bytes read through it into ctr (a recovery's progress)
// and reports them to onRead.
type countingReader struct {
	r      io.Reader
	ctr    *atomic.Int64
	onRead func(int64)
}

func (c *countingReader) Read(p []byte) (int, error) {
	k, err := c.r.Read(p)
	if k > 0 {
		if c.ctr != nil {
			c.ctr.Add(int64(k))
		}
		c.onRead(int64(k))
	}
	return k, err
}

// retainedBundles lists the stored bundles of each shard of the indexes in the
// store's catalogue, of their current incarnations, oldest first: each is a recovery
// point the changelog is kept for. It also returns the bundles of incarnations the
// catalogue no longer holds.
func (n *Node) retainedBundles(ctx context.Context) (retained map[store.ShardID][]bundleRef, gone []bundleRef, err error) {
	list, err := n.listBundles(ctx, bundlePrefix)
	if err != nil || len(list) == 0 {
		return nil, nil, err
	}
	metas, err := n.st.Indexes().List(ctx)
	if err != nil {
		return nil, nil, err
	}
	uids := map[string]string{}
	for i := range metas {
		uids[metas[i].Name] = metas[i].UID
	}
	retained = map[store.ShardID][]bundleRef{}
	for _, b := range list {
		if uids[b.id.Index] != b.uid {
			gone = append(gone, b)
			continue
		}
		retained[b.id] = append(retained[b.id], b)
	}
	return retained, gone, nil
}
