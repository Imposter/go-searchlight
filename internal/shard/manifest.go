package shard

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/Imposter/go-searchlight/internal/schema"
	"github.com/Imposter/go-searchlight/internal/segment"
)

// The manifest is the shard's commit point: the one file that says which segments, with
// which deletes, make up the durable shard, and which changelog seq they cover.
//
// It is a header line, then JSON:
//
//	SLMANIFEST <format> <crc32c of the JSON, 8 hex digits> <JSON length>\n
//	{"gen":7,"seq":1234,"max_seq":1230,"index_uid":"...",
//	 "segments":[{"id":"9f..","docs":1000,"bytes":81234,"del_gen":7,"deleted":12}],
//	 "query_segments":[{"id":"4c..","docs":10,"bytes":2048,"format":"default/1"}]}
//
// gen numbers every commit (refresh or merge), and names the deletes sidecars that
// commit wrote (segment.WriteDeletes(dir, id, gen, ...)): a segment's live sidecar is
// del_gen's, and none when del_gen is 0. seq is the changelog position the segments
// cover (every change at or below it is in them, gaps included); max_seq is the newest
// change they hold. Segments are listed in the order their base ordinals follow.
//
// It is replaced atomically: written to manifest.tmp, fsynced, renamed over manifest,
// and the directory fsynced. A reader therefore sees the old manifest or the new one,
// whole, and the checksum catches a torn or damaged one.

const (
	manifestName   = "manifest"
	manifestFormat = 1
	manifestMagic  = "SLMANIFEST"
)

type manifest struct {
	Gen      uint64 `json:"gen"`
	Seq      int64  `json:"seq"`
	MaxSeq   int64  `json:"max_seq"`
	IndexUID string `json:"index_uid,omitempty"`
	// Mapping is the index mapping as of Seq, MappingVersion its version.
	Mapping        json.RawMessage   `json:"mapping,omitempty"`
	MappingVersion int64             `json:"mapping_version,omitempty"`
	Segments       []manifestSegment `json:"segments"`
	QuerySegments  []manifestSegment `json:"query_segments"`
}

type manifestSegment struct {
	ID string `json:"id"`
	// Docs is the segment's document (or query) ordinals, live and deleted.
	Docs  uint32 `json:"docs"`
	Bytes int64  `json:"bytes"`
	// DelGen is the generation whose deletes sidecar is the segment's; 0: none.
	DelGen uint64 `json:"del_gen,omitempty"`
	// Deleted is that sidecar's cardinality, checked at Open.
	Deleted uint32 `json:"deleted,omitempty"`
	// Format is a query segment's QueryIndexBuilder format.
	Format string `json:"format,omitempty"`
}

// retryDelays are the waits between retryIO's attempts: ten attempts over about
// half a second.
var retryDelays = [...]time.Duration{
	5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond,
	100 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond,
}

// retryIO runs fn, again after each of retryDelays while it fails with an error
// retryableIO accepts (on Windows, another handle in the way: a scanner, an indexer, a
// reader that did not share delete), and returns its last error.
func retryIO(log *slog.Logger, op, path string, fn func() error) error {
	for attempt := 0; ; attempt++ {
		err := fn()
		if err == nil || attempt == len(retryDelays) || !retryableIO(err) {
			return err
		}
		log.Debug("manifest file busy; retrying", slog.String("op", op), slog.String("file", path),
			slog.Int("attempt", attempt+1), slog.Any("error", err))
		time.Sleep(retryDelays[attempt])
	}
}

// ManifestError is a manifest that cannot be read: damaged, or of an unknown format.
// The copy must be recovered.
type ManifestError struct {
	Path   string
	Reason string
}

func (e *ManifestError) Error() string {
	return fmt.Sprintf("shard manifest %s: %s", e.Path, e.Reason)
}

// readManifest reads dir's manifest; an empty manifest when there is none. It opens
// the file sharing it fully (openShared), so the read never stops a commit renaming a
// new manifest over it, and retries briefly while another handle is in the way
// (retryIO); log takes the retries, at debug.
func readManifest(dir string, log *slog.Logger) (*manifest, error) {
	path := filepath.Join(dir, manifestName)
	var data []byte
	err := retryIO(log, "open", path, func() error {
		f, err := openShared(path)
		if err != nil {
			return err
		}
		defer f.Close()
		data, err = io.ReadAll(f)
		return err
	})
	if errors.Is(err, os.ErrNotExist) {
		return &manifest{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("shard: %w", err)
	}
	bad := func(why string) error { return &ManifestError{Path: path, Reason: why} }
	nl := bytes.IndexByte(data, '\n')
	if nl < 0 {
		return nil, bad("no header")
	}
	fields := bytes.Fields(data[:nl])
	if len(fields) != 4 || string(fields[0]) != manifestMagic {
		return nil, bad("bad header")
	}
	format, err := strconv.Atoi(string(fields[1]))
	if err != nil {
		return nil, bad("bad format")
	}
	if format != manifestFormat {
		return nil, bad(fmt.Sprintf("format %d is not %d", format, manifestFormat))
	}
	sum, err := strconv.ParseUint(string(fields[2]), 16, 32)
	if err != nil {
		return nil, bad("bad checksum field")
	}
	n, err := strconv.Atoi(string(fields[3]))
	if err != nil {
		return nil, bad("bad length field")
	}
	body := data[nl+1:]
	if len(body) != n {
		return nil, bad(fmt.Sprintf("body is %d bytes, header says %d", len(body), n))
	}
	if uint64(crc32.Checksum(body, castagnoli)) != sum {
		return nil, bad("checksum mismatch")
	}
	var m manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, bad(err.Error())
	}
	// Ids become file names: only the names the shard itself gives are accepted, so a
	// damaged or crafted manifest can never point outside the directory.
	seen := map[string]bool{}
	for _, list := range [][]manifestSegment{m.Segments, m.QuerySegments} {
		for _, ms := range list {
			if !isSegmentName(ms.ID) {
				return nil, bad(fmt.Sprintf("segment id %q is not a segment name", ms.ID))
			}
			if seen[ms.ID] {
				return nil, bad(fmt.Sprintf("segment %s is listed twice", ms.ID))
			}
			seen[ms.ID] = true
		}
	}
	return &m, nil
}

// Discard turns the shard copy in dir, which no open Shard may be using, into an empty
// one at once: it removes the manifest, the commit point, durably. An Open from then on
// finds no segments, and removes whatever files are left; so a crash while the
// directory is being removed after Discard leaves a copy that opens empty rather than
// one that refuses to open. A directory that does not exist is already empty.
func Discard(dir string) error {
	err := retryIO(slog.Default(), "remove", filepath.Join(dir, manifestName), func() error {
		return os.Remove(filepath.Join(dir, manifestName))
	})
	switch {
	case errors.Is(err, os.ErrNotExist):
		if _, serr := os.Stat(dir); errors.Is(serr, os.ErrNotExist) {
			return nil
		}
	case err != nil:
		return fmt.Errorf("shard: discarding %s: %w", dir, err)
	}
	if err := segment.SyncDir(dir); err != nil {
		return fmt.Errorf("shard: discarding %s: %w", dir, err)
	}
	return nil
}

// mappingState is a mapping and its version, as of some seq. It is never changed: a
// new mapping is a new mappingState, so comparing pointers tells whether it moved.
type mappingState struct {
	m       *schema.Mapping
	version int64
}

// mappingState returns the manifest's mapping, or, when it records none, fallback at
// version 0.
func (m *manifest) mappingState(dir string, fallback *schema.Mapping) (*mappingState, error) {
	if len(m.Mapping) == 0 {
		return &mappingState{m: fallback}, nil
	}
	var mapping schema.Mapping
	if err := json.Unmarshal(m.Mapping, &mapping); err != nil {
		return nil, &ManifestError{Path: filepath.Join(dir, manifestName), Reason: "bad mapping: " + err.Error()}
	}
	return &mappingState{m: &mapping, version: m.MappingVersion}, nil
}

// encodeManifest returns m as the manifest file's bytes.
func encodeManifest(m *manifest) ([]byte, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	header := fmt.Sprintf("%s %d %08x %d\n", manifestMagic, manifestFormat, crc32.Checksum(body, castagnoli), len(body))
	return append([]byte(header), body...), nil
}

// writeManifest replaces dir's manifest with m atomically. renamed reports whether the
// new manifest was renamed into place (a failure after that, the directory fsync,
// leaves the swap's durability unknown); hook is the shard's kill-point hook.
//
// forget is called with manifest.tmp's path before it is written: garbage collection
// may have left it pending removal.
//
// The rename replaces the manifest even while readers hold it (replaceFile), and is
// retried briefly while another handle is in the way (retryIO, logging to log).
func writeManifest(dir string, m *manifest, hook func(point string) error, forget func(paths ...string), log *slog.Logger) (size int64, renamed bool, err error) {
	data, err := encodeManifest(m)
	if err != nil {
		return 0, false, err
	}
	size = int64(len(data))
	path := filepath.Join(dir, manifestName)
	tmp := path + ".tmp"
	forget(tmp)
	f, err := os.Create(tmp)
	if err != nil {
		return size, false, err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return size, false, err
	}
	if err := segment.SyncFile(f); err != nil {
		_ = f.Close()
		return size, false, err
	}
	if err := f.Close(); err != nil {
		return size, false, err
	}
	if err := hook(pointManifestWritten); err != nil {
		return size, false, err
	}
	if err := retryIO(log, "rename", path, func() error { return replaceFile(tmp, path) }); err != nil {
		return size, false, err
	}
	if err := hook(pointManifestRenamed); err != nil {
		return size, true, err
	}
	return size, true, segment.SyncDir(dir)
}
