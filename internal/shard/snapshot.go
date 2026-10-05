package shard

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/Imposter/go-searchlight/internal/segment"
)

// A Snapshot is a consistent copy of a shard copy's files as of one generation, for
// peer recovery (spec section 9): another node streams its files into an empty
// directory, writing the manifest last, and opens a copy identical to this one as of
// [Snapshot.Seq].
//
// It owns its manifest. The files a commit writes are named for the commit, and a
// later commit removes the deletes sidecars it supersedes as soon as its own manifest
// is durable, while the snapshot may still be streaming the older generation. So the
// snapshot does not read sidecars or the manifest from the directory: it encodes them
// from the generation it holds, whose deletes bitmaps never change, and builds the
// manifest that lists exactly those segments, sidecars, seq and mapping. The segment
// and query segment files themselves are immutable and read from the directory: the
// held generation keeps them open, and the shard removes a segment's files only after
// the last generation listing it is released.
type Snapshot struct {
	g     *Generation
	dir   string
	files []SnapshotFile
	// mem holds the files the snapshot encoded itself: sidecars and the manifest.
	mem  map[string][]byte
	once sync.Once
}

// SnapshotFile is one file of a [Snapshot]: its name in the shard directory and its
// size in bytes. Files are listed segments first and the manifest last.
type SnapshotFile struct {
	Name string
	Size int64
}

// ManifestName is the manifest's file name: the last file of every [Snapshot], and the
// one a recovering copy writes last (it is the copy's commit point).
const ManifestName = manifestName

// Snapshot holds the shard's current generation and describes its files. Release it
// when the copy is done: until then the generation's segment files stay on disk.
func (s *Shard) Snapshot() (*Snapshot, error) {
	if err := s.Err(); err != nil {
		return nil, err
	}
	g := s.Acquire()
	if g == nil {
		return nil, ErrClosed
	}
	sn := &Snapshot{g: g, dir: s.dir, mem: map[string][]byte{}}
	if err := sn.build(s.marksUntyped); err != nil {
		g.Release()
		return nil, err
	}
	return sn, nil
}

// build lists the generation's files and encodes its sidecars and manifest.
func (sn *Snapshot) build(marksUntyped bool) error {
	g := sn.g
	entries, err := os.ReadDir(sn.dir)
	if err != nil {
		return fmt.Errorf("shard: snapshot: %w", err)
	}
	var sidecars []SnapshotFile
	addSidecar := func(st *segState) error {
		if st.delGen == 0 {
			return nil
		}
		var buf bytes.Buffer
		if err := segment.EncodeDeletes(&buf, st.deletes); err != nil {
			return fmt.Errorf("shard: snapshot: encoding the deletes of %s: %w", st.ref.id, err)
		}
		name := deletesName(st.ref.id, st.delGen)
		sn.mem[name] = buf.Bytes()
		sidecars = append(sidecars, SnapshotFile{Name: name, Size: int64(buf.Len())})
		return nil
	}
	for i := range g.docs {
		st := &g.docs[i]
		name := st.ref.id + segment.FileExt
		info, err := os.Stat(filepath.Join(sn.dir, name))
		if err != nil {
			return fmt.Errorf("shard: snapshot: %w", err)
		}
		sn.files = append(sn.files, SnapshotFile{Name: name, Size: info.Size()})
		if err := addSidecar(st); err != nil {
			return err
		}
	}
	for i := range g.queries {
		st := &g.queries[i]
		found := false
		for _, e := range entries {
			name := e.Name()
			if e.IsDir() || !strings.HasPrefix(name, st.ref.id+".") || strings.HasSuffix(name, deletesExt) || strings.HasSuffix(name, ".tmp") {
				continue
			}
			info, err := e.Info()
			if err != nil {
				return fmt.Errorf("shard: snapshot: %w", err)
			}
			sn.files = append(sn.files, SnapshotFile{Name: name, Size: info.Size()})
			found = true
		}
		if !found {
			return fmt.Errorf("shard: snapshot: query segment %s has no files", st.ref.id)
		}
		if err := addSidecar(st); err != nil {
			return err
		}
	}
	sn.files = append(sn.files, sidecars...)
	man, err := buildManifest(g.gen, g.seq, g.maxSeq, g.uid, g.mp, g.docs, g.queries)
	if err != nil {
		return err
	}
	if marksUntyped {
		man.UntypedMarks = untypedMarksFormat
	}
	raw, err := encodeManifest(man)
	if err != nil {
		return err
	}
	sn.mem[manifestName] = raw
	sn.files = append(sn.files, SnapshotFile{Name: manifestName, Size: int64(len(raw))})
	return nil
}

// Seq is the changelog position the snapshot covers: a copy made from it replays the
// changelog after it.
func (sn *Snapshot) Seq() int64 { return sn.g.seq }

// IndexUID is the index incarnation the snapshot's copy holds.
func (sn *Snapshot) IndexUID() string { return sn.g.uid }

// MappingVersion is the version of the mapping the snapshot's copy holds.
func (sn *Snapshot) MappingVersion() int64 { return sn.g.mp.version }

// NumDocs is the live documents the snapshot holds.
func (sn *Snapshot) NumDocs() uint64 { return sn.g.numDocs }

// Files lists the snapshot's files, the manifest last.
func (sn *Snapshot) Files() []SnapshotFile { return slices.Clone(sn.files) }

// Path returns where a listed file lives on disk; ok is false for one the snapshot
// encodes itself (a sidecar, the manifest) or does not list.
func (sn *Snapshot) Path(name string) (path string, ok bool) {
	if _, mem := sn.mem[name]; mem {
		return "", false
	}
	if !slices.ContainsFunc(sn.files, func(f SnapshotFile) bool { return f.Name == name }) {
		return "", false
	}
	return filepath.Join(sn.dir, name), true
}

// SnapshotReader reads one file of a snapshot.
type SnapshotReader interface {
	io.ReadSeeker
	io.Closer
}

// ErrNoSuchFile is returned by [Snapshot.Open] for a name the snapshot does not list.
var ErrNoSuchFile = errors.New("shard: the snapshot has no such file")

// Open opens one of the snapshot's files. Close it before releasing the snapshot.
func (sn *Snapshot) Open(name string) (SnapshotReader, error) {
	if b, ok := sn.mem[name]; ok {
		return nopCloser{bytes.NewReader(b)}, nil
	}
	if !slices.ContainsFunc(sn.files, func(f SnapshotFile) bool { return f.Name == name }) {
		return nil, fmt.Errorf("%w: %q", ErrNoSuchFile, name)
	}
	f, err := os.Open(filepath.Join(sn.dir, name))
	if err != nil {
		return nil, fmt.Errorf("shard: snapshot: %w", err)
	}
	return f, nil
}

// Release lets the shard remove the snapshot's files once no generation lists them.
// It is idempotent.
func (sn *Snapshot) Release() { sn.once.Do(sn.g.Release) }

type nopCloser struct{ *bytes.Reader }

func (nopCloser) Close() error { return nil }
