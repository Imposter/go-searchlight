package shard

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/Imposter/go-searchlight/internal/query"
	"github.com/Imposter/go-searchlight/internal/segment"
)

// Percolator query segments.
//
// Saved queries go through the same buffer, refresh, deletes and merges as documents,
// into query segments, whose format belongs to the percolator (Task 7): the shard
// reaches it only through a [QueryIndexBuilder] set in [Options.QueryIndex].
//
// A query segment is named by the shard (a name unique for the shard's whole life, as
// segment names are), and every file a builder writes for it must be named that name,
// a dot, then anything not ending in ".del" or ".tmp" (".del" is the shard's deletes
// sidecar, and any ".tmp" is a crash's leftover). The shard then knows a segment's
// files without asking: it removes them by that prefix once the segment is merged away
// and unmapped, and keeps exactly the referenced ones when it collects garbage.

// StoredQuery is one saved query, as a query segment holds it.
type StoredQuery struct {
	ID string
	// Seq is the change that wrote this version.
	Seq   int64
	Query query.Node
	// Meta is the query's opaque meta object.
	Meta []byte
}

// TermStats are a shard's document statistics, for choosing a query's rarest anchors
// (spec section 7). A [Generation] is one; it is valid while acquired.
type TermStats interface {
	// NumDocs is how many live documents the shard holds.
	NumDocs() uint64
	// DocFreq is how many documents hold term in field's kind dictionary, deleted
	// documents included (an estimate, as in Lucene).
	DocFreq(field string, kind segment.TermKind, term string) uint64
}

// QuerySegment is an open query segment. Its methods must be safe for concurrent use,
// and whatever it hands out stays valid until Close.
type QuerySegment interface {
	// NumQueries is how many query ordinals the segment holds, live and deleted.
	NumQueries() uint32
	// Ord returns the ordinal of the query with this id, false when there is none.
	Ord(id string) (uint32, bool)
	// Query returns the query at ord, for merges and for verification.
	Query(ord uint32) (StoredQuery, error)
	// Close releases the segment; the shard calls it once no generation holds it.
	Close() error
}

// QueryIndexBuilder builds and opens query segments. The percolator implements it with
// its anchor index; [DefaultQueryIndex] stores the queries alone.
type QueryIndexBuilder interface {
	// Format names the builder's on-disk format. The manifest records it per query
	// segment, and Open refuses a segment written in another format (rebuild the copy
	// when it changes).
	Format() string
	// Check refuses a query Build could not store, when it is applied, so that one bad
	// query can never fail a refresh. Build must not fail on a query Check accepted
	// (I/O errors aside).
	Check(q *StoredQuery) error
	// Build writes queries as the query segment name in dir (renamed into place if it
	// renames), and returns the bytes it wrote. It fsyncs nothing: the flush that first
	// persists the segment fsyncs its files, then the directory. stats are the shard's
	// document statistics, valid for the call only. A failed or cancelled Build may
	// leave files behind: the shard removes them.
	Build(ctx context.Context, dir, name string, queries []StoredQuery, stats TermStats) (int64, error)
	// Open opens the query segment name in dir.
	Open(dir, name string) (QuerySegment, error)
}

// DefaultQueryIndex is the built-in [QueryIndexBuilder]: one checksummed file per
// segment holding each query's id, seq, JSON and meta, read wholly into memory at Open.
// It has no anchors (every query is a candidate for every document); the percolator
// replaces it.
type DefaultQueryIndex struct{}

const (
	defaultQueryExt     = ".qry"
	defaultQueryVersion = 1
)

var defaultQueryMagic = [8]byte{'S', 'L', 'Q', 'R', 'Y', '\r', '\n', 0x1a}

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Format implements [QueryIndexBuilder].
func (DefaultQueryIndex) Format() string { return "default/1" }

// Check implements [QueryIndexBuilder]: the query must survive a round trip through
// its JSON form unchanged.
func (DefaultQueryIndex) Check(q *StoredQuery) error {
	raw, err := encodeQuery(q.Query)
	if err != nil {
		return err
	}
	back, problems := query.Parse(raw)
	if len(problems) > 0 {
		return fmt.Errorf("query does not round-trip: %s: %s", problems[0].Loc, problems[0].Message)
	}
	if !bytes.Equal(query.Canonical(back), query.Canonical(q.Query)) {
		return errors.New("query does not round-trip through JSON")
	}
	return nil
}

// Build implements [QueryIndexBuilder].
func (DefaultQueryIndex) Build(ctx context.Context, dir, name string, queries []StoredQuery, _ TermStats) (int64, error) {
	buf := append([]byte(nil), defaultQueryMagic[:]...)
	buf = binary.LittleEndian.AppendUint32(buf, defaultQueryVersion)
	buf = binary.AppendUvarint(buf, uint64(len(queries)))
	for i := range queries {
		if i%1024 == 0 && ctx.Err() != nil {
			return 0, ctx.Err()
		}
		q := &queries[i]
		raw, err := encodeQuery(q.Query)
		if err != nil {
			return 0, fmt.Errorf("query %q: %w", q.ID, err)
		}
		buf = appendBytes(buf, []byte(q.ID))
		buf = binary.AppendVarint(buf, q.Seq)
		buf = appendBytes(buf, raw)
		buf = appendBytes(buf, q.Meta)
	}
	buf = binary.LittleEndian.AppendUint32(buf, crc32.Checksum(buf, castagnoli))
	path := filepath.Join(dir, name+defaultQueryExt)
	if err := writeFile(path, buf); err != nil {
		return 0, err
	}
	return int64(len(buf)), nil
}

// Open implements [QueryIndexBuilder].
func (DefaultQueryIndex) Open(dir, name string) (QuerySegment, error) {
	path := filepath.Join(dir, name+defaultQueryExt)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	corrupt := func(why string) error { return fmt.Errorf("query segment %s: %w: %s", path, segment.ErrCorrupt, why) }
	if len(data) < len(defaultQueryMagic)+8 || !bytes.Equal(data[:8], defaultQueryMagic[:]) {
		return nil, corrupt("bad magic")
	}
	body, sum := data[:len(data)-4], binary.LittleEndian.Uint32(data[len(data)-4:])
	if crc32.Checksum(body, castagnoli) != sum {
		return nil, corrupt("checksum mismatch")
	}
	if v := binary.LittleEndian.Uint32(body[8:]); v != defaultQueryVersion {
		return nil, fmt.Errorf("query segment %s: version %d is not %d: %w", path, v, defaultQueryVersion, segment.FormatError(uint64(v), defaultQueryVersion))
	}
	r := byteReader{b: body[12:]}
	n := r.uvarint()
	if r.err != nil || n > uint64(len(body)) {
		return nil, corrupt("bad count")
	}
	seg := &defaultQuerySegment{ids: make(map[string]uint32, n), queries: make([]StoredQuery, 0, n)}
	for i := range n {
		id := string(r.bytes())
		seq := r.varint()
		raw := r.bytes()
		meta := r.bytes()
		if r.err != nil {
			return nil, corrupt(r.err.Error())
		}
		node, problems := query.Parse(raw)
		if len(problems) > 0 {
			return nil, corrupt(fmt.Sprintf("query %q: %s", id, problems[0].Message))
		}
		if len(meta) == 0 {
			meta = nil
		}
		seg.ids[id] = uint32(i)
		seg.queries = append(seg.queries, StoredQuery{ID: id, Seq: seq, Query: node, Meta: bytes.Clone(meta)})
	}
	if len(r.b) != 0 {
		return nil, corrupt("trailing bytes")
	}
	return seg, nil
}

type defaultQuerySegment struct {
	ids     map[string]uint32
	queries []StoredQuery
}

func (s *defaultQuerySegment) NumQueries() uint32 { return uint32(len(s.queries)) } //nolint:gosec // see Open

func (s *defaultQuerySegment) Ord(id string) (uint32, bool) {
	ord, ok := s.ids[id]
	return ord, ok
}

func (s *defaultQuerySegment) Query(ord uint32) (StoredQuery, error) {
	if int(ord) >= len(s.queries) {
		return StoredQuery{}, fmt.Errorf("query ordinal %d out of range", ord)
	}
	return s.queries[ord], nil
}

func (s *defaultQuerySegment) Close() error { return nil }

func appendBytes(buf, p []byte) []byte {
	buf = binary.AppendUvarint(buf, uint64(len(p)))
	return append(buf, p...)
}

var errShortRead = errors.New("truncated")

type byteReader struct {
	b   []byte
	err error
}

func (r *byteReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.err = errShortRead
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *byteReader) varint() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.b)
	if n <= 0 {
		r.err = errShortRead
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *byteReader) bytes() []byte {
	n := r.uvarint()
	if r.err != nil {
		return nil
	}
	if n > uint64(len(r.b)) {
		r.err = errShortRead
		return nil
	}
	v := r.b[:n]
	r.b = r.b[n:]
	return v
}

// encodeQuery writes n as the query language's JSON, which [query.Parse] reads back.
func encodeQuery(n query.Node) ([]byte, error) {
	var buf bytes.Buffer
	if err := writeQuery(&buf, n); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeQuery(buf *bytes.Buffer, n query.Node) error {
	switch x := n.(type) {
	case *query.All:
		return writeGroup(buf, "all", x.Children)
	case *query.Any:
		return writeGroup(buf, "any", x.Children)
	case *query.Not:
		buf.WriteString(`{"not":`)
		if err := writeQuery(buf, x.Child); err != nil {
			return err
		}
		buf.WriteByte('}')
		return nil
	case *query.Leaf:
		field, err := json.Marshal(x.Field)
		if err != nil {
			return err
		}
		op, err := json.Marshal(x.Op)
		if err != nil {
			return err
		}
		buf.WriteString(`{"field":`)
		buf.Write(field)
		buf.WriteString(`,"op":`)
		buf.Write(op)
		if len(x.Value) > 0 {
			if !json.Valid(x.Value) {
				return fmt.Errorf("condition on %q: value is not JSON", x.Field)
			}
			buf.WriteString(`,"value":`)
			buf.Write(x.Value)
		}
		buf.WriteByte('}')
		return nil
	default:
		return fmt.Errorf("query node %T cannot be stored", n)
	}
}

func writeGroup(buf *bytes.Buffer, key string, children []query.Node) error {
	buf.WriteString(`{"` + key + `":[`)
	for i, c := range children {
		if i > 0 {
			buf.WriteByte(',')
		}
		if err := writeQuery(buf, c); err != nil {
			return err
		}
	}
	buf.WriteString("]}")
	return nil
}

func writeFile(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
