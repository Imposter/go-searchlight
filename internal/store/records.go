package store

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
)

// RecordReader reads the system of record directly: the current version of one
// document or saved query, and the saved queries of an index in id order. Every
// store Open returns implements it. Reads see every committed change, so a GET
// served from here is realtime, whatever a shard copy has applied.
type RecordReader interface {
	// GetRecord returns the document (RecordDocument) or saved query
	// (RecordQuery) id of shard, or ErrNotFound.
	GetRecord(ctx context.Context, kind RecordKind, shard ShardID, id string) (Record, error)
	// ListQueries returns up to limit saved queries of index, across its
	// shards, with ids above after (all when after is ""), by id. With lean, each
	// record holds its id, shard, seq and version alone, and the queries' trees and
	// meta are never read.
	ListQueries(ctx context.Context, index, after string, limit int, lean bool) ([]Record, error)
	// DocumentHashes reads the stored documents of index named by keys: for each key,
	// in order, the document's seq and its body's BodyHash, with Known false when the
	// document does not exist or its row holds no hash of its current version (a row a
	// release before body hashes wrote). It reads at one instant, outside any
	// transaction: what it returns was the committed state when it ran.
	DocumentHashes(ctx context.Context, index string, keys []DocKey) ([]DocHash, error)
}

// DocKey names one document of an index: its shard and id.
type DocKey struct {
	Shard int
	ID    string
}

// DocHash is a stored document's seq and body hash ([RecordReader.DocumentHashes]).
type DocHash struct {
	Seq   int64
	Hash  int64
	Known bool
}

// BodyHash is the hash a document body is stored with: the first 8 bytes of its
// SHA-256, as an int64. Equal bodies hash alike, and two bodies that differ hash alike
// with odds of 2^-64 by chance, and only with a second preimage of SHA-256's prefix by
// design.
func BodyHash(body []byte) int64 {
	sum := sha256.Sum256(body)
	return int64(binary.BigEndian.Uint64(sum[:8])) //nolint:gosec // the hash's bits, not a quantity
}

// hashesPerStatement bounds one DocumentHashes statement's ids, within every engine's
// bind parameter limit (SQLite's 32,766 is the least).
const hashesPerStatement = 1000

func (s *sqlStore) DocumentHashes(ctx context.Context, index string, keys []DocKey) (out []DocHash, err error) {
	ctx, end := s.start(ctx, "document_hashes", attribute.String("index", index), attribute.Int("documents", len(keys)))
	defer end(&err)
	if s.closed.Load() {
		return nil, ErrClosed
	}
	if err := validKey("index name", index, MaxIndexName); err != nil {
		return nil, err
	}
	out = make([]DocHash, len(keys))
	at := make(map[DocKey][]int, len(keys))
	for i, k := range keys {
		if err := validShard(ShardID{Index: index, Shard: k.Shard}); err != nil {
			return nil, err
		}
		if err := validKey("id", k.ID, MaxID); err != nil {
			return nil, err
		}
		at[k] = append(at[k], i)
	}
	for start := 0; start < len(keys); start += hashesPerStatement {
		part := keys[start:min(start+hashesPerStatement, len(keys))]
		byShard := map[int][]string{}
		var order []int
		for _, k := range part {
			if _, ok := byShard[k.Shard]; !ok {
				order = append(order, k.Shard)
			}
			byShard[k.Shard] = append(byShard[k.Shard], k.ID)
		}
		counts := make([]int, len(order))
		args := make([]any, 0, 1+len(order)+len(part))
		args = append(args, index)
		for i, sh := range order {
			counts[i] = len(byShard[sh])
			args = append(args, sh)
			for _, id := range byShard[sh] {
				args = append(args, id)
			}
		}
		if err := s.readHashes(ctx, s.d.Records.DocumentHashes(counts), args, at, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// readHashes runs one DocumentHashes statement, filling out at each row's key.
func (s *sqlStore) readHashes(ctx context.Context, q string, args []any, at map[DocKey][]int, out []DocHash) error {
	rows, err := s.r.QueryContext(ctx, q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var k DocKey
		var seq int64
		var hash, hashSeq sql.NullInt64
		if err := rows.Scan(&k.Shard, &k.ID, &seq, &hash, &hashSeq); err != nil {
			return err
		}
		for _, i := range at[k] {
			out[i] = DocHash{Seq: seq, Hash: hash.Int64, Known: hash.Valid && hashSeq.Valid && hashSeq.Int64 == seq}
		}
	}
	return rows.Err()
}

var _ RecordReader = (*sqlStore)(nil)

// MaxListLimit bounds ListQueries' limit.
const MaxListLimit = 1000

func (s *sqlStore) GetRecord(ctx context.Context, kind RecordKind, shard ShardID, id string) (r Record, err error) {
	ctx, end := s.start(ctx, "get_record", attribute.String("index", shard.Index), attribute.Int("shard", shard.Shard))
	defer end(&err)
	if s.closed.Load() {
		return r, ErrClosed
	}
	if err := validShard(shard); err != nil {
		return r, err
	}
	if err := validKey("id", id, MaxID); err != nil {
		return r, err
	}
	r = Record{Kind: kind, Index: shard.Index, Shard: shard.Shard, ID: id}
	var row *sql.Row
	switch kind {
	case RecordDocument:
		var plain, z []byte
		row = s.r.QueryRowContext(ctx, s.d.Records.GetDocument, shard.Index, shard.Shard, id)
		if err = row.Scan(&plain, &z, &r.Seq, &r.Version); err == nil {
			if r.Body, err = readBody(plain, z); err != nil {
				return r, &CorruptError{Shard: shard, Seq: r.Seq, ID: id, Err: err}
			}
		}
	case RecordQuery:
		row = s.r.QueryRowContext(ctx, s.d.Records.GetQuery, shard.Index, shard.Shard, id)
		err = row.Scan(&r.Body, &r.Meta, &r.Seq, &r.Version)
	default:
		return r, invalidf("record kind %d", kind)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return r, fmt.Errorf("%s %q: %w", shard, id, ErrNotFound)
	}
	return r, err
}

// pageShards is the most shards one page statement reads: SQLite takes at most 500
// selects in one compound select.
const pageShards = 200

// ListQueries pages the index's queries in two steps, each one statement in one
// snapshot: the page's ids, seqs and versions, the first limit by id of every shard's
// primary key range above after (each shard's cut at limit, so the database reads what
// it returns on every shard and sends limit rows), then, unless lean, the trees and meta
// of those ids alone. An index of more than pageShards shards takes one statement per
// pageShards of them for the ids, their pages merged by id.
func (s *sqlStore) ListQueries(ctx context.Context, index, after string, limit int, lean bool) (out []Record, err error) {
	ctx, end := s.start(ctx, "list_queries", attribute.String("index", index), attribute.Bool("lean", lean))
	defer end(&err)
	if s.closed.Load() {
		return nil, ErrClosed
	}
	if err := validKey("index name", index, MaxIndexName); err != nil {
		return nil, err
	}
	if limit <= 0 || limit > MaxListLimit {
		return nil, invalidf("limit %d is not 1 to %d", limit, MaxListLimit)
	}
	tx, err := s.r.BeginTx(ctx, s.d.SnapshotTx)
	if err != nil {
		return nil, err
	}
	defer rollback(tx)
	var mapping, settings []byte
	var version, mv int64
	var uid string
	switch err := tx.QueryRowContext(ctx, s.d.Indexes.Current, index).Scan(&mapping, &settings, &version, &mv, &uid); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, nil
	case err != nil:
		return nil, err
	}
	shards, err := settingsShards(settings)
	if err != nil {
		return nil, err
	}
	for first := 0; first < shards; first += pageShards {
		part := make([]int, 0, min(pageShards, shards-first))
		args := make([]any, 0, 3*cap(part)+1)
		for sh := first; sh < min(first+pageShards, shards); sh++ {
			part = append(part, sh)
			args = append(args, index, after, limit)
		}
		page, err := s.queryPage(ctx, tx, index, part, append(args, limit))
		if err != nil {
			return nil, err
		}
		out = mergeByID([][]Record{out, page}, limit)
	}
	if !lean && len(out) > 0 {
		if err := s.fillQueries(ctx, tx, index, out); err != nil {
			return nil, err
		}
	}
	return out, tx.Commit()
}

func (s *sqlStore) queryPage(ctx context.Context, tx *sql.Tx, index string, shards []int, args []any) ([]Record, error) {
	rows, err := tx.QueryContext(ctx, s.d.Records.QueryPage(shards), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		r := Record{Kind: RecordQuery, Index: index}
		if err := rows.Scan(&r.Shard, &r.ID, &r.Seq, &r.Version); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// fillQueries reads the trees and meta of page's queries, by shard and id.
func (s *sqlStore) fillQueries(ctx context.Context, tx *sql.Tx, index string, page []Record) error {
	byShard := map[int][]int{}
	var order []int
	for i := range page {
		if _, ok := byShard[page[i].Shard]; !ok {
			order = append(order, page[i].Shard)
		}
		byShard[page[i].Shard] = append(byShard[page[i].Shard], i)
	}
	counts := make([]int, len(order))
	args := []any{index}
	for k, sh := range order {
		counts[k] = len(byShard[sh])
		args = append(args, sh)
		for _, i := range byShard[sh] {
			args = append(args, page[i].ID)
		}
	}
	rows, err := tx.QueryContext(ctx, s.d.Records.QueriesByID(counts), args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	at := make(map[[2]string]int, len(page))
	for i := range page {
		at[[2]string{strconv.Itoa(page[i].Shard), page[i].ID}] = i
	}
	for rows.Next() {
		var shard int
		var id string
		var query, meta []byte
		if err := rows.Scan(&shard, &id, &query, &meta); err != nil {
			return err
		}
		if i, ok := at[[2]string{strconv.Itoa(shard), id}]; ok {
			page[i].Body, page[i].Meta = query, meta
		}
	}
	return rows.Err()
}

// mergeByID merges pages, each sorted by id (ids are unique across them: a query lives
// on one shard), into the first limit records by id.
func mergeByID(pages [][]Record, limit int) []Record {
	var nonEmpty [][]Record
	for _, p := range pages {
		if len(p) > 0 {
			nonEmpty = append(nonEmpty, p)
		}
	}
	switch len(nonEmpty) {
	case 0:
		return nil
	case 1:
		return nonEmpty[0][:min(len(nonEmpty[0]), limit)]
	}
	h := pageHeap(nonEmpty)
	heap.Init(&h)
	var out []Record
	for len(h) > 0 && len(out) < limit {
		out = append(out, h[0][0])
		if h[0] = h[0][1:]; len(h[0]) == 0 {
			heap.Pop(&h)
		} else {
			heap.Fix(&h, 0)
		}
	}
	return out
}

// pageHeap orders pages by their first record's id.
type pageHeap [][]Record

func (h pageHeap) Len() int           { return len(h) }
func (h pageHeap) Less(i, j int) bool { return h[i][0].ID < h[j][0].ID }
func (h pageHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *pageHeap) Push(x any)        { *h = append(*h, x.([]Record)) } //nolint:forcetypeassert,errcheck // only pages
func (h *pageHeap) Pop() any {
	old := *h
	p := old[len(old)-1]
	*h = old[:len(old)-1]
	return p
}
