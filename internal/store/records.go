package store

import (
	"container/heap"
	"context"
	"database/sql"
	"errors"
	"fmt"

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

// ListQueries reads each shard's page in its primary key's order, (index, shard, id),
// so a page costs what it returns on every shard however many queries the index holds,
// and merges the shards' pages by id. The pages are read in one snapshot.
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
	pages := make([][]Record, 0, shards)
	for sh := range shards {
		page, err := s.listShard(ctx, tx, index, sh, after, limit, lean)
		if err != nil {
			return nil, err
		}
		if len(page) > 0 {
			pages = append(pages, page)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return mergeByID(pages, limit), nil
}

func (s *sqlStore) listShard(ctx context.Context, tx *sql.Tx, index string, shard int, after string, limit int, lean bool) ([]Record, error) {
	q := s.d.Records.ListQueries
	if lean {
		q = s.d.Records.ListQueryVersions
	}
	rows, err := tx.QueryContext(ctx, q, index, shard, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		r := Record{Kind: RecordQuery, Index: index, Shard: shard}
		if lean {
			err = rows.Scan(&r.ID, &r.Seq, &r.Version)
		} else {
			err = rows.Scan(&r.ID, &r.Body, &r.Meta, &r.Seq, &r.Version)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// mergeByID merges pages, each sorted by id (ids are unique across them: a query lives
// on one shard), into the first limit records by id.
func mergeByID(pages [][]Record, limit int) []Record {
	if len(pages) == 1 {
		return pages[0][:min(len(pages[0]), limit)]
	}
	h := make(pageHeap, 0, len(pages))
	for _, p := range pages {
		h = append(h, p)
	}
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
