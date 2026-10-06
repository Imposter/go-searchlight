package store

import (
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
	// shards, with ids above after (all when after is ""), by id.
	ListQueries(ctx context.Context, index, after string, limit int) ([]Record, error)
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
		var stored []byte
		row = s.r.QueryRowContext(ctx, s.d.Records.GetDocument, shard.Index, shard.Shard, id)
		if err = row.Scan(&stored, &r.Seq); err == nil {
			r.Body, err = decodeBody(stored)
		}
	case RecordQuery:
		row = s.r.QueryRowContext(ctx, s.d.Records.GetQuery, shard.Index, shard.Shard, id)
		err = row.Scan(&r.Body, &r.Meta, &r.Seq)
	default:
		return r, invalidf("record kind %d", kind)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return r, fmt.Errorf("%s %q: %w", shard, id, ErrNotFound)
	}
	return r, err
}

func (s *sqlStore) ListQueries(ctx context.Context, index, after string, limit int) (out []Record, err error) {
	ctx, end := s.start(ctx, "list_queries", attribute.String("index", index))
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
	rows, err := s.r.QueryContext(ctx, s.d.Records.ListQueries, index, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		r := Record{Kind: RecordQuery, Index: index}
		if err := rows.Scan(&r.Shard, &r.ID, &r.Body, &r.Meta, &r.Seq); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
