package postgres

import (
	"fmt"
	"strings"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

// now is the database clock in Unix milliseconds. clock_timestamp, unlike
// now(), moves on within a transaction.
const now = "(EXTRACT(EPOCH FROM clock_timestamp()) * 1000)::BIGINT"

// withNow spells the clock: the statements below write NOW for it.
func withNow(q string) string { return strings.ReplaceAll(q, "NOW", now) }

// notifyChannel is the channel Apply announces commits on and Listen
// subscribes to.
const notifyChannel = "searchlight_changes"

// copyColumns is dialect.Registry's CopyColumns.
const copyColumns = "index_name, shard, slot, node_id, state, applied_seq, epoch, lease_until, NOW"

// fence names one incarnation of a copy: $2..$6 after the statement's own $1.
const fence = " WHERE index_name = $2 AND shard = $3 AND slot = $4 AND node_id = $5 AND epoch = $6"

// indexColumns is dialect.Indexes' IndexColumns.
const indexColumns = "mapping, settings, version, created_at, uid, mapping_version"

var changelog = dialect.Changelog{
	LockCounter:   withNow("SELECT value, NOW FROM sl_counter WHERE id = 1 FOR UPDATE"),
	ReadCounter:   "SELECT value FROM sl_counter WHERE id = 1",
	ReadHead:      withNow("SELECT value, NOW FROM sl_counter WHERE id = 1"),
	IndexState:    "SELECT uid, mapping_version, mapping FROM sl_indexes WHERE name = $1",
	DocumentSeq:   "SELECT seq FROM sl_documents WHERE index_name = $1 AND shard = $2 AND id = $3",
	QuerySeq:      "SELECT seq FROM sl_queries WHERE index_name = $1 AND shard = $2 AND id = $3",
	Write:         write,
	Limits:        limits,
	ChangesAfter:  "SELECT seq, kind, id, payload, payload_z, at, index_uid, mapping_version FROM sl_changes WHERE index_name = $1 AND shard = $2 AND seq > $3 ORDER BY seq LIMIT $4",
	Horizon:       "SELECT below_seq FROM sl_pruned WHERE index_name = $1 AND shard = $2",
	ScanDocuments: "SELECT id, body, body_z, seq FROM sl_documents WHERE index_name = $1 AND shard = $2 ORDER BY id",
	ScanQueries:   "SELECT id, query, meta, seq FROM sl_queries WHERE index_name = $1 AND shard = $2 ORDER BY id",
}

// writeSQL is the whole of an Apply's (or a mapping change's) write as one
// statement of fixed text: every table's rows travel as arrays, so a batch of
// any size is one round trip, parsed and planned once per connection, with no
// bind-parameter limit to chunk around. Empty arrays write nothing; a nil element
// of a bytea array is NULL. The final
// SELECT announces the commit.
//
// Precondition: the parts touch disjoint rows. The store sends at most one
// entry per key across a table's upserts and deletes (netState keeps the
// last change to each key). Postgres does not check this for us: the WITH
// parts all see the snapshot from before the statement, so an upsert and a
// delete of the same key would both "succeed" and the upsert would silently
// win; only two upserts of one key fail ("cannot affect row a second time").
const writeSQL = `WITH changes AS (
	INSERT INTO sl_changes (seq, index_name, shard, kind, id, payload, payload_z, at, index_uid, mapping_version)
	SELECT * FROM unnest($1::bigint[], $2::text[], $3::int[], $4::text[], $5::text[], $6::text[], $7::bytea[], $8::bigint[], $9::text[], $10::bigint[])
), documents AS (
	INSERT INTO sl_documents (index_name, shard, id, body, body_z, seq)
	SELECT * FROM unnest($11::text[], $12::int[], $13::text[], $14::text[], $15::bytea[], $16::bigint[])
	ON CONFLICT (index_name, shard, id) DO UPDATE SET body = EXCLUDED.body, body_z = EXCLUDED.body_z, seq = EXCLUDED.seq
), queries AS (
	INSERT INTO sl_queries (index_name, shard, id, query, meta, seq)
	SELECT * FROM unnest($17::text[], $18::int[], $19::text[], $20::text[], $21::text[], $22::bigint[])
	ON CONFLICT (index_name, shard, id) DO UPDATE SET query = EXCLUDED.query, meta = EXCLUDED.meta, seq = EXCLUDED.seq
), document_deletes AS (
	DELETE FROM sl_documents d USING unnest($23::text[], $24::int[], $25::text[]) AS k (index_name, shard, id)
	WHERE d.index_name = k.index_name AND d.shard = k.shard AND d.id = k.id
), query_deletes AS (
	DELETE FROM sl_queries q USING unnest($26::text[], $27::int[], $28::text[]) AS k (index_name, shard, id)
	WHERE q.index_name = k.index_name AND q.shard = k.shard AND q.id = k.id
), counter AS (
	UPDATE sl_counter SET value = $29 WHERE id = 1
)
SELECT pg_notify('` + notifyChannel + `', payload) FROM unnest($30::text[]) AS payload`

// limits is the engine's: one write statement carries at most 64 MB of
// payload, well below the 1 GB a single array value (and a protocol message)
// may hold. A batch beyond it is written in several statements of the same
// text. Bind parameters never limit it: writeSQL always takes 30.
var limits = dialect.Limits{Bytes: 64 << 20}

// write spells w in parts of about l.Bytes payload bytes: one, unless the
// batch is that large. Each part sets the same counter value; the deletes go
// with the first part and the notifications with the last, so they happen
// once.
func write(w *dialect.Write, l dialect.Limits) []dialect.Stmt {
	limit := l.Bytes
	changes := split(len(w.Changes), limit, func(i int) int { return len(w.Changes[i].Payload) + len(w.Changes[i].PayloadZ) })
	docs := split(len(w.Documents), limit, func(i int) int { return len(w.Documents[i].Body) + len(w.Documents[i].BodyZ) })
	queries := split(len(w.Queries), limit, func(i int) int { return len(w.Queries[i].Query) + len(w.Queries[i].Meta) })
	parts := max(len(changes), len(docs), len(queries), 1)
	out := make([]dialect.Stmt, parts)
	for p := range out {
		var a writeArgs
		if p < len(changes) {
			a.changes(w.Changes[changes[p][0]:changes[p][1]])
		}
		if p < len(docs) {
			a.documents(w.Documents[docs[p][0]:docs[p][1]])
		}
		if p < len(queries) {
			a.queries(w.Queries[queries[p][0]:queries[p][1]])
		}
		if p == 0 {
			a.dDel = keys(w.DocumentDeletes)
			a.qDel = keys(w.QueryDeletes)
		}
		var notify []string
		if p == parts-1 {
			notify = w.Notify
		}
		out[p] = dialect.Stmt{What: "write changes", SQL: writeSQL, Args: a.args(w.Counter, notify)}
	}
	return out
}

// split cuts n rows into [start, end) ranges whose sizes add up to at most
// limit, closing a range before the row that would pass it (a row larger
// than limit goes alone). A limit of 0 is no limit.
func split(n, limit int, size func(int) int) [][2]int {
	var out [][2]int
	for start := 0; start < n; {
		end, bytes := start, 0
		for end < n {
			sz := size(end)
			if limit > 0 && end > start && bytes+sz > limit {
				break
			}
			bytes += sz
			end++
		}
		out = append(out, [2]int{start, end})
		start = end
	}
	return out
}

// columns of the arrays writeSQL takes.
type keyColumns struct {
	index []string
	shard []int
	id    []string
}

func (k *keyColumns) add(index string, shard int, id string) {
	k.index = append(k.index, index)
	k.shard = append(k.shard, shard)
	k.id = append(k.id, id)
}

func keys(groups []dialect.DeleteGroup) keyColumns {
	var k keyColumns
	for _, g := range groups {
		for _, id := range g.IDs {
			k.add(g.Index, g.Shard, id)
		}
	}
	return k
}

type writeArgs struct {
	cSeq, cAt, cMV        []int64
	cKind, cPayload, cUID []string
	cPayloadZ             [][]byte
	c                     keyColumns
	d                     keyColumns
	dBody                 []string
	dBodyZ                [][]byte
	dSeq                  []int64
	q                     keyColumns
	qQuery, qMeta         []string
	qSeq                  []int64
	dDel, qDel            keyColumns
}

func (a *writeArgs) changes(rows []dialect.ChangeRow) {
	for i := range rows {
		r := &rows[i]
		a.cSeq = append(a.cSeq, r.Seq)
		a.c.add(r.Index, r.Shard, r.ID)
		a.cKind = append(a.cKind, r.Kind)
		a.cPayload = append(a.cPayload, r.Payload)
		a.cPayloadZ = append(a.cPayloadZ, r.PayloadZ)
		a.cAt = append(a.cAt, r.At)
		a.cUID = append(a.cUID, r.IndexUID)
		a.cMV = append(a.cMV, r.MappingVersion)
	}
}

func (a *writeArgs) documents(rows []dialect.DocumentRow) {
	for i := range rows {
		r := &rows[i]
		a.d.add(r.Index, r.Shard, r.ID)
		a.dBody = append(a.dBody, r.Body)
		a.dBodyZ = append(a.dBodyZ, r.BodyZ)
		a.dSeq = append(a.dSeq, r.Seq)
	}
}

func (a *writeArgs) queries(rows []dialect.QueryRow) {
	for i := range rows {
		r := &rows[i]
		a.q.add(r.Index, r.Shard, r.ID)
		a.qQuery = append(a.qQuery, r.Query)
		a.qMeta = append(a.qMeta, r.Meta)
		a.qSeq = append(a.qSeq, r.Seq)
	}
}

// args returns writeSQL's 30 arguments. Every array is non-nil: pgx sends a
// nil slice as NULL, and unnest(NULL) is no rows too, but an empty array says
// what is meant.
func (a *writeArgs) args(counter int64, notify []string) []any {
	return []any{
		ints64(a.cSeq), strs(a.c.index), ints(a.c.shard), strs(a.cKind), strs(a.c.id), strs(a.cPayload), byteas(a.cPayloadZ), ints64(a.cAt), strs(a.cUID), ints64(a.cMV),
		strs(a.d.index), ints(a.d.shard), strs(a.d.id), strs(a.dBody), byteas(a.dBodyZ), ints64(a.dSeq),
		strs(a.q.index), ints(a.q.shard), strs(a.q.id), strs(a.qQuery), strs(a.qMeta), ints64(a.qSeq),
		strs(a.dDel.index), ints(a.dDel.shard), strs(a.dDel.id),
		strs(a.qDel.index), ints(a.qDel.shard), strs(a.qDel.id),
		counter,
		strs(notify),
	}
}

func strs(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func byteas(s [][]byte) [][]byte {
	if s == nil {
		return [][]byte{}
	}
	return s
}

func ints(s []int) []int {
	if s == nil {
		return []int{}
	}
	return s
}

func ints64(s []int64) []int64 {
	if s == nil {
		return []int64{}
	}
	return s
}

var records = dialect.Records{
	GetDocument: "SELECT body, body_z, seq FROM sl_documents WHERE index_name = $1 AND shard = $2 AND id = $3",
	GetQuery:    "SELECT query, meta, seq FROM sl_queries WHERE index_name = $1 AND shard = $2 AND id = $3",
	ListQueries: "SELECT shard, id, query, meta, seq FROM sl_queries WHERE index_name = $1 AND id > $2 ORDER BY id LIMIT $3",
}

var registry = dialect.Registry{
	Heartbeat: withNow(`INSERT INTO sl_nodes (node_id, address, version, capacity, body_codecs, heartbeat_at, started_at) VALUES ($1, $2, $3, $4, $5, NOW, NOW)
ON CONFLICT (node_id) DO UPDATE SET address = EXCLUDED.address, version = EXCLUDED.version, capacity = EXCLUDED.capacity, body_codecs = EXCLUDED.body_codecs, heartbeat_at = EXCLUDED.heartbeat_at`),
	RemoveNode:    "DELETE FROM sl_nodes WHERE node_id = $1",
	Nodes:         withNow("SELECT node_id, address, version, capacity, body_codecs, heartbeat_at, started_at, NOW FROM sl_nodes ORDER BY node_id"),
	Features:      "SELECT name, enabled_at FROM sl_features",
	EnableFeature: withNow("INSERT INTO sl_features (name, enabled_at) VALUES ($1, NOW) ON CONFLICT (name) DO NOTHING"),
	IndexExists:   "SELECT COUNT(*) FROM sl_indexes WHERE name = $1",
	Slots:         withNow("SELECT " + copyColumns + " FROM sl_shard_copies WHERE index_name = $1 AND shard = $2 ORDER BY slot"),
	NextEpoch:     dialect.Returning{Read: "UPDATE sl_counter SET value = value + 1 WHERE id = 2 RETURNING value"},
	// The conflict update's WHERE leaves a slot another node holds under a
	// live lease untouched, and RETURNING then returns no row.
	Claim: dialect.Returning{Read: withNow(`INSERT INTO sl_shard_copies (index_name, shard, slot, node_id, state, applied_seq, lease_until, epoch)
VALUES ($1, $2, $3, $4, 'recovering', 0, NOW + $5, $6)
ON CONFLICT (index_name, shard, slot) DO UPDATE SET
	state = CASE WHEN sl_shard_copies.node_id = EXCLUDED.node_id THEN sl_shard_copies.state ELSE EXCLUDED.state END,
	applied_seq = CASE WHEN sl_shard_copies.node_id = EXCLUDED.node_id THEN sl_shard_copies.applied_seq ELSE 0 END,
	epoch = CASE WHEN sl_shard_copies.node_id = EXCLUDED.node_id THEN sl_shard_copies.epoch ELSE EXCLUDED.epoch END,
	node_id = EXCLUDED.node_id,
	lease_until = EXCLUDED.lease_until
WHERE sl_shard_copies.node_id = EXCLUDED.node_id OR sl_shard_copies.lease_until < NOW
RETURNING ` + copyColumns)},
	Renew:         dialect.Returning{Read: withNow("UPDATE sl_shard_copies SET lease_until = NOW + $1 WHERE node_id = $2 AND lease_until >= NOW RETURNING index_name, shard")},
	Release:       "DELETE FROM sl_shard_copies WHERE index_name = $1 AND shard = $2 AND slot = $3 AND node_id = $4 AND epoch = $5",
	Copies:        withNow("SELECT " + copyColumns + " FROM sl_shard_copies ORDER BY index_name, shard, slot"),
	IndexCopies:   withNow("SELECT " + copyColumns + " FROM sl_shard_copies WHERE index_name = $1 ORDER BY index_name, shard, slot"),
	SetState:      withNow("UPDATE sl_shard_copies SET state = $1" + fence + " AND lease_until >= NOW"),
	ReportApplied: "UPDATE sl_shard_copies SET applied_seq = GREATEST(applied_seq, $1)" + fence,
}

var blobs = dialect.Blobs{
	Clock:           withNow("SELECT NOW"),
	Register:        withNow("INSERT INTO sl_blob_uploads (upload_id, name, touched_at) VALUES ($1, $2, NOW)"),
	Touch:           withNow("UPDATE sl_blob_uploads SET touched_at = NOW WHERE upload_id = $1"),
	Unregister:      "DELETE FROM sl_blob_uploads WHERE upload_id = $1",
	RegisterGarbage: "INSERT INTO sl_blob_uploads (upload_id, name, touched_at) VALUES ($1, $2, 0)",
	WriteChunk:      "INSERT INTO sl_blob_chunks (upload_id, chunk, data) VALUES ($1, $2, $3)",
	ReadChunk:       "SELECT data FROM sl_blob_chunks WHERE upload_id = $1 AND chunk = $2",
	MaxChunk:        "SELECT MAX(chunk) FROM sl_blob_chunks WHERE upload_id = $1",
	DeleteChunks:    "DELETE FROM sl_blob_chunks WHERE upload_id = $1 AND chunk >= $2 AND chunk < $3",
	// The no-op conflict update locks an existing row and returns it.
	Take: dialect.Returning{Read: `INSERT INTO sl_blobs (name, upload_id, size, chunks, sha256, created_at) VALUES ($1, '', 0, 0, '', 0)
ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING upload_id`},
	Point:         "UPDATE sl_blobs SET upload_id = $1, size = $2, chunks = $3, sha256 = $4, created_at = $5 WHERE name = $6",
	LockForDelete: "SELECT upload_id FROM sl_blobs WHERE name = $1 FOR UPDATE",
	Delete:        "DELETE FROM sl_blobs WHERE name = $1",
	Stat:          "SELECT upload_id, size, chunks, sha256, created_at FROM sl_blobs WHERE name = $1",
	ListFrom:      "SELECT name, size, chunks, sha256, created_at FROM sl_blobs WHERE name >= $1 ORDER BY name",
	ListRange:     "SELECT name, size, chunks, sha256, created_at FROM sl_blobs WHERE name >= $1 AND name < $2 ORDER BY name",
	Stale: withNow(`SELECT upload_id FROM sl_blob_uploads u WHERE touched_at < NOW - $1
	AND NOT EXISTS (SELECT 1 FROM sl_blobs b WHERE b.upload_id = u.upload_id)`),
	ClaimStale: withNow("DELETE FROM sl_blob_uploads WHERE upload_id = $1 AND touched_at < NOW - $2"),
	Orphans: `SELECT DISTINCT upload_id FROM sl_blob_chunks c
	WHERE NOT EXISTS (SELECT 1 FROM sl_blobs b WHERE b.upload_id = c.upload_id)
	AND NOT EXISTS (SELECT 1 FROM sl_blob_uploads u WHERE u.upload_id = c.upload_id)`,
}

var indexes = dialect.Indexes{
	Create: dialect.Returning{Read: withNow(`INSERT INTO sl_indexes (name, mapping, settings, version, created_at, mapping_version, uid)
VALUES ($1, $2, $3, 1, NOW, 1, $4) RETURNING ` + indexColumns)},
	Get:     "SELECT " + indexColumns + " FROM sl_indexes WHERE name = $1",
	List:    "SELECT name, " + indexColumns + " FROM sl_indexes ORDER BY name",
	Current: "SELECT mapping, settings, version, mapping_version, uid FROM sl_indexes WHERE name = $1",
	Update:  "UPDATE sl_indexes SET mapping = $1, settings = $2, version = version + 1, mapping_version = $3 WHERE name = $4 AND version = $5",
	Drop:    "DELETE FROM sl_indexes WHERE name = $1",
	DropData: []string{
		"DELETE FROM sl_documents WHERE index_name = $1",
		"DELETE FROM sl_queries WHERE index_name = $1",
		"DELETE FROM sl_changes WHERE index_name = $1",
		"DELETE FROM sl_pruned WHERE index_name = $1",
		"DELETE FROM sl_shard_copies WHERE index_name = $1",
	},
}

var maintenance = dialect.Maintenance{
	MinSeq:        "SELECT MIN(seq) FROM sl_changes WHERE index_name = $1 AND shard = $2 AND seq < $3",
	DeleteChanges: "DELETE FROM sl_changes WHERE index_name = $1 AND shard = $2 AND seq >= $3 AND seq < $4",
	RaiseHorizon: dialect.Returning{Read: `INSERT INTO sl_pruned (index_name, shard, below_seq) VALUES ($1, $2, $3)
ON CONFLICT (index_name, shard) DO UPDATE SET below_seq = GREATEST(sl_pruned.below_seq, EXCLUDED.below_seq) RETURNING below_seq`},
	VersionTable: `CREATE TABLE IF NOT EXISTS sl_schema_migrations (
	version INTEGER NOT NULL PRIMARY KEY,
	name TEXT NOT NULL,
	applied_at BIGINT NOT NULL
)`,
	AppliedMigrations: "SELECT version FROM sl_schema_migrations",
	RecordMigration:   withNow("INSERT INTO sl_schema_migrations (version, name, applied_at) VALUES ($1, $2, NOW)"),
	MigrateInTx:       true,
	MigrateLock:       fmt.Sprintf("SELECT pg_advisory_xact_lock(%d)", int64(migrateLockKey)),
	// ALTER TABLE queues for its table's lock behind the queries running on it,
	// and every later query queues behind it meanwhile.
	MigrateLockTimeout: "SET LOCAL lock_timeout = '1s'",
	LockTimedOut:       lockTimedOut,
}
