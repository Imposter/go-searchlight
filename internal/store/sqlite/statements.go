package sqlite

import (
	"strings"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

// now is the database clock in Unix milliseconds.
const now = "CAST((julianday('now') - 2440587.5) * 86400000.0 AS INTEGER)"

// withNow spells the clock: the statements below write NOW for it.
func withNow(q string) string { return strings.ReplaceAll(q, "NOW", "("+now+")") }

// copyColumns is dialect.Registry's CopyColumns.
const copyColumns = "index_name, shard, slot, node_id, state, applied_seq, epoch, lease_until, NOW"

// fence names one incarnation of a copy, after the statement's own argument.
const fence = " WHERE index_name = ? AND shard = ? AND slot = ? AND node_id = ? AND epoch = ?"

// indexColumns is dialect.Indexes' IndexColumns.
const indexColumns = "mapping, settings, version, created_at, uid, mapping_version"

// Writes run on the writer pool, whose transactions begin with BEGIN
// IMMEDIATE: holding SQLite's write lock is holding every row lock, so no
// statement here needs FOR UPDATE.
var changelog = dialect.Changelog{
	LockCounter:   withNow("SELECT value, NOW FROM sl_counter WHERE id = 1"),
	ReadCounter:   "SELECT value FROM sl_counter WHERE id = 1",
	ReadHead:      withNow("SELECT value, NOW FROM sl_counter WHERE id = 1"),
	IndexState:    "SELECT uid, mapping_version, mapping FROM sl_indexes WHERE name = ?",
	DocumentSeq:   "SELECT seq FROM sl_documents WHERE index_name = ? AND shard = ? AND id = ?",
	QuerySeq:      "SELECT seq FROM sl_queries WHERE index_name = ? AND shard = ? AND id = ?",
	Write:         write,
	ChangesAfter:  "SELECT seq, kind, id, payload, payload_z, at, index_uid, mapping_version FROM sl_changes WHERE index_name = ? AND shard = ? AND seq > ? ORDER BY seq LIMIT ?",
	Horizon:       "SELECT below_seq FROM sl_pruned WHERE index_name = ? AND shard = ?",
	ScanDocuments: "SELECT id, body, body_z, seq FROM sl_documents WHERE index_name = ? AND shard = ? ORDER BY id",
	ScanQueries:   "SELECT id, query, meta, seq FROM sl_queries WHERE index_name = ? AND shard = ? ORDER BY id",
}

// write spells w one row a statement; see the package comment for why.
func write(w *dialect.Write, _ dialect.Limits) []dialect.Stmt {
	out := make([]dialect.Stmt, 0, len(w.Changes)+len(w.Documents)+len(w.Queries)+len(w.DocumentDeletes)+len(w.QueryDeletes)+1)
	for i := range w.Changes {
		r := &w.Changes[i]
		out = append(out, dialect.Stmt{
			What: "insert changes", SQL: insertChange,
			Args: []any{r.Seq, r.Index, r.Shard, r.Kind, r.ID, r.Payload, dialect.Blob(r.PayloadZ), r.At, r.IndexUID, r.MappingVersion},
		})
	}
	for i := range w.Documents {
		r := &w.Documents[i]
		out = append(out, dialect.Stmt{What: "upsert documents", SQL: upsertDocument, Args: []any{r.Index, r.Shard, r.ID, r.Body, dialect.Blob(r.BodyZ), r.Seq}})
	}
	for i := range w.Queries {
		r := &w.Queries[i]
		out = append(out, dialect.Stmt{What: "upsert queries", SQL: upsertQuery, Args: []any{r.Index, r.Shard, r.ID, r.Query, r.Meta, r.Seq}})
	}
	for _, g := range w.DocumentDeletes {
		for _, id := range g.IDs {
			out = append(out, dialect.Stmt{What: "delete documents", SQL: deleteDocument, Args: []any{g.Index, g.Shard, id}})
		}
	}
	for _, g := range w.QueryDeletes {
		for _, id := range g.IDs {
			out = append(out, dialect.Stmt{What: "delete queries", SQL: deleteQuery, Args: []any{g.Index, g.Shard, id}})
		}
	}
	return append(out, dialect.Stmt{What: "advance counter", SQL: "UPDATE sl_counter SET value = ? WHERE id = 1", Args: []any{w.Counter}})
}

const (
	insertChange   = "INSERT INTO sl_changes (seq, index_name, shard, kind, id, payload, payload_z, at, index_uid, mapping_version) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
	upsertDocument = "INSERT INTO sl_documents (index_name, shard, id, body, body_z, seq) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (index_name, shard, id) DO UPDATE SET body = excluded.body, body_z = excluded.body_z, seq = excluded.seq"
	upsertQuery    = "INSERT INTO sl_queries (index_name, shard, id, query, meta, seq) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT (index_name, shard, id) DO UPDATE SET query = excluded.query, meta = excluded.meta, seq = excluded.seq"
	deleteDocument = "DELETE FROM sl_documents WHERE index_name = ? AND shard = ? AND id = ?"
	deleteQuery    = "DELETE FROM sl_queries WHERE index_name = ? AND shard = ? AND id = ?"
)

var records = dialect.Records{
	GetDocument: "SELECT body, body_z, seq FROM sl_documents WHERE index_name = ? AND shard = ? AND id = ?",
	GetQuery:    "SELECT query, meta, seq FROM sl_queries WHERE index_name = ? AND shard = ? AND id = ?",
	ListQueries: "SELECT shard, id, query, meta, seq FROM sl_queries WHERE index_name = ? AND id > ? ORDER BY id LIMIT ?",
}

var registry = dialect.Registry{
	Heartbeat: withNow(`INSERT INTO sl_nodes (node_id, address, version, capacity, body_codecs, heartbeat_at, started_at) VALUES (?, ?, ?, ?, ?, NOW, NOW)
ON CONFLICT (node_id) DO UPDATE SET address = excluded.address, version = excluded.version, capacity = excluded.capacity, body_codecs = excluded.body_codecs, heartbeat_at = excluded.heartbeat_at`),
	RemoveNode:    "DELETE FROM sl_nodes WHERE node_id = ?",
	Nodes:         withNow("SELECT node_id, address, version, capacity, body_codecs, heartbeat_at, started_at, NOW FROM sl_nodes ORDER BY node_id"),
	Features:      "SELECT name, enabled_at FROM sl_features",
	EnableFeature: withNow("INSERT INTO sl_features (name, enabled_at) VALUES (?, NOW) ON CONFLICT (name) DO NOTHING"),
	IndexExists:   "SELECT COUNT(*) FROM sl_indexes WHERE name = ?",
	Slots:         withNow("SELECT " + copyColumns + " FROM sl_shard_copies WHERE index_name = ? AND shard = ? ORDER BY slot"),
	NextEpoch:     dialect.Returning{Read: "UPDATE sl_counter SET value = value + 1 WHERE id = 2 RETURNING value"},
	// The conflict update's WHERE leaves a slot another node holds under a
	// live lease untouched, and RETURNING then returns no row.
	Claim: dialect.Returning{Read: withNow(`INSERT INTO sl_shard_copies (index_name, shard, slot, node_id, state, applied_seq, lease_until, epoch)
VALUES (?1, ?2, ?3, ?4, 'recovering', 0, NOW + ?5, ?6)
ON CONFLICT (index_name, shard, slot) DO UPDATE SET
	state = CASE WHEN sl_shard_copies.node_id = excluded.node_id THEN sl_shard_copies.state ELSE excluded.state END,
	applied_seq = CASE WHEN sl_shard_copies.node_id = excluded.node_id THEN sl_shard_copies.applied_seq ELSE 0 END,
	epoch = CASE WHEN sl_shard_copies.node_id = excluded.node_id THEN sl_shard_copies.epoch ELSE excluded.epoch END,
	node_id = excluded.node_id,
	lease_until = excluded.lease_until
WHERE sl_shard_copies.node_id = excluded.node_id OR sl_shard_copies.lease_until < NOW
RETURNING ` + copyColumns)},
	Renew:       dialect.Returning{Read: withNow("UPDATE sl_shard_copies SET lease_until = NOW + ?1 WHERE node_id = ?2 AND lease_until >= NOW RETURNING index_name, shard")},
	Release:     "DELETE FROM sl_shard_copies WHERE index_name = ? AND shard = ? AND slot = ? AND node_id = ? AND epoch = ?",
	Copies:      withNow("SELECT " + copyColumns + " FROM sl_shard_copies ORDER BY index_name, shard, slot"),
	IndexCopies: withNow("SELECT " + copyColumns + " FROM sl_shard_copies WHERE index_name = ? ORDER BY index_name, shard, slot"),
	SetState:    withNow("UPDATE sl_shard_copies SET state = ?" + fence + " AND lease_until >= NOW"),
	// SQLite's multi-argument MAX is the scalar function, not the aggregate.
	ReportApplied: "UPDATE sl_shard_copies SET applied_seq = MAX(applied_seq, ?)" + fence,
}

var blobs = dialect.Blobs{
	Clock:           withNow("SELECT NOW"),
	Register:        withNow("INSERT INTO sl_blob_uploads (upload_id, name, touched_at) VALUES (?, ?, NOW)"),
	Touch:           withNow("UPDATE sl_blob_uploads SET touched_at = NOW WHERE upload_id = ?"),
	Unregister:      "DELETE FROM sl_blob_uploads WHERE upload_id = ?",
	RegisterGarbage: "INSERT INTO sl_blob_uploads (upload_id, name, touched_at) VALUES (?, ?, 0)",
	WriteChunk:      "INSERT INTO sl_blob_chunks (upload_id, chunk, data) VALUES (?, ?, ?)",
	ReadChunk:       "SELECT data FROM sl_blob_chunks WHERE upload_id = ? AND chunk = ?",
	MaxChunk:        "SELECT MAX(chunk) FROM sl_blob_chunks WHERE upload_id = ?",
	DeleteChunks:    "DELETE FROM sl_blob_chunks WHERE upload_id = ? AND chunk >= ? AND chunk < ?",
	Take: dialect.Returning{Read: `INSERT INTO sl_blobs (name, upload_id, size, chunks, sha256, created_at) VALUES (?, '', 0, 0, '', 0)
ON CONFLICT (name) DO UPDATE SET name = excluded.name RETURNING upload_id`},
	Point:         "UPDATE sl_blobs SET upload_id = ?, size = ?, chunks = ?, sha256 = ?, created_at = ? WHERE name = ?",
	LockForDelete: "SELECT upload_id FROM sl_blobs WHERE name = ?",
	Delete:        "DELETE FROM sl_blobs WHERE name = ?",
	Stat:          "SELECT upload_id, size, chunks, sha256, created_at FROM sl_blobs WHERE name = ?",
	ListFrom:      "SELECT name, size, chunks, sha256, created_at FROM sl_blobs WHERE name >= ? ORDER BY name",
	ListRange:     "SELECT name, size, chunks, sha256, created_at FROM sl_blobs WHERE name >= ? AND name < ? ORDER BY name",
	Stale: withNow(`SELECT upload_id FROM sl_blob_uploads u WHERE touched_at < NOW - ?
	AND NOT EXISTS (SELECT 1 FROM sl_blobs b WHERE b.upload_id = u.upload_id)`),
	ClaimStale: withNow("DELETE FROM sl_blob_uploads WHERE upload_id = ? AND touched_at < NOW - ?"),
	Orphans: `SELECT DISTINCT upload_id FROM sl_blob_chunks c
	WHERE NOT EXISTS (SELECT 1 FROM sl_blobs b WHERE b.upload_id = c.upload_id)
	AND NOT EXISTS (SELECT 1 FROM sl_blob_uploads u WHERE u.upload_id = c.upload_id)`,
}

var indexes = dialect.Indexes{
	Create: dialect.Returning{Read: withNow(`INSERT INTO sl_indexes (name, mapping, settings, version, created_at, mapping_version, uid)
VALUES (?, ?, ?, 1, NOW, 1, ?) RETURNING ` + indexColumns)},
	Get:     "SELECT " + indexColumns + " FROM sl_indexes WHERE name = ?",
	List:    "SELECT name, " + indexColumns + " FROM sl_indexes ORDER BY name",
	Current: "SELECT mapping, settings, version, mapping_version, uid FROM sl_indexes WHERE name = ?",
	Update:  "UPDATE sl_indexes SET mapping = ?, settings = ?, version = version + 1, mapping_version = ? WHERE name = ? AND version = ?",
	Drop:    "DELETE FROM sl_indexes WHERE name = ?",
	DropData: []string{
		"DELETE FROM sl_documents WHERE index_name = ?",
		"DELETE FROM sl_queries WHERE index_name = ?",
		"DELETE FROM sl_changes WHERE index_name = ?",
		"DELETE FROM sl_pruned WHERE index_name = ?",
		"DELETE FROM sl_shard_copies WHERE index_name = ?",
	},
}

var maintenance = dialect.Maintenance{
	MinSeq:        "SELECT MIN(seq) FROM sl_changes WHERE index_name = ? AND shard = ? AND seq < ?",
	DeleteChanges: "DELETE FROM sl_changes WHERE index_name = ? AND shard = ? AND seq >= ? AND seq < ?",
	// In DO UPDATE, a bare column is the existing row's value.
	RaiseHorizon: dialect.Returning{Read: `INSERT INTO sl_pruned (index_name, shard, below_seq) VALUES (?, ?, ?)
ON CONFLICT (index_name, shard) DO UPDATE SET below_seq = MAX(below_seq, excluded.below_seq) RETURNING below_seq`},
	VersionTable: `CREATE TABLE IF NOT EXISTS sl_schema_migrations (
	version INTEGER NOT NULL PRIMARY KEY,
	name TEXT NOT NULL,
	applied_at INTEGER NOT NULL
)`,
	AppliedMigrations: "SELECT version FROM sl_schema_migrations",
	RecordMigration:   withNow("INSERT INTO sl_schema_migrations (version, name, applied_at) VALUES (?, ?, NOW)"),
	MigrateInTx:       true, // BEGIN IMMEDIATE excludes every other writer
}
