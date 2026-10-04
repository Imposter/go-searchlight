package mysql

import (
	"strings"

	"github.com/Imposter/go-searchlight/internal/store/dialect"
)

// now is the database clock in Unix milliseconds. Sessions run in UTC (see
// Config), so it is never ambiguous across a DST change.
const now = "CAST(UNIX_TIMESTAMP(NOW(3)) * 1000 AS SIGNED)"

// withNow spells the clock: the statements below write NOW for it.
func withNow(q string) string { return strings.ReplaceAll(q, "NOW", now) }

// limits are the engine's batch limits: MySQL takes up to 65,535
// placeholders in a statement, and the driver interpolates the arguments into
// one packet, which must fit max_allowed_packet. A batch statement stays near
// 16 MB, a quarter of the 64 MB default; the server's max_allowed_packet must
// be at least 64 MB, which a single 32 MB document (MaxPayloadBytes) needs
// anyway.
var limits = dialect.Limits{Params: 65535, Bytes: 16 << 20}

// copyColumns is dialect.Registry's CopyColumns.
const copyColumns = "index_name, shard, slot, node_id, state, applied_seq, epoch, lease_until, NOW"

// fence names one incarnation of a copy, after the statement's own argument.
const fence = " WHERE index_name = ? AND shard = ? AND slot = ? AND node_id = ? AND epoch = ?"

// indexColumns is dialect.Indexes' IndexColumns.
const indexColumns = "mapping, settings, version, created_at, uid, mapping_version"

var changelog = dialect.Changelog{
	LockCounter:   withNow("SELECT value, NOW FROM sl_counter WHERE id = 1 FOR UPDATE"),
	ReadCounter:   "SELECT value FROM sl_counter WHERE id = 1",
	ReadHead:      withNow("SELECT value, NOW FROM sl_counter WHERE id = 1"),
	IndexState:    "SELECT uid, mapping_version, mapping FROM sl_indexes WHERE name = ?",
	DocumentSeq:   "SELECT seq FROM sl_documents WHERE index_name = ? AND shard = ? AND id = ?",
	QuerySeq:      "SELECT seq FROM sl_queries WHERE index_name = ? AND shard = ? AND id = ?",
	Write:         write,
	Limits:        limits,
	ChangesAfter:  "SELECT seq, kind, id, payload, at, index_uid, mapping_version FROM sl_changes WHERE index_name = ? AND shard = ? AND seq > ? ORDER BY seq LIMIT ?",
	Horizon:       "SELECT below_seq FROM sl_pruned WHERE index_name = ? AND shard = ?",
	ScanDocuments: "SELECT id, body, seq FROM sl_documents WHERE index_name = ? AND shard = ? ORDER BY id",
	ScanQueries:   "SELECT id, query, meta, seq FROM sl_queries WHERE index_name = ? AND shard = ? ORDER BY id",
}

// write spells w as multi-row INSERTs as large as l allows, upserts with ON
// DUPLICATE KEY UPDATE through a row alias, and IN-list deletes.
func write(w *dialect.Write, l dialect.Limits) []dialect.Stmt {
	out := values("insert changes", "INSERT INTO sl_changes ("+changeColumns+") VALUES ", "",
		9, l, changeArgs(w.Changes))
	out = append(out, values("upsert documents", "INSERT INTO sl_documents ("+documentColumns+") VALUES ",
		" AS new ON DUPLICATE KEY UPDATE body = new.body, seq = new.seq",
		5, l, documentArgs(w.Documents))...)
	out = append(out, values("upsert queries", "INSERT INTO sl_queries ("+queryColumns+") VALUES ",
		" AS new ON DUPLICATE KEY UPDATE query = new.query, meta = new.meta, seq = new.seq",
		6, l, queryArgs(w.Queries))...)
	for _, g := range w.DocumentDeletes {
		out = append(out, in("delete documents", "DELETE FROM sl_documents WHERE index_name = ? AND shard = ? AND id IN (",
			[]any{g.Index, g.Shard}, g.IDs, l)...)
	}
	for _, g := range w.QueryDeletes {
		out = append(out, in("delete queries", "DELETE FROM sl_queries WHERE index_name = ? AND shard = ? AND id IN (",
			[]any{g.Index, g.Shard}, g.IDs, l)...)
	}
	return append(out, dialect.Stmt{What: "advance counter", SQL: "UPDATE sl_counter SET value = ? WHERE id = 1", Args: []any{w.Counter}})
}

var records = dialect.Records{
	GetDocument: "SELECT body, seq FROM sl_documents WHERE index_name = ? AND shard = ? AND id = ?",
	GetQuery:    "SELECT query, meta, seq FROM sl_queries WHERE index_name = ? AND shard = ? AND id = ?",
	ListQueries: "SELECT shard, id, query, meta, seq FROM sl_queries WHERE index_name = ? AND id > ? ORDER BY id LIMIT ?",
}

var registry = dialect.Registry{
	Heartbeat: withNow(`INSERT INTO sl_nodes (node_id, address, version, capacity, heartbeat_at, started_at) VALUES (?, ?, ?, ?, NOW, NOW)
AS new ON DUPLICATE KEY UPDATE address = new.address, version = new.version, capacity = new.capacity, heartbeat_at = new.heartbeat_at`),
	RemoveNode:  "DELETE FROM sl_nodes WHERE node_id = ?",
	Nodes:       withNow("SELECT node_id, address, version, capacity, heartbeat_at, started_at, NOW FROM sl_nodes ORDER BY node_id"),
	IndexExists: "SELECT COUNT(*) FROM sl_indexes WHERE name = ?",
	Slots:       withNow("SELECT " + copyColumns + " FROM sl_shard_copies WHERE index_name = ? AND shard = ? ORDER BY slot"),
	NextEpoch: dialect.Returning{
		Write: "UPDATE sl_counter SET value = value + 1 WHERE id = 2",
		Read:  "SELECT value FROM sl_counter WHERE id = 2",
	},
	// In ON DUPLICATE KEY UPDATE, sl_shard_copies.column is the existing
	// row's value and new.column the proposed row's (the row alias, MySQL
	// 8.0.19+; a bare column would be ambiguous). MySQL evaluates the
	// assignments left to right, each seeing the ones before, so node_id
	// changes after state, applied_seq and epoch have read the old owner, and
	// lease_until last.
	Claim: dialect.Returning{
		Write: withNow(`INSERT INTO sl_shard_copies (index_name, shard, slot, node_id, state, applied_seq, lease_until, epoch)
VALUES (?, ?, ?, ?, 'recovering', 0, NOW + ?, ?)
AS new ON DUPLICATE KEY UPDATE
	state = IF(sl_shard_copies.node_id = new.node_id, sl_shard_copies.state, IF(sl_shard_copies.lease_until < NOW, new.state, sl_shard_copies.state)),
	applied_seq = IF(sl_shard_copies.node_id = new.node_id, sl_shard_copies.applied_seq, IF(sl_shard_copies.lease_until < NOW, 0, sl_shard_copies.applied_seq)),
	epoch = IF(sl_shard_copies.node_id = new.node_id, sl_shard_copies.epoch, IF(sl_shard_copies.lease_until < NOW, new.epoch, sl_shard_copies.epoch)),
	node_id = IF(sl_shard_copies.lease_until < NOW, new.node_id, sl_shard_copies.node_id),
	lease_until = IF(sl_shard_copies.node_id = new.node_id, new.lease_until, sl_shard_copies.lease_until)`),
		// The claim locked the slot row; this reads who holds it now.
		Read: withNow("SELECT " + copyColumns + " FROM sl_shard_copies WHERE index_name = ? AND shard = ? AND slot = ?"),
	},
	Renew: dialect.Returning{
		Write: withNow("UPDATE sl_shard_copies SET lease_until = NOW + ? WHERE node_id = ? AND lease_until >= NOW"),
		Read:  withNow("SELECT index_name, shard FROM sl_shard_copies WHERE node_id = ? AND lease_until >= NOW"),
	},
	Release:       "DELETE FROM sl_shard_copies WHERE index_name = ? AND shard = ? AND slot = ? AND node_id = ? AND epoch = ?",
	Copies:        withNow("SELECT " + copyColumns + " FROM sl_shard_copies ORDER BY index_name, shard, slot"),
	IndexCopies:   withNow("SELECT " + copyColumns + " FROM sl_shard_copies WHERE index_name = ? ORDER BY index_name, shard, slot"),
	SetState:      withNow("UPDATE sl_shard_copies SET state = ?" + fence + " AND lease_until >= NOW"),
	ReportApplied: "UPDATE sl_shard_copies SET applied_seq = GREATEST(applied_seq, ?)" + fence,
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
	Take: dialect.Returning{
		Write: "INSERT INTO sl_blobs (name, upload_id, size, chunks, sha256, created_at) VALUES (?, '', 0, 0, '', 0) AS new ON DUPLICATE KEY UPDATE name = new.name",
		Read:  "SELECT upload_id FROM sl_blobs WHERE name = ? FOR UPDATE",
	},
	Point:         "UPDATE sl_blobs SET upload_id = ?, size = ?, chunks = ?, sha256 = ?, created_at = ? WHERE name = ?",
	LockForDelete: "SELECT upload_id FROM sl_blobs WHERE name = ? FOR UPDATE",
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
	Create: dialect.Returning{
		Write: withNow("INSERT INTO sl_indexes (name, mapping, settings, version, created_at, mapping_version, uid) VALUES (?, ?, ?, 1, NOW, 1, ?)"),
		Read:  "SELECT " + indexColumns + " FROM sl_indexes WHERE name = ?",
	},
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
	RaiseHorizon: dialect.Returning{
		Write: "INSERT INTO sl_pruned (index_name, shard, below_seq) VALUES (?, ?, ?) AS new ON DUPLICATE KEY UPDATE below_seq = GREATEST(sl_pruned.below_seq, new.below_seq)",
		Read:  "SELECT below_seq FROM sl_pruned WHERE index_name = ? AND shard = ?",
	},
	VersionTable: `CREATE TABLE IF NOT EXISTS sl_schema_migrations (
	version INT NOT NULL PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	applied_at BIGINT NOT NULL
) ENGINE=InnoDB`,
	AppliedMigrations: "SELECT version FROM sl_schema_migrations",
	RecordMigration:   withNow("INSERT INTO sl_schema_migrations (version, name, applied_at) VALUES (?, ?, NOW)"),
	MigrateInTx:       false,
	SessionLock:       "SELECT GET_LOCK(CONCAT('searchlight.migrate.', MD5(DATABASE())), 120)",
	SessionUnlock:     "SELECT RELEASE_LOCK(CONCAT('searchlight.migrate.', MD5(DATABASE())))",
}
