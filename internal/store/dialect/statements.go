package dialect

// The statement tables. Every field is one statement in the engine's own
// spelling (placeholders, upserts, row locks, its clock), documented here by
// the arguments it takes, in order, and the columns it returns, in order. NOW
// stands for the database clock in Unix milliseconds: leases, heartbeats and
// change times are judged by that one clock, so node clock skew never
// matters.
//
// A few statements are built from rows (the batch writes); those are
// functions returning Stmts. A Returning is a write the store reads back.

// Stmt is one statement with its arguments.
type Stmt struct {
	// What names the step in errors ("insert changes", "upsert documents").
	What string
	SQL  string
	Args []any
}

// Returning is a write whose result the store reads back. An engine that can
// return rows from a write (RETURNING) leaves Write empty and makes Read the
// write itself, taking the write's arguments: one round trip. Otherwise Write
// runs first, with the write's arguments, and Read is a plain select with its
// own arguments. Either way Read returns the columns the field documents.
type Returning struct {
	Write string
	Read  string
}

// ChangeRow is one changelog row.
type ChangeRow struct {
	Seq            int64
	Index          string
	Shard          int
	Kind           string
	ID             string
	Payload        string
	At             int64
	IndexUID       string
	MappingVersion int64
}

// DocumentRow is a document's new state.
type DocumentRow struct {
	Index string
	Shard int
	ID    string
	Body  string
	Seq   int64
}

// QueryRow is a saved query's new state.
type QueryRow struct {
	Index string
	Shard int
	ID    string
	Query string
	Meta  string
	Seq   int64
}

// DeleteGroup is the ids of one shard of one index to delete.
type DeleteGroup struct {
	Index string
	Shard int
	IDs   []string
}

// Write is everything a transaction holding the counter lock writes once
// the store has allocated its seqs: the changelog rows, the net effect on
// sl_documents and sl_queries (at most one entry per key across the upserts
// and deletes of a table), the counter's new value, and the notification
// payloads (only when the dialect has Listen).
type Write struct {
	Changes         []ChangeRow
	Documents       []DocumentRow
	Queries         []QueryRow
	DocumentDeletes []DeleteGroup
	QueryDeletes    []DeleteGroup
	Counter         int64
	Notify          []string
}

// Changelog is the write-ahead log and the documents and saved queries it
// maintains.
type Changelog struct {
	// LockCounter: () -> value, NOW of sl_counter row 1, locking the row
	// until the transaction ends ("" FOR UPDATE where the transaction already
	// holds the database's write lock).
	LockCounter string
	// ReadCounter: () -> value of sl_counter row 1, without a lock.
	ReadCounter string
	// ReadHead: () -> value of sl_counter row 1, NOW.
	ReadHead string
	// IndexState: (name) -> uid, mapping_version, mapping of sl_indexes.
	IndexState string
	// DocumentSeq and QuerySeq: (index, shard, id) -> seq.
	DocumentSeq string
	QuerySeq    string
	// Write returns the statements that store w, in order, inside the
	// transaction holding the counter lock. The store prepares a statement
	// text that runs several times in a row once for the run, so one row a
	// statement costs no parse per row.
	Write func(w *Write) []Stmt
	// ChangesAfter: (index, shard, seq, limit) -> seq, kind, id, payload, at,
	// index_uid, mapping_version of the shard's changes after seq, by seq.
	ChangesAfter string
	// Horizon: (index, shard) -> below_seq of sl_pruned.
	Horizon string
	// ScanDocuments: (index, shard) -> id, body, seq, by id.
	ScanDocuments string
	// ScanQueries: (index, shard) -> id, query, meta, seq, by id.
	ScanQueries string
}

// Records reads the system of record directly.
type Records struct {
	// GetDocument: (index, shard, id) -> body, seq.
	GetDocument string
	// GetQuery: (index, shard, id) -> query, meta, seq.
	GetQuery string
	// ListQueries: (index, after, limit) -> shard, id, query, meta, seq of the
	// index's saved queries with id > after, by id.
	ListQueries string
}

// Registry is the cluster registry: sl_nodes and the shard-copy slots of
// sl_shard_copies. CopyColumns below is index_name, shard, slot, node_id,
// state, applied_seq, epoch, lease_until, NOW. A fence is the five
// arguments (index, shard, slot, node_id, epoch) naming one incarnation of a
// copy.
type Registry struct {
	// Heartbeat: (node_id, address, version, capacity) inserts the node with
	// heartbeat_at and started_at NOW, or updates its address, version,
	// capacity and heartbeat_at.
	Heartbeat string
	// RemoveNode: (node_id).
	RemoveNode string
	// Nodes: () -> node_id, address, version, capacity, heartbeat_at,
	// started_at, NOW, by node_id.
	Nodes string
	// IndexExists: (name) -> the number of sl_indexes rows named name.
	IndexExists string
	// Slots: (index, shard) -> CopyColumns of the shard's slots, by slot.
	Slots string
	// NextEpoch: Write () bumps sl_counter row 2; Read () -> its new value.
	NextEpoch Returning
	// Claim is the conditional insert-or-steal of one slot. Write (index,
	// shard, slot, node_id, ttl_ms, epoch) inserts the slot for node_id in
	// state "recovering" at applied_seq 0 with epoch and lease_until NOW +
	// ttl_ms, or takes it over the same way when its lease has expired, or
	// renews only lease_until when node_id already holds it (keeping its
	// state, applied_seq and epoch); otherwise it leaves the slot alone. Read
	// (index, shard, slot) -> CopyColumns of the slot; a RETURNING Read
	// returns no row when the slot was left alone.
	Claim Returning
	// Renew extends node_id's unexpired leases. Write (ttl_ms, node_id) sets
	// lease_until NOW + ttl_ms where lease_until >= NOW; Read (node_id) ->
	// index_name, shard of those copies, in any order.
	Renew Returning
	// Release: fence. Deletes the copy.
	Release string
	// Copies: () -> CopyColumns of every copy, by index_name, shard, slot.
	Copies string
	// IndexCopies: (index) -> CopyColumns of the index's copies, by shard,
	// slot.
	IndexCopies string
	// SetState: (state, fence) where lease_until >= NOW.
	SetState string
	// ReportApplied: (seq, fence) sets applied_seq to the larger of it and
	// seq.
	ReportApplied string
}

// Blobs is the blob store: sl_blobs, sl_blob_uploads and sl_blob_chunks.
type Blobs struct {
	// Clock: () -> NOW.
	Clock string
	// Register: (upload_id, name) registers an upload with touched_at NOW.
	Register string
	// Touch: (upload_id) sets touched_at NOW.
	Touch string
	// Unregister: (upload_id) deletes the registration.
	Unregister string
	// RegisterGarbage: (upload_id, name) registers an upload with
	// touched_at 0.
	RegisterGarbage string
	// WriteChunk: (upload_id, chunk, data).
	WriteChunk string
	// ReadChunk: (upload_id, chunk) -> data.
	ReadChunk string
	// MaxChunk: (upload_id) -> the highest chunk number, or NULL.
	MaxChunk string
	// DeleteChunks: (upload_id, from, to) deletes chunks from <= chunk < to.
	DeleteChunks string
	// Take makes sure the blob's row exists and locks it until the
	// transaction ends. Write (name) inserts an empty row (upload_id '', size
	// 0, chunks 0, sha256 '', created_at 0) unless one exists; Read (name) ->
	// the row's upload_id.
	Take Returning
	// Point: (upload_id, size, chunks, sha256, created_at, name) updates the
	// blob's row.
	Point string
	// LockForDelete: (name) -> upload_id of the row, locked until the
	// transaction ends.
	LockForDelete string
	// Delete: (name) deletes the blob's row.
	Delete string
	// Stat: (name) -> upload_id, size, chunks, sha256, created_at.
	Stat string
	// ListFrom: (from) and ListRange: (from, to) -> name, size, chunks,
	// sha256, created_at of the blobs with from <= name (< to), by name.
	ListFrom  string
	ListRange string
	// Stale: (ms) -> upload_id of registered uploads touched before NOW - ms
	// that no blob references.
	Stale string
	// ClaimStale: (upload_id, ms) deletes the registration if it is still
	// touched before NOW - ms.
	ClaimStale string
	// Orphans: () -> distinct upload_id of chunks that no blob references and
	// no registration names.
	Orphans string
}

// Indexes is the index catalogue in sl_indexes. IndexColumns is mapping,
// settings, version, created_at, uid, mapping_version.
type Indexes struct {
	// Create inserts an index at version 1 and mapping_version 1 with
	// created_at NOW. Write (name, mapping, settings, uid); Read (name) ->
	// IndexColumns. A key violation fails the write.
	Create Returning
	// Get: (name) -> IndexColumns.
	Get string
	// List: () -> name, IndexColumns, by name.
	List string
	// Current: (name) -> mapping, settings, version, mapping_version, uid.
	Current string
	// Update: (mapping, settings, mapping_version, name, version) sets them
	// and moves version on by one where version still matches.
	Update string
	// Drop: (name) deletes the index's row.
	Drop string
	// DropData: each (name), deletes the index's documents, queries,
	// changes, prune horizons and shard copies.
	DropData []string
}

// Maintenance is pruning and the schema migrations.
type Maintenance struct {
	// MinSeq: (index, shard, below) -> the lowest seq of the shard's changes
	// below below, or NULL.
	MinSeq string
	// DeleteChanges: (index, shard, from, to) deletes the shard's changes with
	// from <= seq < to.
	DeleteChanges string
	// RaiseHorizon sets the shard's prune horizon to the larger of the
	// current one (if any) and below. Write (index, shard, below); Read
	// (index, shard) -> the horizon after the write.
	RaiseHorizon Returning

	// VersionTable creates sl_schema_migrations if it is missing.
	VersionTable string
	// AppliedMigrations: () -> version of every applied migration.
	AppliedMigrations string
	// RecordMigration: (version, name) with applied_at NOW.
	RecordMigration string
	// MigrateInTx says DDL is transactional: every pending migration runs in
	// one transaction that first executes MigrateLock (which may be empty when
	// the transaction itself excludes other writers, as on SQLite). When false,
	// migrations run on one connection holding SessionLock until
	// SessionUnlock, and each file is recorded as it completes.
	MigrateInTx   bool
	MigrateLock   string
	SessionLock   string // must return one row whose first column is 1 on success
	SessionUnlock string
}
