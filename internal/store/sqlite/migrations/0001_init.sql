-- Searchlight schema v1 (spec §8). Times are Unix milliseconds from the
-- database clock; JSON is stored as text.

CREATE TABLE IF NOT EXISTS sl_indexes (
	name TEXT NOT NULL PRIMARY KEY,
	mapping TEXT NOT NULL,
	settings TEXT NOT NULL,
	version INTEGER NOT NULL,
	created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sl_documents (
	index_name TEXT NOT NULL,
	shard INTEGER NOT NULL,
	id TEXT NOT NULL,
	body TEXT NOT NULL,
	seq INTEGER NOT NULL,
	PRIMARY KEY (index_name, shard, id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS sl_queries (
	index_name TEXT NOT NULL,
	shard INTEGER NOT NULL,
	id TEXT NOT NULL,
	query TEXT NOT NULL,
	meta TEXT NOT NULL,
	seq INTEGER NOT NULL,
	PRIMARY KEY (index_name, shard, id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS sl_changes (
	seq INTEGER NOT NULL PRIMARY KEY,
	index_name TEXT NOT NULL,
	shard INTEGER NOT NULL,
	kind TEXT NOT NULL,
	id TEXT NOT NULL,
	payload TEXT NOT NULL,
	at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS sl_changes_shard_seq ON sl_changes (index_name, shard, seq);

CREATE TABLE IF NOT EXISTS sl_counter (
	id INTEGER NOT NULL PRIMARY KEY,
	value INTEGER NOT NULL
);

INSERT INTO sl_counter (id, value) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS sl_pruned (
	index_name TEXT NOT NULL,
	shard INTEGER NOT NULL,
	below_seq INTEGER NOT NULL,
	PRIMARY KEY (index_name, shard)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS sl_nodes (
	node_id TEXT NOT NULL PRIMARY KEY,
	address TEXT NOT NULL,
	version TEXT NOT NULL,
	capacity INTEGER NOT NULL,
	heartbeat_at INTEGER NOT NULL,
	started_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sl_shard_copies (
	index_name TEXT NOT NULL,
	shard INTEGER NOT NULL,
	slot INTEGER NOT NULL,
	node_id TEXT NOT NULL,
	state TEXT NOT NULL,
	applied_seq INTEGER NOT NULL,
	lease_until INTEGER NOT NULL,
	PRIMARY KEY (index_name, shard, slot)
) WITHOUT ROWID;

CREATE UNIQUE INDEX IF NOT EXISTS sl_shard_copies_node ON sl_shard_copies (index_name, shard, node_id);

CREATE INDEX IF NOT EXISTS sl_shard_copies_by_node ON sl_shard_copies (node_id);

CREATE TABLE IF NOT EXISTS sl_blobs (
	name TEXT NOT NULL PRIMARY KEY,
	upload_id TEXT NOT NULL,
	size INTEGER NOT NULL,
	chunks INTEGER NOT NULL,
	sha256 TEXT NOT NULL,
	created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sl_blob_chunks (
	upload_id TEXT NOT NULL,
	chunk INTEGER NOT NULL,
	data BLOB NOT NULL,
	PRIMARY KEY (upload_id, chunk)
) WITHOUT ROWID;
