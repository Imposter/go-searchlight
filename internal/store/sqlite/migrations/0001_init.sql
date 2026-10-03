-- Searchlight schema v1 (spec §8). Times are Unix milliseconds from the
-- database clock; JSON is stored as text.

CREATE TABLE IF NOT EXISTS sl_indexes (
	name TEXT NOT NULL PRIMARY KEY,
	mapping TEXT NOT NULL,
	settings TEXT NOT NULL,
	version INTEGER NOT NULL,
	created_at INTEGER NOT NULL,
	-- mapping_version counts the mapping's changes: 1 at create, one more for
	-- every Update that changes it (which logs a mapping change to every shard).
	mapping_version INTEGER NOT NULL,
	-- uid identifies this incarnation of the index: fresh on every create, so
	-- a drop followed by a recreate under the same name gets a new one.
	uid TEXT NOT NULL
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
	at INTEGER NOT NULL,
	-- mapping_version is the index's mapping version as of this change: the
	-- mapping it was written under, logged at a lower seq of the same shard.
	mapping_version INTEGER NOT NULL,
	-- index_uid is the index's incarnation (sl_indexes.uid) as of this
	-- change, so a tailer can tell a drop-and-recreate apart from a
	-- continuing index without an extra query per batch.
	index_uid TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS sl_changes_shard_seq ON sl_changes (index_name, shard, seq);

CREATE TABLE IF NOT EXISTS sl_counter (
	id INTEGER NOT NULL PRIMARY KEY,
	value INTEGER NOT NULL
);

-- Row 1 is the changelog sequence; row 2 issues shard-copy epochs (fencing
-- tokens), so claims never consume sequence numbers.
INSERT INTO sl_counter (id, value) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

INSERT INTO sl_counter (id, value) VALUES (2, 0) ON CONFLICT (id) DO NOTHING;

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
	epoch INTEGER NOT NULL,
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

CREATE INDEX IF NOT EXISTS sl_blobs_upload ON sl_blobs (upload_id);

-- Uploads in progress (touched as they write) and replaced or deleted uploads
-- awaiting removal (touched_at 0); BlobStore.Sweep clears stale ones.
CREATE TABLE IF NOT EXISTS sl_blob_uploads (
	upload_id TEXT NOT NULL PRIMARY KEY,
	name TEXT NOT NULL,
	touched_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sl_blob_chunks (
	upload_id TEXT NOT NULL,
	chunk INTEGER NOT NULL,
	data BLOB NOT NULL,
	PRIMARY KEY (upload_id, chunk)
) WITHOUT ROWID;
