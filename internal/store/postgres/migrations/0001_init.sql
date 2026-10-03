-- Searchlight schema v1 (spec §8). Times are Unix milliseconds from the
-- database clock; JSON is stored as text.

-- Key text columns are COLLATE "C" so equality and ordering are plain byte
-- comparison, as on the other dialects (MySQL's VARBINARY, SQLite's default
-- BINARY collation), regardless of the database's locale.

CREATE TABLE IF NOT EXISTS sl_indexes (
	name TEXT COLLATE "C" NOT NULL PRIMARY KEY,
	mapping TEXT NOT NULL,
	settings TEXT NOT NULL,
	version BIGINT NOT NULL,
	created_at BIGINT NOT NULL,
	-- uid identifies this incarnation of the index: fresh on every create, so
	-- a drop followed by a recreate under the same name gets a new one.
	uid TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sl_documents (
	index_name TEXT COLLATE "C" NOT NULL,
	shard INTEGER NOT NULL,
	id TEXT COLLATE "C" NOT NULL,
	body TEXT NOT NULL,
	seq BIGINT NOT NULL,
	PRIMARY KEY (index_name, shard, id)
);

CREATE TABLE IF NOT EXISTS sl_queries (
	index_name TEXT COLLATE "C" NOT NULL,
	shard INTEGER NOT NULL,
	id TEXT COLLATE "C" NOT NULL,
	query TEXT NOT NULL,
	meta TEXT NOT NULL,
	seq BIGINT NOT NULL,
	PRIMARY KEY (index_name, shard, id)
);

CREATE TABLE IF NOT EXISTS sl_changes (
	seq BIGINT NOT NULL PRIMARY KEY,
	index_name TEXT COLLATE "C" NOT NULL,
	shard INTEGER NOT NULL,
	kind TEXT NOT NULL,
	id TEXT COLLATE "C" NOT NULL,
	payload TEXT NOT NULL,
	at BIGINT NOT NULL,
	-- index_uid is the index's incarnation (sl_indexes.uid) as of this
	-- change, so a tailer can tell a drop-and-recreate apart from a
	-- continuing index without an extra query per batch.
	index_uid TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS sl_changes_shard_seq ON sl_changes (index_name, shard, seq);

CREATE TABLE IF NOT EXISTS sl_counter (
	id INTEGER NOT NULL PRIMARY KEY,
	value BIGINT NOT NULL
);

-- Row 1 is the changelog sequence; row 2 issues shard-copy epochs (fencing
-- tokens), so claims never consume sequence numbers.
INSERT INTO sl_counter (id, value) VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

INSERT INTO sl_counter (id, value) VALUES (2, 0) ON CONFLICT (id) DO NOTHING;

CREATE TABLE IF NOT EXISTS sl_pruned (
	index_name TEXT COLLATE "C" NOT NULL,
	shard INTEGER NOT NULL,
	below_seq BIGINT NOT NULL,
	PRIMARY KEY (index_name, shard)
);

CREATE TABLE IF NOT EXISTS sl_nodes (
	node_id TEXT COLLATE "C" NOT NULL PRIMARY KEY,
	address TEXT NOT NULL,
	version TEXT NOT NULL,
	capacity INTEGER NOT NULL,
	heartbeat_at BIGINT NOT NULL,
	started_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS sl_shard_copies (
	index_name TEXT COLLATE "C" NOT NULL,
	shard INTEGER NOT NULL,
	slot INTEGER NOT NULL,
	node_id TEXT COLLATE "C" NOT NULL,
	state TEXT NOT NULL,
	applied_seq BIGINT NOT NULL,
	lease_until BIGINT NOT NULL,
	epoch BIGINT NOT NULL,
	PRIMARY KEY (index_name, shard, slot)
);

CREATE UNIQUE INDEX IF NOT EXISTS sl_shard_copies_node ON sl_shard_copies (index_name, shard, node_id);

CREATE INDEX IF NOT EXISTS sl_shard_copies_by_node ON sl_shard_copies (node_id);

CREATE TABLE IF NOT EXISTS sl_blobs (
	name TEXT COLLATE "C" NOT NULL PRIMARY KEY,
	upload_id TEXT COLLATE "C" NOT NULL,
	size BIGINT NOT NULL,
	chunks INTEGER NOT NULL,
	sha256 TEXT NOT NULL,
	created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS sl_blobs_upload ON sl_blobs (upload_id);

-- Uploads in progress (touched as they write) and replaced or deleted uploads
-- awaiting removal (touched_at 0); BlobStore.Sweep clears stale ones.
CREATE TABLE IF NOT EXISTS sl_blob_uploads (
	upload_id TEXT COLLATE "C" NOT NULL PRIMARY KEY,
	name TEXT COLLATE "C" NOT NULL,
	touched_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS sl_blob_chunks (
	upload_id TEXT COLLATE "C" NOT NULL,
	chunk INTEGER NOT NULL,
	data BYTEA NOT NULL,
	PRIMARY KEY (upload_id, chunk)
);
