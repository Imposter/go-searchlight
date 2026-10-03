-- Searchlight schema v1 (spec §8). Times are Unix milliseconds from the
-- database clock; JSON is stored as utf8mb4 text. Names and ids are
-- VARBINARY so they compare byte for byte (ids are 1-512 bytes) and fit
-- InnoDB's 3072-byte key limit.

CREATE TABLE IF NOT EXISTS sl_indexes (
	name VARBINARY(255) NOT NULL PRIMARY KEY,
	mapping LONGTEXT CHARACTER SET utf8mb4 NOT NULL,
	settings LONGTEXT CHARACTER SET utf8mb4 NOT NULL,
	version BIGINT NOT NULL,
	created_at BIGINT NOT NULL,
	-- mapping_version counts the mapping's changes: 1 at create, one more for
	-- every Update that changes it (which logs a mapping change to every shard).
	mapping_version BIGINT NOT NULL,
	-- uid identifies this incarnation of the index: fresh on every create, so
	-- a drop followed by a recreate under the same name gets a new one.
	uid VARBINARY(32) NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sl_documents (
	index_name VARBINARY(255) NOT NULL,
	shard INT NOT NULL,
	id VARBINARY(512) NOT NULL,
	body LONGTEXT CHARACTER SET utf8mb4 NOT NULL,
	seq BIGINT NOT NULL,
	PRIMARY KEY (index_name, shard, id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sl_queries (
	index_name VARBINARY(255) NOT NULL,
	shard INT NOT NULL,
	id VARBINARY(512) NOT NULL,
	query LONGTEXT CHARACTER SET utf8mb4 NOT NULL,
	meta LONGTEXT CHARACTER SET utf8mb4 NOT NULL,
	seq BIGINT NOT NULL,
	PRIMARY KEY (index_name, shard, id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sl_changes (
	seq BIGINT NOT NULL PRIMARY KEY,
	index_name VARBINARY(255) NOT NULL,
	shard INT NOT NULL,
	kind VARCHAR(16) NOT NULL,
	id VARBINARY(512) NOT NULL,
	payload LONGTEXT CHARACTER SET utf8mb4 NOT NULL,
	at BIGINT NOT NULL,
	-- mapping_version is the index's mapping version as of this change: the
	-- mapping it was written under, logged at a lower seq of the same shard.
	mapping_version BIGINT NOT NULL,
	-- index_uid is the index's incarnation (sl_indexes.uid) as of this
	-- change, so a tailer can tell a drop-and-recreate apart from a
	-- continuing index without an extra query per batch.
	index_uid VARBINARY(32) NOT NULL,
	KEY sl_changes_shard_seq (index_name, shard, seq)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sl_counter (
	id INT NOT NULL PRIMARY KEY,
	value BIGINT NOT NULL
) ENGINE=InnoDB;

-- Row 1 is the changelog sequence; row 2 issues shard-copy epochs (fencing
-- tokens), so claims never consume sequence numbers.
INSERT IGNORE INTO sl_counter (id, value) VALUES (1, 0), (2, 0);

CREATE TABLE IF NOT EXISTS sl_pruned (
	index_name VARBINARY(255) NOT NULL,
	shard INT NOT NULL,
	below_seq BIGINT NOT NULL,
	PRIMARY KEY (index_name, shard)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sl_nodes (
	node_id VARBINARY(255) NOT NULL PRIMARY KEY,
	address VARCHAR(1024) NOT NULL,
	version VARCHAR(255) NOT NULL,
	capacity INT NOT NULL,
	heartbeat_at BIGINT NOT NULL,
	started_at BIGINT NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sl_shard_copies (
	index_name VARBINARY(255) NOT NULL,
	shard INT NOT NULL,
	slot INT NOT NULL,
	node_id VARBINARY(255) NOT NULL,
	state VARCHAR(16) NOT NULL,
	applied_seq BIGINT NOT NULL,
	lease_until BIGINT NOT NULL,
	epoch BIGINT NOT NULL,
	PRIMARY KEY (index_name, shard, slot),
	UNIQUE KEY sl_shard_copies_node (index_name, shard, node_id),
	KEY sl_shard_copies_by_node (node_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sl_blobs (
	name VARBINARY(512) NOT NULL PRIMARY KEY,
	upload_id VARCHAR(64) NOT NULL,
	size BIGINT NOT NULL,
	chunks INT NOT NULL,
	sha256 VARCHAR(64) NOT NULL,
	created_at BIGINT NOT NULL,
	KEY sl_blobs_upload (upload_id)
) ENGINE=InnoDB;

-- Uploads in progress (touched as they write) and replaced or deleted uploads
-- awaiting removal (touched_at 0); BlobStore.Sweep clears stale ones.
CREATE TABLE IF NOT EXISTS sl_blob_uploads (
	upload_id VARCHAR(64) NOT NULL PRIMARY KEY,
	name VARBINARY(512) NOT NULL,
	touched_at BIGINT NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sl_blob_chunks (
	upload_id VARCHAR(64) NOT NULL,
	chunk INT NOT NULL,
	data LONGBLOB NOT NULL,
	PRIMARY KEY (upload_id, chunk)
) ENGINE=InnoDB;
