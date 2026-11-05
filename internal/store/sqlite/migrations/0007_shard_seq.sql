-- Searchlight schema v7: a sequence per shard. Each shard of each index takes its seqs
-- from its own row of sl_shard_seq, locked by the transactions that commit to it, so
-- commits to different shards no longer wait on one another. The row also holds the
-- index's incarnation (uid) and mapping version, so a commit reads them under its lock
-- rather than from sl_indexes.
--
-- The table starts empty, and commits keep taking their seqs from the global sequence
-- (sl_counter row 1) until the cluster turns shard sequences on: nodes from before
-- this schema may still be writing through it, and know nothing of these rows. The
-- switch, once every live node heartbeats sl_nodes.seq_scheme 1, seeds every shard's
-- row at row 1's value in one transaction under row 1's lock, moves the floor to row
-- 3 and deletes row 1 (internal/store/upgrade.go). A new database switches at once.
--
-- sl_changes is keyed by shard and seq, since seqs repeat across shards once they are
-- on. SQLite cannot change a table's key, so it is rebuilt, inside the migration's
-- transaction. It stays a rowid table: its rows carry document bodies, larger than
-- WITHOUT ROWID suits, and the key's index serves the reads by shard and seq. Unlike
-- the other dialects it keeps no index on seq alone: nodes of two versions never share
-- one SQLite file.

CREATE TABLE IF NOT EXISTS sl_shard_seq (
	index_name TEXT NOT NULL,
	shard INTEGER NOT NULL,
	-- value is the shard's newest seq.
	value INTEGER NOT NULL,
	index_uid TEXT NOT NULL,
	mapping_version INTEGER NOT NULL,
	PRIMARY KEY (index_name, shard)
) WITHOUT ROWID;

CREATE TABLE sl_changes_v7 (
	seq INTEGER NOT NULL,
	index_name TEXT NOT NULL,
	shard INTEGER NOT NULL,
	kind TEXT NOT NULL,
	id TEXT NOT NULL,
	payload TEXT NOT NULL,
	at INTEGER NOT NULL,
	mapping_version INTEGER NOT NULL,
	index_uid TEXT NOT NULL,
	payload_z BLOB,
	nonce INTEGER,
	PRIMARY KEY (index_name, shard, seq)
);

INSERT INTO sl_changes_v7 (seq, index_name, shard, kind, id, payload, at, mapping_version, index_uid, payload_z, nonce)
SELECT seq, index_name, shard, kind, id, payload, at, mapping_version, index_uid, payload_z, nonce FROM sl_changes;

DROP TABLE sl_changes;

ALTER TABLE sl_changes_v7 RENAME TO sl_changes;

ALTER TABLE sl_nodes ADD COLUMN seq_scheme INTEGER NOT NULL DEFAULT 0;
