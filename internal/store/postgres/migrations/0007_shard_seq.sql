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
-- on; that key serves what sl_changes_shard_seq served. The index on seq alone serves
-- nodes from before this schema, which find a change by its seq, until every node runs
-- this one. The key swap holds sl_changes' lock while it builds.

CREATE TABLE IF NOT EXISTS sl_shard_seq (
	index_name TEXT COLLATE "C" NOT NULL,
	shard INTEGER NOT NULL,
	-- value is the shard's newest seq.
	value BIGINT NOT NULL,
	index_uid TEXT NOT NULL,
	mapping_version BIGINT NOT NULL,
	PRIMARY KEY (index_name, shard)
);

ALTER TABLE sl_changes DROP CONSTRAINT sl_changes_pkey, ADD PRIMARY KEY (index_name, shard, seq);

DROP INDEX IF EXISTS sl_changes_shard_seq;

CREATE INDEX IF NOT EXISTS sl_changes_seq ON sl_changes (seq);

ALTER TABLE sl_nodes ADD COLUMN IF NOT EXISTS seq_scheme INTEGER NOT NULL DEFAULT 0;
