-- Searchlight schema v5: client versions (Elasticsearch's external versioning). Each
-- document and saved query keeps the version its last write named (0 for none), and a
-- versioned delete keeps its id's version in sl_tombstones (kind 'd' for a document,
-- 'q' for a saved query), so a replay of an older write after it is a noop too. Adding
-- the columns and the table changes only the schema; nothing is rewritten. A node from
-- before it leaves versions as they are when it writes.

ALTER TABLE sl_documents ADD COLUMN version INTEGER NOT NULL DEFAULT 0;

ALTER TABLE sl_queries ADD COLUMN version INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS sl_tombstones (
	index_name TEXT NOT NULL,
	shard INTEGER NOT NULL,
	kind TEXT NOT NULL,
	id TEXT NOT NULL,
	version INTEGER NOT NULL,
	-- at is when the delete committed, by the database clock (Unix ms): a tombstone
	-- older than its index's gc_deletes is pruned.
	at INTEGER NOT NULL,
	PRIMARY KEY (index_name, shard, kind, id)
) WITHOUT ROWID;
