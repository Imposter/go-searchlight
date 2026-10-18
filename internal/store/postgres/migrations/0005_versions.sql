-- Searchlight schema v5: client versions (Elasticsearch's external versioning). Each
-- document and saved query keeps the version its last write named (0 for none), and a
-- versioned delete keeps its id's version in sl_tombstones (kind 'd' for a document,
-- 'q' for a saved query), so a replay of an older write after it is a noop too. A
-- column with a constant default is a catalogue change that takes milliseconds
-- whatever the table holds; nothing is rewritten. A node from before it leaves
-- versions as they are when it writes.

ALTER TABLE sl_documents ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;

ALTER TABLE sl_queries ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS sl_tombstones (
	index_name TEXT COLLATE "C" NOT NULL,
	shard INTEGER NOT NULL,
	kind TEXT NOT NULL,
	id TEXT COLLATE "C" NOT NULL,
	version BIGINT NOT NULL,
	PRIMARY KEY (index_name, shard, kind, id)
);
