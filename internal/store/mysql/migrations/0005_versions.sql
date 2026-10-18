-- Searchlight schema v5: client versions (Elasticsearch's external versioning). Each
-- document and saved query keeps the version its last write named (0 for none), and a
-- versioned delete keeps its id's version in sl_tombstones (kind 'd' for a document,
-- 'q' for a saved query), so a replay of an older write after it is a noop too. The
-- columns are added in place (ALGORITHM=INSTANT: a metadata change, whatever the table
-- holds); nothing is rewritten. A node from before it leaves versions as they are when
-- it writes.

ALTER TABLE sl_documents ADD COLUMN version BIGINT NOT NULL DEFAULT 0, ALGORITHM=INSTANT;

ALTER TABLE sl_queries ADD COLUMN version BIGINT NOT NULL DEFAULT 0, ALGORITHM=INSTANT;

CREATE TABLE IF NOT EXISTS sl_tombstones (
	index_name VARBINARY(255) NOT NULL,
	shard INT NOT NULL,
	kind VARBINARY(1) NOT NULL,
	id VARBINARY(512) NOT NULL,
	version BIGINT NOT NULL,
	PRIMARY KEY (index_name, shard, kind, id)
) ENGINE=InnoDB;
