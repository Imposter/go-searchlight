-- Searchlight schema v7: a sequence per shard. Each shard of each index takes its seqs
-- from its own row of sl_shard_seq, so the seqs of different shards are independent, as
-- on the other dialects. The row also holds the index's incarnation (uid) and mapping
-- version, so a commit reads them with its seqs rather than from sl_indexes.
--
-- Every shard starts at sl_counter row 1's value, the newest seq of the old global
-- sequence, so the seqs already stored (documents', queries', if_seq conditions) stay
-- below every new one. Row 1 stays as the floor every new shard starts at.
--
-- sl_changes is keyed by shard and seq, since seqs repeat across shards now. SQLite
-- cannot change a table's key, so it is rebuilt, inside the migration's transaction.
-- The index on seq alone serves nodes from before this schema, which find a change by
-- its seq, until every node runs this one.

CREATE TABLE IF NOT EXISTS sl_shard_seq (
	index_name TEXT NOT NULL,
	shard INTEGER NOT NULL,
	-- value is the shard's newest seq.
	value INTEGER NOT NULL,
	index_uid TEXT NOT NULL,
	mapping_version INTEGER NOT NULL,
	PRIMARY KEY (index_name, shard)
) WITHOUT ROWID;

WITH RECURSIVE g (shard) AS (SELECT 0 UNION ALL SELECT shard + 1 FROM g WHERE shard < 1023)
INSERT OR IGNORE INTO sl_shard_seq (index_name, shard, value, index_uid, mapping_version)
SELECT i.name, g.shard, c.value, i.uid, i.mapping_version
FROM sl_indexes i JOIN g JOIN sl_counter c ON c.id = 1
WHERE g.shard < COALESCE(CAST(json_extract(i.settings, '$.shards') AS INTEGER), 1);

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

CREATE INDEX sl_changes_seq ON sl_changes (seq);
