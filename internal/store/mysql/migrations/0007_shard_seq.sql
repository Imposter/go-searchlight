-- Searchlight schema v7: a sequence per shard. Each shard of each index takes its seqs
-- from its own row of sl_shard_seq, locked by the transactions that commit to it, so
-- commits to different shards no longer wait on one another. The row also holds the
-- index's incarnation (uid) and mapping version, so a commit reads them under its lock
-- rather than from sl_indexes.
--
-- Every shard starts at sl_counter row 1's value, the newest seq of the old global
-- sequence, so the seqs already stored (documents', queries', if_seq conditions) stay
-- below every new one. Row 1 stays as the floor every new shard starts at.
--
-- sl_changes is keyed by shard and seq, since seqs repeat across shards now; that key
-- serves what sl_changes_shard_seq served. The index on seq alone serves nodes from
-- before this schema, which find a change by its seq, until every node runs this one.
-- Changing the primary key rebuilds sl_changes in place; writes go on meanwhile, and
-- the statement is atomic, so a rerun that finds sl_changes_shard_seq gone (1091) finds
-- it done.

CREATE TABLE IF NOT EXISTS sl_shard_seq (
	index_name VARBINARY(255) NOT NULL,
	shard INT NOT NULL,
	-- value is the shard's newest seq.
	value BIGINT NOT NULL,
	index_uid VARBINARY(32) NOT NULL,
	mapping_version BIGINT NOT NULL,
	PRIMARY KEY (index_name, shard)
) ENGINE=InnoDB;

INSERT IGNORE INTO sl_shard_seq (index_name, shard, value, index_uid, mapping_version)
WITH RECURSIVE n (v) AS (SELECT 0 UNION ALL SELECT v + 1 FROM n WHERE v < 31)
SELECT i.name, a.v * 32 + b.v, c.value, i.uid, i.mapping_version
FROM sl_indexes i JOIN n a JOIN n b JOIN sl_counter c ON c.id = 1
WHERE a.v * 32 + b.v < COALESCE(CAST(JSON_UNQUOTE(JSON_EXTRACT(i.settings, '$.shards')) AS SIGNED), 1);

ALTER TABLE sl_changes DROP PRIMARY KEY, ADD PRIMARY KEY (index_name, shard, seq), DROP INDEX sl_changes_shard_seq, ADD INDEX sl_changes_seq (seq);
