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
-- The key swap holds sl_changes' lock while it builds.

CREATE TABLE IF NOT EXISTS sl_shard_seq (
	index_name TEXT COLLATE "C" NOT NULL,
	shard INTEGER NOT NULL,
	-- value is the shard's newest seq.
	value BIGINT NOT NULL,
	index_uid TEXT NOT NULL,
	mapping_version BIGINT NOT NULL,
	PRIMARY KEY (index_name, shard)
);

INSERT INTO sl_shard_seq (index_name, shard, value, index_uid, mapping_version)
SELECT i.name, g.shard, c.value, i.uid, i.mapping_version
FROM sl_indexes i
CROSS JOIN sl_counter c
CROSS JOIN LATERAL generate_series(0, COALESCE((i.settings::jsonb ->> 'shards')::int, 1) - 1) AS g (shard)
WHERE c.id = 1
ON CONFLICT (index_name, shard) DO NOTHING;

ALTER TABLE sl_changes DROP CONSTRAINT sl_changes_pkey, ADD PRIMARY KEY (index_name, shard, seq);

DROP INDEX IF EXISTS sl_changes_shard_seq;

CREATE INDEX IF NOT EXISTS sl_changes_seq ON sl_changes (seq);
