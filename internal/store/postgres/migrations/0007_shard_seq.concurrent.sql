-- Searchlight schema v7, outside its transaction: the indexes the re-key needs, built
-- CONCURRENTLY, so writes and reads of sl_changes go on while they build (nodes from
-- before this schema keep committing; their seqs are unique, so the new key's index is
-- too). The migration then swaps the key onto the first and holds sl_changes' lock only
-- for the swap. A run interrupted part way leaves an index that may be invalid, so each
-- is dropped before it is built again.

DROP INDEX CONCURRENTLY IF EXISTS sl_changes_key_v7;

CREATE UNIQUE INDEX CONCURRENTLY sl_changes_key_v7 ON sl_changes (index_name, shard, seq);

DROP INDEX CONCURRENTLY IF EXISTS sl_changes_seq;

CREATE INDEX CONCURRENTLY sl_changes_seq ON sl_changes (seq);
