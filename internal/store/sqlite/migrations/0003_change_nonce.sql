-- Searchlight schema v3: a random nonce per change, so that a change a failover lost
-- and a change that took its seq afterwards never look the same to a copy checking
-- what the changelog holds (internal/replica/rewind.go), even when they name the same
-- document in the same millisecond. Adding a column changes only the schema; nothing
-- is rewritten. Changes written before it, and by nodes from before it (whose inserts
-- name no nonce), are NULL, and copies compare them as they did before.

ALTER TABLE sl_changes ADD COLUMN nonce INTEGER;
