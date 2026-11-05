-- Searchlight schema v3: a random nonce per change, so that a change a failover lost
-- and a change that took its seq afterwards never look the same to a copy checking
-- what the changelog holds (internal/replica/rewind.go), even when they name the same
-- document in the same millisecond. The column is added in place
-- (ALGORITHM=INSTANT: a metadata change, whatever the table holds); nothing is
-- rewritten. Changes written before it, and by nodes from before it (whose inserts
-- name no nonce), are NULL, and copies compare them as they did before.

ALTER TABLE sl_changes ADD COLUMN nonce BIGINT NULL, ALGORITHM=INSTANT;
