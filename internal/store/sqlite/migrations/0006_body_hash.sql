-- Searchlight schema v6: body hashes. Each document keeps a hash of its stored body
-- (body_hash) and the seq of the write that stored it (hash_seq), so an upsert of the
-- same bytes is found to be a noop with one read by id, before it is analyzed or
-- committed. A node from before it moves seq on without them, so a hash is trusted
-- only while hash_seq is the row's seq. Adding the columns changes only the schema;
-- nothing is rewritten, and existing rows hold no hash until they are written again.

ALTER TABLE sl_documents ADD COLUMN body_hash INTEGER;

ALTER TABLE sl_documents ADD COLUMN hash_seq INTEGER;
