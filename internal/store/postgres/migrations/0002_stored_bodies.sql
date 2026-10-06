-- Searchlight schema v2: a document's body and a change's payload are stored as
-- bytes, either the JSON itself or a codec byte followed by its encoding (zstd;
-- internal/store/codec.go). Rows written before keep their JSON, which still reads
-- as it is. A body is compressed before it gets here, so Postgres keeps it out of
-- line when large without trying to compress it again.

ALTER TABLE sl_documents ALTER COLUMN body TYPE BYTEA USING convert_to(body, 'UTF8');

ALTER TABLE sl_documents ALTER COLUMN body SET STORAGE EXTERNAL;

ALTER TABLE sl_changes ALTER COLUMN payload TYPE BYTEA USING convert_to(payload, 'UTF8');

ALTER TABLE sl_changes ALTER COLUMN payload SET STORAGE EXTERNAL;
