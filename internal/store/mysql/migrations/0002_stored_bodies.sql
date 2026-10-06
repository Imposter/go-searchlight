-- Searchlight schema v2: a document's body and a change's payload are stored as
-- bytes, either the JSON itself or a codec byte followed by its encoding (zstd;
-- internal/store/codec.go). Rows written before keep their JSON bytes, which still
-- read as they are.

ALTER TABLE sl_documents MODIFY body LONGBLOB NOT NULL;

ALTER TABLE sl_changes MODIFY payload LONGBLOB NOT NULL;
