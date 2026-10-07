-- Searchlight schema v2: compressed document bodies and change payloads, added
-- beside the text columns so that nodes from before them keep working through a
-- rolling upgrade. Adding a column changes only the schema; nothing is rewritten.
--
-- body_z and payload_z hold a codec byte and the JSON's encoding (zstd;
-- internal/store/codec.go) when body or payload is empty. sl_nodes.body_codecs is
-- the codecs a node reads (0 before codecs), and a zstd_bodies row in sl_features,
-- which the leader writes once every node reads them, lets nodes write compressed
-- bodies.

ALTER TABLE sl_documents ADD COLUMN body_z BLOB;

ALTER TABLE sl_changes ADD COLUMN payload_z BLOB;

ALTER TABLE sl_nodes ADD COLUMN body_codecs INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS sl_features (
	name TEXT NOT NULL PRIMARY KEY,
	enabled_at INTEGER NOT NULL
);
