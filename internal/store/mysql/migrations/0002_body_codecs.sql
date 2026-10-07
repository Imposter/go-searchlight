-- Searchlight schema v2: compressed document bodies and change payloads, added
-- beside the text columns so that nodes from before them keep working through a
-- rolling upgrade. Each column is added in place (ALGORITHM=INSTANT: a metadata
-- change, whatever the tables hold); nothing is rewritten.
--
-- body_z and payload_z hold a codec byte and the JSON's encoding (zstd;
-- internal/store/codec.go) when body or payload is empty. sl_nodes.body_codecs is
-- the codecs a node reads (0 before codecs), and a zstd_bodies row in sl_features,
-- which the leader writes once every node reads them, lets nodes write compressed
-- bodies.

ALTER TABLE sl_documents ADD COLUMN body_z LONGBLOB NULL, ALGORITHM=INSTANT;

ALTER TABLE sl_changes ADD COLUMN payload_z LONGBLOB NULL, ALGORITHM=INSTANT;

ALTER TABLE sl_nodes ADD COLUMN body_codecs INT NOT NULL DEFAULT 0, ALGORITHM=INSTANT;

CREATE TABLE IF NOT EXISTS sl_features (
	name VARBINARY(255) NOT NULL PRIMARY KEY,
	enabled_at BIGINT NOT NULL
) ENGINE=InnoDB;
