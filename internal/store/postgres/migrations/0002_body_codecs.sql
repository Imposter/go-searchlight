-- Searchlight schema v2: compressed document bodies and change payloads, added
-- beside the text columns so that nodes from before them keep working through a
-- rolling upgrade. Every statement is a catalogue change that takes milliseconds
-- whatever the tables hold; nothing is rewritten.
--
-- body_z and payload_z hold a codec byte and the JSON's encoding (zstd;
-- internal/store/codec.go) when body or payload is empty. Already compressed, they
-- are kept out of line when large without Postgres compressing them again.
-- sl_nodes.body_codecs is the codecs a node reads (0 before codecs), and a
-- zstd_bodies row in sl_features, which the leader writes once every node reads
-- them, lets nodes write compressed bodies.

ALTER TABLE sl_documents ADD COLUMN IF NOT EXISTS body_z BYTEA;

ALTER TABLE sl_documents ALTER COLUMN body_z SET STORAGE EXTERNAL;

ALTER TABLE sl_changes ADD COLUMN IF NOT EXISTS payload_z BYTEA;

ALTER TABLE sl_changes ALTER COLUMN payload_z SET STORAGE EXTERNAL;

ALTER TABLE sl_nodes ADD COLUMN IF NOT EXISTS body_codecs INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS sl_features (
	name TEXT COLLATE "C" NOT NULL PRIMARY KEY,
	enabled_at BIGINT NOT NULL
);
