-- Searchlight schema v4: sl_nodes.mapping_types, the field types a node reads, as a
-- level each node heartbeats (store.MappingTypes): 1 reads point fields. A node from
-- before it leaves the column at 0, and a mapping that adds a point field is refused
-- while such a node is live, so no node is handed a mapping it cannot decode.
-- Adding a column changes only the schema; nothing is rewritten.

ALTER TABLE sl_nodes ADD COLUMN mapping_types INT NOT NULL DEFAULT 0, ALGORITHM=INSTANT;
