SET LOCAL search_path TO oxide_batch, pg_catalog;

DO $$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 7 THEN
        RAISE EXCEPTION 'oxide_batch schema version 7 is required before schema 8';
    END IF;
END
$$;

ALTER TABLE ob_repeat_execution
    ADD COLUMN definition_node_id varchar(128) COLLATE "C",
    ADD COLUMN lineage jsonb;

UPDATE ob_repeat_execution repeat
SET definition_node_id = step.step_logical_id,
    lineage = '[]'::jsonb
FROM ob_step_execution step
WHERE step.id = repeat.step_execution_id;

ALTER TABLE ob_repeat_execution
    ALTER COLUMN definition_node_id SET NOT NULL,
    ALTER COLUMN lineage SET NOT NULL,
    ADD CONSTRAINT ob_repeat_execution_definition_node_id_bounds
        CHECK (octet_length(definition_node_id) BETWEEN 1 AND 128),
    ADD CONSTRAINT ob_repeat_execution_lineage_shape
        CHECK (jsonb_typeof(lineage) = 'array'),
    ADD CONSTRAINT ob_repeat_execution_lineage_bounds
        CHECK (pg_column_size(lineage) <= 8192);

DROP INDEX ob_repeat_execution_lookup;
CREATE INDEX ob_repeat_execution_lookup
    ON ob_repeat_execution (repeat_id, definition_node_id, step_execution_id);

UPDATE ob_schema_version
SET version = 8, installed_at = CURRENT_TIMESTAMP
WHERE singleton;
