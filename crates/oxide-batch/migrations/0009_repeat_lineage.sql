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
    ADD COLUMN parent_lineage jsonb;

UPDATE ob_repeat_execution repeat
SET definition_node_id = step.step_logical_id,
    parent_lineage = '[]'::jsonb
FROM ob_step_execution step
WHERE step.id = repeat.step_execution_id;

ALTER TABLE ob_repeat_execution
    ALTER COLUMN definition_node_id SET NOT NULL,
    ALTER COLUMN parent_lineage SET NOT NULL,
    ADD CONSTRAINT ob_repeat_execution_definition_node_id_bound
        CHECK (octet_length(definition_node_id) BETWEEN 1 AND 128),
    ADD CONSTRAINT ob_repeat_execution_parent_lineage_array
        CHECK (jsonb_typeof(parent_lineage) = 'array'),
    ADD CONSTRAINT ob_repeat_execution_parent_lineage_depth
        CHECK (jsonb_array_length(parent_lineage) <= 8),
    ADD CONSTRAINT ob_repeat_execution_parent_lineage_size
        CHECK (pg_column_size(parent_lineage) <= 4096);

UPDATE ob_schema_version
    SET version = 8, installed_at = CURRENT_TIMESTAMP
    WHERE singleton;
