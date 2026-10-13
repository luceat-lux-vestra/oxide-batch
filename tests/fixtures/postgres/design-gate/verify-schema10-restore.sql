\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 10 THEN
        RAISE EXCEPTION 'restored metadata schema is not version 10';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM ob_definition_upgrade
        WHERE from_definition_id = 91001
          AND to_definition_id = 91002
          AND upgrade_key = 'legacy-v9-edge'
          AND upgrade_revision IS NULL
          AND node_mapping IS NULL
          AND state_transforms IS NULL
    ) THEN
        RAISE EXCEPTION 'restored schema10 changed the legacy compatibility edge';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM ob_definition_upgrade
        WHERE from_definition_id = 91002
          AND to_definition_id = 91001
          AND upgrade_key = 'transformed-v10-edge'
          AND upgrade_revision = 'edge-v1'
          AND node_mapping = '{"preserved_step":"preserved_step"}'::jsonb
          AND jsonb_array_length(state_transforms) = 1
          AND state_transforms #>> '{0,revision}' = 'checkpoint-v1-to-v2'
    ) THEN
        RAISE EXCEPTION 'restored schema10 lost transformed upgrade metadata';
    END IF;
END
$verify$;
