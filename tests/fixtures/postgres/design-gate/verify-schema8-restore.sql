\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 8 THEN
        RAISE EXCEPTION 'restored metadata schema is not version 8';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM ob_repeat_execution repeat
        JOIN ob_step_execution step ON step.id = repeat.step_execution_id
        WHERE repeat.step_execution_id = 94001
          AND repeat.repeat_id = 'window'
          AND repeat.ordinal = 3
          AND repeat.definition_node_id = step.step_logical_id
          AND repeat.lineage = '[]'::jsonb
          AND repeat.decision = 'continue'
          AND octet_length(repeat.plan_fingerprint) = 32
    ) THEN
        RAISE EXCEPTION 'restored schema8 repeat lineage state changed';
    END IF;
    IF to_regclass('oxide_batch.ob_repeat_execution_lookup') IS NULL THEN
        RAISE EXCEPTION 'schema8 repeat execution lookup index is missing';
    END IF;
END
$verify$;
