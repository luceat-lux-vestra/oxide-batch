\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 7 THEN
        RAISE EXCEPTION 'schema7 upgrade did not publish version 7';
    END IF;
    IF (SELECT status FROM ob_job_execution WHERE id = 93001) <> 'STOPPED'
       OR (SELECT status FROM ob_job_execution WHERE id = 93002) <> 'COMPLETED' THEN
        RAISE EXCEPTION 'schema7 upgrade changed prior executions';
    END IF;
    IF convert_from(
        (SELECT payload FROM ob_component_state WHERE id = 95001),
        'UTF8'
    ) <> '{"cursor":7}' THEN
        RAISE EXCEPTION 'schema7 upgrade changed prior component-state bytes';
    END IF;
    IF (SELECT count(*) FROM ob_nested_job_link WHERE id = 96001) <> 1 THEN
        RAISE EXCEPTION 'schema7 upgrade changed prior nested-job linkage';
    END IF;
    IF (SELECT count(*) FROM ob_scope_resolution_provenance) <> 5 THEN
        RAISE EXCEPTION 'schema7 upgrade changed prior scope-resolution provenance';
    END IF;
    IF to_regclass('oxide_batch.ob_repeat_execution') IS NULL THEN
        RAISE EXCEPTION 'schema7 repeat execution table is missing';
    END IF;
    IF to_regclass('oxide_batch.ob_repeat_execution_lookup') IS NULL THEN
        RAISE EXCEPTION 'schema7 repeat execution lookup index is missing';
    END IF;
    IF (SELECT count(*) FROM ob_repeat_execution) <> 0 THEN
        RAISE EXCEPTION 'schema7 migration invented repeat state for prior executions';
    END IF;
END
$verify$;

INSERT INTO ob_repeat_execution (
    step_execution_id, repeat_id, ordinal, state_format, state_schema,
    state_schema_version, state_payload, state_checksum, decision,
    plan_fingerprint, updated_at
) VALUES (
    94001, 'window', 3, 1, 'repeat.state',
    1, '{"cursor":"repeat-preserved"}'::jsonb,
    decode(repeat('55', 32), 'hex'), 'continue',
    decode(repeat('66', 32), 'hex'), '2026-09-01 00:00:05+00'
);

DO $verify$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM ob_repeat_execution
        WHERE step_execution_id = 94001
          AND repeat_id = 'window'
          AND ordinal = 3
          AND state_format = 1
          AND state_schema = 'repeat.state'
          AND state_schema_version = 1
          AND state_payload = '{"cursor":"repeat-preserved"}'::jsonb
          AND decision = 'continue'
    ) THEN
        RAISE EXCEPTION 'schema7 repeat fixture changed durable meaning';
    END IF;
    IF octet_length(
        (SELECT state_checksum FROM ob_repeat_execution
         WHERE step_execution_id = 94001 AND repeat_id = 'window')
    ) <> 32
       OR octet_length(
        (SELECT plan_fingerprint FROM ob_repeat_execution
         WHERE step_execution_id = 94001 AND repeat_id = 'window')
    ) <> 32 THEN
        RAISE EXCEPTION 'schema7 repeat fixture changed digest width';
    END IF;
END
$verify$;
