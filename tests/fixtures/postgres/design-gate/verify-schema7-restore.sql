\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 7 THEN
        RAISE EXCEPTION 'restored metadata schema is not version 7';
    END IF;
    IF (SELECT status FROM ob_job_execution WHERE id = 93001) <> 'STOPPED'
       OR (SELECT status FROM ob_job_execution WHERE id = 93002) <> 'COMPLETED' THEN
        RAISE EXCEPTION 'restored schema7 executions changed';
    END IF;
    IF convert_from(
        (SELECT payload FROM ob_component_state WHERE id = 95001),
        'UTF8'
    ) <> '{"cursor":7}' THEN
        RAISE EXCEPTION 'restored schema7 component-state bytes changed';
    END IF;
    IF (SELECT count(*) FROM ob_nested_job_link WHERE id = 96001) <> 1 THEN
        RAISE EXCEPTION 'restored schema7 nested-job linkage is missing';
    END IF;
    IF (SELECT count(*) FROM ob_scope_resolution_provenance) <> 5 THEN
        RAISE EXCEPTION 'restored schema7 scope-resolution provenance changed';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM ob_repeat_execution
        WHERE step_execution_id = 94001
          AND repeat_id = 'window'
          AND ordinal = 3
          AND state_format = 1
          AND state_schema = 'repeat.state'
          AND state_schema_version = 1
          AND state_payload = '{"cursor":"repeat-preserved"}'::jsonb
          AND octet_length(state_checksum) = 32
          AND decision = 'continue'
          AND octet_length(plan_fingerprint) = 32
    ) THEN
        RAISE EXCEPTION 'restored schema7 repeat state changed';
    END IF;
END
$verify$;
