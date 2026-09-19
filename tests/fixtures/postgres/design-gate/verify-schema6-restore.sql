\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 6 THEN
        RAISE EXCEPTION 'restored metadata schema is not version 6';
    END IF;
    IF (SELECT status FROM ob_job_execution WHERE id = 93001) <> 'STOPPED'
       OR (SELECT status FROM ob_job_execution WHERE id = 93002) <> 'COMPLETED' THEN
        RAISE EXCEPTION 'restored schema6 executions changed';
    END IF;
    IF convert_from(
        (SELECT payload FROM ob_component_state WHERE id = 95001),
        'UTF8'
    ) <> '{"cursor":7}' THEN
        RAISE EXCEPTION 'restored schema6 component-state bytes changed';
    END IF;
    IF (SELECT count(*) FROM ob_nested_job_link WHERE id = 96001) <> 1 THEN
        RAISE EXCEPTION 'restored schema6 nested-job linkage is missing';
    END IF;
    IF (SELECT count(*) FROM ob_scope_resolution_provenance) <> 5 THEN
        RAISE EXCEPTION 'restored schema6 provenance row count changed';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM ob_scope_resolution_provenance
        WHERE id = 97003
          AND scope_kind = 'step'
          AND component_id = 'reader'
          AND input_name = 'step_ctx'
          AND source_kind = 'step_context'
          AND source_job_execution_id IS NULL
          AND source_step_execution_id = 94001
          AND source_execution_version = 2
          AND source_schema = 'fixture-context'
          AND source_schema_version = 1
          AND source_path = '["step"]'::jsonb
    ) THEN
        RAISE EXCEPTION 'restored schema6 step-context provenance changed';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM ob_scope_resolution_provenance
        WHERE id = 97005
          AND owner_job_execution_id = 93002
          AND source_step_execution_id = 94001
          AND source_execution_version = 2
          AND source_path = '["step"]'::jsonb
    ) THEN
        RAISE EXCEPTION 'restored schema6 authoritative source reference changed';
    END IF;
    IF to_regclass('oxide_batch.ob_scope_resolution_job_input') IS NULL
       OR to_regclass('oxide_batch.ob_scope_resolution_step_input') IS NULL
       OR to_regclass('oxide_batch.ob_scope_resolution_source_job') IS NULL
       OR to_regclass('oxide_batch.ob_scope_resolution_source_step') IS NULL THEN
        RAISE EXCEPTION 'restored schema6 provenance indexes are missing';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'oxide_batch'
          AND table_name = 'ob_scope_resolution_provenance'
          AND column_name ~ '(value|digest|payload|checksum)'
    ) THEN
        RAISE EXCEPTION 'restored schema6 provenance exposes forbidden value material';
    END IF;
END
$verify$;
