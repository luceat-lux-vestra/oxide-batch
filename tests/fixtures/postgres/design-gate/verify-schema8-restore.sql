\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 8 THEN
        RAISE EXCEPTION 'restored metadata schema is not version 8';
    END IF;
    IF (SELECT status FROM ob_job_execution WHERE id = 93001) <> 'STOPPED'
       OR (SELECT status FROM ob_job_execution WHERE id = 93002) <> 'COMPLETED' THEN
        RAISE EXCEPTION 'restored schema8 executions changed';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM ob_repeat_execution
        WHERE step_execution_id = 94001
          AND repeat_id = 'window'
          AND definition_node_id = 'preserved_step'
          AND parent_lineage = '[]'::jsonb
          AND ordinal = 3
          AND decision = 'continue'
    ) THEN
        RAISE EXCEPTION 'restored schema8 root repeat state changed';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM ob_repeat_execution
        WHERE step_execution_id = 94001
          AND repeat_id = 'inner'
          AND definition_node_id = 'preserved_step'
          AND parent_lineage = '[{"repeat_id":"outer","ordinal":2}]'::jsonb
          AND ordinal = 1
          AND decision = 'complete'
    ) THEN
        RAISE EXCEPTION 'restored schema8 nested repeat lineage changed';
    END IF;
END
$verify$;
