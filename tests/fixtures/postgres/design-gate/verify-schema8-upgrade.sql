\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 8 THEN
        RAISE EXCEPTION 'schema8 upgrade did not publish version 8';
    END IF;
    IF (SELECT status FROM ob_job_execution WHERE id = 93001) <> 'STOPPED'
       OR (SELECT status FROM ob_job_execution WHERE id = 93002) <> 'COMPLETED' THEN
        RAISE EXCEPTION 'schema8 upgrade changed prior executions';
    END IF;
    IF convert_from(
        (SELECT payload FROM ob_component_state WHERE id = 95001),
        'UTF8'
    ) <> '{"cursor":7}' THEN
        RAISE EXCEPTION 'schema8 upgrade changed prior component-state bytes';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM ob_repeat_execution
        WHERE step_execution_id = 94001
          AND repeat_id = 'window'
          AND definition_node_id = 'preserved_step'
          AND parent_lineage = '[]'::jsonb
          AND ordinal = 3
          AND decision = 'continue'
    ) THEN
        RAISE EXCEPTION 'schema8 upgrade failed to backfill root repeat lineage';
    END IF;
END
$verify$;

INSERT INTO ob_repeat_execution (
    step_execution_id, repeat_id, definition_node_id, parent_lineage, ordinal,
    state_format, state_schema, state_schema_version, state_payload,
    state_checksum, decision, plan_fingerprint, updated_at
) VALUES (
    94001, 'inner', 'preserved_step',
    '[{"repeat_id":"outer","ordinal":2}]'::jsonb, 1,
    1, 'repeat.state', 1, '{"cursor":"lineage-preserved"}'::jsonb,
    decode(repeat('77', 32), 'hex'), 'complete',
    decode(repeat('66', 32), 'hex'), '2026-09-01 00:00:06+00'
);

DO $verify$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM ob_repeat_execution
        WHERE step_execution_id = 94001
          AND repeat_id = 'inner'
          AND definition_node_id = 'preserved_step'
          AND parent_lineage = '[{"repeat_id":"outer","ordinal":2}]'::jsonb
          AND ordinal = 1
          AND decision = 'complete'
    ) THEN
        RAISE EXCEPTION 'schema8 nested repeat lineage fixture changed durable meaning';
    END IF;
END
$verify$;
