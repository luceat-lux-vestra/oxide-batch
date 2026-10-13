\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 9 THEN
        RAISE EXCEPTION 'schema9 upgrade did not publish version 9';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM ob_job_definition
        WHERE id = 91001
          AND job_name = 'schema5_fixture_parent'
          AND definition_revision = 'fixture-v1'
          AND manifest_format = 4
          AND manifest_digest = decode(repeat('11', 32), 'hex')
          AND manifest = '{"format":4,"fixture":"schema4-parent"}'::jsonb
          AND artifact_identity IS NULL
    ) THEN
        RAISE EXCEPTION 'schema9 changed or rebound the pre-registry parent definition';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM ob_job_definition
        WHERE id = 91002
          AND job_name = 'schema5_fixture_child'
          AND definition_revision = 'fixture-v1'
          AND manifest_format = 1
          AND manifest_digest = decode(repeat('22', 32), 'hex')
          AND manifest = '{"format":1,"fixture":"schema4-child"}'::jsonb
          AND artifact_identity IS NULL
    ) THEN
        RAISE EXCEPTION 'schema9 changed or rebound the pre-registry child definition';
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
        RAISE EXCEPTION 'schema9 changed existing schema8 repeat lineage state';
    END IF;
END
$verify$;

UPDATE ob_job_definition
SET artifact_identity = 'fixture-artifact-v1'
WHERE id = 91002;

DO $verify$
BEGIN
    IF (SELECT artifact_identity FROM ob_job_definition WHERE id = 91002)
        <> 'fixture-artifact-v1'
    THEN
        RAISE EXCEPTION 'schema9 artifact identity could not be persisted';
    END IF;
END
$verify$;
