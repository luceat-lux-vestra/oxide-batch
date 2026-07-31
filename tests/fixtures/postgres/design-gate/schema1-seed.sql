\set ON_ERROR_STOP on

-- Realistic schema-1 history for the schema-2 upgrade fixture. It covers a
-- completed instance, a failed instance with an active restart attempt, a
-- stopped instance, and an unresolved UNKNOWN instance, because each of those
-- dispositions is affected differently by the migration.

SET search_path TO oxide_batch, pg_catalog;

INSERT INTO ob_job_definition (
    job_name, definition_revision, manifest_format, manifest_digest, manifest,
    registered_at
) VALUES
    ('upgrade_import', 'v1', 1, decode(repeat('a1', 32), 'hex'),
     '{"format":1,"steps":["import"]}'::jsonb, CURRENT_TIMESTAMP);

INSERT INTO ob_job_instance (job_name, instance_key, identifying_parameters, created_at)
SELECT
    'upgrade_import',
    decode(repeat(marker, 32), 'hex'),
    format('{"business_date":{"type":"string","identifying":true,"value":"2026-07-%s"}}', marker)::jsonb,
    CURRENT_TIMESTAMP
FROM (VALUES ('11'), ('22'), ('33'), ('44')) AS keys(marker);

-- One terminal or unresolved attempt per instance, plus one restart attempt on
-- the failed instance. The schema-1 partial unique index still permits at most
-- one unresolved attempt per instance.
INSERT INTO ob_job_execution (
    job_instance_id, definition_id, restart_of_execution_id, attempt, status,
    exit_code, parameters, context_format, context_schema,
    context_schema_version, context_payload, created_at, started_at, ended_at,
    updated_at
)
SELECT
    instance.id,
    definition.id,
    NULL,
    1,
    plan.status,
    plan.exit_code,
    '{}'::jsonb,
    1,
    'fixture.job',
    1,
    '{}'::jsonb,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    CASE WHEN plan.terminal THEN CURRENT_TIMESTAMP ELSE NULL END,
    CURRENT_TIMESTAMP
FROM ob_job_instance AS instance
JOIN ob_job_definition AS definition ON definition.job_name = instance.job_name
JOIN (VALUES
    (decode(repeat('11', 32), 'hex'), 'COMPLETED', 'COMPLETED', true),
    (decode(repeat('22', 32), 'hex'), 'FAILED', 'FAILED', true),
    (decode(repeat('33', 32), 'hex'), 'STOPPED', 'STOPPED', true),
    (decode(repeat('44', 32), 'hex'), 'UNKNOWN', 'UNKNOWN', false)
) AS plan(instance_key, status, exit_code, terminal)
    ON plan.instance_key = instance.instance_key;

INSERT INTO ob_job_execution (
    job_instance_id, definition_id, restart_of_execution_id, attempt, status,
    exit_code, parameters, context_format, context_schema,
    context_schema_version, context_payload, created_at, started_at, updated_at
)
SELECT
    failed.job_instance_id,
    failed.definition_id,
    failed.id,
    2,
    'STARTED',
    'EXECUTING',
    '{}'::jsonb,
    1,
    'fixture.job',
    1,
    '{}'::jsonb,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM ob_job_execution AS failed
JOIN ob_job_instance AS instance ON instance.id = failed.job_instance_id
WHERE instance.instance_key = decode(repeat('22', 32), 'hex');

INSERT INTO ob_step_execution (
    job_execution_id, step_name, status, exit_code, read_count,
    processed_count, write_count, filter_count, commit_count, rollback_count,
    checkpoint_format, checkpoint_schema, checkpoint_schema_version,
    checkpoint_payload, context_format, context_schema,
    context_schema_version, context_payload, created_at, started_at, ended_at,
    updated_at, version
)
SELECT
    execution.id,
    'import',
    execution.status,
    execution.exit_code,
    4, 4, 4, 0, 2, 0,
    1, 'fixture.cursor', 1, '{"cursor":4}'::jsonb,
    1, 'fixture.step', 1, '{}'::jsonb,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    execution.ended_at,
    CURRENT_TIMESTAMP,
    2
FROM ob_job_execution AS execution;
