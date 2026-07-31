\set ON_ERROR_STOP on

SET search_path TO oxide_batch, pg_catalog;

INSERT INTO ob_job_definition (
    job_name,
    definition_revision,
    manifest_format,
    manifest_digest,
    manifest,
    registered_at
) VALUES (
    'fixture_import',
    'v1',
    1,
    decode(repeat('11', 32), 'hex'),
    '{"format":1,"steps":["import"]}'::jsonb,
    CURRENT_TIMESTAMP
);

INSERT INTO ob_job_instance (
    job_name,
    instance_key,
    identifying_parameters,
    created_at
) VALUES (
    'fixture_import',
    decode(repeat('22', 32), 'hex'),
    '{"business_date":{"type":"string","identifying":true,"value":"2026-07-29"}}'::jsonb,
    CURRENT_TIMESTAMP
);

INSERT INTO ob_job_execution (
    job_instance_id,
    definition_id,
    attempt,
    status,
    exit_code,
    parameters,
    context_format,
    context_schema,
    context_schema_version,
    context_payload,
    created_at,
    started_at,
    updated_at
) SELECT
    instance.id,
    definition.id,
    1,
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
FROM ob_job_instance AS instance
JOIN ob_job_definition AS definition
    ON definition.job_name = instance.job_name
WHERE instance.job_name = 'fixture_import';

INSERT INTO ob_step_execution (
    job_execution_id,
    step_name,
    step_logical_id,
    status,
    exit_code,
    checkpoint_format,
    checkpoint_schema,
    checkpoint_schema_version,
    checkpoint_payload,
    context_format,
    context_schema,
    context_schema_version,
    context_payload,
    created_at,
    started_at,
    updated_at
) SELECT
    id,
    'import',
    'import',
    'STARTED',
    'EXECUTING',
    1,
    'fixture.cursor',
    1,
    '{"cursor":0}'::jsonb,
    1,
    'fixture.step',
    1,
    '{}'::jsonb,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
FROM ob_job_execution
WHERE attempt = 1;

-- Schema-2 runtime DML advances the checkpoint, every fault counter, and the
-- retained fault state in one compare-and-swap update. This fixture proves the
-- runtime role may write the new columns; the reservation and commit boundaries
-- that separate them are covered by the repository suite. The row is left at
-- version 1 because the design-gate spike updates it from there.
UPDATE ob_step_execution
SET checkpoint_payload = '{"cursor":2}'::jsonb,
    read_count = 2,
    processed_count = 2,
    write_count = 2,
    commit_count = 1,
    rollback_count = rollback_count + 1,
    read_retry_count = read_retry_count + 1,
    process_skip_count = process_skip_count + 1,
    no_rollback_count = no_rollback_count + 1,
    fault_state_payload = '{"checkpoint": "0000000000000000000000000000000000000000000000000000000000000000", "entries": []}'::jsonb,
    fault_state_checksum = decode(
        'a491114819e0d3bd8b7ca004dc0636f95b45e2fcb1a67ddb5726beaea12f9922',
        'hex'
    ),
    updated_at = CURRENT_TIMESTAMP,
    version = version + 1
WHERE step_name = 'import' AND version = 0;

INSERT INTO ob_flow_decision (
    job_execution_id,
    source_step_execution_id,
    sequence,
    source_node_id,
    observed_outcome,
    target_node_id,
    transition_kind,
    plan_fingerprint,
    input_digest,
    decided_at
) SELECT
    step.job_execution_id,
    step.id,
    1,
    'import',
    'COMPLETED',
    'archive',
    'STEP_EXIT',
    decode(repeat('33', 32), 'hex'),
    decode(repeat('44', 32), 'hex'),
    CURRENT_TIMESTAMP
FROM ob_step_execution AS step
WHERE step.step_logical_id = 'import';

SELECT version FROM ob_schema_version WHERE singleton = true;
SELECT status, attempt FROM ob_job_execution WHERE id > 0;
SELECT checkpoint_schema, checkpoint_schema_version, version
FROM ob_step_execution
WHERE step_name = 'import';
SELECT step_logical_id, process_skip_count, read_retry_count, no_rollback_count
FROM ob_step_execution
WHERE step_logical_id = 'import';
SELECT sequence, transition_kind, target_node_id FROM ob_flow_decision;
