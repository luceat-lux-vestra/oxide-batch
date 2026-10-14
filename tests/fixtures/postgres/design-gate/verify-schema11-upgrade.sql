\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 11 THEN
        RAISE EXCEPTION 'schema11 upgrade did not publish version 11';
    END IF;
    IF (SELECT count(*) FROM ob_job_fork_lineage) <> 0 THEN
        RAISE EXCEPTION 'schema11 fabricated fork lineage for prior executions';
    END IF;
    IF (SELECT status FROM ob_job_execution WHERE id = 93001) <> 'STOPPED'
       OR (SELECT version FROM ob_job_execution WHERE id = 93001) <> 2
       OR (SELECT context_payload FROM ob_job_execution WHERE id = 93001)
          <> '{"parent":"preserved"}'::jsonb
    THEN
        RAISE EXCEPTION 'schema11 changed historical source execution identity/state';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM ob_definition_upgrade
        WHERE from_definition_id = 91001 AND to_definition_id = 91002
          AND upgrade_key = 'legacy-v9-edge'
          AND upgrade_revision IS NULL AND node_mapping IS NULL AND state_transforms IS NULL
    ) OR NOT EXISTS (
        SELECT 1 FROM ob_definition_upgrade
        WHERE from_definition_id = 91002 AND to_definition_id = 91001
          AND upgrade_key = 'transformed-v10-edge' AND upgrade_revision = 'edge-v1'
          AND jsonb_array_length(state_transforms) = 1
    ) THEN
        RAISE EXCEPTION 'schema11 changed historical compatibility edge identities';
    END IF;
END
$verify$;

-- Real FK-bound new instance and first execution, not an upgraded/restarted
-- attempt. No source context or counters carry forward.
INSERT INTO ob_job_instance
    (id, job_name, instance_key, identifying_parameters, created_at)
VALUES
    (92003, 'schema5_fixture_child', decode(repeat('33',32),'hex'),
     '{}'::jsonb, '2026-10-14 00:00:00+00');

INSERT INTO ob_job_execution (
    id, job_instance_id, definition_id, upgrade_from_definition_id,
    restart_of_execution_id, attempt, status, exit_code, parameters,
    context_format, context_schema, context_schema_version, context_payload,
    created_at, updated_at, version
) VALUES (
    93003, 92003, 91002, NULL,
    NULL, 1, 'STARTING', 'UNKNOWN', '{}'::jsonb,
    1, 'oxide_batch.empty.v1', 1, '{}'::jsonb,
    '2026-10-14 00:00:01+00', '2026-10-14 00:00:01+00', 0
);

INSERT INTO ob_job_fork_lineage (
    operation_id, source_job_execution_id, source_execution_version,
    source_definition_digest, source_state_digest, target_definition_id,
    target_job_instance_id, target_job_execution_id, target_parameters_digest
) VALUES (
    'schema11-fixture-fork', 93001, 2,
    decode(repeat('11',32),'hex'), decode(repeat('aa',32),'hex'), 91002,
    92003, 93003, decode(repeat('bb',32),'hex')
);

DO $verify$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM ob_job_fork_lineage fork
        JOIN ob_job_execution target ON target.id = fork.target_job_execution_id
        JOIN ob_job_execution source ON source.id = fork.source_job_execution_id
        JOIN ob_job_definition definition ON definition.id = fork.target_definition_id
        WHERE fork.operation_id = 'schema11-fixture-fork'
          AND fork.source_execution_version = 2
          AND fork.source_definition_digest = decode(repeat('11',32),'hex')
          AND fork.target_parameters_digest = decode(repeat('bb',32),'hex')
          AND target.job_instance_id = 92003
          AND target.definition_id = 91002
          AND target.attempt = 1
          AND target.status = 'STARTING'
          AND target.restart_of_execution_id IS NULL
          AND target.upgrade_from_definition_id IS NULL
          AND target.context_payload = '{}'::jsonb
          AND source.id = 93001 AND source.status = 'STOPPED'
          AND definition.artifact_identity = 'fixture-artifact-v1'
    ) THEN
        RAISE EXCEPTION 'schema11 fork lineage/identity fixture is inconsistent';
    END IF;
END
$verify$;

-- The migration enforces insert-only lineage even when a role accidentally
-- has broad default UPDATE/DELETE privileges.
DO $verify$
BEGIN
    BEGIN
        UPDATE ob_job_fork_lineage SET source_execution_version = 3
        WHERE operation_id = 'schema11-fixture-fork';
        RAISE EXCEPTION 'schema11 lineage UPDATE unexpectedly succeeded';
    EXCEPTION WHEN integrity_constraint_violation THEN NULL;
    END;
    BEGIN
        DELETE FROM ob_job_fork_lineage WHERE operation_id = 'schema11-fixture-fork';
        RAISE EXCEPTION 'schema11 lineage DELETE unexpectedly succeeded';
    EXCEPTION WHEN integrity_constraint_violation THEN NULL;
    END;
END
$verify$;

-- One separately reviewed transformed fork: the new target carries ONLY
-- the explicitly transformed job-level context. Its source history is intact.
INSERT INTO ob_job_instance
    (id, job_name, instance_key, identifying_parameters, created_at)
VALUES
    (92004, 'schema5_fixture_child', decode(repeat('34',32),'hex'),
     '{}'::jsonb, '2026-10-14 00:00:05+00');

INSERT INTO ob_job_execution (
    id, job_instance_id, definition_id, upgrade_from_definition_id,
    restart_of_execution_id, attempt, status, exit_code, parameters,
    context_format, context_schema, context_schema_version, context_payload,
    created_at, updated_at, version
) VALUES (
    93004, 92004, 91002, NULL,
    NULL, 1, 'STARTING', 'UNKNOWN', '{}'::jsonb,
    1, 'fixture-context-v2', 2, '{"value":8}'::jsonb,
    '2026-10-14 00:00:06+00', '2026-10-14 00:00:06+00', 0
);

INSERT INTO ob_job_fork_lineage (
    operation_id, source_job_execution_id, source_execution_version,
    source_definition_digest, source_state_digest, target_definition_id,
    target_job_instance_id, target_job_execution_id, target_parameters_digest,
    fork_transform
) VALUES (
    'schema11-fixture-transformed', 93001, 2,
    decode(repeat('11',32),'hex'), decode(repeat('cc',32),'hex'), 91002,
    92004, 93004, decode(repeat('dd',32),'hex'),
    '{
        "source_step_execution_id": 94001,
        "source_step_version": 2,
        "source_node": "preserved_step",
        "target_node": "preserved_step",
        "kind": "execution-context",
        "state_id": "fixture-context",
        "source_schema": "fixture-context",
        "source_version": 1,
        "target_schema": "fixture-context-v2",
        "target_version": 2,
        "revision": "fixture-transform-v1"
    }'::jsonb
);

DO $verify$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM ob_job_fork_lineage fork
        JOIN ob_job_execution target ON target.id = fork.target_job_execution_id
        WHERE fork.operation_id = 'schema11-fixture-transformed'
          AND fork.fork_transform ->> 'revision' = 'fixture-transform-v1'
          AND fork.fork_transform ->> 'kind' = 'execution-context'
          AND (fork.fork_transform ->> 'source_step_execution_id')::bigint = 94001
          AND (fork.fork_transform ->> 'source_step_version')::bigint = 2
          AND target.context_schema = 'fixture-context-v2'
          AND target.context_schema_version = 2
          AND target.context_payload = '{"value":8}'::jsonb
          AND target.attempt = 1
          AND target.restart_of_execution_id IS NULL
          AND target.upgrade_from_definition_id IS NULL
    ) THEN
        RAISE EXCEPTION 'schema11 transformed fork descriptor or context is inconsistent';
    END IF;
END
$verify$;
