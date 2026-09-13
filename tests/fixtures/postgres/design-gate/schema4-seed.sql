\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

INSERT INTO ob_job_definition (
    id, job_name, definition_revision, manifest_format, manifest_digest,
    manifest, registered_at
) VALUES
    (
        91001, 'schema5_fixture_parent', 'fixture-v1', 4,
        decode(repeat('11', 32), 'hex'),
        '{"format":4,"fixture":"schema4-parent"}'::jsonb,
        '2026-09-01 00:00:00+00'
    ),
    (
        91002, 'schema5_fixture_child', 'fixture-v1', 1,
        decode(repeat('22', 32), 'hex'),
        '{"format":1,"fixture":"schema4-child"}'::jsonb,
        '2026-09-01 00:00:00+00'
    );

INSERT INTO ob_job_instance (
    id, job_name, instance_key, identifying_parameters, created_at
) VALUES
    (
        92001, 'schema5_fixture_parent', decode(repeat('31', 32), 'hex'),
        '{"run":{"type":"STRING","value":"fixture"}}'::jsonb,
        '2026-09-01 00:00:01+00'
    ),
    (
        92002, 'schema5_fixture_child', decode(repeat('32', 32), 'hex'),
        '{"child_key":{"type":"STRING","value":"original"}}'::jsonb,
        '2026-09-01 00:00:01+00'
    );

INSERT INTO ob_job_execution (
    id, job_instance_id, definition_id, upgrade_from_definition_id,
    restart_of_execution_id, attempt, status, exit_code, parameters,
    context_format, context_schema, context_schema_version, context_payload,
    failure_category, failure_id, created_at, started_at, ended_at, updated_at,
    version
) VALUES
    (
        93001, 92001, 91001, NULL, NULL, 1, 'STOPPED', 'STOPPED',
        '{"run":{"role":"IDENTIFYING","type":"STRING","value":"fixture"}}'::jsonb,
        1, 'fixture-context', 1, '{"parent":"preserved"}'::jsonb,
        NULL, NULL,
        '2026-09-01 00:00:02+00', '2026-09-01 00:00:03+00',
        '2026-09-01 00:00:04+00', '2026-09-01 00:00:04+00', 2
    ),
    (
        93002, 92002, 91002, NULL, NULL, 1, 'COMPLETED', 'COMPLETED',
        '{"child_key":{"role":"IDENTIFYING","type":"STRING","value":"original"}}'::jsonb,
        1, 'fixture-context', 1, '{"child":"preserved"}'::jsonb,
        NULL, NULL,
        '2026-09-01 00:00:02+00', '2026-09-01 00:00:03+00',
        '2026-09-01 00:00:04+00', '2026-09-01 00:00:04+00', 2
    );

INSERT INTO ob_step_execution (
    id, job_execution_id, step_name, step_logical_id, status, exit_code,
    read_count, processed_count, write_count, filter_count, commit_count,
    rollback_count, checkpoint_format, checkpoint_schema,
    checkpoint_schema_version, checkpoint_payload, context_format,
    context_schema, context_schema_version, context_payload, failure_category,
    failure_id, created_at, started_at, ended_at, updated_at, version
) VALUES (
    94001, 93001, 'preserved_step', 'preserved_step', 'COMPLETED', 'COMPLETED',
    7, 7, 7, 0, 1, 0,
    1, 'fixture-checkpoint', 1, '{"offset":7}'::jsonb,
    1, 'fixture-context', 1, '{"step":"preserved"}'::jsonb,
    NULL, NULL,
    '2026-09-01 00:00:02+00', '2026-09-01 00:00:03+00',
    '2026-09-01 00:00:04+00', '2026-09-01 00:00:04+00', 2
);

INSERT INTO ob_component_state (
    id, step_execution_id, namespace, schema_id, schema_version, codec_id,
    codec_version, checksum_algorithm, checksum_algorithm_version, checksum,
    payload_kind, payload, external_content_id, external_encoded_len, version,
    updated_at
) VALUES (
    95001, 94001, 'fixture-reader', 'fixture-state', 1, 'fixture-json',
    1, 1, 1, decode(repeat('44', 32), 'hex'),
    'INLINE', convert_to('{"cursor":7}', 'UTF8'), NULL, NULL, 3,
    '2026-09-01 00:00:04+00'
);

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 4 THEN
        RAISE EXCEPTION 'schema4 seed requires schema version 4';
    END IF;
    IF (SELECT count(*) FROM ob_job_execution WHERE id IN (93001, 93002)) <> 2 THEN
        RAISE EXCEPTION 'schema4 execution seed is incomplete';
    END IF;
    IF convert_from(
        (SELECT payload FROM ob_component_state WHERE id = 95001),
        'UTF8'
    ) <> '{"cursor":7}' THEN
        RAISE EXCEPTION 'schema4 component-state seed is not byte preserved';
    END IF;
END
$verify$;
