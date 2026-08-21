-- Representative durable state of an OxideBatch schema-3 database.
--
-- The schema itself is not written here. It is installed by running the
-- immutable migration set up to `0003_operations_and_local_scale.sql` and
-- stopping, so this script only supplies the rows an operator's database
-- would have held when schema 3 was the whole schema.
--
-- It covers everything the [schema-2 fixture](../schema-2/seed.sql) covers
-- and the durable state schema 3 introduced: a stop request recorded on a
-- live attempt, an audited operator request, a retention action, and a
-- step partition. The `ob_job_instance` hold columns are deliberately left
-- NULL (schema 3's all-or-none shape permits this): the M5 upgrade-rollback
-- report places a hold itself, through the retention service, on the
-- already-upgraded database, and a fixture that pre-seeded one would make
-- that call fail against an instance already held.
--
-- The operator request's `request_digest` and the retention action's
-- `plan_digest` are opaque 32-byte values here, the same way the flow
-- decision's fingerprints are in the schema-2 fixture: this script proves
-- the row round-trips, not that it was produced by the real request/plan
-- hashing the runtime uses.

INSERT INTO oxide_batch.ob_job_definition
    (id, job_name, definition_revision, manifest_format, manifest_digest,
     manifest, registered_at)
VALUES
    (1, 'm5_upgrade', 'm5-upgrade-v1', 1,
     decode('1111111111111111111111111111111111111111111111111111111111111111', 'hex'),
     '{"job": "m5_upgrade", "step": "import", "chunk_size": 5}'::jsonb,
     TIMESTAMPTZ '2026-07-30 09:00:00+00'),
    (2, 'm5_upgrade', 'm5-upgrade-v2', 1,
     decode('2222222222222222222222222222222222222222222222222222222222222222', 'hex'),
     '{"job": "m5_upgrade", "step": "import", "chunk_size": 10}'::jsonb,
     TIMESTAMPTZ '2026-07-30 09:05:00+00');

INSERT INTO oxide_batch.ob_definition_upgrade
    (from_definition_id, to_definition_id, upgrade_key, step_mapping, registered_at)
VALUES
    (1, 2, 'm5-upgrade-chunk-size', '{"import": "import"}'::jsonb,
     TIMESTAMPTZ '2026-07-30 09:05:00+00');

INSERT INTO oxide_batch.ob_job_instance
    (id, job_name, instance_key, identifying_parameters, created_at)
VALUES
    (1, 'm5_upgrade',
     decode('3e765bfe745b94b09d79cc090ebd6a6f28abc0d3900c5aea6d1615c1e171e55f', 'hex'),
     '{}'::jsonb,
     TIMESTAMPTZ '2026-07-30 09:10:00+00');

INSERT INTO oxide_batch.ob_job_execution
    (id, job_instance_id, definition_id, upgrade_from_definition_id,
     restart_of_execution_id, attempt, status, exit_code, parameters,
     context_format, context_schema, context_schema_version, context_payload,
     failure_category, failure_id, owner_token, stop_requested_at,
     stop_requested_by, created_at, started_at, ended_at, updated_at,
     version)
VALUES
    (1, 1, 1, NULL, NULL, 1, 'FAILED', 'FAILED', '{}'::jsonb,
     1, 'm5.upgrade.context', 1, '{"tenant": "acme"}'::jsonb,
     'UNKNOWN_COMMIT', 1, NULL, NULL, NULL,
     TIMESTAMPTZ '2026-07-30 09:10:00+00',
     TIMESTAMPTZ '2026-07-30 09:11:00+00',
     TIMESTAMPTZ '2026-07-30 09:20:00+00',
     TIMESTAMPTZ '2026-07-30 09:20:00+00', 3),
    (2, 1, 1, NULL, 1, 2, 'STARTED', 'UNKNOWN', '{}'::jsonb,
     1, 'm5.upgrade.context', 1, '{"tenant": "acme"}'::jsonb,
     NULL, NULL,
     decode('55555555555555555555555555555555', 'hex'),
     TIMESTAMPTZ '2026-07-30 09:40:00+00',
     'operator:m5-upgrade-campaign',
     TIMESTAMPTZ '2026-07-30 09:30:00+00',
     TIMESTAMPTZ '2026-07-30 09:31:00+00',
     NULL,
     TIMESTAMPTZ '2026-07-30 09:40:00+00', 2);

INSERT INTO oxide_batch.ob_step_execution
    (id, job_execution_id, step_name, step_logical_id, status, exit_code,
     read_count, processed_count, write_count, filter_count, commit_count,
     rollback_count, read_retry_count, process_retry_count, write_retry_count,
     read_skip_count, process_skip_count, write_skip_count, no_rollback_count,
     checkpoint_format, checkpoint_schema, checkpoint_schema_version,
     checkpoint_payload, context_format, context_schema,
     context_schema_version, context_payload, failure_category, failure_id,
     created_at, started_at, ended_at, updated_at, version)
VALUES
    (1, 1, 'import', 'import', 'FAILED', 'FAILED', 20, 20, 20, 0, 4, 1,
     2, 1, 0, 1, 0, 0, 3,
     1, 'm5.upgrade.position', 1, '{"position": 20}'::jsonb,
     1, 'm5.upgrade.context', 1, '{"tenant": "acme"}'::jsonb,
     'UNKNOWN_COMMIT', 1,
     TIMESTAMPTZ '2026-07-30 09:11:00+00',
     TIMESTAMPTZ '2026-07-30 09:11:00+00',
     TIMESTAMPTZ '2026-07-30 09:20:00+00',
     TIMESTAMPTZ '2026-07-30 09:20:00+00', 5),
    (2, 2, 'import', 'import', 'STARTED', 'UNKNOWN', 40, 40, 40, 0, 8, 1,
     3, 1, 1, 2, 0, 1, 4,
     1, 'm5.upgrade.position', 1, '{"position": 40}'::jsonb,
     1, 'm5.upgrade.context', 1, '{"tenant": "acme"}'::jsonb,
     NULL, NULL,
     TIMESTAMPTZ '2026-07-30 09:31:00+00',
     TIMESTAMPTZ '2026-07-30 09:31:00+00',
     NULL,
     TIMESTAMPTZ '2026-07-30 09:35:00+00', 9);

INSERT INTO oxide_batch.ob_recovery_decision
    (id, job_execution_id, execution_version, prior_status, resulting_status,
     reason_code, operator_reference, evidence_digest, decided_at)
VALUES
    (1, 1, 2, 'STARTED', 'FAILED', 'HOST_LOST', 'm5-upgrade-campaign',
     decode('3333333333333333333333333333333333333333333333333333333333333333', 'hex'),
     TIMESTAMPTZ '2026-07-30 09:20:00+00');

INSERT INTO oxide_batch.ob_flow_decision
    (id, job_execution_id, source_step_execution_id, reused_decision_id,
     sequence, source_node_id, observed_outcome, target_node_id,
     transition_kind, terminal_kind, plan_fingerprint, input_digest, decided_at)
VALUES
    (1, 2, 2, NULL, 1, 'import', 'COMPLETED', 'reconcile', 'STEP_EXIT', NULL,
     decode('1111111111111111111111111111111111111111111111111111111111111111', 'hex'),
     decode('4444444444444444444444444444444444444444444444444444444444444444', 'hex'),
     TIMESTAMPTZ '2026-07-30 09:34:00+00');

INSERT INTO oxide_batch.ob_operator_request
    (id, job_instance_id, job_execution_id, action, authorization_class,
     operation_id, actor_ref, reason_code, request_digest, observed_version,
     prior_status, result_status, outcome_class, rejection_code,
     requested_at)
VALUES
    (1, 1, 2, 'STOP', 'LIFECYCLE', 'm5-upgrade-schema3-stop',
     'operator:m5-upgrade-campaign', 'PLANNED_MAINTENANCE',
     decode('6666666666666666666666666666666666666666666666666666666666666666', 'hex'),
     2, 'STARTED', 'STARTED', 'APPLIED', NULL,
     TIMESTAMPTZ '2026-07-30 09:40:00+00');

INSERT INTO oxide_batch.ob_retention_action
    (id, job_instance_id, action, operation_id, actor_ref, reason_code,
     plan_digest, batch_bound, deleted_flow_decisions,
     deleted_recovery_decisions, deleted_operator_requests,
     deleted_step_partitions, deleted_step_executions, deleted_job_executions,
     deleted_job_instances, outcome_class, applied_at)
VALUES
    (1, 1, 'HOLD', 'm5-upgrade-schema3-hold', 'operator:m5-upgrade-campaign',
     'PLANNED_MAINTENANCE', NULL, NULL, 0, 0, 0, 0, 0, 0, 0, 'APPLIED',
     TIMESTAMPTZ '2026-07-30 09:41:00+00');

INSERT INTO oxide_batch.ob_step_partition
    (id, step_execution_id, worker_step_execution_id, partition_key,
     partition_ordinal, status, exit_code, read_count, processed_count,
     write_count, filter_count, commit_count, rollback_count, context_format,
     context_schema, context_schema_version, context_payload,
     context_checksum, version)
VALUES
    (1, 2, NULL, 'partition-0', 1, 'STARTED', NULL, 20, 20, 20, 0, 4, 0,
     1, 'm5.upgrade.partition-context', 1, '{"range": "0-19"}'::jsonb,
     decode('7777777777777777777777777777777777777777777777777777777777777777', 'hex'),
     4);

-- A database that had these rows would also have issued their identifiers, so
-- the identity sequences are advanced past them. Leaving them at the start
-- would make the next allocation collide, which is a property of this script
-- rather than of the schema being upgraded.
SELECT setval(pg_get_serial_sequence('oxide_batch.ob_job_definition', 'id'), 2, TRUE);
SELECT setval(pg_get_serial_sequence('oxide_batch.ob_job_instance', 'id'), 1, TRUE);
SELECT setval(pg_get_serial_sequence('oxide_batch.ob_job_execution', 'id'), 2, TRUE);
SELECT setval(pg_get_serial_sequence('oxide_batch.ob_step_execution', 'id'), 2, TRUE);
SELECT setval(pg_get_serial_sequence('oxide_batch.ob_recovery_decision', 'id'), 1, TRUE);
SELECT setval(pg_get_serial_sequence('oxide_batch.ob_flow_decision', 'id'), 1, TRUE);
SELECT setval(pg_get_serial_sequence('oxide_batch.ob_operator_request', 'id'), 1, TRUE);
SELECT setval(pg_get_serial_sequence('oxide_batch.ob_retention_action', 'id'), 1, TRUE);
SELECT setval(pg_get_serial_sequence('oxide_batch.ob_step_partition', 'id'), 1, TRUE);
