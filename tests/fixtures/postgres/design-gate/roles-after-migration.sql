\set ON_ERROR_STOP on

REVOKE ALL ON oxide_batch._sqlx_migrations FROM
    oxide_batch_runtime,
    oxide_batch_operator_reader,
    oxide_batch_operator_writer;

-- The runtime never deletes metadata. Purge is an operator-writer action, so
-- the default table privileges granted at role creation are narrowed here.
REVOKE DELETE ON ALL TABLES IN SCHEMA oxide_batch FROM oxide_batch_runtime;

GRANT INSERT ON oxide_batch.ob_recovery_decision
    TO oxide_batch_operator_writer;
GRANT INSERT ON oxide_batch.ob_operator_request
    TO oxide_batch_operator_writer;
GRANT INSERT ON oxide_batch.ob_retention_action
    TO oxide_batch_operator_writer;
GRANT UPDATE (
    status,
    exit_code,
    failure_category,
    failure_id,
    ended_at,
    updated_at,
    version,
    stop_requested_at,
    stop_requested_by
) ON oxide_batch.ob_job_execution
    TO oxide_batch_operator_writer;
GRANT UPDATE (
    hold_actor,
    hold_reason,
    hold_placed_at
) ON oxide_batch.ob_job_instance
    TO oxide_batch_operator_writer;
GRANT UPDATE (reused_decision_id) ON oxide_batch.ob_flow_decision
    TO oxide_batch_operator_writer;
GRANT UPDATE (job_instance_id) ON oxide_batch.ob_retention_action
    TO oxide_batch_operator_writer;

-- The narrowly granted deletes required by a bounded purge batch. No role may
-- delete a definition, an upgrade edge, or the schema version row.
GRANT DELETE ON
    oxide_batch.ob_flow_decision,
    oxide_batch.ob_recovery_decision,
    oxide_batch.ob_operator_request,
    oxide_batch.ob_step_partition,
    oxide_batch.ob_step_execution,
    oxide_batch.ob_job_execution,
    oxide_batch.ob_job_instance
    TO oxide_batch_operator_writer;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA oxide_batch
    TO oxide_batch_operator_writer;
