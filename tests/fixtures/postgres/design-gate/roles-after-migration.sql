\set ON_ERROR_STOP on

REVOKE ALL ON oxide_batch._sqlx_migrations FROM
    oxide_batch_runtime,
    oxide_batch_operator_reader,
    oxide_batch_operator_writer;

GRANT INSERT ON oxide_batch.ob_recovery_decision
    TO oxide_batch_operator_writer;
GRANT UPDATE (
    status,
    exit_code,
    failure_category,
    failure_id,
    ended_at,
    updated_at,
    version
) ON oxide_batch.ob_job_execution
    TO oxide_batch_operator_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA oxide_batch
    TO oxide_batch_operator_writer;
