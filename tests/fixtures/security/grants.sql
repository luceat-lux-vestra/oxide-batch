-- What each M5 privilege class may do to the schema-3 metadata.
--
-- The migration role applies this after running the migrator, because it owns
-- the objects and because the grants can only name tables that exist. Ownership
-- is the reason the migration class has no grants of its own here: it created
-- these objects and can already reach them, and no grant would restrain it.
-- What restrains it is roles.sql, which denies it every cluster-level
-- privilege, and the absence of any grant reaching outside this schema.
--
-- The classes below are separated by what the services actually do, so a grant
-- that no supported path needs is a grant that is not here:
--
-- runtime    drives the job, step, partition, and flow lifecycle. It writes the
--            execution graph and reads the decisions other classes record. It
--            never deletes, never migrates, and never records an operator or
--            retention action.
-- explorer   answers bounded read questions and nothing else, so it holds
--            SELECT and no other privilege on anything.
-- operator   records guarded, audited requests and resolves executions. It may
--            move an execution's status and ask one to stop; it may not claim
--            ownership of a live execution, create a step, or delete anything.
-- retention  plans and applies purges and places and releases holds. It may
--            remove history and mark an instance held; it may not advance a
--            lifecycle or record an operator decision.
--
-- Two boundaries are expressed at column granularity rather than table
-- granularity, because the table is shared by two classes and the split is the
-- point. The operator's UPDATE on ob_job_execution excludes owner_token, so an
-- operator identity cannot take ownership of an execution a live runtime holds.
-- Retention's UPDATE on ob_job_instance is confined to the hold columns, so
-- placing a hold cannot rewrite an instance's identity.
--
-- The migration bookkeeping table is granted to no one. A class that could
-- rewrite it could tell a runtime it was looking at a different schema.

GRANT USAGE ON SCHEMA oxide_batch TO
    oxide_batch_m5_runtime,
    oxide_batch_m5_explorer,
    oxide_batch_m5_operator,
    oxide_batch_m5_retention;

-- Every class reads the recorded schema version: the repository verifies it on
-- open, and refusing a database it cannot read is not the same as refusing one
-- whose version it is not allowed to see.
GRANT SELECT ON oxide_batch.ob_schema_version TO
    oxide_batch_m5_runtime,
    oxide_batch_m5_explorer,
    oxide_batch_m5_operator,
    oxide_batch_m5_retention;

-- runtime -------------------------------------------------------------------

GRANT SELECT, INSERT, UPDATE ON
    oxide_batch.ob_job_definition,
    oxide_batch.ob_definition_upgrade,
    oxide_batch.ob_job_execution,
    oxide_batch.ob_step_execution,
    oxide_batch.ob_step_partition,
    oxide_batch.ob_flow_decision
    TO oxide_batch_m5_runtime;
-- Creating an attempt locks the instance row for the duration of the
-- transaction, so that two launches cannot both decide they are the first.
-- PostgreSQL requires UPDATE on at least one column to take that lock, and the
-- grant is confined to the identity columns the runtime writes when it creates
-- the instance. The hold columns are not among them, so a runtime cannot place
-- or lift a retention hold.
GRANT SELECT, INSERT ON oxide_batch.ob_job_instance TO oxide_batch_m5_runtime;
GRANT UPDATE (
    job_name,
    instance_key,
    identifying_parameters
) ON oxide_batch.ob_job_instance TO oxide_batch_m5_runtime;
-- Restart reads what the operator and the recovery path decided; it records
-- neither.
GRANT SELECT ON
    oxide_batch.ob_recovery_decision,
    oxide_batch.ob_operator_request,
    oxide_batch.ob_retention_action
    TO oxide_batch_m5_runtime;

-- explorer ------------------------------------------------------------------

GRANT SELECT ON ALL TABLES IN SCHEMA oxide_batch TO oxide_batch_m5_explorer;

-- operator ------------------------------------------------------------------

GRANT SELECT ON ALL TABLES IN SCHEMA oxide_batch TO oxide_batch_m5_operator;
GRANT INSERT ON
    oxide_batch.ob_operator_request,
    oxide_batch.ob_recovery_decision
    TO oxide_batch_m5_operator;
GRANT UPDATE (
    status,
    exit_code,
    failure_category,
    failure_id,
    started_at,
    ended_at,
    updated_at,
    version,
    stop_requested_at,
    stop_requested_by
) ON oxide_batch.ob_job_execution TO oxide_batch_m5_operator;

-- retention -----------------------------------------------------------------

GRANT SELECT ON ALL TABLES IN SCHEMA oxide_batch TO oxide_batch_m5_retention;
GRANT INSERT, UPDATE ON oxide_batch.ob_retention_action TO oxide_batch_m5_retention;
GRANT UPDATE (
    hold_actor,
    hold_reason,
    hold_placed_at
) ON oxide_batch.ob_job_instance TO oxide_batch_m5_retention;
-- A surviving flow decision may cite a purged one as its provenance, and the
-- citation is cleared before the row it names is removed.
GRANT UPDATE (reused_decision_id) ON oxide_batch.ob_flow_decision
    TO oxide_batch_m5_retention;
GRANT DELETE ON
    oxide_batch.ob_flow_decision,
    oxide_batch.ob_recovery_decision,
    oxide_batch.ob_operator_request,
    oxide_batch.ob_step_partition,
    oxide_batch.ob_step_execution,
    oxide_batch.ob_job_execution,
    oxide_batch.ob_job_instance
    TO oxide_batch_m5_retention;

-- sequences -----------------------------------------------------------------

-- Identity columns draw from sequences, so a class that may insert a row must
-- be able to draw the identifier for it. The explorer inserts nothing and is
-- deliberately absent.
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA oxide_batch TO
    oxide_batch_m5_runtime,
    oxide_batch_m5_operator,
    oxide_batch_m5_retention;

-- migration bookkeeping -----------------------------------------------------

REVOKE ALL ON oxide_batch._sqlx_migrations FROM
    oxide_batch_m5_runtime,
    oxide_batch_m5_explorer,
    oxide_batch_m5_operator,
    oxide_batch_m5_retention;
