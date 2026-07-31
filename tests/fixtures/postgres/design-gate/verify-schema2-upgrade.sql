\set ON_ERROR_STOP on

-- Release verification for the schema-1 to schema-2 upgrade. Every assertion
-- raises rather than returning a row, so a regression fails the fixture instead
-- of producing output a reader has to interpret.

-- The whole fixture runs in one transaction that is rolled back, so the
-- constraint probes below cannot leave the upgraded database modified.
BEGIN;

SET search_path TO oxide_batch, pg_catalog;

DO $verify$
DECLARE
    observed bigint;
BEGIN
    SELECT count(*) INTO observed
    FROM ob_schema_version WHERE singleton = true AND version = 2;
    IF observed <> 1 THEN
        RAISE EXCEPTION 'exactly one schema-version row must contain 2, found %', observed;
    END IF;

    SELECT count(*) INTO observed
    FROM ob_step_execution WHERE step_logical_id IS DISTINCT FROM step_name;
    IF observed <> 0 THEN
        RAISE EXCEPTION '% step rows were not backfilled byte for byte', observed;
    END IF;

    SELECT count(*) INTO observed
    FROM ob_step_execution
    WHERE read_retry_count <> 0
       OR process_retry_count <> 0
       OR write_retry_count <> 0
       OR read_skip_count <> 0
       OR process_skip_count <> 0
       OR write_skip_count <> 0
       OR no_rollback_count <> 0;
    IF observed <> 0 THEN
        RAISE EXCEPTION '% upgraded step rows have a non-zero new counter', observed;
    END IF;

    SELECT count(*) INTO observed
    FROM ob_step_execution
    WHERE fault_state_format <> 1
       OR fault_state_schema <> 'oxide-batch.fault-state'
       OR fault_state_schema_version <> 1
       OR fault_state_payload <> '{"checkpoint": "0000000000000000000000000000000000000000000000000000000000000000", "entries": []}'::jsonb
       OR fault_state_checksum <> decode(
              'a491114819e0d3bd8b7ca004dc0636f95b45e2fcb1a67ddb5726beaea12f9922',
              'hex'
          );
    IF observed <> 0 THEN
        RAISE EXCEPTION '% upgraded step rows lack the published empty fault state', observed;
    END IF;

    -- The pre-migration history must survive byte for byte.
    SELECT count(*) INTO observed FROM ob_job_execution;
    IF observed <> 5 THEN
        RAISE EXCEPTION 'expected 5 preserved job executions, found %', observed;
    END IF;
    SELECT count(*) INTO observed
    FROM ob_step_execution
    WHERE read_count = 4 AND commit_count = 2 AND version = 2;
    IF observed <> 5 THEN
        RAISE EXCEPTION 'expected 5 preserved step executions, found %', observed;
    END IF;
    SELECT count(*) INTO observed
    FROM ob_job_execution
    WHERE status IN ('COMPLETED', 'FAILED', 'STOPPED', 'UNKNOWN', 'STARTED');
    IF observed <> 5 THEN
        RAISE EXCEPTION 'a source disposition was lost by the migration';
    END IF;
END
$verify$;

DO $constraints$
DECLARE
    missing text;
BEGIN
    FOREACH missing IN ARRAY ARRAY[
        'ob_step_execution_logical_id_unique',
        'ob_step_execution_logical_id_bounds',
        'ob_step_execution_failure_category_check',
        'ob_job_execution_failure_category_check',
        'ob_flow_decision_sequence_unique',
        'ob_flow_decision_source_unique',
        'ob_flow_decision_target_shape'
    ] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_constraint WHERE conname = missing
        ) THEN
            RAISE EXCEPTION 'schema-2 constraint % is absent', missing;
        END IF;
    END LOOP;

    FOREACH missing IN ARRAY ARRAY['ob_step_execution_logical_history'] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_indexes
            WHERE schemaname = 'oxide_batch' AND indexname = missing
        ) THEN
            RAISE EXCEPTION 'schema-2 index % is absent', missing;
        END IF;
    END LOOP;
END
$constraints$;

DO $categories$
BEGIN
    -- The extended category list accepts the four M3 values and still rejects
    -- an unknown one.
    UPDATE ob_job_execution
    SET failure_category = 'UNKNOWN_COMMIT', failure_id = 1
    WHERE status = 'UNKNOWN';

    BEGIN
        UPDATE ob_job_execution
        SET failure_category = 'NOT_A_CATEGORY', failure_id = 1
        WHERE status = 'UNKNOWN';
        RAISE EXCEPTION 'an unknown failure category was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;
END
$categories$;

DO $bounds$
BEGIN
    -- Corrupt fault state and an out-of-range counter fail closed in the
    -- database, before any runtime interprets them.
    BEGIN
        UPDATE ob_step_execution SET no_rollback_count = -1;
        RAISE EXCEPTION 'a negative fault counter was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        UPDATE ob_step_execution SET fault_state_checksum = decode('00', 'hex');
        RAISE EXCEPTION 'a short fault-state checksum was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        UPDATE ob_step_execution SET fault_state_payload = '[]'::jsonb;
        RAISE EXCEPTION 'a non-object fault state was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO ob_flow_decision (
            job_execution_id, sequence, source_node_id, observed_outcome,
            target_node_id, transition_kind, terminal_kind, plan_fingerprint,
            input_digest, decided_at
        )
        SELECT id, 1, 'import', 'COMPLETED', 'archive', 'STEP_EXIT', 'COMPLETE',
               decode(repeat('55', 32), 'hex'), decode(repeat('66', 32), 'hex'),
               CURRENT_TIMESTAMP
        FROM ob_job_execution LIMIT 1;
        RAISE EXCEPTION 'a decision with both a target and a terminal was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;
END
$bounds$;

ROLLBACK;
