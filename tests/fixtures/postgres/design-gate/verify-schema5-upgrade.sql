\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 5 THEN
        RAISE EXCEPTION 'schema5 upgrade did not publish version 5';
    END IF;
    IF (SELECT status FROM ob_job_execution WHERE id = 93001) <> 'STOPPED' THEN
        RAISE EXCEPTION 'schema5 upgrade changed prior parent execution';
    END IF;
    IF (SELECT status FROM ob_job_execution WHERE id = 93002) <> 'COMPLETED' THEN
        RAISE EXCEPTION 'schema5 upgrade changed prior child execution';
    END IF;
    IF convert_from(
        (SELECT payload FROM ob_component_state WHERE id = 95001),
        'UTF8'
    ) <> '{"cursor":7}' THEN
        RAISE EXCEPTION 'schema5 upgrade changed prior component-state bytes';
    END IF;
    IF to_regclass('oxide_batch.ob_nested_job_link') IS NULL THEN
        RAISE EXCEPTION 'schema5 nested-job link table is missing';
    END IF;
    IF to_regclass('oxide_batch.ob_nested_job_link_parent_lineage') IS NULL THEN
        RAISE EXCEPTION 'schema5 parent-lineage index is missing';
    END IF;
    IF to_regclass('oxide_batch.ob_nested_job_link_child_execution') IS NULL THEN
        RAISE EXCEPTION 'schema5 child-execution index is missing';
    END IF;
END
$verify$;

DO $verify$
BEGIN
    INSERT INTO ob_flow_decision (
        job_execution_id, source_step_execution_id, reused_decision_id,
        sequence, source_node_id, observed_outcome, target_node_id,
        transition_kind, terminal_kind, plan_fingerprint, input_digest, decided_at
    ) VALUES (
        93001, NULL, NULL,
        1, 'nested-exit-fixture', 'COMPLETED', NULL,
        'NESTED_JOB_EXIT', 'COMPLETE',
        decode(repeat('55', 32), 'hex'), decode(repeat('66', 32), 'hex'),
        '2026-09-01 00:00:05+00'
    );

    DELETE FROM ob_flow_decision
        WHERE job_execution_id = 93001
          AND source_node_id = 'nested-exit-fixture';
EXCEPTION
    WHEN check_violation THEN
        RAISE EXCEPTION 'schema5 flow-decision constraint rejects NESTED_JOB_EXIT';
END
$verify$;

INSERT INTO ob_nested_job_link (
    id, parent_job_instance_id, parent_job_execution_id, node_id,
    child_definition_id, child_job_instance_id, child_job_execution_id,
    linked_at, terminal_status, terminal_exit_code, terminal_observed_at
) VALUES (
    96001, 92001, 93001, 'nested-child',
    91002, 92002, 93002,
    '2026-09-01 00:00:05+00', 'COMPLETED', 'COMPLETED',
    '2026-09-01 00:00:06+00'
);

DO $verify$
BEGIN
    IF (SELECT count(*) FROM ob_nested_job_link WHERE id = 96001) <> 1 THEN
        RAISE EXCEPTION 'schema5 nested-job link row was not committed';
    END IF;
    IF (SELECT terminal_status FROM ob_nested_job_link WHERE id = 96001) <> 'COMPLETED' THEN
        RAISE EXCEPTION 'schema5 terminal observation was not preserved';
    END IF;

    BEGIN
        INSERT INTO ob_nested_job_link (
            parent_job_instance_id, parent_job_execution_id, node_id,
            child_definition_id, child_job_instance_id, child_job_execution_id,
            linked_at, terminal_status
        ) VALUES (
            92001, 93001, 'invalid-terminal-shape',
            91002, 92002, 93002,
            '2026-09-01 00:00:07+00', 'COMPLETED'
        );
        RAISE EXCEPTION 'invalid nested-job terminal shape was accepted';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;
END
$verify$;
