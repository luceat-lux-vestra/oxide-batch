\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 5 THEN
        RAISE EXCEPTION 'restored metadata schema is not version 5';
    END IF;
    IF (SELECT status FROM ob_job_execution WHERE id = 93001) <> 'STOPPED' THEN
        RAISE EXCEPTION 'restored parent execution changed';
    END IF;
    IF (SELECT status FROM ob_job_execution WHERE id = 93002) <> 'COMPLETED' THEN
        RAISE EXCEPTION 'restored child execution changed';
    END IF;
    IF convert_from(
        (SELECT payload FROM ob_component_state WHERE id = 95001),
        'UTF8'
    ) <> '{"cursor":7}' THEN
        RAISE EXCEPTION 'restored component-state bytes changed';
    END IF;
    IF (
        SELECT row(
            parent_job_instance_id,
            parent_job_execution_id,
            node_id,
            child_definition_id,
            child_job_instance_id,
            child_job_execution_id,
            terminal_status,
            terminal_exit_code
        )::text
        FROM ob_nested_job_link
        WHERE id = 96001
    ) <> '(92001,93001,nested-child,91002,92002,93002,COMPLETED,COMPLETED)' THEN
        RAISE EXCEPTION 'restored nested-job linkage changed';
    END IF;
    IF to_regclass('oxide_batch.ob_nested_job_link_parent_lineage') IS NULL
       OR to_regclass('oxide_batch.ob_nested_job_link_child_execution') IS NULL THEN
        RAISE EXCEPTION 'restored schema5 nested-job indexes are missing';
    END IF;
END
$verify$;
