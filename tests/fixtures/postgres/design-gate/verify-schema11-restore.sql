\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 11 THEN
        RAISE EXCEPTION 'restored metadata schema is not version 11';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM ob_job_fork_lineage
        WHERE operation_id = 'schema11-fixture-fork'
          AND source_job_execution_id = 93001
          AND source_execution_version = 2
          AND source_definition_digest = decode(repeat('11',32),'hex')
          AND source_state_digest = decode(repeat('aa',32),'hex')
          AND target_definition_id = 91002
          AND target_job_instance_id = 92003
          AND target_job_execution_id = 93003
          AND target_parameters_digest = decode(repeat('bb',32),'hex')
    ) THEN
        RAISE EXCEPTION 'schema11 restore lost exact immutable fork lineage';
    END IF;
    IF (SELECT status FROM ob_job_execution WHERE id = 93001) <> 'STOPPED'
       OR (SELECT status FROM ob_job_execution WHERE id = 93003) <> 'STARTING'
       OR (SELECT restart_of_execution_id FROM ob_job_execution WHERE id = 93003) IS NOT NULL
    THEN
        RAISE EXCEPTION 'schema11 restore changed source or target lifecycle';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM ob_definition_upgrade
        WHERE from_definition_id = 91002 AND to_definition_id = 91001
          AND upgrade_revision = 'edge-v1'
          AND state_transforms #>> '{0,revision}' = 'checkpoint-v1-to-v2'
    ) THEN
        RAISE EXCEPTION 'schema11 restore lost schema10 transformed upgrade edge';
    END IF;
END
$verify$;
