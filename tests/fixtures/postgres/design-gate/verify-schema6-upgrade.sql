\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 6 THEN
        RAISE EXCEPTION 'schema6 upgrade did not publish version 6';
    END IF;
    IF (SELECT status FROM ob_job_execution WHERE id = 93001) <> 'STOPPED'
       OR (SELECT status FROM ob_job_execution WHERE id = 93002) <> 'COMPLETED' THEN
        RAISE EXCEPTION 'schema6 upgrade changed prior executions';
    END IF;
    IF convert_from(
        (SELECT payload FROM ob_component_state WHERE id = 95001),
        'UTF8'
    ) <> '{"cursor":7}' THEN
        RAISE EXCEPTION 'schema6 upgrade changed prior component-state bytes';
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
        RAISE EXCEPTION 'schema6 upgrade changed prior nested-job linkage';
    END IF;
    IF to_regclass('oxide_batch.ob_scope_resolution_provenance') IS NULL THEN
        RAISE EXCEPTION 'schema6 scope provenance table is missing';
    END IF;
    IF to_regclass('oxide_batch.ob_scope_resolution_job_input') IS NULL
       OR to_regclass('oxide_batch.ob_scope_resolution_step_input') IS NULL
       OR to_regclass('oxide_batch.ob_scope_resolution_source_job') IS NULL
       OR to_regclass('oxide_batch.ob_scope_resolution_source_step') IS NULL THEN
        RAISE EXCEPTION 'schema6 scope provenance indexes are missing';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'oxide_batch'
          AND table_name = 'ob_scope_resolution_provenance'
          AND column_name ~ '(value|digest|payload|checksum)'
    ) THEN
        RAISE EXCEPTION 'schema6 provenance exposes a value or value-derived digest column';
    END IF;
    IF (SELECT count(*) FROM ob_scope_resolution_provenance) <> 0 THEN
        RAISE EXCEPTION 'schema6 migration invented provenance for prior executions';
    END IF;
END
$verify$;

INSERT INTO ob_scope_resolution_provenance (
    id, owner_job_execution_id, owner_step_execution_id, scope_kind,
    component_id, input_name, source_kind, source_parameter_name,
    source_node_id, source_framework_field, source_job_execution_id,
    source_step_execution_id, source_execution_version, source_schema,
    source_schema_version, source_path
) VALUES
    (
        97001, 93001, NULL, 'job',
        'client', 'tenant', 'job_parameter', 'run',
        NULL, NULL, 93001,
        NULL, NULL, NULL,
        NULL, NULL
    ),
    (
        97002, 93001, NULL, 'job',
        'client', 'job_ctx', 'job_context', NULL,
        NULL, NULL, 93001,
        NULL, 2, 'fixture-context',
        1, '["parent"]'::jsonb
    ),
    (
        97003, 93001, 94001, 'step',
        'reader', 'step_ctx', 'step_context', NULL,
        'preserved_step', NULL, NULL,
        94001, 2, 'fixture-context',
        1, '["step"]'::jsonb
    ),
    (
        97004, 93001, NULL, 'job',
        'client', 'attempt', 'framework', NULL,
        NULL, 'attempt', 93001,
        NULL, NULL, NULL,
        NULL, NULL
    ),
    (
        97005, 93002, NULL, 'job',
        'child-client', 'parent_step', 'step_context', NULL,
        'preserved_step', NULL, NULL,
        94001, 2, 'fixture-context',
        1, '["step"]'::jsonb
    );

DO $verify$
BEGIN
    IF (SELECT count(*) FROM ob_scope_resolution_provenance) <> 5 THEN
        RAISE EXCEPTION 'schema6 provenance fixture is incomplete';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM ob_scope_resolution_provenance
        WHERE source_kind = 'job_parameter'
          AND (
              source_parameter_name <> 'run'
              OR source_job_execution_id <> 93001
          )
    ) THEN
        RAISE EXCEPTION 'schema6 job-parameter provenance changed identity';
    END IF;
    IF (
        SELECT source_path
        FROM ob_scope_resolution_provenance
        WHERE id = 97002
    ) <> '["parent"]'::jsonb THEN
        RAISE EXCEPTION 'schema6 job-context selector path changed';
    END IF;

    BEGIN
        DELETE FROM ob_component_state WHERE step_execution_id = 94001;
        DELETE FROM ob_step_execution WHERE id = 94001;
        RAISE EXCEPTION 'schema6 allowed deletion of a referenced authoritative step source';
    EXCEPTION
        WHEN restrict_violation OR foreign_key_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO ob_scope_resolution_provenance (
            owner_job_execution_id, scope_kind, component_id, input_name,
            source_kind, source_job_execution_id, source_execution_version,
            source_schema, source_schema_version, source_path
        ) VALUES (
            93001, 'job', 'invalid', 'empty_path',
            'job_context', 93001, 2,
            'fixture-context', 1, '[]'::jsonb
        );
        RAISE EXCEPTION 'schema6 accepted an empty selector path';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;

    BEGIN
        INSERT INTO ob_scope_resolution_provenance (
            owner_job_execution_id, scope_kind, component_id, input_name,
            source_kind, source_parameter_name, source_job_execution_id,
            source_execution_version, source_schema, source_schema_version,
            source_path
        ) VALUES (
            93001, 'job', 'invalid', 'mixed_shape',
            'job_parameter', 'run', 93001,
            2, 'fixture-context', 1, '["parent"]'::jsonb
        );
        RAISE EXCEPTION 'schema6 accepted a contradictory source-family shape';
    EXCEPTION
        WHEN check_violation THEN NULL;
    END;
END
$verify$;
