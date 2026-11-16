IF OBJECT_ID(N'dbo.ob_core_job_definition', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.ob_core_job_definition (
        id BIGINT IDENTITY(1,1) NOT NULL,
        job_name NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
        definition_revision NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
        manifest_format SMALLINT NOT NULL,
        manifest_digest VARBINARY(32) NOT NULL,
        manifest NVARCHAR(MAX) NOT NULL,
        created_ms BIGINT NOT NULL,
        CONSTRAINT pk_ob_core_job_definition PRIMARY KEY (id),
        CONSTRAINT uq_ob_core_definition_revision UNIQUE (job_name, definition_revision)
    );

    CREATE INDEX ix_ob_core_definition_manifest
        ON dbo.ob_core_job_definition(job_name, manifest_digest);
END;

IF COL_LENGTH(N'dbo.ob_job_execution', N'definition_id') IS NULL
BEGIN
    ALTER TABLE dbo.ob_job_execution ADD definition_id BIGINT NULL;
    ALTER TABLE dbo.ob_job_execution
        ADD CONSTRAINT fk_ob_job_execution_core_definition
        FOREIGN KEY (definition_id) REFERENCES dbo.ob_core_job_definition(id);
    CREATE INDEX ix_ob_job_execution_definition
        ON dbo.ob_job_execution(definition_id);
END;

IF COL_LENGTH(N'dbo.ob_step_execution', N'step_logical_id') IS NULL
BEGIN
    ALTER TABLE dbo.ob_step_execution
        ADD step_logical_id NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NULL;

    EXEC(N'
        UPDATE dbo.ob_step_execution
            SET step_logical_id = step_name
            WHERE step_logical_id IS NULL;
    ');

    EXEC(N'
        ALTER TABLE dbo.ob_step_execution
            ALTER COLUMN step_logical_id NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL;
    ');

    EXEC(N'
        CREATE INDEX ix_ob_step_execution_logical
            ON dbo.ob_step_execution(step_logical_id, job_execution_id, id);
    ');
END;

IF OBJECT_ID(N'dbo.ob_flow_decision', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.ob_flow_decision (
        id BIGINT IDENTITY(1,1) NOT NULL,
        job_execution_id BIGINT NOT NULL,
        source_step_execution_id BIGINT NULL,
        reused_decision_id BIGINT NULL,
        sequence BIGINT NOT NULL,
        source_node_id NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
        observed_outcome NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
        target_node_id NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NULL,
        transition_kind NVARCHAR(32) COLLATE Latin1_General_100_BIN2 NOT NULL,
        terminal_kind NVARCHAR(16) COLLATE Latin1_General_100_BIN2 NULL,
        plan_fingerprint VARBINARY(32) NOT NULL,
        input_digest VARBINARY(32) NOT NULL,
        decided_ms BIGINT NOT NULL,
        CONSTRAINT pk_ob_flow_decision PRIMARY KEY (id),
        CONSTRAINT uq_ob_flow_decision_sequence UNIQUE (job_execution_id, sequence),
        CONSTRAINT uq_ob_flow_decision_source UNIQUE (job_execution_id, source_node_id),
        CONSTRAINT fk_ob_flow_decision_job
            FOREIGN KEY (job_execution_id) REFERENCES dbo.ob_job_execution(id),
        CONSTRAINT fk_ob_flow_decision_step
            FOREIGN KEY (source_step_execution_id) REFERENCES dbo.ob_step_execution(id),
        CONSTRAINT fk_ob_flow_decision_reused
            FOREIGN KEY (reused_decision_id) REFERENCES dbo.ob_flow_decision(id)
    );

    CREATE INDEX ix_ob_flow_decision_reuse
        ON dbo.ob_flow_decision(
            source_node_id,
            transition_kind,
            plan_fingerprint,
            input_digest,
            job_execution_id
        );
END;
