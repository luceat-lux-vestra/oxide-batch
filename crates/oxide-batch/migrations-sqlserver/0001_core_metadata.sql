IF OBJECT_ID(N'dbo.ob_job_instance', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.ob_job_instance (
        id BIGINT IDENTITY(1,1) NOT NULL,
        job_name NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
        instance_key VARBINARY(32) NOT NULL,
        identifying_parameters NVARCHAR(MAX) NOT NULL,
        created_ms BIGINT NOT NULL,
        CONSTRAINT pk_ob_job_instance PRIMARY KEY (id),
        CONSTRAINT uq_ob_job_instance_key UNIQUE (job_name, instance_key)
    );
END;

IF OBJECT_ID(N'dbo.ob_job_execution', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.ob_job_execution (
        id BIGINT IDENTITY(1,1) NOT NULL,
        job_instance_id BIGINT NOT NULL,
        attempt BIGINT NOT NULL,
        status NVARCHAR(16) COLLATE Latin1_General_100_BIN2 NOT NULL,
        exit_code NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
        failure_category NVARCHAR(64) COLLATE Latin1_General_100_BIN2 NULL,
        failure_id BIGINT NULL,
        created_ms BIGINT NOT NULL,
        started_ms BIGINT NULL,
        ended_ms BIGINT NULL,
        updated_ms BIGINT NOT NULL,
        version BIGINT NOT NULL,
        CONSTRAINT pk_ob_job_execution PRIMARY KEY (id),
        CONSTRAINT uq_ob_job_execution_attempt UNIQUE (job_instance_id, attempt),
        CONSTRAINT fk_ob_job_execution_instance
            FOREIGN KEY (job_instance_id) REFERENCES dbo.ob_job_instance(id)
    );
END;

IF OBJECT_ID(N'dbo.ob_step_execution', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.ob_step_execution (
        id BIGINT IDENTITY(1,1) NOT NULL,
        job_execution_id BIGINT NOT NULL,
        step_name NVARCHAR(255) COLLATE Latin1_General_100_BIN2 NOT NULL,
        status NVARCHAR(16) COLLATE Latin1_General_100_BIN2 NOT NULL,
        exit_code NVARCHAR(128) COLLATE Latin1_General_100_BIN2 NOT NULL,
        failure_category NVARCHAR(64) COLLATE Latin1_General_100_BIN2 NULL,
        failure_id BIGINT NULL,
        read_count BIGINT NOT NULL CONSTRAINT df_ob_step_read_count DEFAULT 0,
        processed_count BIGINT NOT NULL CONSTRAINT df_ob_step_processed_count DEFAULT 0,
        write_count BIGINT NOT NULL CONSTRAINT df_ob_step_write_count DEFAULT 0,
        filter_count BIGINT NOT NULL CONSTRAINT df_ob_step_filter_count DEFAULT 0,
        commit_count BIGINT NOT NULL CONSTRAINT df_ob_step_commit_count DEFAULT 0,
        rollback_count BIGINT NOT NULL CONSTRAINT df_ob_step_rollback_count DEFAULT 0,
        created_ms BIGINT NOT NULL,
        started_ms BIGINT NULL,
        ended_ms BIGINT NULL,
        updated_ms BIGINT NOT NULL,
        version BIGINT NOT NULL,
        CONSTRAINT pk_ob_step_execution PRIMARY KEY (id),
        CONSTRAINT fk_ob_step_execution_job
            FOREIGN KEY (job_execution_id) REFERENCES dbo.ob_job_execution(id)
    );

    CREATE INDEX ix_ob_step_execution_job
        ON dbo.ob_step_execution(job_execution_id, id);
END;
