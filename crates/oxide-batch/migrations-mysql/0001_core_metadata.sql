CREATE TABLE IF NOT EXISTS ob_job_instance (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_name VARCHAR(255) NOT NULL,
    instance_key BINARY(32) NOT NULL,
    identifying_parameters LONGTEXT NOT NULL,
    created_ms BIGINT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_ob_job_instance_key (job_name, instance_key)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS ob_job_execution (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_instance_id BIGINT UNSIGNED NOT NULL,
    attempt BIGINT UNSIGNED NOT NULL,
    status VARCHAR(16) NOT NULL,
    exit_code VARCHAR(128) NOT NULL,
    failure_category VARCHAR(64) NULL,
    failure_id BIGINT UNSIGNED NULL,
    created_ms BIGINT NOT NULL,
    started_ms BIGINT NULL,
    ended_ms BIGINT NULL,
    updated_ms BIGINT NOT NULL,
    version BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_ob_job_execution_attempt (job_instance_id, attempt),
    CONSTRAINT fk_ob_job_execution_instance
        FOREIGN KEY (job_instance_id) REFERENCES ob_job_instance(id)
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS ob_step_execution (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_execution_id BIGINT UNSIGNED NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    status VARCHAR(16) NOT NULL,
    exit_code VARCHAR(128) NOT NULL,
    failure_category VARCHAR(64) NULL,
    failure_id BIGINT UNSIGNED NULL,
    read_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    processed_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    write_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    filter_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    commit_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    rollback_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
    created_ms BIGINT NOT NULL,
    started_ms BIGINT NULL,
    ended_ms BIGINT NULL,
    updated_ms BIGINT NOT NULL,
    version BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (id),
    KEY ix_ob_step_execution_job (job_execution_id, id),
    CONSTRAINT fk_ob_step_execution_job
        FOREIGN KEY (job_execution_id) REFERENCES ob_job_execution(id)
) ENGINE = InnoDB;
