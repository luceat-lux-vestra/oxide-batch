ALTER TABLE ob_job_execution
    ADD COLUMN definition_revision VARCHAR(128) NULL,
    ADD COLUMN manifest_format SMALLINT UNSIGNED NULL,
    ADD COLUMN manifest_digest BINARY(32) NULL,
    ADD COLUMN manifest LONGTEXT NULL;

CREATE INDEX ix_ob_job_execution_definition
    ON ob_job_execution (definition_revision, manifest_digest);

ALTER TABLE ob_step_execution
    ADD COLUMN step_logical_id VARCHAR(128) NULL;

UPDATE ob_step_execution
    SET step_logical_id = step_name
    WHERE step_logical_id IS NULL;

ALTER TABLE ob_step_execution
    MODIFY step_logical_id VARCHAR(128) NOT NULL;

CREATE INDEX ix_ob_step_execution_logical_history
    ON ob_step_execution (step_logical_id, job_execution_id, id);

CREATE TABLE ob_flow_decision (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    job_execution_id BIGINT UNSIGNED NOT NULL,
    source_step_execution_id BIGINT UNSIGNED NULL,
    reused_decision_id BIGINT UNSIGNED NULL,
    sequence BIGINT UNSIGNED NOT NULL,
    source_node_id VARCHAR(128) NOT NULL,
    observed_outcome VARCHAR(128) NOT NULL,
    target_node_id VARCHAR(128) NULL,
    transition_kind VARCHAR(32) NOT NULL,
    terminal_kind VARCHAR(16) NULL,
    plan_fingerprint BINARY(32) NOT NULL,
    input_digest BINARY(32) NOT NULL,
    decided_ms BIGINT NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_ob_flow_decision_sequence (job_execution_id, sequence),
    UNIQUE KEY uq_ob_flow_decision_source (job_execution_id, source_node_id),
    KEY ix_ob_flow_decision_reuse
        (source_node_id, transition_kind, plan_fingerprint, input_digest, job_execution_id),
    CONSTRAINT fk_ob_flow_decision_job
        FOREIGN KEY (job_execution_id) REFERENCES ob_job_execution(id),
    CONSTRAINT fk_ob_flow_decision_step
        FOREIGN KEY (source_step_execution_id) REFERENCES ob_step_execution(id),
    CONSTRAINT fk_ob_flow_decision_reused
        FOREIGN KEY (reused_decision_id) REFERENCES ob_flow_decision(id)
) ENGINE = InnoDB;
