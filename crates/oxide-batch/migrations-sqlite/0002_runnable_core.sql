CREATE TABLE IF NOT EXISTS ob_core_job_definition (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name TEXT NOT NULL,
    definition_revision TEXT NOT NULL,
    manifest_format INTEGER NOT NULL,
    manifest_digest BLOB NOT NULL,
    manifest TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    UNIQUE (job_name, definition_revision)
);

CREATE INDEX IF NOT EXISTS ix_ob_core_definition_manifest
    ON ob_core_job_definition(job_name, manifest_digest);

ALTER TABLE ob_job_execution
    ADD COLUMN definition_id INTEGER NULL
    REFERENCES ob_core_job_definition(id);

CREATE INDEX IF NOT EXISTS ix_ob_job_execution_definition
    ON ob_job_execution(definition_id);

CREATE TABLE ob_step_execution_v2 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_execution_id INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    step_logical_id TEXT NOT NULL,
    status TEXT NOT NULL,
    exit_code TEXT NOT NULL,
    failure_category TEXT NULL,
    failure_id INTEGER NULL,
    read_count INTEGER NOT NULL DEFAULT 0,
    processed_count INTEGER NOT NULL DEFAULT 0,
    write_count INTEGER NOT NULL DEFAULT 0,
    filter_count INTEGER NOT NULL DEFAULT 0,
    commit_count INTEGER NOT NULL DEFAULT 0,
    rollback_count INTEGER NOT NULL DEFAULT 0,
    created_ms INTEGER NOT NULL,
    started_ms INTEGER NULL,
    ended_ms INTEGER NULL,
    updated_ms INTEGER NOT NULL,
    version INTEGER NOT NULL,
    FOREIGN KEY (job_execution_id) REFERENCES ob_job_execution(id)
);

INSERT INTO ob_step_execution_v2 (
    id,
    job_execution_id,
    step_name,
    step_logical_id,
    status,
    exit_code,
    failure_category,
    failure_id,
    read_count,
    processed_count,
    write_count,
    filter_count,
    commit_count,
    rollback_count,
    created_ms,
    started_ms,
    ended_ms,
    updated_ms,
    version
)
SELECT
    id,
    job_execution_id,
    step_name,
    step_name,
    status,
    exit_code,
    failure_category,
    failure_id,
    read_count,
    processed_count,
    write_count,
    filter_count,
    commit_count,
    rollback_count,
    created_ms,
    started_ms,
    ended_ms,
    updated_ms,
    version
FROM ob_step_execution;

DROP TABLE ob_step_execution;
ALTER TABLE ob_step_execution_v2 RENAME TO ob_step_execution;

CREATE INDEX IF NOT EXISTS ix_ob_step_execution_job
    ON ob_step_execution(job_execution_id, id);

CREATE INDEX IF NOT EXISTS ix_ob_step_execution_logical
    ON ob_step_execution(step_logical_id, job_execution_id, id);

CREATE TABLE IF NOT EXISTS ob_flow_decision (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_execution_id INTEGER NOT NULL,
    source_step_execution_id INTEGER NULL,
    reused_decision_id INTEGER NULL,
    sequence INTEGER NOT NULL,
    source_node_id TEXT NOT NULL,
    observed_outcome TEXT NOT NULL,
    target_node_id TEXT NULL,
    transition_kind TEXT NOT NULL,
    terminal_kind TEXT NULL,
    plan_fingerprint BLOB NOT NULL,
    input_digest BLOB NOT NULL,
    decided_ms INTEGER NOT NULL,
    UNIQUE (job_execution_id, sequence),
    UNIQUE (job_execution_id, source_node_id),
    FOREIGN KEY (job_execution_id) REFERENCES ob_job_execution(id),
    FOREIGN KEY (source_step_execution_id) REFERENCES ob_step_execution(id),
    FOREIGN KEY (reused_decision_id) REFERENCES ob_flow_decision(id)
);

CREATE INDEX IF NOT EXISTS ix_ob_flow_decision_reuse
    ON ob_flow_decision(
        source_node_id,
        transition_kind,
        plan_fingerprint,
        input_digest,
        job_execution_id
    );
