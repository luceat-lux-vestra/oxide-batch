CREATE TABLE IF NOT EXISTS ob_job_instance (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name TEXT NOT NULL,
    instance_key BLOB NOT NULL,
    identifying_parameters TEXT NOT NULL,
    created_ms INTEGER NOT NULL,
    UNIQUE (job_name, instance_key)
);

CREATE TABLE IF NOT EXISTS ob_job_execution (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_instance_id INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    exit_code TEXT NOT NULL,
    failure_category TEXT NULL,
    failure_id INTEGER NULL,
    created_ms INTEGER NOT NULL,
    started_ms INTEGER NULL,
    ended_ms INTEGER NULL,
    updated_ms INTEGER NOT NULL,
    version INTEGER NOT NULL,
    UNIQUE (job_instance_id, attempt),
    FOREIGN KEY (job_instance_id) REFERENCES ob_job_instance(id)
);

CREATE TABLE IF NOT EXISTS ob_step_execution (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_execution_id INTEGER NOT NULL,
    step_name TEXT NOT NULL,
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

CREATE INDEX IF NOT EXISTS ix_ob_step_execution_job
    ON ob_step_execution(job_execution_id, id);
