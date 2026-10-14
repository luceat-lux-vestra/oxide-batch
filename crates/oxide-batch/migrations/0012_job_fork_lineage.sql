SET LOCAL search_path TO oxide_batch, pg_catalog;

DO $$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 10 THEN
        RAISE EXCEPTION 'oxide_batch schema version 10 is required before schema 11';
    END IF;
END
$$;

-- New-lineage fork is a first attempt, never an inferred restart or upgrade.
-- No raw parameters, application state, executable paths, or credentials live here.
CREATE TABLE ob_job_fork_lineage (
    operation_id varchar(64) COLLATE "C" PRIMARY KEY
        CHECK (octet_length(operation_id) BETWEEN 1 AND 64),
    source_job_execution_id bigint NOT NULL
        REFERENCES ob_job_execution (id) ON DELETE RESTRICT,
    source_execution_version bigint NOT NULL
        CHECK (source_execution_version >= 0),
    source_definition_digest bytea NOT NULL
        CHECK (octet_length(source_definition_digest) = 32),
    source_state_digest bytea NOT NULL
        CHECK (octet_length(source_state_digest) = 32),
    target_definition_id bigint NOT NULL
        REFERENCES ob_job_definition (id) ON DELETE RESTRICT,
    target_job_instance_id bigint NOT NULL UNIQUE
        REFERENCES ob_job_instance (id) ON DELETE RESTRICT,
    target_job_execution_id bigint NOT NULL UNIQUE
        REFERENCES ob_job_execution (id) ON DELETE RESTRICT,
    target_parameters_digest bytea NOT NULL
        CHECK (octet_length(target_parameters_digest) = 32),
    -- Reviewed descriptor only; transformed application state is stored in the
    -- target execution context, never as a lineage payload.
    fork_transform jsonb NULL
        CHECK (fork_transform IS NULL OR jsonb_typeof(fork_transform) = 'object'),
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ob_job_fork_lineage_new_identity
        CHECK (source_job_execution_id <> target_job_execution_id)
);

-- Defense in depth: a broad default runtime table grant must not permit
-- rewriting or deleting the historical identity of an existing fork.
CREATE FUNCTION ob_reject_job_fork_lineage_mutation()
RETURNS trigger LANGUAGE plpgsql AS $fork_immutable$
BEGIN
    RAISE EXCEPTION 'immutable fork lineage cannot be modified'
        USING ERRCODE = 'integrity_constraint_violation';
END
$fork_immutable$;

CREATE TRIGGER ob_job_fork_lineage_immutable
BEFORE UPDATE OR DELETE ON ob_job_fork_lineage
FOR EACH ROW EXECUTE FUNCTION ob_reject_job_fork_lineage_mutation();

CREATE INDEX ob_job_fork_lineage_source
    ON ob_job_fork_lineage (source_job_execution_id);

UPDATE ob_schema_version
SET version = 11, installed_at = CURRENT_TIMESTAMP
WHERE singleton;
