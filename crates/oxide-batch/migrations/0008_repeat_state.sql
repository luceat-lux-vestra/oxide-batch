SET LOCAL search_path TO oxide_batch, pg_catalog;

DO $$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 6 THEN
        RAISE EXCEPTION 'oxide_batch schema version 6 is required before schema 7';
    END IF;
END
$$;

CREATE TABLE ob_repeat_execution (
    step_execution_id bigint NOT NULL
        REFERENCES ob_step_execution (id) ON DELETE CASCADE,
    repeat_id varchar(128) COLLATE "C" NOT NULL
        CHECK (octet_length(repeat_id) BETWEEN 1 AND 128),
    ordinal bigint NOT NULL
        CHECK (ordinal BETWEEN 0 AND 4294967295),
    state_format smallint NOT NULL CHECK (state_format > 0),
    state_schema varchar(128) COLLATE "C" NOT NULL
        CHECK (octet_length(state_schema) BETWEEN 1 AND 128),
    state_schema_version integer NOT NULL CHECK (state_schema_version > 0),
    state_payload jsonb NOT NULL
        CHECK (jsonb_typeof(state_payload) = 'object')
        CHECK (pg_column_size(state_payload) <= 1048576),
    state_checksum bytea NOT NULL CHECK (octet_length(state_checksum) = 32),
    decision varchar(16) COLLATE "C" NOT NULL
        CHECK (decision IN ('continue', 'complete')),
    plan_fingerprint bytea NOT NULL CHECK (octet_length(plan_fingerprint) = 32),
    updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (step_execution_id, repeat_id)
);

CREATE INDEX ob_repeat_execution_lookup
    ON ob_repeat_execution (repeat_id, step_execution_id);

UPDATE ob_schema_version
    SET version = 7, installed_at = CURRENT_TIMESTAMP
    WHERE singleton;
