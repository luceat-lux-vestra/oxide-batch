SET LOCAL search_path TO oxide_batch, pg_catalog;

DO $$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 8 THEN
        RAISE EXCEPTION 'oxide_batch schema version 8 is required before schema 9';
    END IF;
END
$$;

ALTER TABLE ob_job_definition
    ADD COLUMN artifact_identity varchar(128) COLLATE "C",
    ADD CONSTRAINT ob_job_definition_artifact_identity_bounds
        CHECK (
            artifact_identity IS NULL
            OR octet_length(artifact_identity) BETWEEN 1 AND 128
        );

UPDATE ob_schema_version
SET version = 9, installed_at = CURRENT_TIMESTAMP
WHERE singleton;
