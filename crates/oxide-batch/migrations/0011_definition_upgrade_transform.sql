SET LOCAL search_path TO oxide_batch, pg_catalog;

DO $$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 9 THEN
        RAISE EXCEPTION 'oxide_batch schema version 9 is required before schema 10';
    END IF;
END
$$;

ALTER TABLE ob_definition_upgrade
    ADD COLUMN upgrade_revision varchar(128) COLLATE "C"
        CHECK (
            upgrade_revision IS NULL
            OR octet_length(upgrade_revision) BETWEEN 1 AND 128
        ),
    ADD COLUMN node_mapping jsonb
        CHECK (
            node_mapping IS NULL
            OR (
                jsonb_typeof(node_mapping) = 'object'
                AND pg_column_size(node_mapping) <= 65536
            )
        ),
    ADD COLUMN state_transforms jsonb
        CHECK (
            state_transforms IS NULL
            OR (
                jsonb_typeof(state_transforms) = 'array'
                AND pg_column_size(state_transforms) <= 65536
            )
        ),
    ADD CONSTRAINT ob_definition_upgrade_transform_shape
        CHECK (
            (upgrade_revision IS NULL AND node_mapping IS NULL AND state_transforms IS NULL)
            OR
            (upgrade_revision IS NOT NULL AND node_mapping IS NOT NULL AND state_transforms IS NOT NULL)
        );

UPDATE ob_schema_version
SET version = 10, installed_at = CURRENT_TIMESTAMP
WHERE singleton;
