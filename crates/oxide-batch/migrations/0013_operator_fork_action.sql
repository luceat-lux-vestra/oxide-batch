SET LOCAL search_path TO oxide_batch, pg_catalog;

DO $$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 11 THEN
        RAISE EXCEPTION 'oxide_batch schema version 11 is required before schema 12';
    END IF;
END
$$;

-- Schema 3 froze the then-known operator action vocabulary in an unnamed
-- column CHECK. M7 adds FORK as an explicit new-lineage action rather than
-- encoding it as RESTART, so replace only that closed-set constraint.
DO $$
DECLARE
    generated text;
BEGIN
    FOR generated IN
        SELECT conname
        FROM pg_catalog.pg_constraint
        WHERE conrelid = 'oxide_batch.ob_operator_request'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%LAUNCH%'
          AND pg_get_constraintdef(oid) LIKE '%RECOVER%'
    LOOP
        EXECUTE format(
            'ALTER TABLE oxide_batch.ob_operator_request DROP CONSTRAINT %I',
            generated
        );
    END LOOP;
END
$$;

ALTER TABLE ob_operator_request
    ADD CONSTRAINT ob_operator_request_action_check
        CHECK (action IN ('LAUNCH', 'RESTART', 'FORK', 'STOP', 'ABANDON', 'RECOVER'));

UPDATE ob_schema_version
SET version = 12, installed_at = CURRENT_TIMESTAMP
WHERE singleton;
