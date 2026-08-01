\set ON_ERROR_STOP on

DO $verify$
DECLARE
    installed integer;
BEGIN
    SELECT version
    INTO STRICT installed
    FROM oxide_batch.ob_schema_version
    WHERE singleton = true;

    IF installed > 3 THEN
        RAISE EXCEPTION
            'OxideBatch metadata schema % is newer than supported version 3',
            installed;
    END IF;
END
$verify$;
