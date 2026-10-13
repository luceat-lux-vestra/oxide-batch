\set ON_ERROR_STOP on
SET search_path TO oxide_batch, pg_catalog;

DO $verify$
BEGIN
    IF (SELECT version FROM ob_schema_version WHERE singleton) <> 10 THEN
        RAISE EXCEPTION 'schema10 upgrade did not publish version 10';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM ob_definition_upgrade
        WHERE from_definition_id = 91001
          AND to_definition_id = 91002
          AND upgrade_key = 'legacy-v9-edge'
          AND step_mapping = '{"preserved_step":"preserved_step"}'::jsonb
          AND upgrade_revision IS NULL
          AND node_mapping IS NULL
          AND state_transforms IS NULL
    ) THEN
        RAISE EXCEPTION 'schema10 reinterpreted the legacy schema9 compatibility edge';
    END IF;
END
$verify$;

INSERT INTO ob_definition_upgrade (
    from_definition_id,
    to_definition_id,
    upgrade_key,
    step_mapping,
    upgrade_revision,
    node_mapping,
    state_transforms,
    registered_at
) VALUES (
    91002,
    91001,
    'transformed-v10-edge',
    '{}'::jsonb,
    'edge-v1',
    '{"preserved_step":"preserved_step"}'::jsonb,
    '[{"source_node":"preserved_step","target_node":"preserved_step","kind":"checkpoint","state_id":"reader-checkpoint","source_schema":"fixture-checkpoint","source_version":1,"target_schema":"fixture-checkpoint","target_version":2,"revision":"checkpoint-v1-to-v2"}]'::jsonb,
    '2026-10-13 00:00:01+00'
);
