#!/usr/bin/env bash
set -euo pipefail

container_name="${1:?container name is required}"
repository_root="${2:?repository root is required}"
fixture_root="${3:?fixture root is required}"
temporary_root="${4:?temporary root is required}"
database="oxide_batch_upgrade10"
restore_database="oxide_batch_upgrade10_restore"

upgrade10_psql() {
  docker exec -i --env PGPASSWORD=postgres "${container_name}" psql \
    --username postgres --dbname "${database}" --set ON_ERROR_STOP=on --quiet
}

docker exec --env PGPASSWORD=postgres "${container_name}" \
  createdb --username postgres "${database}"

{
  echo 'CREATE SCHEMA oxide_batch;'
  for migration in \
    0001_initial_metadata.sql \
    0002_fault_tolerance_and_flow.sql \
    0003_operations_and_local_scale.sql \
    0004_schema3_split_aggregate_patch.sql \
    0005_item_stream_component_state.sql
  do
    echo 'BEGIN;'
    cat "${repository_root}/crates/oxide-batch/migrations/${migration}"
    echo 'COMMIT;'
  done
} | upgrade10_psql

upgrade10_psql <"${fixture_root}/design-gate/schema4-seed.sql"

for migration_and_verify in \
  "0006_nested_job_linkage.sql verify-schema5-upgrade.sql" \
  "0007_scope_resolution_provenance.sql verify-schema6-upgrade.sql" \
  "0008_repeat_state.sql verify-schema7-upgrade.sql" \
  "0009_repeat_lineage.sql verify-schema8-upgrade.sql"
do
  read -r migration verifier <<<"${migration_and_verify}"
  {
    echo 'BEGIN;'
    cat "${repository_root}/crates/oxide-batch/migrations/${migration}"
    echo 'COMMIT;'
  } | upgrade10_psql
  upgrade10_psql <"${fixture_root}/design-gate/${verifier}"
done

{
  echo 'BEGIN;'
  cat "${repository_root}/crates/oxide-batch/migrations/0010_definition_registry.sql"
  echo 'COMMIT;'
} | upgrade10_psql
upgrade10_psql <"${fixture_root}/design-gate/verify-schema9-upgrade.sql"

# One real schema-9 compatibility edge must remain a legacy row after schema 10.
upgrade10_psql <<'SQL'
INSERT INTO oxide_batch.ob_definition_upgrade (
    from_definition_id,
    to_definition_id,
    upgrade_key,
    step_mapping,
    registered_at
) VALUES (
    91001,
    91002,
    'legacy-v9-edge',
    '{"preserved_step":"preserved_step"}'::jsonb,
    '2026-10-13 00:00:00+00'
);
SQL

upgrade_started="$(date +%s)"
{
  echo 'BEGIN;'
  cat "${repository_root}/crates/oxide-batch/migrations/0011_definition_upgrade_transform.sql"
  echo 'COMMIT;'
} | upgrade10_psql
echo "schema 9 to 10 migration took $(( $(date +%s) - upgrade_started ))s"

upgrade10_psql <"${fixture_root}/design-gate/verify-schema10-upgrade.sql"

set +e
reapply_output="$(
  {
    echo 'BEGIN;'
    cat "${repository_root}/crates/oxide-batch/migrations/0011_definition_upgrade_transform.sql"
    echo 'COMMIT;'
  } | upgrade10_psql 2>&1
)"
reapply_status=$?
set -e
if [[ ${reapply_status} -eq 0 ]]; then
  echo "schema 10 migration was applied twice" >&2
  exit 1
fi
if [[ "${reapply_output}" != *"schema version 9 is required before schema 10"* ]]; then
  echo "${reapply_output}" >&2
  echo "reapplying schema 10 returned an unexpected diagnostic" >&2
  exit 1
fi

docker exec --env PGPASSWORD=postgres "${container_name}" \
  pg_dump --username postgres --dbname "${database}" --format=custom --schema=oxide_batch \
  >"${temporary_root}/schema10-populated.dump"
docker exec --env PGPASSWORD=postgres "${container_name}" \
  createdb --username postgres "${restore_database}"
docker exec -i --env PGPASSWORD=postgres "${container_name}" \
  pg_restore --username postgres --dbname "${restore_database}" --exit-on-error \
  <"${temporary_root}/schema10-populated.dump"
docker exec -i --env PGPASSWORD=postgres "${container_name}" \
  psql --username postgres --dbname "${restore_database}" --set ON_ERROR_STOP=on --quiet \
  <"${fixture_root}/design-gate/verify-schema10-restore.sql"

echo "populated schema 9 to 10 upgrade and restore fixture passed"
