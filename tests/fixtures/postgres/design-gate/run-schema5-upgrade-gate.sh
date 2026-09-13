#!/usr/bin/env bash
set -euo pipefail

container_name="${1:?container name is required}"
repository_root="${2:?repository root is required}"
fixture_root="${3:?fixture root is required}"
database="oxide_batch_upgrade5"
restore_database="oxide_batch_upgrade5_restore"
temporary_root="${4:?temporary root is required}"

upgrade5_psql() {
  docker exec -i \
    --env PGPASSWORD=postgres \
    "${container_name}" \
    psql \
    --username postgres \
    --dbname "${database}" \
    --set ON_ERROR_STOP=on \
    --quiet
}

docker exec \
  --env PGPASSWORD=postgres \
  "${container_name}" \
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
} | upgrade5_psql

upgrade5_psql <"${fixture_root}/design-gate/schema4-seed.sql"

upgrade_started="$(date +%s)"
{
  echo 'BEGIN;'
  cat "${repository_root}/crates/oxide-batch/migrations/0006_nested_job_linkage.sql"
  echo 'COMMIT;'
} | upgrade5_psql
echo "schema 4 to 5 migration took $(($(date +%s) - upgrade_started))s"

upgrade5_psql <"${fixture_root}/design-gate/verify-schema5-upgrade.sql"

set +e
reapply_output="$(
  {
    echo 'BEGIN;'
    cat "${repository_root}/crates/oxide-batch/migrations/0006_nested_job_linkage.sql"
    echo 'COMMIT;'
  } | upgrade5_psql 2>&1
)"
reapply_status=$?
set -e
if [[ ${reapply_status} -eq 0 ]]; then
  echo "schema 5 migration was applied twice" >&2
  exit 1
fi
if [[ "${reapply_output}" != *"schema version 4 is required before schema 5"* ]]; then
  echo "${reapply_output}" >&2
  echo "reapplying schema 5 returned an unexpected diagnostic" >&2
  exit 1
fi

docker exec \
  --env PGPASSWORD=postgres \
  "${container_name}" \
  pg_dump \
  --username postgres \
  --dbname "${database}" \
  --format=custom \
  --schema=oxide_batch \
  >"${temporary_root}/schema5-populated.dump"

docker exec \
  --env PGPASSWORD=postgres \
  "${container_name}" \
  createdb --username postgres "${restore_database}"
docker exec -i \
  --env PGPASSWORD=postgres \
  "${container_name}" \
  pg_restore \
  --username postgres \
  --dbname "${restore_database}" \
  --exit-on-error \
  <"${temporary_root}/schema5-populated.dump"

docker exec -i \
  --env PGPASSWORD=postgres \
  "${container_name}" \
  psql \
  --username postgres \
  --dbname "${restore_database}" \
  --set ON_ERROR_STOP=on \
  --quiet \
  <"${fixture_root}/design-gate/verify-schema5-restore.sql"

echo "populated schema 4 to 5 upgrade and restore fixture passed"
