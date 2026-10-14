#!/usr/bin/env bash
set -euo pipefail

container_name="${1:?container name is required}"
repository_root="${2:?repository root is required}"
fixture_root="${3:?fixture root is required}"
temporary_root="${4:?temporary root is required}"

# Schema-10 fixture is already populated and verified by the preceding
# run-schema10-upgrade-gate.sh invocation in run-design-gate.sh.
database="oxide_batch_upgrade10"
restore_database="oxide_batch_upgrade11_restore"

upgrade11_psql() {
  docker exec -i --env PGPASSWORD=postgres "${container_name}" psql \
    --username postgres --dbname "${database}" --set ON_ERROR_STOP=on --quiet
}

# Existing schema-10 history includes legacy and transformed compatibility
# edges plus populated executions/step state; migration must leave all intact.
upgrade_started="$(date +%s)"
{
  echo 'BEGIN;'
  cat "${repository_root}/crates/oxide-batch/migrations/0012_job_fork_lineage.sql"
  echo 'COMMIT;'
} | upgrade11_psql
echo "schema 10 to 11 migration took $(( $(date +%s) - upgrade_started ))s"

upgrade11_psql <"${fixture_root}/design-gate/verify-schema11-upgrade.sql"

set +e
reapply_output="$(
  {
    echo 'BEGIN;'
    cat "${repository_root}/crates/oxide-batch/migrations/0012_job_fork_lineage.sql"
    echo 'COMMIT;'
  } | upgrade11_psql 2>&1
)"
reapply_status=$?
set -e
if [[ ${reapply_status} -eq 0 ]]; then
  echo "schema 11 migration was applied twice" >&2
  exit 1
fi
if [[ "${reapply_output}" != *"schema version 10 is required before schema 11"* ]]; then
  echo "${reapply_output}" >&2
  echo "reapplying schema 11 returned an unexpected diagnostic" >&2
  exit 1
fi

docker exec --env PGPASSWORD=postgres "${container_name}" \
  pg_dump --username postgres --dbname "${database}" --format=custom --schema=oxide_batch \
  >"${temporary_root}/schema11-populated.dump"
docker exec --env PGPASSWORD=postgres "${container_name}" \
  createdb --username postgres "${restore_database}"
docker exec -i --env PGPASSWORD=postgres "${container_name}" \
  pg_restore --username postgres --dbname "${restore_database}" --exit-on-error \
  <"${temporary_root}/schema11-populated.dump"
docker exec -i --env PGPASSWORD=postgres "${container_name}" \
  psql --username postgres --dbname "${restore_database}" --set ON_ERROR_STOP=on --quiet \
  <"${fixture_root}/design-gate/verify-schema11-restore.sql"

echo "populated schema 10 to 11 upgrade, fork evidence and restore fixture passed"
