#!/usr/bin/env bash
set -euo pipefail

image="${OXIDEBATCH_SQLSERVER_IMAGE:?pinned SQL Server image is required}"
lane="${OXIDEBATCH_SQLSERVER_LANE:?SQL Server lane is required}"
expected_version="${OXIDEBATCH_SQLSERVER_EXPECTED_PRODUCT_VERSION:?expected ProductVersion is required}"
encryption="${OXIDEBATCH_SQLSERVER_ENCRYPTION:?SQL Server encryption mode is required}"

case "$lane" in
  2022-cu28)
    expected_image='mcr.microsoft.com/mssql/server:2022-CU28-ubuntu-22.04@sha256:4525e3efac9f3e241cb003bb2ff2a82818b502f37e12049d788b1a9fa7e13b7f'
    expected_version_literal='16.0.4325.4'
    expected_encryption='required'
    ;;
  2025-cu11)
    expected_image='mcr.microsoft.com/mssql/server:2025-CU11-ubuntu-24.04@sha256:05644dad1266ca3d445d1e8f6d2ebb9a6a03df9a39376e0f7b48917356dca9d9'
    expected_version_literal='17.0.5065.9'
    expected_encryption='strict'
    ;;
  *)
    echo "unsupported SQL Server certification lane: $lane" >&2
    exit 1
    ;;
esac

test "$image" = "$expected_image"
test "$expected_version" = "$expected_version_literal"
test "$encryption" = "$expected_encryption"

container="oxide-batch-sqlserver-${lane//[^a-zA-Z0-9_.-]/-}"
server_name="sqlserver-${lane}.oxide-batch.test"
workdir="$(mktemp -d)"
sa_password='OxideBatch!674SqlServer'

cleanup() {
  status=$?
  trap - EXIT
  if [ "$status" -ne 0 ] && docker inspect "$container" >/dev/null 2>&1; then
    echo "=== SQL Server container log tail ($lane) ===" >&2
    docker logs "$container" 2>&1 | tail -n 240 >&2 || true
  fi
  docker rm -f "$container" >/dev/null 2>&1 || true
  rm -rf "$workdir"
  exit "$status"
}
trap cleanup EXIT

openssl genrsa -out "$workdir/ca.key" 2048 >/dev/null 2>&1
openssl req -x509 -new -sha256 -days 2 \
  -key "$workdir/ca.key" \
  -subj '/CN=OxideBatch SQL Server CI Root' \
  -out "$workdir/ca.pem"

openssl genrsa -out "$workdir/wrong-ca.key" 2048 >/dev/null 2>&1
openssl req -x509 -new -sha256 -days 2 \
  -key "$workdir/wrong-ca.key" \
  -subj '/CN=OxideBatch SQL Server Wrong CI Root' \
  -out "$workdir/wrong-ca.pem"

openssl genrsa -out "$workdir/mssql.key" 2048 >/dev/null 2>&1
openssl req -new -sha256 \
  -key "$workdir/mssql.key" \
  -subj "/CN=$server_name" \
  -out "$workdir/mssql.csr"
cat >"$workdir/mssql.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:$server_name
EOF
openssl x509 -req -sha256 -days 2 \
  -in "$workdir/mssql.csr" \
  -CA "$workdir/ca.pem" \
  -CAkey "$workdir/ca.key" \
  -CAcreateserial \
  -extfile "$workdir/mssql.ext" \
  -out "$workdir/mssql.pem" >/dev/null 2>&1

docker pull "$image"
pulled_digest="$(docker image inspect "$image" --format '{{join .RepoDigests "\n"}}' | sed -n 's/.*@\(sha256:[0-9a-f]\{64\}\)$/\1/p' | head -n 1)"
expected_digest="${image##*@}"
test "$pulled_digest" = "$expected_digest"
echo "SQLSERVER_PIN lane=$lane digest=$pulled_digest expected_product_version=$expected_version encryption=$encryption"

docker run --detach \
  --name "$container" \
  --hostname "$server_name" \
  --env ACCEPT_EULA=Y \
  --env MSSQL_PID=Developer \
  --env MSSQL_SA_PASSWORD="$sa_password" \
  --publish 1433:1433 \
  "$image" >/dev/null

sqlcmd_path=''
for candidate in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do
  if docker exec "$container" test -x "$candidate" >/dev/null 2>&1; then
    sqlcmd_path="$candidate"
    break
  fi
done
if [ -z "$sqlcmd_path" ]; then
  echo "sqlcmd was not found in SQL Server image $image" >&2
  exit 1
fi

bcp_path=''
for candidate in /opt/mssql-tools18/bin/bcp /opt/mssql-tools/bin/bcp; do
  if docker exec "$container" test -x "$candidate" >/dev/null 2>&1; then
    bcp_path="$candidate"
    break
  fi
done
if [ -z "$bcp_path" ]; then
  echo "bcp was not found in SQL Server image $image" >&2
  exit 1
fi

wait_for_sqlserver() {
  local attempt
  for attempt in $(seq 1 90); do
    if docker exec "$container" "$sqlcmd_path" \
      -S localhost -U sa -P "$sa_password" -C -b -Q 'SELECT 1' \
      >/dev/null 2>&1
    then
      return 0
    fi
    if [ "$(docker inspect "$container" --format '{{.State.Running}}')" != 'true' ]; then
      echo "SQL Server container stopped during startup" >&2
      return 1
    fi
    sleep 2
  done
  echo "SQL Server did not become ready within the certification startup bound" >&2
  return 1
}

wait_for_sqlserver

docker exec --user 0 "$container" mkdir -p /var/opt/mssql/oxide-batch-tls
docker cp "$workdir/mssql.pem" "$container:/var/opt/mssql/oxide-batch-tls/mssql.pem"
docker cp "$workdir/mssql.key" "$container:/var/opt/mssql/oxide-batch-tls/mssql.key"
docker cp "$workdir/ca.pem" "$container:/usr/local/share/ca-certificates/oxide-batch-sqlserver.crt"
docker exec --user 0 "$container" update-ca-certificates >/dev/null
docker exec --user 0 "$container" \
  chown mssql:mssql \
  /var/opt/mssql/oxide-batch-tls/mssql.pem \
  /var/opt/mssql/oxide-batch-tls/mssql.key
docker exec --user 0 "$container" \
  chmod 600 \
  /var/opt/mssql/oxide-batch-tls/mssql.pem \
  /var/opt/mssql/oxide-batch-tls/mssql.key

docker exec --user 0 "$container" /opt/mssql/bin/mssql-conf set \
  network.tlscert /var/opt/mssql/oxide-batch-tls/mssql.pem
docker exec --user 0 "$container" /opt/mssql/bin/mssql-conf set \
  network.tlskey /var/opt/mssql/oxide-batch-tls/mssql.key
docker exec --user 0 "$container" /opt/mssql/bin/mssql-conf set \
  network.tlsprotocols 1.2
docker exec --user 0 "$container" /opt/mssql/bin/mssql-conf set \
  network.forceencryption 1

docker restart "$container" >/dev/null
wait_for_sqlserver

docker exec "$container" "$sqlcmd_path" \
  -S localhost -U sa -P "$sa_password" -C -b \
  -Q "IF DB_ID(N'oxide_batch_repository') IS NULL CREATE DATABASE oxide_batch_repository;" \
  >/dev/null

migrator_username='oxide_batch_migrator'
migrator_password='OxideBatch!674Migrator'
runtime_username='oxide_batch_runtime'
runtime_password='OxideBatch!674Runtime'

docker exec "$container" "$sqlcmd_path" \
  -S localhost -U sa -P "$sa_password" -C -b \
  -Q "
IF SUSER_ID(N'$migrator_username') IS NULL
  CREATE LOGIN [$migrator_username] WITH PASSWORD=N'$migrator_password', CHECK_POLICY=OFF;
IF SUSER_ID(N'$runtime_username') IS NULL
  CREATE LOGIN [$runtime_username] WITH PASSWORD=N'$runtime_password', CHECK_POLICY=OFF;
USE [oxide_batch_repository];
IF USER_ID(N'$migrator_username') IS NULL
  CREATE USER [$migrator_username] FOR LOGIN [$migrator_username];
IF USER_ID(N'$runtime_username') IS NULL
  CREATE USER [$runtime_username] FOR LOGIN [$runtime_username];
GRANT CREATE TABLE TO [$migrator_username];
GRANT ALTER ON SCHEMA::dbo TO [$migrator_username];
GRANT REFERENCES ON SCHEMA::dbo TO [$migrator_username];
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dbo TO [$migrator_username];
" >/dev/null

export OXIDEBATCH_SQLSERVER_TEST_HOST=127.0.0.1
export OXIDEBATCH_SQLSERVER_TEST_PORT=1433
export OXIDEBATCH_SQLSERVER_TEST_DATABASE=oxide_batch_repository
export OXIDEBATCH_SQLSERVER_TEST_USERNAME="$runtime_username"
export OXIDEBATCH_SQLSERVER_TEST_PASSWORD="$runtime_password"
export OXIDEBATCH_SQLSERVER_MIGRATOR_USERNAME="$migrator_username"
export OXIDEBATCH_SQLSERVER_MIGRATOR_PASSWORD="$migrator_password"
export OXIDEBATCH_SQLSERVER_TEST_CERT_HOSTNAME="$server_name"
export OXIDEBATCH_SQLSERVER_TEST_CA_PEM
OXIDEBATCH_SQLSERVER_TEST_CA_PEM="$(cat "$workdir/ca.pem")"
export OXIDEBATCH_SQLSERVER_TEST_WRONG_CA_PEM
OXIDEBATCH_SQLSERVER_TEST_WRONG_CA_PEM="$(cat "$workdir/wrong-ca.pem")"
if [ "$encryption" = strict ]; then
  export OXIDEBATCH_SQLSERVER_TEST_TDS80_STRICT=1
else
  unset OXIDEBATCH_SQLSERVER_TEST_TDS80_STRICT || true
fi

# Bootstrap the immutable schema through the dedicated migrator principal before
# granting runtime DML. This proves the migrator can operate without sa/db_owner.
cargo test -p oxide-batch --features sqlserver \
  --test sqlserver_repository_core \
  migration_is_idempotent_for_sql_server \
  -- --exact --nocapture --test-threads=1

docker exec "$container" "$sqlcmd_path" \
  -S localhost -U sa -P "$sa_password" -C -b \
  -Q "
USE [oxide_batch_repository];
GRANT SELECT ON SCHEMA::dbo TO [$runtime_username];
GRANT INSERT, UPDATE, DELETE ON OBJECT::dbo.ob_job_instance TO [$runtime_username];
GRANT INSERT, UPDATE, DELETE ON OBJECT::dbo.ob_job_execution TO [$runtime_username];
GRANT INSERT, UPDATE, DELETE ON OBJECT::dbo.ob_step_execution TO [$runtime_username];
GRANT INSERT, UPDATE, DELETE ON OBJECT::dbo.ob_core_job_definition TO [$runtime_username];
GRANT INSERT, UPDATE, DELETE ON OBJECT::dbo.ob_flow_decision TO [$runtime_username];
" >/dev/null

cargo test -p oxide-batch --features sqlserver \
  --test sqlserver_repository_core \
  -- --nocapture --test-threads=1

# Final runnable-core operational gate: take a table-level logical backup of a
# deliberately restartable durable fixture, reconstruct the immutable schema in
# a clean database, restore the row archive with constraints enabled, and prove
# the restored FAILED attempt restarts to completion through the repository API.
source_database="$OXIDEBATCH_SQLSERVER_TEST_DATABASE"
restore_database='oxide_batch_repository_restore'
backup_dir="/tmp/oxide-batch-logical-backup-$lane"

docker exec "$container" "$sqlcmd_path" \
  -S localhost -U sa -P "$sa_password" -C -b -d "$source_database" \
  -Q "
UPDATE dbo.ob_flow_decision SET reused_decision_id = NULL;
DELETE FROM dbo.ob_flow_decision;
DELETE FROM dbo.ob_step_execution;
DELETE FROM dbo.ob_job_execution;
DELETE FROM dbo.ob_job_instance;
DELETE FROM dbo.ob_core_job_definition;
" >/dev/null

OXIDEBATCH_SQLSERVER_BACKUP_RESTORE_MODE=seed \
cargo test -p oxide-batch --features sqlserver \
  --test sqlserver_repository_core \
  logical_backup_source_contains_restartable_state \
  -- --exact --nocapture --test-threads=1

docker exec "$container" sh -c "rm -rf '$backup_dir' && mkdir -p '$backup_dir'"
backup_tables=(
  ob_core_job_definition
  ob_job_instance
  ob_job_execution
  ob_step_execution
  ob_flow_decision
)
for table in "${backup_tables[@]}"; do
  docker exec "$container" "$bcp_path" "dbo.$table" out "$backup_dir/$table.bcp" \
    -n -S "$server_name" -d "$source_database" \
    -U "$migrator_username" -P "$migrator_password"
done
for table in ob_core_job_definition ob_job_instance ob_job_execution ob_step_execution; do
  docker exec "$container" test -s "$backup_dir/$table.bcp"
done

docker exec "$container" "$sqlcmd_path" \
  -S localhost -U sa -P "$sa_password" -C -b \
  -Q "
IF DB_ID(N'$restore_database') IS NOT NULL
BEGIN
  ALTER DATABASE [$restore_database] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE [$restore_database];
END;
CREATE DATABASE [$restore_database];
USE [$restore_database];
CREATE USER [$migrator_username] FOR LOGIN [$migrator_username];
CREATE USER [$runtime_username] FOR LOGIN [$runtime_username];
GRANT CREATE TABLE TO [$migrator_username];
GRANT ALTER ON SCHEMA::dbo TO [$migrator_username];
GRANT REFERENCES ON SCHEMA::dbo TO [$migrator_username];
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::dbo TO [$migrator_username];
" >/dev/null

export OXIDEBATCH_SQLSERVER_TEST_DATABASE="$restore_database"
cargo test -p oxide-batch --features sqlserver \
  --test sqlserver_repository_core \
  migration_is_idempotent_for_sql_server \
  -- --exact --nocapture --test-threads=1

docker exec "$container" "$sqlcmd_path" \
  -S localhost -U sa -P "$sa_password" -C -b -d "$restore_database" \
  -Q "
GRANT SELECT ON SCHEMA::dbo TO [$runtime_username];
GRANT INSERT, UPDATE, DELETE ON OBJECT::dbo.ob_job_instance TO [$runtime_username];
GRANT INSERT, UPDATE, DELETE ON OBJECT::dbo.ob_job_execution TO [$runtime_username];
GRANT INSERT, UPDATE, DELETE ON OBJECT::dbo.ob_step_execution TO [$runtime_username];
GRANT INSERT, UPDATE, DELETE ON OBJECT::dbo.ob_core_job_definition TO [$runtime_username];
GRANT INSERT, UPDATE, DELETE ON OBJECT::dbo.ob_flow_decision TO [$runtime_username];
" >/dev/null

for table in "${backup_tables[@]}"; do
  if docker exec "$container" test -s "$backup_dir/$table.bcp"; then
    docker exec "$container" "$bcp_path" "dbo.$table" in "$backup_dir/$table.bcp" \
      -n -E -k -h "CHECK_CONSTRAINTS" -S "$server_name" -d "$restore_database" \
      -U "$migrator_username" -P "$migrator_password"
  fi
done

OXIDEBATCH_SQLSERVER_BACKUP_RESTORE_MODE=restore \
cargo test -p oxide-batch --features sqlserver \
  --test sqlserver_repository_core \
  logical_backup_restores_restartable_state \
  -- --exact --nocapture --test-threads=1

echo "SQLSERVER_LOGICAL_BACKUP_RESTORE lane=$lane source=$source_database restore=$restore_database method=bcp-native constraint_check=enabled"

export OXIDEBATCH_SQLSERVER_TEST_DATABASE="$source_database"
docker exec "$container" "$sqlcmd_path" \
  -S localhost -U sa -P "$sa_password" -C -b \
  -Q "
ALTER DATABASE [$restore_database] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE [$restore_database];
" >/dev/null
