#!/usr/bin/env bash
set -euo pipefail

image="${OXIDEBATCH_SQLSERVER_IMAGE:?pinned SQL Server image is required}"
lane="${OXIDEBATCH_SQLSERVER_LANE:?SQL Server lane is required}"
expected_version="${OXIDEBATCH_SQLSERVER_EXPECTED_PRODUCT_VERSION:?expected ProductVersion is required}"

case "$lane" in
  2022-cu28)
    expected_image='mcr.microsoft.com/mssql/server:2022-CU28-ubuntu-22.04@sha256:e10cd49443c94dbdcb7a050987db22ff4d8c87052ac53768abe1ed848d3fcef6'
    expected_version_literal='16.0.4325.4'
    ;;
  2025-cu11)
    expected_image='mcr.microsoft.com/mssql/server:2025-CU11-ubuntu-24.04@sha256:a554b8cf6ab60b92838737f03d0422c4babcd491d09cba3bec0e5e41e292b921'
    expected_version_literal='17.0.5065.9'
    ;;
  *)
    echo "unsupported SQL Server certification lane: $lane" >&2
    exit 1
    ;;
esac

test "$image" = "$expected_image"
test "$expected_version" = "$expected_version_literal"

container="oxide-batch-sqlserver-${lane//[^a-zA-Z0-9_.-]/-}"
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

openssl genrsa -out "$workdir/mssql.key" 2048 >/dev/null 2>&1
openssl req -new -sha256 \
  -key "$workdir/mssql.key" \
  -subj '/CN=localhost' \
  -out "$workdir/mssql.csr"
cat >"$workdir/mssql.ext" <<'EOF'
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:localhost,IP:127.0.0.1
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
echo "SQLSERVER_PIN lane=$lane digest=$pulled_digest expected_product_version=$expected_version"

docker run --detach \
  --name "$container" \
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
docker exec --user 0 "$container" \
  chown mssql:mssql \
  /var/opt/mssql/oxide-batch-tls/mssql.pem \
  /var/opt/mssql/oxide-batch-tls/mssql.key
docker exec --user 0 "$container" \
  chmod 400 \
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

export OXIDEBATCH_SQLSERVER_TEST_HOST=127.0.0.1
export OXIDEBATCH_SQLSERVER_TEST_PORT=1433
export OXIDEBATCH_SQLSERVER_TEST_DATABASE=oxide_batch_repository
export OXIDEBATCH_SQLSERVER_TEST_USERNAME=sa
export OXIDEBATCH_SQLSERVER_TEST_PASSWORD="$sa_password"
export OXIDEBATCH_SQLSERVER_TEST_CERT_HOSTNAME=localhost
export OXIDEBATCH_SQLSERVER_TEST_CA_PEM
OXIDEBATCH_SQLSERVER_TEST_CA_PEM="$(cat "$workdir/ca.pem")"

cargo test -p oxide-batch --features sqlserver \
  --test sqlserver_repository_core \
  -- --nocapture --test-threads=1
