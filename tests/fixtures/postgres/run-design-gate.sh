#!/usr/bin/env bash
set -euo pipefail

postgres_major="${1:-18}"
case "${postgres_major}" in
  15|16|17|18) ;;
  *)
    echo "usage: $0 {15|16|17|18}" >&2
    exit 2
    ;;
esac

for required_command in cargo docker openssl; do
  if ! command -v "${required_command}" >/dev/null 2>&1; then
    echo "required command is unavailable: ${required_command}" >&2
    exit 1
  fi
done

fixture_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repository_root="$(cd "${fixture_root}/../../.." && pwd)"
temporary_root="$(mktemp -d)"
container_name="oxide-batch-pg${postgres_major}-design-${PPID}-$$"
volume_name="${container_name}-tls"
database_port="${OXIDEBATCH_DESIGN_GATE_PORT:-$((55000 + ($$ % 1000)))}"

cleanup() {
  docker rm --force "${container_name}" >/dev/null 2>&1 || true
  docker volume rm --force "${volume_name}" >/dev/null 2>&1 || true
  rm -rf "${temporary_root}"
}
trap cleanup EXIT

cat >"${temporary_root}/server.cnf" <<'EOF'
[req]
distinguished_name = subject
prompt = no
req_extensions = server_extensions

[subject]
CN = localhost

[server_extensions]
subjectAltName = @subject_alt_names
extendedKeyUsage = serverAuth

[subject_alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

openssl req \
  -x509 \
  -newkey rsa:2048 \
  -nodes \
  -days 1 \
  -subj "/CN=OxideBatch design-gate CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout "${temporary_root}/ca.key" \
  -out "${temporary_root}/ca.crt" \
  >/dev/null 2>&1
openssl req \
  -newkey rsa:2048 \
  -nodes \
  -keyout "${temporary_root}/server.key" \
  -out "${temporary_root}/server.csr" \
  -config "${temporary_root}/server.cnf" \
  >/dev/null 2>&1
openssl x509 \
  -req \
  -days 1 \
  -in "${temporary_root}/server.csr" \
  -CA "${temporary_root}/ca.crt" \
  -CAkey "${temporary_root}/ca.key" \
  -CAcreateserial \
  -out "${temporary_root}/server.crt" \
  -extfile "${temporary_root}/server.cnf" \
  -extensions server_extensions \
  >/dev/null 2>&1

docker volume create "${volume_name}" >/dev/null
docker run --rm \
  --user root \
  --mount "type=volume,source=${volume_name},target=/tls" \
  --mount "type=bind,source=${temporary_root},target=/input,readonly" \
  "postgres:${postgres_major}" \
  sh -c \
  'cp /input/ca.crt /input/server.crt /input/server.key /tls/ &&
   chown postgres:postgres /tls/ca.crt /tls/server.crt /tls/server.key &&
   chmod 600 /tls/server.key &&
   chmod 644 /tls/ca.crt /tls/server.crt'

docker run --detach --rm \
  --name "${container_name}" \
  --publish "127.0.0.1:${database_port}:5432" \
  --mount "type=volume,source=${volume_name},target=/tls,readonly" \
  --env POSTGRES_PASSWORD=postgres \
  --env POSTGRES_DB=oxide_batch_design \
  "postgres:${postgres_major}" \
  -c ssl=on \
  -c ssl_ca_file=/tls/ca.crt \
  -c ssl_cert_file=/tls/server.crt \
  -c ssl_key_file=/tls/server.key \
  >/dev/null

ready=false
for _ in $(seq 1 45); do
  if docker exec "${container_name}" \
    pg_isready --username postgres --dbname oxide_batch_design >/dev/null 2>&1
  then
    ready=true
    break
  fi
  sleep 1
done
if [[ "${ready}" != "true" ]]; then
  echo "PostgreSQL ${postgres_major} did not become ready" >&2
  docker logs "${container_name}" >&2
  exit 1
fi

docker exec -i \
  --env PGPASSWORD=postgres \
  "${container_name}" \
  psql \
  --username postgres \
  --dbname oxide_batch_design \
  <"${fixture_root}/design-gate/roles.sql"

docker exec -i \
  --env PGPASSWORD=fixture-migrator-only \
  "${container_name}" \
  psql \
  "host=localhost dbname=oxide_batch_design user=oxide_batch_migrator sslmode=verify-full sslrootcert=/tls/ca.crt" \
  <"${fixture_root}/design-gate/0001_draft_metadata.sql"

docker exec -i \
  --env PGPASSWORD=fixture-runtime-only \
  "${container_name}" \
  psql \
  "host=localhost dbname=oxide_batch_design user=oxide_batch_runtime sslmode=verify-full sslrootcert=/tls/ca.crt" \
  <"${fixture_root}/design-gate/smoke.sql"

(
  cd "${repository_root}"
  OXIDEBATCH_DESIGN_GATE_RUNTIME_URL="postgres://oxide_batch_runtime:fixture-runtime-only@localhost:${database_port}/oxide_batch_design" \
  OXIDEBATCH_DESIGN_GATE_READER_URL="postgres://oxide_batch_operator_reader:fixture-reader-only@localhost:${database_port}/oxide_batch_design" \
  OXIDEBATCH_DESIGN_GATE_TLS_ROOT="${temporary_root}/ca.crt" \
    cargo test \
      -p oxide-batch-m0-spikes \
      --test postgres_design_gate \
      -- \
      --nocapture \
      --test-threads=1
)

docker exec \
  --env PGPASSWORD=fixture-migrator-only \
  "${container_name}" \
  pg_dump \
  "host=localhost dbname=oxide_batch_design user=oxide_batch_migrator sslmode=verify-full sslrootcert=/tls/ca.crt" \
  --format=custom \
  --schema=oxide_batch \
  >"${temporary_root}/metadata.dump"

docker exec \
  --env PGPASSWORD=postgres \
  "${container_name}" \
  createdb --username postgres oxide_batch_restore
docker exec -i \
  --env PGPASSWORD=postgres \
  "${container_name}" \
  pg_restore \
  --username postgres \
  --dbname oxide_batch_restore \
  --exit-on-error \
  <"${temporary_root}/metadata.dump"
docker exec \
  --env PGPASSWORD=fixture-runtime-only \
  "${container_name}" \
  psql \
  "host=localhost dbname=oxide_batch_restore user=oxide_batch_runtime sslmode=verify-full sslrootcert=/tls/ca.crt" \
  --tuples-only \
  --command \
  "SELECT version FROM oxide_batch.ob_schema_version WHERE singleton = true" \
  | tr -d '[:space:]' \
  | grep -qx '1'

docker exec \
  --env PGPASSWORD=fixture-migrator-only \
  "${container_name}" \
  psql \
  "host=localhost dbname=oxide_batch_restore user=oxide_batch_migrator sslmode=verify-full sslrootcert=/tls/ca.crt" \
  --command "UPDATE oxide_batch.ob_schema_version SET version = 2" \
  >/dev/null

set +e
newer_schema_output="$(
  docker exec -i \
    --env PGPASSWORD=fixture-runtime-only \
    "${container_name}" \
    psql \
    "host=localhost dbname=oxide_batch_restore user=oxide_batch_runtime sslmode=verify-full sslrootcert=/tls/ca.crt" \
    2>&1 \
    <"${fixture_root}/design-gate/verify_supported_schema.sql"
)"
newer_schema_status=$?
set -e
if [[ ${newer_schema_status} -eq 0 ]]; then
  echo "newer metadata schema was not rejected" >&2
  exit 1
fi
if [[ "${newer_schema_output}" != *"newer than supported version 1"* ]]; then
  echo "${newer_schema_output}" >&2
  echo "newer-schema rejection returned an unexpected diagnostic" >&2
  exit 1
fi

echo "PostgreSQL ${postgres_major} design gate passed"
