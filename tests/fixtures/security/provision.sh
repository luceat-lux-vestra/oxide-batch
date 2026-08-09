#!/usr/bin/env bash
# Provisions the M5 security campaign's fixture and runs the campaign.
#
# The campaign needs more than a database. It needs a server that really speaks
# TLS, a private authority that really signed that server's certificate, a
# second authority that signed nothing, a name the certificate does not carry,
# and a reachable endpoint that offers no TLS at all. None of that can be
# supplied by a connection string, and none of it can be arranged by the
# `services:` block of a CI job, because a service container starts before any
# step has run and therefore before any certificate exists.
#
# So the fixture is built here: two containers, one with TLS configured against
# a certificate generated moments earlier and one without TLS at all, plus the
# authorities the campaign trusts and does not trust. The campaign is then run
# against them by the runner, which is what decides whether it passed.
#
# The certificate deliberately carries only the DNS name. Reaching the same
# server by its address is then a host-name mismatch and nothing else, which is
# what makes that refusal about host-name verification rather than about
# reachability.
#
# Everything here is disposable and bound to loopback. The certificates live for
# a day, the containers and volumes are removed on exit, and no credential is
# reused.
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
container_prefix="oxide-batch-pg${postgres_major}-security-${PPID}-$$"
tls_container="${container_prefix}-tls"
plain_container="${container_prefix}-plain"
volume_name="${tls_container}-material"
tls_port="${OXIDEBATCH_SECURITY_TLS_PORT:-$((56000 + ($$ % 1000)))}"
plain_port="${OXIDEBATCH_SECURITY_PLAINTEXT_PORT:-$((57000 + ($$ % 1000)))}"

cleanup() {
  docker rm --force "${tls_container}" >/dev/null 2>&1 || true
  docker rm --force "${plain_container}" >/dev/null 2>&1 || true
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
# The address is deliberately absent. Reaching this server at 127.0.0.1 must be
# a host-name mismatch, which is the only way that refusal is about the name.
subjectAltName = DNS:localhost
extendedKeyUsage = serverAuth
EOF

openssl req \
  -x509 \
  -newkey rsa:2048 \
  -nodes \
  -days 1 \
  -subj "/CN=OxideBatch M5 security campaign CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout "${temporary_root}/ca.key" \
  -out "${temporary_root}/ca.crt" \
  >/dev/null 2>&1

# An authority that signs nothing this server presents. The campaign trusts it
# for one attempt, which must then fail on the issuer.
openssl req \
  -x509 \
  -newkey rsa:2048 \
  -nodes \
  -days 1 \
  -subj "/CN=OxideBatch M5 unrelated CA" \
  -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout "${temporary_root}/untrusted-ca.key" \
  -out "${temporary_root}/untrusted-ca.crt" \
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
  --name "${tls_container}" \
  --publish "127.0.0.1:${tls_port}:5432" \
  --mount "type=volume,source=${volume_name},target=/tls,readonly" \
  --env POSTGRES_PASSWORD=postgres \
  --env POSTGRES_DB=oxide_batch_security \
  "postgres:${postgres_major}" \
  -c ssl=on \
  -c ssl_ca_file=/tls/ca.crt \
  -c ssl_cert_file=/tls/server.crt \
  -c ssl_key_file=/tls/server.key \
  >/dev/null

# The endpoint that offers no TLS. Without it the campaign can only show that a
# bad certificate is refused, which a client that silently continued
# unencrypted whenever TLS was unavailable would also show.
docker run --detach --rm \
  --name "${plain_container}" \
  --publish "127.0.0.1:${plain_port}:5432" \
  --env POSTGRES_PASSWORD=postgres \
  --env POSTGRES_DB=oxide_batch_security \
  "postgres:${postgres_major}" \
  -c ssl=off \
  >/dev/null

await_ready() {
  local container="$1"
  for _ in $(seq 1 45); do
    # The official image starts a socket-only temporary server while
    # initializing an empty data directory. Probe TCP so only the final server
    # can become ready.
    if docker exec "${container}" \
      pg_isready \
        --host 127.0.0.1 \
        --port 5432 \
        --username postgres \
        --dbname oxide_batch_security \
        >/dev/null 2>&1
    then
      return 0
    fi
    sleep 1
  done
  echo "PostgreSQL ${postgres_major} did not become ready in ${container}" >&2
  docker logs "${container}" >&2
  return 1
}

await_ready "${tls_container}"
await_ready "${plain_container}"

cd "${repository_root}"

# The campaign creates every database it reports on, so the administrative
# connection points at the maintenance database rather than at one of them.
export OXIDEBATCH_POSTGRES_ADMIN_TEST_URL="postgres://postgres:postgres@localhost:${tls_port}/postgres"
export OXIDEBATCH_SECURITY_PLAINTEXT_TEST_URL="postgres://postgres:postgres@localhost:${plain_port}/postgres"
export OXIDEBATCH_SECURITY_TLS_HOST="localhost"
export OXIDEBATCH_SECURITY_TLS_MISMATCH_HOST="127.0.0.1"
export OXIDEBATCH_SECURITY_TLS_CA="${temporary_root}/ca.crt"
export OXIDEBATCH_SECURITY_TLS_UNTRUSTED_CA="${temporary_root}/untrusted-ca.crt"
export OXIDEBATCH_CAMPAIGN_MATRIX="${OXIDEBATCH_CAMPAIGN_MATRIX:-postgres-${postgres_major}}"
export OXIDEBATCH_CAMPAIGN_DIR="${OXIDEBATCH_CAMPAIGN_DIR:-target/m5-campaigns}"

cargo run --package oxide-batch-xtask -- security
