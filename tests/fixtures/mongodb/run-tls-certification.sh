#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 4 ]; then
  echo "usage: $0 <mongo-image> <lane> <expected-version> <database>" >&2
  exit 2
fi

image="$1"
lane="$2"
expected_version="$3"
database="$4"
port=27019
container="oxide-mongodb-tls-${lane}"
volume="oxide-mongodb-tls-${lane}"
temporary_root="$(mktemp -d)"

cleanup() {
  docker logs "$container" >/dev/null 2>&1 || true
  docker rm --force "$container" >/dev/null 2>&1 || true
  docker volume rm --force "$volume" >/dev/null 2>&1 || true
  rm -rf "$temporary_root"
}
trap cleanup EXIT

cat >"$temporary_root/server.cnf" <<'EOF'
[req]
distinguished_name = subject
prompt = no

[subject]
CN = 127.0.0.1

[server_extensions]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names

[alt_names]
IP.1 = 127.0.0.1
EOF

openssl genrsa -out "$temporary_root/ca.key" 2048 >/dev/null 2>&1
openssl req -x509 -new -sha256 -days 1 \
  -key "$temporary_root/ca.key" \
  -subj "/CN=OxideBatch MongoDB TLS Test CA" \
  -out "$temporary_root/ca.crt" \
  >/dev/null 2>&1

openssl genrsa -out "$temporary_root/server.key" 2048 >/dev/null 2>&1
openssl req -new -sha256 \
  -key "$temporary_root/server.key" \
  -config "$temporary_root/server.cnf" \
  -out "$temporary_root/server.csr" \
  >/dev/null 2>&1
openssl x509 -req -sha256 -days 1 \
  -in "$temporary_root/server.csr" \
  -CA "$temporary_root/ca.crt" \
  -CAkey "$temporary_root/ca.key" \
  -CAcreateserial \
  -extfile "$temporary_root/server.cnf" \
  -extensions server_extensions \
  -out "$temporary_root/server.crt" \
  >/dev/null 2>&1
cat "$temporary_root/server.crt" "$temporary_root/server.key" >"$temporary_root/server.pem"

openssl genrsa -out "$temporary_root/wrong-ca.key" 2048 >/dev/null 2>&1
openssl req -x509 -new -sha256 -days 1 \
  -key "$temporary_root/wrong-ca.key" \
  -subj "/CN=OxideBatch MongoDB Wrong CA" \
  -out "$temporary_root/wrong-ca.crt" \
  >/dev/null 2>&1

docker volume create "$volume" >/dev/null
docker run --rm \
  --user root \
  --mount "type=volume,source=$volume,target=/tls" \
  --mount "type=bind,source=$temporary_root,target=/input,readonly" \
  --entrypoint /bin/bash \
  "$image" \
  -lc '
    set -euo pipefail
    cp /input/ca.crt /input/server.pem /tls/
    chown mongodb:mongodb /tls/ca.crt /tls/server.pem
    chmod 644 /tls/ca.crt
    chmod 600 /tls/server.pem
  '

docker run --detach --rm \
  --name "$container" \
  --publish "127.0.0.1:${port}:27017" \
  --mount "type=volume,source=$volume,target=/tls,readonly" \
  "$image" \
  --replSet rs0 \
  --bind_ip_all \
  --tlsMode requireTLS \
  --tlsCertificateKeyFile /tls/server.pem \
  --tlsCAFile /tls/ca.crt \
  --tlsAllowConnectionsWithoutCertificates \
  >/dev/null

ready=false
for _ in $(seq 1 60); do
  if docker exec "$container" mongosh --quiet \
    --host 127.0.0.1 \
    --tls \
    --tlsCAFile /tls/ca.crt \
    --eval 'db.adminCommand({ ping: 1 }).ok' 2>/dev/null | grep -qx '1'; then
    ready=true
    break
  fi
  sleep 1
done
if [ "$ready" != "true" ]; then
  echo "::error::TLS MongoDB fixture did not become reachable"
  docker logs "$container"
  exit 1
fi

docker exec "$container" mongosh --quiet \
  --host 127.0.0.1 \
  --tls \
  --tlsCAFile /tls/ca.crt \
  --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"127.0.0.1:27017"}]})' \
  >/dev/null

primary=false
for _ in $(seq 1 60); do
  if docker exec "$container" mongosh --quiet \
    --host 127.0.0.1 \
    --tls \
    --tlsCAFile /tls/ca.crt \
    --eval 'db.hello().isWritablePrimary' 2>/dev/null | grep -qx 'true'; then
    primary=true
    break
  fi
  sleep 1
done
if [ "$primary" != "true" ]; then
  echo "::error::TLS MongoDB fixture did not elect a writable primary"
  docker logs "$container"
  exit 1
fi

actual_version="$(
  docker exec "$container" mongosh --quiet \
    --host 127.0.0.1 \
    --tls \
    --tlsCAFile /tls/ca.crt \
    --eval 'db.version()'
)"
if [ "$actual_version" != "$expected_version" ]; then
  echo "::error::TLS MongoDB fixture expected version $expected_version but observed $actual_version"
  exit 1
fi

valid_uri="mongodb://127.0.0.1:${port}/?directConnection=true&replicaSet=rs0&tls=true&tlsCAFile=${temporary_root}/ca.crt"
wrong_ca_uri="mongodb://127.0.0.1:${port}/?directConnection=true&replicaSet=rs0&tls=true&tlsCAFile=${temporary_root}/wrong-ca.crt"
wrong_identity_uri="mongodb://localhost:${port}/?directConnection=true&replicaSet=rs0&tls=true&tlsCAFile=${temporary_root}/ca.crt"
plaintext_uri="mongodb://127.0.0.1:${port}/?directConnection=true&replicaSet=rs0"

OXIDEBATCH_MONGODB_TLS_VALID_URI="$valid_uri" \
OXIDEBATCH_MONGODB_TLS_WRONG_CA_URI="$wrong_ca_uri" \
OXIDEBATCH_MONGODB_TLS_WRONG_IDENTITY_URI="$wrong_identity_uri" \
OXIDEBATCH_MONGODB_TLS_PLAINTEXT_URI="$plaintext_uri" \
OXIDEBATCH_MONGODB_TLS_TEST_DATABASE="$database" \
cargo test -p oxide-batch --features mongodb \
  --test mongodb_repository_core \
  mongodb_tls_validates_ca_identity_and_rejects_plaintext \
  -- --exact --nocapture --test-threads=1

echo "MongoDB TLS certification: PASS (${lane}, ${expected_version})"
