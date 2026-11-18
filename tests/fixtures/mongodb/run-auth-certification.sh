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
port=27018
container="oxide-mongodb-auth-${lane}"
root_user="fixture_root"
migrator_user="oxide_migrator"
runtime_user="oxide_runtime"

root_password="$(openssl rand -hex 24)"
migrator_password="$(openssl rand -hex 24)"
runtime_password="$(openssl rand -hex 24)"
key_material="$(openssl rand -base64 756 | tr -d '\n')"

cleanup() {
  docker logs "$container" >/dev/null 2>&1 || true
  docker rm --force "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --detach --rm \
  --name "$container" \
  --publish "${port}:27017" \
  --env OXIDEBATCH_MONGO_KEYFILE="$key_material" \
  --entrypoint /bin/bash \
  "$image" \
  -lc '
    set -euo pipefail
    umask 077
    printf "%s" "$OXIDEBATCH_MONGO_KEYFILE" > /tmp/oxide-mongo-keyfile
    chown mongodb:mongodb /tmp/oxide-mongo-keyfile
    exec /usr/local/bin/docker-entrypoint.sh mongod \
      --replSet rs0 \
      --bind_ip_all \
      --auth \
      --keyFile /tmp/oxide-mongo-keyfile
  ' >/dev/null

ready=false
for _ in $(seq 1 60); do
  if docker exec "$container" mongosh --quiet \
    --eval 'db.adminCommand({ ping: 1 }).ok' 2>/dev/null | grep -qx '1'; then
    ready=true
    break
  fi
  sleep 1
done
if [ "$ready" != "true" ]; then
  echo "::error::authenticated MongoDB fixture did not become reachable"
  docker logs "$container"
  exit 1
fi

docker exec "$container" mongosh --quiet --eval \
  'rs.initiate({_id:"rs0",members:[{_id:0,host:"127.0.0.1:27017"}]})' >/dev/null

primary=false
for _ in $(seq 1 60); do
  if docker exec "$container" mongosh --quiet \
    --eval 'db.hello().isWritablePrimary' 2>/dev/null | grep -qx 'true'; then
    primary=true
    break
  fi
  sleep 1
done
if [ "$primary" != "true" ]; then
  echo "::error::authenticated MongoDB fixture did not elect a writable primary"
  docker logs "$container"
  exit 1
fi

docker exec "$container" mongosh --quiet admin --eval \
  "db.createUser({user:'${root_user}',pwd:'${root_password}',roles:[{role:'root',db:'admin'}]})" \
  >/dev/null

actual_version="$(
  docker exec "$container" mongosh --quiet \
    --username "$root_user" \
    --password "$root_password" \
    --authenticationDatabase admin \
    admin \
    --eval 'db.version()'
)"
if [ "$actual_version" != "$expected_version" ]; then
  echo "::error::authenticated MongoDB fixture expected version $expected_version but observed $actual_version"
  exit 1
fi

docker exec "$container" mongosh --quiet \
  --username "$root_user" \
  --password "$root_password" \
  --authenticationDatabase admin \
  "$database" \
  --eval "
    db.createRole({
      role: 'oxideBatchMigrator',
      privileges: [{
        resource: { db: '${database}', collection: '' },
        actions: [
          'find', 'insert', 'update', 'remove',
          'createCollection', 'createIndex', 'listCollections', 'listIndexes'
        ]
      }],
      roles: []
    });
    db.createRole({
      role: 'oxideBatchRuntime',
      privileges: [
        {
          resource: { db: '${database}', collection: '' },
          actions: ['find']
        },
        {
          resource: { db: '${database}', collection: 'ob_counter' },
          actions: ['insert', 'update', 'remove']
        },
        {
          resource: { db: '${database}', collection: 'ob_core_job_definition' },
          actions: ['insert', 'update', 'remove']
        },
        {
          resource: { db: '${database}', collection: 'ob_job_instance' },
          actions: ['insert', 'update', 'remove']
        },
        {
          resource: { db: '${database}', collection: 'ob_job_execution' },
          actions: ['insert', 'update', 'remove']
        },
        {
          resource: { db: '${database}', collection: 'ob_step_execution' },
          actions: ['insert', 'update', 'remove']
        },
        {
          resource: { db: '${database}', collection: 'ob_flow_decision' },
          actions: ['insert', 'update', 'remove']
        }
      ],
      roles: []
    });
    db.createUser({
      user: '${migrator_user}',
      pwd: '${migrator_password}',
      roles: [{ role: 'oxideBatchMigrator', db: '${database}' }]
    });
    db.createUser({
      user: '${runtime_user}',
      pwd: '${runtime_password}',
      roles: [{ role: 'oxideBatchRuntime', db: '${database}' }]
    });
  " >/dev/null

migrator_uri="mongodb://${migrator_user}:${migrator_password}@127.0.0.1:${port}/?directConnection=true&replicaSet=rs0&authSource=${database}"
runtime_uri="mongodb://${runtime_user}:${runtime_password}@127.0.0.1:${port}/?directConnection=true&replicaSet=rs0&authSource=${database}"
bad_runtime_uri="mongodb://${runtime_user}:definitely-wrong-password@127.0.0.1:${port}/?directConnection=true&replicaSet=rs0&authSource=${database}"

OXIDEBATCH_MONGODB_AUTH_MIGRATOR_URI="$migrator_uri" \
OXIDEBATCH_MONGODB_AUTH_RUNTIME_URI="$runtime_uri" \
OXIDEBATCH_MONGODB_AUTH_BAD_RUNTIME_URI="$bad_runtime_uri" \
OXIDEBATCH_MONGODB_AUTH_TEST_DATABASE="$database" \
cargo test -p oxide-batch --features mongodb \
  --test mongodb_repository_core \
  mongodb_authenticated_runtime_is_scoped_and_bad_credentials_fail_closed \
  -- --exact --nocapture --test-threads=1

echo "authenticated MongoDB privilege certification: PASS (${lane}, ${expected_version})"
