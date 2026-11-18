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
source_port=27020
restored_port=27021
source_container="oxide-mongodb-backup-source-${lane}"
restored_container="oxide-mongodb-backup-restored-${lane}"
temporary_root="$(mktemp -d)"
archive="$temporary_root/metadata.archive"
max_archive_bytes=$((16 * 1024 * 1024))

cleanup() {
  docker logs "$source_container" >/dev/null 2>&1 || true
  docker logs "$restored_container" >/dev/null 2>&1 || true
  docker rm --force "$source_container" >/dev/null 2>&1 || true
  docker rm --force "$restored_container" >/dev/null 2>&1 || true
  rm -rf "$temporary_root"
}
trap cleanup EXIT

start_replica_set() {
  local container="$1"
  local port="$2"
  local role="$3"

  docker run --detach --rm     --name "$container"     --publish "127.0.0.1:${port}:27017"     "$image"     --replSet rs0     --bind_ip_all     >/dev/null

  local ready=false
  for _ in $(seq 1 60); do
    if docker exec "$container" mongosh --quiet       --eval 'db.adminCommand({ ping: 1 }).ok' 2>/dev/null | grep -qx '1'; then
      ready=true
      break
    fi
    sleep 1
  done
  if [ "$ready" != "true" ]; then
    echo "::error::${role} MongoDB backup/restore fixture did not become reachable"
    docker logs "$container"
    exit 1
  fi

  docker exec "$container" mongosh --quiet --eval     'rs.initiate({_id:"rs0",members:[{_id:0,host:"127.0.0.1:27017"}]})'     >/dev/null

  local primary=false
  for _ in $(seq 1 60); do
    if docker exec "$container" mongosh --quiet       --eval 'db.hello().isWritablePrimary' 2>/dev/null | grep -qx 'true'; then
      primary=true
      break
    fi
    sleep 1
  done
  if [ "$primary" != "true" ]; then
    echo "::error::${role} MongoDB backup/restore fixture did not elect a writable primary"
    docker logs "$container"
    exit 1
  fi

  local actual_version
  actual_version="$(
    docker exec "$container" mongosh --quiet --eval 'db.version()'
  )"
  if [ "$actual_version" != "$expected_version" ]; then
    echo "::error::${role} MongoDB fixture expected version $expected_version but observed $actual_version"
    exit 1
  fi
}

start_replica_set "$source_container" "$source_port" "source"

source_uri="mongodb://127.0.0.1:${source_port}/?directConnection=true&replicaSet=rs0"
restored_uri="mongodb://127.0.0.1:${restored_port}/?directConnection=true&replicaSet=rs0"

OXIDEBATCH_MONGODB_BACKUP_SOURCE_URI="$source_uri" OXIDEBATCH_MONGODB_BACKUP_RESTORED_URI="$restored_uri" OXIDEBATCH_MONGODB_BACKUP_TEST_DATABASE="$database" cargo test -p oxide-batch --features mongodb   --test mongodb_repository_core   mongodb_backup_restore_seed_source   -- --exact --nocapture --test-threads=1

# The seed process and repository clients have exited before the logical backup,
# so no OxideBatch application writer remains active during mongodump.
timeout 45s docker exec "$source_container"   mongodump   --host 127.0.0.1   --port 27017   --db "$database"   --archive=/tmp/oxide-batch-metadata.archive

docker cp   "${source_container}:/tmp/oxide-batch-metadata.archive"   "$archive"   >/dev/null

archive_size="$(stat -c '%s' "$archive")"
if [ "$archive_size" -le 0 ] || [ "$archive_size" -gt "$max_archive_bytes" ]; then
  echo "::error::MongoDB backup archive size $archive_size is outside the certification bound 1..=$max_archive_bytes"
  exit 1
fi
archive_sha256="$(sha256sum "$archive" | awk '{print $1}')"
if ! [[ "$archive_sha256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "::error::MongoDB backup archive SHA-256 was not canonical"
  exit 1
fi

start_replica_set "$restored_container" "$restored_port" "restored"

pre_restore_collections="$(
  docker exec "$restored_container" mongosh --quiet "$database"     --eval 'db.getCollectionNames().length'
)"
if [ "$pre_restore_collections" != "0" ]; then
  echo "::error::restore target was not clean before mongorestore"
  exit 1
fi

docker cp   "$archive"   "${restored_container}:/tmp/oxide-batch-metadata.archive"   >/dev/null

timeout 45s docker exec "$restored_container"   mongorestore   --host 127.0.0.1   --port 27017   --archive=/tmp/oxide-batch-metadata.archive   --nsInclude "${database}.*"   --drop

OXIDEBATCH_MONGODB_BACKUP_SOURCE_URI="$source_uri" OXIDEBATCH_MONGODB_BACKUP_RESTORED_URI="$restored_uri" OXIDEBATCH_MONGODB_BACKUP_TEST_DATABASE="$database" cargo test -p oxide-batch --features mongodb   --test mongodb_repository_core   mongodb_backup_restore_reconciles_and_mutates_clean_target   -- --exact --nocapture --test-threads=1

echo "MongoDB backup/restore certification: PASS (${lane}, ${expected_version}, schema=3, archive_sha256=${archive_sha256}, archive_bytes=${archive_size})"
