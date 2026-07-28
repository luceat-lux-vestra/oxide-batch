#!/usr/bin/env bash
set -euo pipefail

container_name="oxide-batch-m0-spike-${PPID}-$$"
database_port="${OXIDEBATCH_SPIKE_PORT:-55432}"
database_url="postgres://postgres:postgres@127.0.0.1:${database_port}/oxide_batch_spike"

cleanup() {
  docker rm --force "${container_name}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --detach --rm \
  --name "${container_name}" \
  --publish "127.0.0.1:${database_port}:5432" \
  --env POSTGRES_PASSWORD=postgres \
  --env POSTGRES_DB=oxide_batch_spike \
  postgres:18 >/dev/null

for _ in $(seq 1 30); do
  if docker exec "${container_name}" pg_isready --username postgres --dbname oxide_batch_spike >/dev/null 2>&1; then
    OXIDEBATCH_SPIKE_DATABASE_URL="${database_url}" \
      cargo test -p oxide-batch-m0-spikes --test postgres -- --nocapture --test-threads=1
    exit 0
  fi
  sleep 1
done

echo "PostgreSQL did not become ready within 30 seconds" >&2
exit 1
