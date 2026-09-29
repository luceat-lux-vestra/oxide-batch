#!/usr/bin/env bash
# Runs one deterministic M5 conformance target shard the way CI runs it.
#
# Usage: run-ci-campaign.sh <postgres-major> <shard-index> <shard-count>

set -euo pipefail

major="${1:?usage: run-ci-campaign.sh <postgres-major> <shard-index> <shard-count>}"
shard_index="${2:?usage: run-ci-campaign.sh <postgres-major> <shard-index> <shard-count>}"
shard_count="${3:?usage: run-ci-campaign.sh <postgres-major> <shard-index> <shard-count>}"

case "${major}" in
  15 | 18) ;;
  *)
    echo "run-ci-campaign.sh: ${major} is not a supported matrix point (15, 18)" >&2
    exit 2
    ;;
esac

if [[ "${shard_count}" != "2" || ! "${shard_index}" =~ ^[01]$ ]]; then
  echo "run-ci-campaign.sh: expected shard-index 0..1 and shard-count 2, got ${shard_index}/${shard_count}" >&2
  exit 2
fi

url="postgres://postgres:postgres@127.0.0.1:5432/oxide_batch_campaign"

export OXIDEBATCH_POSTGRES_ADMIN_TEST_URL="${url}"
export OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL="${url}"
export OXIDEBATCH_POSTGRES_TEST_URL="${url}"
export OXIDEBATCH_CAMPAIGN_DIR="target/m5-campaigns"
export OXIDEBATCH_CAMPAIGN_MATRIX="postgres-${major}"
export OXIDEBATCH_CONFORMANCE_SHARD_INDEX="${shard_index}"
export OXIDEBATCH_CONFORMANCE_SHARD_COUNT="${shard_count}"
unset OXIDEBATCH_CONFORMANCE_MERGE_DIR || true

exec cargo run --package oxide-batch-xtask -- conformance
