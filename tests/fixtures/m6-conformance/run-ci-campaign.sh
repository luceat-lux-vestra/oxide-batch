#!/usr/bin/env bash
# Runs one deterministic M6 full-component conformance target shard.
#
# Usage: run-ci-campaign.sh <shard-index> <shard-count>

set -euo pipefail

shard_index="${1:?usage: run-ci-campaign.sh <shard-index> <shard-count>}"
shard_count="${2:?usage: run-ci-campaign.sh <shard-index> <shard-count>}"

if [[ "${shard_count}" != "2" || ! "${shard_index}" =~ ^[01]$ ]]; then
  echo "run-ci-campaign.sh: expected shard-index 0..1 and shard-count 2, got ${shard_index}/${shard_count}" >&2
  exit 2
fi

export OXIDEBATCH_CAMPAIGN_DIR="target/m6-campaigns"
export OXIDEBATCH_M6_CONFORMANCE_SHARD_INDEX="${shard_index}"
export OXIDEBATCH_M6_CONFORMANCE_SHARD_COUNT="${shard_count}"
unset OXIDEBATCH_M6_CONFORMANCE_MERGE_DIR || true

exec cargo run --package oxide-batch-xtask -- m6-conformance
