#!/usr/bin/env bash
# Merges every M6 conformance shard into the canonical retained report.
#
# Usage: merge-ci-campaign.sh <shard-count> <shard-directory>

set -euo pipefail

shard_count="${1:?usage: merge-ci-campaign.sh <shard-count> <shard-directory>}"
shard_directory="${2:?usage: merge-ci-campaign.sh <shard-count> <shard-directory>}"

if [[ "${shard_count}" != "2" ]]; then
  echo "merge-ci-campaign.sh: expected shard-count 2, got ${shard_count}" >&2
  exit 2
fi
if [[ ! -d "${shard_directory}" ]]; then
  echo "merge-ci-campaign.sh: shard directory does not exist: ${shard_directory}" >&2
  exit 2
fi

export OXIDEBATCH_CAMPAIGN_DIR="target/m6-campaigns"
export OXIDEBATCH_M6_CONFORMANCE_SHARD_COUNT="${shard_count}"
export OXIDEBATCH_M6_CONFORMANCE_MERGE_DIR="${shard_directory}"
unset OXIDEBATCH_M6_CONFORMANCE_SHARD_INDEX || true

exec cargo run --package oxide-batch-xtask -- m6-conformance
