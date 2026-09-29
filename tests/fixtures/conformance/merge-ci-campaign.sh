#!/usr/bin/env bash
# Merges every M5 conformance shard into the canonical retained report.
#
# Usage: merge-ci-campaign.sh <postgres-major> <shard-count> <shard-directory>

set -euo pipefail

major="${1:?usage: merge-ci-campaign.sh <postgres-major> <shard-count> <shard-directory>}"
shard_count="${2:?usage: merge-ci-campaign.sh <postgres-major> <shard-count> <shard-directory>}"
shard_directory="${3:?usage: merge-ci-campaign.sh <postgres-major> <shard-count> <shard-directory>}"

case "${major}" in
  15 | 18) ;;
  *)
    echo "merge-ci-campaign.sh: ${major} is not a supported matrix point (15, 18)" >&2
    exit 2
    ;;
esac

if [[ "${shard_count}" != "2" ]]; then
  echo "merge-ci-campaign.sh: expected shard-count 2, got ${shard_count}" >&2
  exit 2
fi
if [[ ! -d "${shard_directory}" ]]; then
  echo "merge-ci-campaign.sh: shard directory does not exist: ${shard_directory}" >&2
  exit 2
fi

export OXIDEBATCH_CAMPAIGN_DIR="target/m5-campaigns"
export OXIDEBATCH_CAMPAIGN_MATRIX="postgres-${major}"
export OXIDEBATCH_CONFORMANCE_SHARD_COUNT="${shard_count}"
export OXIDEBATCH_CONFORMANCE_MERGE_DIR="${shard_directory}"
unset OXIDEBATCH_CONFORMANCE_SHARD_INDEX || true

exec cargo run --package oxide-batch-xtask -- conformance
