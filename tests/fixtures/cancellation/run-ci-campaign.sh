#!/usr/bin/env bash
# Runs the M5 PostgreSQL cancellation campaign the way CI runs it.
#
# The workflow provisions a runner and a database and then calls this; how the
# campaign is actually executed lives here rather than in the workflow. That
# split keeps the execution semantics reviewable in one place beside the
# campaign, rather than spread through a workflow that also builds everything
# else. It does not narrow what invalidates evidence: the workflow file is in
# the closure as well, so an unrelated CI job forces a rerun too.
#
# The values below and the ones in execution-contract.json describe the same
# execution. Both files, and the workflow that calls this one, are inside the
# campaign's semantic closure, so a change to any of them invalidates retained
# evidence and forces the campaign to be run again — which is the mechanism.
# Nothing here relies on the three being kept in step by review.
#
# Usage: run-ci-campaign.sh <postgres-major>

set -euo pipefail

major="${1:?usage: run-ci-campaign.sh <postgres-major>}"

case "${major}" in
  15 | 18) ;;
  *)
    echo "run-ci-campaign.sh: ${major} is not a supported matrix point (15, 18)" >&2
    exit 2
    ;;
esac

url="postgres://postgres:postgres@127.0.0.1:5432/oxide_batch_cancellation"

export OXIDEBATCH_POSTGRES_TEST_URL="${url}"
export OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL="${url}"
export OXIDEBATCH_CAMPAIGN_DIR="target/m5-campaigns"
export OXIDEBATCH_CAMPAIGN_MATRIX="postgres-${major}"

exec cargo run --package oxide-batch-xtask -- cancellation
