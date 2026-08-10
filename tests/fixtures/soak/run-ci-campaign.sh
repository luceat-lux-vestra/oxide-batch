#!/usr/bin/env bash
# Runs the M5 PostgreSQL soak campaign the way CI runs it.
#
# The workflow provisions a runner and a database and then calls this; how the
# campaign is actually executed lives here rather than in the workflow. That
# split is what lets the two kinds of change be treated differently: adding an
# unrelated CI job leaves retained evidence valid, and changing the command, the
# environment, or the database the soak runs against does not — this file is in
# the campaign's semantic closure, and the workflow is not.
#
# The values below are the ones execution-contract.json declares. They are
# stated once here and read from there by review; a mismatch between the two is
# a defect in this file.
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

url="postgres://postgres:postgres@127.0.0.1:5432/oxide_batch_soak"

export OXIDEBATCH_POSTGRES_TEST_URL="${url}"
export OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL="${url}"
export OXIDEBATCH_CAMPAIGN_DIR="target/m5-campaigns"
export OXIDEBATCH_CAMPAIGN_MATRIX="postgres-${major}"

exec cargo run --package oxide-batch-xtask -- soak
