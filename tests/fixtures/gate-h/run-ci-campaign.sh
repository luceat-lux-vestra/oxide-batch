#!/usr/bin/env bash
# Runs the M6 Gate H campaign the way CI runs it.
#
# The dedicated workflow provisions a runner and then calls this; how the
# campaign is actually executed lives here rather than in the workflow. The
# workflow's contract-check step verifies the important provisioning and
# artifact values before this script runs.
#
# The values below and the ones in execution-contract.json describe the same
# execution. Both files, the contract checker, and the dedicated workflow are
# inside the campaign's semantic closure, so a change to any of them
# invalidates retained evidence and forces the campaign to be run again.
#
# Usage: run-ci-campaign.sh

set -euo pipefail

export OXIDEBATCH_CAMPAIGN_DIR="target/m6-campaigns"

exec cargo run --package oxide-batch-xtask -- gate-h
