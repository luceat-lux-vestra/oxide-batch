#!/usr/bin/env bash
# Runs the M6 full component conformance campaign in its CI PostgreSQL matrix.

set -euo pipefail

export OXIDEBATCH_CAMPAIGN_DIR="target/m6-campaigns"

exec cargo run --package oxide-batch-xtask -- m6-conformance
