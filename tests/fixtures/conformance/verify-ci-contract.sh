#!/usr/bin/env bash
# Fail closed when the dedicated M5 conformance workflow drifts from its contract.

set -euo pipefail

workflow_path="${1:?usage: verify-ci-contract.sh <workflow-path>}"
contract="tests/fixtures/conformance/execution-contract.json"
script_path="$(jq -er '.script_path' "${contract}")"
merge_script_path="$(jq -er '.merge_script_path' "${contract}")"

fail() {
  echo "conformance execution contract drift: $*" >&2
  exit 1
}

require_literal() {
  local label="$1"
  local literal="$2"
  grep -Fq -- "${literal}" "${workflow_path}" || fail "${label} is missing: ${literal}"
}

require_literal_in() {
  local label="$1"
  local file="$2"
  local literal="$3"
  grep -Fq -- "${literal}" "${file}" || fail "${label} is missing from ${file}: ${literal}"
}

require_exact_line_in() {
  local label="$1"
  local file="$2"
  local line="$3"
  grep -Fxq -- "${line}" "${file}" || fail "${label} exact line is missing from ${file}: ${line}"
}

require_exec_count() {
  local label="$1"
  local file="$2"
  local expected="$3"
  local actual
  actual="$(grep -Ec '^[[:space:]]*exec[[:space:]]+' "${file}" || true)"
  test "${actual}" = "${expected}" ||
    fail "${label} exec count is ${actual}, expected ${expected}"
}

require_count() {
  local label="$1"
  local literal="$2"
  local expected="$3"
  local actual
  actual="$(grep -Fc -- "${literal}" "${workflow_path}" || true)"
  test "${actual}" = "${expected}" ||
    fail "${label} count is ${actual}, expected ${expected}: ${literal}"
}

require_git_blob() {
  local label="$1"
  local file="$2"
  local expected="$3"
  local actual
  [[ "${expected}" =~ ^[0-9a-f]{40}$ ]] ||
    fail "${label} expected Git blob is malformed: ${expected}"
  actual="$(git hash-object --no-filters "${file}")"
  test "${actual}" = "${expected}" ||
    fail "${label} Git blob drifted: expected ${expected}, observed ${actual}"
}


test "${workflow_path}" = "$(jq -er '.workflow_path' "${contract}")" ||
  fail "workflow path does not match the contract"
for file in "${workflow_path}" "${script_path}" "${merge_script_path}"; do
  test -f "${file}" || fail "required execution file is missing: ${file}"
done

require_git_blob "workflow" "${workflow_path}" "$(jq -er '.git_blobs.workflow' "${contract}")"
require_git_blob "campaign script" "${script_path}" "$(jq -er '.git_blobs.script' "${contract}")"
require_git_blob "merge script" "${merge_script_path}" "$(jq -er '.git_blobs.merge_script' "${contract}")"

require_literal "workflow name" "name: $(jq -er '.workflow_name' "${contract}")"
require_literal "workflow_dispatch trigger" "  workflow_dispatch:"
require_literal "workflow_call trigger" "  workflow_call:"
require_literal "contents permission" "  contents: read"
require_literal "pull-request read permission" "  pull-requests: read"
require_literal "runner" "runs-on: $(jq -er '.runner' "${contract}")"

python3 - "${workflow_path}" <<'PY'
from pathlib import Path
import re
import sys

path = Path(sys.argv[1])
lines = path.read_text(encoding="utf-8").splitlines()

def fail(message: str) -> None:
    print(f"conformance execution contract drift: {message}", file=sys.stderr)
    raise SystemExit(1)

def block(start_line: str) -> list[str]:
    try:
        start = lines.index(start_line)
    except ValueError:
        fail(f"missing block: {start_line}")
    out = [lines[start]]
    for line in lines[start + 1:]:
        if re.match(r"^  [A-Za-z0-9_-]+:$", line):
            break
        out.append(line)
    return out

try:
    on_index = lines.index("on:")
except ValueError:
    fail("missing on block")
events = []
for line in lines[on_index + 1:]:
    if line and not line.startswith(" "):
        break
    match = re.match(r"^  ([A-Za-z0-9_-]+):(?:\s.*)?$", line)
    if match:
        events.append(match.group(1))
if events != ["workflow_call", "workflow_dispatch"]:
    fail(f"trigger set must be exactly workflow_call, workflow_dispatch; observed {events}")

if sum(line == "permissions:" for line in lines) != 1:
    fail("workflow must declare exactly one top-level permissions block")
if any(re.match(r"^\s+permissions:$", line) for line in lines):
    fail("job-level permissions overrides are forbidden")
if any(re.match(r"^\s+[A-Za-z0-9_-]+:\s*write(?:\s|#|$)", line) for line in lines):
    fail("write permissions are forbidden in M5 conformance")

if any(re.match(r"^\s+include:$", line) for line in lines):
    fail("matrix include expansion is forbidden")

expected_steps = {
    "conformance-shard-15": [
        "Check out repository",
        "Fail closed if the execution contract drifts",
        "Run PostgreSQL 15 conformance shard",
        "Retain PostgreSQL 15 shard evidence",
    ],
    "conformance-shard-18": [
        "Check out repository",
        "Fail closed if the execution contract drifts",
        "Run PostgreSQL 18 conformance shard",
        "Retain PostgreSQL 18 shard evidence",
    ],
    "conformance-deep-15": [
        "Check out repository",
        "Fail closed if the execution contract drifts",
        "Download PostgreSQL 15 shard evidence",
        "Merge PostgreSQL 15 canonical conformance report",
        "Retain the PostgreSQL 15 canonical campaign report",
    ],
    "conformance-deep-18": [
        "Check out repository",
        "Fail closed if the execution contract drifts",
        "Download PostgreSQL 18 shard evidence",
        "Merge PostgreSQL 18 canonical conformance report",
        "Retain the PostgreSQL 18 canonical campaign report",
    ],
    "conformance-campaign": [
        "Emit required conformance context",
    ],
}
for job, expected in expected_steps.items():
    observed = []
    for line in block(f"  {job}:"):
        match = re.match(r"^      - name: (.+)$", line)
        if match:
            observed.append(match.group(1))
    if observed != expected:
        fail(f"{job} step inventory drifted: expected {expected}, observed {observed}")
PY

shard_count="$(jq -er '.sharding.shard_count' "${contract}")"
test "${shard_count}" = "2" || fail "this workflow contract requires exactly two shards"
require_count "two-way shard matrix" "shard: [0, 1]" "2"

for major in $(jq -er '.supported_matrix[]' "${contract}"); do
  image="$(jq -er --arg major "${major}" '.database.images[$major]' "${contract}")"
  require_literal "PostgreSQL ${major} image" "image: ${image}"
  require_literal "PostgreSQL ${major} shard job" "  conformance-shard-${major}:"
  require_literal "PostgreSQL ${major} deep aggregate" "  conformance-deep-${major}:"
  require_literal "PostgreSQL ${major} shard env binding" "SHARD_INDEX: \${{ matrix.shard }}"
  require_literal "PostgreSQL ${major} shard command"     'run: ./tests/fixtures/conformance/run-ci-campaign.sh '"${major}"' "$SHARD_INDEX" 2'
  require_literal "PostgreSQL ${major} partial artifact"     "name: conformance-shard-postgres-${major}-\${{ matrix.shard }}"
  require_literal "PostgreSQL ${major} partial path"     "path: target/m5-campaigns/conformance-shard-\${{ matrix.shard }}.json"
  require_literal "PostgreSQL ${major} artifact download"     "pattern: conformance-shard-postgres-${major}-*"
  require_literal "PostgreSQL ${major} merge command"     "run: bash ./tests/fixtures/conformance/merge-ci-campaign.sh ${major} ${shard_count} $(jq -er '.sharding.merge_input_dir' "${contract}")"
  require_literal "PostgreSQL ${major} canonical artifact"     "name: conformance-campaign-postgres-${major}"
done

require_literal "database name" "POSTGRES_DB: $(jq -er '.database.database_name' "${contract}")"
require_literal "health check" "$(jq -er '.database.health_check' "${contract}")"
require_literal "shard timeout" "timeout-minutes: $(jq -er '.timeout_minutes' "${contract}")"
require_literal "merge timeout" "timeout-minutes: $(jq -er '.sharding.merge_timeout_minutes' "${contract}")"
require_literal "canonical report path" "path: $(jq -er '.report_path' "${contract}")"
require_literal "trusted route job" "  $(jq -er '.pr_routing.route_job' "${contract}"):"
require_literal "required context emitter job" "  $(jq -er '.pr_routing.required_context_job' "${contract}"):"
require_literal "required context matrix" 'postgres: ["15", "18"]'
require_literal "required emitter PG15 result" 'DEEP_15_RESULT: ${{ needs.conformance-deep-15.result }}'
require_literal "required emitter PG18 result" 'DEEP_18_RESULT: ${{ needs.conformance-deep-18.result }}'
require_literal "required emitter selector" 'case "$POSTGRES" in'
require_literal "trusted base checkout" 'ref: ${{ github.event.pull_request.base.sha }}'
require_literal "trusted base directory" "path: .trusted-base"
require_literal "route-job failure fallback" "needs.route.result != 'success'"
require_literal "classification failure fallback" "needs.route.outputs.classification_outcome != 'success'"
require_literal "direct-proof conformance membership" "contains(needs.route.outputs.direct_workflows, '.github/workflows/m5-conformance.yml')"
require_literal "draft placeholder" "M5 conformance is deferred until the pull request is ready for review"
require_literal "failure retention" 'if: ${{ always() }}'
require_literal "missing report failure" "if-no-files-found: error"
require_literal "merged partial downloads" "merge-multiple: true"

for variable in   OXIDEBATCH_POSTGRES_ADMIN_TEST_URL   OXIDEBATCH_POSTGRES_MIGRATOR_TEST_URL   OXIDEBATCH_POSTGRES_TEST_URL   OXIDEBATCH_CAMPAIGN_DIR   OXIDEBATCH_CAMPAIGN_MATRIX   OXIDEBATCH_CONFORMANCE_SHARD_INDEX   OXIDEBATCH_CONFORMANCE_SHARD_COUNT
do
  require_literal_in "shard environment ${variable}" "${script_path}" "export ${variable}="
done
require_literal_in "shard merge-mode reset" "${script_path}" "unset OXIDEBATCH_CONFORMANCE_MERGE_DIR"
require_exact_line_in "shard cargo command" "${script_path}" "exec $(jq -er '.command | join(" ")' "${contract}")"
require_exec_count "shard script" "${script_path}" "1"

for variable in   OXIDEBATCH_CAMPAIGN_DIR   OXIDEBATCH_CAMPAIGN_MATRIX   OXIDEBATCH_CONFORMANCE_SHARD_COUNT   OXIDEBATCH_CONFORMANCE_MERGE_DIR
do
  require_literal_in "merge environment ${variable}" "${merge_script_path}" "export ${variable}="
done
require_literal_in "merge shard-index reset" "${merge_script_path}" "unset OXIDEBATCH_CONFORMANCE_SHARD_INDEX"
require_exact_line_in "merge cargo command" "${merge_script_path}" "exec $(jq -er '.command | join(" ")' "${contract}")"
require_exec_count "merge script" "${merge_script_path}" "1"

echo "conformance execution contract matches ${workflow_path}"
