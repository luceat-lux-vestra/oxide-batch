#!/usr/bin/env bash
# Fail closed when the dedicated M5 performance workflow drifts from its contract.

set -euo pipefail

workflow_path="${1:?usage: verify-ci-contract.sh <workflow-path>}"
contract="tests/fixtures/performance/execution-contract.json"
script_path="$(jq -er '.script_path' "${contract}")"

fail() {
  echo "performance execution contract drift: $*" >&2
  exit 1
}

require_literal() {
  local label="$1"
  local literal="$2"
  grep -Fq -- "${literal}" "${workflow_path}" || fail "${label} is missing: ${literal}"
}

# The fail-closed authority. A literal check only proves an expected string
# is present; it says nothing about whether something *else* was added
# beside it — an extra trigger, a job-level permission override, a widened
# matrix, a lost --release. Exact git blob identity catches all of that, and
# any other byte-level change, uniformly: the file the workflow run actually
# executed either has the identity this contract recorded, or the contract
# is stale and the campaign has to be re-run and re-retained against the new
# one, deliberately.
require_exact_identity() {
  local label="$1"
  local file="$2"
  local expected="$3"
  local actual
  actual="$(git hash-object "${file}")"
  test "${actual}" = "${expected}" ||
    fail "${label} (${file}) has git blob identity ${actual}, and the contract records ${expected}; a byte-level change here — even one that leaves every literal check below satisfied — means the retained evidence's execution semantics are stale and the contract must be deliberately updated before this campaign runs again"
}

expand_matrix() {
  printf '%s' "$1" | sed 's/{major}/${{ matrix.postgres }}/g'
}

test "${workflow_path}" = "$(jq -er '.workflow_path' "${contract}")" ||
  fail "workflow path does not match the contract"
test -f "${workflow_path}" || fail "workflow file is missing"
test -f "${script_path}" || fail "campaign script is missing"

require_literal "workflow name" "name: $(jq -er '.workflow_name' "${contract}")"
require_literal "pull_request trigger" "  pull_request:"
require_literal "push trigger" "  push:"
require_literal "workflow_dispatch trigger" "  workflow_dispatch:"
require_literal "pull_request main branch" "      - main"
require_literal "contents permission" "  contents: read"
require_literal "runner" "runs-on: $(jq -er '.runner' "${contract}")"
require_literal "matrix" "postgres: [$(jq -er '.supported_matrix | map("\"" + . + "\"") | join(", ")' "${contract}")]"
require_literal "database image" "image: $(expand_matrix "$(jq -er '.database.image' "${contract}")")"
require_literal "database name" "POSTGRES_DB: $(jq -er '.database.database_name' "${contract}")"
require_literal "health check" "$(jq -er '.database.health_check' "${contract}")"
require_literal "timeout" "timeout-minutes: $(jq -er '.timeout_minutes' "${contract}")"
require_literal "campaign script" "run: $(expand_matrix "$(jq -er '.script' "${contract}")")"
require_literal "report path" "path: $(jq -er '.report_path' "${contract}")"
require_literal "artifact name" "name: $(expand_matrix "$(jq -er '.artifact_name' "${contract}")")"
require_literal "failure retention" "if: always()"
require_literal "missing report failure" "if-no-files-found: error"

while IFS=$'\t' read -r key value; do
  require_literal "environment ${key}" "${key}: $(expand_matrix "${value}")"
done < <(jq -r '.environment | to_entries[] | [.key, .value] | @tsv' "${contract}")

command="$(jq -er '.command | join(" ")' "${contract}")"
grep -Fq -- "${command}" "${script_path}" || fail "campaign command is missing from ${script_path}"
grep -Fq -- "cargo run" "${script_path}" || fail "campaign is not run through cargo"
test "$(jq -er '.cargo_profile' "${contract}")" = "release" ||
  fail "unexpected cargo profile in the performance contract"

require_exact_identity "dedicated workflow" "${workflow_path}" "$(jq -er '.workflow_git_blob' "${contract}")"
require_exact_identity "campaign script" "${script_path}" "$(jq -er '.script_git_blob' "${contract}")"

echo "performance execution contract matches ${workflow_path}"
