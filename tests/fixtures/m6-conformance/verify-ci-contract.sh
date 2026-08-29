#!/usr/bin/env bash
# Fail closed when the dedicated M6 conformance workflow drifts.

set -euo pipefail

workflow_path="${1:?usage: verify-ci-contract.sh <workflow-path>}"
contract="tests/fixtures/m6-conformance/execution-contract.json"

fail() {
  echo "m6-conformance execution contract drift: $*" >&2
  exit 1
}

require_literal() {
  local label="$1"
  local literal="$2"
  grep -Fq -- "${literal}" "${workflow_path}" || fail "${label} is missing: ${literal}"
}

test "${workflow_path}" = "$(jq -er '.workflow_path' "${contract}")" ||
  fail "workflow path does not match the contract"
test -f "${workflow_path}" || fail "workflow file is missing"
test -f "$(jq -er '.script_path' "${contract}")" || fail "campaign script is missing"

require_literal "workflow name" "name: $(jq -er '.workflow_name' "${contract}")"
require_literal "pull_request trigger" "  pull_request:"
require_literal "push trigger" "  push:"
require_literal "workflow_dispatch trigger" "  workflow_dispatch:"
require_literal "pull_request main branch" "      - main"
require_literal "contents permission" "  contents: read"
require_literal "runner" "runs-on: $(jq -er '.runner' "${contract}")"
require_literal "fetch-depth" "fetch-depth: 0"
require_literal "timeout" "timeout-minutes: $(jq -er '.timeout_minutes' "${contract}")"
require_literal "campaign script" "run: $(jq -er '.script' "${contract}")"
require_literal "report path" "path: $(jq -er '.report_path' "${contract}")"
require_literal "artifact name" "name: $(jq -er '.artifact_name' "${contract}")"
require_literal "failure retention" "if: always()"
require_literal "missing report failure" "if-no-files-found: error"
require_literal "matrix 15" 'postgres: ["15", "18"]'

while IFS=$'\t' read -r key value; do
  require_literal "environment ${key}" "${key}: ${value}"
done < <(jq -r '.environment | to_entries[] | [.key, .value] | @tsv' "${contract}")

script_path="$(jq -er '.script_path' "${contract}")"
command="$(jq -er '.command | join(" ")' "${contract}")"
grep -Fq -- "${command}" "${script_path}" || fail "campaign command is missing from ${script_path}"
grep -Fq -- "cargo run" "${script_path}" || fail "campaign is not run through cargo"

echo "m6-conformance execution contract matches ${workflow_path}"
