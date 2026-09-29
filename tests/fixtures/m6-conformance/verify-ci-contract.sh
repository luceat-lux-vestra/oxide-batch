#!/usr/bin/env bash
# Fail closed when the dedicated sharded M6 conformance workflow drifts.

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
test -f "$(jq -er '.script_path' "${contract}")" || fail "campaign shard script is missing"
test -f "$(jq -er '.sharding.merge_script_path' "${contract}")" || fail "campaign merge script is missing"

workflow_blob="$(git hash-object "${workflow_path}")"
test "${workflow_blob}" = "$(jq -er '.workflow_git_blob' "${contract}")" ||
  fail "workflow Git blob does not match the execution contract"

require_literal "workflow name" "name: $(jq -er '.workflow_name' "${contract}")"
require_literal "workflow_call trigger" "  workflow_call:"
require_literal "workflow_dispatch trigger" "  workflow_dispatch:"
require_literal "contents permission" "  contents: read"
require_literal "runner" "runs-on: $(jq -er '.runner' "${contract}")"
require_literal "fetch-depth" "fetch-depth: 0"
require_literal "PostgreSQL matrix" 'postgres: ["15", "18"]'
require_literal "shard matrix" 'shard: [0, 1]'
require_literal "shard timeout" "timeout-minutes: $(jq -er '.timeout_minutes' "${contract}")"
require_literal "merge timeout" "timeout-minutes: $(jq -er '.merge_timeout_minutes' "${contract}")"
require_literal "shard command" 'run: ./tests/fixtures/m6-conformance/run-ci-campaign.sh "$SHARD_INDEX" 2'
require_literal "partial artifact" 'name: m6-conformance-shard-postgres-${{ matrix.postgres }}-${{ matrix.shard }}'
require_literal "partial report path" 'path: target/m6-campaigns/m6-conformance-shard-${{ matrix.shard }}.json'
require_literal "aggregate dependency" "needs: [m6-conformance-shard]"
require_literal "aggregate always" 'if: ${{ always() }}'
require_literal "aggregate shard result" 'SHARD_RESULT: ${{ needs.m6-conformance-shard.result }}'
require_literal "canonical job name" 'name: postgres-${{ matrix.postgres }}-m6-conformance'
require_literal "partial artifact download" 'pattern: m6-conformance-shard-postgres-${{ matrix.postgres }}-*'
require_literal "merge input directory" "path: $(jq -er '.sharding.merge_input_dir' "${contract}")"
require_literal "canonical merge command" "run: $(jq -er '.sharding.merge_script' "${contract}") 2 $(jq -er '.sharding.merge_input_dir' "${contract}")"
require_literal "canonical report path" "path: $(jq -er '.report_path' "${contract}")"
require_literal "canonical artifact name" "name: $(jq -er '.artifact_name' "${contract}")"
require_literal "failure retention" "if: always()"
require_literal "missing report failure" "if-no-files-found: error"

while IFS=$'\t' read -r key value; do
  require_literal "environment ${key}" "${key}: ${value}"
done < <(jq -r '.environment | to_entries[] | [.key, .value] | @tsv' "${contract}")

script_path="$(jq -er '.script_path' "${contract}")"
merge_script_path="$(jq -er '.sharding.merge_script_path' "${contract}")"
command="$(jq -er '.command | join(" ")' "${contract}")"

grep -Fq -- "${command}" "${script_path}" || fail "campaign command is missing from ${script_path}"
grep -Fq -- "$(jq -er '.sharding.shard_index_env' "${contract}")" "${script_path}" ||
  fail "shard index environment is missing from ${script_path}"
grep -Fq -- "$(jq -er '.sharding.shard_count_env' "${contract}")" "${script_path}" ||
  fail "shard count environment is missing from ${script_path}"

grep -Fq -- "${command}" "${merge_script_path}" || fail "campaign command is missing from ${merge_script_path}"
grep -Fq -- "$(jq -er '.sharding.shard_count_env' "${contract}")" "${merge_script_path}" ||
  fail "merge shard count environment is missing from ${merge_script_path}"
grep -Fq -- "$(jq -er '.sharding.merge_dir_env' "${contract}")" "${merge_script_path}" ||
  fail "merge directory environment is missing from ${merge_script_path}"

echo "m6-conformance execution contract matches ${workflow_path}"
