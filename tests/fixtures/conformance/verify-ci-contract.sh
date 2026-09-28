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

require_count() {
  local label="$1"
  local literal="$2"
  local expected="$3"
  local actual
  actual="$(grep -Fc -- "${literal}" "${workflow_path}" || true)"
  test "${actual}" = "${expected}" ||
    fail "${label} count is ${actual}, expected ${expected}: ${literal}"
}

require_exact_identity() {
  local label="$1"
  local file="$2"
  local expected="$3"
  local actual
  actual="$(git hash-object "${file}")"
  test "${actual}" = "${expected}" ||
    fail "${label} (${file}) has git blob identity ${actual}, and the contract records ${expected}; byte-level drift makes retained evidence stale"
}

test "${workflow_path}" = "$(jq -er '.workflow_path' "${contract}")" ||
  fail "workflow path does not match the contract"
for file in "${workflow_path}" "${script_path}" "${merge_script_path}"; do
  test -f "${file}" || fail "required execution file is missing: ${file}"
done

require_literal "workflow name" "name: $(jq -er '.workflow_name' "${contract}")"
require_literal "pull_request trigger" "  pull_request:"
require_literal "workflow_dispatch trigger" "  workflow_dispatch:"
require_literal "pull_request main branch" "      - main"
require_literal "contents permission" "  contents: read"
require_literal "pull-request read permission" "  pull-requests: read"
require_literal "runner" "runs-on: $(jq -er '.runner' "${contract}")"

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
require_literal_in "shard cargo command" "${script_path}" "exec $(jq -er '.command | join(" ")' "${contract}")"

for variable in   OXIDEBATCH_CAMPAIGN_DIR   OXIDEBATCH_CAMPAIGN_MATRIX   OXIDEBATCH_CONFORMANCE_SHARD_COUNT   OXIDEBATCH_CONFORMANCE_MERGE_DIR
do
  require_literal_in "merge environment ${variable}" "${merge_script_path}" "export ${variable}="
done
require_literal_in "merge shard-index reset" "${merge_script_path}" "unset OXIDEBATCH_CONFORMANCE_SHARD_INDEX"
require_literal_in "merge cargo command" "${merge_script_path}" "exec $(jq -er '.command | join(" ")' "${contract}")"

require_exact_identity "dedicated workflow" "${workflow_path}" "$(jq -er '.workflow_git_blob' "${contract}")"
require_exact_identity "campaign shard script" "${script_path}" "$(jq -er '.script_git_blob' "${contract}")"
require_exact_identity "campaign merge script" "${merge_script_path}" "$(jq -er '.merge_script_git_blob' "${contract}")"

echo "conformance execution contract matches ${workflow_path}"
