#!/usr/bin/env ruby
# frozen_string_literal: true

require 'json'
require 'fileutils'
require 'minitest/autorun'
require 'tmpdir'
require_relative 'verify-merge-gates'

class MergeGateVerifierTest < Minitest::Test
  LEGACY = [
    'Analyze (actions)',
    'quality',
    'postgres-15-repository',
    'postgres-18-repository',
    'postgres-15-conformance-campaign',
    'postgres-18-conformance-campaign'
  ].freeze

  FINAL = [
    'Analyze (actions)',
    'quality',
    'postgresql',
    'postgresql-conformance'
  ].freeze

  def with_repo
    Dir.mktmpdir do |root|
      FileUtils.mkdir_p(File.join(root, '.github/workflows'))
      FileUtils.mkdir_p(File.join(root, '.github/scripts'))
      policy = {
        'schema_version' => 6,
        'ruleset' => {'id' => 7, 'name' => 'Protect main'},
        'workflow_defaults' => [
          {'pattern' => '.github/workflows/ci.yml', 'classification' => 'required'},
          {'pattern' => '.github/workflows/evidence.yml', 'classification' => 'advisory'},
          {'pattern' => '.github/workflows/supply-chain.yml', 'classification' => 'advisory'},
          {'pattern' => '.github/workflows/campaign-orchestrator.yml', 'classification' => 'advisory'},
          {'pattern' => '.github/workflows/m5-*.yml', 'classification' => 'advisory'},
          {'pattern' => '.github/workflows/deep-*.yml', 'classification' => 'advisory'},
          {'pattern' => '.github/workflows/pr-labeler.yml', 'classification' => 'advisory'}
        ],
        'job_overrides' => [
          {
            'workflow' => '.github/workflows/ci.yml',
            'job' => 'postgresql-merge-gate',
            'classification' => 'advisory'
          },
          {
            'workflow' => '.github/workflows/ci.yml',
            'job' => 'quality-fast',
            'classification' => 'advisory'
          },
          {
            'workflow' => '.github/workflows/ci.yml',
            'job' => 'quality-integration-0',
            'classification' => 'advisory'
          },
          {
            'workflow' => '.github/workflows/ci.yml',
            'job' => 'quality-integration-1',
            'classification' => 'advisory'
          },
          {
            'workflow' => '.github/workflows/ci.yml',
            'job' => 'quality-integration-2',
            'classification' => 'advisory'
          },
          {
            'workflow' => '.github/workflows/ci.yml',
            'job' => 'quality-integration-3',
            'classification' => 'advisory'
          },
          {
            'workflow' => '.github/workflows/ci.yml',
            'job' => 'quality-bin-doc',
            'classification' => 'advisory'
          },
          {
            'workflow' => '.github/workflows/ci.yml',
            'job' => 'quality-contracts',
            'classification' => 'advisory'
          },
          {
            'workflow' => '.github/workflows/m5-conformance.yml',
            'job' => 'conformance-campaign',
            'classification' => 'required'
          }
        ],
        'managed_required_contexts' => ['Analyze (actions)'],
        'aggregate_gates' => [
          {
            'context' => 'postgresql',
            'state' => 'candidate',
            'migration_group' => 'postgresql',
            'producer' => {
              'workflow' => '.github/workflows/ci.yml',
              'job' => 'postgresql-merge-gate'
            },
            'members' => ['postgres-15-repository', 'postgres-18-repository']
          },
          {
            'context' => 'postgresql-conformance',
            'state' => 'candidate',
            'migration_group' => 'postgresql',
            'producer' => {
              'workflow' => '.github/workflows/m5-conformance.yml',
              'job' => 'postgresql-conformance-merge-gate'
            },
            'members' => [
              'postgres-15-conformance-campaign',
              'postgres-18-conformance-campaign'
            ]
          }
        ],
        'repository_merge_gate' => {
          'context' => 'merge-gate',
          'state' => 'candidate',
          'producer' => {
            'workflow' => '.github/workflows/pr-labeler.yml',
            'job' => 'merge-gate'
          },
          'members' => [
            {'context' => 'Analyze (actions)', 'workflow' => '.github/workflows/codeql.yml'},
            {'context' => 'postgresql', 'workflow' => '.github/workflows/ci.yml'},
            {'context' => 'postgresql-conformance', 'workflow' => '.github/workflows/m5-conformance.yml'},
            {'context' => 'quality', 'workflow' => '.github/workflows/ci.yml'}
          ]
        },
        'pending_ruleset_contexts' => ['postgresql', 'postgresql-conformance', 'merge-gate'],
        'pr_scope' => {
          'docs_only' => {
            'exact_paths' => ['README.md'],
            'markdown_prefixes' => ['docs/'],
            'excluded_prefixes' => []
          },
          'campaign_semantics_glob' => 'tests/fixtures/**/campaign-semantics.json',
          'retained_evidence_policy' => 'docs/engineering/retained-evidence-policy.json',
          'global_campaign_paths' => ['Cargo.lock'],
          'global_direct_proof_paths' => [
            '.github/scripts/pr-scope.py',
            '.github/merge-gate-policy.json',
            '.github/workflows/campaign-orchestrator.yml',
            'docs/engineering/retained-evidence-policy.json'
          ],
          'trusted_tree_contract' => 'exact-git-base-sha',
          'docs_only_applicability' => {
            'supply_chain' => {
              'sensitive_exact_paths' => ['docs/engineering/dependency-policy.md'],
              'sensitive_prefixes' => []
            },
            'evidence_provenance' => {
              'sensitive_exact_paths' => [],
              'sensitive_prefixes' => ['docs/engineering/campaigns/']
            }
          }
        },
        'post_main' => {
          'default_branch' => 'main',
          'allowed_push_workflows' => []
        }
      }
      write_json(root, '.github/merge-gate-policy.json', policy)
      write(root, '.github/scripts/pr-scope.py', "# trusted scope fixture\n")
      write(root, '.github/scripts/run-integration-shard.py', <<~PY)
        # "cargo", "metadata", "--no-deps", "--format-version", "1"
        # "test" not in target.get("kind", [])
        # integration target names must be workspace-unique before name-based sharding
        # integration shard partition is not an exact one-to-one cover
        command = ["cargo", "test", "--workspace", "--all-features"]
        command.extend(["--test", name])
      PY
      write(root, '.github/workflows/supply-chain.yml', <<~YAML)
        name: Supply chain
        on:
          pull_request:
          schedule:
            - cron: "17 18 * * 1"
        permissions:
          contents: read
          pull-requests: read
        jobs:
          supply-chain:
            name: supply-chain
            steps:
              - name: Check out exact trusted base for supply-chain applicability
                id: supply-trusted-base
                if: ${{ github.event_name == 'pull_request' && github.event.pull_request.draft == false }}
                continue-on-error: true
                uses: actions/checkout@0000000000000000000000000000000000000001
                with:
                  ref: ${{ github.event.pull_request.base.sha }}
                  path: .supply-trusted-base
                  fetch-depth: 1
                  persist-credentials: false
              - name: Classify supply-chain applicability from trusted base
                id: supply-impact
                if: ${{ github.event_name == 'pull_request' && github.event.pull_request.draft == false }}
                continue-on-error: true
                env:
                  GH_TOKEN: ${{ github.token }}
                  BASE_SHA: ${{ github.event.pull_request.base.sha }}
                  HEAD_SHA: ${{ github.event.pull_request.head.sha }}
                  PR_NUMBER: ${{ github.event.pull_request.number }}
                  TRUSTED_CHECKOUT: ${{ steps.supply-trusted-base.outcome }}
                run: |
                  echo 'impact=true' >> "$GITHUB_OUTPUT"
                  echo 'set -euo pipefail'
                  echo 'test "$TRUSTED_CHECKOUT" = "success"'
                  echo 'repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}'
                  echo '.base.sha == $base'
                  echo '.head.sha == $head'
                  echo '.base.repo.full_name == $repo'
                  echo '.changed_files > 0'
                  echo '--paginate --slurp'
                  echo 'pulls/${PR_NUMBER}/files?per_page=100'
                  echo '@tsv'
                  echo '.supply-trusted-base/.github/scripts/pr-scope.py'
                  echo '--repo-root .supply-trusted-base'
                  echo '--policy .github/merge-gate-policy.json'
                  echo '--expected-count "$expected_count"'
                  echo '--trusted-base-sha "$BASE_SHA"'
                  echo '.supply_chain_impact'
                  echo 'type) == "boolean"'
                  echo 'impact=$impact'
              - name: Documentation-only supply-chain fast path
                if: ${{ github.event_name == 'pull_request' && github.event.pull_request.draft == false && steps.supply-impact.outcome == 'success' && steps.supply-impact.outputs.impact == 'false' }}
                run: echo light
              - name: Test supply-chain exception policy
                if: ${{ github.event_name != 'pull_request' || (github.event.pull_request.draft == false && (steps.supply-impact.outcome != 'success' || steps.supply-impact.outputs.impact != 'false')) }}
                run: echo full
              - name: Validate supply-chain exception registry
                if: ${{ github.event_name != 'pull_request' || (github.event.pull_request.draft == false && (steps.supply-impact.outcome != 'success' || steps.supply-impact.outputs.impact != 'false')) }}
                run: echo full
              - name: Check advisories, licenses, bans, and sources
                if: ${{ github.event_name != 'pull_request' || (github.event.pull_request.draft == false && (steps.supply-impact.outcome != 'success' || steps.supply-impact.outputs.impact != 'false')) }}
                run: echo full
      YAML

      write(root, '.github/workflows/evidence.yml', <<~YAML)
        name: Evidence
        on:
          pull_request:
        permissions:
          contents: read
          pull-requests: read
        jobs:
          evidence-provenance:
            name: evidence-provenance
            steps:
              - name: Check out exact trusted base for evidence applicability
                id: evidence-trusted-base
                if: ${{ github.event.pull_request.draft == false }}
                continue-on-error: true
                uses: actions/checkout@0000000000000000000000000000000000000001
                with:
                  ref: ${{ github.event.pull_request.base.sha }}
                  path: .evidence-trusted-base
                  fetch-depth: 1
                  persist-credentials: false
              - name: Classify evidence applicability from trusted base
                id: evidence-impact
                if: ${{ github.event.pull_request.draft == false }}
                continue-on-error: true
                env:
                  GH_TOKEN: ${{ github.token }}
                  BASE_SHA: ${{ github.event.pull_request.base.sha }}
                  HEAD_SHA: ${{ github.event.pull_request.head.sha }}
                  PR_NUMBER: ${{ github.event.pull_request.number }}
                  TRUSTED_CHECKOUT: ${{ steps.evidence-trusted-base.outcome }}
                run: |
                  echo 'impact=true' >> "$GITHUB_OUTPUT"
                  echo 'set -euo pipefail'
                  echo 'test "$TRUSTED_CHECKOUT" = "success"'
                  echo 'repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}'
                  echo '.base.sha == $base'
                  echo '.head.sha == $head'
                  echo '.base.repo.full_name == $repo'
                  echo '.changed_files > 0'
                  echo '--paginate --slurp'
                  echo 'pulls/${PR_NUMBER}/files?per_page=100'
                  echo '@tsv'
                  echo '.evidence-trusted-base/.github/scripts/pr-scope.py'
                  echo '--repo-root .evidence-trusted-base'
                  echo '--policy .github/merge-gate-policy.json'
                  echo '--expected-count "$expected_count"'
                  echo '--trusted-base-sha "$BASE_SHA"'
                  echo '.evidence_impact'
                  echo 'type) == "boolean"'
                  echo 'impact=$impact'
              - name: Documentation-only evidence fast path
                if: ${{ github.event.pull_request.draft == false && steps.evidence-impact.outcome == 'success' && steps.evidence-impact.outputs.impact == 'false' }}
                run: echo light
              - name: Verify repository-wide retained evidence policy
                if: ${{ github.event_name != 'pull_request' || (github.event.pull_request.draft == false && (steps.evidence-impact.outcome != 'success' || steps.evidence-impact.outputs.impact != 'false')) }}
                run: echo full
              - name: Hydrate locked dependency graph for closure verification
                if: ${{ github.event_name != 'pull_request' || (github.event.pull_request.draft == false && (steps.evidence-impact.outcome != 'success' || steps.evidence-impact.outputs.impact != 'false')) }}
                run: echo full
              - name: Verify campaign dependency closure metadata
                if: ${{ github.event_name != 'pull_request' || (github.event.pull_request.draft == false && (steps.evidence-impact.outcome != 'success' || steps.evidence-impact.outputs.impact != 'false')) }}
                run: echo full
              - name: Verify retained campaign evidence integrity and provenance
                if: ${{ github.event_name != 'pull_request' || (github.event.pull_request.draft == false && (steps.evidence-impact.outcome != 'success' || steps.evidence-impact.outputs.impact != 'false')) }}
                run: echo full
      YAML

      write(root, '.github/workflows/fast-branch.yml', <<~YAML)
        name: Fast branch CI
        on:
          push:
            branches-ignore: [main]
        permissions:
          contents: read
        jobs:
          fast:
            name: fast
            runs-on: ubuntu-latest
            timeout-minutes: 15
            steps:
              - name: Check out repository
                uses: actions/checkout@0000000000000000000000000000000000000001
              - name: Check commit hygiene
                run: git log -1 --check
              - name: Resolve exact protected trusted base
                id: resolve-trusted-base
                continue-on-error: true
                run: |
                  echo 'base_sha=' >> "$GITHUB_OUTPUT"
                  set -euo pipefail
                  echo 'repos/${GITHUB_REPOSITORY}'
                  echo '.full_name == $repo and .default_branch == $branch'
                  echo 'repos/${GITHUB_REPOSITORY}/branches/${DEFAULT_BRANCH}'
                  echo '[[ "$base_sha" =~ ^[0-9a-f]{40}$ ]]'
                  echo '[[ "$HEAD_SHA" =~ ^[0-9a-f]{40}$ ]]'
                  echo 'base_sha=$base_sha'
              - name: Check out exact trusted base for docs-only classification
                id: fast-trusted-base
                if: ${{ steps.resolve-trusted-base.outcome == 'success' }}
                continue-on-error: true
                uses: actions/checkout@0000000000000000000000000000000000000001
                with:
                  ref: ${{ steps.resolve-trusted-base.outputs.base_sha }}
                  path: .fast-trusted-base
                  fetch-depth: 1
                  persist-credentials: false
              - name: Classify documentation-only scope from trusted base
                id: docs-only-scope
                if: ${{ steps.resolve-trusted-base.outcome == 'success' && steps.fast-trusted-base.outcome == 'success' }}
                continue-on-error: true
                run: |
                  echo 'docs_only=false' >> "$GITHUB_OUTPUT"
                  echo 'test "$TRUSTED_CHECKOUT" = "success"'
                  echo 'compare/${BASE_SHA}...${HEAD_SHA}'
                  echo '.head_commit.sha == $head'
                  echo '(.files | length) > 0'
                  echo '(.files | length) < 300'
                  echo '@tsv'
                  echo '.fast-trusted-base/.github/scripts/pr-scope.py'
                  echo '--repo-root .fast-trusted-base'
                  echo '--policy .github/merge-gate-policy.json'
                  echo '--expected-count "$expected_count"'
                  echo '--trusted-base-sha "$BASE_SHA"'
                  echo '.classification_valid == true'
                  echo '(.docs_only | type) == "boolean"'
                  echo 'docs_only=$docs_only'
              - name: Documentation-only fast path
                if: ${{ steps.docs-only-scope.outcome == 'success' && steps.docs-only-scope.outputs.docs_only == 'true' }}
                run: echo docs
              - name: Show toolchain
                if: ${{ steps.docs-only-scope.outcome != 'success' || steps.docs-only-scope.outputs.docs_only != 'true' }}
                run: rustup show
              - name: Check formatting
                if: ${{ steps.docs-only-scope.outcome != 'success' || steps.docs-only-scope.outputs.docs_only != 'true' }}
                run: cargo fmt --all -- --check
              - name: Run Clippy
                if: ${{ steps.docs-only-scope.outcome != 'success' || steps.docs-only-scope.outputs.docs_only != 'true' }}
                run: cargo clippy --workspace --all-targets --all-features --
              - name: Run workspace unit tests
                if: ${{ steps.docs-only-scope.outcome != 'success' || steps.docs-only-scope.outputs.docs_only != 'true' }}
                run: cargo test --workspace --all-features --lib
      YAML
      write(root, '.github/workflows/ci.yml', <<~YAML)
        name: Rust
        on:
          pull_request:
            branches: [main]
        jobs:
          quality-fast:
            name: quality-fast-internal
            if: ${{ github.event.pull_request.draft == false }}
            runs-on: ubuntu-latest
            steps:
              - name: Resolve exact-SHA Fast evidence
                id: fast-evidence
                run: |
                  echo "mode=none" >> "$GITHUB_OUTPUT"
                  echo 'actions/workflows/fast-branch.yml/runs?event=push&head_sha=$EXPECTED_SHA&per_page=100'
                  echo 'mode=full'
                  echo 'mode=docs-only'
                  echo "step_conclusion 'Documentation-only fast path'"
                  echo "step_conclusion 'Check formatting'"
                  echo "step_conclusion 'Run Clippy'"
                  echo "step_conclusion 'Run workspace unit tests'"
              - name: Check diff hygiene
                run: git diff --check
              - name: Check formatting
                if: ${{ (github.event_name != 'pull_request' || github.event.pull_request.draft == false) && steps.fast-evidence.outputs.mode != 'full'  && (steps.docs-only-scope.outcome != 'success' || steps.docs-only-scope.outputs.docs_only != 'true') }}
                run: cargo fmt --all -- --check
              - name: Run Clippy
                if: ${{ (github.event_name != 'pull_request' || github.event.pull_request.draft == false) && steps.fast-evidence.outputs.mode != 'full'  && (steps.docs-only-scope.outcome != 'success' || steps.docs-only-scope.outputs.docs_only != 'true') }}
                run: cargo clippy --workspace --all-targets --all-features --
              - name: Run workspace unit tests
                if: ${{ (github.event_name != 'pull_request' || github.event.pull_request.draft == false) && steps.fast-evidence.outputs.mode != 'full'  && (steps.docs-only-scope.outcome != 'success' || steps.docs-only-scope.outputs.docs_only != 'true') }}
                run: cargo test --workspace --all-features --lib
              - name: Verify narrow audit-shape Clippy exceptions
                run: cargo clippy -p oxide-batch-xtask --all-targets --all-features --message-format=json --
          quality-integration-0:
            name: quality-integration-0-internal
            if: ${{ github.event.pull_request.draft == false }}
            runs-on: ubuntu-latest
            steps:
              - run: python3 .github/scripts/run-integration-shard.py 0 4
          quality-integration-1:
            name: quality-integration-1-internal
            if: ${{ github.event.pull_request.draft == false }}
            runs-on: ubuntu-latest
            steps:
              - run: python3 .github/scripts/run-integration-shard.py 1 4
          quality-integration-2:
            name: quality-integration-2-internal
            if: ${{ github.event.pull_request.draft == false }}
            runs-on: ubuntu-latest
            steps:
              - run: python3 .github/scripts/run-integration-shard.py 2 4
          quality-integration-3:
            name: quality-integration-3-internal
            if: ${{ github.event.pull_request.draft == false }}
            runs-on: ubuntu-latest
            steps:
              - run: python3 .github/scripts/run-integration-shard.py 3 4
          quality-bin-doc:
            name: quality-bin-doc-internal
            if: ${{ github.event.pull_request.draft == false }}
            runs-on: ubuntu-latest
            steps:
              - run: cargo test --workspace --all-features --bins
              - run: cargo test --workspace --all-features --doc
          quality-contracts:
            name: quality-contracts-internal
            if: ${{ github.event.pull_request.draft == false }}
            runs-on: ubuntu-latest
            steps:
              - run: cargo check -p oxide-batch --no-default-features
              - run: cargo check -p oxide-batch-cli --no-default-features --all-targets
              - run: cargo doc --workspace --all-features --no-deps
              - run: cargo run --package oxide-batch-xtask -- deps
              - run: cargo run --package oxide-batch-xtask -- surface
              - run: cargo run --package oxide-batch-xtask -- release-crates
          quality:
            name: quality
            needs: [quality-fast, quality-integration-0, quality-integration-1, quality-integration-2, quality-integration-3, quality-bin-doc, quality-contracts]
            if: ${{ always() }}
            runs-on: ubuntu-latest
            timeout-minutes: 5
            steps:
              - name: Check out repository
                uses: actions/checkout@0000000000000000000000000000000000000001
              - name: Test trusted PR scope classifier
                run: >-
                  python3 .github/scripts/pr-scope.py
                  --repo-root .
                  --policy .github/merge-gate-policy.json
                  --self-test
              - name: Require all quality components
                env:
                  FAST_RESULT: ${{ needs.quality-fast.result }}
                  INTEGRATION_0_RESULT: ${{ needs.quality-integration-0.result }}
                  INTEGRATION_1_RESULT: ${{ needs.quality-integration-1.result }}
                  INTEGRATION_2_RESULT: ${{ needs.quality-integration-2.result }}
                  INTEGRATION_3_RESULT: ${{ needs.quality-integration-3.result }}
                  BIN_DOC_RESULT: ${{ needs.quality-bin-doc.result }}
                  CONTRACTS_RESULT: ${{ needs.quality-contracts.result }}
                run: |
                  results=( "$FAST_RESULT" "$INTEGRATION_0_RESULT" "$INTEGRATION_1_RESULT" "$INTEGRATION_2_RESULT" "$INTEGRATION_3_RESULT" "$BIN_DOC_RESULT" "$CONTRACTS_RESULT" )
                  for result in "${results[@]}"; do
                    if [ "$result" != "success" ]; then
                      exit 1
                    fi
                  done
          postgres:
            name: postgres-${{ matrix.postgres }}-repository
            strategy:
              matrix:
                postgres: ["15", "18"]
            runs-on: ubuntu-latest
          postgresql-merge-gate:
            name: postgresql
            if: ${{ always() }}
            needs: [postgres]
            runs-on: ubuntu-latest
            timeout-minutes: 5
            permissions:
              actions: read
              contents: read
            steps:
              - name: Check out repository
                uses: actions/checkout@0000000000000000000000000000000000000001
              - name: Evaluate selective-rerun-safe PostgreSQL aggregate
                env:
                  GITHUB_TOKEN: ${{ github.token }}
                run: ruby .github/scripts/evaluate-aggregate-run.rb postgresql
      YAML
      write(root, '.github/workflows/pr-labeler.yml', <<~YAML)
        name: Pull request labels
        on:
          pull_request_target:
            types: [opened, edited, synchronize, reopened, ready_for_review]
        permissions: {}
        jobs:
          merge-gate:
            name: merge-gate
            runs-on: ubuntu-latest
            timeout-minutes: 20
            permissions:
              actions: read
              contents: read
              pull-requests: read
            steps:
              - name: Evaluate base-trusted repository merge authority
                env:
                  GITHUB_TOKEN: ${{ github.token }}
                  BASE_SHA: ${{ github.event.pull_request.base.sha }}
                  HEAD_SHA: ${{ github.event.pull_request.head.sha }}
                  PR_NUMBER: ${{ github.event.pull_request.number }}
                  PR_DRAFT: ${{ github.event.pull_request.draft }}
                run: |
                  echo 'contents/.github/merge-gate-policy.json'
                  echo '{"ref": base_sha}'
                  echo 'repository_merge_gate'
                  echo 'pr.get("base", {}).get("sha") != base_sha'
                  echo 'pr.get("head", {}).get("sha") != head_sha'
                  echo 'pr.get("base", {}).get("repo", {}).get("full_name") != repository'
                  echo 'actions/workflows/{workflow_id}/runs'
                  echo '"event": "pull_request"'
                  echo '"head_sha": head_sha'
                  echo 'linked_pr.get("number") == pr_number_int'
                  echo 'actions/runs/{run_id}/jobs'
                  echo '"filter": "all"'
                  echo 'run_attempt'
                  echo 'duplicate member jobs at latest run_attempt'
                  echo 'status != "completed"'
                  echo 'conclusion != "success"'
                  echo 'timed out waiting for exact-head merge authority'
      YAML
      write(root, '.github/workflows/m5-conformance.yml', <<~YAML)
        name: M5 Conformance
        on:
          pull_request:
            branches: [main]
          workflow_dispatch:
        permissions:
          contents: read
          pull-requests: read
        jobs:
          route:
            if: ${{ github.event_name == 'pull_request' && github.event.pull_request.draft == false }}
            name: trusted-campaign-route
            runs-on: ubuntu-24.04
            timeout-minutes: 5
            outputs:
              classification_outcome: ${{ steps.classify.outcome }}
              direct_workflows: ${{ steps.classify.outputs.direct_workflows }}
            steps:
              - name: Check out exact trusted base
                id: trusted-base
                continue-on-error: true
                uses: actions/checkout@0000000000000000000000000000000000000002
                with:
                  ref: ${{ github.event.pull_request.base.sha }}
                  path: .trusted-base
                  fetch-depth: 1
                  persist-credentials: false
              - name: Classify direct-proof campaigns from trusted base
                id: classify
                continue-on-error: true
                env:
                  GH_TOKEN: ${{ github.token }}
                  BASE_SHA: ${{ github.event.pull_request.base.sha }}
                  HEAD_SHA: ${{ github.event.pull_request.head.sha }}
                  PR_NUMBER: ${{ github.event.pull_request.number }}
                  TRUSTED_CHECKOUT: ${{ steps.trusted-base.outcome }}
                run: |
                  echo 'direct_workflows=[]' >> "$GITHUB_OUTPUT"
                  set -euo pipefail
                  test "$TRUSTED_CHECKOUT" = "success"
                  gh api "repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}"
                  echo '.base.sha == $base .head.sha == $head .base.repo.full_name == $repo .changed_files > 0'
                  gh api --paginate --slurp "repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}/files?per_page=100"
                  echo '@tsv'
                  python3 .trusted-base/.github/scripts/pr-scope.py \
                    --repo-root .trusted-base \
                    --policy .github/merge-gate-policy.json \
                    --expected-count "$expected_count" \
                    --trusted-base-sha "$BASE_SHA"
                  echo '.direct_proof_campaign_workflows'
                  echo "direct_workflows=$direct_workflows" >> "$GITHUB_OUTPUT"
          conformance-shard-15:
            name: deep-postgres-15-conformance-shard-${{ matrix.shard }}
            needs: route
            if: ${{ always() && (github.event_name == 'workflow_dispatch' || (github.event_name == 'pull_request' && github.event.pull_request.draft == false && (needs.route.result != 'success' || needs.route.outputs.classification_outcome != 'success' || contains(needs.route.outputs.direct_workflows, '.github/workflows/m5-conformance.yml')))) }}
            runs-on: ubuntu-latest
            strategy:
              matrix:
                shard: [0, 1]
            services:
              postgres:
                image: postgres:15
            steps:
              - run: ./tests/fixtures/conformance/verify-ci-contract.sh .github/workflows/m5-conformance.yml
              - name: Run PostgreSQL 15 conformance shard
                env:
                  SHARD_INDEX: ${{ matrix.shard }}
                run: ./tests/fixtures/conformance/run-ci-campaign.sh 15 "$SHARD_INDEX" 2
              - uses: actions/upload-artifact@0000000000000000000000000000000000000003
                with:
                  name: conformance-shard-postgres-15-${{ matrix.shard }}
                  path: target/m5-campaigns/conformance-shard-${{ matrix.shard }}.json
                  if-no-files-found: error
          conformance-shard-18:
            name: deep-postgres-18-conformance-shard-${{ matrix.shard }}
            needs: route
            if: ${{ always() && (github.event_name == 'workflow_dispatch' || (github.event_name == 'pull_request' && github.event.pull_request.draft == false && (needs.route.result != 'success' || needs.route.outputs.classification_outcome != 'success' || contains(needs.route.outputs.direct_workflows, '.github/workflows/m5-conformance.yml')))) }}
            runs-on: ubuntu-latest
            strategy:
              matrix:
                shard: [0, 1]
            services:
              postgres:
                image: postgres:18
            steps:
              - run: ./tests/fixtures/conformance/verify-ci-contract.sh .github/workflows/m5-conformance.yml
              - name: Run PostgreSQL 18 conformance shard
                env:
                  SHARD_INDEX: ${{ matrix.shard }}
                run: ./tests/fixtures/conformance/run-ci-campaign.sh 18 "$SHARD_INDEX" 2
              - uses: actions/upload-artifact@0000000000000000000000000000000000000003
                with:
                  name: conformance-shard-postgres-18-${{ matrix.shard }}
                  path: target/m5-campaigns/conformance-shard-${{ matrix.shard }}.json
                  if-no-files-found: error
          conformance-deep-15:
            name: deep-postgres-15-conformance-campaign
            needs: [route, conformance-shard-15]
            if: ${{ always() && (github.event_name == 'workflow_dispatch' || (github.event_name == 'pull_request' && github.event.pull_request.draft == false && (needs.route.result != 'success' || needs.route.outputs.classification_outcome != 'success' || contains(needs.route.outputs.direct_workflows, '.github/workflows/m5-conformance.yml')))) }}
            runs-on: ubuntu-latest
            steps:
              - run: ./tests/fixtures/conformance/verify-ci-contract.sh .github/workflows/m5-conformance.yml
              - uses: actions/download-artifact@0000000000000000000000000000000000000004
                with:
                  pattern: conformance-shard-postgres-15-*
                  path: target/m5-campaign-shards
                  merge-multiple: true
              - run: bash ./tests/fixtures/conformance/merge-ci-campaign.sh 15 2 target/m5-campaign-shards
              - uses: actions/upload-artifact@0000000000000000000000000000000000000003
                with:
                  name: conformance-campaign-postgres-15
                  path: target/m5-campaigns/conformance-campaign.json
                  if-no-files-found: error
          conformance-deep-18:
            name: deep-postgres-18-conformance-campaign
            needs: [route, conformance-shard-18]
            if: ${{ always() && (github.event_name == 'workflow_dispatch' || (github.event_name == 'pull_request' && github.event.pull_request.draft == false && (needs.route.result != 'success' || needs.route.outputs.classification_outcome != 'success' || contains(needs.route.outputs.direct_workflows, '.github/workflows/m5-conformance.yml')))) }}
            runs-on: ubuntu-latest
            steps:
              - run: ./tests/fixtures/conformance/verify-ci-contract.sh .github/workflows/m5-conformance.yml
              - uses: actions/download-artifact@0000000000000000000000000000000000000004
                with:
                  pattern: conformance-shard-postgres-18-*
                  path: target/m5-campaign-shards
                  merge-multiple: true
              - run: bash ./tests/fixtures/conformance/merge-ci-campaign.sh 18 2 target/m5-campaign-shards
              - uses: actions/upload-artifact@0000000000000000000000000000000000000003
                with:
                  name: conformance-campaign-postgres-18
                  path: target/m5-campaigns/conformance-campaign.json
                  if-no-files-found: error
          conformance-campaign:
            name: postgres-${{ matrix.postgres }}-conformance-campaign
            needs: [route, conformance-deep-15, conformance-deep-18]
            if: ${{ always() }}
            runs-on: ubuntu-latest
            strategy:
              matrix:
                postgres: ["15", "18"]
            steps:
              - name: Emit required conformance context
                env:
                  POSTGRES: ${{ matrix.postgres }}
                  DEEP_15_RESULT: ${{ needs.conformance-deep-15.result }}
                  DEEP_18_RESULT: ${{ needs.conformance-deep-18.result }}
                run: |
                  ROUTE_RESULT=x
                  CLASSIFICATION_OUTCOME=x
                  DIRECT_REQUIRED=x
                  echo "M5 conformance is deferred until the pull request is ready for review"
                  case "$POSTGRES" in
                    15) DEEP_RESULT="$DEEP_15_RESULT" ;;
                    18) DEEP_RESULT="$DEEP_18_RESULT" ;;
                  esac
                  if [[ "$DEEP_RESULT" != "success" ]]; then exit 1; fi
                  if [[ "$DEEP_RESULT" != "skipped" ]]; then exit 1; fi
          postgresql-conformance-merge-gate:
            name: postgresql-conformance
            if: ${{ always() }}
            needs: [conformance-campaign]
            runs-on: ubuntu-latest
            timeout-minutes: 5
            permissions:
              actions: read
              contents: read
            steps:
              - name: Check out repository
                uses: actions/checkout@0000000000000000000000000000000000000002
              - name: Evaluate selective-rerun-safe PostgreSQL aggregate
                env:
                  GITHUB_TOKEN: ${{ github.token }}
                run: ruby .github/scripts/evaluate-aggregate-run.rb postgresql-conformance
      YAML
      write_json(root, 'docs/engineering/retained-evidence-policy.json', {
        'artifact_producers' => [
          {'workflow' => '.github/workflows/m5-conformance.yml'},
          {'workflow' => '.github/workflows/m5-soak.yml'}
        ]
      })
      write(root, '.github/workflows/m5-soak.yml', <<~YAML)
        name: M5 Soak
        on:
          workflow_call:
          workflow_dispatch:
        jobs:
          soak-campaign:
            name: soak
            runs-on: ubuntu-latest
      YAML
      write(root, '.github/workflows/campaign-orchestrator.yml', <<~YAML)
        name: Campaign Orchestrator
        on:
          pull_request:
            branches: [main]
        permissions:
          contents: read
          pull-requests: read
        jobs:
          route:
            if: ${{ github.event.pull_request.draft == false }}
            name: trusted-campaign-route
            runs-on: ubuntu-24.04
            timeout-minutes: 5
            outputs:
              classification_outcome: ${{ steps.classify.outcome }}
              direct_workflows: ${{ steps.classify.outputs.direct_workflows }}
            steps:
              - name: Check out exact trusted base
                id: trusted-base
                continue-on-error: true
                uses: actions/checkout@0000000000000000000000000000000000000001
                with:
                  ref: ${{ github.event.pull_request.base.sha }}
                  path: .trusted-base
                  fetch-depth: 1
                  persist-credentials: false
              - name: Classify direct-proof campaigns from trusted base
                id: classify
                continue-on-error: true
                env:
                  GH_TOKEN: ${{ github.token }}
                  BASE_SHA: ${{ github.event.pull_request.base.sha }}
                  HEAD_SHA: ${{ github.event.pull_request.head.sha }}
                  PR_NUMBER: ${{ github.event.pull_request.number }}
                  TRUSTED_CHECKOUT: ${{ steps.trusted-base.outcome }}
                run: |
                  echo 'direct_workflows=[]' >> "$GITHUB_OUTPUT"
                  set -euo pipefail
                  test "$TRUSTED_CHECKOUT" = "success"
                  gh api "repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}"
                  echo '.base.sha == $base .head.sha == $head .base.repo.full_name == $repo .changed_files > 0'
                  gh api --paginate --slurp "repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}/files?per_page=100"
                  echo '@tsv'
                  python3 .trusted-base/.github/scripts/pr-scope.py \
                    --repo-root .trusted-base \
                    --policy .github/merge-gate-policy.json \
                    --expected-count "$expected_count" \
                    --trusted-base-sha "$BASE_SHA"
                  echo '.direct_proof_campaign_workflows'
                  echo "direct_workflows=$direct_workflows" >> "$GITHUB_OUTPUT"
          m5_soak:
            needs: route
            if: ${{ always() && github.event.pull_request.draft == false && (needs.route.result != 'success' || needs.route.outputs.classification_outcome != 'success' || contains(needs.route.outputs.direct_workflows, '.github/workflows/m5-soak.yml')) }}
            uses: ./.github/workflows/m5-soak.yml
      YAML
      write(root, '.github/workflows/deep-soak.yml', <<~YAML)
        name: Deep
        on:
          pull_request:
        jobs:
          soak:
            name: soak
            if: github.repository_owner == 'example'
            runs-on: ubuntu-latest
      YAML
      write_json(root, 'ruleset.json', ruleset_with(*LEGACY))
      yield root, policy
    end
  end

  def verify(root)
    MergeGateVerifier.verify(
      root: root,
      policy_path: File.join(root, '.github/merge-gate-policy.json'),
      ruleset_path: File.join(root, 'ruleset.json')
    ).first
  end

  def test_clean_candidate_policy
    with_repo { |root, _policy| assert_empty verify(root) }
  end

  def test_pr_scope_self_test_removal_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub(
        /\n      - name: Test trusted PR scope classifier\n        run: >-\n(?:          .*\n){4}/,
        "\n"
      )
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'canonical self-test'
    end
  end

  def test_pr_scope_trusted_tree_contract_drift_is_rejected
    with_repo do |root, policy|
      policy['pr_scope']['trusted_tree_contract'] = 'head-tree'
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join('\n'), 'trusted_tree_contract'
    end
  end

  def test_pr_scope_classifier_file_removal_is_rejected
    with_repo do |root, _policy|
      FileUtils.rm(File.join(root, '.github/scripts/pr-scope.py'))
      assert_includes verify(root).join('\n'), 'classifier .github/scripts/pr-scope.py is missing'
    end
  end

  def test_pr_scope_global_direct_proof_path_removal_is_rejected
    with_repo do |root, policy|
      policy['pr_scope']['global_direct_proof_paths'].delete('.github/workflows/campaign-orchestrator.yml')
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join('\n'), 'global_direct_proof_paths must exactly match canonical routing control-plane paths'
    end
  end

  def test_pr_scope_global_direct_proof_path_extra_is_rejected
    with_repo do |root, policy|
      policy['pr_scope']['global_direct_proof_paths'] << '.github/workflows/ci.yml'
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join('\n'), 'global_direct_proof_paths must exactly match canonical routing control-plane paths'
    end
  end

  def test_pr_scope_global_direct_proof_overlap_is_rejected
    with_repo do |root, policy|
      policy['pr_scope']['global_campaign_paths'] << '.github/scripts/pr-scope.py'
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join('\n'), 'global_direct_proof_paths must not overlap global_campaign_paths'
    end
  end

  def test_main_push_validation_is_rejected
    with_repo do |root, _policy|
      write(root, '.github/workflows/post-main.yml', <<~YAML)
        name: Post-main validation
        on:
          push:
            branches: [main]
        jobs:
          validate:
            runs-on: ubuntu-latest
      YAML
      assert_includes verify(root).join('\n'), 'targets push to main'
    end
  end

  def test_main_push_ignore_is_allowed
    with_repo do |root, _policy|
      write(root, '.github/workflows/feature-push.yml', <<~YAML)
        name: Feature push
        on:
          push:
            branches-ignore: [main]
        jobs:
          validate:
            runs-on: ubuntu-latest
      YAML
      refute_includes verify(root).join('\n'), 'feature-push.yml targets push to main'
    end
  end

  def test_tag_only_push_is_allowed
    with_repo do |root, _policy|
      write(root, '.github/workflows/release-tag.yml', <<~YAML)
        name: Release tag
        on:
          push:
            tags: ['v*']
        jobs:
          release:
            runs-on: ubuntu-latest
      YAML
      refute_includes verify(root).join('\n'), 'release-tag.yml targets push to main'
    end
  end

  def test_required_job_rename_is_detected_as_ruleset_drift
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      write(
        root,
        '.github/workflows/ci.yml',
        File.read(path).sub("\n  quality:\n    name: quality\n", "\n  quality:\n    name: quality-renamed\n")
      )
      assert_includes verify(root).join('\n'), 'quality-renamed'
    end
  end

  def test_required_job_removal_makes_live_context_stale
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      body = File.read(path).sub(/\n  quality:\n.*?(?=\n  postgres:)/m, "\n")
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'quality'
    end
  end

  def test_policy_required_context_missing_from_ruleset
    with_repo do |root, _policy|
      write_json(root, 'ruleset.json', ruleset_with(*(LEGACY - ['postgres-18-repository'])))
      assert_includes verify(root).join('\n'), 'postgres-18-repository'
    end
  end

  def test_stale_live_context_is_rejected
    with_repo do |root, _policy|
      write_json(root, 'ruleset.json', ruleset_with(*(LEGACY + ['old-job'])))
      assert_includes verify(root).join('\n'), 'old-job'
    end
  end

  def test_required_workflow_path_filter_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub('branches: [main]', "branches: [main]\n    paths: ['src/**']")
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'path filters'
    end
  end

  def test_required_workflow_pull_request_target_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub('pull_request:', 'pull_request_target:')
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'must not use pull_request_target'
    end
  end

  def test_required_job_condition_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub(
        "  quality:\n    name: quality\n    needs: [quality-fast, quality-integration-0, quality-integration-1, quality-integration-2, quality-integration-3, quality-bin-doc, quality-contracts]\n    if: ${{ always() }}",
        "  quality:\n    name: quality\n    needs: [quality-fast, quality-integration-0, quality-integration-1, quality-integration-2, quality-integration-3, quality-bin-doc, quality-contracts]\n    if: ${{ always() && github.actor != 'nobody' }}"
      )
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'not guaranteed to emit'
    end
  end

  def test_quality_aggregate_dependency_removal_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub(
        'needs: [quality-fast, quality-integration-0, quality-integration-1, quality-integration-2, quality-integration-3, quality-bin-doc, quality-contracts]',
        'needs: [quality-fast, quality-integration-0, quality-integration-1, quality-integration-2, quality-bin-doc, quality-contracts]'
      )
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'must depend on every parallel quality component'
    end
  end

  def test_quality_component_command_removal_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub('python3 .github/scripts/run-integration-shard.py 2 4', 'echo omitted')
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'is missing quality obligations'
    end
  end

  def test_quality_aggregate_result_binding_removal_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub(
        'INTEGRATION_3_RESULT: ${{ needs.quality-integration-3.result }}',
        'INTEGRATION_3_RESULT: success'
      )
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'must bind every component result exactly'
    end
  end

  def test_quality_integration_shard_exact_cover_guard_is_required
    with_repo do |root, _policy|
      path = File.join(root, '.github/scripts/run-integration-shard.py')
      original = File.read(path)
      body = original.sub('integration shard partition is not an exact one-to-one cover', 'weakened')
      refute_equal original, body
      write(root, '.github/scripts/run-integration-shard.py', body)
      assert_includes verify(root).join('\n'), 'missing fail-closed shard contract'
    end
  end

  def test_repository_merge_gate_missing_member_is_rejected
    with_repo do |root, policy|
      policy['repository_merge_gate']['members'].reject! { |member| member['context'] == 'quality' }
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join("\n"), 'repository merge gate member inventory mismatch'
    end
  end

  def test_repository_merge_gate_wrong_source_workflow_is_rejected
    with_repo do |root, policy|
      member = policy['repository_merge_gate']['members'].find { |entry| entry['context'] == 'quality' }
      member['workflow'] = '.github/workflows/m5-conformance.yml'
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join("\n"), 'quality must bind source workflow'
    end
  end

  def test_repository_merge_gate_pull_request_trigger_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/pr-labeler.yml')
      original = File.read(path)
      body = original.sub('pull_request_target:', 'pull_request:')
      refute_equal original, body
      write(root, '.github/workflows/pr-labeler.yml', body)
      assert_includes verify(root).join("\n"), 'must execute from protected-base pull_request_target authority'
    end
  end

  def test_repository_merge_gate_checkout_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/pr-labeler.yml')
      original = File.read(path)
      body = original.sub(
        "    steps:\n      - name: Evaluate base-trusted repository merge authority",
        "    steps:\n      - uses: actions/checkout@0000000000000000000000000000000000000001\n      - name: Evaluate base-trusted repository merge authority"
      )
      refute_equal original, body
      write(root, '.github/workflows/pr-labeler.yml', body)
      assert_includes verify(root).join("\n"), 'must contain exactly one inline evaluator step'
    end
  end

  def test_repository_merge_gate_success_check_removal_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/pr-labeler.yml')
      original = File.read(path)
      body = original.sub('conclusion != "success"', 'conclusion is ignored')
      refute_equal original, body
      write(root, '.github/workflows/pr-labeler.yml', body)
      assert_includes verify(root).join("\n"), 'is missing fail-closed contract tokens'
    end
  end

  def test_repository_merge_gate_candidate_must_be_pending
    with_repo do |root, policy|
      policy['pending_ruleset_contexts'].delete('merge-gate')
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join("\n"), 'candidate repository merge gate merge-gate must be pending'
    end
  end

  def test_repository_merge_gate_cutover_accepts_single_gate_topology
    with_repo do |root, policy|
      policy['aggregate_gates'].each { |gate| gate['state'] = 'active' }
      policy['pending_ruleset_contexts'] = ['merge-gate']
      policy['repository_merge_gate']['state'] = 'cutover'
      write_json(root, '.github/merge-gate-policy.json', policy)
      write_json(root, 'ruleset.json', ruleset_with('merge-gate'))
      assert_empty verify(root)
    end
  end

  def test_repository_merge_gate_active_requires_single_gate_topology
    with_repo do |root, policy|
      policy['aggregate_gates'].each { |gate| gate['state'] = 'active' }
      policy['pending_ruleset_contexts'] = []
      policy['repository_merge_gate']['state'] = 'active'
      write_json(root, '.github/merge-gate-policy.json', policy)
      write_json(root, 'ruleset.json', ruleset_with('merge-gate'))
      assert_empty verify(root)
    end
  end

  def test_repository_merge_gate_active_rejects_legacy_topology
    with_repo do |root, policy|
      policy['aggregate_gates'].each { |gate| gate['state'] = 'active' }
      policy['pending_ruleset_contexts'] = []
      policy['repository_merge_gate']['state'] = 'active'
      write_json(root, '.github/merge-gate-policy.json', policy)
      write_json(root, 'ruleset.json', ruleset_with(*FINAL))
      assert_includes verify(root).join("\n"), 'live ruleset requires stale/unaccepted contexts'
    end
  end

  def test_repository_merge_gate_context_spoof_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub(
        "  quality-fast:\n    name: quality-fast-internal",
        "  quality-fast:\n    name: merge-gate"
      )
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join("\n"), 'repository merge gate context must have exactly one canonical producer'
    end
  end

  def test_repository_merge_gate_pr_identity_binding_removal_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/pr-labeler.yml')
      original = File.read(path)
      body = original.sub(
        'linked_pr.get("number") == pr_number_int',
        'linked PR identity binding removed'
      )
      refute_equal original, body
      write(root, '.github/workflows/pr-labeler.yml', body)
      assert_includes verify(root).join("\n"), 'is missing fail-closed contract tokens'
    end
  end

  def test_matrix_context_set_mismatch_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub('["15", "18"]', '["15", "17", "18"]')
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'postgres-17-repository'
    end
  end

  def test_new_unclassified_pr_workflow_is_rejected
    with_repo do |root, _policy|
      write(root, '.github/workflows/new.yml', <<~YAML)
        name: New
        on: [pull_request]
        jobs:
          new-job:
            runs-on: ubuntu-latest
      YAML
      assert_includes verify(root).join('\n'), 'unclassified'
    end
  end

  def test_managed_required_context_needs_no_checked_in_producer
    with_repo { |root, _policy| refute_includes verify(root).join('\n'), 'Analyze (actions)' }
  end

  def test_pending_context_must_still_be_required
    with_repo do |root, policy|
      policy['pending_ruleset_contexts'] = ['does-not-exist']
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join('\n'), 'not required producers'
    end
  end

  def test_dangling_job_override_is_rejected
    with_repo do |root, policy|
      policy['job_overrides'] << {
        'workflow' => '.github/workflows/ci.yml',
        'job' => 'missing-job',
        'classification' => 'required'
      }
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join('\n'), 'job override references missing PR job'
    end
  end

  def test_candidate_aggregate_must_be_pending
    with_repo do |root, policy|
      policy['pending_ruleset_contexts'].delete('postgresql')
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join('\n'), 'candidate aggregate postgresql must be pending'
    end
  end

  def test_cutover_aggregate_must_be_pending
    with_repo do |root, policy|
      policy['aggregate_gates'].each { |gate| gate['state'] = 'cutover' }
      policy['pending_ruleset_contexts'].delete('postgresql')
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join('\n'), 'cutover aggregate postgresql must be pending'
    end
  end

  def test_atomic_cutover_accepts_legacy_and_final_topologies
    with_repo do |root, policy|
      policy['aggregate_gates'].each { |gate| gate['state'] = 'cutover' }
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_empty verify(root)

      write_json(root, 'ruleset.json', ruleset_with(*FINAL))
      assert_empty verify(root)
    end
  end

  def test_atomic_cutover_rejects_partial_group_replacement
    with_repo do |root, policy|
      policy['aggregate_gates'].each { |gate| gate['state'] = 'cutover' }
      write_json(root, '.github/merge-gate-policy.json', policy)
      partial = [
        'Analyze (actions)',
        'quality',
        'postgresql',
        'postgres-15-conformance-campaign',
        'postgres-18-conformance-campaign'
      ]
      write_json(root, 'ruleset.json', ruleset_with(*partial))
      refute_empty verify(root)
    end
  end

  def test_migration_group_cannot_mix_states
    with_repo do |root, policy|
      policy['aggregate_gates'][0]['state'] = 'cutover'
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join('\n'), 'migration group postgresql has mixed states'
    end
  end

  def test_active_aggregates_require_final_topology
    with_repo do |root, policy|
      policy['aggregate_gates'].each { |gate| gate['state'] = 'active' }
      policy['pending_ruleset_contexts'] = ['merge-gate']
      write_json(root, '.github/merge-gate-policy.json', policy)
      write_json(root, 'ruleset.json', ruleset_with(*FINAL))
      assert_empty verify(root)
    end
  end

  def test_aggregate_member_removal_is_fail_closed
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      body = File.read(path).sub('["15", "18"]', '["15"]')
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'members are not required producers: postgres-18-repository'
    end
  end

  def test_aggregate_members_must_share_producer_workflow
    with_repo do |root, policy|
      policy['aggregate_gates'][0]['members'] << 'postgres-15-conformance-campaign'
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join('\n'), 'members must all be produced'
    end
  end

  def test_aggregate_producer_must_emit_exact_context
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      body = File.read(path).sub("name: postgresql\n", "name: postgresql-renamed\n")
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'must emit exact job context'
    end
  end

  def test_aggregate_producer_workflow_path_filter_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub('branches: [main]', "branches: [main]\n    paths-ignore: ['docs/**']")
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes(
        verify(root).join('\n'),
        'aggregate postgresql producer workflow .github/workflows/ci.yml can suppress pull_request via path filters'
      )
    end
  end

  def test_aggregate_producer_must_use_always
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      body = File.read(path).sub(
        "  postgresql-merge-gate:\n    name: postgresql\n    if: ${{ always() }}",
        "  postgresql-merge-gate:\n    name: postgresql\n    if: success()"
      )
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'must use if:'
    end
  end

  def test_aggregate_needs_must_match_member_job_ids
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      body = File.read(path).sub('needs: [postgres]', 'needs: [quality]')
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'needs mismatch'
    end
  end

  def test_aggregate_evaluator_invocation_must_match_canonical_script
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub(
        'run: ruby .github/scripts/evaluate-aggregate-run.rb postgresql',
        'run: ruby .github/scripts/evaluate-aggregate-run.rb postgresql-typo'
      )
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'canonical evaluator invocation'
    end
  end

  def test_aggregate_evaluator_step_requires_github_token_env
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub("        env:\n          GITHUB_TOKEN: ${{ github.token }}\n", '')
      refute_equal original, body
      refute_includes body, 'GITHUB_TOKEN'
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'canonical evaluator invocation'
    end
  end

  def test_aggregate_producer_must_declare_least_privilege_permissions
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub("    permissions:\n      actions: read\n      contents: read\n", '')
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'least-privilege permissions'
    end
  end

  def test_aggregate_producer_permissions_cannot_grant_write
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub('actions: read', 'actions: write')
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'least-privilege permissions'
    end
  end

  def test_aggregate_producer_checkout_must_reuse_pinned_sha
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub(
        "      - name: Check out repository\n        uses: actions/checkout@0000000000000000000000000000000000000001\n      - name: Evaluate",
        "      - name: Check out repository\n        uses: actions/checkout@1111111111111111111111111111111111111111\n      - name: Evaluate"
      )
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'must reuse the pinned'
    end
  end

  def test_aggregate_producer_step_count_is_bounded
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub(
        "        run: ruby .github/scripts/evaluate-aggregate-run.rb postgresql\n",
        "        run: ruby .github/scripts/evaluate-aggregate-run.rb postgresql\n      - name: Extra step\n        run: echo hi\n"
      )
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'exactly a checkout step and an evaluator step'
    end
  end

  def test_aggregate_producer_shape_is_bounded
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      body = File.read(path).sub(
        "  postgresql-merge-gate:\n    name: postgresql\n    if: ${{ always() }}\n    needs: [postgres]\n    runs-on: ubuntu-latest\n    timeout-minutes: 5",
        "  postgresql-merge-gate:\n    name: postgresql\n    if: ${{ always() }}\n    needs: [postgres]\n    runs-on: ubuntu-latest\n    timeout-minutes: 30"
      )
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'ubuntu-latest with timeout-minutes: 5'
    end
  end

  def test_aggregate_producer_must_be_advisory
    with_repo do |root, policy|
      policy['job_overrides'].reject! { |entry| entry['job'] == 'postgresql-merge-gate' }
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join('\n'), 'must be classified advisory'
    end
  end

  def test_foreign_job_cannot_reuse_aggregate_context
    with_repo do |root, _policy|
      write(root, '.github/workflows/deep-soak.yml', <<~YAML)
        name: Deep
        on:
          pull_request:
        jobs:
          soak:
            name: postgresql
            runs-on: ubuntu-latest
      YAML
      assert_includes verify(root).join('\n'), 'aggregate context postgresql collides with PR jobs'
    end
  end

  def test_campaign_orchestrator_missing_advisory_producer_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/campaign-orchestrator.yml')
      original = File.read(path)
      body = original.sub(/\n  m5_soak:\n.*?uses: \.\/\.github\/workflows\/m5-soak\.yml\n/m, "\n")
      refute_equal original, body
      write(root, '.github/workflows/campaign-orchestrator.yml', body)
      assert_includes verify(root).join('\n'), 'misses advisory producers'
    end
  end
  def test_advisory_campaign_direct_pr_trigger_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/m5-soak.yml')
      body = File.read(path).sub("  workflow_call:\n", "  pull_request:\n")
      write(root, '.github/workflows/m5-soak.yml', body)
      assert_includes verify(root).join('\n'), 'must expose workflow_call'
      assert_includes verify(root).join('\n'), 'must not trigger directly on pull_request'
    end
  end

  def test_campaign_orchestrator_unknown_call_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/campaign-orchestrator.yml')
      original = File.read(path)
      unknown = "\n  unknown:\n    if: ${{ github.event.pull_request.draft == false }}\n    uses: ./.github/workflows/deep-soak.yml\n"
      body = original + unknown
      write(root, '.github/workflows/campaign-orchestrator.yml', body)
      assert_includes verify(root).join('\n'), 'calls non-advisory/unknown producers'
    end
  end

  def test_campaign_orchestrator_fail_closed_classifier_fallback_removal_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/campaign-orchestrator.yml')
      original = File.read(path)
      body = original.sub("needs.route.outputs.classification_outcome != 'success' || ", '')
      refute_equal original, body
      write(root, '.github/workflows/campaign-orchestrator.yml', body)
      assert_includes verify(root).join('\n'), 'must route direct proof and fail closed to execution'
    end
  end

  def test_m5_conformance_deep_classifier_fallback_removal_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/m5-conformance.yml')
      original = File.read(path)
      body = original.sub(
        "needs.route.outputs.classification_outcome != 'success' || ",
        ''
      )
      refute_equal original, body
      write(root, '.github/workflows/m5-conformance.yml', body)
      assert_includes(
        verify(root).join('\n'),
        'conformance-shard-15 must run for manual/direct proof and fail closed on routing ambiguity'
      )
    end
  end
  def test_m5_conformance_shard_matrix_weakening_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/m5-conformance.yml')
      original = File.read(path)
      body = original.sub('shard: [0, 1]', 'shard: [0]')
      refute_equal original, body
      write(root, '.github/workflows/m5-conformance.yml', body)
      assert_includes verify(root).join("\n"), 'must retain the exact two-way shard matrix [0, 1]'
    end
  end

  def test_m5_conformance_shard_env_binding_removal_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/m5-conformance.yml')
      original = File.read(path)
      body = original.sub('SHARD_INDEX: ${{ matrix.shard }}', 'SHARD_INDEX: weakened')
      refute_equal original, body
      write(root, '.github/workflows/m5-conformance.yml', body)
      assert_includes verify(root).join("\n"), 'must pass the checked-in shard index through env before shell execution'
    end
  end

  def test_m5_conformance_canonical_merge_command_removal_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/m5-conformance.yml')
      original = File.read(path)
      body = original.sub(
        'bash ./tests/fixtures/conformance/merge-ci-campaign.sh 15 2 target/m5-campaign-shards',
        'echo weakened'
      )
      refute_equal original, body
      write(root, '.github/workflows/m5-conformance.yml', body)
      assert_includes verify(root).join("\n"), 'conformance-deep-15 is missing canonical merge commands'
    end
  end

  def test_m5_conformance_emitter_pg_binding_removal_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/m5-conformance.yml')
      original = File.read(path)
      body = original.sub(
        'DEEP_15_RESULT: ${{ needs.conformance-deep-15.result }}',
        'DEEP_15_RESULT: success'
      )
      refute_equal original, body
      write(root, '.github/workflows/m5-conformance.yml', body)
      assert_includes verify(root).join("\n"), 'context emitter must bind DEEP_15_RESULT'
    end
  end

  def test_m5_conformance_required_context_emitter_services_are_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/m5-conformance.yml')
      original = File.read(path)
      body = original.sub(
        "  conformance-campaign:\n    name: postgres-${{ matrix.postgres }}-conformance-campaign\n",
        "  conformance-campaign:\n    name: postgres-${{ matrix.postgres }}-conformance-campaign\n    services:\n      postgres:\n        image: postgres:15\n"
      )
      refute_equal original, body
      write(root, '.github/workflows/m5-conformance.yml', body)
      assert_includes verify(root).join('\n'), 'required context emitter must not declare services'
    end
  end
  def test_fast_docs_only_true_guard_weakening_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/fast-branch.yml')
      original = File.read(path)
      body = original.sub(
        "      - name: Documentation-only fast path\n        if: ${{ steps.docs-only-scope.outcome == 'success' && steps.docs-only-scope.outputs.docs_only == 'true' }}",
        "      - name: Documentation-only fast path\n        if: ${{ steps.docs-only-scope.outcome == 'success' }}"
      )
      refute_equal original, body
      write(root, '.github/workflows/fast-branch.yml', body)
      assert_includes verify(root).join('\n'), 'documentation-only fast path must require successful true trusted classification'
    end
  end

  def test_fast_uncertain_full_fallback_weakening_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/fast-branch.yml')
      original = File.read(path)
      body = original.sub(
        "      - name: Run workspace unit tests\n        if: ${{ steps.docs-only-scope.outcome != 'success' || steps.docs-only-scope.outputs.docs_only != 'true' }}",
        "      - name: Run workspace unit tests\n        if: ${{ steps.docs-only-scope.outputs.docs_only != 'true' }}"
      )
      refute_equal original, body
      write(root, '.github/workflows/fast-branch.yml', body)
      assert_includes verify(root).join('\n'), 'must run on every non-docs or uncertain classification'
    end
  end

  def test_fast_head_classifier_substitution_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/fast-branch.yml')
      original = File.read(path)
      body = original.sub(
        '.fast-trusted-base/.github/scripts/pr-scope.py',
        '.github/scripts/pr-scope.py'
      )
      refute_equal original, body
      write(root, '.github/workflows/fast-branch.yml', body)
      assert_includes verify(root).join('\n'), 'docs-only classifier is missing trusted/fail-closed tokens'
    end
  end

  def test_quality_fast_non_docs_local_fallback_weakening_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub(
        "      - name: Run workspace unit tests\n        if: ${{ (github.event_name != 'pull_request' || github.event.pull_request.draft == false) && steps.fast-evidence.outputs.mode != 'full'  && (steps.docs-only-scope.outcome != 'success' || steps.docs-only-scope.outputs.docs_only != 'true') }}",
        "      - name: Run workspace unit tests\n        if: ${{ steps.fast-evidence.outputs.mode != 'none' }}"
      )
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'must locally fall back unless full Fast evidence or trusted PR docs-only proof applies'
    end
  end

  def test_quality_fast_mode_integrity_removal_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub("          echo 'mode=full'\n", "          echo 'mode=unknown'\n")
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'Fast evidence resolver is missing mode-integrity tokens'
    end
  end

  def test_docs_applicability_policy_weakening_is_rejected
    with_repo do |root, policy|
      policy['pr_scope']['docs_only_applicability']['evidence_provenance']['sensitive_prefixes'] = []
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join("\n"), 'docs_only_applicability must exactly match canonical supply/evidence ownership'
    end
  end

  def test_supply_lightweight_guard_weakening_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/supply-chain.yml')
      original = File.read(path)
      body = original.sub(
        "steps.supply-impact.outcome == 'success' && steps.supply-impact.outputs.impact == 'false'",
        "steps.supply-impact.outputs.impact == 'false'"
      )
      refute_equal original, body
      write(root, '.github/workflows/supply-chain.yml', body)
      assert_includes verify(root).join("\n"), 'lightweight success must require successful false trusted impact'
    end
  end

  def test_supply_uncertainty_fallback_weakening_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/supply-chain.yml')
      original = File.read(path)
      body = original.sub(
        "steps.supply-impact.outcome != 'success' || steps.supply-impact.outputs.impact != 'false'",
        "steps.supply-impact.outputs.impact != 'false'"
      )
      refute_equal original, body
      write(root, '.github/workflows/supply-chain.yml', body)
      assert_includes verify(root).join("\n"), 'must run on impact or classifier uncertainty'
    end
  end

  def test_evidence_lightweight_guard_weakening_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/evidence.yml')
      original = File.read(path)
      body = original.sub(
        "steps.evidence-impact.outcome == 'success' && steps.evidence-impact.outputs.impact == 'false'",
        "steps.evidence-impact.outputs.impact == 'false'"
      )
      refute_equal original, body
      write(root, '.github/workflows/evidence.yml', body)
      assert_includes verify(root).join("\n"), 'lightweight success must require successful false trusted impact'
    end
  end

  def test_evidence_uncertainty_fallback_weakening_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/evidence.yml')
      original = File.read(path)
      body = original.sub(
        "steps.evidence-impact.outcome != 'success' || steps.evidence-impact.outputs.impact != 'false'",
        "steps.evidence-impact.outputs.impact != 'false'"
      )
      refute_equal original, body
      write(root, '.github/workflows/evidence.yml', body)
      assert_includes verify(root).join("\n"), 'must run on impact or classifier uncertainty'
    end
  end

  def test_supply_head_classifier_substitution_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/supply-chain.yml')
      original = File.read(path)
      body = original.sub(
        '.supply-trusted-base/.github/scripts/pr-scope.py',
        '.github/scripts/pr-scope.py'
      )
      refute_equal original, body
      write(root, '.github/workflows/supply-chain.yml', body)
      assert_includes verify(root).join("\n"), 'classifier is missing fail-closed applicability tokens'
    end
  end

  def test_v7_pr_proof_policy_accepts_legacy_absence
    with_v7_topology_contract do |_root, policy|
      refute policy.key?('pr_proof')
      assert_empty MergeGateVerifier.pr_proof_policy_contract(policy: policy)
    end
  end

  def test_v7_pr_proof_policy_accepts_canonical_optional_authorities
    with_v7_topology_contract do |_root, policy|
      policy['pr_proof'] = canonical_pr_proof_policy
      assert_empty MergeGateVerifier.pr_proof_policy_contract(policy: policy)
    end
  end

  def test_v7_pr_proof_policy_rejects_missing_optional_authority
    with_v7_topology_contract do |_root, policy|
      policy['pr_proof'] = canonical_pr_proof_policy
      policy['pr_proof']['members'].reject! { |member| member['id'] == 'codeql' }
      violations = MergeGateVerifier.pr_proof_policy_contract(policy: policy)
      assert_includes violations.join("\n"), 'canonical trusted-scope optional-authority inventory'
    end
  end

  def test_v7_pr_proof_policy_rejects_weakened_applicability
    with_v7_topology_contract do |_root, policy|
      policy['pr_proof'] = canonical_pr_proof_policy
      policy['pr_proof']['members']
            .find { |member| member['id'] == 'evidence' }['applicability'] = 'non_docs'
      violations = MergeGateVerifier.pr_proof_policy_contract(policy: policy)
      assert_includes violations.join("\n"), 'canonical trusted-scope optional-authority inventory'
    end
  end

  def test_v7_pr_proof_required_authorities_fail_closed_on_scope_failure
    expected = %w[rust dependency codeql evidence supply]
    actual = MergeGateVerifier.required_pr_proof_authority_ids(
      scope_result: 'failure',
      classification_outcome: 'success',
      legacy_base: 'false',
      docs_only: 'true',
      evidence_impact: 'false',
      supply_chain_impact: 'false'
    )
    assert_equal expected, actual
  end

  def test_v7_pr_proof_required_authorities_fail_closed_on_malformed_classification
    expected = %w[rust dependency codeql evidence supply]
    actual = MergeGateVerifier.required_pr_proof_authority_ids(
      scope_result: 'success',
      classification_outcome: 'success',
      legacy_base: 'false',
      docs_only: 'unknown',
      evidence_impact: 'false',
      supply_chain_impact: 'false'
    )
    assert_equal expected, actual
  end

  def test_v7_pr_proof_required_authorities_honor_trusted_applicability
    actual = MergeGateVerifier.required_pr_proof_authority_ids(
      scope_result: 'success',
      classification_outcome: 'success',
      legacy_base: 'false',
      docs_only: 'true',
      evidence_impact: 'true',
      supply_chain_impact: 'false'
    )
    assert_equal ['evidence'], actual
  end

  def test_v7_dispatched_runtime_accepts_canonical_contract
    jobs = canonical_dispatched_runtime_jobs
    docs = canonical_dispatched_authority_docs
    policy = {'schema_version' => 7, 'pr_proof' => canonical_pr_proof_policy}
    assert_empty MergeGateVerifier.pr_proof_dispatch_runtime_contract(
      jobs: jobs,
      docs: docs,
      policy: policy
    )
  end

  def test_v7_dispatched_runtime_rejects_static_optional_authority_job
    jobs = canonical_dispatched_runtime_jobs
    jobs['rust'] = {'uses' => './.github/workflows/ci.yml'}
    docs = canonical_dispatched_authority_docs
    policy = {'schema_version' => 7, 'pr_proof' => canonical_pr_proof_policy}
    violations = MergeGateVerifier.pr_proof_dispatch_runtime_contract(
      jobs: jobs,
      docs: docs,
      policy: policy
    )
    assert_includes violations.join("\n"), 'must not materialize static optional-authority jobs: rust'
  end

  def test_v7_dispatched_runtime_rejects_actions_write_weakening
    jobs = canonical_dispatched_runtime_jobs
    jobs['dispatch-authorities']['permissions']['actions'] = 'read'
    docs = canonical_dispatched_authority_docs
    policy = {'schema_version' => 7, 'pr_proof' => canonical_pr_proof_policy}
    violations = MergeGateVerifier.pr_proof_dispatch_runtime_contract(
      jobs: jobs,
      docs: docs,
      policy: policy
    )
    assert_includes violations.join("\n"), 'must keep exact Actions-write dispatch permissions'
  end

  def test_v7_dispatched_runtime_rejects_missing_caller_attempt_proof
    jobs = canonical_dispatched_runtime_jobs
    proof_step = jobs['pr-proof']['steps'].find { |step| step['name'] == 'Verify dispatched authorities' }
    proof_step['run'] = proof_step['run'].sub(
      '--caller-run-attempt "$CALLER_RUN_ATTEMPT"',
      '--caller-run-attempt omitted'
    )
    docs = canonical_dispatched_authority_docs
    policy = {'schema_version' => 7, 'pr_proof' => canonical_pr_proof_policy}
    violations = MergeGateVerifier.pr_proof_dispatch_runtime_contract(
      jobs: jobs,
      docs: docs,
      policy: policy
    )
    assert_includes violations.join("\n"), 'is missing dispatched proof tokens'
  end

  def test_v7_dispatched_runtime_rejects_optional_authority_input_weakening
    jobs = canonical_dispatched_runtime_jobs
    docs = canonical_dispatched_authority_docs
    docs['.github/workflows/evidence.yml']['on']['workflow_dispatch']['inputs']['caller_run_attempt']['required'] = false
    policy = {'schema_version' => 7, 'pr_proof' => canonical_pr_proof_policy}
    violations = MergeGateVerifier.pr_proof_dispatch_runtime_contract(
      jobs: jobs,
      docs: docs,
      policy: policy
    )
    assert_includes violations.join("\n"), 'workflow_dispatch input caller_run_attempt must be required string'
  end

  def test_v7_topology_dispatch_marker_selects_dispatched_contract
    with_v7_topology_contract do |root, policy|
      policy['pr_proof'] = canonical_pr_proof_policy
      path = File.join(root, '.github/workflows/pr-ci.yml')
      original = File.read(path)
      body = original.sub(
        "  pr-proof:\n",
        "  dispatch-authorities:\n    name: dispatch-required-authorities\n  pr-proof:\n"
      )
      refute_equal original, body
      write(root, '.github/workflows/pr-ci.yml', body)
      policy.dig('repository_merge_gate', 'protected_workflows')
            .find { |entry| entry['workflow'] == '.github/workflows/pr-ci.yml' }['accepted_blobs'] << MergeGateVerifier.git_blob_sha(body)
      violations = MergeGateVerifier.pr_topology_v7_contract(
        root: root,
        policy: policy,
        producer_summary: v7_producer_summary(root)
      )
      assert_includes violations.join("\n"), 'must not materialize static optional-authority jobs'
    end
  end

  def test_v7_topology_contract_accepts_canonical_single_entrypoint
    with_v7_topology_contract do |root, policy|
      assert_empty MergeGateVerifier.pr_topology_v7_contract(
        root: root,
        policy: policy,
        producer_summary: v7_producer_summary(root)
      )
    end
  end

  def test_v7_topology_accepts_campaign_free_entrypoint
    with_v7_topology_contract do |root, policy|
      path = File.join(root, '.github/workflows/pr-ci.yml')
      original = File.read(path)
      body = original.sub(
        /  campaigns:\n.*?(?=  pr-proof:\n)/m,
        ''
      )
      refute_equal original, body
      write(root, '.github/workflows/pr-ci.yml', body)
      policy.dig('repository_merge_gate', 'protected_workflows')
            .find { |entry| entry['workflow'] == '.github/workflows/pr-ci.yml' }['accepted_blobs'] << MergeGateVerifier.git_blob_sha(body)
      violations = MergeGateVerifier.pr_topology_v7_contract(
        root: root,
        policy: policy,
        producer_summary: v7_producer_summary(root)
      )
      assert_empty violations
    end
  end

  def test_v7_topology_rejects_reactivated_campaign_compatibility_job
    with_v7_topology_contract do |root, policy|
      path = File.join(root, '.github/workflows/pr-ci.yml')
      original = File.read(path)
      body = original.sub(') && false }}', ') && (false || true) }}')
      refute_equal original, body
      write(root, '.github/workflows/pr-ci.yml', body)
      policy.dig('repository_merge_gate', 'protected_workflows')
            .find { |entry| entry['workflow'] == '.github/workflows/pr-ci.yml' }['accepted_blobs'] << MergeGateVerifier.git_blob_sha(body)
      violations = MergeGateVerifier.pr_topology_v7_contract(
        root: root,
        policy: policy,
        producer_summary: v7_producer_summary(root)
      )
      assert_includes violations.join("\n"), 'campaigns compatibility job must remain permanently disabled while present'
    end
  end

  def test_v7_topology_rejects_entrypoint_path_filter
    with_v7_topology_contract do |root, policy|
      path = File.join(root, '.github/workflows/pr-ci.yml')
      body = File.read(path).sub(
        "    branches:\n      - main\n",
        "    branches:\n      - main\n    paths:\n      - docs/**\n"
      )
      write(root, '.github/workflows/pr-ci.yml', body)
      violations = MergeGateVerifier.pr_topology_v7_contract(
        root: root,
        policy: policy,
        producer_summary: v7_producer_summary(root)
      )
      assert_includes violations.join("\n"), 'must not suppress pull_request events with path filters'
    end
  end

  def test_v7_topology_rejects_missing_workflow_call
    with_v7_topology_contract do |root, policy|
      path = File.join(root, '.github/workflows/evidence.yml')
      body = File.read(path).sub("  workflow_call:\n", '')
      write(root, '.github/workflows/evidence.yml', body)
      violations = MergeGateVerifier.pr_topology_v7_contract(
        root: root,
        policy: policy,
        producer_summary: v7_producer_summary(root)
      )
      assert_includes violations.join("\n"), 'must expose workflow_call'
    end
  end

  def test_v7_topology_rejects_legacy_true_as_unknown_default
    with_v7_topology_contract do |root, policy|
      path = File.join(root, '.github/workflows/pr-ci.yml')
      original = File.read(path)
      body = original.sub("echo 'legacy_base=unknown'", "echo 'legacy_base=true'")
      refute_equal original, body
      write(root, '.github/workflows/pr-ci.yml', body)
      violations = MergeGateVerifier.pr_topology_v7_contract(
        root: root,
        policy: policy,
        producer_summary: v7_producer_summary(root)
      )
      assert_includes violations.join("\n"), 'scope classifier is missing trusted/fail-closed tokens'
    end
  end

  def test_v7_topology_rejects_weakened_uncertainty_route
    with_v7_topology_contract do |root, policy|
      path = File.join(root, '.github/workflows/pr-ci.yml')
      original = File.read(path)
      body = original.sub(
        "if: ${{ needs.scope.outputs.legacy_base != 'true' && (needs.scope.outputs.classification_outcome != 'success' || needs.scope.outputs.docs_only != 'true') }}",
        "if: ${{ needs.scope.outputs.legacy_base != 'true' && needs.scope.outputs.docs_only != 'true' }}"
      )
      refute_equal original, body
      write(root, '.github/workflows/pr-ci.yml', body)
      violations = MergeGateVerifier.pr_topology_v7_contract(
        root: root,
        policy: policy,
        producer_summary: v7_producer_summary(root)
      )
      assert_includes violations.join("\n"), 'must fail closed on legacy/uncertain scope'
    end
  end

  def test_v7_topology_rejects_pr_proof_missing_authority
    with_v7_topology_contract do |root, policy|
      path = File.join(root, '.github/workflows/pr-ci.yml')
      original = File.read(path)
      body = original.sub(
        '    needs: [scope, rust, dependency, codeql, evidence, supply]',
        '    needs: [scope, rust, dependency, evidence, supply]'
      )
      refute_equal original, body
      write(root, '.github/workflows/pr-ci.yml', body)
      violations = MergeGateVerifier.pr_topology_v7_contract(
        root: root,
        policy: policy,
        producer_summary: v7_producer_summary(root)
      )
      assert_includes violations.join("\n"), 'needs mismatch'
    end
  end

  def test_v7_internal_aggregate_policy_rejects_missing_catalog
    with_v7_topology_contract do |_root, policy|
      policy['internal_aggregates'] = []
      violations = MergeGateVerifier.internal_aggregate_policy_contract(policy)
      assert_includes violations.join("\n"), 'internal_aggregates must exactly declare the canonical PostgreSQL reusable-workflow aggregate'
    end
  end

  def test_v7_internal_aggregate_policy_rejects_branch_aggregate_reuse
    with_v7_topology_contract do |_root, policy|
      policy['aggregate_gates'] = policy['internal_aggregates']
      violations = MergeGateVerifier.internal_aggregate_policy_contract(policy)
      assert_includes violations.join("\n"), 'branch aggregate_gates must remain empty'
    end
  end

  def test_v7_topology_rejects_unapproved_entrypoint_blob
    with_v7_topology_contract do |root, policy|
      path = File.join(root, '.github/workflows/pr-ci.yml')
      write(root, '.github/workflows/pr-ci.yml', File.read(path) + "\n# unapproved mutation\n")
      violations = MergeGateVerifier.pr_topology_v7_contract(
        root: root,
        policy: policy,
        producer_summary: v7_producer_summary(root)
      )
      assert_includes violations.join("\n"), 'is not accepted by policy'
    end
  end

  def test_v7_topology_rejects_unapproved_merge_authority_blob
    with_v7_topology_contract do |root, policy|
      path = File.join(root, '.github/workflows/evidence.yml')
      write(root, '.github/workflows/evidence.yml', File.read(path) + "\n# bypass attempt\n")
      violations = MergeGateVerifier.pr_topology_v7_contract(
        root: root,
        policy: policy,
        producer_summary: v7_producer_summary(root)
      )
      assert_includes violations.join("\n"), 'protected workflow .github/workflows/evidence.yml blob'
      assert_includes violations.join("\n"), 'is not accepted by policy'
    end
  end

  private

  def canonical_pr_proof_policy
    {
      'schema' => 'trusted-scope-authorities-v1',
      'members' => [
        {'id' => 'rust', 'workflow' => '.github/workflows/ci.yml', 'applicability' => 'non_docs'},
        {'id' => 'dependency', 'workflow' => '.github/workflows/dependency-review.yml', 'applicability' => 'non_docs'},
        {'id' => 'codeql', 'workflow' => '.github/workflows/codeql.yml', 'applicability' => 'non_docs'},
        {'id' => 'evidence', 'workflow' => '.github/workflows/evidence.yml', 'applicability' => 'evidence_impact'},
        {'id' => 'supply', 'workflow' => '.github/workflows/supply-chain.yml', 'applicability' => 'supply_chain_impact'}
      ]
    }
  end

  def v7_producer_summary(root)
    workflow_docs = {}
    Dir[File.join(root, '.github/workflows/*.{yml,yaml}')].sort.each do |absolute|
      relative = Pathname(absolute).relative_path_from(Pathname(root)).to_s
      workflow_docs[relative] = MergeGateVerifier.load_yaml(absolute)
    end
    {'workflow_docs' => workflow_docs}
  end

  def canonical_dispatched_runtime_jobs
    dispatch_env = {
      'GH_TOKEN' => '${{ github.token }}',
      'BASE_SHA' => '${{ github.event.pull_request.base.sha }}',
      'HEAD_SHA' => '${{ github.event.pull_request.head.sha }}',
      'HEAD_REPO' => '${{ github.event.pull_request.head.repo.full_name }}',
      'PR_NUMBER' => '${{ github.event.pull_request.number }}',
      'DEFAULT_BRANCH' => '${{ github.event.repository.default_branch }}',
      'CALLER_RUN_ID' => '${{ github.run_id }}',
      'CALLER_RUN_ATTEMPT' => '${{ github.run_attempt }}',
      'SCOPE_RESULT' => '${{ needs.scope.result }}',
      'CLASSIFICATION_OUTCOME' => '${{ needs.scope.outputs.classification_outcome }}',
      'LEGACY_BASE' => '${{ needs.scope.outputs.legacy_base }}',
      'DOCS_ONLY' => '${{ needs.scope.outputs.docs_only }}',
      'EVIDENCE_IMPACT' => '${{ needs.scope.outputs.evidence_impact }}',
      'SUPPLY_IMPACT' => '${{ needs.scope.outputs.supply_chain_impact }}'
    }
    dispatch_run = [
      'python3 .dispatch-trusted-base/.github/scripts/pr-authority-runtime.py plan',
      '--policy .dispatch-trusted-base/.github/merge-gate-policy.json',
      '--scope-result "$SCOPE_RESULT"',
      '--classification-outcome "$CLASSIFICATION_OUTCOME"',
      '--legacy-base "$LEGACY_BASE"',
      '--docs-only "$DOCS_ONLY"',
      '--evidence-impact "$EVIDENCE_IMPACT"',
      '--supply-chain-impact "$SUPPLY_IMPACT"',
      'gh api actions/workflows/${workflow}/dispatches',
      '-f ref="$DEFAULT_BRANCH"',
      '-f inputs[base_sha]="$BASE_SHA"',
      '-f inputs[head_sha]="$HEAD_SHA"',
      '-f inputs[head_repo]="$HEAD_REPO"',
      '-f inputs[pr_number]="$PR_NUMBER"',
      '-f inputs[caller_run_id]="$CALLER_RUN_ID"',
      '-f inputs[caller_run_attempt]="$CALLER_RUN_ATTEMPT"'
    ].join("\n")
    proof_env = dispatch_env.reject { |key, _| key == 'DEFAULT_BRANCH' }.merge(
      'DISPATCH_RESULT' => '${{ needs.dispatch-authorities.result }}'
    )
    proof_run = [
      'python3 .proof-trusted-base/.github/scripts/pr-authority-runtime.py verify',
      '--policy .proof-trusted-base/.github/merge-gate-policy.json',
      '--scope-result "$SCOPE_RESULT"',
      '--classification-outcome "$CLASSIFICATION_OUTCOME"',
      '--legacy-base "$LEGACY_BASE"',
      '--docs-only "$DOCS_ONLY"',
      '--evidence-impact "$EVIDENCE_IMPACT"',
      '--supply-chain-impact "$SUPPLY_IMPACT"',
      '--dispatch-result "$DISPATCH_RESULT"',
      '--base-sha "$BASE_SHA"',
      '--head-sha "$HEAD_SHA"',
      '--head-repo "$HEAD_REPO"',
      '--pr-number "$PR_NUMBER"',
      '--caller-run-id "$CALLER_RUN_ID"',
      '--caller-run-attempt "$CALLER_RUN_ATTEMPT"'
    ].join("\n")

    {
      'scope' => {},
      'dispatch-authorities' => {
        'name' => 'dispatch-required-authorities',
        'needs' => 'scope',
        'if' => '${{ always() && github.event.pull_request.draft == false }}',
        'runs-on' => 'ubuntu-slim',
        'timeout-minutes' => 5,
        'permissions' => {'actions' => 'write', 'contents' => 'read'},
        'steps' => [
          {
            'id' => 'dispatch-trusted-base',
            'uses' => 'actions/checkout@0000000000000000000000000000000000000001',
            'with' => {
              'ref' => '${{ github.event.pull_request.base.sha }}',
              'path' => '.dispatch-trusted-base',
              'fetch-depth' => 1,
              'persist-credentials' => false
            }
          },
          {
            'name' => 'Dispatch required authorities',
            'env' => dispatch_env,
            'run' => dispatch_run
          }
        ]
      },
      'pr-proof' => {
        'name' => 'pr-proof',
        'needs' => ['scope', 'dispatch-authorities'],
        'if' => '${{ always() }}',
        'runs-on' => 'ubuntu-slim',
        'timeout-minutes' => 5,
        'permissions' => {'actions' => 'read', 'contents' => 'read'},
        'steps' => [
          {
            'id' => 'proof-trusted-base',
            'uses' => 'actions/checkout@0000000000000000000000000000000000000001',
            'with' => {
              'ref' => '${{ github.event.pull_request.base.sha }}',
              'path' => '.proof-trusted-base',
              'fetch-depth' => 1,
              'persist-credentials' => false
            }
          },
          {
            'name' => 'Verify dispatched authorities',
            'env' => proof_env,
            'run' => proof_run
          }
        ]
      }
    }
  end

  def canonical_dispatched_authority_docs
    inputs = %w[base_sha head_sha head_repo pr_number caller_run_id caller_run_attempt].to_h do |name|
      [name, {'required' => true, 'type' => 'string'}]
    end
    canonical_pr_proof_policy.fetch('members').to_h do |member|
      authority_id = member.fetch('id')
      workflow = member.fetch('workflow')
      run_name = "pr-authority/#{authority_id}/pr-${{ inputs.pr_number }}/${{ inputs.head_sha }}/caller-${{ inputs.caller_run_id }}-${{ inputs.caller_run_attempt }}"
      [
        workflow,
        {
          'run-name' => run_name,
          'on' => {
            'workflow_dispatch' => {
              'inputs' => Marshal.load(Marshal.dump(inputs))
            }
          }
        }
      ]
    end
  end

  def with_v7_topology_contract
    Dir.mktmpdir do |root|
      FileUtils.mkdir_p(File.join(root, '.github/workflows'))
      FileUtils.mkdir_p(File.join(root, '.github/scripts'))

      pr_ci = <<~YAML
        name: PR CI
        on:
          pull_request:
            branches:
              - main
        permissions:
          contents: read
          pull-requests: read
        jobs:
          scope:
            name: trusted-pr-scope
            outputs:
              legacy_base: __EXPR__{{ steps.classify.outputs.legacy_base }}
              classification_outcome: __EXPR__{{ steps.classify.outcome }}
              docs_only: __EXPR__{{ steps.classify.outputs.docs_only }}
              supply_chain_impact: __EXPR__{{ steps.classify.outputs.supply_chain_impact }}
              evidence_impact: __EXPR__{{ steps.classify.outputs.evidence_impact }}
            steps:
              - id: trusted-base
                continue-on-error: true
                uses: actions/checkout@0000000000000000000000000000000000000001
                with:
                  ref: __EXPR__{{ github.event.pull_request.base.sha }}
                  path: .trusted-base
                  fetch-depth: 1
                  persist-credentials: false
              - id: classify
                continue-on-error: true
                run: |
                  {
                    echo 'legacy_base=unknown'
                    echo 'docs_only=false'
                    echo 'supply_chain_impact=true'
                    echo 'evidence_impact=true'
                  } >> "$GITHUB_OUTPUT"
                  echo '.trusted-base/.github/merge-gate-policy.json schema_version'
                  schema_version=7
                  if [ "$schema_version" -ge 7 ]; then
                    echo 'legacy_base=false'
                  else
                    echo 'legacy_base=true'
                  fi
                  echo 'repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}'
                  echo 'pulls/${PR_NUMBER}/files?per_page=100'
                  echo '.trusted-base/.github/scripts/pr-scope.py'
                  echo '--trusted-base-sha "$BASE_SHA"'
                  echo '.docs_only .supply_chain_impact .evidence_impact'
          rust:
            needs: scope
            if: __EXPR__{{ needs.scope.outputs.legacy_base != 'true' && (needs.scope.outputs.classification_outcome != 'success' || needs.scope.outputs.docs_only != 'true') }}
            uses: ./.github/workflows/ci.yml
            permissions:
              actions: read
              contents: read
          dependency:
            needs: scope
            if: __EXPR__{{ needs.scope.outputs.legacy_base != 'true' && (needs.scope.outputs.classification_outcome != 'success' || needs.scope.outputs.docs_only != 'true') }}
            uses: ./.github/workflows/dependency-review.yml
            permissions:
              contents: read
          codeql:
            needs: scope
            if: __EXPR__{{ needs.scope.outputs.legacy_base != 'true' && (needs.scope.outputs.classification_outcome != 'success' || needs.scope.outputs.docs_only != 'true') }}
            uses: ./.github/workflows/codeql.yml
            permissions:
              contents: read
              pull-requests: read
              security-events: write
          evidence:
            needs: scope
            if: __EXPR__{{ needs.scope.outputs.legacy_base != 'true' && (needs.scope.outputs.classification_outcome != 'success' || needs.scope.outputs.evidence_impact != 'false') }}
            uses: ./.github/workflows/evidence.yml
            permissions:
              contents: read
          supply:
            needs: scope
            if: __EXPR__{{ needs.scope.outputs.legacy_base != 'true' && (needs.scope.outputs.classification_outcome != 'success' || needs.scope.outputs.supply_chain_impact != 'false') }}
            uses: ./.github/workflows/supply-chain.yml
            permissions:
              contents: read
          campaigns:
            needs: scope
            if: __EXPR__{{ needs.scope.outputs.legacy_base != 'true' && (needs.scope.outputs.classification_outcome != 'success' || needs.scope.outputs.docs_only != 'true') && false }}
            uses: ./.github/workflows/campaign-orchestrator.yml
            permissions:
              contents: read
              pull-requests: read
          pr-proof:
            name: pr-proof
            needs: [scope, rust, dependency, codeql, evidence, supply]
            if: __EXPR__{{ always() }}
            steps:
              - run: |
                  if [ "$LEGACY_BASE" = "true" ]; then
                    echo legacy
                  fi
                  echo "$LEGACY_BASE $CLASSIFICATION_OUTCOME $DOCS_ONLY"
                  echo "$RUST_RESULT $DEPENDENCY_RESULT $CODEQL_RESULT"
                  echo "$EVIDENCE_RESULT $SUPPLY_RESULT"
      YAML
      pr_ci = pr_ci.gsub('__EXPR__', '$')
      write(root, '.github/workflows/pr-ci.yml', pr_ci)
      write(root, '.github/ci-topology-v7-migration', "schema-v7 migration marker\n")

      authorities = %w[
        ci.yml
        dependency-review.yml
        codeql.yml
        evidence.yml
        supply-chain.yml
        campaign-orchestrator.yml
      ]
      authorities.each do |name|
        write(root, ".github/workflows/#{name}", <<~YAML)
          name: #{name}
          on:
            pull_request:
              paths:
                - .github/ci-topology-v7-migration
            workflow_call:
          jobs:
            noop:
              runs-on: ubuntu-latest
              steps:
                - run: echo ok
        YAML
      end

      write(root, '.github/workflows/supply-chain-audit.yml', <<~YAML)
        name: Scheduled supply-chain audit
        on:
          schedule:
            - cron: "17 18 * * 1"
        permissions: {}
        jobs:
          supply-chain:
            uses: ./.github/workflows/supply-chain.yml
            permissions:
              contents: read
          report-failure:
            needs:
              - supply-chain
            if: __EXPR__{{ always() && needs.supply-chain.result == 'failure' }}
            permissions:
              contents: read
              issues: write
            steps:
              - run: echo report
      YAML
      audit_path = File.join(root, '.github/workflows/supply-chain-audit.yml')
      File.write(audit_path, File.read(audit_path).gsub('__EXPR__', '$'))

      write(root, '.github/workflows/m5-conformance.yml', <<~YAML)
        name: M5 Conformance
        on:
          pull_request:
            branches:
              - main
          workflow_dispatch:
        jobs:
          conformance-campaign:
            name: postgres-__EXPR__{{ matrix.postgres }}-conformance-campaign
            strategy:
              matrix:
                postgres: ["15", "18"]
            runs-on: ubuntu-latest
            steps:
              - run: echo conformance
      YAML
      m5_path = File.join(root, '.github/workflows/m5-conformance.yml')
      File.write(m5_path, File.read(m5_path).gsub('__EXPR__', '$'))

      write(root, '.github/workflows/pr-labeler.yml', <<~YAML)
        name: Pull request labels
        on:
          pull_request_target:
        permissions: {}
        jobs:
          merge-gate:
            name: merge-gate
            runs-on: ubuntu-latest
            steps:
              - run: echo trusted-base
      YAML

      protected_workflows = [
        '.github/workflows/pr-ci.yml',
        '.github/workflows/ci.yml',
        '.github/workflows/dependency-review.yml',
        '.github/workflows/codeql.yml',
        '.github/workflows/evidence.yml',
        '.github/workflows/supply-chain.yml',
        '.github/workflows/m5-conformance.yml',
        '.github/workflows/pr-labeler.yml'
      ]

      policy = {
        'schema_version' => 7,
        'pr_topology' => {
          'schema' => 'single-pr-entrypoint-v1',
          'entrypoint' => '.github/workflows/pr-ci.yml',
          'migration_marker' => '.github/ci-topology-v7-migration',
          'reusable_authorities' => authorities.map { |name| ".github/workflows/#{name}" },
          'independent_pr_authorities' => [
            {
              'workflow' => '.github/workflows/m5-conformance.yml',
              'reason' => 'retained-evidence-provenance',
              'contexts' => [
                'postgres-15-conformance-campaign',
                'postgres-18-conformance-campaign'
              ]
            }
          ]
        },
        'aggregate_gates' => [],
        'internal_aggregates' => [
          {
            'context' => 'postgresql',
            'producer' => {
              'workflow' => '.github/workflows/ci.yml',
              'job' => 'postgresql-merge-gate'
            },
            'members' => [
              'postgres-15-design-gate',
              'postgres-15-item-components',
              'postgres-15-repository',
              'postgres-16-design-gate',
              'postgres-17-design-gate',
              'postgres-18-design-gate',
              'postgres-18-item-components',
              'postgres-18-repository'
            ]
          }
        ],
        'repository_merge_gate' => {
          'context' => 'merge-gate',
          'state' => 'active',
          'producer' => {
            'workflow' => '.github/workflows/pr-labeler.yml',
            'job' => 'merge-gate'
          },
          'members' => [
            {'context' => 'pr-proof', 'workflow' => '.github/workflows/pr-ci.yml'},
            {'context' => 'postgres-15-conformance-campaign', 'workflow' => '.github/workflows/m5-conformance.yml'},
            {'context' => 'postgres-18-conformance-campaign', 'workflow' => '.github/workflows/m5-conformance.yml'}
          ],
          'protected_workflows' => protected_workflows.map do |workflow|
            {
              'workflow' => workflow,
              'accepted_blobs' => [
                MergeGateVerifier.git_blob_sha(
                  File.binread(File.join(root, workflow))
                )
              ]
            }
          end
        }
      }
      yield root, policy
    end
  end

  def write(root, relative, content)
    path = File.join(root, relative)
    FileUtils.mkdir_p(File.dirname(path))
    File.write(path, content)
  end

  def write_json(root, relative, value)
    write(root, relative, JSON.pretty_generate(value))
  end

  def ruleset_with(*contexts)
    {
      'id' => 7,
      'name' => 'Protect main',
      'enforcement' => 'active',
      'rules' => [{
        'type' => 'required_status_checks',
        'parameters' => {
          'required_status_checks' => contexts.map { |context| {'context' => context} }
        }
      }]
    }
  end
end
