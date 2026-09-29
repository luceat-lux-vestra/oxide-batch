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
          'trusted_tree_contract' => 'exact-git-base-sha'
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
                run: echo 'actions/workflows/fast-branch.yml/runs?event=push&head_sha=$EXPECTED_SHA&per_page=100'
              - name: Check diff hygiene
                run: git diff --check
              - name: Check formatting
                run: cargo fmt --all -- --check
              - name: Run Clippy
                run: cargo clippy --workspace --all-targets --all-features --
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
  private

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
