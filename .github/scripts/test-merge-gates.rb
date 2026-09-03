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
      policy = {
        'schema_version' => 2,
        'ruleset' => {'id' => 7, 'name' => 'Protect main'},
        'workflow_defaults' => [
          {'pattern' => '.github/workflows/ci.yml', 'classification' => 'required'},
          {'pattern' => '.github/workflows/m5-*.yml', 'classification' => 'advisory'},
          {'pattern' => '.github/workflows/deep-*.yml', 'classification' => 'advisory'}
        ],
        'job_overrides' => [
          {
            'workflow' => '.github/workflows/ci.yml',
            'job' => 'postgresql-merge-gate',
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
        'pending_ruleset_contexts' => ['postgresql', 'postgresql-conformance']
      }
      write_json(root, '.github/merge-gate-policy.json', policy)
      write(root, '.github/workflows/ci.yml', <<~YAML)
        name: Rust
        on:
          pull_request:
            branches: [main]
        jobs:
          quality:
            name: quality
            runs-on: ubuntu-latest
            steps:
              - name: Check out repository
                uses: actions/checkout@0000000000000000000000000000000000000001
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
      write(root, '.github/workflows/m5-conformance.yml', <<~YAML)
        name: M5 Conformance
        on:
          pull_request:
            branches: [main]
        jobs:
          conformance-campaign:
            name: postgres-${{ matrix.postgres }}-conformance-campaign
            strategy:
              matrix:
                postgres: ["15", "18"]
            runs-on: ubuntu-latest
            steps:
              - name: Check out repository
                uses: actions/checkout@0000000000000000000000000000000000000002
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

  def test_required_job_rename_is_detected_as_ruleset_drift
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      write(root, '.github/workflows/ci.yml', File.read(path).sub('name: quality', 'name: quality-renamed'))
      assert_includes verify(root).join('\n'), 'quality-renamed'
    end
  end

  def test_required_job_removal_makes_live_context_stale
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      body = File.read(path).sub(
        "\n  quality:\n    name: quality\n    runs-on: ubuntu-latest\n    steps:\n      - name: Check out repository\n        uses: actions/checkout@0000000000000000000000000000000000000001\n",
        "\n"
      )
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

  def test_required_job_condition_is_rejected
    with_repo do |root, _policy|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub(
        "name: quality\n    runs-on:",
        "name: quality\n    if: github.actor != 'nobody'\n    runs-on:"
      )
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'not guaranteed to emit'
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
      policy['pending_ruleset_contexts'] = []
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
      body = File.read(path).sub('if: ${{ always() }}', 'if: success()')
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
      body = File.read(path).sub('timeout-minutes: 5', 'timeout-minutes: 30')
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
