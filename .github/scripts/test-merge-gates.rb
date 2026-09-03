#!/usr/bin/env ruby
# frozen_string_literal: true

require 'json'
require 'fileutils'
require 'minitest/autorun'
require 'tmpdir'
require_relative 'verify-merge-gates'

class MergeGateVerifierTest < Minitest::Test
  def with_repo
    Dir.mktmpdir do |root|
      FileUtils.mkdir_p(File.join(root, '.github/workflows'))
      policy = {
        'schema_version' => 2,
        'ruleset' => {'id' => 7, 'name' => 'Protect main'},
        'workflow_defaults' => [
          {'pattern' => '.github/workflows/ci.yml', 'classification' => 'required'},
          {'pattern' => '.github/workflows/evidence.yml', 'classification' => 'required'},
          {'pattern' => '.github/workflows/postgres-merge-gate.yml', 'classification' => 'advisory'},
          {'pattern' => '.github/workflows/deep-*.yml', 'classification' => 'advisory'}
        ],
        'job_overrides' => [],
        'managed_required_contexts' => ['Analyze (actions)'],
        'aggregate_gates' => [{
          'context' => 'postgresql',
          'state' => 'candidate',
          'producer' => {
            'workflow' => '.github/workflows/postgres-merge-gate.yml',
            'job' => 'publish'
          },
          'members' => ['postgres-15-repository', 'postgres-18-repository']
        }],
        'pending_ruleset_contexts' => ['evidence-provenance', 'postgresql']
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
          postgres:
            name: postgres-${{ matrix.postgres }}-repository
            strategy:
              matrix:
                postgres: ["15", "18"]
            runs-on: ubuntu-latest
      YAML
      write(root, '.github/workflows/evidence.yml', <<~YAML)
        name: Evidence
        on:
          pull_request:
        jobs:
          evidence-provenance:
            name: evidence-provenance
            runs-on: ubuntu-latest
      YAML
      write(root, '.github/workflows/postgres-merge-gate.yml', <<~YAML)
        name: PostgreSQL Merge Gate
        on:
          pull_request_target:
            branches: [main]
          workflow_run:
            workflows: [Rust]
            types: [in_progress, completed]
        permissions:
          contents: read
          actions: read
          statuses: write
        jobs:
          publish:
            runs-on: ubuntu-latest
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
      ruleset = ruleset_with('Analyze (actions)', 'quality', 'postgres-15-repository', 'postgres-18-repository')
      write_json(root, 'ruleset.json', ruleset)
      yield root, policy, ruleset
    end
  end

  def verify(root)
    MergeGateVerifier.verify(
      root: root,
      policy_path: File.join(root, '.github/merge-gate-policy.json'),
      ruleset_path: File.join(root, 'ruleset.json')
    ).first
  end

  def test_clean_staged_policy
    with_repo { |root, _policy, _ruleset| assert_empty verify(root) }
  end

  def test_required_job_rename_is_detected_as_ruleset_drift
    with_repo do |root, _policy, _ruleset|
      path = File.join(root, '.github/workflows/ci.yml')
      write(root, '.github/workflows/ci.yml', File.read(path).sub('name: quality', 'name: quality-renamed'))
      assert_includes verify(root).join('\n'), 'quality-renamed'
    end
  end

  def test_required_job_removal_makes_live_context_stale
    with_repo do |root, _policy, _ruleset|
      path = File.join(root, '.github/workflows/ci.yml')
      body = File.read(path).sub(/\n  quality:\n    name: quality\n    runs-on: ubuntu-latest\n/, "\n")
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'quality'
    end
  end

  def test_policy_required_context_missing_from_ruleset
    with_repo do |root, _policy, _ruleset|
      write_json(root, 'ruleset.json', ruleset_with('Analyze (actions)', 'quality', 'postgres-15-repository'))
      assert_includes verify(root).join('\n'), 'postgres-18-repository'
    end
  end

  def test_stale_live_context_is_rejected
    with_repo do |root, _policy, _ruleset|
      write_json(root, 'ruleset.json', ruleset_with('Analyze (actions)', 'quality', 'postgres-15-repository', 'postgres-18-repository', 'old-job'))
      assert_includes verify(root).join('\n'), 'old-job'
    end
  end

  def test_required_workflow_path_filter_is_rejected
    with_repo do |root, _policy, _ruleset|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub('branches: [main]', "branches: [main]\n    paths: ['src/**']")
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'path filters'
    end
  end

  def test_required_job_condition_is_rejected
    with_repo do |root, _policy, _ruleset|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub("name: quality\n    runs-on:", "name: quality\n    if: github.actor != 'nobody'\n    runs-on:")
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'not guaranteed to emit'
    end
  end

  def test_matrix_context_set_mismatch_is_rejected
    with_repo do |root, _policy, _ruleset|
      path = File.join(root, '.github/workflows/ci.yml')
      original = File.read(path)
      body = original.sub('["15", "18"]', '["15", "17", "18"]')
      refute_equal original, body
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'postgres-17-repository'
    end
  end

  def test_new_unclassified_pr_workflow_is_rejected
    with_repo do |root, _policy, _ruleset|
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
    with_repo { |root, _policy, _ruleset| refute_includes verify(root).join('\n'), 'Analyze (actions)' }
  end

  def test_pending_context_must_still_be_required
    with_repo do |root, policy, _ruleset|
      policy['pending_ruleset_contexts'] = ['does-not-exist']
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join('\n'), 'not required producers'
    end
  end

  def test_candidate_aggregate_must_be_pending
    with_repo do |root, policy, _ruleset|
      policy['pending_ruleset_contexts'] = ['evidence-provenance']
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join('\n'), 'must be pending ruleset promotion'
    end
  end

  def test_active_aggregate_replaces_member_contexts
    with_repo do |root, policy, _ruleset|
      policy['aggregate_gates'][0]['state'] = 'active'
      policy['pending_ruleset_contexts'] = ['evidence-provenance']
      write_json(root, '.github/merge-gate-policy.json', policy)
      write_json(root, 'ruleset.json', ruleset_with('Analyze (actions)', 'quality', 'postgresql'))
      assert_empty verify(root)
    end
  end

  def test_aggregate_member_removal_is_fail_closed
    with_repo do |root, _policy, _ruleset|
      path = File.join(root, '.github/workflows/ci.yml')
      body = File.read(path).sub('["15", "18"]', '["15"]')
      write(root, '.github/workflows/ci.yml', body)
      assert_includes verify(root).join('\n'), 'members are not required producers: postgres-18-repository'
    end
  end

  def test_aggregate_producer_must_be_trusted_and_status_capable
    with_repo do |root, _policy, _ruleset|
      path = File.join(root, '.github/workflows/postgres-merge-gate.yml')
      body = File.read(path).sub('statuses: write', 'statuses: read')
      write(root, '.github/workflows/postgres-merge-gate.yml', body)
      assert_includes verify(root).join('\n'), 'permissions must equal'
    end
  end

  def test_aggregate_workflow_run_must_cover_every_member_source_workflow
    with_repo do |root, _policy, _ruleset|
      path = File.join(root, '.github/workflows/postgres-merge-gate.yml')
      body = File.read(path).sub('workflows: [Rust]', 'workflows: [Other]')
      write(root, '.github/workflows/postgres-merge-gate.yml', body)
      assert_includes verify(root).join('\n'), 'workflow_run workflows mismatch'
    end
  end

  def test_aggregate_workflow_run_must_reset_status_on_rerun_start
    with_repo do |root, _policy, _ruleset|
      path = File.join(root, '.github/workflows/postgres-merge-gate.yml')
      body = File.read(path).sub('types: [in_progress, completed]', 'types: [completed]')
      write(root, '.github/workflows/postgres-merge-gate.yml', body)
      assert_includes verify(root).join('\n'), 'workflow_run types mismatch'
    end
  end

  def test_dangling_job_override_is_rejected
    with_repo do |root, policy, _ruleset|
      policy['job_overrides'] = [{
        'workflow' => '.github/workflows/ci.yml',
        'job' => 'missing-job',
        'classification' => 'required'
      }]
      write_json(root, '.github/merge-gate-policy.json', policy)
      assert_includes verify(root).join('\n'), 'job override references missing PR job'
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
        'parameters' => {'required_status_checks' => contexts.map { |context| {'context' => context} }}
      }]
    }
  end
end
