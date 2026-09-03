#!/usr/bin/env ruby
# frozen_string_literal: true

require 'minitest/autorun'
require_relative 'reconcile-aggregate-status'

class AggregateStatusTest < Minitest::Test
  GATE = {
    'context' => 'postgresql',
    'members' => ['postgres-15-repository', 'postgres-18-repository']
  }.freeze
  SOURCES = {
    'postgres-15-repository' => {'kind' => 'job', 'workflow' => '.github/workflows/ci.yml', 'job' => 'postgres'},
    'postgres-18-repository' => {'kind' => 'job', 'workflow' => '.github/workflows/ci.yml', 'job' => 'postgres'}
  }.freeze
  NAMES = {'.github/workflows/ci.yml' => 'Rust'}.freeze

  def check_run(name, status: 'completed', conclusion: 'success', id: 1)
    {'id' => id, 'name' => name, 'status' => status, 'conclusion' => conclusion, 'app' => {'slug' => 'github-actions'}}
  end

  def evaluate(checks, running_workflow: nil, completed_workflow: nil)
    AggregateStatus.evaluate(
      gate: GATE,
      check_runs: checks,
      running_workflow: running_workflow,
      completed_workflow: completed_workflow,
      context_sources: SOURCES,
      workflow_names: NAMES
    )
  end

  def test_all_members_success
    state, details = evaluate([
      check_run('postgres-15-repository', id: 1),
      check_run('postgres-18-repository', id: 2)
    ])
    assert_equal 'success', state
    assert_empty details['failures']
    assert_empty details['pending']
  end

  def test_failure_is_fail_closed
    state, details = evaluate([
      check_run('postgres-15-repository', conclusion: 'failure', id: 1),
      check_run('postgres-18-repository', id: 2)
    ])
    assert_equal 'failure', state
    assert_includes details['failures'].join('\n'), 'failure'
  end

  def test_cancelled_is_fail_closed
    state, = evaluate([
      check_run('postgres-15-repository', conclusion: 'cancelled', id: 1),
      check_run('postgres-18-repository', id: 2)
    ])
    assert_equal 'failure', state
  end

  def test_skipped_is_fail_closed
    state, = evaluate([
      check_run('postgres-15-repository', conclusion: 'skipped', id: 1),
      check_run('postgres-18-repository', id: 2)
    ])
    assert_equal 'failure', state
  end

  def test_in_progress_member_keeps_aggregate_pending
    state, details = evaluate([
      check_run('postgres-15-repository', status: 'in_progress', conclusion: nil, id: 1),
      check_run('postgres-18-repository', id: 2)
    ])
    assert_equal 'pending', state
    assert_includes details['pending'].join('\n'), 'in_progress'
  end

  def test_missing_member_is_pending_before_source_workflow_completes
    state, = evaluate([check_run('postgres-15-repository')])
    assert_equal 'pending', state
  end

  def test_missing_member_fails_after_source_workflow_completes
    state, details = evaluate([check_run('postgres-15-repository')], completed_workflow: 'Rust')
    assert_equal 'failure', state
    assert_includes details['failures'].join('\n'), 'missing after Rust completed'
  end

  def test_latest_check_run_wins_for_reruns
    state, = evaluate([
      check_run('postgres-15-repository', conclusion: 'failure', id: 1),
      check_run('postgres-15-repository', conclusion: 'success', id: 3),
      check_run('postgres-18-repository', id: 2)
    ])
    assert_equal 'success', state
  end

  def test_source_rerun_forces_members_pending_before_new_checks_materialize
    state, details = evaluate([
      check_run('postgres-15-repository', conclusion: 'success', id: 1),
      check_run('postgres-18-repository', conclusion: 'success', id: 2)
    ], running_workflow: 'Rust')
    assert_equal 'pending', state
    assert_includes details['pending'].join('\n'), 'source workflow Rust in progress'
  end

  def test_non_github_actions_check_cannot_spoof_member
    checks = [
      {'id' => 1, 'name' => 'postgres-15-repository', 'status' => 'completed', 'conclusion' => 'success', 'app' => {'slug' => 'other'}},
      check_run('postgres-18-repository', id: 2)
    ]
    state, = evaluate(checks, completed_workflow: 'Rust')
    assert_equal 'failure', state
  end
end
