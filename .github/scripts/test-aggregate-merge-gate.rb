#!/usr/bin/env ruby
# frozen_string_literal: true

require 'minitest/autorun'
require_relative 'reconcile-aggregate-status'

class AggregateStatusTest < Minitest::Test
  GATE = {
    'context' => 'postgresql',
    'members' => ['postgres-15-repository', 'postgres-18-conformance-campaign']
  }.freeze
  SOURCES = {
    'postgres-15-repository' => {'kind' => 'job', 'workflow' => '.github/workflows/ci.yml', 'job' => 'postgres'},
    'postgres-18-conformance-campaign' => {'kind' => 'job', 'workflow' => '.github/workflows/m5-conformance.yml', 'job' => 'conformance-campaign'}
  }.freeze

  def workflow_run(id:, status: 'completed', started: '2026-09-03T10:00:00Z', attempt: 1)
    {'id' => id, 'status' => status, 'run_started_at' => started, 'run_attempt' => attempt}
  end

  def job(name, status: 'completed', conclusion: 'success', id: 1)
    {'id' => id, 'name' => name, 'status' => status, 'conclusion' => conclusion}
  end

  def snapshots(ci_run: workflow_run(id: 1), ci_jobs: [job('postgres-15-repository')], conformance_run: workflow_run(id: 2), conformance_jobs: [job('postgres-18-conformance-campaign')])
    {
      '.github/workflows/ci.yml' => ci_run && {'run' => ci_run, 'jobs' => ci_jobs},
      '.github/workflows/m5-conformance.yml' => conformance_run && {'run' => conformance_run, 'jobs' => conformance_jobs}
    }
  end

  def evaluate(source_snapshots)
    AggregateStatus.evaluate(gate: GATE, source_snapshots: source_snapshots, context_sources: SOURCES)
  end

  def test_all_members_success
    state, details = evaluate(snapshots)
    assert_equal 'success', state
    assert_empty details['failures']
    assert_empty details['pending']
  end

  def test_failed_member_is_fail_closed
    state, details = evaluate(snapshots(ci_jobs: [job('postgres-15-repository', conclusion: 'failure')]))
    assert_equal 'failure', state
    assert_includes details['failures'].join('\n'), 'failure'
  end

  def test_cancelled_member_is_fail_closed
    state, = evaluate(snapshots(ci_jobs: [job('postgres-15-repository', conclusion: 'cancelled')]))
    assert_equal 'failure', state
  end

  def test_skipped_member_is_fail_closed
    state, = evaluate(snapshots(ci_jobs: [job('postgres-15-repository', conclusion: 'skipped')]))
    assert_equal 'failure', state
  end

  def test_active_source_workflow_keeps_aggregate_pending_even_with_old_success_jobs
    state, details = evaluate(snapshots(
      ci_run: workflow_run(id: 1, status: 'in_progress'),
      ci_jobs: [job('postgres-15-repository', conclusion: 'success')]
    ))
    assert_equal 'pending', state
    assert_includes details['pending'].join('\n'), 'source workflow in_progress'
  end

  def test_missing_source_workflow_run_is_pending
    state, = evaluate(snapshots(ci_run: nil, ci_jobs: []))
    assert_equal 'pending', state
  end

  def test_missing_member_from_completed_source_workflow_fails
    state, details = evaluate(snapshots(ci_jobs: []))
    assert_equal 'failure', state
    assert_includes details['failures'].join('\n'), 'missing from completed source workflow run'
  end

  def test_incomplete_job_from_completed_source_is_pending
    state, = evaluate(snapshots(ci_jobs: [job('postgres-15-repository', status: 'in_progress', conclusion: nil)]))
    assert_equal 'pending', state
  end

  def test_active_run_wins_over_completed_runs
    selected = AggregateStatus.select_source_run([
      workflow_run(id: 10, status: 'completed', started: '2026-09-03T11:00:00Z'),
      workflow_run(id: 5, status: 'in_progress', started: '2026-09-03T09:00:00Z', attempt: 2)
    ])
    assert_equal 5, selected['id']
  end

  def test_most_recently_started_completed_run_wins_even_if_id_is_older
    selected = AggregateStatus.select_source_run([
      workflow_run(id: 10, started: '2026-09-03T10:00:00Z'),
      workflow_run(id: 5, started: '2026-09-03T12:00:00Z', attempt: 2)
    ])
    assert_equal 5, selected['id']
  end
end
