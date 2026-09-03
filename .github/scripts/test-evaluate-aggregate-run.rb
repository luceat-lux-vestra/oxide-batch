#!/usr/bin/env ruby
# frozen_string_literal: true

require 'json'
require 'minitest/autorun'
require 'socket'
require 'tmpdir'
require_relative 'evaluate-aggregate-run'

class AggregateEvaluatorTest < Minitest::Test
  MEMBERS = %w[postgres-15-design-gate postgres-18-design-gate].freeze

  POLICY_MEMBERS = %w[
    postgres-15-design-gate
    postgres-15-item-components
    postgres-15-repository
    postgres-16-design-gate
    postgres-17-design-gate
    postgres-18-design-gate
    postgres-18-item-components
    postgres-18-repository
    postgres-spike
  ].freeze

  def job(name, attempt, status, conclusion, id: nil)
    {'name' => name, 'run_attempt' => attempt, 'status' => status, 'conclusion' => conclusion, 'id' => id}.compact
  end

  def reasons(diagnostics)
    diagnostics.reject { |d| d['reason'] == 'pass' }.map { |d| d['member'] }
  end

  # 1. all members latest success -> PASS
  def test_all_members_latest_success_passes
    jobs = MEMBERS.map { |name| job(name, 1, 'completed', 'success') }
    passed, diagnostics = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
    assert passed
    assert(diagnostics.all? { |d| d['reason'] == 'pass' })
  end

  def test_all_nine_policy_members_latest_success_passes
    jobs = POLICY_MEMBERS.map { |name| job(name, 1, 'completed', 'success') }
    passed, = AggregateEvaluator.evaluate(members: POLICY_MEMBERS, jobs: jobs)
    assert passed
  end

  # 2. latest failure -> FAIL
  def test_latest_failure_fails
    jobs = [job('postgres-15-design-gate', 1, 'completed', 'failure'), job('postgres-18-design-gate', 1, 'completed', 'success')]
    passed, diagnostics = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
    refute passed
    assert_equal ['postgres-15-design-gate'], reasons(diagnostics)
  end

  # 3. latest cancelled -> FAIL
  def test_latest_cancelled_fails
    jobs = [job('postgres-15-design-gate', 1, 'completed', 'cancelled'), job('postgres-18-design-gate', 1, 'completed', 'success')]
    passed, = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
    refute passed
  end

  # 4. latest skipped -> FAIL
  def test_latest_skipped_fails
    jobs = [job('postgres-15-design-gate', 1, 'completed', 'skipped'), job('postgres-18-design-gate', 1, 'completed', 'success')]
    passed, = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
    refute passed
  end

  def test_every_non_success_conclusion_fails
    %w[failure cancelled skipped neutral timed_out action_required stale startup_failure].each do |conclusion|
      jobs = [job('postgres-15-design-gate', 1, 'completed', conclusion), job('postgres-18-design-gate', 1, 'completed', 'success')]
      passed, = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
      refute passed, "expected conclusion #{conclusion.inspect} to fail the aggregate"
    end
  end

  # 5. missing canonical member -> FAIL
  def test_missing_member_fails
    jobs = [job('postgres-15-design-gate', 1, 'completed', 'success')]
    passed, diagnostics = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
    refute passed
    missing = diagnostics.find { |d| d['member'] == 'postgres-18-design-gate' }
    assert_equal 'missing', missing['reason']
  end

  # 6. latest member queued/in_progress -> FAIL
  def test_latest_in_progress_fails
    jobs = [job('postgres-15-design-gate', 1, 'in_progress', nil), job('postgres-18-design-gate', 1, 'completed', 'success')]
    passed, diagnostics = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
    refute passed
    assert_equal 'not_completed', diagnostics.find { |d| d['member'] == 'postgres-15-design-gate' }['reason']
  end

  def test_latest_queued_fails
    jobs = [job('postgres-15-design-gate', 1, 'queued', nil), job('postgres-18-design-gate', 1, 'completed', 'success')]
    passed, = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
    refute passed
  end

  # 7. duplicate entries for the same member at the same latest attempt -> FAIL
  def test_duplicate_latest_attempt_fails
    jobs = [
      job('postgres-15-design-gate', 2, 'completed', 'success', id: 1),
      job('postgres-15-design-gate', 2, 'completed', 'success', id: 2),
      job('postgres-18-design-gate', 1, 'completed', 'success')
    ]
    passed, diagnostics = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
    refute passed
    assert_equal 'duplicate', diagnostics.find { |d| d['member'] == 'postgres-15-design-gate' }['reason']
  end

  # 8. Case A: old failure + selective rerun success, sibling previously success -> PASS
  def test_selective_rerun_repairs_single_failure
    jobs = [
      job('postgres-15-design-gate', 1, 'completed', 'failure'),
      job('postgres-15-design-gate', 2, 'completed', 'success'),
      job('postgres-18-design-gate', 1, 'completed', 'success')
    ]
    passed, = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
    assert passed
  end

  # 9. Case B: two old failures, selective rerun repairs only one -> FAIL
  def test_selective_rerun_leaves_unrepaired_sibling_failing
    jobs = [
      job('postgres-15-design-gate', 1, 'completed', 'failure'),
      job('postgres-15-design-gate', 2, 'completed', 'success'),
      job('postgres-18-design-gate', 1, 'completed', 'failure')
    ]
    passed, diagnostics = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
    refute passed
    assert_equal ['postgres-18-design-gate'], reasons(diagnostics)
  end

  # 10. old success + newer full-rerun failure -> FAIL
  def test_newer_full_rerun_failure_overrides_old_success
    jobs = [
      job('postgres-15-design-gate', 1, 'completed', 'success'),
      job('postgres-15-design-gate', 2, 'completed', 'failure'),
      job('postgres-18-design-gate', 1, 'completed', 'success'),
      job('postgres-18-design-gate', 2, 'completed', 'success')
    ]
    passed, diagnostics = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
    refute passed
    assert_equal ['postgres-15-design-gate'], reasons(diagnostics)
  end

  # 11. old failure must not override newer success
  def test_old_failure_does_not_override_newer_success
    jobs = [
      job('postgres-15-design-gate', 1, 'completed', 'failure'),
      job('postgres-15-design-gate', 2, 'completed', 'failure'),
      job('postgres-15-design-gate', 3, 'completed', 'success'),
      job('postgres-18-design-gate', 1, 'completed', 'success')
    ]
    passed, = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
    assert passed
  end

  def test_selection_is_per_member_not_global_attempt_number
    # postgres-15 has already reached attempt 3; postgres-18 never needed a
    # rerun and is still sitting at attempt 1. The evaluator must not demand
    # attempt 3 globally.
    jobs = [
      job('postgres-15-design-gate', 1, 'completed', 'failure'),
      job('postgres-15-design-gate', 2, 'completed', 'failure'),
      job('postgres-15-design-gate', 3, 'completed', 'success'),
      job('postgres-18-design-gate', 1, 'completed', 'success')
    ]
    passed, = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
    assert passed
  end

  def test_schema_error_on_non_integer_run_attempt_fails_closed
    jobs = [
      {'name' => 'postgres-15-design-gate', 'run_attempt' => nil, 'status' => 'completed', 'conclusion' => 'success'},
      job('postgres-18-design-gate', 1, 'completed', 'success')
    ]
    passed, diagnostics = AggregateEvaluator.evaluate(members: MEMBERS, jobs: jobs)
    refute passed
    assert_equal 'schema_error', diagnostics.find { |d| d['member'] == 'postgres-15-design-gate' }['reason']
  end

  def test_evaluate_rejects_empty_member_list
    assert_raises(AggregateEvaluator::EvaluationError) { AggregateEvaluator.evaluate(members: [], jobs: []) }
  end

  def test_evaluate_rejects_non_array_jobs
    assert_raises(AggregateEvaluator::EvaluationError) { AggregateEvaluator.evaluate(members: MEMBERS, jobs: nil) }
  end

  # --- policy loading -----------------------------------------------------

  def test_aggregate_members_reads_canonical_list_from_policy
    Dir.mktmpdir do |dir|
      path = File.join(dir, 'policy.json')
      File.write(path, JSON.generate(
        'aggregate_gates' => [
          {'context' => 'postgresql', 'members' => ['postgres-15-design-gate', 'postgres-18-design-gate']}
        ]
      ))
      assert_equal MEMBERS, AggregateEvaluator.aggregate_members(policy_path: path, context: 'postgresql')
    end
  end

  def test_aggregate_members_reads_real_repository_policy
    root = File.expand_path('../..', __dir__)
    members = AggregateEvaluator.aggregate_members(
      policy_path: File.join(root, '.github/merge-gate-policy.json'),
      context: 'postgresql'
    )
    assert_equal POLICY_MEMBERS.sort, members.sort
  end

  def test_aggregate_members_fails_closed_on_unknown_context
    Dir.mktmpdir do |dir|
      path = File.join(dir, 'policy.json')
      File.write(path, JSON.generate('aggregate_gates' => []))
      assert_raises(AggregateEvaluator::EvaluationError) do
        AggregateEvaluator.aggregate_members(policy_path: path, context: 'postgresql')
      end
    end
  end

  def test_aggregate_members_fails_closed_on_invalid_json
    Dir.mktmpdir do |dir|
      path = File.join(dir, 'policy.json')
      File.write(path, 'not json')
      assert_raises(AggregateEvaluator::EvaluationError) do
        AggregateEvaluator.aggregate_members(policy_path: path, context: 'postgresql')
      end
    end
  end

  # --- pagination against a real local HTTP server ------------------------

  def test_fetch_all_jobs_paginates_until_exhausted
    server = TCPServer.new('127.0.0.1', 0)
    port = server.addr[1]
    page_one = Array.new(100) { |i| job("filler-#{i}", 1, 'completed', 'success') }
    page_two = [job('postgres-15-design-gate', 1, 'completed', 'success')]

    thread = Thread.new do
      2.times do
        client = server.accept
        request_line = client.gets
        headers = []
        loop do
          line = client.gets
          break if line.nil? || line == "\r\n"
          headers << line
        end
        page = request_line[/[?&]page=(\d+)/, 1].to_i
        body = JSON.generate('jobs' => page == 1 ? page_one : page_two)
        client.write("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: #{body.bytesize}\r\nConnection: close\r\n\r\n#{body}")
        client.close
      end
    end

    jobs = AggregateEvaluator.fetch_all_jobs(
      api_url: "http://127.0.0.1:#{port}",
      repository: 'owner/repo',
      run_id: '123',
      token: 'test-token'
    )
    thread.join
    server.close

    assert_equal 101, jobs.length
    assert(jobs.any? { |j| j['name'] == 'postgres-15-design-gate' })
  end

  def test_fetch_all_jobs_fails_closed_on_http_error
    server = TCPServer.new('127.0.0.1', 0)
    port = server.addr[1]

    thread = Thread.new do
      client = server.accept
      client.gets
      loop do
        line = client.gets
        break if line.nil? || line == "\r\n"
      end
      body = 'boom'
      client.write("HTTP/1.1 500 Internal Server Error\r\nContent-Type: text/plain\r\nContent-Length: #{body.bytesize}\r\nConnection: close\r\n\r\n#{body}")
      client.close
    end

    error = assert_raises(AggregateEvaluator::EvaluationError) do
      AggregateEvaluator.fetch_all_jobs(api_url: "http://127.0.0.1:#{port}", repository: 'owner/repo', run_id: '1', token: 't')
    end
    thread.join
    server.close
    assert_match(/HTTP 500/, error.message)
  end

  def test_fetch_all_jobs_fails_closed_on_malformed_json
    server = TCPServer.new('127.0.0.1', 0)
    port = server.addr[1]

    thread = Thread.new do
      client = server.accept
      client.gets
      loop do
        line = client.gets
        break if line.nil? || line == "\r\n"
      end
      body = '{not json'
      client.write("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: #{body.bytesize}\r\nConnection: close\r\n\r\n#{body}")
      client.close
    end

    assert_raises(AggregateEvaluator::EvaluationError) do
      AggregateEvaluator.fetch_all_jobs(api_url: "http://127.0.0.1:#{port}", repository: 'owner/repo', run_id: '1', token: 't')
    end
    thread.join
    server.close
  end
end
