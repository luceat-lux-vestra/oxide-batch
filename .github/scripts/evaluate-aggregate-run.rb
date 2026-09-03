#!/usr/bin/env ruby
# frozen_string_literal: true

require 'json'
require 'net/http'
require 'uri'

# Selective-rerun-safe evaluator for the native `postgresql` GitHub Actions
# aggregate. Raw workflow-level `needs.<job>.result` collapses every matrix
# child of a job id into one boolean and cannot see per-matrix-child rerun
# history, so a selective rerun of one failed matrix child can make the
# `needs` result look green while an un-rerun sibling matrix child is still
# failing on an older attempt. This evaluator instead reads every job
# execution recorded for the current workflow run via the GitHub Actions
# Jobs API (`filter=all`, paginated across all workflow attempts) and, for
# each canonical member context declared in `.github/merge-gate-policy.json`,
# independently selects that member's own highest `run_attempt` and requires
# that specific execution to be a completed success.
module AggregateEvaluator
  module_function

  class EvaluationError < StandardError; end

  API_VERSION = '2022-11-28'
  PER_PAGE = 100
  COMPLETED_STATUS = 'completed'
  SUCCESS_CONCLUSION = 'success'

  # Pure, network-free reconciliation: given the canonical member context
  # names and the raw Jobs API job entries for a run (any number of
  # attempts, any number of unrelated jobs), decide pass/fail and produce a
  # diagnostic per member.
  #
  # jobs: array of hashes with at least 'name', 'run_attempt', 'status',
  # 'conclusion', and optionally 'id'.
  def evaluate(members:, jobs:)
    raise EvaluationError, 'members must be a non-empty array' unless members.is_a?(Array) && !members.empty?
    raise EvaluationError, 'jobs must be an array' unless jobs.is_a?(Array)

    diagnostics = members.map { |member| evaluate_member(member: member, jobs: jobs) }
    passed = diagnostics.all? { |diagnostic| diagnostic['reason'] == 'pass' }
    [passed, diagnostics]
  end

  def evaluate_member(member:, jobs:)
    entries = jobs.select { |job| job.is_a?(Hash) && job['name'] == member }
    return member_diagnostic(member: member, reason: 'missing', detail: 'no Jobs API entry for this canonical member context') if entries.empty?

    attempts = entries.map { |entry| entry['run_attempt'] }
    if attempts.any? { |attempt| !attempt.is_a?(Integer) }
      return member_diagnostic(member: member, reason: 'schema_error', detail: 'job entry missing an integer run_attempt')
    end

    max_attempt = attempts.max
    latest = entries.select { |entry| entry['run_attempt'] == max_attempt }

    if latest.length > 1
      return member_diagnostic(
        member: member,
        reason: 'duplicate',
        detail: "#{latest.length} job entries share run_attempt=#{max_attempt}",
        attempt: max_attempt
      )
    end

    job = latest.first
    status = job['status']
    conclusion = job['conclusion']

    return member_diagnostic(member: member, reason: 'not_completed', status: status, conclusion: conclusion, attempt: max_attempt, job_id: job['id']) if status != COMPLETED_STATUS
    return member_diagnostic(member: member, reason: 'unsuccessful_conclusion', status: status, conclusion: conclusion, attempt: max_attempt, job_id: job['id']) if conclusion != SUCCESS_CONCLUSION

    member_diagnostic(member: member, reason: 'pass', status: status, conclusion: conclusion, attempt: max_attempt, job_id: job['id'])
  end

  def member_diagnostic(member:, reason:, detail: nil, status: nil, conclusion: nil, attempt: nil, job_id: nil)
    {
      'member' => member,
      'reason' => reason,
      'detail' => detail,
      'status' => status,
      'conclusion' => conclusion,
      'run_attempt' => attempt,
      'job_id' => job_id
    }.compact
  end

  def aggregate_members(policy_path:, context:)
    policy = JSON.parse(File.read(policy_path))
    raise EvaluationError, "#{policy_path}: not a JSON object" unless policy.is_a?(Hash)

    gates = policy['aggregate_gates']
    raise EvaluationError, "#{policy_path}: missing aggregate_gates array" unless gates.is_a?(Array)

    gate = gates.find { |candidate| candidate.is_a?(Hash) && candidate['context'] == context }
    raise EvaluationError, "#{policy_path}: no aggregate_gates entry for context #{context.inspect}" unless gate

    members = gate['members']
    raise EvaluationError, "#{policy_path}: aggregate #{context.inspect} has no non-empty member list" unless members.is_a?(Array) && !members.empty?
    raise EvaluationError, "#{policy_path}: aggregate #{context.inspect} members must all be strings" unless members.all? { |member| member.is_a?(String) && !member.empty? }

    members
  rescue JSON::ParserError => e
    raise EvaluationError, "#{policy_path}: invalid JSON: #{e.message}"
  rescue Errno::ENOENT => e
    raise EvaluationError, "#{policy_path}: #{e.message}"
  end

  # Fetches every job entry recorded for the run, across every workflow
  # attempt (`filter=all`), following pagination until GitHub reports no
  # further pages.
  def fetch_all_jobs(api_url:, repository:, run_id:, token:)
    jobs = []
    page = 1

    loop do
      uri = URI("#{api_url}/repos/#{repository}/actions/runs/#{run_id}/jobs")
      uri.query = URI.encode_www_form(filter: 'all', per_page: PER_PAGE, page: page)

      response = request_json(uri: uri, token: token)
      batch = response['jobs']
      raise EvaluationError, "Jobs API page #{page}: response missing a \"jobs\" array" unless batch.is_a?(Array)

      jobs.concat(batch)
      break if batch.length < PER_PAGE

      page += 1
    end

    jobs
  end

  def request_json(uri:, token:)
    request = Net::HTTP::Get.new(uri)
    request['Authorization'] = "Bearer #{token}"
    request['Accept'] = 'application/vnd.github+json'
    request['X-GitHub-Api-Version'] = API_VERSION
    request['User-Agent'] = 'oxide-batch-evaluate-aggregate-run'

    response = Net::HTTP.start(uri.hostname, uri.port, use_ssl: uri.scheme == 'https', open_timeout: 10, read_timeout: 20) do |http|
      http.request(request)
    end

    unless response.is_a?(Net::HTTPSuccess)
      raise EvaluationError, "GitHub Jobs API request failed: HTTP #{response.code} #{response.message}"
    end

    payload = JSON.parse(response.body)
    raise EvaluationError, 'GitHub Jobs API response was not a JSON object' unless payload.is_a?(Hash)

    payload
  rescue JSON::ParserError => e
    raise EvaluationError, "GitHub Jobs API response was not valid JSON: #{e.message}"
  rescue Timeout::Error, IOError, SocketError, SystemCallError => e
    raise EvaluationError, "GitHub Jobs API request failed: #{e.class}: #{e.message}"
  end

  def env_fetch(name)
    value = ENV[name]
    raise EvaluationError, "#{name} is required" if value.nil? || value.empty?

    value
  end

  def run(aggregate_context:, policy_path:)
    members = aggregate_members(policy_path: policy_path, context: aggregate_context)

    api_url = env_fetch('GITHUB_API_URL')
    repository = env_fetch('GITHUB_REPOSITORY')
    run_id = env_fetch('GITHUB_RUN_ID')
    token = env_fetch('GITHUB_TOKEN')

    jobs = fetch_all_jobs(api_url: api_url, repository: repository, run_id: run_id, token: token)
    evaluate(members: members, jobs: jobs)
  end
end

if __FILE__ == $PROGRAM_NAME
  aggregate_context = ARGV[0] || 'postgresql'
  policy_path = ARGV[1] || '.github/merge-gate-policy.json'

  begin
    passed, diagnostics = AggregateEvaluator.run(aggregate_context: aggregate_context, policy_path: policy_path)

    if passed
      puts "aggregate #{aggregate_context}: all #{diagnostics.length} canonical members' latest attempts are completed successes"
      diagnostics.each do |d|
        puts "  pass member=#{d['member']} run_attempt=#{d['run_attempt']} job_id=#{d['job_id']}"
      end
      exit 0
    end

    warn "aggregate #{aggregate_context}: selective-rerun-safe evaluation FAILED"
    diagnostics.reject { |d| d['reason'] == 'pass' }.each do |d|
      warn "  fail member=#{d['member']} reason=#{d['reason']} run_attempt=#{d['run_attempt'].inspect} " \
           "job_id=#{d['job_id'].inspect} status=#{d['status'].inspect} conclusion=#{d['conclusion'].inspect} " \
           "detail=#{d['detail'].inspect}"
    end
    exit 1
  rescue AggregateEvaluator::EvaluationError => e
    warn "aggregate #{aggregate_context} evaluator failed closed: #{e.message}"
    exit 1
  rescue StandardError => e
    warn "aggregate #{aggregate_context} evaluator failed closed on unexpected error: #{e.class}: #{e.message}"
    exit 1
  end
end
