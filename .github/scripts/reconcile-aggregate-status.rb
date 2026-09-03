#!/usr/bin/env ruby
# frozen_string_literal: true

require 'json'
require 'net/http'
require 'time'
require 'uri'
require_relative 'verify-merge-gates'

module AggregateStatus
  module_function

  def evaluate(gate:, source_snapshots:, context_sources:)
    failures = []
    pending = []

    gate.fetch('members').each do |member|
      source = context_sources.fetch(member)
      workflow = source.fetch('workflow')
      snapshot = source_snapshots[workflow]
      if snapshot.nil?
        pending << "#{member}: source workflow has no pull-request run for this head"
        next
      end

      run = snapshot.fetch('run')
      unless run['status'] == 'completed'
        pending << "#{member}: source workflow #{run['status'] || 'unknown'}"
        next
      end

      jobs = snapshot.fetch('jobs').group_by { |job| job.fetch('name') }
      job = jobs.fetch(member, []).max_by { |candidate| candidate.fetch('id', 0) }
      if job.nil?
        failures << "#{member}: missing from completed source workflow run #{run.fetch('id')}"
        next
      end
      unless job['status'] == 'completed'
        pending << "#{member}: #{job['status'] || 'unknown'}"
        next
      end

      conclusion = job['conclusion']
      failures << "#{member}: #{conclusion || 'no conclusion'}" unless conclusion == 'success'
    end

    state = if failures.any?
              'failure'
            elsif pending.any?
              'pending'
            else
              'success'
            end
    [state, {'failures' => failures, 'pending' => pending}]
  end

  def select_source_run(runs)
    active = runs.reject { |run| run['status'] == 'completed' }
    candidates = active.empty? ? runs : active
    candidates.max_by do |run|
      timestamp = run['run_started_at'] || run['updated_at'] || run['created_at'] || ''
      [timestamp, run.fetch('run_attempt', 0), run.fetch('id', 0)]
    end
  end

  def event_head_sha(event_name, event)
    case event_name
    when 'pull_request_target'
      event.dig('pull_request', 'head', 'sha')
    when 'workflow_run'
      run = event['workflow_run'] || {}
      return nil unless run['event'] == 'pull_request'
      run['head_sha']
    end
  end

  def request_json(method, uri, token, body = nil)
    request = method == :get ? Net::HTTP::Get.new(uri) : Net::HTTP::Post.new(uri)
    request['Accept'] = 'application/vnd.github+json'
    request['Authorization'] = "Bearer #{token}"
    request['X-GitHub-Api-Version'] = '2022-11-28'
    if body
      request['Content-Type'] = 'application/json'
      request.body = JSON.generate(body)
    end
    response = Net::HTTP.start(uri.host, uri.port, use_ssl: uri.scheme == 'https') { |http| http.request(request) }
    raise "GitHub API #{method.to_s.upcase} #{uri} failed: #{response.code} #{response.body}" unless response.is_a?(Net::HTTPSuccess)
    response.body.empty? ? {} : JSON.parse(response.body)
  end

  def fetch_workflow_runs(api:, repository:, workflow:, sha:, token:)
    page = 1
    runs = []
    loop do
      query = URI.encode_www_form(event: 'pull_request', head_sha: sha, per_page: 100, page: page)
      filename = URI.encode_www_form_component(File.basename(workflow))
      uri = URI("#{api}/repos/#{repository}/actions/workflows/#{filename}/runs?#{query}")
      batch = request_json(:get, uri, token).fetch('workflow_runs')
      runs.concat(batch.select { |run| run['head_sha'] == sha && run['event'] == 'pull_request' })
      break if batch.length < 100
      page += 1
    end
    runs
  end

  def fetch_jobs(api:, repository:, run_id:, token:)
    page = 1
    jobs = []
    loop do
      query = URI.encode_www_form(filter: 'latest', per_page: 100, page: page)
      uri = URI("#{api}/repos/#{repository}/actions/runs/#{run_id}/jobs?#{query}")
      batch = request_json(:get, uri, token).fetch('jobs')
      jobs.concat(batch)
      break if batch.length < 100
      page += 1
    end
    jobs
  end

  def source_snapshots(api:, repository:, sha:, token:, gate:, context_sources:)
    workflows = gate.fetch('members').map { |member| context_sources.fetch(member).fetch('workflow') }.uniq
    workflows.to_h do |workflow|
      run = select_source_run(fetch_workflow_runs(
        api: api,
        repository: repository,
        workflow: workflow,
        sha: sha,
        token: token
      ))
      snapshot = if run.nil?
                   nil
                 elsif run['status'] == 'completed'
                   {'run' => run, 'jobs' => fetch_jobs(api: api, repository: repository, run_id: run.fetch('id'), token: token)}
                 else
                   {'run' => run, 'jobs' => []}
                 end
      [workflow, snapshot]
    end
  end

  def publish_status(api:, repository:, sha:, token:, context:, state:, details:)
    description = case state
                  when 'success' then 'All canonical PostgreSQL merge jobs passed'
                  when 'failure' then 'A canonical PostgreSQL merge job failed or disappeared'
                  else 'Waiting for canonical PostgreSQL merge jobs'
                  end
    uri = URI("#{api}/repos/#{repository}/statuses/#{sha}")
    request_json(:post, uri, token, {'state' => state, 'context' => context, 'description' => description})
    warn JSON.generate({'context' => context, 'state' => state}.merge(details))
  end

  def run(root: Dir.pwd)
    policy = JSON.parse(File.read(File.join(root, '.github/merge-gate-policy.json')))
    gates = policy.fetch('aggregate_gates', [])
    raise "expected exactly one aggregate gate, found #{gates.length}" unless gates.length == 1
    gate = gates.first

    event = JSON.parse(File.read(ENV.fetch('GITHUB_EVENT_PATH')))
    sha = event_head_sha(ENV.fetch('GITHUB_EVENT_NAME'), event)
    return if sha.nil?

    api = ENV.fetch('GITHUB_API_URL', 'https://api.github.com')
    repository = ENV.fetch('GITHUB_REPOSITORY')
    token = ENV.fetch('GITHUB_TOKEN')

    # Reset first. If policy/API reconciliation fails afterwards, an old success
    # cannot remain merge-authoritative for a same-SHA rerun.
    publish_status(
      api: api,
      repository: repository,
      sha: sha,
      token: token,
      context: gate.fetch('context'),
      state: 'pending',
      details: {'phase' => 'reset'}
    )

    producer_violations, summary = MergeGateVerifier.producer_inventory(root: root, policy: policy)
    aggregate_violations, = MergeGateVerifier.aggregate_inventory(root: root, policy: policy, producer_summary: summary)
    violations = producer_violations + aggregate_violations
    raise "aggregate policy inventory invalid: #{violations.join('; ')}" unless violations.empty?

    snapshots = source_snapshots(
      api: api,
      repository: repository,
      sha: sha,
      token: token,
      gate: gate,
      context_sources: summary.fetch('context_sources')
    )
    state, details = evaluate(
      gate: gate,
      source_snapshots: snapshots,
      context_sources: summary.fetch('context_sources')
    )
    publish_status(
      api: api,
      repository: repository,
      sha: sha,
      token: token,
      context: gate.fetch('context'),
      state: state,
      details: details
    )
  end
end

AggregateStatus.run if __FILE__ == $PROGRAM_NAME
