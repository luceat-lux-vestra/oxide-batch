#!/usr/bin/env ruby
# frozen_string_literal: true

require 'json'
require 'net/http'
require 'uri'
require_relative 'verify-merge-gates'

module AggregateStatus
  module_function

  def evaluate(gate:, check_runs:, running_workflow: nil, completed_workflow: nil, context_sources:, workflow_names:)
    runs = check_runs.select { |run| run.dig('app', 'slug') == 'github-actions' }
    latest = runs.group_by { |run| run.fetch('name') }.transform_values do |items|
      items.max_by { |item| item.fetch('id', 0) }
    end
    failures = []
    pending = []

    gate.fetch('members').each do |member|
      source = context_sources[member]
      source_name = source && workflow_names[source['workflow']]
      if running_workflow && source_name == running_workflow
        pending << "#{member}: source workflow #{running_workflow} in progress"
        next
      end

      run = latest[member]
      if run.nil?
        if completed_workflow && source_name == completed_workflow
          failures << "#{member}: missing after #{completed_workflow} completed"
        else
          pending << "#{member}: missing"
        end
        next
      end

      unless run['status'] == 'completed'
        pending << "#{member}: #{run['status'] || 'unknown'}"
        next
      end

      conclusion = run['conclusion']
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

  def event_target(event_name, event)
    case event_name
    when 'pull_request_target'
      [event.dig('pull_request', 'head', 'sha'), nil, nil]
    when 'workflow_run'
      run = event['workflow_run'] || {}
      return [nil, nil, nil] unless run['event'] == 'pull_request'
      running = event['action'] == 'in_progress' ? run['name'] : nil
      completed = event['action'] == 'completed' ? run['name'] : nil
      [run['head_sha'], running, completed]
    else
      [nil, nil, nil]
    end
  end

  def workflow_names(root, sources)
    sources.values.filter_map { |source| source['workflow'] if source['kind'] == 'job' }.uniq.to_h do |workflow|
      doc = MergeGateVerifier.load_yaml(File.join(root, workflow))
      [workflow, doc['name'] || workflow]
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

  def fetch_check_runs(api:, repository:, sha:, token:)
    page = 1
    all = []
    loop do
      uri = URI("#{api}/repos/#{repository}/commits/#{sha}/check-runs?filter=latest&per_page=100&page=#{page}")
      body = request_json(:get, uri, token)
      batch = body.fetch('check_runs')
      all.concat(batch)
      break if batch.length < 100
      page += 1
    end
    all
  end

  def publish_status(api:, repository:, sha:, token:, context:, state:, details:)
    description = case state
                  when 'success' then 'All canonical PostgreSQL merge checks passed'
                  when 'failure' then 'A canonical PostgreSQL merge check failed or disappeared'
                  else 'Waiting for canonical PostgreSQL merge checks'
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
    sha, running_workflow, completed_workflow = event_target(ENV.fetch('GITHUB_EVENT_NAME'), event)
    return if sha.nil?

    producer_violations, summary = MergeGateVerifier.producer_inventory(root: root, policy: policy)
    aggregate_violations, = MergeGateVerifier.aggregate_inventory(root: root, policy: policy, producer_summary: summary)
    violations = producer_violations + aggregate_violations
    raise "aggregate policy inventory invalid: #{violations.join('; ')}" unless violations.empty?

    names = workflow_names(root, summary.fetch('context_sources'))
    checks = fetch_check_runs(
      api: ENV.fetch('GITHUB_API_URL', 'https://api.github.com'),
      repository: ENV.fetch('GITHUB_REPOSITORY'),
      sha: sha,
      token: ENV.fetch('GITHUB_TOKEN')
    )
    state, details = evaluate(
      gate: gate,
      check_runs: checks,
      running_workflow: running_workflow,
      completed_workflow: completed_workflow,
      context_sources: summary.fetch('context_sources'),
      workflow_names: names
    )
    publish_status(
      api: ENV.fetch('GITHUB_API_URL', 'https://api.github.com'),
      repository: ENV.fetch('GITHUB_REPOSITORY'),
      sha: sha,
      token: ENV.fetch('GITHUB_TOKEN'),
      context: gate.fetch('context'),
      state: state,
      details: details
    )
  end
end

AggregateStatus.run if __FILE__ == $PROGRAM_NAME
