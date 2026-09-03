#!/usr/bin/env ruby
# frozen_string_literal: true

require 'json'
require 'yaml'
require 'pathname'

module MergeGateVerifier
  module_function

  VALID_CLASSIFICATIONS = %w[required advisory optional].freeze
  MATRIX_EXPR = /\$\{\{\s*matrix\.([A-Za-z0-9_-]+)\s*\}\}/

  def load_yaml(path)
    YAML.safe_load(File.read(path), aliases: true) || {}
  rescue Psych::SyntaxError => e
    raise "cannot parse #{path}: #{e.message}"
  end

  def event_map(doc)
    doc['on'] || doc[true] || {}
  end

  def pr_trigger?(doc)
    events = event_map(doc)
    return events.any? { |name| %w[pull_request pull_request_target].include?(name.to_s) } if events.is_a?(Array)
    return %w[pull_request pull_request_target].include?(events.to_s) unless events.is_a?(Hash)

    events.key?('pull_request') || events.key?('pull_request_target')
  end

  def pr_event_configs(doc)
    events = event_map(doc)
    return [] unless events.is_a?(Hash)

    %w[pull_request pull_request_target].filter_map do |name|
      next unless events.key?(name)
      [name, events[name]]
    end
  end

  def default_classification(policy, workflow)
    matches = policy.fetch('workflow_defaults', []).select do |entry|
      File.fnmatch?(entry.fetch('pattern'), workflow, File::FNM_PATHNAME)
    end
    raise "workflow #{workflow} matches multiple policy defaults" if matches.length > 1
    matches.first&.fetch('classification', nil)
  end

  def job_policy(policy, workflow, job_id)
    override = policy.fetch('job_overrides', []).find do |entry|
      entry.fetch('workflow') == workflow && entry.fetch('job') == job_id
    end
    classification = override&.fetch('classification', nil) || default_classification(policy, workflow)
    [classification, override]
  end

  def required_job_contexts(job_id, job)
    name = job['name'] || job_id
    matrix = job.dig('strategy', 'matrix')
    return [name] unless matrix
    raise "required job #{job_id} uses a non-object matrix" unless matrix.is_a?(Hash)

    axes = matrix.reject { |key, _| %w[include exclude].include?(key.to_s) }
    raise "required job #{job_id} uses matrix include/exclude; model it explicitly before requiring it" if matrix.key?('include') || matrix.key?('exclude')
    values = axes.map do |key, raw|
      raise "required job #{job_id} matrix axis #{key} is not a literal array" unless raw.is_a?(Array) && raw.all? { |value| value.is_a?(String) || value.is_a?(Numeric) || value == true || value == false }
      [key.to_s, raw]
    end

    combinations = values.reduce([{}]) do |acc, (key, axis_values)|
      acc.flat_map { |combo| axis_values.map { |value| combo.merge(key => value.to_s) } }
    end

    combinations.map do |combo|
      context = name.gsub(MATRIX_EXPR) { combo.fetch(Regexp.last_match(1)) }
      raise "required job #{job_id} context name contains unresolved expression: #{context}" if context.include?('${{')
      context
    end
  end

  def live_required_contexts(ruleset)
    rule = ruleset.fetch('rules', []).find { |candidate| candidate['type'] == 'required_status_checks' }
    raise 'ruleset has no required_status_checks rule' unless rule
    rule.dig('parameters', 'required_status_checks').to_a.map { |entry| entry.fetch('context') }
  end

  def verify(root:, policy_path:, ruleset_path:)
    policy = JSON.parse(File.read(policy_path))
    ruleset = JSON.parse(File.read(ruleset_path))
    violations = []

    violations << "unsupported policy schema_version #{policy['schema_version'].inspect}" unless policy['schema_version'] == 1
    violations << "ruleset id mismatch: expected #{policy.dig('ruleset', 'id')}, got #{ruleset['id']}" unless ruleset['id'] == policy.dig('ruleset', 'id')
    violations << "ruleset name mismatch: expected #{policy.dig('ruleset', 'name').inspect}, got #{ruleset['name'].inspect}" unless ruleset['name'] == policy.dig('ruleset', 'name')
    violations << 'ruleset is not active' unless ruleset['enforcement'] == 'active'

    workflow_dir = Pathname(root).join('.github/workflows')
    workflow_paths = Dir[workflow_dir.join('*.{yml,yaml}').to_s].sort
    required_contexts = policy.fetch('managed_required_contexts', []).dup
    classified_jobs = []

    workflow_paths.each do |absolute|
      doc = load_yaml(absolute)
      next unless pr_trigger?(doc)

      workflow = Pathname(absolute).relative_path_from(Pathname(root)).to_s
      default = default_classification(policy, workflow)
      jobs = doc['jobs']
      unless jobs.is_a?(Hash)
        violations << "#{workflow} is PR-triggered but has no jobs object"
        next
      end

      jobs.each do |job_id, job|
        classification, = job_policy(policy, workflow, job_id.to_s)
        unless VALID_CLASSIFICATIONS.include?(classification)
          violations << "#{workflow} job #{job_id} is unclassified"
          next
        end
        classified_jobs << [workflow, job_id.to_s, classification]
        next unless classification == 'required'

        pr_event_configs(doc).each do |event_name, config|
          next unless config.is_a?(Hash)
          if config.key?('paths') || config.key?('paths-ignore')
            violations << "required workflow #{workflow} can suppress #{event_name} via path filters"
          end
        end
        if job.is_a?(Hash) && job.key?('if')
          violations << "required job #{workflow}##{job_id} has an if condition and is not guaranteed to emit"
        end

        begin
          required_contexts.concat(required_job_contexts(job_id.to_s, job || {}))
        rescue StandardError => e
          violations << "#{workflow}##{job_id}: #{e.message}"
        end
      end

      if default.nil? && jobs.keys.none? { |job_id| policy.fetch('job_overrides', []).any? { |entry| entry['workflow'] == workflow && entry['job'] == job_id.to_s } }
        violations << "PR workflow #{workflow} has no policy classification"
      end
    end

    duplicates = required_contexts.group_by(&:itself).select { |_context, entries| entries.length > 1 }.keys
    violations << "required contexts are duplicated in policy/producer expansion: #{duplicates.sort.join(', ')}" unless duplicates.empty?

    pending = policy.fetch('pending_ruleset_contexts', [])
    unknown_pending = pending - required_contexts
    violations << "pending ruleset contexts are not required producers: #{unknown_pending.sort.join(', ')}" unless unknown_pending.empty?

    expected_live = (required_contexts - pending).sort
    actual_live = live_required_contexts(ruleset).sort
    missing = expected_live - actual_live
    stale = actual_live - expected_live
    violations << "policy-required contexts missing from live ruleset: #{missing.join(', ')}" unless missing.empty?
    violations << "live ruleset requires stale/unaccepted contexts: #{stale.join(', ')}" unless stale.empty?

    [violations, {
      'required_contexts' => required_contexts.sort,
      'pending_ruleset_contexts' => pending.sort,
      'live_required_contexts' => actual_live,
      'classified_jobs' => classified_jobs.sort
    }]
  end
end

if __FILE__ == $PROGRAM_NAME
  policy_path = ARGV[0] || '.github/merge-gate-policy.json'
  ruleset_path = ARGV[1] || ENV['OXIDEBATCH_RULESET_JSON']
  abort 'usage: verify-merge-gates.rb [policy.json] <ruleset.json>' unless ruleset_path

  violations, summary = MergeGateVerifier.verify(root: Dir.pwd, policy_path: policy_path, ruleset_path: ruleset_path)
  puts JSON.pretty_generate(summary)
  if violations.empty?
    puts 'merge gate policy matches PR producers and live ruleset'
    exit 0
  end

  violations.each { |violation| warn "merge gate violation: #{violation}" }
  exit 1
end
