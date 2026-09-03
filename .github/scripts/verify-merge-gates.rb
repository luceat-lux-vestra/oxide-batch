#!/usr/bin/env ruby
# frozen_string_literal: true

require 'json'
require 'yaml'
require 'pathname'

module MergeGateVerifier
  module_function

  VALID_CLASSIFICATIONS = %w[required advisory optional].freeze
  AGGREGATE_STATES = %w[candidate active].freeze
  MATRIX_EXPR = /\$\{\{\s*matrix\.([A-Za-z0-9_-]+)\s*\}\}/

  def load_yaml(path)
    YAML.safe_load(File.read(path), aliases: true) || {}
  rescue Psych::SyntaxError => e
    raise "cannot parse #{path}: #{e.message}"
  end

  def event_map(doc)
    doc['on'] || doc[true] || {}
  end

  def event_config(doc, name)
    events = event_map(doc)
    return nil unless events.is_a?(Hash)
    events[name]
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
    if matrix.key?('include') || matrix.key?('exclude')
      raise "required job #{job_id} uses matrix include/exclude; model it explicitly before requiring it"
    end
    values = axes.map do |key, raw|
      valid = raw.is_a?(Array) && raw.all? { |value| value.is_a?(String) || value.is_a?(Numeric) || value == true || value == false }
      raise "required job #{job_id} matrix axis #{key} is not a literal array" unless valid
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

  def producer_inventory(root:, policy:)
    violations = []
    contexts = policy.fetch('managed_required_contexts', []).dup
    sources = contexts.to_h { |context| [context, {'kind' => 'managed'}] }
    classified_jobs = []
    seen_jobs = []

    workflow_dir = Pathname(root).join('.github/workflows')
    workflow_paths = Dir[workflow_dir.join('*.{yml,yaml}').to_s].sort
    workflow_docs = {}

    workflow_paths.each do |absolute|
      doc = load_yaml(absolute)
      workflow = Pathname(absolute).relative_path_from(Pathname(root)).to_s
      workflow_docs[workflow] = doc
      next unless pr_trigger?(doc)

      default = default_classification(policy, workflow)
      jobs = doc['jobs']
      unless jobs.is_a?(Hash)
        violations << "#{workflow} is PR-triggered but has no jobs object"
        next
      end

      jobs.each do |job_id, job|
        job_id = job_id.to_s
        seen_jobs << [workflow, job_id]
        classification, = job_policy(policy, workflow, job_id)
        unless VALID_CLASSIFICATIONS.include?(classification)
          violations << "#{workflow} job #{job_id} is unclassified"
          next
        end
        classified_jobs << [workflow, job_id, classification]
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
          required_job_contexts(job_id, job || {}).each do |context|
            contexts << context
            sources[context] = {'kind' => 'job', 'workflow' => workflow, 'job' => job_id}
          end
        rescue StandardError => e
          violations << "#{workflow}##{job_id}: #{e.message}"
        end
      end

      if default.nil? && jobs.keys.none? { |job_id| policy.fetch('job_overrides', []).any? { |entry| entry['workflow'] == workflow && entry['job'] == job_id.to_s } }
        violations << "PR workflow #{workflow} has no policy classification"
      end
    end

    policy.fetch('job_overrides', []).each do |entry|
      key = [entry.fetch('workflow'), entry.fetch('job')]
      violations << "job override references missing PR job #{key.join('#')}" unless seen_jobs.include?(key)
    end

    duplicates = contexts.group_by(&:itself).select { |_context, entries| entries.length > 1 }.keys
    violations << "required contexts are duplicated in policy/producer expansion: #{duplicates.sort.join(', ')}" unless duplicates.empty?

    [violations, {
      'required_contexts' => contexts.sort,
      'context_sources' => sources,
      'classified_jobs' => classified_jobs.sort,
      'workflow_docs' => workflow_docs
    }]
  end

  def aggregate_inventory(root:, policy:, producer_summary:)
    violations = []
    required_contexts = producer_summary.fetch('required_contexts')
    workflow_docs = producer_summary.fetch('workflow_docs')
    aggregates = []
    member_owners = {}

    policy.fetch('aggregate_gates', []).each do |gate|
      context = gate['context']
      state = gate['state']
      members = gate['members']
      producer = gate['producer']

      violations << 'aggregate context must be a non-empty string' unless context.is_a?(String) && !context.empty?
      violations << "aggregate #{context.inspect} has unsupported state #{state.inspect}" unless AGGREGATE_STATES.include?(state)
      unless members.is_a?(Array) && !members.empty? && members.all? { |member| member.is_a?(String) && !member.empty? }
        violations << "aggregate #{context.inspect} must declare a non-empty string member inventory"
        members = []
      end
      duplicate_members = members.group_by(&:itself).select { |_member, entries| entries.length > 1 }.keys
      violations << "aggregate #{context} duplicates members: #{duplicate_members.sort.join(', ')}" unless duplicate_members.empty?

      unknown = members - required_contexts
      violations << "aggregate #{context} members are not required producers: #{unknown.sort.join(', ')}" unless unknown.empty?
      members.each do |member|
        if member_owners.key?(member)
          violations << "aggregate member #{member} belongs to both #{member_owners[member]} and #{context}"
        else
          member_owners[member] = context
        end
      end

      unless producer.is_a?(Hash)
        violations << "aggregate #{context} has no producer"
        producer = {}
      end
      workflow = producer['workflow']
      job_id = producer['job']
      doc = workflow_docs[workflow]
      if doc.nil?
        violations << "aggregate #{context} producer workflow #{workflow.inspect} does not exist"
      else
        jobs = doc['jobs']
        violations << "aggregate #{context} producer job #{workflow}##{job_id} does not exist" unless jobs.is_a?(Hash) && jobs.key?(job_id)
        events = event_map(doc)
        unless events.is_a?(Hash) && events.key?('pull_request_target') && events.key?('workflow_run')
          violations << "aggregate #{context} producer must be triggered by pull_request_target and workflow_run"
        end
        target = event_config(doc, 'pull_request_target')
        if target.is_a?(Hash) && (target.key?('paths') || target.key?('paths-ignore'))
          violations << "aggregate #{context} producer can suppress pull_request_target via path filters"
        end
        permissions = doc['permissions']
        unless permissions.is_a?(Hash) && permissions['contents'] == 'read' && permissions['statuses'] == 'write'
          violations << "aggregate #{context} producer must use contents: read and statuses: write permissions"
        end
      end

      aggregates << {'context' => context, 'state' => state, 'members' => members, 'producer' => producer}
    end

    aggregate_contexts = aggregates.map { |gate| gate['context'] }
    duplicate_contexts = aggregate_contexts.group_by(&:itself).select { |_context, entries| entries.length > 1 }.keys
    violations << "aggregate contexts are duplicated: #{duplicate_contexts.sort.join(', ')}" unless duplicate_contexts.empty?
    collisions = aggregate_contexts & required_contexts
    violations << "aggregate contexts collide with required child/managed producers: #{collisions.sort.join(', ')}" unless collisions.empty?

    [violations, aggregates]
  end

  def verify(root:, policy_path:, ruleset_path:)
    policy = JSON.parse(File.read(policy_path))
    ruleset = JSON.parse(File.read(ruleset_path))
    violations = []

    violations << "unsupported policy schema_version #{policy['schema_version'].inspect}" unless policy['schema_version'] == 2
    violations << "ruleset id mismatch: expected #{policy.dig('ruleset', 'id')}, got #{ruleset['id']}" unless ruleset['id'] == policy.dig('ruleset', 'id')
    violations << "ruleset name mismatch: expected #{policy.dig('ruleset', 'name').inspect}, got #{ruleset['name'].inspect}" unless ruleset['name'] == policy.dig('ruleset', 'name')
    violations << 'ruleset is not active' unless ruleset['enforcement'] == 'active'

    producer_violations, producer_summary = producer_inventory(root: root, policy: policy)
    violations.concat(producer_violations)
    aggregate_violations, aggregates = aggregate_inventory(root: root, policy: policy, producer_summary: producer_summary)
    violations.concat(aggregate_violations)

    required_contexts = producer_summary.fetch('required_contexts') + aggregates.map { |gate| gate['context'] }
    pending = policy.fetch('pending_ruleset_contexts', [])
    unknown_pending = pending - required_contexts
    violations << "pending ruleset contexts are not required producers: #{unknown_pending.sort.join(', ')}" unless unknown_pending.empty?

    aggregates.each do |gate|
      context = gate['context']
      case gate['state']
      when 'candidate'
        violations << "candidate aggregate #{context} must be pending ruleset promotion" unless pending.include?(context)
      when 'active'
        violations << "active aggregate #{context} cannot remain pending ruleset promotion" if pending.include?(context)
      end
    end

    expected_live = required_contexts.dup
    aggregates.select { |gate| gate['state'] == 'active' }.each do |gate|
      expected_live -= gate['members']
    end
    expected_live = (expected_live - pending).sort
    actual_live = live_required_contexts(ruleset).sort
    missing = expected_live - actual_live
    stale = actual_live - expected_live
    violations << "policy-required contexts missing from live ruleset: #{missing.join(', ')}" unless missing.empty?
    violations << "live ruleset requires stale/unaccepted contexts: #{stale.join(', ')}" unless stale.empty?

    [violations, {
      'required_contexts' => required_contexts.sort,
      'aggregate_gates' => aggregates,
      'pending_ruleset_contexts' => pending.sort,
      'live_required_contexts' => actual_live,
      'classified_jobs' => producer_summary.fetch('classified_jobs')
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
