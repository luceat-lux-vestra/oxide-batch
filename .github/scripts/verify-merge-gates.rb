#!/usr/bin/env ruby
# frozen_string_literal: true

require 'json'
require 'yaml'
require 'pathname'

module MergeGateVerifier
  module_function

  VALID_CLASSIFICATIONS = %w[required advisory optional].freeze
  AGGREGATE_STATES = %w[candidate cutover active].freeze
  MATRIX_EXPR = /\$\{\{\s*matrix\.([A-Za-z0-9_-]+)\s*\}\}/

  def event_config(doc, name)
    events = event_map(doc)
    if events.is_a?(Hash)
      return [events.key?(name), events[name]]
    end
    if events.is_a?(Array)
      return [events.map(&:to_s).include?(name), nil]
    end
    [events.to_s == name, nil]
  end

  def pattern_matches_branch?(pattern, branch)
    value = pattern.to_s
    return false if value.empty?
    File.fnmatch?(value, branch, File::FNM_PATHNAME | File::FNM_EXTGLOB)
  end

  def ordered_branch_filters_include?(patterns, branch)
    included = false
    Array(patterns).each do |pattern|
      value = pattern.to_s
      if value.start_with?('!')
        included = false if pattern_matches_branch?(value.delete_prefix('!'), branch)
      elsif pattern_matches_branch?(value, branch)
        included = true
      end
    end
    included
  end

  def push_targets_branch?(doc, branch)
    present, config = event_config(doc, 'push')
    return false unless present
    return true if config.nil? || config == true
    return true unless config.is_a?(Hash)

    if config.key?('branches')
      return ordered_branch_filters_include?(config['branches'], branch)
    end
    if config.key?('branches-ignore')
      ignored = Array(config['branches-ignore']).any? { |pattern| pattern_matches_branch?(pattern, branch) }
      return !ignored
    end

    # A push trigger constrained only by tag filters does not run for branch pushes.
    return false if config.key?('tags') || config.key?('tags-ignore')

    true
  end

  def post_main_contract(policy:, producer_summary:)
    config = policy['post_main']
    return ['schema v5 policy must declare post_main'] unless config.is_a?(Hash)

    branch = config['default_branch']
    allowed = config['allowed_push_workflows']
    violations = []
    unless branch.is_a?(String) && !branch.empty?
      violations << 'post_main.default_branch must be a non-empty string'
      return violations
    end
    unless allowed.is_a?(Array) && allowed.all? { |entry| entry.is_a?(String) && !entry.empty? }
      violations << 'post_main.allowed_push_workflows must be a string array'
      return violations
    end
    if allowed.uniq.length != allowed.length
      violations << 'post_main.allowed_push_workflows contains duplicates'
    end

    workflow_docs = producer_summary.fetch('workflow_docs')
    unknown = allowed - workflow_docs.keys
    violations << "post_main allowlist references missing workflows: #{unknown.sort.join(', ')}" unless unknown.empty?

    workflow_docs.each do |workflow, doc|
      next if allowed.include?(workflow)
      if push_targets_branch?(doc, branch)
        violations << "#{workflow} targets push to #{branch}; post-main validation is forbidden unless explicitly allowlisted"
      end
    end
    violations
  end

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

  def pull_request_target_trigger?(doc)
    events = event_map(doc)
    return events.any? { |name| name.to_s == 'pull_request_target' } if events.is_a?(Array)
    return events.to_s == 'pull_request_target' unless events.is_a?(Hash)

    events.key?('pull_request_target')
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
      valid = raw.is_a?(Array) && raw.all? do |value|
        value.is_a?(String) || value.is_a?(Numeric) || value == true || value == false
      end
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
    static_job_contexts = []

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
      required_workflow = false
      unless jobs.is_a?(Hash)
        violations << "#{workflow} is PR-triggered but has no jobs object"
        next
      end

      jobs.each do |job_id, job|
        job_id = job_id.to_s
        seen_jobs << [workflow, job_id]
        job_name = (job.is_a?(Hash) && job['name']) || job_id
        unless job_name.include?('${{')
          static_job_contexts << {'workflow' => workflow, 'job' => job_id, 'context' => job_name}
        end
        classification, = job_policy(policy, workflow, job_id)
        unless VALID_CLASSIFICATIONS.include?(classification)
          violations << "#{workflow} job #{job_id} is unclassified"
          next
        end
        classified_jobs << [workflow, job_id, classification]
        next unless classification == 'required'

        required_workflow = true
        pr_event_configs(doc).each do |event_name, config|
          next unless config.is_a?(Hash)
          if config.key?('paths') || config.key?('paths-ignore')
            violations << "required workflow #{workflow} can suppress #{event_name} via path filters"
          end
        end
        if job.is_a?(Hash) && job.key?('if') && !always_condition?(job['if'])
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

      if required_workflow && pull_request_target_trigger?(doc)
        violations << "required workflow #{workflow} must not use pull_request_target"
      end

      if default.nil? && jobs.keys.none? do |job_id|
        policy.fetch('job_overrides', []).any? do |entry|
          entry['workflow'] == workflow && entry['job'] == job_id.to_s
        end
      end
        violations << "PR workflow #{workflow} has no policy classification"
      end
    end

    policy.fetch('job_overrides', []).each do |entry|
      key = [entry.fetch('workflow'), entry.fetch('job')]
      violations << "job override references missing PR job #{key.join('#')}" unless seen_jobs.include?(key)
    end

    duplicates = contexts.group_by(&:itself).select { |_context, entries| entries.length > 1 }.keys
    unless duplicates.empty?
      violations << "required contexts are duplicated in policy/producer expansion: #{duplicates.sort.join(', ')}"
    end

    [violations, {
      'required_contexts' => contexts.sort,
      'context_sources' => sources,
      'classified_jobs' => classified_jobs.sort,
      'static_job_contexts' => static_job_contexts,
      'workflow_docs' => workflow_docs
    }]
  end

  def normalize_needs(job)
    raw = job['needs']
    case raw
    when String then [raw]
    when Array then raw.map(&:to_s)
    when nil then []
    else
      raise 'needs must be a string or array of job ids'
    end
  end

  def always_condition?(value)
    value.to_s.gsub(/\s+/, '') == '${{always()}}'
  end

  PR_SCOPE_SCRIPT = '.github/scripts/pr-scope.py'
  PR_SCOPE_WORKFLOW = '.github/workflows/ci.yml'
  PR_SCOPE_JOB = 'quality'
  PR_SCOPE_SELF_TEST = 'python3 .github/scripts/pr-scope.py --repo-root . --policy .github/merge-gate-policy.json --self-test'
  PR_SCOPE_TRUST_CONTRACT = 'exact-git-base-sha'
  PR_SCOPE_SEMANTICS_GLOB = 'tests/fixtures/**/campaign-semantics.json'
  PR_SCOPE_RETAINED_POLICY = 'docs/engineering/retained-evidence-policy.json'
  CAMPAIGN_ORCHESTRATOR_WORKFLOW = '.github/workflows/campaign-orchestrator.yml'
  M5_CONFORMANCE_WORKFLOW = '.github/workflows/m5-conformance.yml'
  CAMPAIGN_ROUTE_JOB = 'route'
  CAMPAIGN_ROUTE_CLASSIFY_STEP = 'classify'
  CAMPAIGN_ROUTE_CHECKOUT_STEP = 'trusted-base'
  M5_CONFORMANCE_DEEP_JOB = 'conformance-deep'
  M5_CONFORMANCE_CONTEXT_JOB = 'conformance-campaign'
  QUALITY_WORKFLOW = '.github/workflows/ci.yml'
  QUALITY_AGGREGATE_JOB = 'quality'
  QUALITY_COMPONENT_JOBS = %w[quality-fast quality-integration quality-bin-doc quality-contracts].freeze
  PR_SCOPE_GLOBAL_DIRECT_PROOF_PATHS = [
    PR_SCOPE_SCRIPT,
    '.github/merge-gate-policy.json',
    CAMPAIGN_ORCHESTRATOR_WORKFLOW,
    PR_SCOPE_RETAINED_POLICY
  ].freeze

  def normalized_shell(command)
    command.to_s.split.join(' ')
  end

  def pr_scope_contract(root:, policy:, producer_summary:)
    violations = []
    scope = policy['pr_scope']
    unless scope.is_a?(Hash)
      return ['schema v5 policy must declare pr_scope']
    end

    unless scope['trusted_tree_contract'] == PR_SCOPE_TRUST_CONTRACT
      violations << "pr_scope trusted_tree_contract must be #{PR_SCOPE_TRUST_CONTRACT.inspect}"
    end
    unless scope['campaign_semantics_glob'] == PR_SCOPE_SEMANTICS_GLOB
      violations << "pr_scope campaign_semantics_glob must be #{PR_SCOPE_SEMANTICS_GLOB.inspect}"
    end
    unless scope['retained_evidence_policy'] == PR_SCOPE_RETAINED_POLICY
      violations << "pr_scope retained_evidence_policy must be #{PR_SCOPE_RETAINED_POLICY.inspect}"
    end

    global_direct = scope['global_direct_proof_paths']
    unless global_direct.is_a?(Array) &&
           global_direct.all? { |entry| entry.is_a?(String) && !entry.empty? } &&
           global_direct.uniq.length == global_direct.length &&
           global_direct.sort == PR_SCOPE_GLOBAL_DIRECT_PROOF_PATHS.sort
      violations << 'pr_scope global_direct_proof_paths must exactly match canonical routing control-plane paths'
    end
    global_campaign = scope['global_campaign_paths']
    if global_campaign.is_a?(Array) && global_direct.is_a?(Array)
      overlap = global_campaign & global_direct
      unless overlap.empty?
        violations << "pr_scope global_direct_proof_paths must not overlap global_campaign_paths: #{overlap.sort.join(', ')}"
      end
    end

    script = Pathname(root).join(PR_SCOPE_SCRIPT)
    violations << "trusted PR scope classifier #{PR_SCOPE_SCRIPT} is missing" unless script.file?

    doc = producer_summary.fetch('workflow_docs')[PR_SCOPE_WORKFLOW]
    job = doc.is_a?(Hash) && doc['jobs'].is_a?(Hash) ? doc['jobs'][PR_SCOPE_JOB] : nil
    unless job.is_a?(Hash)
      violations << "trusted PR scope self-test owner #{PR_SCOPE_WORKFLOW}##{PR_SCOPE_JOB} is missing"
      return violations
    end

    steps = job['steps']
    matches = steps.is_a?(Array) ? steps.select do |step|
      step.is_a?(Hash) && normalized_shell(step['run']) == PR_SCOPE_SELF_TEST
    end : []
    if matches.length != 1
      violations << "trusted PR scope classifier must have exactly one canonical self-test in #{PR_SCOPE_WORKFLOW}##{PR_SCOPE_JOB}"
    elsif matches.first['continue-on-error']
      violations << 'trusted PR scope classifier self-test cannot continue on error'
    end

    violations
  end

  def workflow_event?(doc, name)
    present, = event_config(doc, name)
    present
  end

  def trusted_campaign_route_contract(workflow:, job:, expected_if:)
    violations = []
    unless job.is_a?(Hash)
      return ["#{workflow} must declare #{CAMPAIGN_ROUTE_JOB} as the trusted routing job"]
    end

    unless normalized_shell(job['if']) == normalized_shell(expected_if)
      violations << "#{workflow} trusted route must run only under the canonical pull-request condition"
    end
    violations << "#{workflow} trusted route must run on ubuntu-24.04" unless job['runs-on'] == 'ubuntu-24.04'
    violations << "#{workflow} trusted route timeout must remain 5 minutes" unless job['timeout-minutes'] == 5

    expected_outputs = {
      'classification_outcome' => '${{ steps.classify.outcome }}',
      'direct_workflows' => '${{ steps.classify.outputs.direct_workflows }}'
    }
    unless job['outputs'] == expected_outputs
      violations << "#{workflow} trusted route must expose classifier outcome and direct workflow output"
    end

    steps = job['steps']
    unless steps.is_a?(Array)
      return violations + ["#{workflow} trusted route must declare steps"]
    end

    checkout = steps.find { |step| step.is_a?(Hash) && step['id'] == CAMPAIGN_ROUTE_CHECKOUT_STEP }
    unless checkout.is_a?(Hash)
      violations << "#{workflow} trusted route is missing exact-base checkout"
    else
      checkout_uses = checkout['uses'].to_s
      unless checkout_uses.match?(/\Aactions\/checkout@[0-9a-f]{40}\z/)
        violations << "#{workflow} trusted route checkout must use a SHA-pinned actions/checkout"
      end
      violations << "#{workflow} trusted route checkout must continue on error for fail-closed fallback" unless checkout['continue-on-error'] == true
      expected_with = {
        'ref' => '${{ github.event.pull_request.base.sha }}',
        'path' => '.trusted-base',
        'fetch-depth' => 1,
        'persist-credentials' => false
      }
      unless checkout['with'] == expected_with
        violations << "#{workflow} trusted route checkout must target only the exact PR base SHA in .trusted-base"
      end
    end

    classify = steps.find { |step| step.is_a?(Hash) && step['id'] == CAMPAIGN_ROUTE_CLASSIFY_STEP }
    unless classify.is_a?(Hash)
      violations << "#{workflow} trusted route is missing classifier step"
      return violations
    end
    violations << "#{workflow} classifier step must continue on error so consumers can fall back to full execution" unless classify['continue-on-error'] == true
    expected_env = {
      'GH_TOKEN' => '${{ github.token }}',
      'BASE_SHA' => '${{ github.event.pull_request.base.sha }}',
      'HEAD_SHA' => '${{ github.event.pull_request.head.sha }}',
      'PR_NUMBER' => '${{ github.event.pull_request.number }}',
      'TRUSTED_CHECKOUT' => '${{ steps.trusted-base.outcome }}'
    }
    unless classify['env'] == expected_env
      violations << "#{workflow} classifier step must bind canonical PR/base/head/token inputs"
    end

    command = classify['run'].to_s
    required_tokens = [
      "echo 'direct_workflows=[]' >> \"$GITHUB_OUTPUT\"",
      'set -euo pipefail',
      'test "$TRUSTED_CHECKOUT" = "success"',
      'repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}',
      '.base.sha == $base',
      '.head.sha == $head',
      '.base.repo.full_name == $repo',
      '.changed_files > 0',
      '--paginate --slurp',
      'pulls/${PR_NUMBER}/files?per_page=100',
      '@tsv',
      '.trusted-base/.github/scripts/pr-scope.py',
      '--repo-root .trusted-base',
      '--policy .github/merge-gate-policy.json',
      '--expected-count "$expected_count"',
      '--trusted-base-sha "$BASE_SHA"',
      '.direct_proof_campaign_workflows',
      'direct_workflows=$direct_workflows'
    ]
    missing = required_tokens.reject { |token| command.include?(token) }
    unless missing.empty?
      violations << "#{workflow} classifier step is missing trusted routing tokens: #{missing.join(', ')}"
    end

    violations
  end

  def campaign_orchestrator_contract(root:, policy:, producer_summary:)
    violations = []
    workflow_docs = producer_summary.fetch('workflow_docs')
    orchestrator = workflow_docs[CAMPAIGN_ORCHESTRATOR_WORKFLOW]
    unless orchestrator.is_a?(Hash)
      return ["campaign orchestrator #{CAMPAIGN_ORCHESTRATOR_WORKFLOW} is missing"]
    end

    unless pr_trigger?(orchestrator)
      violations << "#{CAMPAIGN_ORCHESTRATOR_WORKFLOW} must be pull_request-triggered"
    end
    if pull_request_target_trigger?(orchestrator)
      violations << "#{CAMPAIGN_ORCHESTRATOR_WORKFLOW} must not use pull_request_target"
    end
    expected_permissions = {'contents' => 'read', 'pull-requests' => 'read'}
    unless orchestrator['permissions'] == expected_permissions
      violations << "#{CAMPAIGN_ORCHESTRATOR_WORKFLOW} must keep only contents/pull-requests read permissions"
    end

    retained_path = policy.dig('pr_scope', 'retained_evidence_policy')
    retained = retained_path && JSON.parse(File.read(Pathname(root).join(retained_path)))
    producers = retained ? retained.fetch('artifact_producers', []).map { |entry| entry.fetch('workflow') }.uniq : []

    expected = producers.select do |workflow|
      doc = workflow_docs[workflow]
      jobs = doc.is_a?(Hash) ? doc['jobs'] : nil
      next false unless jobs.is_a?(Hash)
      jobs.keys.all? { |job_id| job_policy(policy, workflow, job_id.to_s).first == 'advisory' }
    end.sort

    jobs = orchestrator['jobs']
    unless jobs.is_a?(Hash)
      return violations + ["#{CAMPAIGN_ORCHESTRATOR_WORKFLOW} must declare jobs"]
    end

    route = jobs[CAMPAIGN_ROUTE_JOB]
    violations.concat(trusted_campaign_route_contract(
      workflow: CAMPAIGN_ORCHESTRATOR_WORKFLOW,
      job: route,
      expected_if: '${{ github.event.pull_request.draft == false }}'
    ))

    calls = []
    jobs.each do |job_id, job|
      next if job_id.to_s == CAMPAIGN_ROUTE_JOB
      unless job.is_a?(Hash) && job['uses'].is_a?(String)
        violations << "#{CAMPAIGN_ORCHESTRATOR_WORKFLOW} job #{job_id} must be a reusable-workflow call"
        next
      end
      uses = job['uses']
      unless uses.start_with?('./.github/workflows/')
        violations << "#{CAMPAIGN_ORCHESTRATOR_WORKFLOW} job #{job_id} must call a local checked-in workflow"
        next
      end
      workflow = uses.delete_prefix('./')
      calls << workflow

      unless normalize_needs(job) == [CAMPAIGN_ROUTE_JOB]
        violations << "#{CAMPAIGN_ORCHESTRATOR_WORKFLOW} job #{job_id} must depend only on the trusted route"
      end
      expected_if = "${{ always() && github.event.pull_request.draft == false && (needs.route.result != 'success' || needs.route.outputs.classification_outcome != 'success' || contains(needs.route.outputs.direct_workflows, '#{workflow}')) }}"
      unless normalized_shell(job['if']) == normalized_shell(expected_if)
        violations << "#{CAMPAIGN_ORCHESTRATOR_WORKFLOW} job #{job_id} must route direct proof and fail closed to execution"
      end
    end

    duplicates = calls.group_by(&:itself).select { |_workflow, entries| entries.length > 1 }.keys
    violations << "campaign orchestrator duplicates producers: #{duplicates.sort.join(', ')}" unless duplicates.empty?

    missing = expected - calls
    extra = calls - expected
    violations << "campaign orchestrator misses advisory producers: #{missing.join(', ')}" unless missing.empty?
    violations << "campaign orchestrator calls non-advisory/unknown producers: #{extra.join(', ')}" unless extra.empty?

    expected.each do |workflow|
      doc = workflow_docs[workflow]
      unless workflow_event?(doc, 'workflow_call')
        violations << "advisory campaign producer #{workflow} must expose workflow_call"
      end
      unless workflow_event?(doc, 'workflow_dispatch')
        violations << "advisory campaign producer #{workflow} must retain workflow_dispatch"
      end
      if pr_trigger?(doc)
        violations << "advisory campaign producer #{workflow} must not trigger directly on pull_request"
      end
    end

    violations
  rescue Errno::ENOENT, JSON::ParserError, KeyError => e
    ["campaign orchestrator contract could not be evaluated: #{e.message}"]
  end

  def m5_conformance_routing_contract(policy:, producer_summary:)
    violations = []
    workflow = producer_summary.fetch('workflow_docs')[M5_CONFORMANCE_WORKFLOW]
    unless workflow.is_a?(Hash)
      return ["#{M5_CONFORMANCE_WORKFLOW} is missing"]
    end
    unless pr_trigger?(workflow) && workflow_event?(workflow, 'workflow_dispatch')
      violations << "#{M5_CONFORMANCE_WORKFLOW} must retain pull_request and workflow_dispatch triggers"
    end
    expected_permissions = {'contents' => 'read', 'pull-requests' => 'read'}
    unless workflow['permissions'] == expected_permissions
      violations << "#{M5_CONFORMANCE_WORKFLOW} must keep only contents/pull-requests read permissions"
    end

    jobs = workflow['jobs']
    unless jobs.is_a?(Hash)
      return violations + ["#{M5_CONFORMANCE_WORKFLOW} must declare jobs"]
    end

    route = jobs[CAMPAIGN_ROUTE_JOB]
    violations.concat(trusted_campaign_route_contract(
      workflow: M5_CONFORMANCE_WORKFLOW,
      job: route,
      expected_if: "${{ github.event_name == 'pull_request' && github.event.pull_request.draft == false }}"
    ))

    deep = jobs[M5_CONFORMANCE_DEEP_JOB]
    unless deep.is_a?(Hash)
      violations << "#{M5_CONFORMANCE_WORKFLOW} must declare #{M5_CONFORMANCE_DEEP_JOB}"
    else
      violations << "#{M5_CONFORMANCE_WORKFLOW} deep job must be advisory" unless job_policy(policy, M5_CONFORMANCE_WORKFLOW, M5_CONFORMANCE_DEEP_JOB).first == 'advisory'
      violations << "#{M5_CONFORMANCE_WORKFLOW} deep job must depend only on the trusted route" unless normalize_needs(deep) == [CAMPAIGN_ROUTE_JOB]
      expected_if = "${{ always() && (github.event_name == 'workflow_dispatch' || (github.event_name == 'pull_request' && github.event.pull_request.draft == false && (needs.route.result != 'success' || needs.route.outputs.classification_outcome != 'success' || contains(needs.route.outputs.direct_workflows, '.github/workflows/m5-conformance.yml')))) }}"
      unless normalized_shell(deep['if']) == normalized_shell(expected_if)
        violations << "#{M5_CONFORMANCE_WORKFLOW} deep job must run for manual/direct proof and fail closed on routing ambiguity"
      end
      unless deep['name'] == 'deep-postgres-${{ matrix.postgres }}-conformance-campaign'
        violations << "#{M5_CONFORMANCE_WORKFLOW} deep job must not reuse required context names"
      end
      unless deep.dig('strategy', 'matrix', 'postgres') == ['15', '18']
        violations << "#{M5_CONFORMANCE_WORKFLOW} deep job must retain PostgreSQL 15/18 matrix"
      end
      unless deep.dig('services', 'postgres').is_a?(Hash)
        violations << "#{M5_CONFORMANCE_WORKFLOW} deep job must own PostgreSQL service provisioning"
      end
    end

    emitter = jobs[M5_CONFORMANCE_CONTEXT_JOB]
    unless emitter.is_a?(Hash)
      violations << "#{M5_CONFORMANCE_WORKFLOW} must declare #{M5_CONFORMANCE_CONTEXT_JOB}"
    else
      violations << "#{M5_CONFORMANCE_WORKFLOW} context emitter must remain required" unless job_policy(policy, M5_CONFORMANCE_WORKFLOW, M5_CONFORMANCE_CONTEXT_JOB).first == 'required'
      unless normalize_needs(emitter).sort == [CAMPAIGN_ROUTE_JOB, M5_CONFORMANCE_DEEP_JOB].sort
        violations << "#{M5_CONFORMANCE_WORKFLOW} context emitter must depend on routing and deep proof"
      end
      violations << "#{M5_CONFORMANCE_WORKFLOW} context emitter must use always()" unless always_condition?(emitter['if'])
      unless emitter['name'] == 'postgres-${{ matrix.postgres }}-conformance-campaign'
        violations << "#{M5_CONFORMANCE_WORKFLOW} context emitter must preserve required PostgreSQL context names"
      end
      unless emitter.dig('strategy', 'matrix', 'postgres') == ['15', '18']
        violations << "#{M5_CONFORMANCE_WORKFLOW} context emitter must retain PostgreSQL 15/18 matrix"
      end
      if emitter.key?('services')
        violations << "#{M5_CONFORMANCE_WORKFLOW} required context emitter must not declare services"
      end
      emitter_run = Array(emitter['steps']).filter_map { |step| step.is_a?(Hash) ? step['run'] : nil }.join("\n")
      required_tokens = [
        'M5 conformance is deferred until the pull request is ready for review',
        'ROUTE_RESULT',
        'CLASSIFICATION_OUTCOME',
        'DIRECT_REQUIRED',
        'DEEP_RESULT',
        '!= "success"',
        '!= "skipped"'
      ]
      missing = required_tokens.reject { |token| emitter_run.include?(token) }
      unless missing.empty?
        violations << "#{M5_CONFORMANCE_WORKFLOW} context emitter is missing fail-closed proof checks: #{missing.join(', ')}"
      end
    end

    violations
  end

  def quality_parallel_contract(policy:, producer_summary:)
    violations = []
    workflow = producer_summary.fetch('workflow_docs')[QUALITY_WORKFLOW]
    unless workflow.is_a?(Hash) && workflow['jobs'].is_a?(Hash)
      return ["#{QUALITY_WORKFLOW} must declare parallel quality jobs"]
    end
    jobs = workflow['jobs']

    component_specs = {
      'quality-fast' => {
        'name' => 'quality-fast-internal',
        'tokens' => [
          'actions/workflows/fast-branch.yml/runs?event=push&head_sha=$EXPECTED_SHA&per_page=100',
          'git diff --check',
          'cargo fmt --all -- --check',
          'cargo clippy --workspace --all-targets --all-features --',
          'cargo clippy -p oxide-batch-xtask --all-targets --all-features --message-format=json --'
        ]
      },
      'quality-integration' => {
        'name' => 'quality-integration-internal',
        'tokens' => ["cargo test --workspace --all-features --test '*'"]
      },
      'quality-bin-doc' => {
        'name' => 'quality-bin-doc-internal',
        'tokens' => [
          'cargo test --workspace --all-features --bins',
          'cargo test --workspace --all-features --doc'
        ]
      },
      'quality-contracts' => {
        'name' => 'quality-contracts-internal',
        'tokens' => [
          'cargo check -p oxide-batch --no-default-features',
          'cargo check -p oxide-batch-cli --no-default-features --all-targets',
          'cargo doc --workspace --all-features --no-deps',
          'cargo run --package oxide-batch-xtask -- deps',
          'cargo run --package oxide-batch-xtask -- surface',
          'cargo run --package oxide-batch-xtask -- release-crates'
        ]
      }
    }

    component_specs.each do |job_id, spec|
      job = jobs[job_id]
      unless job.is_a?(Hash)
        violations << "#{QUALITY_WORKFLOW} is missing internal quality component #{job_id}"
        next
      end
      unless job_policy(policy, QUALITY_WORKFLOW, job_id).first == 'advisory'
        violations << "#{QUALITY_WORKFLOW}##{job_id} must remain advisory/internal"
      end
      unless job['name'] == spec['name']
        violations << "#{QUALITY_WORKFLOW}##{job_id} must keep internal context name #{spec['name']}"
      end
      expected_if = "${{ github.event.pull_request.draft == false }}"
      unless normalized_shell(job['if']) == normalized_shell(expected_if)
        violations << "#{QUALITY_WORKFLOW}##{job_id} must run on every ready pull request"
      end
      unless normalize_needs(job).empty?
        violations << "#{QUALITY_WORKFLOW}##{job_id} must start independently without serial needs"
      end
      body = Array(job['steps']).filter_map { |step| step.is_a?(Hash) ? step['run'] : nil }.join("\n")
      missing = spec['tokens'].reject { |token| body.include?(token) }
      unless missing.empty?
        violations << "#{QUALITY_WORKFLOW}##{job_id} is missing quality obligations: #{missing.join(', ')}"
      end
    end

    aggregate = jobs[QUALITY_AGGREGATE_JOB]
    unless aggregate.is_a?(Hash)
      return violations + ["#{QUALITY_WORKFLOW} must declare required #{QUALITY_AGGREGATE_JOB} aggregate"]
    end
    unless job_policy(policy, QUALITY_WORKFLOW, QUALITY_AGGREGATE_JOB).first == 'required'
      violations << "#{QUALITY_WORKFLOW}##{QUALITY_AGGREGATE_JOB} must remain required"
    end
    violations << "#{QUALITY_WORKFLOW}##{QUALITY_AGGREGATE_JOB} must emit context quality" unless aggregate['name'] == 'quality'
    violations << "#{QUALITY_WORKFLOW}##{QUALITY_AGGREGATE_JOB} must use unconditional always()" unless always_condition?(aggregate['if'])
    unless normalize_needs(aggregate).sort == QUALITY_COMPONENT_JOBS.sort
      violations << "#{QUALITY_WORKFLOW}##{QUALITY_AGGREGATE_JOB} must depend on every parallel quality component"
    end

    result_step = Array(aggregate['steps']).find do |step|
      step.is_a?(Hash) && step['name'] == 'Require all quality components'
    end
    unless result_step.is_a?(Hash)
      violations << "#{QUALITY_WORKFLOW}##{QUALITY_AGGREGATE_JOB} must fail closed over component results"
      return violations
    end
    expected_env = {
      'FAST_RESULT' => "${{ needs.quality-fast.result }}",
      'INTEGRATION_RESULT' => "${{ needs.quality-integration.result }}",
      'BIN_DOC_RESULT' => "${{ needs.quality-bin-doc.result }}",
      'CONTRACTS_RESULT' => "${{ needs.quality-contracts.result }}"
    }
    unless result_step['env'] == expected_env
      violations << "#{QUALITY_WORKFLOW}##{QUALITY_AGGREGATE_JOB} must bind every component result exactly"
    end
    result_body = result_step['run'].to_s
    required_tokens = [
      '"$FAST_RESULT"',
      '"$INTEGRATION_RESULT"',
      '"$BIN_DOC_RESULT"',
      '"$CONTRACTS_RESULT"',
      'if [ "$result" != "success" ]; then',
      'exit 1'
    ]
    missing = required_tokens.reject { |token| result_body.include?(token) }
    unless missing.empty?
      violations << "#{QUALITY_WORKFLOW}##{QUALITY_AGGREGATE_JOB} is missing fail-closed result checks: #{missing.join(', ')}"
    end

    violations
  end

  EVALUATOR_SCRIPT = '.github/scripts/evaluate-aggregate-run.rb'
  AGGREGATE_PRODUCER_PERMISSIONS = {'actions' => 'read', 'contents' => 'read'}.freeze
  TOKEN_ENV_EXPR = /\A\$\{\{\s*github\.token\s*\}\}\z/

  # The canonical checkout pin isn't duplicated here: it's read back from the
  # other jobs already checked into the same workflow, so the aggregate
  # producer is only required to reuse whatever SHA the workflow already
  # pins rather than the verifier hard-coding a second copy of it.
  def other_jobs_checkout_pins(doc, exclude_job_id)
    pins = []
    (doc['jobs'] || {}).each do |id, job|
      next if id.to_s == exclude_job_id
      next unless job.is_a?(Hash) && job['steps'].is_a?(Array)
      job['steps'].each do |step|
        uses = step.is_a?(Hash) ? step['uses'] : nil
        pins << uses if uses.is_a?(String) && uses.start_with?('actions/checkout@')
      end
    end
    pins.uniq
  end

  def evaluator_invocation_script(context)
    "ruby #{EVALUATOR_SCRIPT} #{context}"
  end

  def aggregate_inventory(root:, policy:, producer_summary:)
    violations = []
    required_contexts = producer_summary.fetch('required_contexts')
    workflow_docs = producer_summary.fetch('workflow_docs')
    context_sources = producer_summary.fetch('context_sources')
    classified_jobs = producer_summary.fetch('classified_jobs')
    static_job_contexts = producer_summary.fetch('static_job_contexts')
    aggregates = []
    member_owners = {}

    policy.fetch('aggregate_gates', []).each do |gate|
      context = gate['context']
      state = gate['state']
      members = gate['members']
      producer = gate['producer']
      migration_group = gate['migration_group'] || context

      violations << 'aggregate context must be a non-empty string' unless context.is_a?(String) && !context.empty?
      unless AGGREGATE_STATES.include?(state)
        violations << "aggregate #{context.inspect} has unsupported state #{state.inspect}"
      end
      unless migration_group.is_a?(String) && !migration_group.empty?
        violations << "aggregate #{context.inspect} must declare a non-empty migration_group"
      end
      unless members.is_a?(Array) && !members.empty? && members.all? { |member| member.is_a?(String) && !member.empty? }
        violations << "aggregate #{context.inspect} must declare a non-empty string member inventory"
        members = []
      end
      duplicate_members = members.group_by(&:itself).select { |_member, entries| entries.length > 1 }.keys
      violations << "aggregate #{context} duplicates members: #{duplicate_members.sort.join(', ')}" unless duplicate_members.empty?

      unknown = members - required_contexts
      unless unknown.empty?
        violations << "aggregate #{context} members are not required producers: #{unknown.sort.join(', ')}"
      end
      non_job_members = members.select { |member| context_sources.dig(member, 'kind') != 'job' }
      unless non_job_members.empty?
        violations << "aggregate #{context} members must be checked-in workflow jobs: #{non_job_members.sort.join(', ')}"
      end
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
      job = doc.is_a?(Hash) && doc['jobs'].is_a?(Hash) ? doc['jobs'][job_id] : nil
      if doc.nil?
        violations << "aggregate #{context} producer workflow #{workflow.inspect} does not exist"
      elsif job.nil?
        violations << "aggregate #{context} producer job #{workflow}##{job_id} does not exist"
      else
        unless pr_trigger?(doc)
          violations << "aggregate #{context} producer workflow #{workflow} must trigger on pull_request"
        end
        pr_event_configs(doc).each do |event_name, config|
          next unless config.is_a?(Hash)
          if config.key?('paths') || config.key?('paths-ignore')
            violations << "aggregate #{context} producer workflow #{workflow} can suppress #{event_name} via path filters"
          end
        end

        producer_name = job['name'] || job_id
        unless producer_name == context
          violations << "aggregate #{context} producer must emit exact job context #{context.inspect}, got #{producer_name.inspect}"
        end
        violations << "aggregate #{context} producer cannot use a matrix strategy" if job.key?('strategy')
        unless always_condition?(job['if'])
          violations << "aggregate #{context} producer must use if: \${{ always() }}"
        end
        unless job['runs-on'] == 'ubuntu-latest' && job['timeout-minutes'] == 5
          violations << "aggregate #{context} producer must use ubuntu-latest with timeout-minutes: 5"
        end
        violations << "aggregate #{context} producer cannot continue on error" if job['continue-on-error']
        unless job['permissions'] == AGGREGATE_PRODUCER_PERMISSIONS
          violations << "aggregate #{context} producer must declare exact least-privilege permissions #{AGGREGATE_PRODUCER_PERMISSIONS.inspect}, got #{job['permissions'].inspect}"
        end

        member_sources = members.filter_map { |member| context_sources[member] }
        source_workflows = member_sources.map { |source| source['workflow'] }.uniq
        unless source_workflows == [workflow]
          violations << "aggregate #{context} members must all be produced in #{workflow}; got #{source_workflows.sort.join(', ')}"
        end
        expected_needs = member_sources.map { |source| source['job'] }.uniq.sort
        begin
          actual_needs = normalize_needs(job).sort
          unless actual_needs == expected_needs
            violations << "aggregate #{context} needs mismatch: expected #{expected_needs.join(', ')}, got #{actual_needs.join(', ')}"
          end

          steps = job['steps']
          unless steps.is_a?(Array) && steps.length == 2
            violations << "aggregate #{context} producer must have exactly a checkout step and an evaluator step"
          end

          checkout_step = steps.is_a?(Array) ? steps[0] : nil
          checkout_uses = checkout_step.is_a?(Hash) ? checkout_step['uses'] : nil
          canonical_pins = other_jobs_checkout_pins(doc, job_id)
          if canonical_pins.length != 1
            violations << "aggregate #{context} producer cannot resolve a single already-pinned actions/checkout SHA from #{workflow}"
          elsif checkout_uses != canonical_pins.first
            violations << "aggregate #{context} producer checkout must reuse the pinned #{canonical_pins.first.inspect}, got #{checkout_uses.inspect}"
          end

          evaluator_step = steps.is_a?(Array) ? steps[1] : nil
          expected_run = evaluator_invocation_script(context)
          valid_evaluator_step = evaluator_step.is_a?(Hash) &&
                                  !evaluator_step['continue-on-error'] &&
                                  evaluator_step['run'].to_s.strip == expected_run &&
                                  evaluator_step['env'].is_a?(Hash) &&
                                  TOKEN_ENV_EXPR.match?(evaluator_step['env']['GITHUB_TOKEN'].to_s.strip)
          unless valid_evaluator_step
            violations << "aggregate #{context} producer step does not match the canonical evaluator invocation #{expected_run.inspect} with a GITHUB_TOKEN environment"
          end
        rescue StandardError => e
          violations << "aggregate #{context} producer #{workflow}##{job_id}: #{e.message}"
        end

        producer_classification = classified_jobs.find do |candidate|
          candidate[0] == workflow && candidate[1] == job_id
        end&.[](2)
        unless producer_classification == 'advisory'
          violations << "aggregate #{context} producer must be classified advisory"
        end
      end

      colliding_jobs = static_job_contexts.select { |entry| entry['context'] == context }
      foreign_collisions = colliding_jobs.reject do |entry|
        entry['workflow'] == workflow && entry['job'] == job_id
      end
      unless foreign_collisions.empty?
        labels = foreign_collisions.map { |entry| "#{entry['workflow']}##{entry['job']}" }.sort
        violations << "aggregate context #{context} collides with PR jobs: #{labels.join(', ')}"
      end

      aggregates << {
        'context' => context,
        'state' => state,
        'migration_group' => migration_group,
        'members' => members,
        'producer' => producer
      }
    end

    aggregate_contexts = aggregates.map { |gate| gate['context'] }
    duplicate_contexts = aggregate_contexts.group_by(&:itself).select { |_context, entries| entries.length > 1 }.keys
    violations << "aggregate contexts are duplicated: #{duplicate_contexts.sort.join(', ')}" unless duplicate_contexts.empty?
    collisions = aggregate_contexts & required_contexts
    unless collisions.empty?
      violations << "aggregate contexts collide with required child/managed producers: #{collisions.sort.join(', ')}"
    end

    aggregates.group_by { |gate| gate['migration_group'] }.each do |group, gates|
      states = gates.map { |gate| gate['state'] }.uniq
      if states.length > 1
        violations << "aggregate migration group #{group} has mixed states: #{states.sort.join(', ')}"
      end
    end

    [violations, aggregates]
  end

  def accepted_live_context_sets(required_contexts:, aggregates:, pending:)
    base = (required_contexts - pending).uniq.sort

    aggregates.select { |gate| gate['state'] == 'active' }.each do |gate|
      base = ((base - gate['members']) + [gate['context']]).uniq.sort
    end

    accepted = [base]
    cutover_groups = aggregates.select { |gate| gate['state'] == 'cutover' }
                                .group_by { |gate| gate['migration_group'] }
    cutover_groups.each_value do |gates|
      replacements = accepted.map do |contexts|
        gates.reduce(contexts) do |next_contexts, gate|
          ((next_contexts - gate['members']) + [gate['context']]).uniq.sort
        end
      end
      accepted = (accepted + replacements).uniq
    end

    accepted
  end

  def verify(root:, policy_path:, ruleset_path:)
    policy = JSON.parse(File.read(policy_path))
    ruleset = JSON.parse(File.read(ruleset_path))
    violations = []

    violations << "unsupported policy schema_version #{policy['schema_version'].inspect}" unless policy['schema_version'] == 5
    violations << "ruleset id mismatch: expected #{policy.dig('ruleset', 'id')}, got #{ruleset['id']}" unless ruleset['id'] == policy.dig('ruleset', 'id')
    violations << "ruleset name mismatch: expected #{policy.dig('ruleset', 'name').inspect}, got #{ruleset['name'].inspect}" unless ruleset['name'] == policy.dig('ruleset', 'name')
    violations << 'ruleset is not active' unless ruleset['enforcement'] == 'active'

    producer_violations, producer_summary = producer_inventory(root: root, policy: policy)
    violations.concat(producer_violations)
    aggregate_violations, aggregates = aggregate_inventory(root: root, policy: policy, producer_summary: producer_summary)
    violations.concat(aggregate_violations)

    violations.concat(pr_scope_contract(root: root, policy: policy, producer_summary: producer_summary))
    violations.concat(campaign_orchestrator_contract(root: root, policy: policy, producer_summary: producer_summary))
    violations.concat(m5_conformance_routing_contract(policy: policy, producer_summary: producer_summary))
    violations.concat(quality_parallel_contract(policy: policy, producer_summary: producer_summary))
    violations.concat(post_main_contract(policy: policy, producer_summary: producer_summary))

    required_contexts = producer_summary.fetch('required_contexts') + aggregates.map { |gate| gate['context'] }
    pending = policy.fetch('pending_ruleset_contexts', [])
    unknown_pending = pending - required_contexts
    unless unknown_pending.empty?
      violations << "pending ruleset contexts are not required producers: #{unknown_pending.sort.join(', ')}"
    end

    aggregates.each do |gate|
      context = gate['context']
      case gate['state']
      when 'candidate', 'cutover'
        violations << "#{gate['state']} aggregate #{context} must be pending ruleset promotion" unless pending.include?(context)
      when 'active'
        violations << "active aggregate #{context} cannot remain pending ruleset promotion" if pending.include?(context)
      end
    end

    accepted_live = accepted_live_context_sets(
      required_contexts: required_contexts,
      aggregates: aggregates,
      pending: pending
    )
    actual_live = live_required_contexts(ruleset).sort
    unless accepted_live.include?(actual_live)
      closest = accepted_live.min_by do |expected|
        (expected - actual_live).length + (actual_live - expected).length
      end || []
      missing = closest - actual_live
      stale = actual_live - closest
      violations << "policy-required contexts missing from live ruleset: #{missing.join(', ')}" unless missing.empty?
      violations << "live ruleset requires stale/unaccepted contexts: #{stale.join(', ')}" unless stale.empty?
    end

    [violations, {
      'required_contexts' => required_contexts.sort,
      'aggregate_gates' => aggregates,
      'pending_ruleset_contexts' => pending.sort,
      'accepted_live_required_contexts' => accepted_live,
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
