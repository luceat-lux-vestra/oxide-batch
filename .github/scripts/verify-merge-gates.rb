#!/usr/bin/env ruby
# frozen_string_literal: true

require 'json'
require 'yaml'
require 'pathname'
require 'digest/sha1'

module MergeGateVerifier
  module_function

  VALID_CLASSIFICATIONS = %w[required advisory optional].freeze
  AGGREGATE_STATES = %w[candidate cutover active].freeze
  REPOSITORY_MERGE_GATE_STATES = AGGREGATE_STATES
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
    return ['schema v6 policy must declare post_main'] unless config.is_a?(Hash)

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
  M5_CONFORMANCE_SHARD_JOBS = {'15' => 'conformance-shard-15', '18' => 'conformance-shard-18'}.freeze
  M5_CONFORMANCE_DEEP_JOBS = {'15' => 'conformance-deep-15', '18' => 'conformance-deep-18'}.freeze
  M5_CONFORMANCE_CONTEXT_JOB = 'conformance-campaign'
  QUALITY_WORKFLOW = '.github/workflows/ci.yml'
  FAST_WORKFLOW = '.github/workflows/fast-branch.yml'
  FAST_JOB = 'fast'
  SUPPLY_CHAIN_WORKFLOW = '.github/workflows/supply-chain.yml'
  SUPPLY_CHAIN_AUDIT_WORKFLOW = '.github/workflows/supply-chain-audit.yml'
  SUPPLY_CHAIN_JOB = 'supply-chain'
  EVIDENCE_WORKFLOW = '.github/workflows/evidence.yml'
  EVIDENCE_JOB = 'evidence-provenance'
  QUALITY_AGGREGATE_JOB = 'quality'
  QUALITY_INTEGRATION_SHARD_SCRIPT = '.github/scripts/run-integration-shard.py'
  QUALITY_INTEGRATION_JOBS = %w[quality-integration-0 quality-integration-1 quality-integration-2 quality-integration-3].freeze
  QUALITY_COMPONENT_JOBS = (%w[quality-fast quality-bin-doc quality-contracts] + QUALITY_INTEGRATION_JOBS).freeze
  REPOSITORY_MERGE_GATE_CONTEXT = 'merge-gate'
  REPOSITORY_MERGE_GATE_WORKFLOW = '.github/workflows/pr-labeler.yml'
  REPOSITORY_MERGE_GATE_JOB = 'merge-gate'
  PR_CI_WORKFLOW = '.github/workflows/pr-ci.yml'
  PR_CI_SCOPE_JOB = 'scope'
  PR_CI_PROOF_JOB = 'pr-proof'
  PR_TOPOLOGY_SCHEMA = 'single-pr-entrypoint-v1'
  PR_TOPOLOGY_MIGRATION_MARKER = '.github/ci-topology-v7-migration'
  PR_TOPOLOGY_INDEPENDENT_M5 = '.github/workflows/m5-conformance.yml'
  PR_TOPOLOGY_REUSABLE_AUTHORITIES = [
    '.github/workflows/ci.yml',
    '.github/workflows/dependency-review.yml',
    '.github/workflows/codeql.yml',
    '.github/workflows/evidence.yml',
    '.github/workflows/supply-chain.yml',
    '.github/workflows/campaign-orchestrator.yml'
  ].freeze
  PR_TOPOLOGY_PROTECTED_AUTHORITIES = [
    PR_CI_WORKFLOW,
    '.github/workflows/ci.yml',
    '.github/workflows/dependency-review.yml',
    '.github/workflows/codeql.yml',
    '.github/workflows/evidence.yml',
    '.github/workflows/supply-chain.yml',
    PR_TOPOLOGY_INDEPENDENT_M5,
    REPOSITORY_MERGE_GATE_WORKFLOW
  ].freeze
  INTERNAL_POSTGRESQL_AGGREGATE = {
    'context' => 'postgresql',
    'producer' => {
      'workflow' => QUALITY_WORKFLOW,
      'job' => 'postgresql-merge-gate'
    },
    'members' => %w[
      postgres-15-design-gate
      postgres-15-item-components
      postgres-15-repository
      postgres-16-design-gate
      postgres-17-design-gate
      postgres-18-design-gate
      postgres-18-item-components
      postgres-18-repository
    ]
  }.freeze
  REPOSITORY_MERGE_GATE_PERMISSIONS = {
    'actions' => 'read',
    'contents' => 'read',
    'pull-requests' => 'read'
  }.freeze
  REPOSITORY_MERGE_GATE_EVENT_TYPES = %w[opened edited synchronize reopened ready_for_review].freeze
  PR_SCOPE_GLOBAL_DIRECT_PROOF_PATHS = [
    PR_SCOPE_SCRIPT,
    '.github/merge-gate-policy.json',
    CAMPAIGN_ORCHESTRATOR_WORKFLOW,
    PR_SCOPE_RETAINED_POLICY
  ].freeze
  PR_SCOPE_DOCS_APPLICABILITY = {
    'supply_chain' => {
      'sensitive_exact_paths' => ['docs/engineering/dependency-policy.md'],
      'sensitive_prefixes' => []
    },
    'evidence_provenance' => {
      'sensitive_exact_paths' => [],
      'sensitive_prefixes' => ['docs/engineering/campaigns/']
    }
  }.freeze

  def normalized_shell(command)
    command.to_s.split.join(' ')
  end

  def fast_branch_docs_contract(producer_summary:)
    violations = []
    doc = producer_summary.fetch('workflow_docs')[FAST_WORKFLOW]
    unless doc.is_a?(Hash)
      return ["#{FAST_WORKFLOW} is missing"]
    end

    unless workflow_event?(doc, 'push') && !push_targets_branch?(doc, 'main')
      violations << "#{FAST_WORKFLOW} must run on non-main branch pushes and must not target main"
    end
    unless doc['permissions'] == {'contents' => 'read'}
      violations << "#{FAST_WORKFLOW} must keep contents: read as its only workflow permission"
    end

    jobs = doc['jobs']
    job = jobs.is_a?(Hash) ? jobs[FAST_JOB] : nil
    unless job.is_a?(Hash)
      return violations + ["#{FAST_WORKFLOW} must declare canonical #{FAST_JOB} job"]
    end
    violations << "#{FAST_WORKFLOW}##{FAST_JOB} must emit context fast" unless job['name'] == 'fast'
    violations << "#{FAST_WORKFLOW}##{FAST_JOB} must run on ubuntu-latest" unless job['runs-on'] == 'ubuntu-latest'
    violations << "#{FAST_WORKFLOW}##{FAST_JOB} timeout must remain 15 minutes" unless job['timeout-minutes'] == 15

    steps = job['steps']
    unless steps.is_a?(Array)
      return violations + ["#{FAST_WORKFLOW}##{FAST_JOB} must declare steps"]
    end
    by_name = steps.select { |step| step.is_a?(Hash) && step['name'].is_a?(String) }
                   .group_by { |step| step['name'] }
    required_names = [
      'Check out repository',
      'Check commit hygiene',
      'Resolve exact protected trusted base',
      'Check out exact trusted base for docs-only classification',
      'Classify documentation-only scope from trusted base',
      'Documentation-only fast path',
      'Show toolchain',
      'Check formatting',
      'Run Clippy',
      'Run workspace unit tests'
    ]
    required_names.each do |name|
      count = Array(by_name[name]).length
      violations << "#{FAST_WORKFLOW}##{FAST_JOB} must contain exactly one #{name.inspect} step" unless count == 1
    end
    return violations unless required_names.all? { |name| Array(by_name[name]).length == 1 }

    resolve = by_name['Resolve exact protected trusted base'].first
    unless resolve['id'] == 'resolve-trusted-base' && resolve['continue-on-error'] == true
      violations << "#{FAST_WORKFLOW} trusted-base resolver must be continue-on-error id resolve-trusted-base"
    end
    resolve_tokens = [
      "echo 'base_sha=' >> \"$GITHUB_OUTPUT\"",
      'set -euo pipefail',
      'repos/${GITHUB_REPOSITORY}',
      '.full_name == $repo and .default_branch == $branch',
      'repos/${GITHUB_REPOSITORY}/branches/${DEFAULT_BRANCH}',
      '[[ "$base_sha" =~ ^[0-9a-f]{40}$ ]]',
      '[[ "$HEAD_SHA" =~ ^[0-9a-f]{40}$ ]]',
      'base_sha=$base_sha'
    ]
    missing = resolve_tokens.reject { |token| resolve['run'].to_s.include?(token) }
    violations << "#{FAST_WORKFLOW} trusted-base resolver is missing fail-closed tokens: #{missing.join(', ')}" unless missing.empty?

    trusted = by_name['Check out exact trusted base for docs-only classification'].first
    unless trusted['id'] == 'fast-trusted-base' &&
           trusted['continue-on-error'] == true &&
           normalized_shell(trusted['if']) == normalized_shell("${{ steps.resolve-trusted-base.outcome == 'success' }}")
      violations << "#{FAST_WORKFLOW} trusted-base checkout must be conditional fail-closed id fast-trusted-base"
    end
    unless trusted['uses'].to_s.match?(/\Aactions\/checkout@[0-9a-f]{40}\z/)
      violations << "#{FAST_WORKFLOW} trusted-base checkout must use SHA-pinned actions/checkout"
    end
    expected_trusted_with = {
      'ref' => '${{ steps.resolve-trusted-base.outputs.base_sha }}',
      'path' => '.fast-trusted-base',
      'fetch-depth' => 1,
      'persist-credentials' => false
    }
    violations << "#{FAST_WORKFLOW} trusted-base checkout must target only the resolved exact base SHA" unless trusted['with'] == expected_trusted_with

    classify = by_name['Classify documentation-only scope from trusted base'].first
    expected_classify_if = "${{ steps.resolve-trusted-base.outcome == 'success' && steps.fast-trusted-base.outcome == 'success' }}"
    unless classify['id'] == 'docs-only-scope' &&
           classify['continue-on-error'] == true &&
           normalized_shell(classify['if']) == normalized_shell(expected_classify_if)
      violations << "#{FAST_WORKFLOW} docs-only classifier must be fail-closed id docs-only-scope"
    end
    classify_tokens = [
      "echo 'docs_only=false' >> \"$GITHUB_OUTPUT\"",
      'test "$TRUSTED_CHECKOUT" = "success"',
      'compare/${BASE_SHA}...${HEAD_SHA}',
      '.head_commit.sha == $head',
      '(.files | length) > 0',
      '(.files | length) < 300',
      '@tsv',
      '.fast-trusted-base/.github/scripts/pr-scope.py',
      '--repo-root .fast-trusted-base',
      '--policy .github/merge-gate-policy.json',
      '--expected-count "$expected_count"',
      '--trusted-base-sha "$BASE_SHA"',
      '.classification_valid == true',
      '(.docs_only | type) == "boolean"',
      'docs_only=$docs_only'
    ]
    missing = classify_tokens.reject { |token| classify['run'].to_s.include?(token) }
    violations << "#{FAST_WORKFLOW} docs-only classifier is missing trusted/fail-closed tokens: #{missing.join(', ')}" unless missing.empty?

    docs_if = "${{ steps.docs-only-scope.outcome == 'success' && steps.docs-only-scope.outputs.docs_only == 'true' }}"
    unless normalized_shell(by_name['Documentation-only fast path'].first['if']) == normalized_shell(docs_if)
      violations << "#{FAST_WORKFLOW} documentation-only fast path must require successful true trusted classification"
    end

    full_if = "${{ steps.docs-only-scope.outcome != 'success' || steps.docs-only-scope.outputs.docs_only != 'true' }}"
    ['Show toolchain', 'Check formatting', 'Run Clippy', 'Run workspace unit tests'].each do |name|
      unless normalized_shell(by_name[name].first['if']) == normalized_shell(full_if)
        violations << "#{FAST_WORKFLOW} #{name.inspect} must run on every non-docs or uncertain classification"
      end
    end
    violations << "#{FAST_WORKFLOW} must retain cargo fmt full-path proof" unless by_name['Check formatting'].first['run'].to_s.include?('cargo fmt --all -- --check')
    violations << "#{FAST_WORKFLOW} must retain workspace Clippy full-path proof" unless by_name['Run Clippy'].first['run'].to_s.include?('cargo clippy --workspace --all-targets --all-features --')
    violations << "#{FAST_WORKFLOW} must retain workspace lib-test full-path proof" unless by_name['Run workspace unit tests'].first['run'].to_s.include?('cargo test --workspace --all-features --lib')

    violations
  end

  def docs_applicability_workflow_contract(
    producer_summary:,
    workflow:,
    job_id:,
    context:,
    checkout_id:,
    checkout_path:,
    classify_id:,
    impact_key:,
    light_step_name:,
    heavy_step_names:,
    classifier_if:,
    light_if:,
    full_if:,
    require_schedule: false
  )
    violations = []
    doc = producer_summary.fetch('workflow_docs')[workflow]
    unless doc.is_a?(Hash)
      return ["#{workflow} is missing"]
    end
    violations << "#{workflow} must remain pull_request-triggered" unless workflow_event?(doc, 'pull_request')
    if require_schedule && !workflow_event?(doc, 'schedule')
      violations << "#{workflow} must retain scheduled full validation"
    end
    expected_permissions = {'contents' => 'read', 'pull-requests' => 'read'}
    unless doc['permissions'] == expected_permissions
      violations << "#{workflow} must keep only contents/pull-requests read permissions"
    end

    jobs = doc['jobs']
    job = jobs.is_a?(Hash) ? jobs[job_id] : nil
    unless job.is_a?(Hash)
      return violations + ["#{workflow} must declare canonical #{job_id} job"]
    end
    violations << "#{workflow}##{job_id} must emit context #{context}" unless job['name'] == context
    steps = Array(job['steps']).select { |step| step.is_a?(Hash) }
    by_name = steps.select { |step| step['name'].is_a?(String) }.group_by { |step| step['name'] }

    trusted = steps.find { |step| step['id'] == checkout_id }
    unless trusted.is_a?(Hash)
      violations << "#{workflow}##{job_id} is missing exact trusted-base applicability checkout"
    else
      violations << "#{workflow}##{job_id} trusted-base checkout must continue on error for full fallback" unless trusted['continue-on-error'] == true
      unless normalized_shell(trusted['if']) == normalized_shell(classifier_if)
        violations << "#{workflow}##{job_id} trusted-base checkout must use the canonical PR condition"
      end
      unless trusted['uses'].to_s.match?(/\Aactions\/checkout@[0-9a-f]{40}\z/)
        violations << "#{workflow}##{job_id} trusted-base checkout must use SHA-pinned actions/checkout"
      end
      expected_with = {
        'ref' => '${{ github.event.pull_request.base.sha }}',
        'path' => checkout_path,
        'fetch-depth' => 1,
        'persist-credentials' => false
      }
      unless trusted['with'] == expected_with
        violations << "#{workflow}##{job_id} trusted-base checkout must target exact PR base SHA in #{checkout_path}"
      end
    end

    classify = steps.find { |step| step['id'] == classify_id }
    unless classify.is_a?(Hash)
      return violations + ["#{workflow}##{job_id} is missing trusted applicability classifier"]
    end
    violations << "#{workflow}##{job_id} classifier must continue on error for full fallback" unless classify['continue-on-error'] == true
    unless normalized_shell(classify['if']) == normalized_shell(classifier_if)
      violations << "#{workflow}##{job_id} classifier must use the canonical PR condition"
    end
    expected_env = {
      'GH_TOKEN' => '${{ github.token }}',
      'BASE_SHA' => '${{ github.event.pull_request.base.sha }}',
      'HEAD_SHA' => '${{ github.event.pull_request.head.sha }}',
      'PR_NUMBER' => '${{ github.event.pull_request.number }}',
      'TRUSTED_CHECKOUT' => "${{ steps.#{checkout_id}.outcome }}"
    }
    unless classify['env'] == expected_env
      violations << "#{workflow}##{job_id} classifier must bind canonical exact base/head/PR inputs"
    end
    command = classify['run'].to_s
    required_tokens = [
      "echo 'impact=true' >> \"$GITHUB_OUTPUT\"",
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
      "#{checkout_path}/.github/scripts/pr-scope.py",
      "--repo-root #{checkout_path}",
      '--policy .github/merge-gate-policy.json',
      '--expected-count "$expected_count"',
      '--trusted-base-sha "$BASE_SHA"',
      ".#{impact_key}",
      'type) == "boolean"',
      'impact=$impact'
    ]
    missing = required_tokens.reject { |token| command.include?(token) }
    unless missing.empty?
      violations << "#{workflow}##{job_id} classifier is missing fail-closed applicability tokens: #{missing.join(', ')}"
    end

    light = by_name[light_step_name]&.first
    unless light.is_a?(Hash) && normalized_shell(light['if']) == normalized_shell(light_if)
      violations << "#{workflow}##{job_id} lightweight success must require successful false trusted impact"
    end

    heavy_step_names.each do |name|
      candidates = by_name[name]
      if !candidates || candidates.length != 1
        violations << "#{workflow}##{job_id} must contain exactly one #{name.inspect} heavy step"
        next
      end
      unless normalized_shell(candidates.first['if']) == normalized_shell(full_if)
        violations << "#{workflow}##{job_id} #{name.inspect} must run on impact or classifier uncertainty"
      end
    end
    violations
  end

  def docs_applicability_contract(policy:, producer_summary:)
    violations = []
    unless policy.dig('pr_scope', 'docs_only_applicability') == PR_SCOPE_DOCS_APPLICABILITY
      violations << 'pr_scope docs_only_applicability must exactly match canonical supply/evidence ownership'
    end

    supply_classifier_if = "${{ github.event_name == 'pull_request' && github.event.pull_request.draft == false }}"
    supply_light_if = "${{ github.event_name == 'pull_request' && github.event.pull_request.draft == false && steps.supply-impact.outcome == 'success' && steps.supply-impact.outputs.impact == 'false' }}"
    supply_full_if = "${{ github.event_name != 'pull_request' || (github.event.pull_request.draft == false && (steps.supply-impact.outcome != 'success' || steps.supply-impact.outputs.impact != 'false')) }}"
    violations.concat(
      docs_applicability_workflow_contract(
        producer_summary: producer_summary,
        workflow: SUPPLY_CHAIN_WORKFLOW,
        job_id: SUPPLY_CHAIN_JOB,
        context: 'supply-chain',
        checkout_id: 'supply-trusted-base',
        checkout_path: '.supply-trusted-base',
        classify_id: 'supply-impact',
        impact_key: 'supply_chain_impact',
        light_step_name: 'Documentation-only supply-chain fast path',
        heavy_step_names: [
          'Test supply-chain exception policy',
          'Validate supply-chain exception registry',
          'Check advisories, licenses, bans, and sources'
        ],
        classifier_if: supply_classifier_if,
        light_if: supply_light_if,
        full_if: supply_full_if
      )
    )

    evidence_classifier_if = "${{ github.event.pull_request.draft == false }}"
    evidence_light_if = "${{ github.event.pull_request.draft == false && steps.evidence-impact.outcome == 'success' && steps.evidence-impact.outputs.impact == 'false' }}"
    evidence_full_if = "${{ github.event_name != 'pull_request' || (github.event.pull_request.draft == false && (steps.evidence-impact.outcome != 'success' || steps.evidence-impact.outputs.impact != 'false')) }}"
    violations.concat(
      docs_applicability_workflow_contract(
        producer_summary: producer_summary,
        workflow: EVIDENCE_WORKFLOW,
        job_id: EVIDENCE_JOB,
        context: 'evidence-provenance',
        checkout_id: 'evidence-trusted-base',
        checkout_path: '.evidence-trusted-base',
        classify_id: 'evidence-impact',
        impact_key: 'evidence_impact',
        light_step_name: 'Documentation-only evidence fast path',
        heavy_step_names: [
          'Verify repository-wide retained evidence policy',
          'Hydrate locked dependency graph for closure verification',
          'Verify campaign dependency closure metadata',
          'Verify retained campaign evidence integrity and provenance'
        ],
        classifier_if: evidence_classifier_if,
        light_if: evidence_light_if,
        full_if: evidence_full_if
      )
    )
    violations
  end

  def git_blob_sha(content)
    Digest::SHA1.hexdigest("blob #{content.bytesize}\0#{content}")
  end

  def pr_topology_v7_contract(root:, policy:, producer_summary:)
    return [] unless policy['schema_version'] == 7

    violations = []
    topology = policy['pr_topology']
    expected_topology = {
      'schema' => PR_TOPOLOGY_SCHEMA,
      'entrypoint' => PR_CI_WORKFLOW,
      'migration_marker' => PR_TOPOLOGY_MIGRATION_MARKER,
      'reusable_authorities' => PR_TOPOLOGY_REUSABLE_AUTHORITIES,
      'independent_pr_authorities' => [
        {
          'workflow' => PR_TOPOLOGY_INDEPENDENT_M5,
          'reason' => 'retained-evidence-provenance',
          'contexts' => [
            'postgres-15-conformance-campaign',
            'postgres-18-conformance-campaign'
          ]
        }
      ]
    }
    unless topology == expected_topology
      violations << 'schema v7 pr_topology must exactly match the canonical single-entrypoint contract'
    end

    docs = producer_summary.fetch('workflow_docs')
    entrypoint = docs[PR_CI_WORKFLOW]
    unless entrypoint.is_a?(Hash)
      return violations + ["schema v7 entrypoint #{PR_CI_WORKFLOW} is missing"]
    end

    unless workflow_event?(entrypoint, 'pull_request') && !pull_request_target_trigger?(entrypoint)
      violations << "#{PR_CI_WORKFLOW} must be the ordinary pull_request entrypoint"
    end
    present, config = event_config(entrypoint, 'pull_request')
    if present && config.is_a?(Hash) && (config.key?('paths') || config.key?('paths-ignore'))
      violations << "#{PR_CI_WORKFLOW} must not suppress pull_request events with path filters"
    end
    expected_permissions = {'contents' => 'read', 'pull-requests' => 'read'}
    unless entrypoint['permissions'] == expected_permissions
      violations << "#{PR_CI_WORKFLOW} must keep only contents/pull-requests read workflow permissions"
    end

    jobs = entrypoint['jobs']
    unless jobs.is_a?(Hash)
      return violations + ["#{PR_CI_WORKFLOW} must declare jobs"]
    end
    expected_jobs = %w[scope rust dependency codeql evidence supply campaigns pr-proof]
    missing_jobs = expected_jobs - jobs.keys.map(&:to_s)
    violations << "#{PR_CI_WORKFLOW} is missing canonical jobs: #{missing_jobs.join(', ')}" unless missing_jobs.empty?

    call_contracts = {
      'rust' => '.github/workflows/ci.yml',
      'dependency' => '.github/workflows/dependency-review.yml',
      'codeql' => '.github/workflows/codeql.yml',
      'evidence' => '.github/workflows/evidence.yml',
      'supply' => '.github/workflows/supply-chain.yml',
      'campaigns' => '.github/workflows/campaign-orchestrator.yml'
    }
    call_contracts.each do |job_id, workflow|
      job = jobs[job_id]
      unless job.is_a?(Hash) && job['uses'] == "./#{workflow}"
        violations << "#{PR_CI_WORKFLOW}##{job_id} must call #{workflow}"
        next
      end
      unless normalize_needs(job).include?(PR_CI_SCOPE_JOB)
        violations << "#{PR_CI_WORKFLOW}##{job_id} must depend on trusted scope"
      end
      condition = normalized_shell(job['if'])
      unless condition.include?("needs.scope.outputs.legacy_base != 'true'") &&
             condition.include?("needs.scope.outputs.classification_outcome != 'success'")
        violations << "#{PR_CI_WORKFLOW}##{job_id} must fail closed on legacy/uncertain scope"
      end
    end

    %w[rust dependency codeql campaigns].each do |job_id|
      condition = normalized_shell(jobs.dig(job_id, 'if'))
      unless condition.include?("needs.scope.outputs.docs_only != 'true'")
        violations << "#{PR_CI_WORKFLOW}##{job_id} must suppress only proven docs-only scope"
      end
    end
    evidence_if = normalized_shell(jobs.dig('evidence', 'if'))
    unless evidence_if.include?("needs.scope.outputs.evidence_impact != 'false'")
      violations << "#{PR_CI_WORKFLOW}#evidence must run unless trusted evidence impact is false"
    end
    supply_if = normalized_shell(jobs.dig('supply', 'if'))
    unless supply_if.include?("needs.scope.outputs.supply_chain_impact != 'false'")
      violations << "#{PR_CI_WORKFLOW}#supply must run unless trusted supply impact is false"
    end

    scope = jobs[PR_CI_SCOPE_JOB]
    unless scope.is_a?(Hash)
      violations << "#{PR_CI_WORKFLOW} must declare trusted scope job"
    else
      outputs = scope['outputs']
      expected_outputs = %w[legacy_base classification_outcome docs_only supply_chain_impact evidence_impact]
      missing_outputs = expected_outputs.reject { |name| outputs.is_a?(Hash) && outputs.key?(name) }
      violations << "#{PR_CI_WORKFLOW} scope is missing outputs: #{missing_outputs.join(', ')}" unless missing_outputs.empty?
      steps = Array(scope['steps']).select { |step| step.is_a?(Hash) }
      checkout = steps.find { |step| step['id'] == 'trusted-base' }
      classify = steps.find { |step| step['id'] == 'classify' }
      unless checkout.is_a?(Hash) &&
             checkout['continue-on-error'] == true &&
             checkout['uses'].to_s.match?(/\Aactions\/checkout@[0-9a-f]{40}\z/) &&
             checkout.dig('with', 'ref') == '${{ github.event.pull_request.base.sha }}' &&
             checkout.dig('with', 'path') == '.trusted-base' &&
             checkout.dig('with', 'persist-credentials') == false
        violations << "#{PR_CI_WORKFLOW} scope must establish the exact trusted base fail-closed"
      end
      unless classify.is_a?(Hash) && classify['continue-on-error'] == true
        violations << "#{PR_CI_WORKFLOW} scope classifier must continue on error for full fallback"
      else
        command = classify['run'].to_s
        required_tokens = [
          "echo 'legacy_base=unknown'",
          "echo 'legacy_base=true'",
          "echo 'legacy_base=false'",
          "echo 'docs_only=false'",
          "echo 'supply_chain_impact=true'",
          "echo 'evidence_impact=true'",
          '>> "$GITHUB_OUTPUT"',
          '.trusted-base/.github/merge-gate-policy.json',
          'schema_version',
          'repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}',
          'pulls/${PR_NUMBER}/files?per_page=100',
          '.trusted-base/.github/scripts/pr-scope.py',
          '--trusted-base-sha "$BASE_SHA"',
          '.docs_only',
          '.supply_chain_impact',
          '.evidence_impact'
        ]
        missing = required_tokens.reject { |token| command.include?(token) }
        unless missing.empty?
          violations << "#{PR_CI_WORKFLOW} scope classifier is missing trusted/fail-closed tokens: #{missing.join(', ')}"
        end
      end
    end

    proof = jobs[PR_CI_PROOF_JOB]
    unless proof.is_a?(Hash)
      violations << "#{PR_CI_WORKFLOW} must declare #{PR_CI_PROOF_JOB}"
    else
      violations << "#{PR_CI_WORKFLOW}##{PR_CI_PROOF_JOB} must emit context pr-proof" unless proof['name'] == 'pr-proof'
      expected_needs = %w[scope rust dependency codeql evidence supply].sort
      actual_needs = normalize_needs(proof).sort
      unless actual_needs == expected_needs
        violations << "#{PR_CI_WORKFLOW}##{PR_CI_PROOF_JOB} needs mismatch: expected=#{expected_needs.inspect} actual=#{actual_needs.inspect}"
      end
      violations << "#{PR_CI_WORKFLOW}##{PR_CI_PROOF_JOB} must use always()" unless always_condition?(proof['if'])
      proof_command = Array(proof['steps']).filter_map { |step| step.is_a?(Hash) ? step['run'] : nil }.join("\n")
      %w[RUST_RESULT DEPENDENCY_RESULT CODEQL_RESULT EVIDENCE_RESULT SUPPLY_RESULT].each do |token|
        violations << "#{PR_CI_WORKFLOW}##{PR_CI_PROOF_JOB} is missing #{token} authority check" unless proof_command.include?(token)
      end
      unless proof_command.include?('CLASSIFICATION_OUTCOME') &&
             proof_command.include?('DOCS_ONLY') &&
             proof_command.include?('LEGACY_BASE') &&
             proof_command.include?('if [ "$LEGACY_BASE" = "true" ]; then')
        violations << "#{PR_CI_WORKFLOW}##{PR_CI_PROOF_JOB} must bind trusted routing outputs and reserve the legacy shortcut for an explicit trusted legacy base"
      end
    end

    PR_TOPOLOGY_REUSABLE_AUTHORITIES.each do |workflow|
      doc = docs[workflow]
      unless doc.is_a?(Hash)
        violations << "reusable authority #{workflow} is missing"
        next
      end
      violations << "#{workflow} must expose workflow_call" unless workflow_event?(doc, 'workflow_call')
      pr_present, pr_config = event_config(doc, 'pull_request')
      unless pr_present && pr_config.is_a?(Hash) &&
             pr_config['paths'] == [PR_TOPOLOGY_MIGRATION_MARKER] &&
             !pr_config.key?('paths-ignore')
        violations << "#{workflow} direct pull_request trigger must be marker-only during v7 migration"
      end
    end

    independent = docs[PR_TOPOLOGY_INDEPENDENT_M5]
    unless independent.is_a?(Hash)
      violations << "independent provenance authority #{PR_TOPOLOGY_INDEPENDENT_M5} is missing"
    else
      violations << "#{PR_TOPOLOGY_INDEPENDENT_M5} must remain directly pull_request-triggered" unless workflow_event?(independent, 'pull_request')
      violations << "#{PR_TOPOLOGY_INDEPENDENT_M5} must not become a reusable workflow in schema v7" if workflow_event?(independent, 'workflow_call')
      pr_present, pr_config = event_config(independent, 'pull_request')
      if pr_present && pr_config.is_a?(Hash) && (pr_config.key?('paths') || pr_config.key?('paths-ignore'))
        violations << "#{PR_TOPOLOGY_INDEPENDENT_M5} provenance authority must not be path-suppressed"
      end
    end

    audit = docs[SUPPLY_CHAIN_AUDIT_WORKFLOW]
    unless audit.is_a?(Hash)
      violations << "scheduled supply-chain audit #{SUPPLY_CHAIN_AUDIT_WORKFLOW} is missing"
    else
      violations << "#{SUPPLY_CHAIN_AUDIT_WORKFLOW} must be schedule-triggered" unless workflow_event?(audit, 'schedule')
      violations << "#{SUPPLY_CHAIN_AUDIT_WORKFLOW} must not be pull_request-triggered" if workflow_event?(audit, 'pull_request')
      unless audit['permissions'] == {}
        violations << "#{SUPPLY_CHAIN_AUDIT_WORKFLOW} must keep workflow-level permissions empty"
      end
      audit_jobs = audit['jobs']
      supply_job = audit_jobs.is_a?(Hash) ? audit_jobs['supply-chain'] : nil
      report_job = audit_jobs.is_a?(Hash) ? audit_jobs['report-failure'] : nil
      unless supply_job.is_a?(Hash) &&
             supply_job['uses'] == "./#{SUPPLY_CHAIN_WORKFLOW}" &&
             supply_job['permissions'] == {
               'contents' => 'read',
               'pull-requests' => 'read'
             }
        violations << "#{SUPPLY_CHAIN_AUDIT_WORKFLOW} must call the read-only reusable supply-chain authority"
      end
      unless report_job.is_a?(Hash) &&
             normalize_needs(report_job) == ['supply-chain'] &&
             normalized_shell(report_job['if']).include?('needs.supply-chain.result') &&
             report_job['permissions'] == {
               'contents' => 'read',
               'issues' => 'write'
             }
        violations << "#{SUPPLY_CHAIN_AUDIT_WORKFLOW} failure reporter must be isolated behind the supply-chain result with issues: write"
      end
    end

    gate = policy['repository_merge_gate']
    protected = gate.is_a?(Hash) ? gate['protected_workflows'] : nil
    if !protected.is_a?(Array)
      violations << 'schema v7 repository merge gate must declare protected workflow authorities'
    else
      protected_by_workflow = {}
      protected.each do |entry|
        unless entry.is_a?(Hash) && entry['workflow'].is_a?(String)
          violations << "schema v7 protected workflow entry is malformed: #{entry.inspect}"
          next
        end
        workflow = entry['workflow']
        if protected_by_workflow.key?(workflow)
          violations << "schema v7 protected workflow inventory duplicates #{workflow}"
          next
        end
        protected_by_workflow[workflow] = entry
      end

      actual_workflows = protected_by_workflow.keys.sort
      expected_workflows = PR_TOPOLOGY_PROTECTED_AUTHORITIES.sort
      unless actual_workflows == expected_workflows
        violations << "schema v7 protected workflow inventory mismatch: expected=#{expected_workflows.inspect} actual=#{actual_workflows.inspect}"
      end

      PR_TOPOLOGY_PROTECTED_AUTHORITIES.each do |workflow|
        entry = protected_by_workflow[workflow]
        next unless entry.is_a?(Hash)

        accepted = entry['accepted_blobs']
        valid = accepted.is_a?(Array) && !accepted.empty? &&
                accepted.uniq.length == accepted.length &&
                accepted.all? { |sha| sha.is_a?(String) && sha.match?(/\A[0-9a-f]{40}\z/) }
        unless valid
          violations << "schema v7 protected workflow #{workflow} accepted blob inventory is malformed"
          next
        end

        path = Pathname(root).join(workflow)
        actual = path.file? ? git_blob_sha(path.binread) : nil
        unless accepted.include?(actual)
          violations << "schema v7 protected workflow #{workflow} blob #{actual.inspect} is not accepted by policy"
        end
      end
    end

    violations
  end

  def pr_scope_contract(root:, policy:, producer_summary:)
    violations = []
    scope = policy['pr_scope']
    unless scope.is_a?(Hash)
      return ['schema v6 policy must declare pr_scope']
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

    expected_if = "${{ always() && (github.event_name == 'workflow_dispatch' || (github.event_name == 'pull_request' && github.event.pull_request.draft == false && (needs.route.result != 'success' || needs.route.outputs.classification_outcome != 'success' || contains(needs.route.outputs.direct_workflows, '.github/workflows/m5-conformance.yml')))) }}"

    if jobs.key?('conformance-deep')
      violations << "#{M5_CONFORMANCE_WORKFLOW} must not retain the obsolete unsharded conformance-deep job"
    end

    M5_CONFORMANCE_SHARD_JOBS.each do |major, job_id|
      shard = jobs[job_id]
      unless shard.is_a?(Hash)
        violations << "#{M5_CONFORMANCE_WORKFLOW} must declare #{job_id}"
        next
      end
      unless job_policy(policy, M5_CONFORMANCE_WORKFLOW, job_id).first == 'advisory'
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} must remain advisory"
      end
      unless normalize_needs(shard) == [CAMPAIGN_ROUTE_JOB]
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} must depend only on the trusted route"
      end
      unless normalized_shell(shard['if']) == normalized_shell(expected_if)
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} must run for manual/direct proof and fail closed on routing ambiguity"
      end
      expected_name = "deep-postgres-#{major}-conformance-shard-${{ matrix.shard }}"
      unless shard['name'] == expected_name
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} must keep the internal shard context name"
      end
      unless shard.dig('strategy', 'matrix', 'shard') == [0, 1]
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} must retain the exact two-way shard matrix [0, 1]"
      end
      unless shard.dig('services', 'postgres').is_a?(Hash)
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} must own PostgreSQL service provisioning"
      end
      shard_step = Array(shard['steps']).find do |step|
        step.is_a?(Hash) && step['name'] == "Run PostgreSQL #{major} conformance shard"
      end
      unless shard_step.is_a?(Hash) &&
             shard_step.dig('env', 'SHARD_INDEX') == "${{ matrix.shard }}" &&
             shard_step['run'] == "./tests/fixtures/conformance/run-ci-campaign.sh #{major} \"$SHARD_INDEX\" 2"
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} must pass the checked-in shard index through env before shell execution"
      end
      body = Array(shard['steps']).filter_map { |step| step.is_a?(Hash) ? step['run'] : nil }.join("\n")
      required_tokens = [
        "./tests/fixtures/conformance/verify-ci-contract.sh .github/workflows/m5-conformance.yml"
      ]
      missing = required_tokens.reject { |token| body.include?(token) }
      unless missing.empty?
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} is missing shard proof commands: #{missing.join(', ')}"
      end
      upload = Array(shard['steps']).find do |step|
        step.is_a?(Hash) && step.dig('with', 'name') == "conformance-shard-postgres-#{major}-${{ matrix.shard }}"
      end
      unless upload.is_a?(Hash) &&
             upload.dig('with', 'path') == 'target/m5-campaigns/conformance-shard-${{ matrix.shard }}.json' &&
             upload.dig('with', 'if-no-files-found') == 'error'
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} must retain each shard report fail closed"
      end
    end

    M5_CONFORMANCE_DEEP_JOBS.each do |major, job_id|
      shard_job = M5_CONFORMANCE_SHARD_JOBS.fetch(major)
      deep = jobs[job_id]
      unless deep.is_a?(Hash)
        violations << "#{M5_CONFORMANCE_WORKFLOW} must declare #{job_id}"
        next
      end
      unless job_policy(policy, M5_CONFORMANCE_WORKFLOW, job_id).first == 'advisory'
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} must remain advisory"
      end
      unless normalize_needs(deep).sort == [CAMPAIGN_ROUTE_JOB, shard_job].sort
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} must depend on routing and only its PostgreSQL shard family"
      end
      unless normalized_shell(deep['if']) == normalized_shell(expected_if)
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} must run for manual/direct proof and fail closed on routing ambiguity"
      end
      unless deep['name'] == "deep-postgres-#{major}-conformance-campaign"
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} must not reuse required context names"
      end
      if deep.key?('services')
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} canonical merge must not provision PostgreSQL"
      end
      steps = Array(deep['steps'])
      download = steps.find do |step|
        step.is_a?(Hash) &&
          step.dig('with', 'pattern') == "conformance-shard-postgres-#{major}-*"
      end
      unless download.is_a?(Hash) &&
             download.dig('with', 'path') == 'target/m5-campaign-shards' &&
             download.dig('with', 'merge-multiple') == true
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} must download and merge the complete shard artifact family"
      end
      body = steps.filter_map { |step| step.is_a?(Hash) ? step['run'] : nil }.join("\n")
      required_tokens = [
        "./tests/fixtures/conformance/verify-ci-contract.sh .github/workflows/m5-conformance.yml",
        "bash ./tests/fixtures/conformance/merge-ci-campaign.sh #{major} 2 target/m5-campaign-shards"
      ]
      missing = required_tokens.reject { |token| body.include?(token) }
      unless missing.empty?
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} is missing canonical merge commands: #{missing.join(', ')}"
      end
      upload = steps.find do |step|
        step.is_a?(Hash) && step.dig('with', 'name') == "conformance-campaign-postgres-#{major}"
      end
      unless upload.is_a?(Hash) &&
             upload.dig('with', 'path') == 'target/m5-campaigns/conformance-campaign.json' &&
             upload.dig('with', 'if-no-files-found') == 'error'
        violations << "#{M5_CONFORMANCE_WORKFLOW} #{job_id} must retain one canonical report"
      end
    end

    emitter = jobs[M5_CONFORMANCE_CONTEXT_JOB]
    unless emitter.is_a?(Hash)
      violations << "#{M5_CONFORMANCE_WORKFLOW} must declare #{M5_CONFORMANCE_CONTEXT_JOB}"
    else
      violations << "#{M5_CONFORMANCE_WORKFLOW} context emitter must remain required" unless job_policy(policy, M5_CONFORMANCE_WORKFLOW, M5_CONFORMANCE_CONTEXT_JOB).first == 'required'
      expected_needs = [CAMPAIGN_ROUTE_JOB] + M5_CONFORMANCE_DEEP_JOBS.values
      unless normalize_needs(emitter).sort == expected_needs.sort
        violations << "#{M5_CONFORMANCE_WORKFLOW} context emitter must depend on routing and both per-PostgreSQL canonical proofs"
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
      env = Array(emitter['steps']).find { |step| step.is_a?(Hash) && step['name'] == 'Emit required conformance context' }&.fetch('env', {})
      expected_bindings = {
        'POSTGRES' => '${{ matrix.postgres }}',
        'DEEP_15_RESULT' => '${{ needs.conformance-deep-15.result }}',
        'DEEP_18_RESULT' => '${{ needs.conformance-deep-18.result }}'
      }
      expected_bindings.each do |key, value|
        unless env[key] == value
          violations << "#{M5_CONFORMANCE_WORKFLOW} context emitter must bind #{key} to #{value}"
        end
      end
      emitter_run = Array(emitter['steps']).filter_map { |step| step.is_a?(Hash) ? step['run'] : nil }.join("\n")
      required_tokens = [
        'M5 conformance is deferred until the pull request is ready for review',
        'ROUTE_RESULT',
        'CLASSIFICATION_OUTCOME',
        'DIRECT_REQUIRED',
        'case "$POSTGRES" in',
        'DEEP_RESULT="$DEEP_15_RESULT"',
        'DEEP_RESULT="$DEEP_18_RESULT"',
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

  def quality_parallel_contract(root:, policy:, producer_summary:)
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
          'cargo test --workspace --all-features --lib',
          'mode=full',
          'mode=docs-only',
          'cargo clippy -p oxide-batch-xtask --all-targets --all-features --message-format=json --'
        ]
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

    QUALITY_INTEGRATION_JOBS.each_with_index do |job_id, index|
      component_specs[job_id] = {
        'name' => "quality-integration-#{index}-internal",
        'tokens' => ["python3 #{QUALITY_INTEGRATION_SHARD_SCRIPT} #{index} 4"]
      }
    end

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

    fast_job = jobs['quality-fast']
    fast_steps = Array(fast_job['steps']).select { |step| step.is_a?(Hash) }
    fast_resolver = fast_steps.find { |step| step['id'] == 'fast-evidence' }
    unless fast_resolver.is_a?(Hash)
      violations << "#{QUALITY_WORKFLOW}#quality-fast must keep id fast-evidence resolver"
    else
      resolver_tokens = [
        'echo "mode=none" >> "$GITHUB_OUTPUT"',
        'mode=full',
        'mode=docs-only',
        "step_conclusion 'Documentation-only fast path'",
        "step_conclusion 'Check formatting'",
        "step_conclusion 'Run Clippy'",
        "step_conclusion 'Run workspace unit tests'"
      ]
      missing = resolver_tokens.reject { |token| fast_resolver['run'].to_s.include?(token) }
      violations << "#{QUALITY_WORKFLOW}#quality-fast Fast evidence resolver is missing mode-integrity tokens: #{missing.join(', ')}" unless missing.empty?
    end

    fallback_if = "${{ (github.event_name != 'pull_request' || github.event.pull_request.draft == false) && steps.fast-evidence.outputs.mode != 'full'  && (steps.docs-only-scope.outcome != 'success' || steps.docs-only-scope.outputs.docs_only != 'true') }}"
    ['Check formatting', 'Run Clippy', 'Run workspace unit tests'].each do |name|
      step = fast_steps.find { |candidate| candidate['name'] == name }
      unless step.is_a?(Hash) && normalized_shell(step['if']) == normalized_shell(fallback_if)
        violations << "#{QUALITY_WORKFLOW}#quality-fast #{name.inspect} must locally fall back unless full Fast evidence or trusted PR docs-only proof applies"
      end
    end

    shard_script = Pathname(root).join(QUALITY_INTEGRATION_SHARD_SCRIPT)
    if shard_script.file?
      shard_body = shard_script.read
      shard_tokens = [
        '"cargo", "metadata", "--no-deps", "--format-version", "1"',
        '"test" not in target.get("kind", [])',
        'integration target names must be workspace-unique before name-based sharding',
        'integration shard partition is not an exact one-to-one cover',
        'command = ["cargo", "test", "--workspace", "--all-features"]',
        'command.extend(["--test", name])'
      ]
      missing = shard_tokens.reject { |token| shard_body.include?(token) }
      unless missing.empty?
        violations << "#{QUALITY_INTEGRATION_SHARD_SCRIPT} is missing fail-closed shard contract: #{missing.join(', ')}"
      end
    else
      violations << "#{QUALITY_INTEGRATION_SHARD_SCRIPT} is missing"
    end

    aggregate = jobs[QUALITY_AGGREGATE_JOB]
    unless aggregate.is_a?(Hash)
      return violations + ["#{QUALITY_WORKFLOW} must declare required #{QUALITY_AGGREGATE_JOB} aggregate"]
    end
    expected_quality_classification = policy['schema_version'].to_i >= 7 ? 'advisory' : 'required'
    unless job_policy(policy, QUALITY_WORKFLOW, QUALITY_AGGREGATE_JOB).first == expected_quality_classification
      violations << "#{QUALITY_WORKFLOW}##{QUALITY_AGGREGATE_JOB} must be #{expected_quality_classification} under schema v#{policy['schema_version']}"
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
      'INTEGRATION_0_RESULT' => "${{ needs.quality-integration-0.result }}",
      'INTEGRATION_1_RESULT' => "${{ needs.quality-integration-1.result }}",
      'INTEGRATION_2_RESULT' => "${{ needs.quality-integration-2.result }}",
      'INTEGRATION_3_RESULT' => "${{ needs.quality-integration-3.result }}",
      'BIN_DOC_RESULT' => "${{ needs.quality-bin-doc.result }}",
      'CONTRACTS_RESULT' => "${{ needs.quality-contracts.result }}"
    }
    unless result_step['env'] == expected_env
      violations << "#{QUALITY_WORKFLOW}##{QUALITY_AGGREGATE_JOB} must bind every component result exactly"
    end
    result_body = result_step['run'].to_s
    required_tokens = expected_env.keys.map { |key| "\"$#{key}\"" } + [
      'if [ "$result" != "success" ]; then',
      'exit 1'
    ]
    missing = required_tokens.reject { |token| result_body.include?(token) }
    unless missing.empty?
      violations << "#{QUALITY_WORKFLOW}##{QUALITY_AGGREGATE_JOB} is missing fail-closed result checks: #{missing.join(', ')}"
    end

    violations
  end


  def internal_aggregate_policy_contract(policy)
    return [] unless policy['schema_version'].to_i >= 7

    violations = []
    unless policy.fetch('aggregate_gates', []) == []
      violations << 'schema v7 branch aggregate_gates must remain empty; reusable-workflow aggregates belong in internal_aggregates'
    end
    unless policy['internal_aggregates'] == [INTERNAL_POSTGRESQL_AGGREGATE]
      violations << 'schema v7 internal_aggregates must exactly declare the canonical PostgreSQL reusable-workflow aggregate'
    end
    violations
  end

  def internal_aggregate_contract(root:, policy:, producer_summary:)
    violations = internal_aggregate_policy_contract(policy)
    return violations unless policy['schema_version'].to_i >= 7
    return violations unless policy['internal_aggregates'].is_a?(Array)

    gate = policy['internal_aggregates'].find do |entry|
      entry.is_a?(Hash) && entry['context'] == 'postgresql'
    end
    return violations unless gate.is_a?(Hash)

    workflow = gate.dig('producer', 'workflow')
    job_id = gate.dig('producer', 'job')
    docs = producer_summary.fetch('workflow_docs')
    doc = docs[workflow]
    job = doc.is_a?(Hash) && doc['jobs'].is_a?(Hash) ? doc['jobs'][job_id] : nil
    unless job.is_a?(Hash)
      return violations + ['schema v7 internal PostgreSQL aggregate producer is missing']
    end

    unless workflow_event?(doc, 'workflow_call')
      violations << 'schema v7 internal PostgreSQL aggregate must live in a reusable workflow'
    end
    if job_policy(policy, workflow, job_id).first != 'advisory'
      violations << 'schema v7 internal PostgreSQL aggregate producer must remain advisory to repository merge topology'
    end
    violations << 'schema v7 internal PostgreSQL aggregate must emit context postgresql' unless job['name'] == 'postgresql'
    violations << 'schema v7 internal PostgreSQL aggregate must use unconditional always()' unless always_condition?(job['if'])
    unless job['runs-on'] == 'ubuntu-latest' && job['timeout-minutes'] == 5
      violations << 'schema v7 internal PostgreSQL aggregate must use ubuntu-latest with timeout-minutes: 5'
    end
    unless job['permissions'] == AGGREGATE_PRODUCER_PERMISSIONS
      violations << "schema v7 internal PostgreSQL aggregate must keep #{AGGREGATE_PRODUCER_PERMISSIONS.inspect}"
    end

    expected_needs = normalize_needs(job).sort
    expanded_members = {}
    expected_needs.each do |member_job_id|
      member_job = doc.dig('jobs', member_job_id)
      unless member_job.is_a?(Hash)
        violations << "schema v7 internal PostgreSQL aggregate references missing member job #{member_job_id}"
        next
      end
      unless job_policy(policy, workflow, member_job_id).first == 'advisory'
        violations << "schema v7 internal PostgreSQL aggregate member #{member_job_id} must remain advisory to repository merge topology"
      end
      begin
        required_job_contexts(member_job_id, member_job).each do |context|
          if expanded_members.key?(context)
            violations << "schema v7 internal PostgreSQL aggregate context #{context} is emitted by multiple member jobs"
          else
            expanded_members[context] = member_job_id
          end
        end
      rescue StandardError => e
        violations << "schema v7 internal PostgreSQL aggregate cannot expand #{member_job_id}: #{e.message}"
      end
    end

    declared_members = gate['members'].sort
    actual_members = expanded_members.keys.sort
    unless declared_members == actual_members
      violations << "schema v7 internal PostgreSQL aggregate member inventory mismatch: expected #{actual_members.inspect}, declared #{declared_members.inspect}"
    end

    steps = Array(job['steps']).select { |step| step.is_a?(Hash) }
    unless steps.length == 2
      violations << 'schema v7 internal PostgreSQL aggregate must have exactly checkout + evaluator steps'
    end
    checkout = steps[0]
    evaluator = steps[1]
    pins = other_jobs_checkout_pins(doc, job_id)
    unless pins.length == 1 && checkout.is_a?(Hash) && checkout['uses'] == pins.first
      violations << 'schema v7 internal PostgreSQL aggregate checkout must reuse the workflow canonical pinned checkout'
    end
    unless evaluator.is_a?(Hash) &&
           evaluator.dig('env', 'GITHUB_TOKEN').to_s.match?(TOKEN_ENV_EXPR) &&
           normalized_shell(evaluator['run']) == normalized_shell(evaluator_invocation_script('postgresql'))
      violations << 'schema v7 internal PostgreSQL aggregate must invoke the canonical selective-rerun evaluator with github.token'
    end

    evaluator_path = Pathname(root).join(EVALUATOR_SCRIPT)
    if evaluator_path.file?
      evaluator_body = evaluator_path.read
      %w[internal_aggregates aggregate_gates schema_version].each do |token|
        violations << "#{EVALUATOR_SCRIPT} must preserve v7/v6 aggregate catalog separation token #{token.inspect}" unless evaluator_body.include?(token)
      end
    else
      violations << "#{EVALUATOR_SCRIPT} is missing"
    end

    violations
  end

  def repository_authority_members(producer_summary:, aggregates:)
    contexts = producer_summary.fetch('required_contexts').dup
    sources = producer_summary.fetch('context_sources').dup

    aggregates.each do |gate|
      contexts = ((contexts - gate.fetch('members')) + [gate.fetch('context')]).uniq
      sources[gate.fetch('context')] = {
        'kind' => 'aggregate',
        'workflow' => gate.dig('producer', 'workflow'),
        'job' => gate.dig('producer', 'job')
      }
    end

    contexts.sort.map do |context|
      source = sources[context]
      {
        'context' => context,
        'workflow' => source.is_a?(Hash) ? source['workflow'] : nil
      }
    end
  end

  def repository_merge_gate_contract(policy:, producer_summary:, aggregates:)
    violations = []
    gate = policy['repository_merge_gate']
    unless gate.is_a?(Hash)
      return ['schema v6 policy must declare repository_merge_gate'], nil
    end

    context = gate['context']
    state = gate['state']
    producer = gate['producer']
    members = gate['members']

    violations << "repository merge gate context must be #{REPOSITORY_MERGE_GATE_CONTEXT.inspect}" unless context == REPOSITORY_MERGE_GATE_CONTEXT
    unless REPOSITORY_MERGE_GATE_STATES.include?(state)
      violations << "repository merge gate has unsupported state #{state.inspect}"
    end
    unless producer == {'workflow' => REPOSITORY_MERGE_GATE_WORKFLOW, 'job' => REPOSITORY_MERGE_GATE_JOB}
      violations << 'repository merge gate producer must be the audited pr-labeler merge-gate job'
    end

    unless members.is_a?(Array) && !members.empty? && members.all? { |member| member.is_a?(Hash) }
      violations << 'repository merge gate members must be a non-empty object array'
      members = []
    end

    member_contexts = members.filter_map { |member| member['context'] }
    duplicate_contexts = member_contexts.group_by(&:itself).select { |_context, entries| entries.length > 1 }.keys
    violations << "repository merge gate duplicates members: #{duplicate_contexts.sort.join(', ')}" unless duplicate_contexts.empty?

    canonical = repository_authority_members(producer_summary: producer_summary, aggregates: aggregates)
    canonical_contexts = canonical.map { |member| member['context'] }
    unless member_contexts.sort == canonical_contexts.sort
      missing = canonical_contexts - member_contexts
      extra = member_contexts - canonical_contexts
      violations << "repository merge gate member inventory mismatch: missing=#{missing.sort.inspect} extra=#{extra.sort.inspect}"
    end

    canonical_by_context = canonical.to_h { |member| [member['context'], member] }
    members.each do |member|
      context_name = member['context']
      workflow = member['workflow']
      unless context_name.is_a?(String) && !context_name.empty? &&
             workflow.is_a?(String) && workflow.start_with?('.github/workflows/')
        violations << "repository merge gate has malformed member #{member.inspect}"
        next
      end
      expected = canonical_by_context[context_name]
      next unless expected
      expected_workflow = expected['workflow']
      if expected_workflow && workflow != expected_workflow
        violations << "repository merge gate member #{context_name} must bind source workflow #{expected_workflow}, got #{workflow}"
      end
    end

    if state != 'candidate'
      non_active = aggregates.reject { |aggregate| aggregate['state'] == 'active' }.map { |aggregate| aggregate['context'] }
      unless non_active.empty?
        violations << "repository merge gate #{state} requires all child aggregates active: #{non_active.sort.join(', ')}"
      end
    end

    workflow = producer_summary.fetch('workflow_docs')[REPOSITORY_MERGE_GATE_WORKFLOW]
    unless workflow.is_a?(Hash)
      return violations + ["repository merge gate workflow #{REPOSITORY_MERGE_GATE_WORKFLOW} is missing"], gate
    end
    unless pull_request_target_trigger?(workflow)
      violations << 'repository merge gate must execute from protected-base pull_request_target authority'
    end
    present_pr, = event_config(workflow, 'pull_request')
    violations << 'repository merge gate workflow must not also use pull_request' if present_pr
    target_present, target_config = event_config(workflow, 'pull_request_target')
    unless target_present && target_config.is_a?(Hash)
      violations << 'repository merge gate pull_request_target trigger must be explicitly configured'
    else
      types = Array(target_config['types']).map(&:to_s)
      unless types.sort == REPOSITORY_MERGE_GATE_EVENT_TYPES.sort
        violations << "repository merge gate pull_request_target types must exactly match #{REPOSITORY_MERGE_GATE_EVENT_TYPES.sort.inspect}"
      end
    end
    violations << 'repository merge gate host workflow must keep workflow-level permissions empty' unless workflow['permissions'] == {}

    jobs = workflow['jobs']
    job = jobs.is_a?(Hash) ? jobs[REPOSITORY_MERGE_GATE_JOB] : nil
    unless job.is_a?(Hash)
      return violations + ['repository merge gate job is missing'], gate
    end
    violations << 'repository merge gate job must emit exact context merge-gate' unless job['name'] == REPOSITORY_MERGE_GATE_CONTEXT
    violations << 'repository merge gate job must run on ubuntu-latest' unless job['runs-on'] == 'ubuntu-latest'
    violations << 'repository merge gate timeout must remain 20 minutes' unless job['timeout-minutes'] == 20
    violations << 'repository merge gate job cannot be conditional' if job.key?('if')
    violations << 'repository merge gate job cannot use a matrix' if job.key?('strategy')
    violations << 'repository merge gate job cannot continue on error' if job['continue-on-error']

    collisions = producer_summary.fetch('static_job_contexts').select do |entry|
      entry['context'] == REPOSITORY_MERGE_GATE_CONTEXT
    end
    expected_collision = {
      'workflow' => REPOSITORY_MERGE_GATE_WORKFLOW,
      'job' => REPOSITORY_MERGE_GATE_JOB,
      'context' => REPOSITORY_MERGE_GATE_CONTEXT
    }
    unless collisions == [expected_collision]
      labels = collisions.map { |entry| "#{entry['workflow']}##{entry['job']}" }.sort
      violations << "repository merge gate context must have exactly one canonical producer; got #{labels.join(', ')}"
    end

    unless job['permissions'] == REPOSITORY_MERGE_GATE_PERMISSIONS
      violations << "repository merge gate job must keep exact read-only permissions #{REPOSITORY_MERGE_GATE_PERMISSIONS.inspect}"
    end

    steps = job['steps']
    unless steps.is_a?(Array) && steps.length == 1
      violations << 'repository merge gate job must contain exactly one inline evaluator step'
      return violations, gate
    end
    step = steps.first
    if step.is_a?(Hash) && step.key?('uses')
      violations << 'repository merge gate must not execute external actions or checkout repository code'
    end

    expected_env = {
      'GITHUB_TOKEN' => '${{ github.token }}',
      'BASE_SHA' => '${{ github.event.pull_request.base.sha }}',
      'HEAD_SHA' => '${{ github.event.pull_request.head.sha }}',
      'PR_NUMBER' => '${{ github.event.pull_request.number }}',
      'PR_DRAFT' => '${{ github.event.pull_request.draft }}'
    }
    unless step.is_a?(Hash) && step['env'] == expected_env
      violations << 'repository merge gate evaluator must bind canonical token/base/head/PR inputs'
    end

    body = step.is_a?(Hash) ? step['run'].to_s : ''
    required_tokens = [
      'contents/.github/merge-gate-policy.json',
      '{"ref": base_sha}',
      'repository_merge_gate',
      'pr.get("base", {}).get("sha") != base_sha',
      'pr.get("head", {}).get("sha") != head_sha',
      'pr.get("base", {}).get("repo", {}).get("full_name") != repository',
      'actions/workflows/{workflow_id}/runs',
      '"event": "pull_request"',
      '"head_sha": head_sha',
      'linked_pr.get("number") == pr_number_int',
      'actions/runs/{run_id}/jobs',
      '"filter": "all"',
      'run_attempt',
      'duplicate member jobs at latest run_attempt',
      'status != "completed"',
      'conclusion != "success"',
      'timed out waiting for exact-head merge authority'
    ]
    if policy['schema_version'].to_i >= 7
      required_tokens.concat([
        'contents/.github/merge-gate-policy.json',
        '{"ref": head_sha}',
        'normalized_policy_without_blob_sets',
        'PR-head merge-gate policy changes trust semantics outside',
        'protected_inventory(policy, "trusted-base")',
        'protected_inventory(head_policy, "PR-head")',
        'base_allowed.issubset(head_allowed)',
        'head_blob not in base_allowed',
        'head_blob not in head_allowed',
        'head_allowed.issubset(base_allowed)',
        'was not pre-admitted by trusted-base policy',
        'workflow replacement cannot admit',
        'additional blobs in the same PR'
      ])
    end

    missing = required_tokens.reject { |token| body.include?(token) }
    unless missing.empty?
      violations << "repository merge gate evaluator is missing fail-closed contract tokens: #{missing.join(', ')}"
    end

    [violations, gate]
  end

  def repository_merge_gate_live_sets(child_live_sets:, gate:)
    return child_live_sets unless gate.is_a?(Hash)

    context = gate['context']
    case gate['state']
    when 'candidate'
      child_live_sets
    when 'cutover'
      (child_live_sets + [[context]]).uniq
    when 'active'
      [[context]]
    else
      child_live_sets
    end
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

    violations << "unsupported policy schema_version #{policy['schema_version'].inspect}" unless [6, 7].include?(policy['schema_version'])
    violations << "ruleset id mismatch: expected #{policy.dig('ruleset', 'id')}, got #{ruleset['id']}" unless ruleset['id'] == policy.dig('ruleset', 'id')
    violations << "ruleset name mismatch: expected #{policy.dig('ruleset', 'name').inspect}, got #{ruleset['name'].inspect}" unless ruleset['name'] == policy.dig('ruleset', 'name')
    violations << 'ruleset is not active' unless ruleset['enforcement'] == 'active'

    producer_violations, producer_summary = producer_inventory(root: root, policy: policy)
    violations.concat(producer_violations)
    aggregate_violations, aggregates = aggregate_inventory(root: root, policy: policy, producer_summary: producer_summary)
    violations.concat(aggregate_violations)

    violations.concat(pr_topology_v7_contract(root: root, policy: policy, producer_summary: producer_summary))
    violations.concat(pr_scope_contract(root: root, policy: policy, producer_summary: producer_summary))
    violations.concat(docs_applicability_contract(policy: policy, producer_summary: producer_summary))
    violations.concat(fast_branch_docs_contract(producer_summary: producer_summary))
    violations.concat(campaign_orchestrator_contract(root: root, policy: policy, producer_summary: producer_summary))
    violations.concat(m5_conformance_routing_contract(policy: policy, producer_summary: producer_summary))
    violations.concat(quality_parallel_contract(root: root, policy: policy, producer_summary: producer_summary))
    violations.concat(internal_aggregate_contract(root: root, policy: policy, producer_summary: producer_summary))
    repository_gate_violations, repository_gate = repository_merge_gate_contract(policy: policy, producer_summary: producer_summary, aggregates: aggregates)
    violations.concat(repository_gate_violations)
    violations.concat(post_main_contract(policy: policy, producer_summary: producer_summary))

    required_contexts = producer_summary.fetch('required_contexts') + aggregates.map { |gate| gate['context'] }
    pending = policy.fetch('pending_ruleset_contexts', [])
    known_pending_contexts = required_contexts + [repository_gate&.dig('context')].compact
    unknown_pending = pending - known_pending_contexts
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

    if repository_gate.is_a?(Hash)
      context = repository_gate['context']
      case repository_gate['state']
      when 'candidate', 'cutover'
        violations << "#{repository_gate['state']} repository merge gate #{context} must be pending ruleset promotion" unless pending.include?(context)
      when 'active'
        violations << "active repository merge gate #{context} cannot remain pending ruleset promotion" if pending.include?(context)
      end
    end

    child_live = accepted_live_context_sets(
      required_contexts: required_contexts,
      aggregates: aggregates,
      pending: pending
    )
    accepted_live = repository_merge_gate_live_sets(
      child_live_sets: child_live,
      gate: repository_gate
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
      'internal_aggregates' => policy.fetch('internal_aggregates', []),
      'repository_merge_gate' => repository_gate,
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
