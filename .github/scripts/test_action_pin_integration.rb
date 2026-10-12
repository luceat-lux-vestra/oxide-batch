#!/usr/bin/env ruby
# frozen_string_literal: true

# Exercise the deployed independent Ruby authority, including the *actual*
# protected-base Python subprocess. Upstream GitHub API is mocked only in a
# temporary, child-process-only sitecustomize module; no network is needed.
require 'json'
require 'fileutils'
require 'minitest/autorun'
require 'pathname'
require 'tmpdir'
require_relative 'verify-merge-gates'

class TrustedActionPinIntegrationTest < Minitest::Test
  WORKFLOW = '.github/workflows/codeql.yml'
  HELPER = '.github/scripts/action_pin_only.py'
  NEXT_SHA = '2' * 40
  WRONG_SHA = '3' * 40

  def with_trusted_fixture
    Dir.mktmpdir('oxide-pin-authority-test-') do |dir|
      root = Pathname(dir)
      trusted = root.join('.merge-gate-trusted-base')
      helper_bytes = Pathname(__dir__).join('action_pin_only.py').binread
      workflow_bytes = Pathname(__dir__).join('..', 'workflows', 'codeql.yml').binread
      assert_equal MergeGateVerifier::ACTION_PIN_ONLY_HELPER_BLOB,
                   MergeGateVerifier.git_blob_sha(helper_bytes),
                   'test cannot substitute a different trusted-base helper'
      [trusted.join(HELPER), trusted.join(WORKFLOW), root.join(WORKFLOW)].each do |p|
        FileUtils.mkdir_p(p.dirname)
      end
      trusted.join(HELPER).binwrite(helper_bytes)
      trusted.join(WORKFLOW).binwrite(workflow_bytes)
      root.join(WORKFLOW).binwrite(workflow_bytes)
      yield root, trusted
    end
  end

  def patch_codeql(root)
    target = root.join(WORKFLOW)
    before = target.binread
    old = '24c54180a607b1449ed407dd24f251e4e9147c8d # v4.38.3'
    replacement = "#{NEXT_SHA} # v4.38.4"
    assert_equal 4, before.scan(old).size, 'the audited CodeQL action layout changed'
    target.binwrite(before.gsub(old, replacement))
  end

  def with_token
    previous = ENV['GH_TOKEN']
    ENV['GH_TOKEN'] = 'unit-test-token'
    yield
  ensure
    previous.nil? ? ENV.delete('GH_TOKEN') : ENV['GH_TOKEN'] = previous
  end

  def with_stub_upstream(root, matching: true, published: true)
    previous = ENV['PYTHONPATH']
    root.join('sitecustomize.py').write(<<~PY)
      import io
      import json
      import urllib.request
      import urllib.error

      def _mock_github_api(request, timeout=None):
          address = request.full_url
          if address.endswith('/repos/github/codeql-action/releases/tags/v4.38.4'):
              body = {"draft": False, "prerelease": False, "tag_name": "v4.38.4"} if #{published ? 'True' : 'False'} else {"draft": True, "prerelease": False, "tag_name": "v4.38.4"}
          elif address.endswith('/repos/github/codeql-action/git/ref/tags/v4.38.4'):
              body = {"ref": "refs/tags/v4.38.4", "object": {"type": "commit", "sha": "#{matching ? NEXT_SHA : WRONG_SHA}"}}
          else:
              raise urllib.error.URLError("unrecognized mock path")
          return io.BytesIO(json.dumps(body).encode('utf-8'))

      urllib.request.urlopen = _mock_github_api
    PY
    ENV['PYTHONPATH'] = root.to_s
    yield
  ensure
    previous.nil? ? ENV.delete('PYTHONPATH') : ENV['PYTHONPATH'] = previous
  end

  def test_identical_trusted_baseline_allowed_without_upstream_lookup
    with_trusted_fixture do |root, _|
      previous = ENV.delete('GH_TOKEN')
      begin
        assert MergeGateVerifier.trusted_action_patch?(root: root.to_s, workflow: WORKFLOW)
      ensure
        ENV['GH_TOKEN'] = previous if previous
      end
    end
  end

  def test_real_python_subprocess_accepts_exact_patch_with_verified_mock_release
    with_trusted_fixture do |root, _|
      patch_codeql(root)
      with_token do
        with_stub_upstream(root) do
          assert MergeGateVerifier.trusted_action_patch?(root: root.to_s, workflow: WORKFLOW)
        end
      end
    end
  end

  def test_real_python_subprocess_rejects_forged_release_commit
    with_trusted_fixture do |root, _|
      patch_codeql(root)
      with_token do
        with_stub_upstream(root, matching: false) do
          refute MergeGateVerifier.trusted_action_patch?(root: root.to_s, workflow: WORKFLOW)
        end
      end
    end
  end

  def test_real_python_subprocess_rejects_draft_release
    with_trusted_fixture do |root, _|
      patch_codeql(root)
      with_token do
        with_stub_upstream(root, published: false) do
          refute MergeGateVerifier.trusted_action_patch?(root: root.to_s, workflow: WORKFLOW)
        end
      end
    end
  end

  def test_structural_permission_change_rejected_even_with_legitimate_release
    with_trusted_fixture do |root, _|
      patch_codeql(root)
      target = root.join(WORKFLOW)
      target.binwrite(target.binread.sub('permissions: {}', 'permissions: write-all'))
      with_token do
        with_stub_upstream(root) do
          refute MergeGateVerifier.trusted_action_patch?(root: root.to_s, workflow: WORKFLOW)
        end
      end
    end
  end

  def test_missing_token_rejects_changed_workflow
    with_trusted_fixture do |root, _|
      patch_codeql(root)
      previous = ENV.delete('GH_TOKEN')
      begin
        refute MergeGateVerifier.trusted_action_patch?(root: root.to_s, workflow: WORKFLOW)
      ensure
        ENV['GH_TOKEN'] = previous if previous
      end
    end
  end

  def test_unrecognized_workflow_is_not_eligible
    with_trusted_fixture do |root, _|
      refute MergeGateVerifier.trusted_action_patch?(root: root.to_s,
        workflow: '.github/workflows/pr-labeler.yml')
    end
  end

  def test_modified_protected_base_helper_rejects_even_unchanged_workflow
    with_trusted_fixture do |root, trusted|
      trusted.join(HELPER).binwrite(trusted.join(HELPER).binread + "\n# forgery\n")
      refute MergeGateVerifier.trusted_action_patch?(root: root.to_s, workflow: WORKFLOW)
    end
  end

  def test_missing_protected_base_workflow_denied
    with_trusted_fixture do |root, trusted|
      trusted.join(WORKFLOW).delete
      refute MergeGateVerifier.trusted_action_patch?(root: root.to_s, workflow: WORKFLOW)
    end
  end

  def test_tampered_uses_identity_denied
    with_trusted_fixture do |root, _|
      patch_codeql(root)
      target = root.join(WORKFLOW)
      target.binwrite(target.binread.sub('github/codeql-action/init@', 'attacker/codeql-action/init@'))
      with_token do
        with_stub_upstream(root) do
          refute MergeGateVerifier.trusted_action_patch?(root: root.to_s, workflow: WORKFLOW)
        end
      end
    end
  end

  def test_partial_patch_denied
    with_trusted_fixture do |root, _|
      patch_codeql(root)
      target = root.join(WORKFLOW)
      new_bytes = target.binread
      assert_equal 4, new_bytes.scan(NEXT_SHA).size
      target.binwrite(new_bytes.sub(NEXT_SHA, '24c54180a607b1449ed407dd24f251e4e9147c8d').sub(
        '# v4.38.4', '# v4.38.3'))
      with_token do
        with_stub_upstream(root) do
          refute MergeGateVerifier.trusted_action_patch?(root: root.to_s, workflow: WORKFLOW)
        end
      end
    end
  end
end
