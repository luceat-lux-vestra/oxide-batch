#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from unittest import mock
import unittest

ROOT = Path(__file__).resolve().parents[2]


def load(name, filename):
    path = ROOT / ".github" / "scripts" / filename
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


RUNNER = load("hardening_drift_runner", "run-hardening-drift-audit.py")
REPORTER = load("hardening_drift_reporter", "report-hardening-drift-audit.py")


class Completed:
    def __init__(self, returncode=0, stdout=""):
        self.returncode = returncode
        self.stdout = stdout


class FakeReadClient:
    def __init__(self, responses):
        self.responses = responses

    def get(self, path):
        value = self.responses[path]
        if isinstance(value, Exception):
            raise value
        return value


def ruleset(active=True, tag=False):
    if tag:
        return {
            "enforcement": "active" if active else "disabled",
            "conditions": {"ref_name": {"include": ["refs/tags/v*"]}},
            "rules": [{"type": "deletion"}, {"type": "update"}],
            "bypass_actors": [],
        }
    return {
        "enforcement": "active" if active else "disabled",
        "conditions": {"ref_name": {"include": ["~DEFAULT_BRANCH"]}},
        "rules": [
            {"type": "deletion"},
            {"type": "non_fast_forward"},
            {"type": "required_linear_history"},
            {
                "type": "pull_request",
                "parameters": {
                    "required_approving_review_count": 0,
                    "required_review_thread_resolution": True,
                    "require_extra_approval_for_unattributed_changes": True,
                    "allowed_merge_methods": ["squash"],
                },
            },
            {
                "type": "required_status_checks",
                "parameters": {"strict_required_status_checks_policy": True},
            },
        ],
        "bypass_actors": [],
    }


class RunnerTests(unittest.TestCase):
    def test_classification_precedence(self):
        self.assertEqual("clean", RUNNER.classify([], []))
        self.assertEqual("policy-drift", RUNNER.classify([{"x": 1}], []))
        self.assertEqual(
            "infrastructure-failure",
            RUNNER.classify([{"x": 1}], [{"y": 2}]),
        )

    def test_each_composed_leaf_failure_becomes_policy_drift(self):
        expected = {
            "actions-security",
            "supply-chain-exceptions",
            "release-inventory",
            "release-negative-contract",
            "retained-evidence-policy",
            "retained-evidence-provenance",
        }
        self.assertEqual(expected, {control for control, _ in RUNNER.STATIC_CHECKS})

        for target, target_argv in RUNNER.STATIC_CHECKS:
            with self.subTest(target=target):
                def fake_run(argv):
                    if argv == ["cargo", "fetch", "--locked"]:
                        return Completed()
                    if argv == target_argv:
                        return Completed(1, f"{target} rejected safe fixture")
                    return Completed()

                with mock.patch.object(
                    RUNNER, "reporting_path_violations", return_value=[]
                ):
                    findings, infra = RUNNER.run_static_checks(fake_run)
                self.assertEqual([], infra)
                self.assertIn(target, [entry["control"] for entry in findings])
                self.assertEqual("policy-drift", RUNNER.classify(findings, infra))

    def test_cargo_fetch_failure_is_infrastructure_not_policy_drift(self):
        def fake_run(argv):
            if argv == ["cargo", "fetch", "--locked"]:
                return Completed(1, "network unavailable")
            return Completed()

        with mock.patch.object(
            RUNNER, "reporting_path_violations", return_value=[]
        ):
            findings, infra = RUNNER.run_static_checks(fake_run)
        self.assertEqual([], findings)
        self.assertEqual("cargo-tooling", infra[0]["control"])
        self.assertEqual("infrastructure-failure", RUNNER.classify(findings, infra))

    def test_merge_gate_failure_is_policy_drift(self):
        policy = {"main_ruleset_id": 19905142}
        client = FakeReadClient({"/rulesets/19905142": ruleset()})

        def fake_run(argv):
            self.assertIn("verify-merge-gates.rb", argv[1])
            return Completed(1, "required context mismatch")

        findings, infra = RUNNER.run_merge_gate_check(client, policy, fake_run)
        self.assertEqual([], infra)
        self.assertEqual("merge-gate", findings[0]["control"])

    def test_live_repository_setting_drift_is_detected(self):
        policy = {
            "main_ruleset_id": 19905142,
            "release_tag_ruleset_id": 19905157,
            "controls": [
                {
                    "id": "repository.visibility",
                    "classification": "required",
                    "readback": "repository-api",
                    "expected": "public",
                }
            ],
        }
        client = FakeReadClient({"": {"visibility": "private"}})
        findings, infra = RUNNER.run_live_policy_checks(client, policy)
        self.assertEqual([], infra)
        self.assertEqual("repository.visibility", findings[0]["control"])

    def test_manual_readback_inventory_is_explicit(self):
        policy = json.loads(RUNNER.POLICY_PATH.read_text(encoding="utf-8"))
        manual = {entry["id"] for entry in RUNNER.manual_inventory(policy)}
        required = {
            "actions.default_workflow_permissions",
            "actions.sha_pinning_required",
            "actions.fork_pr_approval_policy",
            "security.dependabot_alerts",
            "security.secret_scanning",
            "codeql.languages",
        }
        self.assertTrue(required <= manual)


class FakeIssueClient:
    def __init__(self, issues=None):
        self.issues = list(issues or [])
        self.comments = []
        self.updates = []
        self.created = []

    def all_issues(self):
        return iter(self.issues)

    def create_issue(self, title, body, labels):
        issue = {"number": 77, "state": "open", "title": title, "body": body, "labels": labels}
        self.issues.append(issue)
        self.created.append(issue)
        return issue

    def update_issue(self, number, **payload):
        self.updates.append((number, payload))
        for issue in self.issues:
            if issue["number"] == number:
                issue.update(payload)
        return {}

    def comment(self, number, body):
        self.comments.append((number, body))
        return {}


class ReporterTests(unittest.TestCase):
    def audit(self, classification):
        return {
            "classification": classification,
            "policy_findings": [],
            "infrastructure_failures": [],
            "manual_readback": [],
        }

    def test_owned_issue_create_update_reopen_and_recovery_close(self):
        client = FakeIssueClient()
        self.assertIn("created", REPORTER.reconcile_issue(client, self.audit("policy-drift"), "run-1"))
        self.assertEqual(1, len(client.created))

        self.assertIn("updated", REPORTER.reconcile_issue(client, self.audit("infrastructure-failure"), "run-2"))
        self.assertEqual(1, len(client.created))

        client.issues[0]["state"] = "closed"
        self.assertIn("updated", REPORTER.reconcile_issue(client, self.audit("policy-drift"), "run-3"))
        self.assertEqual("open", client.updates[-1][1]["state"])

        client.issues[0]["state"] = "open"
        self.assertIn("closed recovered", REPORTER.reconcile_issue(client, self.audit("clean"), "run-4"))
        self.assertEqual("closed", client.updates[-1][1]["state"])

    def test_duplicate_owned_markers_fail_closed(self):
        owned = {"number": 1, "state": "open", "body": REPORTER.MARKER}
        client = FakeIssueClient([dict(owned), {**owned, "number": 2}])
        with self.assertRaises(RuntimeError):
            REPORTER.find_owned_issue(client)

    def test_rendered_issue_body_is_bounded(self):
        audit = {
            "classification": "policy-drift",
            "policy_findings": [{"control": "x", "details": "x" * 50000}],
            "infrastructure_failures": [{"control": "y", "details": "y" * 50000}],
            "manual_readback": [{"id": "z", "rationale": "z" * 50000}],
        }
        self.assertLess(len(REPORTER.render_body(audit, "run")), 65536)


class WorkflowContractTests(unittest.TestCase):
    def test_scheduled_workflow_uses_bounded_job_output_not_evidence_artifacts(self):
        source = (ROOT / ".github" / "workflows" / "hardening-drift-audit.yml").read_text(
            encoding="utf-8"
        )
        self.assertIn('cron: "37 18 * * 1"', source)
        self.assertIn("result: ${{ steps.publish-result.outputs.result }}", source)
        self.assertIn("AUDIT_RESULT: ${{ needs.detect.outputs.result }}", source)
        self.assertIn("--result-json \"$AUDIT_RESULT\"", source)
        self.assertNotIn("actions/upload-artifact@", source)
        self.assertNotIn("actions/download-artifact@", source)
        self.assertNotIn("actions: read", source)

    def test_write_permission_is_isolated_to_report_job(self):
        source = (ROOT / ".github" / "workflows" / "hardening-drift-audit.yml").read_text(
            encoding="utf-8"
        )
        detect = source.split("  detect:\n", 1)[1].split("\n  report:\n", 1)[0]
        report = source.split("\n  report:\n", 1)[1]
        self.assertNotIn("issues: write", detect)
        self.assertEqual(1, report.count("issues: write"))
        self.assertEqual(1, source.count("issues: write"))
        self.assertIn("permissions: {}", source)


if __name__ == "__main__":
    unittest.main()
