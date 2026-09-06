#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = ROOT / ".github" / "repository-settings-policy.json"

STATIC_CHECKS = [
    ("actions-security", [sys.executable, ".github/scripts/validate_actions_security.py"]),
    ("supply-chain-exceptions", [sys.executable, ".github/scripts/validate_supply_chain_exceptions.py"]),
    ("release-inventory", ["cargo", "run", "--locked", "--offline", "--package", "oxide-batch-xtask", "--", "release-crates"]),
    ("release-negative-contract", ["cargo", "test", "--locked", "--offline", "--package", "oxide-batch-xtask", "--test", "release_negative_contract"]),
    ("retained-evidence-policy", ["cargo", "run", "--locked", "--offline", "--package", "oxide-batch-xtask", "--bin", "retained_evidence_policy"]),
    ("retained-evidence-provenance", ["cargo", "run", "--locked", "--offline", "--package", "oxide-batch-xtask", "--", "evidence"]),
]


class ApiFailure(RuntimeError):
    pass


class GitHubReadClient:
    def __init__(self, repository: str, token: str):
        self.base = f"https://api.github.com/repos/{repository}"
        self.headers = {
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "oxide-batch-hardening-drift-audit",
        }

    def get(self, path: str):
        request = urllib.request.Request(self.base + path, headers=self.headers, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                raw = response.read()
                return json.loads(raw) if raw else None
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as exc:
            raise ApiFailure(f"GET {path or '/'} failed: {exc}") from exc


def command(argv):
    return subprocess.run(
        argv, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )


def finding(control: str, details: str):
    return {"control": control, "details": details}


def result(classification, policy_findings, infrastructure_failures, manual_readback):
    return {
        "schema_version": 1,
        "classification": classification,
        "policy_findings": policy_findings,
        "infrastructure_failures": infrastructure_failures,
        "manual_readback": manual_readback,
    }


def classify(policy_findings, infrastructure_failures):
    if infrastructure_failures:
        return "infrastructure-failure"
    if policy_findings:
        return "policy-drift"
    return "clean"


def manual_inventory(policy):
    return [
        {
            "id": control["id"],
            "classification": control["classification"],
            "expected": control["expected"],
            "rationale": control.get("rationale"),
        }
        for control in policy["controls"]
        if control["readback"] == "manual-readback"
    ]


def reporting_path_violations(root=ROOT):
    security = (root / "SECURITY.md").read_text(encoding="utf-8")
    conduct = (root / "CODE_OF_CONDUCT.md").read_text(encoding="utf-8")
    violations = []
    for needle in ("Report a vulnerability", "[SECURITY]", "CODE_OF_CONDUCT.md"):
        if needle not in security:
            violations.append(f"SECURITY.md no longer contains {needle!r}")
    for needle in ("Report a vulnerability", "[CONDUCT]", "SECURITY.md"):
        if needle not in conduct:
            violations.append(f"CODE_OF_CONDUCT.md no longer contains {needle!r}")
    return violations


def run_static_checks(run=command):
    policy_findings = []
    infrastructure = []

    for detail in reporting_path_violations():
        policy_findings.append(finding("reporting-paths", detail))

    cargo_fetch = run(["cargo", "fetch", "--locked"])
    cargo_ready = cargo_fetch.returncode == 0
    if not cargo_ready:
        infrastructure.append(
            finding("cargo-tooling", cargo_fetch.stdout.strip() or "cargo fetch --locked failed")
        )

    for control, argv in STATIC_CHECKS:
        if argv[0] == "cargo" and not cargo_ready:
            continue
        completed = run(argv)
        if completed.returncode != 0:
            policy_findings.append(
                finding(control, completed.stdout.strip() or f"exit {completed.returncode}")
            )
    return policy_findings, infrastructure


def rules_by_type(ruleset):
    return {
        rule.get("type"): rule
        for rule in ruleset.get("rules", [])
        if isinstance(rule, dict)
    }


def ruleset_value(control_id, ruleset):
    rules = rules_by_type(ruleset)
    pr = rules.get("pull_request", {}).get("parameters", {})
    status = rules.get("required_status_checks", {}).get("parameters", {})
    if control_id.endswith(".enforcement"):
        return ruleset.get("enforcement")
    if control_id.endswith(".target_default_branch"):
        return ruleset.get("conditions", {}).get("ref_name", {}).get("include") == ["~DEFAULT_BRANCH"]
    if control_id.endswith(".ref_pattern"):
        return ruleset.get("conditions", {}).get("ref_name", {}).get("include")
    if control_id.endswith(".deletion_protection"):
        return "deletion" in rules
    if control_id.endswith(".non_fast_forward_protection"):
        return "non_fast_forward" in rules
    if control_id.endswith(".required_linear_history"):
        return "required_linear_history" in rules
    if control_id.endswith(".pull_request_required"):
        return "pull_request" in rules
    if control_id.endswith(".allowed_merge_methods"):
        return pr.get("allowed_merge_methods")
    if control_id.endswith(".review_thread_resolution"):
        return pr.get("required_review_thread_resolution")
    if control_id.endswith(".required_approving_review_count"):
        return pr.get("required_approving_review_count")
    if control_id.endswith(".extra_approval_for_unattributed_changes"):
        return pr.get("require_extra_approval_for_unattributed_changes")
    if control_id.endswith(".strict_required_status_checks"):
        return status.get("strict_required_status_checks_policy")
    if control_id.endswith(".bypass_actors"):
        return ruleset.get("bypass_actors", [])
    if control_id.endswith(".update_protection"):
        return "update" in rules
    if control_id == "security.signed_commits":
        return "required_signatures" in rules
    raise KeyError(control_id)


def repository_value(control_id, repository):
    mapping = {
        "repository.visibility": "visibility",
        "repository.default_branch": "default_branch",
    }
    key = mapping[control_id]
    if key not in repository:
        raise ApiFailure(f"repository readback omitted {key!r}")
    return repository[key]


def compare_expected(expected, actual):
    if isinstance(expected, list):
        return sorted(expected) == sorted(actual or [])
    return expected == actual


def run_live_policy_checks(client, policy):
    findings = []
    infrastructure = []
    cache = {}

    def get_cached(name, path):
        if name not in cache:
            cache[name] = client.get(path)
        return cache[name]

    for control in policy["controls"]:
        if control["readback"] == "manual-readback":
            continue
        cid = control["id"]
        try:
            if control["readback"] == "repository-api":
                actual = repository_value(cid, get_cached("repository", ""))
            elif control["readback"] == "main-ruleset-api":
                actual = ruleset_value(
                    cid, get_cached("main-ruleset", f"/rulesets/{policy['main_ruleset_id']}")
                )
            elif control["readback"] == "release-tag-ruleset-api":
                actual = ruleset_value(
                    cid, get_cached("tag-ruleset", f"/rulesets/{policy['release_tag_ruleset_id']}")
                )
            elif control["readback"] == "dependency-graph-api":
                payload = get_cached("dependency-graph", "/dependency-graph/sbom")
                actual = isinstance(payload, dict) and isinstance(payload.get("sbom"), dict)
            elif control["readback"] == "pvr-api":
                payload = get_cached("pvr", "/private-vulnerability-reporting")
                if not isinstance(payload, dict) or "enabled" not in payload:
                    raise ApiFailure("private-vulnerability-reporting returned malformed payload")
                actual = payload["enabled"]
            else:
                raise KeyError(control["readback"])
        except (ApiFailure, KeyError) as exc:
            infrastructure.append(finding(cid, str(exc)))
            continue

        if not compare_expected(control["expected"], actual):
            findings.append(
                finding(cid, f"expected {control['expected']!r}, live readback {actual!r}")
            )
    return findings, infrastructure


def run_merge_gate_check(client, policy, run=command):
    try:
        ruleset = client.get(f"/rulesets/{policy['main_ruleset_id']}")
    except ApiFailure as exc:
        return [], [finding("merge-gate", str(exc))]
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".json", delete=False) as handle:
        json.dump(ruleset, handle)
        path = handle.name
    try:
        completed = run([
            "ruby",
            ".github/scripts/verify-merge-gates.rb",
            ".github/merge-gate-policy.json",
            path,
        ])
    finally:
        Path(path).unlink(missing_ok=True)
    if completed.returncode == 0:
        return [], []
    return [finding("merge-gate", completed.stdout.strip() or "merge gate verifier failed")], []


def run_audit(client, run=command):
    policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    policy_findings, infrastructure = run_static_checks(run)
    merge_findings, merge_infra = run_merge_gate_check(client, policy, run)
    live_findings, live_infra = run_live_policy_checks(client, policy)
    policy_findings.extend(merge_findings)
    policy_findings.extend(live_findings)
    infrastructure.extend(merge_infra)
    infrastructure.extend(live_infra)
    manual = manual_inventory(policy)
    return result(classify(policy_findings, infrastructure), policy_findings, infrastructure, manual)


def synthetic(mode):
    policy = json.loads(POLICY_PATH.read_text(encoding="utf-8"))
    manual = manual_inventory(policy)
    if mode == "clean":
        return result("clean", [], [], manual)
    if mode == "policy-drift":
        return result("policy-drift", [finding("synthetic", "safe synthetic policy drift")], [], manual)
    if mode == "infrastructure-failure":
        return result("infrastructure-failure", [], [finding("synthetic", "safe synthetic readback failure")], manual)
    raise ValueError(mode)


def exit_code_for(classification):
    return {"clean": 0, "policy-drift": 1, "infrastructure-failure": 2}[classification]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--test-mode",
        choices=("live", "clean", "policy-drift", "infrastructure-failure"),
        default="live",
    )
    args = parser.parse_args()
    try:
        if args.test_mode == "live":
            repository = os.environ.get("GITHUB_REPOSITORY")
            token = os.environ.get("GITHUB_TOKEN")
            if not repository or not token:
                raise ApiFailure("GITHUB_REPOSITORY and GITHUB_TOKEN are required for live audit")
            audit = run_audit(GitHubReadClient(repository, token))
        else:
            audit = synthetic(args.test_mode)
    except (ApiFailure, OSError, ValueError, json.JSONDecodeError) as exc:
        audit = result(
            "infrastructure-failure", [], [finding("audit-runner", str(exc))], []
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(audit, indent=2, sort_keys=True))
    return exit_code_for(audit["classification"])


if __name__ == "__main__":
    raise SystemExit(main())
