#!/usr/bin/env python3
"""Read-only advisory PR risk evaluation. Never a required merge authority.

Run only from a trusted default-branch checkout. PR-head files are DATA;
no checkout, import, shell execution, or self-reported PR checks are trusted.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import unittest
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from validate_actions_security import check_workflow

SHA = re.compile(r"^[0-9a-f]{40}$")
SAFE_REPO = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
WORKFLOW_PREFIX = ".github/workflows/"
KNOWN_STATUSES = frozenset({"added", "modified", "removed", "renamed"})
DOC_ROOT = frozenset({
    "README.md", "AGENTS.md", "CHANGELOG.md", "CODE_OF_CONDUCT.md",
    "CONTRIBUTING.md",
})
# Security guidance is a policy surface, not ordinary editorial documentation.
SECURITY_POLICY_DOCS = frozenset({
    "SECURITY.md", "docs/engineering/actions-security.md",
})
# CI contract tests validate the trust boundary rather than batch semantics.
CI_POLICY_TESTS = frozenset({"xtask/tests/merge_gate_policy.rs"})
CORE_MARKERS = (
    "checkpoint", "recovery", "transaction", "repository", "postgres",
    "migration", "scheduler", "retry", "partition", "repeat",
)


class ContractError(ValueError):
    pass


def sha(value: object, label: str) -> str:
    if not isinstance(value, str) or SHA.fullmatch(value) is None:
        raise ContractError(f"{label} must be an exact lowercase 40-hex SHA")
    return value


def path(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value.startswith("/")
        or "\\" in value
        or "//" in value
        or any(part in ("", ".", "..") for part in value.split("/"))
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ContractError("unsafe or invalid changed-file path")
    return value


def classify(filename: str) -> set[str]:
    if filename in SECURITY_POLICY_DOCS:
        return {"docs", "ci_security"}
    if filename in CI_POLICY_TESTS:
        return {"ci_security"}
    # These documents are authoritative policy/evidence inputs, not just prose.
    if filename == "docs/engineering/retained-evidence-policy.json":
        return {"ci_security", "evidence_provenance"}
    if filename == "docs/engineering/dependency-policy.md":
        return {"docs", "dependencies"}
    if filename.startswith("docs/engineering/campaigns/"):
        return {"docs", "evidence_provenance"}
    if filename.startswith(".github/"):
        return {"ci_security"}
    if filename in {"Cargo.toml", "Cargo.lock", "deny.toml", "rust-toolchain.toml"}:
        return {"dependencies", "core"}
    if filename in DOC_ROOT or filename.startswith("docs/"):
        return {"docs"}
    if any(part in filename.lower() for part in CORE_MARKERS):
        return {"core", "database_recovery"}
    # Unrecognized paths are deliberately not classified as harmless.
    return {"core"}


def evaluate(
    pr: dict, files: list[dict], workflow_texts: dict[str, str],
    *, repository: str, trusted_base: str, expected_head: str,
) -> dict:
    if not isinstance(repository, str) or not SAFE_REPO.fullmatch(repository):
        raise ContractError("invalid repository identity")
    sha(trusted_base, "trusted_base")
    sha(expected_head, "expected_head")
    if trusted_base == expected_head:
        raise ContractError("base/head identity unexpectedly identical")
    if not isinstance(pr, dict) or not isinstance(files, list):
        raise ContractError("invalid PR metadata shape")
    if pr.get("base", {}).get("repo", {}).get("full_name") != repository:
        raise ContractError("PR base repository mismatch")
    if pr.get("base", {}).get("ref") != "main":
        raise ContractError("expected target branch main")
    if pr.get("base", {}).get("sha") != trusted_base:
        raise ContractError("stale trusted base SHA")
    if pr.get("head", {}).get("sha") != expected_head:
        raise ContractError("stale or spoofed PR head SHA")
    if pr.get("state") != "open" or pr.get("draft") is not False:
        raise ContractError("PR must be open and ready for review")
    count = pr.get("changed_files")
    if type(count) is not int or not 1 <= count <= 300 or len(files) != count:
        raise ContractError("incomplete, oversized, or inconsistent changed-file inventory")

    changed: set[str] = set()
    workflow_to_scan: set[str] = set()
    for item in files:
        if not isinstance(item, dict):
            raise ContractError("invalid changed-file record")
        status = item.get("status")
        if status not in KNOWN_STATUSES:
            raise ContractError("unsupported changed-file status")
        current = path(item.get("filename"))
        if current in changed:
            raise ContractError("duplicate changed path")
        changed.add(current)
        if status == "renamed":
            previous = path(item.get("previous_filename"))
            if previous == current:
                raise ContractError("rename source equals destination")
            changed.add(previous)
        elif item.get("previous_filename") not in (None, ""):
            raise ContractError("unexpected previous_filename")
        if current.startswith(WORKFLOW_PREFIX) and status != "removed":
            if not current.endswith((".yml", ".yaml")):
                raise ContractError("non-YAML workflow path")
            workflow_to_scan.add(current)

    if set(workflow_texts) != workflow_to_scan:
        raise ContractError("missing or unexpected PR-head workflow contents")
    static_violations: list[str] = []
    for filename in sorted(workflow_to_scan):
        content = workflow_texts[filename]
        if not isinstance(content, str) or not content.strip():
            raise ContractError("empty or non-text PR-head workflow")
        static_violations.extend(check_workflow(Path(filename), content))

    classes: set[str] = set()
    for filename in changed:
        classes.update(classify(filename))
    required: set[str] = set()
    if classes == {"docs"}:
        required.add("documentation_checks")
    if "ci_security" in classes:
        required.update({"trusted_static_policy_review", "independent_security_review"})
    if "dependencies" in classes:
        required.add("dependency_supply_chain")
    if "evidence_provenance" in classes:
        required.add("evidence_provenance")
    if "core" in classes:
        required.update({"rust_fast_and_unit", "integration_regression"})
    if "database_recovery" in classes:
        required.add("postgres_15_18_and_recovery")
    if not required:
        raise ContractError("empty risk plan")
    recommendation = (
        "BLOCK_STATIC_VIOLATIONS" if static_violations
        else "REQUIRE_INDEPENDENT_REVIEW" if "ci_security" in classes
        else "ADVISORY_ROUTING_ONLY"
    )
    return {
        "schema": "oxide-shadow-plan-v1",
        "authority": "ADVISORY_ONLY_NOT_A_MERGE_CHECK",
        "repository": repository,
        "base_sha": trusted_base,
        "head_sha": expected_head,
        "changed_count": len(files),
        "risk_classes": sorted(classes),
        "recommended_checks": sorted(required),
        "workflow_static_violations": sorted(static_violations),
        "recommendation": recommendation,
    }


class GitHubReadOnly:
    def __init__(self, repository: str, token: str):
        if SAFE_REPO.fullmatch(repository) is None or not token:
            raise ContractError("missing API repository or token")
        self.repository = repository
        self.token = token
        self.prefix = f"https://api.github.com/repos/{repository}"

    def get(self, route: str, *, raw: bool = False):
        request = urllib.request.Request(
            self.prefix + "/" + route,
            headers={
                "Authorization": "Bearer " + self.token,
                "Accept": (
                    "application/vnd.github.raw+json" if raw
                    else "application/vnd.github+json"
                ),
                "User-Agent": "oxide-batch-shadow-ci-policy",
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                body = response.read(1_048_577)
        except (OSError, urllib.error.HTTPError) as exc:
            raise ContractError("GitHub API read failed") from exc
        if len(body) > 1_048_576:
            raise ContractError("GitHub API response exceeds size limit")
        try:
            result = body.decode("utf-8")
            return result if raw else json.loads(result)
        except (UnicodeError, ValueError) as exc:
            raise ContractError("invalid GitHub API response") from exc


def inspect(api: GitHubReadOnly, number: int, base: str, head: str) -> dict:
    if type(number) is not int or number < 1:
        raise ContractError("invalid PR number")
    pr = api.get(f"pulls/{number}")
    if not isinstance(pr, dict) or pr.get("number") != number:
        raise ContractError("PR metadata identity mismatch")
    changed_files = pr.get("changed_files")
    if type(changed_files) is not int or not 1 <= changed_files <= 300:
        raise ContractError("invalid PR file count")
    files = []
    for page in range(1, 5):
        batch = api.get(f"pulls/{number}/files?per_page=100&page={page}")
        if not isinstance(batch, list):
            raise ContractError("invalid PR files API response")
        files.extend(batch)
        if len(batch) < 100:
            break
    if len(files) != changed_files:
        raise ContractError("PR changed-file pagination mismatch")
    texts = {}
    for item in files:
        if not isinstance(item, dict):
            raise ContractError("invalid file record")
        filename = path(item.get("filename"))
        if filename.startswith(WORKFLOW_PREFIX) and item.get("status") != "removed":
            encoded = urllib.parse.quote(filename, safe="/")
            texts[filename] = api.get(f"contents/{encoded}?ref={head}", raw=True)
    # PR metadata may change while the file pages and head-source texts load.
    # Re-read identity to prevent a stale-head advisory plan being presented.
    latest = api.get(f"pulls/{number}")
    if not isinstance(latest, dict) or any(
        latest.get(key) != pr.get(key)
        for key in ("base", "head", "changed_files", "state", "draft")
    ):
        raise ContractError("PR identity changed during advisory inspection")
    return evaluate(
        latest, files, texts, repository=api.repository,
        trusted_base=base, expected_head=head,
    )


class ShadowTests(unittest.TestCase):
    BASE = "a" * 40
    HEAD = "b" * 40
    REPO = "owner/oxide-batch"

    def pr(self, count=1):
        return {
            "number": 42, "state": "open", "draft": False,
            "changed_files": count,
            "base": {"sha": self.BASE, "ref": "main",
                     "repo": {"full_name": self.REPO}},
            "head": {"sha": self.HEAD},
            # Intentionally ignore any self-reported checks or labels.
            "checks": [{"name": "merge-gate", "conclusion": "success"}],
        }

    def plan(self, names, *, pr=None, texts=None):
        files = [{"filename": name, "status": "modified"} for name in names]
        return evaluate(
            pr or self.pr(len(files)), files, texts or {},
            repository=self.REPO, trusted_base=self.BASE, expected_head=self.HEAD,
        )

    def test_sensitive_evidence_policy_is_not_docs_only(self):
        p = self.plan(["docs/engineering/retained-evidence-policy.json"])
        self.assertEqual(p["recommendation"], "REQUIRE_INDEPENDENT_REVIEW")
        self.assertIn("evidence_provenance", p["recommended_checks"])

    def test_sensitive_dependency_docs_require_supply_chain(self):
        p = self.plan(["docs/engineering/dependency-policy.md"])
        self.assertIn("dependency_supply_chain", p["recommended_checks"])

    def test_campaign_docs_require_evidence(self):
        p = self.plan(["docs/engineering/campaigns/decisions.md"])
        self.assertIn("evidence_provenance", p["recommended_checks"])

    def test_security_policy_docs_require_independent_review(self):
        for filename in sorted(SECURITY_POLICY_DOCS):
            with self.subTest(filename=filename):
                p = self.plan([filename])
                self.assertIn("ci_security", p["risk_classes"])
                self.assertIn("trusted_static_policy_review", p["recommended_checks"])
                self.assertIn("independent_security_review", p["recommended_checks"])
                self.assertEqual(p["recommendation"], "REQUIRE_INDEPENDENT_REVIEW")
                self.assertNotIn("postgres_15_18_and_recovery", p["recommended_checks"])

    def test_security_document_rename_cannot_launder_risk(self):
        for source, destination in (
            ("SECURITY.md", "docs/project/security-history.md"),
            ("docs/project/security-history.md", "SECURITY.md"),
            ("docs/engineering/actions-security.md", "docs/engineering/history.md"),
            ("docs/engineering/history.md", "docs/engineering/actions-security.md"),
        ):
            with self.subTest(source=source, destination=destination):
                p = evaluate(
                    self.pr(), [{
                        "filename": destination,
                        "previous_filename": source,
                        "status": "renamed",
                    }], {}, repository=self.REPO,
                    trusted_base=self.BASE, expected_head=self.HEAD,
                )
                self.assertEqual(p["recommendation"], "REQUIRE_INDEPENDENT_REVIEW")

    def test_ci_merge_gate_contract_tests_are_security_not_core(self):
        p = self.plan(["xtask/tests/merge_gate_policy.rs"])
        self.assertEqual(p["risk_classes"], ["ci_security"])
        self.assertEqual(p["recommendation"], "REQUIRE_INDEPENDENT_REVIEW")
        self.assertNotIn("integration_regression", p["recommended_checks"])

    def test_other_editorial_docs_stay_docs_only(self):
        p = self.plan(["docs/product/vision-and-scope.md"])
        self.assertEqual(p["risk_classes"], ["docs"])
        self.assertEqual(p["recommended_checks"], ["documentation_checks"])

    def test_agents_is_docs_only(self):
        self.assertEqual(self.plan(["AGENTS.md"])["recommended_checks"], ["documentation_checks"])

    def test_mid_inspection_head_change_denied(self):
        class MutatingAPI:
            repository = ShadowTests.REPO
            calls = 0

            def get(self, route, *, raw=False):
                if route == "pulls/42":
                    self.calls += 1
                    p = self_pr()
                    if self.calls > 1:
                        p["head"]["sha"] = "c" * 40
                    return p
                if route.startswith("pulls/42/files?"):
                    return [{"filename": "README.md", "status": "modified"}]
                raise AssertionError("unexpected API call")

        self_pr = self.pr
        with self.assertRaisesRegex(ContractError, "identity changed"):
            inspect(MutatingAPI(), 42, self.BASE, self.HEAD)

    def test_docs_narrow(self):
        p = self.plan(["docs/usage.md"])
        self.assertEqual(p["recommended_checks"], ["documentation_checks"])
        self.assertEqual(p["recommendation"], "ADVISORY_ROUTING_ONLY")

    def test_unknown_is_not_docs(self):
        self.assertIn("integration_regression", self.plan(["unknown.ext"])["recommended_checks"])

    def test_recovery_requires_pg(self):
        self.assertIn("postgres_15_18_and_recovery", self.plan(["src/checkpoint.rs"])["recommended_checks"])

    def test_policy_requires_independent_review(self):
        p = self.plan([".github/merge-gate-policy.json"])
        self.assertEqual(p["recommendation"], "REQUIRE_INDEPENDENT_REVIEW")

    def test_stale_head_denied(self):
        pr = self.pr()
        pr["head"]["sha"] = "c" * 40
        with self.assertRaises(ContractError):
            self.plan(["README.md"], pr=pr)

    def test_stale_base_denied(self):
        pr = self.pr()
        pr["base"]["sha"] = "c" * 40
        with self.assertRaises(ContractError):
            self.plan(["README.md"], pr=pr)

    def test_wrong_repo_denied(self):
        pr = self.pr()
        pr["base"]["repo"]["full_name"] = "attacker/repo"
        with self.assertRaises(ContractError):
            self.plan(["README.md"], pr=pr)

    def test_missing_file_denied(self):
        with self.assertRaises(ContractError):
            self.plan(["README.md"], pr=self.pr(2))

    def test_unknown_status_denied(self):
        with self.assertRaises(ContractError):
            evaluate(self.pr(), [{"filename": "README.md", "status": "copied"}], {},
                     repository=self.REPO, trusted_base=self.BASE, expected_head=self.HEAD)

    def test_unsafe_path_denied(self):
        for filename in ("../secret", "/tmp/safe", "docs//x", "docs\\x"):
            with self.subTest(filename=filename), self.assertRaises(ContractError):
                self.plan([filename])

    def test_rename_checks_old_path(self):
        result = evaluate(
            self.pr(), [{"filename": "docs/new.md", "previous_filename": ".github/workflows/pr-ci.yml", "status": "renamed"}],
            {}, repository=self.REPO, trusted_base=self.BASE, expected_head=self.HEAD,
        )
        self.assertIn("ci_security", result["risk_classes"])

    def test_invalid_rename_denied(self):
        with self.assertRaises(ContractError):
            evaluate(self.pr(), [{"filename": "docs/new.md", "status": "renamed"}],
                     {}, repository=self.REPO, trusted_base=self.BASE, expected_head=self.HEAD)

    def test_missing_workflow_text_denied(self):
        with self.assertRaises(ContractError):
            self.plan([".github/workflows/ci.yml"])

    def test_unpinned_workflow_action_flagged(self):
        wf = "name: attack\non: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n"
        result = self.plan([".github/workflows/ci.yml"], texts={".github/workflows/ci.yml": wf})
        self.assertEqual(result["recommendation"], "BLOCK_STATIC_VIOLATIONS")

    def test_target_checkout_flagged(self):
        wf = "on: pull_request_target\njobs:\n  x:\n    steps:\n      - uses: actions/checkout@" + "1" * 40 + "\n        with:\n          persist-credentials: false\n"
        result = self.plan([".github/workflows/evil.yml"], texts={".github/workflows/evil.yml": wf})
        self.assertTrue(any("pull_request_target" in s for s in result["workflow_static_violations"]))

    def test_fake_green_not_used_as_evidence(self):
        p = self.plan([".github/workflows/new.yml"], texts={".github/workflows/new.yml": "name: safe\non: push\n"})
        self.assertEqual(p["authority"], "ADVISORY_ONLY_NOT_A_MERGE_CHECK")
        self.assertNotEqual(p["recommendation"], "PASS")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--repository")
    parser.add_argument("--pr", type=int)
    parser.add_argument("--trusted-base")
    parser.add_argument("--expected-head")
    args = parser.parse_args()

    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(ShadowTests)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1

    try:
        if os.environ.get("GITHUB_REF") != "refs/heads/main":
            raise ContractError("shadow evaluator must run from protected main")
        if os.environ.get("GITHUB_SHA") != args.trusted_base:
            raise ContractError("workflow execution SHA differs from trusted base")
        api = GitHubReadOnly(args.repository, os.environ.get("GH_TOKEN", ""))
        result = inspect(
            api, args.pr,
            sha(args.trusted_base, "trusted_base"),
            sha(args.expected_head, "expected_head"),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (ContractError, KeyError, TypeError) as exc:
        print(f"shadow evaluator: UNVERIFIED: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
