#!/usr/bin/env python3
"""Selective read-only CodeQL PR runtime compatibility contract for oxide-batch."""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import re
import sys
import tempfile
from collections import Counter
from pathlib import Path, PurePosixPath

ROOT = Path(__file__).resolve().parents[2]
SHA = re.compile(r"[0-9a-f]{40}\Z")
ACTION_LINE = re.compile(
    r"^\s*uses:\s*github/codeql-action/(?P<action>init|analyze)@"
    r"(?P<sha>[0-9a-f]{40})(?:\s+#.*)?\s*$"
)
SIMPLE_STATUSES = frozenset({"added", "modified", "removed"})
MOVE_STATUSES = frozenset({"renamed", "copied"})
# A production CodeQL workflow change must be independently classifiable from a
# trusted base. No exception for a particular future Git blob is permitted.
RUST_ARM_RUNNER_LINE = re.compile(r"^(?P<indent> {12})os: (?P<runner>ubuntu-\d{2}\.\d{2}-arm)\s*$")
RELEVANT_PATHS = frozenset({
    ".github/workflows/codeql.yml",
    ".github/workflows/pr-ci.yml",
    ".github/scripts/trusted-codeql-runtime-validation.py",
    ".github/scripts/check_codeql_sarif.py",
    ".github/scripts/check_release_codeql.py",
    ".github/scripts/test_release_codeql_policy.py",
    ".github/scripts/validate_actions_security.py",
    ".github/scripts/test_validate_actions_security.py",
})

def reject(message: str) -> None:
    raise ValueError(message)

def safe_path(value: object) -> str:
    if not isinstance(value, str) or not value or value.startswith("/") or "\\" in value:
        reject(f"unsafe repository path: {value!r}")
    parts = PurePosixPath(value).parts
    if not parts or any(part in {"", ".", ".."} for part in parts):
        reject(f"unsafe repository path: {value!r}")
    return value

def _load_sibling(filename: str, module_name: str):
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        reject(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def _flatten_pages(raw: object) -> list[dict]:
    if not isinstance(raw, list) or any(not isinstance(page, list) for page in raw):
        reject("changed-file response must be an array of page arrays")
    records = [item for page in raw for item in page]
    if any(not isinstance(item, dict) for item in records):
        reject("changed-file response contains a non-object record")
    return records

def require_supported_codeql_patch(record: dict) -> None:
    """Admit only typed, independently reviewable CodeQL maintenance changes.

    The trusted-base classifier runs before merge, so it cannot rely on a
    proposed PR validator to authorize itself. Changes must be narrowly
    constrained here; the PR CodeQL runtime separately checks its contract.
    """
    if record.get("status") != "modified":
        reject("codeql.yml maintenance requires an in-place modification")
    patch = record.get("patch")
    if not isinstance(patch, str) or not patch:
        reject("codeql.yml patch is missing or truncated; refusing coverage claim")

    action_changes: list[tuple[str, str, str]] = []
    runner_changes: list[tuple[str, str]] = []
    for line in patch.splitlines():
        if line.startswith(("+++", "---", "@@")) or not line.startswith(("+", "-")):
            continue
        value = line[1:]
        action = ACTION_LINE.fullmatch(value)
        if action is not None:
            action_changes.append((line[0], action.group("action"), action.group("sha")))
            continue
        runner = RUST_ARM_RUNNER_LINE.fullmatch(value)
        if runner is not None:
            runner_changes.append((line[0], runner.group("runner")))
            continue
        reject("unsupported CodeQL production workflow mutation (not a typed action pin or Rust runner selector)")

    if not action_changes and not runner_changes:
        reject("CodeQL workflow patch has no supported changes")

    if action_changes:
        expected = Counter({("-", "init"): 1, ("+", "init"): 1, ("-", "analyze"): 1, ("+", "analyze"): 1})
        if Counter((sign, action) for sign, action, _ in action_changes) != expected:
            reject("CodeQL action upgrade must atomically replace init and analyze pins")
        original = {action: sha for sign, action, sha in action_changes if sign == "-"}
        replacement = {action: sha for sign, action, sha in action_changes if sign == "+"}
        if original["init"] != original["analyze"] or replacement["init"] != replacement["analyze"]:
            reject("CodeQL init/analyze action pin parity would be broken")
        if original["init"] == replacement["init"]:
            reject("CodeQL action pin was not changed")

    if runner_changes:
        if Counter(sign for sign, _ in runner_changes) != Counter({"-": 1, "+": 1}):
            reject("CodeQL Rust ARM runner change must replace one runner selector")
        if "          - language: rust" not in patch:
            reject("CodeQL runner change is not inside the Rust matrix entry")
        original = next(value for sign, value in runner_changes if sign == "-")
        replacement = next(value for sign, value in runner_changes if sign == "+")
        if original == replacement:
            reject("CodeQL Rust ARM runner selector was not changed")

def classify(pr: object, pages: object, *, repo: str, base: str, head: str, head_repo: str) -> bool:
    if not SHA.fullmatch(base) or not SHA.fullmatch(head):
        reject("expected full base/head SHA")
    if not isinstance(pr, dict):
        reject("pull request metadata must be an object")
    if pr.get("base", {}).get("sha") != base:
        reject("live pull request base SHA drifted")
    if pr.get("head", {}).get("sha") != head:
        reject("live pull request head SHA drifted")
    if pr.get("base", {}).get("repo", {}).get("full_name") != repo:
        reject("live pull request base repository drifted")
    if pr.get("head", {}).get("repo", {}).get("full_name") != head_repo:
        reject("live pull request head repository drifted")
    expected = pr.get("changed_files")
    if not isinstance(expected, int) or expected <= 0 or expected > 3000:
        reject("live pull request changed_files is outside the supported bound")
    records = _flatten_pages(pages)
    if len(records) != expected:
        reject(f"incomplete changed-file enumeration: expected {expected}, got {len(records)}")
    seen: set[tuple[str, str, str]] = set()
    relevant = False
    for record in records:
        status = record.get("status")
        filename = safe_path(record.get("filename"))
        previous_raw = record.get("previous_filename")
        if status in SIMPLE_STATUSES:
            if previous_raw not in (None, ""):
                reject(f"unexpected previous_filename for {status}: {filename}")
            previous = ""
        elif status in MOVE_STATUSES:
            previous = safe_path(previous_raw)
        else:
            reject(f"unsupported changed-file status {status!r} for {filename!r}")
        key = (status, filename, previous)
        if key in seen:
            reject(f"duplicate changed-file record: {filename}")
        seen.add(key)
        paths = (filename, previous) if previous else (filename,)
        if ".github/workflows/codeql.yml" in paths:
            require_supported_codeql_patch(record)
        if any(path in RELEVANT_PATHS for path in paths):
            relevant = True
    return relevant

def _job_block(text: str, job_id: str) -> str:
    match = re.search(rf"(?ms)^  {re.escape(job_id)}:\n(?P<body>.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)", text)
    if match is None:
        reject(f"missing job {job_id}")
    return match.group(0)

def _action_pin(block: str, action: str, where: str) -> str:
    matches = re.findall(rf"(?m)^\s+uses:\s*github/codeql-action/{re.escape(action)}@([0-9a-f]{{40}})(?:\s+#.*)?$", block)
    if len(matches) != 1:
        reject(f"{where}: expected exactly one immutable CodeQL {action} action")
    return matches[0]

def _job_permissions(block: str) -> dict[str, str]:
    # Parse only job-local permission entries; inline comments must never consume
    # the following YAML lines, and duplicate/malformed entries fail closed.
    matches = list(re.finditer(r"(?m)^    permissions:[ \t]*\n", block))
    if len(matches) != 1:
        reject("codeql-runtime job-local permissions missing or duplicated")
    pairs: dict[str, str] = {}
    for line in block[matches[0].end():].splitlines():
        if not line.startswith("      "):
            break
        item = re.fullmatch(r" {6}([A-Za-z0-9_-]+):[ \t]*([A-Za-z-]+)[ \t]*(?:#[^\r\n]*)?", line)
        if item is None:
            reject("malformed codeql-runtime permission")
        key = item.group(1)
        if key in pairs:
            reject("duplicate codeql-runtime permission")
        pairs[key] = item.group(2)
    if not pairs:
        reject("codeql-runtime permissions are empty")
    return pairs


def check_contract(root: Path = ROOT) -> None:
    codeql = (root / ".github/workflows/codeql.yml").read_text(encoding="utf-8")
    pr_ci = (root / ".github/workflows/pr-ci.yml").read_text(encoding="utf-8")
    if "  pull_request:" in codeql or re.search(r"(?m)^  push:", codeql):
        reject("production CodeQL must remain schedule/manual only")
    prod = _job_block(codeql, "analyze")
    runtime = _job_block(pr_ci, "codeql-runtime")
    scope = _job_block(pr_ci, "scope")
    gate = _job_block(pr_ci, "direct-merge-gate")
    prod_init = _action_pin(prod, "init", "production")
    prod_analyze = _action_pin(prod, "analyze", "production")
    pr_init = _action_pin(runtime, "init", "PR runtime")
    pr_analyze = _action_pin(runtime, "analyze", "PR runtime")
    if prod_init != prod_analyze:
        reject("production CodeQL init/analyze pins differ")
    if pr_init != prod_init or pr_analyze != prod_analyze:
        reject("PR runtime CodeQL pins differ from production")
    if _job_permissions(runtime) != {"contents": "read", "pull-requests": "read"}:
        reject("codeql-runtime permissions must be exactly contents:read + pull-requests:read")
    for forbidden in ("security-events: write", "actions: write", "id-token: write", "packages: write"):
        if forbidden in runtime:
            reject(f"codeql-runtime gained forbidden authority: {forbidden}")
    required_runtime = (
        "needs: scope",
        "if: ${{ needs.scope.outputs.codeql_runtime_required == 'true' }}",
        "repository: ${{ github.event.pull_request.head.repo.full_name }}",
        "ref: ${{ github.event.pull_request.head.sha }}",
        "persist-credentials: false",
        "trusted-codeql-runtime-validation.py self-test",
        "trusted-codeql-runtime-validation.py check-contract",
        "languages: actions", "build-mode: none", "category: /language:actions",
        "upload: never", "output: ${{ runner.temp }}/pr-codeql-runtime",
        "trusted-codeql-runtime-validation.py verify",
    )
    for fragment in required_runtime:
        if fragment not in runtime:
            reject(f"codeql-runtime contract missing: {fragment}")
    if "upload-artifact" in runtime:
        reject("PR CodeQL runtime must not retain SARIF as an artifact")
    required_scope = (
        "codeql_runtime_classification_outcome: ${{ steps.codeql-runtime-scope.outcome }}",
        "codeql_runtime_required: ${{ steps.codeql-runtime-scope.outputs.required }}",
        "Classify selective CodeQL runtime from trusted base",
        ".trusted-base/.github/scripts/trusted-codeql-runtime-validation.py",
        "echo 'required=true' >> \"$GITHUB_OUTPUT\"",
        "--github-output \"$GITHUB_OUTPUT\"",
    )
    for fragment in required_scope:
        if fragment not in scope:
            reject(f"trusted-base CodeQL classifier contract missing: {fragment}")
    if "\n      - codeql-runtime\n" not in gate:
        reject("Direct Merge Gate CodeQL dependency missing: codeql-runtime")
    required_gate = (
        "CODEQL_RUNTIME_CLASSIFICATION_OUTCOME:", "CODEQL_RUNTIME_REQUIRED:", "CODEQL_RUNTIME_RESULT:",
        'require_success codeql-runtime-scope "$CODEQL_RUNTIME_CLASSIFICATION_OUTCOME"',
        'require_success codeql-runtime "$CODEQL_RUNTIME_RESULT"',
        'require_skipped codeql-runtime "$CODEQL_RUNTIME_RESULT"',
    )
    for fragment in required_gate:
        if fragment not in gate:
            reject(f"Direct Merge Gate CodeQL contract missing: {fragment}")

def prove_no_production_promotion(head: str) -> None:
    if not SHA.fullmatch(head):
        reject("head must be an exact immutable SHA")
    gate = _load_sibling("check_release_codeql.py", "oxide_release_codeql_gate")
    repo = "owner/repo"
    workflow = {"id": 42}
    run = {
        "id": 5, "workflow_id": 42, "path": ".github/workflows/codeql.yml",
        "event": "pull_request", "head_repository": {"full_name": repo},
        "head_branch": "main", "head_sha": head, "status": "completed",
        "conclusion": "success", "run_attempt": 1, "created_at": "2026-01-01T00:00:00Z",
    }
    expected_jobs = {f"Analyze ({lang})" for lang in gate.LANGUAGES} | {"CodeQL schedule guard"}
    jobs = {"total_count": len(expected_jobs), "jobs": [{"name": name, "status": "completed", "conclusion": "success"} for name in sorted(expected_jobs)]}
    artifacts = {
        "total_count": 2 * len(gate.LANGUAGES),
        "artifacts": [
            {"name": f"release-codeql-{head}-{lang}", "id": index + 1, "size_in_bytes": 100, "expired": False, "workflow_run": {"head_sha": head}}
            for index, lang in enumerate(gate.LANGUAGES)
        ] + [
            {"name": f"release-codeql-source-{head}-{lang}", "id": index + 10, "size_in_bytes": 100, "expired": False, "workflow_run": {"head_sha": head}}
            for index, lang in enumerate(gate.LANGUAGES)
        ],
    }
    try:
        gate.metadata_ok(run, workflow, jobs, artifacts, repo, head, dt.datetime.now(dt.timezone.utc))
    except ValueError as exc:
        if "untrusted or mismatched" not in str(exc):
            raise
        return
    raise AssertionError("pull_request CodeQL evidence was accepted as production evidence")

def verify_runtime(directory: Path, head: str) -> dict:
    verdict_module = _load_sibling("check_codeql_sarif.py", "oxide_codeql_sarif")
    verdict = verdict_module.check(directory, "actions", head)
    prove_no_production_promotion(head)
    return {"schema": 1, "event": "pull_request", "source_sha": head, "trust_domain": "untrusted-pr-codeql-runtime", "production_evidence": False, **verdict}

def self_test() -> None:
    repo = "owner/repo"; base = "a" * 40; head = "b" * 40; head_repo = "contributor/repo"
    def pr(count: int = 1) -> dict:
        return {"base": {"sha": base, "repo": {"full_name": repo}}, "head": {"sha": head, "repo": {"full_name": head_repo}}, "changed_files": count}
    assert classify(pr(), [[{"status": "modified", "filename": ".github/workflows/pr-ci.yml"}]], repo=repo, base=base, head=head, head_repo=head_repo)
    assert not classify(pr(), [[{"status": "modified", "filename": "README.md"}]], repo=repo, base=base, head=head, head_repo=head_repo)
    allowed_patch = "\n".join([
        "@@ -1,2 +1,2 @@",
        "-        uses: github/codeql-action/init@" + "1" * 40 + " # old",
        "+        uses: github/codeql-action/init@" + "2" * 40 + " # new",
        "-        uses: github/codeql-action/analyze@" + "1" * 40 + " # old",
        "+        uses: github/codeql-action/analyze@" + "2" * 40 + " # new",
    ])
    assert classify(pr(), [[{"status": "modified", "filename": ".github/workflows/codeql.yml", "patch": allowed_patch}]], repo=repo, base=base, head=head, head_repo=head_repo)
    runner_patch = "\\n".join([
        "@@ -81,7 +81,7 @@",
        "          - language: rust",
        "            build-mode: none",
        "-            os: ubuntu-24.04-arm",
        "+            os: ubuntu-26.04-arm",
    ])
    assert classify(
        pr(), [[{"status": "modified", "filename": ".github/workflows/codeql.yml", "patch": runner_patch}]],
        repo=repo, base=base, head=head, head_repo=head_repo,
    )
    assert classify(
        pr(), [[{"status": "modified", "filename": ".github/workflows/codeql.yml",
                "patch": allowed_patch + "\\n" + runner_patch}]],
        repo=repo, base=base, head=head, head_repo=head_repo,
    )
    for unsupported_patch in (
        "@@ -1 +1 @@\\n-old\\n+unreviewed arbitrary workflow change",
        "@@ -81,7 +81,7 @@\\n          - language: java\\n-            os: ubuntu-24.04-arm\\n+            os: ubuntu-26.04-arm",
        "@@ -81,7 +81,7 @@\\n          - language: rust\\n-            os: ubuntu-24.04-arm\\n+            os: self-hosted",
        runner_patch + "\\n+      security-events: write",
        runner_patch.replace("ubuntu-26.04-arm", "ubuntu-24.04-arm"),
    ):
        try:
            classify(
                pr(), [[{"status": "modified", "filename": ".github/workflows/codeql.yml",
                         "patch": unsupported_patch}]], repo=repo, base=base, head=head, head_repo=head_repo,
            )
        except ValueError:
            pass
        else:
            raise AssertionError("unsupported CodeQL workflow change was accepted")
    try:
        classify(pr(), [[{"status": "modified", "filename": ".github/workflows/codeql.yml", "patch": allowed_patch + "\n+      security-events: write"}]], repo=repo, base=base, head=head, head_repo=head_repo)
    except ValueError:
        pass
    else:
        raise AssertionError("unsupported codeql.yml mutation was accepted")
    for pages in ([[]], [[{"status": "modified", "filename": "../unsafe"}]], [[{"status": "mystery", "filename": "README.md"}]]):
        try:
            classify(pr(), pages, repo=repo, base=base, head=head, head_repo=head_repo)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid changed-file evidence was accepted")
    permission_block = (
        "  codeql-runtime:\n"
        "    permissions:\n"
        "      contents: read # exact PR source\n"
        "      pull-requests: read # live changed-file metadata\n"
        "    steps:\n"
    )
    expected_permissions = {"contents": "read", "pull-requests": "read"}
    assert _job_permissions(permission_block) == expected_permissions
    for invalid in (
        permission_block.replace("      contents: read # exact PR source\n", "      contents: read # exact PR source\n      contents: read # duplicate\n", 1),
        permission_block.replace("      pull-requests: read # live changed-file metadata\n", "      pull-requests: read # live changed-file metadata\n      metadata: [invalid]\n", 1),
    ):
        try:
            _job_permissions(invalid)
        except ValueError:
            pass
        else:
            raise AssertionError("malformed/duplicate CodeQL runtime permissions accepted")
    assert _job_permissions(permission_block.replace("contents: read", "contents: write", 1)) != expected_permissions
    prove_no_production_promotion(head)
    verdict_module = _load_sibling("check_codeql_sarif.py", "oxide_codeql_sarif_selftest")
    with tempfile.TemporaryDirectory(prefix="oxide-codeql-runtime-selftest-") as tmp:
        try:
            verdict_module.check(Path(tmp), "actions", head)
        except ValueError as exc:
            if "exactly one SARIF" not in str(exc):
                raise
        else:
            raise AssertionError("missing SARIF was accepted")
    print("trusted CodeQL PR runtime adapter self-tests passed")

def write_github_output(path: Path, required: bool) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"required={'true' if required else 'false'}\n")

def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("self-test"); sub.add_parser("check-contract")
    classify_parser = sub.add_parser("classify")
    classify_parser.add_argument("--pr-json", type=Path, required=True); classify_parser.add_argument("--files-json", type=Path, required=True)
    classify_parser.add_argument("--repo", required=True); classify_parser.add_argument("--base", required=True); classify_parser.add_argument("--head", required=True)
    classify_parser.add_argument("--head-repo", required=True); classify_parser.add_argument("--github-output", type=Path, required=True)
    verify_parser = sub.add_parser("verify"); verify_parser.add_argument("--sarif-dir", type=Path, required=True); verify_parser.add_argument("--head", required=True)
    args = parser.parse_args()
    if args.command == "self-test":
        self_test()
    elif args.command == "check-contract":
        check_contract(); print("selective CodeQL PR runtime contract: PASS")
    elif args.command == "classify":
        required = classify(json.loads(args.pr_json.read_text(encoding="utf-8")), json.loads(args.files_json.read_text(encoding="utf-8")), repo=args.repo, base=args.base, head=args.head, head_repo=args.head_repo)
        write_github_output(args.github_output, required); print(json.dumps({"required": required}, sort_keys=True))
    elif args.command == "verify":
        print(json.dumps(verify_runtime(args.sarif_dir, args.head), sort_keys=True))
    return 0

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ValueError, TypeError, KeyError, OSError, json.JSONDecodeError) as exc:
        print(f"::error::Trusted CodeQL PR runtime rejected: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
