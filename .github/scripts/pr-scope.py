#!/usr/bin/env python3
"""Trusted-base PR scope and retained-campaign applicability classifier.

The workflow that consumes this script must execute the copy from the pull
request's base SHA. The script never decides merge authority itself; it returns
scope/applicability data that callers must consume fail-closed.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable


SIMPLE_STATUSES = frozenset({"added", "modified", "removed"})
MOVE_STATUSES = frozenset({"renamed", "copied"})


@dataclass(frozen=True)
class Change:
    status: str
    filename: str
    previous_filename: str = ""


@dataclass(frozen=True)
class DocsPolicy:
    exact_paths: frozenset[str]
    markdown_prefixes: tuple[str, ...]
    excluded_prefixes: tuple[str, ...]


@dataclass(frozen=True)
class Campaign:
    semantics_path: str
    workflow: str
    paths: tuple[str, ...]
    direct_paths: tuple[str, ...]


@dataclass(frozen=True)
class ScopePolicy:
    docs: DocsPolicy
    semantics_glob: str
    retained_evidence_policy: str
    global_campaign_paths: tuple[str, ...]
    trusted_tree_contract: str


def safe_path(path: str) -> bool:
    if not path or path.startswith("/") or "\\" in path:
        return False
    parts = PurePosixPath(path).parts
    return bool(parts) and all(part not in {"", ".", ".."} for part in parts)


def _require_path(value: object, label: str, *, prefix: bool = False) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a string")
    candidate = value.rstrip("/") if prefix else value
    if not safe_path(candidate):
        raise ValueError(f"{label} is not a safe repository-relative path: {value!r}")
    if prefix and not value.endswith("/"):
        raise ValueError(f"{label} must end with '/': {value!r}")
    return value


def load_policy(path: Path) -> ScopePolicy:
    raw_document = json.loads(path.read_text(encoding="utf-8"))
    raw = raw_document.get("pr_scope")
    if not isinstance(raw, dict):
        raise ValueError("missing pr_scope policy")

    docs_raw = raw.get("docs_only")
    if not isinstance(docs_raw, dict):
        raise ValueError("pr_scope.docs_only must be an object")

    exact_raw = docs_raw.get("exact_paths")
    prefixes_raw = docs_raw.get("markdown_prefixes")
    excluded_raw = docs_raw.get("excluded_prefixes", [])
    if not isinstance(exact_raw, list) or not exact_raw:
        raise ValueError("pr_scope.docs_only.exact_paths must be a non-empty list")
    if not isinstance(prefixes_raw, list) or not prefixes_raw:
        raise ValueError("pr_scope.docs_only.markdown_prefixes must be a non-empty list")
    if not isinstance(excluded_raw, list):
        raise ValueError("pr_scope.docs_only.excluded_prefixes must be a list")

    exact = tuple(_require_path(v, "docs-only exact path") for v in exact_raw)
    prefixes = tuple(
        _require_path(v, "docs-only markdown prefix", prefix=True) for v in prefixes_raw
    )
    excluded = tuple(
        _require_path(v, "docs-only excluded prefix", prefix=True) for v in excluded_raw
    )
    if len(set(exact)) != len(exact):
        raise ValueError("docs-only exact paths contain duplicates")
    if len(set(prefixes)) != len(prefixes):
        raise ValueError("docs-only markdown prefixes contain duplicates")
    if len(set(excluded)) != len(excluded):
        raise ValueError("docs-only excluded prefixes contain duplicates")

    semantics_glob = raw.get("campaign_semantics_glob")
    if semantics_glob != "tests/fixtures/**/campaign-semantics.json":
        raise ValueError(
            "campaign_semantics_glob must remain the canonical "
            "tests/fixtures/**/campaign-semantics.json"
        )

    retained = _require_path(
        raw.get("retained_evidence_policy"), "retained evidence policy"
    )

    global_raw = raw.get("global_campaign_paths", [])
    if not isinstance(global_raw, list):
        raise ValueError("pr_scope.global_campaign_paths must be a list")
    global_paths = tuple(
        _require_path(v, "global campaign path") for v in global_raw
    )
    if len(set(global_paths)) != len(global_paths):
        raise ValueError("global campaign paths contain duplicates")

    trusted_tree_contract = raw.get("trusted_tree_contract")
    if trusted_tree_contract != "exact-git-base-sha":
        raise ValueError(
            "pr_scope.trusted_tree_contract must remain exact-git-base-sha"
        )

    return ScopePolicy(
        docs=DocsPolicy(frozenset(exact), prefixes, excluded),
        semantics_glob=semantics_glob,
        retained_evidence_policy=retained,
        global_campaign_paths=global_paths,
        trusted_tree_contract=trusted_tree_contract,
    )


def verify_trusted_base(repo_root: Path, expected_sha: str) -> None:
    if not re.fullmatch(r"[0-9a-f]{40}", expected_sha):
        raise ValueError("trusted base SHA must be an exact 40-character lowercase SHA")
    try:
        actual = subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise ValueError(f"cannot resolve trusted repository HEAD: {error}") from error
    if actual != expected_sha:
        raise ValueError(
            f"trusted repository root is {actual}, expected base {expected_sha}"
        )


def path_matches_declared(path: str, declared: str) -> bool:
    return path == declared or path.startswith(f"{declared.rstrip('/')}/")


def is_docs_path(path: str, policy: DocsPolicy) -> bool:
    if not safe_path(path):
        return False
    if path in policy.exact_paths:
        return True
    if any(path.startswith(prefix) for prefix in policy.excluded_prefixes):
        return False
    return path.endswith(".md") and any(
        path.startswith(prefix) for prefix in policy.markdown_prefixes
    )


def discover_campaigns(repo_root: Path, policy: ScopePolicy) -> tuple[Campaign, ...]:
    semantics_files = sorted(repo_root.glob(policy.semantics_glob))
    if not semantics_files:
        raise ValueError("no campaign semantic-closure documents found")

    campaigns: list[Campaign] = []
    workflows_seen: set[str] = set()

    for semantics_file in semantics_files:
        semantics_rel = semantics_file.relative_to(repo_root).as_posix()
        raw = json.loads(semantics_file.read_text(encoding="utf-8"))
        categories = raw.get("categories")
        if not isinstance(categories, dict) or not categories:
            raise ValueError(
                f"{semantics_rel}: categories must be a non-empty object"
            )

        declared: list[str] = []
        direct_declared: list[str] = []
        for category, value in categories.items():
            if not isinstance(category, str) or not category:
                raise ValueError(
                    f"{semantics_rel}: category names must be non-empty strings"
                )
            if not isinstance(value, dict):
                raise ValueError(
                    f"{semantics_rel}: category {category!r} must be an object"
                )
            proof_mode = value.get("pr_proof")
            if proof_mode not in {"direct", "stale-only"}:
                raise ValueError(
                    f"{semantics_rel}: category {category!r} pr_proof "
                    "must be 'direct' or 'stale-only'"
                )
            paths = value.get("paths")
            if not isinstance(paths, list) or not paths:
                raise ValueError(
                    f"{semantics_rel}: category {category!r} paths "
                    "must be a non-empty list"
                )
            for raw_path in paths:
                declared_path = _require_path(
                    raw_path,
                    f"{semantics_rel}:{category} path",
                )
                declared.append(declared_path)
                if proof_mode == "direct":
                    direct_declared.append(declared_path)

        if len(set(declared)) != len(declared):
            raise ValueError(
                f"{semantics_rel}: semantic closure contains duplicate paths"
            )
        if semantics_rel not in declared:
            raise ValueError(
                f"{semantics_rel}: semantic closure must contain itself"
            )

        workflow_candidates = sorted(
            {
                path
                for path in declared
                if path.startswith(".github/workflows/")
                and ("/m5-" in path or "/m6-" in path)
                and path.endswith(".yml")
            }
        )
        if len(workflow_candidates) != 1:
            raise ValueError(
                f"{semantics_rel}: expected exactly one dedicated M5/M6 workflow "
                f"in the closure, found {workflow_candidates}"
            )
        workflow = workflow_candidates[0]
        if workflow in workflows_seen:
            raise ValueError(
                f"campaign workflow {workflow} is owned by multiple semantic closures"
            )
        workflows_seen.add(workflow)
        if not direct_declared:
            raise ValueError(
                f"{semantics_rel}: semantic closure declares no direct PR proof paths"
            )
        campaigns.append(
            Campaign(
                semantics_rel,
                workflow,
                tuple(sorted(declared)),
                tuple(sorted(direct_declared)),
            )
        )

    retained_path = repo_root / policy.retained_evidence_policy
    retained = json.loads(retained_path.read_text(encoding="utf-8"))
    producers = retained.get("artifact_producers")
    if not isinstance(producers, list) or not producers:
        raise ValueError(
            "retained-evidence policy artifact_producers must be a non-empty list"
        )
    producer_workflows: list[str] = []
    for entry in producers:
        if not isinstance(entry, dict):
            raise ValueError(
                "retained-evidence artifact producer entries must be objects"
            )
        workflow = _require_path(
            entry.get("workflow"),
            "retained-evidence artifact producer workflow",
        )
        producer_workflows.append(workflow)
    if len(set(producer_workflows)) != len(producer_workflows):
        raise ValueError(
            "retained-evidence artifact producer workflows contain duplicates"
        )

    discovered = set(workflows_seen)
    expected = set(producer_workflows)
    if discovered != expected:
        missing = sorted(expected - discovered)
        extra = sorted(discovered - expected)
        raise ValueError(
            "campaign semantic-closure inventory disagrees with "
            f"retained-evidence producers; missing={missing} extra={extra}"
        )

    return tuple(sorted(campaigns, key=lambda campaign: campaign.workflow))


def parse_record(line: str) -> Change | None:
    fields = line.rstrip("\n").split("\t")
    if len(fields) != 3:
        return None
    status, filename, previous_filename = fields
    if status in SIMPLE_STATUSES:
        if previous_filename:
            return None
    elif status in MOVE_STATUSES:
        if not previous_filename:
            return None
    else:
        return None
    if not safe_path(filename) or (
        previous_filename and not safe_path(previous_filename)
    ):
        return None
    return Change(status, filename, previous_filename)


def change_paths(change: Change) -> tuple[str, ...]:
    if change.previous_filename:
        return (change.filename, change.previous_filename)
    return (change.filename,)


def classify(
    changes: list[Change],
    expected_count: int,
    policy: ScopePolicy,
    campaigns: tuple[Campaign, ...],
) -> dict[str, object] | None:
    if expected_count <= 0 or not changes or len(changes) != expected_count:
        return None
    if len(set(changes)) != len(changes):
        return None

    all_paths = tuple(
        path for change in changes for path in change_paths(change)
    )
    docs_only = all(is_docs_path(path, policy.docs) for path in all_paths)

    global_hits = sorted(
        {
            declared
            for declared in policy.global_campaign_paths
            if any(
                path_matches_declared(path, declared) for path in all_paths
            )
        }
    )

    campaign_results: dict[str, dict[str, object]] = {}
    affected: list[str] = []
    direct_proof: list[str] = []
    stale_only: list[str] = []
    for campaign in campaigns:
        reasons: list[str] = []
        direct_reasons: list[str] = []
        if global_hits:
            reasons.extend(f"global:{path}" for path in global_hits)
        for changed in all_paths:
            matched = [
                declared
                for declared in campaign.paths
                if path_matches_declared(changed, declared)
            ]
            direct_matched = [
                declared
                for declared in campaign.direct_paths
                if path_matches_declared(changed, declared)
            ]
            for declared in matched:
                reasons.append(f"{changed} -> {declared}")
            for declared in direct_matched:
                direct_reasons.append(f"{changed} -> {declared}")
        applicable = bool(reasons)
        requires_direct_proof = bool(direct_reasons)
        if applicable:
            affected.append(campaign.workflow)
            if requires_direct_proof:
                direct_proof.append(campaign.workflow)
            else:
                stale_only.append(campaign.workflow)
        campaign_results[campaign.workflow] = {
            "applicable": applicable,
            "direct_proof": requires_direct_proof,
            "semantics": campaign.semantics_path,
            "reasons": sorted(set(reasons)),
            "direct_proof_reasons": sorted(set(direct_reasons)),
        }

    return {
        "classification_valid": True,
        "docs_only": docs_only,
        "affected_campaign_workflows": affected,
        "direct_proof_campaign_workflows": direct_proof,
        "stale_only_campaign_workflows": stale_only,
        "campaigns": campaign_results,
    }


def read_changes(lines: Iterable[str]) -> list[Change] | None:
    changes: list[Change] = []
    for line in lines:
        change = parse_record(line)
        if change is None:
            return None
        changes.append(change)
    return changes


def self_test(
    repo_root: Path,
    policy: ScopePolicy,
    campaigns: tuple[Campaign, ...],
) -> None:
    assert campaigns, "campaign inventory must not be empty"

    actual_head = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    verify_trusted_base(repo_root, actual_head)
    wrong_head = "0" * 40 if actual_head != "0" * 40 else "1" * 40
    try:
        verify_trusted_base(repo_root, wrong_head)
    except ValueError:
        pass
    else:
        raise AssertionError("trusted-base mismatch must fail closed")

    workflows = {campaign.workflow for campaign in campaigns}
    assert ".github/workflows/m5-conformance.yml" in workflows
    assert ".github/workflows/m6-conformance.yml" in workflows

    docs = [Change("modified", "README.md")]
    result = classify(docs, 1, policy, campaigns)
    assert result is not None and result["docs_only"] is True
    assert result["affected_campaign_workflows"] == []
    assert result["direct_proof_campaign_workflows"] == []
    assert result["stale_only_campaign_workflows"] == []

    docs_rename = [
        Change("renamed", "docs/new.md", "docs/old.md")
    ]
    result = classify(docs_rename, 1, policy, campaigns)
    assert result is not None and result["docs_only"] is True

    source = [
        Change("modified", "crates/oxide-batch/src/lib.rs")
    ]
    result = classify(source, 1, policy, campaigns)
    assert result is not None and result["docs_only"] is False
    assert (
        ".github/workflows/m5-conformance.yml"
        in result["affected_campaign_workflows"]
    )
    assert (
        ".github/workflows/m5-conformance.yml"
        not in result["direct_proof_campaign_workflows"]
    )
    assert (
        ".github/workflows/m5-conformance.yml"
        in result["stale_only_campaign_workflows"]
    )

    lock = [Change("modified", "Cargo.lock")]
    result = classify(lock, 1, policy, campaigns)
    assert result is not None
    assert set(result["affected_campaign_workflows"]) == workflows
    assert result["direct_proof_campaign_workflows"] == []
    assert set(result["stale_only_campaign_workflows"]) == workflows

    semantics_change = [
        Change(
            "modified",
            "tests/fixtures/conformance/campaign-semantics.json",
        )
    ]
    result = classify(semantics_change, 1, policy, campaigns)
    assert result is not None
    assert (
        ".github/workflows/m5-conformance.yml"
        in result["affected_campaign_workflows"]
    )
    assert (
        ".github/workflows/m5-conformance.yml"
        in result["direct_proof_campaign_workflows"]
    )
    assert (
        ".github/workflows/m5-conformance.yml"
        not in result["stale_only_campaign_workflows"]
    )

    verifier_change = [Change("modified", "xtask/src/evidence.rs")]
    result = classify(verifier_change, 1, policy, campaigns)
    assert result is not None
    assert set(result["direct_proof_campaign_workflows"]) == workflows
    assert result["stale_only_campaign_workflows"] == []

    boundary_move = [
        Change(
            "renamed",
            "docs/moved.md",
            "crates/oxide-batch/src/moved.md",
        )
    ]
    result = classify(boundary_move, 1, policy, campaigns)
    assert result is not None and result["docs_only"] is False
    assert (
        ".github/workflows/m5-conformance.yml"
        in result["affected_campaign_workflows"]
    )
    assert (
        ".github/workflows/m5-conformance.yml"
        not in result["direct_proof_campaign_workflows"]
    )
    assert (
        ".github/workflows/m5-conformance.yml"
        in result["stale_only_campaign_workflows"]
    )

    assert parse_record("changed\tREADME.md\t\n") is None
    assert parse_record("renamed\tdocs/new.md\t\n") is None
    assert (
        classify(
            [Change("modified", "README.md")],
            2,
            policy,
            campaigns,
        )
        is None
    )
    duplicate = [
        Change("modified", "README.md"),
        Change("modified", "README.md"),
    ]
    assert classify(duplicate, 2, policy, campaigns) is None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    parser.add_argument("--policy", type=Path, required=True)
    parser.add_argument("--expected-count", type=int)
    parser.add_argument("--trusted-base-sha")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    try:
        repo_root = args.repo_root.resolve()
        policy_path = args.policy
        if not policy_path.is_absolute():
            policy_path = repo_root / policy_path
        policy = load_policy(policy_path)
        campaigns = discover_campaigns(repo_root, policy)
        if args.self_test:
            self_test(repo_root, policy, campaigns)
            print(
                f"trusted PR scope self-test passed "
                f"({len(campaigns)} campaigns)"
            )
            return 0
        if args.trusted_base_sha is None:
            parser.error(
                "--trusted-base-sha is required for classification"
            )
        verify_trusted_base(repo_root, args.trusted_base_sha)
        if args.expected_count is None:
            parser.error(
                "--expected-count is required unless --self-test is used"
            )
        changes = read_changes(sys.stdin)
        if changes is None:
            print("invalid changed-file metadata", file=sys.stderr)
            return 2
        result = classify(
            changes,
            args.expected_count,
            policy,
            campaigns,
        )
        if result is None:
            print(
                "changed-file metadata failed exact-count/uniqueness validation",
                file=sys.stderr,
            )
            return 2
        print(
            json.dumps(
                result,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    except (
        OSError,
        ValueError,
        json.JSONDecodeError,
        AssertionError,
    ) as error:
        print(
            f"trusted PR scope classification failed: {error}",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
