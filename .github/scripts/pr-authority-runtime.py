#!/usr/bin/env python3
"""Plan and verify dispatched PR authorities from protected-base policy state."""

from __future__ import annotations

import argparse
import copy
import io
import json
import os
import re
import sys
import time
import unittest
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable

PR_PROOF_SCHEMA = "trusted-scope-authorities-v1"
VALID_APPLICABILITY = {"non_docs", "evidence_impact", "supply_chain_impact"}
SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class ContractError(RuntimeError):
    """Fail-closed runtime contract violation."""


@dataclass(frozen=True)
class Authority:
    id: str
    workflow: str
    applicability: str


def load_authorities(policy_path: str | Path) -> list[Authority]:
    try:
        policy = json.loads(Path(policy_path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContractError(f"could not read protected pr_proof policy: {exc}") from exc

    proof = policy.get("pr_proof")
    if not isinstance(proof, dict):
        raise ContractError("protected policy must contain pr_proof object")
    if proof.get("schema") != PR_PROOF_SCHEMA:
        raise ContractError(
            f"protected pr_proof schema must be {PR_PROOF_SCHEMA!r}"
        )
    members = proof.get("members")
    if not isinstance(members, list) or not members:
        raise ContractError("protected pr_proof members must be a non-empty list")

    authorities: list[Authority] = []
    seen_ids: set[str] = set()
    seen_workflows: set[str] = set()
    for index, raw in enumerate(members):
        if not isinstance(raw, dict):
            raise ContractError(f"pr_proof member {index} must be an object")
        authority_id = raw.get("id")
        workflow = raw.get("workflow")
        applicability = raw.get("applicability")
        if not isinstance(authority_id, str) or not re.fullmatch(r"[a-z0-9_-]+", authority_id):
            raise ContractError(f"pr_proof member {index} has invalid id")
        if not isinstance(workflow, str) or not workflow.startswith(".github/workflows/"):
            raise ContractError(f"pr_proof member {authority_id} has invalid workflow")
        if applicability not in VALID_APPLICABILITY:
            raise ContractError(
                f"pr_proof member {authority_id} has unsupported applicability {applicability!r}"
            )
        if authority_id in seen_ids:
            raise ContractError(f"pr_proof duplicates authority id {authority_id}")
        if workflow in seen_workflows:
            raise ContractError(f"pr_proof duplicates workflow {workflow}")
        seen_ids.add(authority_id)
        seen_workflows.add(workflow)
        authorities.append(Authority(authority_id, workflow, applicability))
    return authorities


def _trusted_boolean(value: str) -> bool:
    return value in {"true", "false"}


def required_authorities(
    authorities: Iterable[Authority],
    *,
    scope_result: str,
    classification_outcome: str,
    legacy_base: str,
    docs_only: str,
    evidence_impact: str,
    supply_chain_impact: str,
) -> list[Authority]:
    authorities = list(authorities)
    trusted = (
        scope_result == "success"
        and classification_outcome == "success"
        and legacy_base == "false"
        and _trusted_boolean(docs_only)
        and _trusted_boolean(evidence_impact)
        and _trusted_boolean(supply_chain_impact)
    )
    if not trusted:
        return authorities

    required: list[Authority] = []
    for authority in authorities:
        if authority.applicability == "non_docs" and docs_only != "true":
            required.append(authority)
        elif authority.applicability == "evidence_impact" and evidence_impact != "false":
            required.append(authority)
        elif authority.applicability == "supply_chain_impact" and supply_chain_impact != "false":
            required.append(authority)
    return required


def plan_payload(authorities: Iterable[Authority], required: Iterable[Authority]) -> dict[str, object]:
    required = list(required)
    return {
        "schema": PR_PROOF_SCHEMA,
        "required": [
            {
                "id": authority.id,
                "workflow": authority.workflow,
                "applicability": authority.applicability,
            }
            for authority in required
        ],
    }


def expected_run_name(
    authority: Authority,
    *,
    pr_number: str,
    head_sha: str,
    caller_run_id: str,
    caller_run_attempt: str,
) -> str:
    return (
        f"pr-authority/{authority.id}/pr-{pr_number}/{head_sha}/"
        f"caller-{caller_run_id}-{caller_run_attempt}"
    )


def validate_correlation(
    *,
    base_sha: str,
    head_sha: str,
    head_repo: str,
    pr_number: str,
    caller_run_id: str,
    caller_run_attempt: str,
) -> None:
    if not SHA_RE.fullmatch(base_sha):
        raise ContractError("base_sha must be a lowercase 40-hex commit SHA")
    if not SHA_RE.fullmatch(head_sha):
        raise ContractError("head_sha must be a lowercase 40-hex commit SHA")
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", head_repo):
        raise ContractError("head_repo must be owner/repository")
    for label, value in (
        ("pr_number", pr_number),
        ("caller_run_id", caller_run_id),
        ("caller_run_attempt", caller_run_attempt),
    ):
        if not value.isdigit() or int(value) <= 0:
            raise ContractError(f"{label} must be a positive integer string")



def _snapshot_repository(value: object) -> tuple[object, object] | None:
    """Only stable repository ownership, never changing description/metrics."""
    if not isinstance(value, dict):
        return None
    return value.get("id"), value.get("full_name")


def _snapshot_actor(value: object) -> tuple[object, object] | None:
    if not isinstance(value, dict):
        return None
    return value.get("id"), value.get("login")


def _snapshot_pr_ref(value: object) -> tuple[object, object, object] | None:
    if not isinstance(value, dict):
        return None
    return value.get("ref"), value.get("sha"), _snapshot_repository(value.get("repo"))


def _snapshot_pr_identity(value: dict[str, object]) -> tuple[object, ...]:
    """Identity and security-relevant PR state for repeat pagination reads."""
    return (
        value.get("number"), value.get("id"), value.get("node_id"),
        value.get("state"), value.get("draft"), value.get("merged_at"),
        _snapshot_actor(value.get("user")),
        _snapshot_pr_ref(value.get("head")),
        _snapshot_pr_ref(value.get("base")),
    )


def _snapshot_run_identity(value: dict[str, object]) -> tuple[object, ...]:
    """Invariant workflow run provenance. Excludes mutable status and timestamps."""
    return (
        value.get("id"), value.get("run_number"), value.get("run_attempt"),
        value.get("workflow_id"), value.get("event"), value.get("path"),
        value.get("head_sha"), value.get("head_branch"),
        value.get("display_title"), _snapshot_repository(value.get("repository")),
        _snapshot_repository(value.get("head_repository")),
        _snapshot_actor(value.get("actor")),
        _snapshot_actor(value.get("triggering_actor")),
    )


def _snapshot_safe_run_transition(before: dict[str, object], after: dict[str, object]) -> bool:
    """Permit forward-only run progress; never accept terminal regression/rerun."""
    old, new = before.get("status"), after.get("status")
    initial, final = before.get("conclusion"), after.get("conclusion")
    # Minimal test fixtures can omit status; real run APIs always supply it.
    if old is None and new is None:
        return initial is None and final is None
    ranks = {
        "requested": 0, "pending": 0, "queued": 1, "waiting": 1,
        "in_progress": 2, "completed": 3,
    }
    if old not in ranks or new not in ranks or ranks[new] < ranks[old]:
        return False
    if old == "completed":
        return new == "completed" and final == initial and initial is not None
    if initial is not None:
        return False
    if new == "completed":
        return final in {
            "success", "failure", "cancelled", "skipped", "timed_out",
            "action_required", "neutral", "stale",
        }
    return final is None


class GitHubActionsClient:
    def __init__(
        self,
        *,
        api_url: str,
        repository: str,
        token: str,
        opener: Callable[..., object] = urllib.request.urlopen,
    ) -> None:
        if not repository or "/" not in repository:
            raise ContractError("GITHUB_REPOSITORY must be owner/repository")
        if not token:
            raise ContractError("GH_TOKEN is required to verify authority runs")
        self.api_url = api_url.rstrip("/")
        self.repository = repository
        self.token = token
        self.opener = opener

    def _read_json(self, path: str) -> dict[str, object]:
        request = urllib.request.Request(
            f"{self.api_url}/repos/{self.repository}/{path}",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "oxide-batch-pr-authority-runtime",
            },
        )
        try:
            with self.opener(request, timeout=20) as response:
                payload = json.load(response)
        except Exception as exc:
            raise ContractError(f"could not read GitHub API {path}: {exc}") from exc
        if not isinstance(payload, dict):
            raise ContractError(f"GitHub API {path} returned malformed metadata")
        return payload

    def pull_request(self, number: str) -> dict[str, object]:
        if not number.isdigit() or int(number) <= 0:
            raise ContractError("PR number must be a positive integer")
        # Read-only PR metadata is accessible with the existing contents:read token.
        return self._read_json(f"pulls/{number}")

    def workflow_runs(self, workflow: str) -> list[dict[str, object]]:
        workflow_id = Path(workflow).name
        if not workflow_id:
            raise ContractError("workflow path must contain a file name")
        encoded = urllib.parse.quote(workflow_id, safe="")
        payload = self._read_json(
            f"actions/workflows/{encoded}/runs?event=workflow_dispatch&per_page=100"
        )
        runs = payload.get("workflow_runs")
        if not isinstance(runs, list):
            raise ContractError(f"workflow run response for {workflow} is malformed")
        return [run for run in runs if isinstance(run, dict)]



    def _read_array(self, path: str) -> list[dict[str, object]]:
        """Read a GitHub REST array without granting or invoking write APIs."""
        request = urllib.request.Request(
            f"{self.api_url}/repos/{self.repository}/{path}",
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "oxide-batch-orphan-readonly-audit",
            },
        )
        try:
            with self.opener(request, timeout=20) as response:
                payload = json.load(response)
        except Exception as exc:
            raise ContractError(f"orphan audit GitHub GET {path} failed: {exc}") from exc
        if not isinstance(payload, list) or any(not isinstance(row, dict) for row in payload):
            raise ContractError("orphan audit GitHub array response is malformed")
        return payload

    def audit_workflow_runs_bounded(
        self, workflow: str, *, max_pages: int = 3
    ) -> list[dict[str, object]]:
        """Bounded complete read-only snapshot; ambiguous/changed pages fail closed.

        Never feed these rows directly to a cancellation API. GitHub lists can
        change after this snapshot and no cross-request transaction is offered.
        """
        if not re.fullmatch(r"\.github/workflows/[A-Za-z0-9_.-]+\.yml", workflow):
            raise ContractError("orphan audit workflow path is invalid")
        if type(max_pages) is not int or not 1 <= max_pages <= 3:
            raise ContractError("orphan audit page budget is invalid")

        filename = Path(workflow).name

        def read_page(page: int) -> tuple[int, list[dict[str, object]]]:
            payload = self._read_json(
                f"actions/workflows/{filename}/runs?"
                f"event=workflow_dispatch&per_page=100&page={page}"
            )
            count = payload.get("total_count")
            rows = payload.get("workflow_runs")
            if (
                type(count) is not int or count < 0 or count > max_pages * 100
                or not isinstance(rows, list)
                or any(not isinstance(row, dict) for row in rows)
            ):
                raise ContractError("orphan audit workflow page is malformed or exceeds budget")
            return count, rows

        count, first = read_page(1)
        pages = max(1, (count + 99) // 100)
        results: list[dict[str, object]] = []
        seen: set[int] = set()
        for page in range(1, pages + 1):
            page_count, rows = (count, first) if page == 1 else read_page(page)
            expected = min(100, max(0, count - (page - 1) * 100))
            if page_count != count or len(rows) != expected:
                raise ContractError("orphan audit workflow pages are incomplete or changed")
            for row in rows:
                run_id = row.get("id")
                if (
                    type(run_id) is not int or run_id <= 0 or run_id in seen
                    or row.get("event") != "workflow_dispatch"
                ):
                    raise ContractError("orphan audit workflow run is duplicate or malformed")
                seen.add(run_id)
                results.append(row)
        latest_count, latest_first = read_page(1)
        if latest_count != count or len(latest_first) != len(first):
            raise ContractError("orphan audit workflow first page membership drift")
        for before, after in zip(first, latest_first):
            if (_snapshot_run_identity(before) != _snapshot_run_identity(after)
                    or not _snapshot_safe_run_transition(before, after)):
                raise ContractError("orphan audit workflow first page provenance drift")
        return latest_first + results[len(first):]

    def audit_commit_prs_bounded(
        self, commit_sha: str, *, max_pages: int = 3
    ) -> list[dict[str, object]]:
        """Enumerate all commit->PR associations, including the final short page."""
        if not SHA_RE.fullmatch(commit_sha):
            raise ContractError("orphan audit commit identity is invalid")
        if type(max_pages) is not int or not 1 <= max_pages <= 3:
            raise ContractError("orphan audit page budget is invalid")

        def read_page(page: int) -> list[dict[str, object]]:
            return self._read_array(
                f"commits/{commit_sha}/pulls?per_page=100&page={page}"
            )

        first = read_page(1)
        seen: set[int] = set()
        results: list[dict[str, object]] = []
        for page in range(1, max_pages + 1):
            rows = first if page == 1 else read_page(page)
            if len(rows) > 100:
                raise ContractError("orphan audit commit page exceeds GitHub page size")
            for row in rows:
                pr_id = row.get("number")
                if type(pr_id) is not int or pr_id <= 0 or pr_id in seen:
                    raise ContractError("orphan audit commit PR association is duplicate or malformed")
                seen.add(pr_id)
                results.append(row)
            if len(rows) < 100:
                if read_page(1) != first:
                    raise ContractError("orphan audit commit first page changed during inspection")
                return results
        raise ContractError("orphan audit commit association page budget exhausted")



    def audit_commit_prs_link_bounded(
        self, commit_sha: str, *, max_pages: int = 3
    ) -> list[dict[str, object]]:
        """GET-only, Link-aware double-pass audit. Never a cancellation grant."""
        if not SHA_RE.fullmatch(commit_sha):
            raise ContractError("Link audit needs exact commit SHA")
        if type(max_pages) is not int or not 1 <= max_pages <= 3:
            raise ContractError("Link audit page budget invalid")
        root = f"{self.api_url}/repos/{self.repository}/commits/{commit_sha}/pulls"
        target = urllib.parse.urlsplit(root)

        def read_page(page: int) -> tuple[list[dict[str, object]], bool]:
            request = urllib.request.Request(
                f"{root}?per_page=100&page={page}",
                headers={
                    "Authorization": f"Bearer {self.token}",
                    "Accept": "application/vnd.github+json",
                    "X-GitHub-Api-Version": "2022-11-28",
                    "User-Agent": "oxide-batch-link-audit",
                },
            )
            try:
                with self.opener(request, timeout=20) as response:
                    rows = json.load(response)
                    link = response.headers.get("Link")
            except Exception as exc:
                raise ContractError(f"Link audit GET failed: {exc}") from exc
            if (
                not isinstance(rows, list) or len(rows) > 100
                or any(not isinstance(x, dict) for x in rows)
                or (link is not None and not isinstance(link, str))
            ):
                raise ContractError("Link audit malformed page")
            relations: dict[str, int] = {}
            if link:
                for element in link.split(","):
                    item = re.fullmatch(
                        r'\s*<([^<>]+)>;\s*rel="(next|prev|first|last)"\s*',
                        element,
                    )
                    if not item:
                        raise ContractError("Link audit malformed relation")
                    url, rel = item.groups()
                    parsed = urllib.parse.urlsplit(url)
                    try:
                        query = urllib.parse.parse_qs(
                            parsed.query, keep_blank_values=True, strict_parsing=True
                        )
                    except ValueError as exc:
                        raise ContractError("Link audit bad query") from exc
                    number = query.get("page")
                    if (
                        rel in relations
                        or (parsed.scheme, parsed.netloc, parsed.path) !=
                        (target.scheme, target.netloc, target.path)
                        or parsed.fragment or parsed.username or parsed.password
                        or set(query) != {"page", "per_page"}
                        or query.get("per_page") != ["100"]
                        or not isinstance(number, list) or len(number) != 1
                        or not re.fullmatch(r"[1-9][0-9]*", number[0])
                    ):
                        raise ContractError("Link audit foreign or duplicate pagination")
                    relations[rel] = int(number[0])
                if (
                    ("next" in relations and relations["next"] != page + 1)
                    or ("prev" in relations and relations["prev"] != page - 1)
                    or ("first" in relations and relations["first"] != 1)
                    or ("last" in relations and relations["last"] < page)
                ):
                    raise ContractError("Link audit out-of-sequence pagination")
            has_next = "next" in relations
            if has_next and len(rows) != 100:
                raise ContractError("Link audit short page has next")
            return rows, has_next

        def scan() -> list[tuple[list[dict[str, object]], bool]]:
            pages: list[tuple[list[dict[str, object]], bool]] = []
            seen: set[int] = set()
            for page in range(1, max_pages + 1):
                rows, more = read_page(page)
                for item in rows:
                    number = item.get("number")
                    if type(number) is not int or number <= 0 or number in seen:
                        raise ContractError("Link audit invalid or duplicate PR number")
                    seen.add(number)
                pages.append((rows, more))
                if not more:
                    return pages
            raise ContractError("Link audit page budget exhausted")

        initial = scan()
        latest = scan()
        # GitHub mutates updated_at, mergeable status, review metadata, etc.
        # Comparing whole PR objects falsely rejects legitimate active runs.
        # Pagination shape/order and provenance-relevant PR identity must match.
        def projection(pages: list[tuple[list[dict[str, object]], bool]]) -> list[tuple[bool, list[tuple[object, ...]]]]:
            return [(more, [_snapshot_pr_identity(row) for row in rows])
                    for rows, more in pages]
        if projection(latest) != projection(initial):
            raise ContractError("Link audit stable PR identity/page drift")
        return [record for page, _ in latest for record in page]



def audit_orphan_authority_candidate(
    *,
    authority: Authority,
    old_caller: dict[str, object],
    dispatched: dict[str, object],
    live_pr: dict[str, object],
    associated_prs: list[dict[str, object]],
    pr_number: int,
    current_caller_run_id: int,
    repository: str,
) -> dict[str, object]:
    """Read-only candidate evidence; NEVER an authorization to cancel a run.

    The separate commit->pulls lookup is required because GitHub may return
    pull_requests=[] for both old PR CI and workflow_dispatch runs. Caller
    and candidate metadata alone do not establish PR ownership.
    """
    def positive(value: object) -> bool:
        return type(value) is int and value > 0

    def repo_id(run: dict[str, object], key: str) -> object:
        info = run.get(key)
        return info.get("id") if isinstance(info, dict) else None

    if not positive(pr_number) or not positive(current_caller_run_id):
        raise ContractError("orphan audit needs exact positive PR/current caller IDs")
    base, head = live_pr.get("base"), live_pr.get("head")
    if not isinstance(base, dict) or not isinstance(head, dict):
        raise ContractError("orphan audit live PR base/head is malformed")
    base_repo, head_repo = base.get("repo"), head.get("repo")
    if not isinstance(base_repo, dict) or not isinstance(head_repo, dict):
        raise ContractError("orphan audit live PR repositories are malformed")
    repository_id = base_repo.get("id")
    if (
        live_pr.get("number") != pr_number
        or live_pr.get("state") != "open"
        or live_pr.get("draft") is not False
        or base_repo.get("full_name") != repository
        or head_repo.get("full_name") != repository
        or not positive(repository_id)
        or head_repo.get("id") != repository_id
        or base.get("ref") != "main"
        or not isinstance(head.get("ref"), str)
        or not SHA_RE.fullmatch(str(base.get("sha", "")))
        or not SHA_RE.fullmatch(str(head.get("sha", "")))
    ):
        raise ContractError("orphan audit live PR identity is not trusted")
    old_id = old_caller.get("id")
    old_attempt = old_caller.get("run_attempt")
    old_sha = old_caller.get("head_sha")
    if (
        not positive(old_id)
        or old_id >= current_caller_run_id
        or not positive(old_attempt)
        or not isinstance(old_sha, str)
        or not SHA_RE.fullmatch(old_sha)
        or old_sha == head["sha"]
        or old_caller.get("event") != "pull_request"
        or old_caller.get("path") != ".github/workflows/pr-ci.yml"
        or old_caller.get("head_branch") != head["ref"]
        or repo_id(old_caller, "repository") != repository_id
        or repo_id(old_caller, "head_repository") != repository_id
    ):
        raise ContractError("orphan audit old caller identity is not trusted")
    if not isinstance(associated_prs, list) or sum(
        type(pr.get("number")) is int and pr["number"] == pr_number
        for pr in associated_prs if isinstance(pr, dict)
    ) != 1:
        raise ContractError("orphan audit old commit lacks unique PR association")
    association = next(
        pr for pr in associated_prs
        if isinstance(pr, dict) and pr.get("number") == pr_number
    )
    linked_head, linked_base = association.get("head"), association.get("base")
    if (
        not isinstance(linked_head, dict)
        or not isinstance(linked_base, dict)
        or linked_head.get("ref") != head["ref"]
        or linked_base.get("ref") != "main"
    ):
        raise ContractError("orphan audit commit/PR association conflicts")
    run_id = dispatched.get("id")
    actor = dispatched.get("actor")
    if (
        not positive(run_id)
        or run_id == old_id
        or dispatched.get("run_attempt") != 1
        or dispatched.get("event") != "workflow_dispatch"
        or dispatched.get("path") != authority.workflow
        or dispatched.get("head_sha") != base["sha"]
        or dispatched.get("head_branch") != "main"
        or dispatched.get("status") not in {"queued", "in_progress", "waiting", "pending", "requested"}
        or dispatched.get("conclusion") is not None
        or not isinstance(actor, dict)
        or actor.get("login") != "github-actions[bot]"
        or repo_id(dispatched, "repository") != repository_id
        or repo_id(dispatched, "head_repository") != repository_id
        or dispatched.get("display_title") != expected_run_name(
            authority, pr_number=str(pr_number), head_sha=old_sha,
            caller_run_id=str(old_id), caller_run_attempt=str(old_attempt),
        )
    ):
        raise ContractError("orphan audit dispatched run identity is not trusted")
    return {
        "candidate_only": True,
        "cancel_authorized": False,
        "authority": authority.id,
        "old_caller_run_id": old_id,
        "old_head_sha": old_sha,
        "dispatched_run_id": run_id,
    }


def audit_orphan_live_readonly(
    *,
    client: GitHubActionsClient,
    authority: Authority,
    pr_number: int,
    old_caller_run_id: int,
    current_caller_run_id: int,
    dispatched_run_id: int,
    expected_base_sha: str,
    expected_current_head_sha: str,
    trusted_main_sha: str,
) -> dict[str, object]:
    """GET-only, non-authorizing orphan provenance audit against live GitHub.

    A positive audit is NOT cancellation authority: GitHub provides no atomic
    commit/PR/run snapshot and rechecks cannot prevent ABA or post-read drift.
    """
    for label, number in (
        ("pr_number", pr_number),
        ("old_caller_run_id", old_caller_run_id),
        ("current_caller_run_id", current_caller_run_id),
        ("dispatched_run_id", dispatched_run_id),
    ):
        if type(number) is not int or number <= 0:
            raise ContractError(f"live orphan audit {label} must be positive")
    if len({old_caller_run_id, current_caller_run_id, dispatched_run_id}) != 3:
        raise ContractError("live orphan audit run identities are not distinct")
    for label, sha in (
        ("expected_base_sha", expected_base_sha),
        ("expected_current_head_sha", expected_current_head_sha),
        ("trusted_main_sha", trusted_main_sha),
    ):
        if not isinstance(sha, str) or not SHA_RE.fullmatch(sha):
            raise ContractError(f"live orphan audit {label} is invalid")

    main_branch = client._read_json("branches/main")
    main_info = main_branch.get("commit")
    if not isinstance(main_info, dict) or main_info.get("sha") != trusted_main_sha:
        raise ContractError("live orphan audit trusted main SHA changed")

    def fetch_run(number: int) -> dict[str, object]:
        record = client._read_json(f"actions/runs/{number}")
        if record.get("id") != number:
            raise ContractError("live orphan audit requested run ID mismatched")
        return record

    current = fetch_run(current_caller_run_id)
    old = fetch_run(old_caller_run_id)
    target = fetch_run(dispatched_run_id)
    live = client.pull_request(str(pr_number))
    current_head = live.get("head")
    current_base = live.get("base")
    if not isinstance(current_head, dict) or not isinstance(current_base, dict):
        raise ContractError("live orphan audit PR head/base is malformed")
    repo_info = current_base.get("repo")
    head_repo = current_head.get("repo")
    if not isinstance(repo_info, dict) or not isinstance(head_repo, dict):
        raise ContractError("live orphan audit PR repositories are malformed")
    repo_id = repo_info.get("id")
    if (
        live.get("number") != pr_number or live.get("state") != "open"
        or live.get("draft") is not False
        or current_base.get("ref") != "main"
        or current_base.get("sha") != expected_base_sha
        or current_head.get("sha") != expected_current_head_sha
        or repo_info.get("full_name") != client.repository
        or head_repo.get("full_name") != client.repository
        or type(repo_id) is not int or repo_id <= 0
        or head_repo.get("id") != repo_id
    ):
        raise ContractError("live orphan audit exact current PR identity changed")

    def run_repository_matches(record: dict[str, object]) -> bool:
        return (
            isinstance(record.get("repository"), dict)
            and isinstance(record.get("head_repository"), dict)
            and record["repository"].get("id") == repo_id
            and record["head_repository"].get("id") == repo_id
        )

    # Each denial reports the failing invariant without emitting mutable
    # GitHub JSON, tokens, identities or attacker-controlled values. A safe
    # diagnosis is NOT authorization to cancel, and all checks remain strict.
    current_caller_checks = (
        ("event", current.get("event") == "pull_request"),
        ("workflow", current.get("path") == ".github/workflows/pr-ci.yml"),
        ("head_sha", current.get("head_sha") == expected_current_head_sha),
        ("head_branch", current.get("head_branch") == current_head.get("ref")),
        ("run_attempt", current.get("run_attempt") == 1),
        ("repository", run_repository_matches(current)),
        ("active_status", current.get("status") in {"in_progress", "completed"}),
        ("completed_success", current.get("status") != "completed"
         or current.get("conclusion") == "success"),
        ("active_no_conclusion", current.get("status") != "in_progress"
         or current.get("conclusion") is None),
    )
    for invariant, trusted in current_caller_checks:
        if not trusted:
            raise ContractError(
                f"live orphan audit current caller is not trusted: {invariant}"
            )
    if (
        old.get("status") != "completed" or old.get("conclusion") != "cancelled"
        or old.get("run_attempt") != 1
    ):
        raise ContractError("live orphan audit old caller is not cancelled")

    old_sha = old.get("head_sha")
    if not isinstance(old_sha, str) or not SHA_RE.fullmatch(old_sha):
        raise ContractError("live orphan audit old caller SHA is malformed")
    associations = client.audit_commit_prs_link_bounded(old_sha)
    result = audit_orphan_authority_candidate(
        authority=authority,
        old_caller=old,
        dispatched=target,
        live_pr=live,
        associated_prs=associations,
        pr_number=pr_number,
        current_caller_run_id=current_caller_run_id,
        repository=client.repository,
    )

    # A GitHub run can advance while these independent GETs are in flight.
    # Compare trusted identities, not whole mutable API records (updated_at,
    # status, timing, PR descriptions). This is still NOT an atomic snapshot.
    def repo_identity(value: object) -> tuple[object, object] | None:
        if not isinstance(value, dict):
            return None
        return value.get("id"), value.get("full_name")

    def pr_identity(value: dict[str, object]) -> tuple[object, ...]:
        base, head = value.get("base"), value.get("head")
        if not isinstance(base, dict) or not isinstance(head, dict):
            return (None,)
        return (
            value.get("number"), value.get("state"), value.get("draft"),
            base.get("ref"), base.get("sha"), repo_identity(base.get("repo")),
            head.get("ref"), head.get("sha"), repo_identity(head.get("repo")),
        )

    def run_identity(value: dict[str, object]) -> tuple[object, ...]:
        actor = value.get("actor")
        return (
            value.get("id"), value.get("event"), value.get("path"),
            value.get("head_sha"), value.get("head_branch"),
            value.get("run_attempt"), value.get("display_title"),
            actor.get("login") if isinstance(actor, dict) else None,
            repo_identity(value.get("repository")),
            repo_identity(value.get("head_repository")),
        )

    reread_pr = client.pull_request(str(pr_number))
    reread_current = fetch_run(current_caller_run_id)
    reread_old = fetch_run(old_caller_run_id)
    reread_target = fetch_run(dispatched_run_id)
    reread_main = client._read_json("branches/main")
    if pr_identity(reread_pr) != pr_identity(live):
        raise ContractError("live orphan audit PR identity changed during final readback")
    for label, earlier, later in (
        ("current caller", current, reread_current),
        ("old caller", old, reread_old),
        ("CodeQL target", target, reread_target),
    ):
        if run_identity(later) != run_identity(earlier):
            raise ContractError(
                f"live orphan audit {label} identity changed during final readback"
            )
    new_main = reread_main.get("commit")
    if not isinstance(new_main, dict) or new_main.get("sha") != trusted_main_sha:
        raise ContractError("live orphan audit main identity changed during final readback")

    # Permit in-progress -> completed/success for the NEW caller, but never
    # accept a failed/retried caller or any backwards state transition.
    new_current_state = (reread_current.get("status"), reread_current.get("conclusion"))
    if new_current_state not in {
        ("in_progress", None), ("completed", "success")
    } or (
        current.get("status") == "completed"
        and new_current_state != ("completed", "success")
    ):
        raise ContractError("live orphan audit current caller state changed unsafely")
    if (reread_old.get("status"), reread_old.get("conclusion")) != (
        "completed", "cancelled"
    ):
        raise ContractError("live orphan audit old caller state changed unsafely")
    active = {"queued", "in_progress", "waiting", "pending", "requested"}
    if (
        reread_target.get("status") not in active
        or reread_target.get("conclusion") is not None
        or (target.get("status") == "in_progress"
            and reread_target.get("status") != "in_progress")
    ):
        raise ContractError("live orphan audit CodeQL target state changed unsafely")
    return {
        "schema": "orphan-readonly-audit-v1",
        "decision": "READ_ONLY_CANDIDATE",
        "candidate_only": True,
        "cancel_authorized": False,
        "transactional_snapshot": False,
        "trusted_main_sha": trusted_main_sha,
        "current_head_sha": expected_current_head_sha,
        "current_caller_run_id": current_caller_run_id,
        "pr_number": pr_number,
        "base_sha": expected_base_sha,
        **result,
    }



def audit_orphan_auto_readonly(
    *,
    client: GitHubActionsClient,
    authorities: list[Authority],
    old_caller_run_id: int,
    trusted_main_sha: str,
    authority_id: str = "codeql",
) -> dict[str, object]:
    """Conservative GET-only workflow_run discovery, never cancellation consent.

    Completed non-cancelled PR CI events are intentionally non-candidates.
    Ambiguous provenance, partial pages and untrusted metadata are failures.
    """
    if type(old_caller_run_id) is not int or old_caller_run_id <= 0:
        raise ContractError("automatic orphan audit old caller ID invalid")
    if not isinstance(trusted_main_sha, str) or not SHA_RE.fullmatch(trusted_main_sha):
        raise ContractError("automatic orphan audit trusted main SHA invalid")
    choices = [a for a in authorities if a.id == authority_id]
    if len(choices) != 1:
        raise ContractError("automatic orphan audit authority not in protected policy")
    authority = choices[0]

    def skip(reason: str) -> dict[str, object]:
        return {
            "schema": "orphan-readonly-audit-v1", "decision": "NO_CANDIDATE",
            "reason": reason, "old_caller_run_id": old_caller_run_id,
            "candidate_only": False, "cancel_authorized": False,
            "transactional_snapshot": False,
        }

    old = client._read_json(f"actions/runs/{old_caller_run_id}")
    old_sha = old.get("head_sha")
    old_repo, old_head_repo = old.get("repository"), old.get("head_repository")
    if (
        old.get("id") != old_caller_run_id or old.get("event") != "pull_request"
        or old.get("path") != ".github/workflows/pr-ci.yml"
        or old.get("run_attempt") != 1
        or not isinstance(old_sha, str) or not SHA_RE.fullmatch(old_sha)
        or not isinstance(old_repo, dict) or not isinstance(old_head_repo, dict)
        or old_repo.get("full_name") != client.repository
        or old_head_repo.get("full_name") != client.repository
        or type(old_repo.get("id")) is not int or old_repo["id"] <= 0
        or old_head_repo.get("id") != old_repo["id"]
        or not isinstance(old.get("head_branch"), str)
    ):
        raise ContractError("automatic orphan audit old caller is not trusted")
    if old.get("status") != "completed":
        raise ContractError("automatic orphan audit triggering caller not completed")
    if old.get("conclusion") != "cancelled":
        return skip("old_caller_not_cancelled")

    associations = client.audit_commit_prs_link_bounded(old_sha)
    if len(associations) != 1:
        raise ContractError("automatic orphan audit old commit association ambiguous")
    linked = associations[0]
    linked_head, linked_base = linked.get("head"), linked.get("base")
    number = linked.get("number")
    if (
        type(number) is not int or number <= 0
        or not isinstance(linked_head, dict) or not isinstance(linked_base, dict)
        or linked_head.get("ref") != old["head_branch"]
        or linked_base.get("ref") != "main"
    ):
        raise ContractError("automatic orphan audit old commit association invalid")

    live = client.pull_request(str(number))
    if live.get("number") != number:
        raise ContractError("automatic orphan audit association/PR number mismatch")
    if live.get("state") != "open" or live.get("draft") is not False:
        return skip("pr_not_ready")
    live_head, live_base = live.get("head"), live.get("base")
    if not isinstance(live_head, dict) or not isinstance(live_base, dict):
        raise ContractError("automatic orphan audit live PR malformed")
    current_sha, base_sha = live_head.get("sha"), live_base.get("sha")
    base_repo, head_repo = live_base.get("repo"), live_head.get("repo")
    if (
        not isinstance(current_sha, str) or not SHA_RE.fullmatch(current_sha)
        or not isinstance(base_sha, str) or not SHA_RE.fullmatch(base_sha)
        or not isinstance(base_repo, dict) or not isinstance(head_repo, dict)
        or base_repo.get("full_name") != client.repository
        or head_repo.get("full_name") != client.repository
        or base_repo.get("id") != old_repo["id"]
        or head_repo.get("id") != old_repo["id"]
        or live_base.get("ref") != "main"
        or live_head.get("ref") != old["head_branch"]
    ):
        raise ContractError("automatic orphan audit live PR identity ambiguous")
    if current_sha == old_sha:
        return skip("head_not_superseded")

    # Fully bounded exact-head PR CI discovery; a duplicate/rerun is not accepted.
    path = (
        "actions/workflows/pr-ci.yml/runs?event=pull_request&"
        f"head_sha={current_sha}&per_page=100"
    )
    initial = client._read_json(path)
    total, rows = initial.get("total_count"), initial.get("workflow_runs")
    if (
        type(total) is not int or not 1 <= total <= 100
        or not isinstance(rows, list) or len(rows) != total
        or any(not isinstance(x, dict) for x in rows)
    ):
        raise ContractError("automatic orphan audit current run page incomplete")
    latest = client._read_json(path)
    latest_count, latest_rows = latest.get("total_count"), latest.get("workflow_runs")
    if (latest_count != total or not isinstance(latest_rows, list)
            or len(latest_rows) != len(rows)
            or any(not isinstance(row, dict) for row in latest_rows)):
        raise ContractError("automatic orphan audit current run page membership drift")
    for before, after in zip(rows, latest_rows):
        if (_snapshot_run_identity(before) != _snapshot_run_identity(after)
                or not _snapshot_safe_run_transition(before, after)):
            raise ContractError("automatic orphan audit current run page provenance drift")
    rows = latest_rows
    matches = [
        run for run in rows
        if (
            run.get("event") == "pull_request"
            and run.get("path") == ".github/workflows/pr-ci.yml"
            and run.get("head_sha") == current_sha
            and run.get("head_branch") == live_head["ref"]
            and isinstance(run.get("repository"), dict)
            and run["repository"].get("id") == old_repo["id"]
            and isinstance(run.get("head_repository"), dict)
            and run["head_repository"].get("id") == old_repo["id"]
            and run.get("run_attempt") == 1
        )
    ]
    if len(matches) != 1 or len(rows) != 1:
        raise ContractError("automatic orphan audit current caller not unique")
    current_id = matches[0].get("id")
    if type(current_id) is not int or current_id <= old_caller_run_id:
        raise ContractError("automatic orphan audit current caller chronology invalid")

    # Same M2 bounded GET method; no raw workflow list can grant cancellation.
    rows = client.audit_workflow_runs_bounded(authority.workflow)
    expected = expected_run_name(
        authority, pr_number=str(number), head_sha=old_sha,
        caller_run_id=str(old_caller_run_id), caller_run_attempt="1",
    )
    targets = [row for row in rows if row.get("display_title") == expected]
    if len(targets) > 1:
        raise ContractError("automatic orphan audit duplicate old Authority target")
    if not targets:
        return skip("old_authority_not_found")
    target = targets[0]
    if target.get("status") == "completed":
        return skip("old_authority_already_completed")
    target_id = target.get("id")
    if type(target_id) is not int or target_id <= 0:
        raise ContractError("automatic orphan audit old Authority run ID invalid")

    # Reuse Stage M4 live GET/provenance and M3 Link double-pass, with final
    # independent PR/run/main GET readback. No atomicity or cancellation grant.
    return audit_orphan_live_readonly(
        client=client, authority=authority, pr_number=number,
        old_caller_run_id=old_caller_run_id,
        current_caller_run_id=current_id, dispatched_run_id=target_id,
        expected_base_sha=base_sha, expected_current_head_sha=current_sha,
        trusted_main_sha=trusted_main_sha,
    )


def preflight_orphan_codeql_cancel(
    *,
    client: GitHubActionsClient,
    authorities: list[Authority],
    old_caller_run_id: int,
    trusted_main_sha: str,
) -> dict[str, object]:
    """M5 pre-write boundary: independently rediscover and recheck CodeQL.

    Not a cancellation grant: GitHub has no conditional run-attempt cancel
    and no atomic PR/run snapshot. The future writer must recheck before POST.
    """
    choices = [a for a in authorities if a.id == "codeql"]
    if len(choices) != 1 or choices[0].workflow != ".github/workflows/codeql.yml":
        raise ContractError("M5 preflight requires exact protected CodeQL workflow")

    first = audit_orphan_auto_readonly(
        client=client, authorities=authorities,
        old_caller_run_id=old_caller_run_id,
        trusted_main_sha=trusted_main_sha, authority_id="codeql",
    )
    if first.get("decision") == "NO_CANDIDATE":
        return {
            "schema": "orphan-cancel-preflight-v1", "decision": "NO_WRITE",
            "reason": first["reason"], "cancel_authorized": False,
            "transactional_snapshot": False,
        }
    if (
        first.get("decision") != "READ_ONLY_CANDIDATE"
        or first.get("candidate_only") is not True
        or first.get("cancel_authorized") is not False
        or first.get("transactional_snapshot") is not False
        or first.get("authority") != "codeql"
        or type(first.get("pr_number")) is not int
        or type(first.get("dispatched_run_id")) is not int
    ):
        raise ContractError("M5 preflight cannot promote audit into write authority")

    # Independently GET all live identities again, never trust an earlier
    # observer report or the workflow_run payload as cancellation permission.
    second = audit_orphan_live_readonly(
        client=client, authority=choices[0],
        pr_number=first["pr_number"],
        old_caller_run_id=old_caller_run_id,
        current_caller_run_id=first["current_caller_run_id"],
        dispatched_run_id=first["dispatched_run_id"],
        expected_base_sha=first["base_sha"],
        expected_current_head_sha=first["current_head_sha"],
        trusted_main_sha=trusted_main_sha,
    )
    for key in (
        "authority", "pr_number", "base_sha", "old_caller_run_id",
        "old_head_sha", "current_head_sha", "current_caller_run_id",
        "dispatched_run_id", "trusted_main_sha",
    ):
        if first.get(key) != second.get(key):
            raise ContractError("M5 preflight identity drift on independent recheck")
    target_id = first["dispatched_run_id"]
    target = client._read_json(f"actions/runs/{target_id}")
    if (
        target.get("id") != target_id
        or target.get("run_attempt") != 1
        or target.get("path") != choices[0].workflow
        or target.get("status") not in {"queued", "in_progress", "waiting", "pending", "requested"}
        or target.get("conclusion") is not None
    ):
        raise ContractError("M5 preflight target changed before write boundary")
    return {
        "schema": "orphan-cancel-preflight-v1",
        "decision": "PREWRITE_CANDIDATE",
        "authority": "codeql",
        "pr_number": first["pr_number"],
        "base_sha": first["base_sha"],
        "old_caller_run_id": old_caller_run_id,
        "old_head_sha": first["old_head_sha"],
        "current_head_sha": first["current_head_sha"],
        "current_caller_run_id": first["current_caller_run_id"],
        "dispatched_run_id": target_id,
        "run_attempt": 1,
        "cancel_authorized": False,
        "transactional_snapshot": False,
    }


def verify_runs(
    *,
    authorities: list[Authority],
    required: list[Authority],
    client: GitHubActionsClient | object,
    dispatch_result: str,
    pr_number: str,
    base_sha: str,
    head_sha: str,
    head_repo: str,
    caller_run_id: str,
    caller_run_attempt: str,
    timeout_seconds: float,
    poll_interval_seconds: float,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    if dispatch_result != "success":
        raise ContractError(
            f"dispatch-authorities did not succeed (result={dispatch_result!r})"
        )
    if timeout_seconds < 0 or timeout_seconds > 900:
        raise ContractError("timeout_seconds must be between 0 and 900")
    if poll_interval_seconds < 0 or poll_interval_seconds > 60:
        raise ContractError("poll_interval_seconds must be between 0 and 60")

    required_ids = {authority.id for authority in required}
    deadline = monotonic() + timeout_seconds

    while True:
        # An older workflow may keep polling independently dispatched jobs
        # after the PR head changes. Never use stale-head results as proof.
        live = client.pull_request(pr_number)
        if not isinstance(live, dict):
            raise ContractError("live PR response must be an object")
        live_base, live_head = live.get("base"), live.get("head")
        if not isinstance(live_base, dict) or not isinstance(live_head, dict):
            raise ContractError("live PR base/head metadata is malformed")
        base_repo, head_repo_data = live_base.get("repo"), live_head.get("repo")
        if not isinstance(base_repo, dict) or not isinstance(head_repo_data, dict):
            raise ContractError("live PR repository metadata is malformed")
        if live.get("state") != "open" or live.get("draft") is not False:
            raise ContractError("live PR is closed or no longer ready for proof")
        if live_base.get("sha") != base_sha or base_repo.get("full_name") != client.repository:
            raise ContractError("live PR base identity changed during polling")
        if live_head.get("sha") != head_sha or head_repo_data.get("full_name") != head_repo:
            raise ContractError("live PR head identity changed during polling")

        waiting: list[str] = []
        evidence: dict[str, dict[str, object]] = {}

        for authority in authorities:
            title = expected_run_name(
                authority,
                pr_number=pr_number,
                head_sha=head_sha,
                caller_run_id=caller_run_id,
                caller_run_attempt=caller_run_attempt,
            )
            runs = client.workflow_runs(authority.workflow)
            matches = [
                run
                for run in runs
                if run.get("event") == "workflow_dispatch"
                and run.get("display_title") == title
            ]
            if len(matches) > 1:
                raise ContractError(
                    f"{authority.id} has duplicate correlated workflow runs: "
                    + ", ".join(str(run.get("id", "unknown")) for run in matches)
                )

            required_here = authority.id in required_ids
            if not required_here:
                if matches:
                    raise ContractError(
                        f"non-applicable authority {authority.id} unexpectedly materialized "
                        f"(run_id={matches[0].get('id', 'unknown')})"
                    )
                continue

            if not matches:
                waiting.append(f"{authority.id}:missing")
                continue

            run = matches[0]
            status = run.get("status")
            conclusion = run.get("conclusion")
            if status != "completed":
                waiting.append(f"{authority.id}:{status or 'unknown'}")
                continue
            if conclusion != "success":
                raise ContractError(
                    f"required authority {authority.id} concluded {conclusion!r} "
                    f"(run_id={run.get('id', 'unknown')})"
                )
            run_id = run.get("id")
            if not isinstance(run_id, int) or run_id <= 0:
                raise ContractError(
                    f"required authority {authority.id} has invalid run id"
                )
            evidence[authority.id] = {
                "workflow": authority.workflow,
                "run_id": run_id,
                "display_title": title,
                "conclusion": "success",
            }

        if not waiting:
            return {
                "schema": PR_PROOF_SCHEMA,
                "verified": evidence,
            }

        now = monotonic()
        if now >= deadline:
            raise ContractError(
                "timed out waiting for required authority runs: " + ", ".join(waiting)
            )
        sleep(min(poll_interval_seconds, max(0.0, deadline - now)))


def add_scope_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--policy", required=True)
    parser.add_argument("--scope-result", required=True)
    parser.add_argument("--classification-outcome", required=True)
    parser.add_argument("--legacy-base", required=True)
    parser.add_argument("--docs-only", required=True)
    parser.add_argument("--evidence-impact", required=True)
    parser.add_argument("--supply-chain-impact", required=True)


def required_from_args(args: argparse.Namespace) -> tuple[list[Authority], list[Authority]]:
    authorities = load_authorities(args.policy)
    required = required_authorities(
        authorities,
        scope_result=args.scope_result,
        classification_outcome=args.classification_outcome,
        legacy_base=args.legacy_base,
        docs_only=args.docs_only,
        evidence_impact=args.evidence_impact,
        supply_chain_impact=args.supply_chain_impact,
    )
    return authorities, required


class _FakeClient:
    repository = "owner/repo"

    def __init__(self, runs: dict[str, list[dict[str, object]]]) -> None:
        self.runs = runs
        self.live_pr: dict[str, object] = {
            "state": "open",
            "draft": False,
            "base": {"sha": "a" * 40, "repo": {"full_name": self.repository}},
            "head": {"sha": "b" * 40, "repo": {"full_name": self.repository}},
        }
        self.pr_reads = 0

    def pull_request(self, number: str) -> dict[str, object]:
        self.pr_reads += 1
        return self.live_pr

    def workflow_runs(self, workflow: str) -> list[dict[str, object]]:
        return list(self.runs.get(workflow, []))


def _test_authorities() -> list[Authority]:
    return [
        Authority("rust", ".github/workflows/ci.yml", "non_docs"),
        Authority("dependency", ".github/workflows/dependency-review.yml", "non_docs"),
        Authority("codeql", ".github/workflows/codeql.yml", "non_docs"),
        Authority("evidence", ".github/workflows/evidence.yml", "evidence_impact"),
        Authority("supply", ".github/workflows/supply-chain.yml", "supply_chain_impact"),
    ]


def _run(authority: Authority, *, conclusion: str = "success", run_id: int = 10) -> dict[str, object]:
    return {
        "id": run_id,
        "event": "workflow_dispatch",
        "display_title": expected_run_name(
            authority,
            pr_number="42",
            head_sha="b" * 40,
            caller_run_id="1001",
            caller_run_attempt="2",
        ),
        "status": "completed",
        "conclusion": conclusion,
    }


class RuntimeContractTests(unittest.TestCase):




    def test_m6_snapshot_security_invariants(self) -> None:
        repo = {"id": 1, "full_name": "owner/repo"}
        before_pr = {
            "id": 22, "number": 42, "state": "open", "draft": False,
            "user": {"id": 7, "login": "owner"},
            "head": {"sha": "a" * 40, "ref": "feature", "repo": repo},
            "base": {"sha": "b" * 40, "ref": "main", "repo": repo},
            "updated_at": "old", "mergeable": None,
        }
        newer_pr = copy.deepcopy(before_pr)
        newer_pr["updated_at"] = "new"
        newer_pr["mergeable"] = True
        self.assertEqual(_snapshot_pr_identity(before_pr),
                         _snapshot_pr_identity(newer_pr))
        for path, field, value in (
            ("root", "number", 99),
            ("root", "draft", True),
            ("head", "sha", "c" * 40),
            ("head", "ref", "elsewhere"),
            ("base", "sha", "d" * 40),
            ("user", "login", "attacker"),
            ("head_repo", "id", 999),
        ):
            changed = copy.deepcopy(before_pr)
            if path == "root":
                changed[field] = value
            elif path == "head_repo":
                changed["head"]["repo"][field] = value
            else:
                changed[path][field] = value
            with self.subTest(path=path, field=field):
                self.assertNotEqual(_snapshot_pr_identity(before_pr),
                                    _snapshot_pr_identity(changed))

        before_run = {
            "id": 100, "run_attempt": 1, "event": "pull_request",
            "path": ".github/workflows/pr-ci.yml",
            "head_sha": "a" * 40, "head_branch": "feature",
            "actor": {"id": 7, "login": "owner"},
            "repository": repo, "head_repository": repo,
            "status": "queued", "conclusion": None,
        }
        active = copy.deepcopy(before_run)
        active.update(status="in_progress", updated_at="new")
        self.assertEqual(_snapshot_run_identity(before_run),
                         _snapshot_run_identity(active))
        self.assertTrue(_snapshot_safe_run_transition(before_run, active))
        done = copy.deepcopy(active)
        done.update(status="completed", conclusion="success")
        self.assertTrue(_snapshot_safe_run_transition(active, done))
        self.assertFalse(_snapshot_safe_run_transition(done, active))
        failed = copy.deepcopy(active)
        failed.update(status="completed", conclusion="failure")
        self.assertTrue(_snapshot_safe_run_transition(active, failed))
        self.assertFalse(_snapshot_safe_run_transition(failed, done))
        for key, value in (
            ("id", 101), ("run_attempt", 2), ("head_sha", "c" * 40),
            ("path", ".github/workflows/other.yml"),
            ("head_branch", "other"),
        ):
            mutated = copy.deepcopy(active)
            mutated[key] = value
            with self.subTest(key=key):
                self.assertNotEqual(_snapshot_run_identity(active),
                                    _snapshot_run_identity(mutated))
        actor_drift = copy.deepcopy(active)
        actor_drift["actor"]["login"] = "attacker"
        self.assertNotEqual(_snapshot_run_identity(active),
                            _snapshot_run_identity(actor_drift))

    def test_m6_link_page_metadata_allowed_identity_rejected(self) -> None:
        class Response(io.StringIO):
            headers: dict[str, str] = {}

        for kind in ("benign", "head-drift", "actor-drift", "page-drift"):
            calls = [0]
            def opener(request: urllib.request.Request, timeout: int) -> Response:
                calls[0] += 1
                record: dict[str, object] = {
                    "number": 466, "id": 123, "updated_at": "before",
                    "head": {"sha": "a" * 40, "ref": "feature",
                             "repo": {"id": 1, "full_name": "owner/repo"}},
                    "base": {"sha": "b" * 40, "ref": "main",
                             "repo": {"id": 1, "full_name": "owner/repo"}},
                    "user": {"id": 7, "login": "owner"},
                }
                if calls[0] == 2:
                    record["updated_at"] = "after"
                    if kind == "head-drift":
                        record["head"]["sha"] = "c" * 40
                    elif kind == "actor-drift":
                        record["user"]["login"] = "attacker"
                    elif kind == "page-drift":
                        record["number"] = 467
                return Response(json.dumps([record]))
            api = GitHubActionsClient(
                api_url="https://api.github.test", repository="owner/repo",
                token="test", opener=opener,
            )
            with self.subTest(kind=kind):
                if kind == "benign":
                    self.assertEqual(466, api.audit_commit_prs_link_bounded("a" * 40)[0]["number"])
                    self.assertEqual(2, calls[0])
                else:
                    with self.assertRaisesRegex(ContractError, "stable PR identity/page drift"):
                        api.audit_commit_prs_link_bounded("a" * 40)


    def test_link_audit_two_pages_fail_closed_and_get_only(self) -> None:
        sha = "a" * 40

        class Response(io.StringIO):
            def __init__(self, records: object, link: str | None) -> None:
                super().__init__(json.dumps(records))
                self.headers = {"Link": link} if link else {}

        class Stub:
            def __init__(self) -> None:
                self.calls: list[tuple[str, int]] = []
                self.mode = "normal"

            def __call__(self, request: urllib.request.Request, timeout: int) -> Response:
                self.calls.append((request.get_method(), timeout))
                url = urllib.parse.urlsplit(request.full_url)
                page = int(urllib.parse.parse_qs(url.query)["page"][0])
                root = request.full_url.split("?")[0]
                link_url = f"{root}?page=2&per_page=100"
                if self.mode == "foreign":
                    link_url = f"https://evil.invalid/commits/{sha}/pulls?per_page=100&page=2"
                if self.mode == "out-of-order":
                    link_url = f"{root}?per_page=100&page=3"
                if page == 1:
                    rows = [{"number": i} for i in range(1, 101)]
                    if self.mode == "short":
                        rows = rows[:3]
                    link = f'<{link_url}>; rel="next"'
                    if self.mode == "duplicate-link":
                        link += f', <{link_url}>; rel="next"'
                    if self.mode == "invalid-link":
                        link = "not a link"
                else:
                    rows = [{"number": 101}]
                    if self.mode == "drift" and len(self.calls) >= 4:
                        rows = [{"number": 102}]
                    link = None
                return Response(rows, link)

        api_stub = Stub()
        api = GitHubActionsClient(
            api_url="https://api.github.test", repository="owner/repo",
            token="test", opener=api_stub,
        )
        self.assertEqual(
            list(range(1, 102)),
            [row["number"] for row in api.audit_commit_prs_link_bounded(sha)],
        )
        self.assertEqual([("GET", 20)] * 4, api_stub.calls)
        for mode in ("foreign", "out-of-order", "short", "duplicate-link", "invalid-link", "drift"):
            with self.subTest(mode=mode):
                stub = Stub()
                stub.mode = mode
                other = GitHubActionsClient(
                    api_url="https://api.github.test", repository="owner/repo",
                    token="test", opener=stub,
                )
                with self.assertRaises(ContractError):
                    other.audit_commit_prs_link_bounded(sha)
        with self.assertRaisesRegex(ContractError, "page budget"):
            api.audit_commit_prs_link_bounded(sha, max_pages=1)
        with self.assertRaises(ContractError):
            api.audit_commit_prs_link_bounded("bad")


    def test_m6_bounded_workflow_page_forward_progress(self) -> None:
        class MutableClient(GitHubActionsClient):
            def __init__(self, mode: str) -> None:
                super().__init__(
                    api_url="https://api.github.test", repository="owner/repo",
                    token="test", opener=lambda *_a, **_k: None,
                )
                self.calls = 0
                self.mode = mode

            def _read_json(self, path: str) -> dict[str, object]:
                self.calls += 1
                run = {
                    "id": 81, "event": "workflow_dispatch", "run_attempt": 1,
                    "path": ".github/workflows/codeql.yml",
                    "head_sha": "a" * 40, "status": "in_progress",
                    "conclusion": None,
                    "repository": {"id": 1, "full_name": "owner/repo"},
                }
                if self.calls == 2:
                    run["updated_at"] = "new timestamp"
                    if self.mode == "status-forward":
                        run.update(status="completed", conclusion="success")
                    elif self.mode == "attempt-drift":
                        run["run_attempt"] = 2
                    elif self.mode == "repo-drift":
                        run["repository"]["id"] = 2
                    elif self.mode == "head-drift":
                        run["head_sha"] = "b" * 40
                return {"total_count": 1, "workflow_runs": [run]}

        for mode in ("metadata", "status-forward"):
            with self.subTest(mode=mode):
                client = MutableClient(mode)
                self.assertEqual([81], [
                    row["id"] for row in client.audit_workflow_runs_bounded(
                        ".github/workflows/codeql.yml"
                    )
                ])
                self.assertEqual(2, client.calls)
        for mode in ("attempt-drift", "repo-drift", "head-drift"):
            with self.subTest(mode=mode), self.assertRaisesRegex(
                ContractError, "first page provenance drift"
            ):
                MutableClient(mode).audit_workflow_runs_bounded(
                    ".github/workflows/codeql.yml"
                )

    def test_link_audit_single_page_still_get_only(self) -> None:
        class Response(io.StringIO):
            headers: dict[str, str] = {}
        verbs: list[str] = []
        def opener(request: urllib.request.Request, timeout: int) -> Response:
            verbs.append(request.get_method())
            return Response('[{"number":451}]')
        api = GitHubActionsClient(
            api_url="https://api.github.test", repository="owner/repo",
            token="test", opener=opener,
        )
        self.assertEqual(
            [{"number": 451}], api.audit_commit_prs_link_bounded("f" * 40)
        )
        self.assertEqual(["GET", "GET"], verbs)

    def test_orphan_readonly_bounded_workflow_and_commit_collection(self) -> None:
        class Stub(GitHubActionsClient):
            def __init__(self) -> None:
                super().__init__(
                    api_url="https://api.github.test", repository="owner/repo",
                    token="test", opener=lambda *_a, **_k: None,
                )
                self.workflow_count = 2
                self.workflow_rows = [
                    {"id": 10, "event": "workflow_dispatch"},
                    {"id": 11, "event": "workflow_dispatch"},
                ]
                self.associations = [{"number": 42}]
                self.calls: list[str] = []

            def _read_json(self, path: str) -> dict[str, object]:
                self.calls.append(path)
                return {
                    "total_count": self.workflow_count,
                    "workflow_runs": copy.deepcopy(self.workflow_rows),
                }

            def _read_array(self, path: str) -> list[dict[str, object]]:
                self.calls.append(path)
                return copy.deepcopy(self.associations)

        stub = Stub()
        self.assertEqual(
            [10, 11],
            [r["id"] for r in stub.audit_workflow_runs_bounded(
                ".github/workflows/codeql.yml"
            )],
        )
        self.assertEqual(2, len(stub.calls))
        self.assertEqual(stub.calls[0], stub.calls[1])
        self.assertIn("event=workflow_dispatch&per_page=100&page=1", stub.calls[0])
        stub.calls.clear()
        self.assertEqual(
            [42],
            [p["number"] for p in stub.audit_commit_prs_bounded("a" * 40)],
        )
        self.assertEqual(2, len(stub.calls))
        self.assertEqual(stub.calls[0], stub.calls[1])
        self.assertIn("/pulls?per_page=100&page=1", stub.calls[0])

        for name, adjust in (
            ("bad count", lambda c: setattr(c, "workflow_count", True)),
            ("too many", lambda c: setattr(c, "workflow_count", 301)),
            ("short response", lambda c: setattr(c, "workflow_count", 3)),
            ("duplicate run", lambda c: c.workflow_rows[1].update(id=10)),
            ("wrong event", lambda c: c.workflow_rows[1].update(event="push")),
            ("bad id", lambda c: c.workflow_rows[1].update(id=True)),
        ):
            with self.subTest(case=name):
                c = Stub()
                adjust(c)
                with self.assertRaises(ContractError):
                    c.audit_workflow_runs_bounded(".github/workflows/codeql.yml")
        for name, rows in (
            ("duplicate PR", [{"number": 42}, {"number": 42}]),
            ("bad PR", [{"number": False}]),
        ):
            with self.subTest(case=name):
                c = Stub()
                c.associations = rows
                with self.assertRaises(ContractError):
                    c.audit_commit_prs_bounded("a" * 40)

        class ChangingStub(Stub):
            def _read_json(self, path: str) -> dict[str, object]:
                result = super()._read_json(path)
                if self.calls.count(path) > 1:
                    result["workflow_runs"] = [{"id": 13, "event": "workflow_dispatch"}]
                    result["total_count"] = 1
                return result

            def _read_array(self, path: str) -> list[dict[str, object]]:
                rows = super()._read_array(path)
                if self.calls.count(path) > 1:
                    return [{"number": 43}]
                return rows

        with self.assertRaises(ContractError):
            ChangingStub().audit_workflow_runs_bounded(".github/workflows/ci.yml")
        with self.assertRaises(ContractError):
            ChangingStub().audit_commit_prs_bounded("a" * 40)
        with self.assertRaises(ContractError):
            Stub().audit_workflow_runs_bounded("../bad")
        with self.assertRaises(ContractError):
            Stub().audit_commit_prs_bounded("broken")
        with self.assertRaises(ContractError):
            Stub().audit_commit_prs_bounded("a" * 40, max_pages=0)

    def test_orphan_readonly_rejects_full_last_commit_page(self) -> None:
        class FullStub(GitHubActionsClient):
            def __init__(self) -> None:
                super().__init__(
                    api_url="https://api.github.test", repository="owner/repo",
                    token="test", opener=lambda *_a, **_k: None,
                )
            def _read_array(self, path: str) -> list[dict[str, object]]:
                page = int(path.rsplit("=", 1)[-1])
                return [{"number": page * 100 + i} for i in range(100)]
        with self.assertRaisesRegex(ContractError, "page budget exhausted"):
            FullStub().audit_commit_prs_bounded("a" * 40, max_pages=2)

    def test_orphan_readonly_list_get_is_not_write(self) -> None:
        observed: dict[str, object] = {}
        def opener(request: urllib.request.Request, timeout: int) -> io.StringIO:
            observed["url"] = request.full_url
            observed["verb"] = request.get_method()
            observed["timeout"] = timeout
            return io.StringIO('[{"number":42}]')
        client = GitHubActionsClient(
            api_url="https://api.github.test", repository="owner/repo",
            token="test", opener=opener,
        )
        self.assertEqual(42, client._read_array("commits/" + "a" * 40 + "/pulls")[0]["number"])
        self.assertEqual("GET", observed["verb"])
        self.assertEqual(20, observed["timeout"])
        self.assertIn("/commits/" + "a" * 40 + "/pulls", observed["url"])


    def test_orphan_audit_read_only_candidate_and_adversarial_inputs(self) -> None:
        # Model the real Stage L shape: dispatched run head_sha is *base*,
        # and both run.pull_requests arrays may be empty.
        authority = _test_authorities()[2]
        repo = {"id": 1315088383, "full_name": "owner/repo"}
        def fixture() -> dict[str, object]:
            return {
                "authority": authority,
                "old_caller": {
                    "id": 1001, "run_attempt": 1, "event": "pull_request",
                    "path": ".github/workflows/pr-ci.yml",
                    "head_sha": "b" * 40, "head_branch": "feat/pr-42",
                    "repository": dict(repo), "head_repository": dict(repo), "pull_requests": [],
                },
                "dispatched": {
                    "id": 1002, "run_attempt": 1, "event": "workflow_dispatch",
                    "path": authority.workflow, "head_sha": "a" * 40,
                    "head_branch": "main", "status": "in_progress",
                    "conclusion": None, "actor": {"login": "github-actions[bot]"},
                    "repository": dict(repo), "head_repository": dict(repo), "pull_requests": [],
                    "display_title": expected_run_name(
                        authority, pr_number="42", head_sha="b" * 40,
                        caller_run_id="1001", caller_run_attempt="1",
                    ),
                },
                "live_pr": {
                    "number": 42, "state": "open", "draft": False,
                    "base": {"sha": "a" * 40, "ref": "main", "repo": dict(repo)},
                    "head": {"sha": "c" * 40, "ref": "feat/pr-42", "repo": dict(repo)},
                },
                "associated_prs": [
                    {"number": 42, "head": {"ref": "feat/pr-42"}, "base": {"ref": "main"}}
                ],
                "pr_number": 42, "current_caller_run_id": 2001,
                "repository": "owner/repo",
            }
        result = audit_orphan_authority_candidate(**fixture())
        self.assertTrue(result["candidate_only"])
        self.assertIs(result["cancel_authorized"], False)
        self.assertEqual(1002, result["dispatched_run_id"])
        cases = {
            "no commit association": ("associated_prs", []),
            "ambiguous association": ("associated_prs", fixture()["associated_prs"] * 2),
            "wrong PR": ("pr_number", 43),
            "wrong repository": ("repository", "foreign/repo"),
            "untrusted actor": ("dispatched.actor.login", "attacker"),
            "spoofed title": ("dispatched.display_title", "pr-authority/codeql/spoof"),
            "unrelated workflow": ("dispatched.path", ".github/workflows/ci.yml"),
            "completed": ("dispatched.status", "completed"),
            "rerun": ("dispatched.run_attempt", 2),
            "foreign dispatched repo": ("dispatched.repository.id", 111),
            "reused branch": ("old_caller.head_branch", "old/branch"),
            "current head": ("old_caller.head_sha", "c" * 40),
            "invalid old event": ("old_caller.event", "workflow_dispatch"),
            "foreign old caller": ("old_caller.head_repository.id", 111),
            "invalid caller order": ("current_caller_run_id", 999),
            "draft PR": ("live_pr.draft", True),
            "closed PR": ("live_pr.state", "closed"),
            "wrong base": ("live_pr.base.sha", "f" * 40),
        }
        for label, (path, value) in cases.items():
            case = copy.deepcopy(fixture())
            target = case
            segments = path.split(".")
            for segment in segments[:-1]:
                target = target[segment]
            target[segments[-1]] = value
            with self.subTest(case=label), self.assertRaises(ContractError):
                audit_orphan_authority_candidate(**case)


    def test_live_orphan_audit_get_only_and_fail_closed(self) -> None:
        authority = _test_authorities()[2]
        repo = {"id": 1315088383, "full_name": "owner/repo"}
        old_head, new_head, base = "b" * 40, "c" * 40, "a" * 40

        def records() -> dict[str, object]:
            return {
                "branches/main": {"commit": {"sha": base}},
                "pulls/42": {
                    "number": 42, "state": "open", "draft": False,
                    "base": {"sha": base, "ref": "main", "repo": dict(repo)},
                    "head": {"sha": new_head, "ref": "feat/pr-42", "repo": dict(repo)},
                },
                "actions/runs/1001": {
                    "id": 1001, "event": "pull_request",
                    "path": ".github/workflows/pr-ci.yml",
                    "head_sha": old_head, "head_branch": "feat/pr-42",
                    "run_attempt": 1, "status": "completed", "conclusion": "cancelled",
                    "repository": dict(repo), "head_repository": dict(repo),
                },
                "actions/runs/2001": {
                    "id": 2001, "event": "pull_request",
                    "path": ".github/workflows/pr-ci.yml",
                    "head_sha": new_head, "head_branch": "feat/pr-42",
                    "run_attempt": 1, "status": "in_progress", "conclusion": None,
                    "repository": dict(repo), "head_repository": dict(repo),
                },
                "actions/runs/1002": {
                    "id": 1002, "event": "workflow_dispatch",
                    "path": authority.workflow, "head_sha": base,
                    "head_branch": "main", "run_attempt": 1,
                    "status": "in_progress", "conclusion": None,
                    "actor": {"login": "github-actions[bot]"},
                    "display_title": expected_run_name(
                        authority, pr_number="42", head_sha=old_head,
                        caller_run_id="1001", caller_run_attempt="1",
                    ),
                    "repository": dict(repo), "head_repository": dict(repo),
                },
            }

        class Stub:
            repository = "owner/repo"

            def __init__(self, entries: dict[str, object]) -> None:
                self.entries = entries
                self.calls: list[str] = []

            def _read_json(self, path: str) -> dict[str, object]:
                self.calls.append("GET " + path)
                return copy.deepcopy(self.entries[path])

            def pull_request(self, number: str) -> dict[str, object]:
                return self._read_json("pulls/" + number)

            def audit_commit_prs_link_bounded(self, sha: str) -> list[dict[str, object]]:
                self.calls.append("GET-LINK " + sha)
                return [{
                    "number": 42, "head": {"ref": "feat/pr-42"},
                    "base": {"ref": "main"},
                }]

        def execute(stub: Stub, **overrides: object) -> dict[str, object]:
            args: dict[str, object] = {
                "client": stub, "authority": authority, "pr_number": 42,
                "old_caller_run_id": 1001, "current_caller_run_id": 2001,
                "dispatched_run_id": 1002, "expected_base_sha": base,
                "expected_current_head_sha": new_head, "trusted_main_sha": base,
            }
            args.update(overrides)
            return audit_orphan_live_readonly(**args)

        stub = Stub(records())
        result = execute(stub)
        self.assertEqual("READ_ONLY_CANDIDATE", result["decision"])
        self.assertIs(result["cancel_authorized"], False)
        self.assertIs(result["transactional_snapshot"], False)
        self.assertEqual(1002, result["dispatched_run_id"])
        self.assertEqual(1, stub.calls.count("GET-LINK " + old_head))
        self.assertTrue(all(call.startswith(("GET ", "GET-LINK ")) for call in stub.calls))
        self.assertEqual(2, stub.calls.count("GET pulls/42"))

        cases = {
            "wrong new sha": ("actions/runs/2001", "head_sha", old_head),
            "new rerun": ("actions/runs/2001", "run_attempt", 2),
            "foreign current repo": ("actions/runs/2001", "repository", {"id": 1}),
            "failed current": ("actions/runs/2001", "conclusion", "failure"),
            "active old caller": ("actions/runs/1001", "status", "in_progress"),
            "old failed not superseded": ("actions/runs/1001", "conclusion", "failure"),
            "untrusted target": ("actions/runs/1002", "actor", {"login": "attacker"}),
            "wrong target": ("actions/runs/1002", "id", 123),
            "wrong PR": ("pulls/42", "number", 43),
            "closed PR": ("pulls/42", "state", "closed"),
            "stale PR head": ("pulls/42", "head", {"sha": old_head}),
            "wrong main": ("branches/main", "commit", {"sha": "f" * 40}),
        }
        for name, (path, key, value) in cases.items():
            data = records()
            data[path][key] = value
            with self.subTest(name=name), self.assertRaises(ContractError):
                execute(Stub(data))
        # First-pass failures must say WHICH invariant failed. This exposes no
        # GitHub response values and must NEVER weaken the denial or POST.
        initial_denials = {
            "event": ("event", "workflow_dispatch"),
            "workflow": ("path", ".github/workflows/ci.yml"),
            "head_sha": ("head_sha", old_head),
            "head_branch": ("head_branch", "evil/other"),
            "run_attempt": ("run_attempt", 2),
            "repository": ("repository", {"id": 7, "full_name": "owner/repo"}),
            "active_status": ("status", "queued"),
            "completed_success": ("status", "completed"),
            "active_no_conclusion": ("conclusion", "failure"),
        }
        for invariant, (field, changed) in initial_denials.items():
            with self.subTest(invariant=invariant):
                data = records()
                data["actions/runs/2001"][field] = changed
                with self.assertRaisesRegex(
                    ContractError, "current caller is not trusted: " + invariant
                ):
                    execute(Stub(data))
        ended_failure = records()
        ended_failure["actions/runs/2001"].update(
            status="completed", conclusion="failure",
        )
        with self.assertRaisesRegex(ContractError, "completed_success"):
            execute(Stub(ended_failure))

        for name, override in {
            "negative run id": {"dispatched_run_id": -2},
            "same run": {"dispatched_run_id": 1001},
            "head mismatch": {"expected_current_head_sha": old_head},
            "untrusted base": {"expected_base_sha": "f" * 40},
        }.items():
            with self.subTest(name=name), self.assertRaises(ContractError):
                execute(Stub(records()), **override)

        # A normal in-flight caller or CodeQL metadata update is NOT an
        # authorization identity change. These scenarios previously failed
        # because full GitHub API JSON records were compared byte-for-byte.
        class Reread(Stub):
            def __init__(
                self, entries: dict[str, object],
                changes: dict[str, dict[str, object]],
            ) -> None:
                super().__init__(entries)
                self.changes = changes

            def _read_json(self, path: str) -> dict[str, object]:
                value = super()._read_json(path)
                if self.calls.count("GET " + path) == 2:
                    for dotted, changed in self.changes.get(path, {}).items():
                        node = value
                        parts = dotted.split(".")
                        for part in parts[:-1]:
                            node = node[part]
                        node[parts[-1]] = changed
                return value

        benign = {
            "pulls/42": {"updated_at": "2026-10-09T04:50:20Z"},
            "actions/runs/1001": {"updated_at": "2026-10-09T04:50:21Z"},
            "actions/runs/2001": {
                "updated_at": "2026-10-09T04:50:22Z",
                "status": "completed", "conclusion": "success",
            },
            "actions/runs/1002": {"updated_at": "2026-10-09T04:50:23Z"},
            "branches/main": {"protected": True},
        }
        self.assertEqual(
            "READ_ONLY_CANDIDATE",
            execute(Reread(records(), benign))["decision"],
        )
        initially_queued = records()
        initially_queued["actions/runs/1002"]["status"] = "queued"
        self.assertEqual(
            "READ_ONLY_CANDIDATE",
            execute(Reread(initially_queued, {
                "actions/runs/1002": {"status": "in_progress"},
            }))["decision"],
        )

        unsafe = {
            "draft transition": ("pulls/42", "draft", True),
            "PR head drift": ("pulls/42", "head.sha", "f" * 40),
            "PR base repo drift": ("pulls/42", "base.repo.id", 5),
            "main SHA drift": ("branches/main", "commit.sha", "f" * 40),
            "old caller rerun": ("actions/runs/1001", "run_attempt", 2),
            "old caller no longer cancelled": (
                "actions/runs/1001", "conclusion", "success",
            ),
            "new caller rerun": ("actions/runs/2001", "run_attempt", 2),
            "new caller failure": ("actions/runs/2001", "conclusion", "failure"),
            "new caller identity": ("actions/runs/2001", "head_sha", old_head),
            "target rerun ABA": ("actions/runs/1002", "run_attempt", 2),
            "target completed": ("actions/runs/1002", "status", "completed"),
            "target workflow drift": (
                "actions/runs/1002", "path", ".github/workflows/ci.yml",
            ),
            "target actor drift": (
                "actions/runs/1002", "actor.login", "attacker",
            ),
            "target repository drift": (
                "actions/runs/1002", "head_repository.id", 5,
            ),
        }
        for name, (path, key, value) in unsafe.items():
            with self.subTest(unsafe_transition=name), self.assertRaises(ContractError):
                execute(Reread(records(), {path: {key: value}}))
        already_complete = records()
        already_complete["actions/runs/2001"].update(
            status="completed", conclusion="success",
        )
        with self.assertRaisesRegex(ContractError, "state changed unsafely"):
            execute(Reread(already_complete, {
                "actions/runs/2001": {"status": "in_progress", "conclusion": None},
            }))
        with self.assertRaisesRegex(ContractError, "state changed unsafely"):
            execute(Reread(records(), {
                "actions/runs/1002": {"status": "queued"},
            }))

        class Changing(Stub):
            def pull_request(self, number: str) -> dict[str, object]:
                value = super().pull_request(number)
                if self.calls.count("GET pulls/42") == 2:
                    value["state"] = "closed"
                return value

        with self.assertRaisesRegex(ContractError, "final readback"):
            execute(Changing(records()))

    def test_auto_orphan_untrusted_inputs_denied_without_api_calls(self) -> None:
        authority = _test_authorities()[2]

        class NeverGet:
            repository = "owner/repo"

            def _read_json(self, path: str) -> dict[str, object]:
                raise AssertionError("invalid input must not make GitHub API request")

        fake = NeverGet()
        for bad in (0, -1, True, "1001", None):
            with self.subTest(old_run=bad), self.assertRaises(ContractError):
                audit_orphan_auto_readonly(
                    client=fake, authorities=[authority],
                    old_caller_run_id=bad, trusted_main_sha="a" * 40,
                )
        for bad_sha in ("", "a" * 39, "g" * 40, None, 123):
            with self.subTest(main_sha=bad_sha), self.assertRaises(ContractError):
                audit_orphan_auto_readonly(
                    client=fake, authorities=[authority],
                    old_caller_run_id=1001, trusted_main_sha=bad_sha,
                )
        with self.assertRaisesRegex(ContractError, "protected policy"):
            audit_orphan_auto_readonly(
                client=fake, authorities=[authority], authority_id="arbitrary",
                old_caller_run_id=1001, trusted_main_sha="a" * 40,
            )

    def test_auto_orphan_discovery_is_get_only_and_fail_closed(self) -> None:
        authority = _test_authorities()[2]
        repo = {"id": 1315088383, "full_name": "owner/repo"}
        old_sha, new_sha, base = "b" * 40, "c" * 40, "a" * 40
        old_id, current_id, target_id = 1001, 2001, 1002
        title = expected_run_name(
            authority, pr_number="42", head_sha=old_sha,
            caller_run_id=str(old_id), caller_run_attempt="1",
        )
        query = (
            "actions/workflows/pr-ci.yml/runs?event=pull_request&"
            f"head_sha={new_sha}&per_page=100"
        )

        def data() -> dict[str, object]:
            current = {
                "id": current_id, "event": "pull_request",
                "path": ".github/workflows/pr-ci.yml",
                "head_sha": new_sha, "head_branch": "feat/pr-42",
                "run_attempt": 1, "status": "in_progress", "conclusion": None,
                "repository": dict(repo), "head_repository": dict(repo),
            }
            return {
                "branches/main": {"commit": {"sha": base}},
                "actions/runs/1001": {
                    "id": old_id, "event": "pull_request",
                    "path": ".github/workflows/pr-ci.yml",
                    "head_sha": old_sha, "head_branch": "feat/pr-42",
                    "run_attempt": 1, "status": "completed", "conclusion": "cancelled",
                    "repository": dict(repo), "head_repository": dict(repo),
                },
                "actions/runs/2001": current,
                "actions/runs/1002": {
                    "id": target_id, "event": "workflow_dispatch",
                    "path": authority.workflow,
                    "head_sha": base, "head_branch": "main",
                    "run_attempt": 1, "status": "in_progress", "conclusion": None,
                    "actor": {"login": "github-actions[bot]"},
                    "display_title": title, "repository": dict(repo),
                    "head_repository": dict(repo),
                },
                "pulls/42": {
                    "number": 42, "state": "open", "draft": False,
                    "base": {"sha": base, "ref": "main", "repo": dict(repo)},
                    "head": {"sha": new_sha, "ref": "feat/pr-42", "repo": dict(repo)},
                },
                query: {"total_count": 1, "workflow_runs": [current]},
            }

        class Stub:
            repository = "owner/repo"

            def __init__(self, metadata: dict[str, object]) -> None:
                self.metadata = metadata
                self.calls: list[str] = []
                self.associations: list[dict[str, object]] = [
                    {"number": 42, "head": {"ref": "feat/pr-42"},
                     "base": {"ref": "main"}}
                ]
                self.targets: list[dict[str, object]] = [self.metadata["actions/runs/1002"]]

            def _read_json(self, path: str) -> dict[str, object]:
                self.calls.append("GET " + path)
                return copy.deepcopy(self.metadata[path])

            def pull_request(self, number: str) -> dict[str, object]:
                return self._read_json("pulls/" + number)

            def audit_commit_prs_link_bounded(self, sha: str) -> list[dict[str, object]]:
                self.calls.append("GET-LINK " + sha)
                return copy.deepcopy(self.associations)

            def audit_workflow_runs_bounded(self, workflow: str) -> list[dict[str, object]]:
                self.calls.append("GET-BOUNDED " + workflow)
                return copy.deepcopy(self.targets)

        def check(stub: Stub) -> dict[str, object]:
            return audit_orphan_auto_readonly(
                client=stub, authorities=[authority], old_caller_run_id=old_id,
                trusted_main_sha=base,
            )

        healthy = Stub(data())
        accepted = check(healthy)
        self.assertEqual("READ_ONLY_CANDIDATE", accepted["decision"])
        self.assertIs(accepted["cancel_authorized"], False)
        self.assertEqual(target_id, accepted["dispatched_run_id"])
        self.assertEqual(2, healthy.calls.count("GET " + query))
        self.assertEqual(2, healthy.calls.count("GET-LINK " + old_sha))
        self.assertTrue(all(c.startswith(("GET ", "GET-LINK ", "GET-BOUNDED ")) for c in healthy.calls))

        # M5 pre-write boundary is callable but cannot POST or authorize POST.
        preflight_client = Stub(data())
        preflight = preflight_orphan_codeql_cancel(
            client=preflight_client, authorities=[authority],
            old_caller_run_id=old_id, trusted_main_sha=base,
        )
        self.assertEqual("PREWRITE_CANDIDATE", preflight["decision"])
        self.assertEqual(target_id, preflight["dispatched_run_id"])
        self.assertEqual(1, preflight["run_attempt"])
        self.assertIs(preflight["cancel_authorized"], False)
        self.assertIs(preflight["transactional_snapshot"], False)
        self.assertEqual(5, preflight_client.calls.count("GET pulls/42"))
        self.assertTrue(all(call.startswith(("GET ", "GET-LINK ", "GET-BOUNDED "))
                            for call in preflight_client.calls))

        completed_preflight = Stub(data())
        completed_preflight.targets[0] = dict(completed_preflight.targets[0],
                                              status="completed", conclusion="success")
        self.assertEqual(
            "NO_WRITE", preflight_orphan_codeql_cancel(
                client=completed_preflight, authorities=[authority],
                old_caller_run_id=old_id, trusted_main_sha=base,
            )["decision"],
        )
        with self.assertRaisesRegex(ContractError, "exact protected CodeQL"):
            preflight_orphan_codeql_cancel(
                client=Stub(data()),
                authorities=[Authority("codeql", ".github/workflows/other.yml", "non_docs")],
                old_caller_run_id=old_id, trusted_main_sha=base,
            )

        class AttemptDrift(Stub):
            def _read_json(self, path: str) -> dict[str, object]:
                value = super()._read_json(path)
                if path == "actions/runs/1002" and self.calls.count("GET " + path) == 5:
                    value["run_attempt"] = 2
                return value

        with self.assertRaisesRegex(ContractError, "target changed"):
            preflight_orphan_codeql_cancel(
                client=AttemptDrift(data()), authorities=[authority],
                old_caller_run_id=old_id, trusted_main_sha=base,
            )

        class HeadDrift(Stub):
            def _read_json(self, path: str) -> dict[str, object]:
                value = super()._read_json(path)
                if path == "pulls/42" and self.calls.count("GET " + path) == 4:
                    value["head"]["sha"] = old_sha
                return value

        with self.assertRaises(ContractError):
            preflight_orphan_codeql_cancel(
                client=HeadDrift(data()), authorities=[authority],
                old_caller_run_id=old_id, trusted_main_sha=base,
            )

        non_cancelled = Stub(data())
        non_cancelled.metadata["actions/runs/1001"]["conclusion"] = "success"
        self.assertEqual("NO_CANDIDATE", check(non_cancelled)["decision"])
        self.assertNotIn("GET-LINK " + old_sha, non_cancelled.calls)

        # A failed/timed-out/neutral predecessor is NOT proof of supersession.
        # Do not probe commit associations or an old independent Authority.
        for conclusion in ("failure", "timed_out", "skipped", "neutral", "action_required"):
            stalled = Stub(data())
            stalled.metadata["actions/runs/1001"]["conclusion"] = conclusion
            with self.subTest(old_conclusion=conclusion):
                status = check(stalled)
                self.assertEqual("NO_CANDIDATE", status["decision"])
                self.assertEqual("old_caller_not_cancelled", status["reason"])
                self.assertNotIn("GET-LINK " + old_sha, stalled.calls)
                self.assertFalse(any(c.startswith("GET-BOUNDED ") for c in stalled.calls))


        completed = Stub(data())
        completed.targets[0] = dict(completed.targets[0], status="completed", conclusion="success")
        self.assertEqual("old_authority_already_completed", check(completed)["reason"])

        unchanged = Stub(data())
        unchanged.metadata["pulls/42"]["head"]["sha"] = old_sha
        self.assertEqual("head_not_superseded", check(unchanged)["reason"])

        ambiguous = Stub(data())
        ambiguous.associations.append(dict(ambiguous.associations[0], number=43))
        with self.assertRaises(ContractError):
            check(ambiguous)

        dup_current = Stub(data())
        dup_current.metadata[query]["workflow_runs"].append(copy.deepcopy(
            dup_current.metadata[query]["workflow_runs"][0]
        ))
        dup_current.metadata[query]["total_count"] = 2
        with self.assertRaises(ContractError):
            check(dup_current)

        wrong_head = Stub(data())
        wrong_head.metadata[query]["workflow_runs"][0]["head_sha"] = old_sha
        with self.assertRaises(ContractError):
            check(wrong_head)

        tampered = Stub(data())
        tampered.metadata["actions/runs/1001"]["head_repository"]["id"] = 1
        with self.assertRaises(ContractError):
            check(tampered)

        wrong_actor = Stub(data())
        wrong_actor.metadata["actions/runs/1002"]["actor"]["login"] = "attacker"
        with self.assertRaises(ContractError):
            check(wrong_actor)

        wrong_repo = Stub(data())
        wrong_repo.metadata["pulls/42"]["base"]["repo"]["full_name"] = "other/repo"
        with self.assertRaises(ContractError):
            check(wrong_repo)

        wrong_branch = Stub(data())
        wrong_branch.metadata["pulls/42"]["head"]["ref"] = "other"
        with self.assertRaises(ContractError):
            check(wrong_branch)

        class Drifting(Stub):
            def _read_json(self, path: str) -> dict[str, object]:
                result = super()._read_json(path)
                if path == query and self.calls.count("GET " + query) == 2:
                    result["total_count"] = 2
                return result

        with self.assertRaisesRegex(ContractError, "page membership drift"):
            check(Drifting(data()))


        # Duplicate GETs of a *live* current caller may disagree on updated_at
        # and forward status transitions without any PR/run identity drift.
        class PageReread(Stub):
            def __init__(self, metadata: dict[str, object], mode: str) -> None:
                super().__init__(metadata)
                self.mode = mode

            def _read_json(self, path: str) -> dict[str, object]:
                result = super()._read_json(path)
                if path == query and self.calls.count("GET " + query) == 2:
                    row = result["workflow_runs"][0]
                    row["updated_at"] = "2026-10-09T06:36:09Z"
                    if self.mode == "status-forward":
                        row["status"] = "completed"
                        row["conclusion"] = "success"
                    elif self.mode == "head-drift":
                        row["head_sha"] = old_sha
                    elif self.mode == "attempt-drift":
                        row["run_attempt"] = 2
                    elif self.mode == "repo-drift":
                        row["repository"]["id"] = 999
                    elif self.mode == "status-regression":
                        row["status"] = "queued"
                return result

        for mode in ("metadata", "status-forward"):
            with self.subTest(mode=mode):
                self.assertEqual("READ_ONLY_CANDIDATE",
                                 check(PageReread(data(), mode))["decision"])
        for mode in ("head-drift", "attempt-drift", "repo-drift"):
            with self.subTest(mode=mode), self.assertRaisesRegex(
                ContractError, "page provenance drift"
            ):
                check(PageReread(data(), mode))
        completed_caller = data()
        completed_caller[query]["workflow_runs"][0]["status"] = "completed"
        completed_caller[query]["workflow_runs"][0]["conclusion"] = "success"
        with self.assertRaisesRegex(ContractError, "page provenance drift"):
            check(PageReread(completed_caller, "status-regression"))

    def test_workflow_runs_uses_workflow_filename_endpoint(self) -> None:
        seen: dict[str, object] = {}

        def opener(request: urllib.request.Request, timeout: int) -> io.StringIO:
            seen["url"] = request.full_url
            seen["timeout"] = timeout
            return io.StringIO('{"workflow_runs":[]}')

        client = GitHubActionsClient(
            api_url="https://api.github.test",
            repository="owner/repo",
            token="token",
            opener=opener,
        )
        self.assertEqual(
            [],
            client.workflow_runs(".github/workflows/supply-chain.yml"),
        )
        self.assertEqual(
            "https://api.github.test/repos/owner/repo/actions/workflows/"
            "supply-chain.yml/runs?event=workflow_dispatch&per_page=100",
            seen["url"],
        )
        self.assertEqual(20, seen["timeout"])

    def test_pull_lookup_uses_read_only_get_with_contents_token(self) -> None:
        observed: dict[str, object] = {}

        def opener(request: urllib.request.Request, timeout: int) -> io.StringIO:
            observed["url"] = request.full_url
            observed["verb"] = request.get_method()
            observed["timeout"] = timeout
            return io.StringIO('{"state":"open"}')

        client = GitHubActionsClient(
            api_url="https://api.github.test",
            repository="owner/repo",
            token="token",
            opener=opener,
        )
        self.assertEqual("open", client.pull_request("42")["state"])
        self.assertEqual("GET", observed["verb"])
        self.assertEqual("https://api.github.test/repos/owner/repo/pulls/42", observed["url"])
        self.assertEqual(20, observed["timeout"])

    def test_stale_head_fails_even_if_previous_authorities_succeeded(self) -> None:
        authority = _test_authorities()[0]
        client = _FakeClient({authority.workflow: [_run(authority)]})
        client.live_pr["head"]["sha"] = "c" * 40
        with self.assertRaisesRegex(ContractError, "live PR head identity changed"):
            verify_runs(
                authorities=[authority], required=[authority], client=client,
                dispatch_result="success", pr_number="42",
                base_sha="a" * 40, head_sha="b" * 40, head_repo="owner/repo",
                caller_run_id="1001", caller_run_attempt="2",
                timeout_seconds=0, poll_interval_seconds=0,
            )

    def test_stale_head_detected_on_next_poll_before_timeout(self) -> None:
        authority = _test_authorities()[0]
        client = _FakeClient({})
        def supersede(_: float) -> None:
            client.live_pr["head"]["sha"] = "c" * 40
        with self.assertRaisesRegex(ContractError, "live PR head identity changed"):
            verify_runs(
                authorities=[authority], required=[authority], client=client,
                dispatch_result="success", pr_number="42",
                base_sha="a" * 40, head_sha="b" * 40, head_repo="owner/repo",
                caller_run_id="1001", caller_run_attempt="2",
                timeout_seconds=20, poll_interval_seconds=0, sleep=supersede,
            )
        self.assertEqual(2, client.pr_reads)

    def test_live_base_repo_supersession_during_poll_fails_closed(self) -> None:
        authority = _test_authorities()[0]
        client = _FakeClient({})
        def switch_base_repo(_: float) -> None:
            client.live_pr["base"]["repo"]["full_name"] = "other/repo"
        with self.assertRaisesRegex(ContractError, "live PR base identity changed"):
            verify_runs(
                authorities=[authority], required=[authority], client=client,
                dispatch_result="success", pr_number="42",
                base_sha="a" * 40, head_sha="b" * 40, head_repo="owner/repo",
                caller_run_id="1001", caller_run_attempt="2",
                timeout_seconds=20, poll_interval_seconds=0, sleep=switch_base_repo,
            )
        self.assertEqual(2, client.pr_reads)

    def test_live_head_repo_supersession_during_poll_fails_closed(self) -> None:
        authority = _test_authorities()[0]
        client = _FakeClient({})
        def switch_head_repo(_: float) -> None:
            client.live_pr["head"]["repo"]["full_name"] = "other/repo"
        with self.assertRaisesRegex(ContractError, "live PR head identity changed"):
            verify_runs(
                authorities=[authority], required=[authority], client=client,
                dispatch_result="success", pr_number="42",
                base_sha="a" * 40, head_sha="b" * 40, head_repo="owner/repo",
                caller_run_id="1001", caller_run_attempt="2",
                timeout_seconds=20, poll_interval_seconds=0, sleep=switch_head_repo,
            )
        self.assertEqual(2, client.pr_reads)

    def test_live_pr_identity_and_state_are_fail_closed(self) -> None:
        authority = _test_authorities()[0]
        for case in ("base", "base_repo", "head_repo", "closed", "draft", "missing", "malformed"):
            client = _FakeClient({authority.workflow: [_run(authority)]})
            if case == "base":
                client.live_pr["base"]["sha"] = "c" * 40
            elif case == "base_repo":
                client.live_pr["base"]["repo"]["full_name"] = "other/repo"
            elif case == "head_repo":
                client.live_pr["head"]["repo"]["full_name"] = "other/repo"
            elif case == "closed":
                client.live_pr["state"] = "closed"
            elif case == "draft":
                client.live_pr["draft"] = True
            elif case == "missing":
                del client.live_pr["head"]["repo"]
            else:
                client.live_pr = {"head": "invalid"}
            with self.subTest(case=case), self.assertRaises(ContractError):
                verify_runs(
                    authorities=[authority], required=[authority], client=client,
                    dispatch_result="success", pr_number="42",
                    base_sha="a" * 40, head_sha="b" * 40, head_repo="owner/repo",
                    caller_run_id="1001", caller_run_attempt="2",
                    timeout_seconds=0, poll_interval_seconds=0,
                )

    def test_pr_api_error_is_not_accepted_as_proof(self) -> None:
        def broken(request: urllib.request.Request, timeout: int) -> io.StringIO:
            raise OSError("temporary upstream failure")
        client = GitHubActionsClient(
            api_url="https://api.github.test", repository="owner/repo",
            token="token", opener=broken,
        )
        with self.assertRaisesRegex(ContractError, "could not read GitHub API pulls/42"):
            client.pull_request("42")

    def test_docs_only_without_impacts_requires_no_optional_authority(self) -> None:
        required = required_authorities(
            _test_authorities(),
            scope_result="success",
            classification_outcome="success",
            legacy_base="false",
            docs_only="true",
            evidence_impact="false",
            supply_chain_impact="false",
        )
        self.assertEqual([], [authority.id for authority in required])

    def test_non_docs_requires_rust_dependency_and_codeql(self) -> None:
        required = required_authorities(
            _test_authorities(),
            scope_result="success",
            classification_outcome="success",
            legacy_base="false",
            docs_only="false",
            evidence_impact="false",
            supply_chain_impact="false",
        )
        self.assertEqual(
            ["rust", "dependency", "codeql"],
            [authority.id for authority in required],
        )

    def test_independent_impacts_route_independently(self) -> None:
        required = required_authorities(
            _test_authorities(),
            scope_result="success",
            classification_outcome="success",
            legacy_base="false",
            docs_only="true",
            evidence_impact="true",
            supply_chain_impact="true",
        )
        self.assertEqual(
            ["evidence", "supply"],
            [authority.id for authority in required],
        )

    def test_scope_failure_requires_all_authorities(self) -> None:
        required = required_authorities(
            _test_authorities(),
            scope_result="failure",
            classification_outcome="success",
            legacy_base="false",
            docs_only="true",
            evidence_impact="false",
            supply_chain_impact="false",
        )
        self.assertEqual(
            ["rust", "dependency", "codeql", "evidence", "supply"],
            [authority.id for authority in required],
        )

    def test_malformed_applicability_requires_all_authorities(self) -> None:
        required = required_authorities(
            _test_authorities(),
            scope_result="success",
            classification_outcome="success",
            legacy_base="false",
            docs_only="unknown",
            evidence_impact="false",
            supply_chain_impact="false",
        )
        self.assertEqual(
            ["rust", "dependency", "codeql", "evidence", "supply"],
            [authority.id for authority in required],
        )

    def test_verify_accepts_exact_required_successes(self) -> None:
        authorities = _test_authorities()
        required = authorities[:3]
        client = _FakeClient(
            {
                authority.workflow: [_run(authority, run_id=100 + index)]
                for index, authority in enumerate(required)
            }
        )
        result = verify_runs(
            authorities=authorities,
            required=required,
            client=client,
            dispatch_result="success",
            pr_number="42",
            base_sha="a" * 40,
            head_sha="b" * 40,
            head_repo="owner/repo",
            caller_run_id="1001",
            caller_run_attempt="2",
            timeout_seconds=0,
            poll_interval_seconds=0,
        )
        self.assertEqual(
            {"rust", "dependency", "codeql"},
            set(result["verified"]),
        )

    def test_verify_rejects_missing_required_run(self) -> None:
        authorities = _test_authorities()
        with self.assertRaisesRegex(ContractError, "timed out waiting"):
            verify_runs(
                authorities=authorities,
                required=[authorities[0]],
                client=_FakeClient({}),
                dispatch_result="success",
                pr_number="42",
                base_sha="a" * 40,
                head_sha="b" * 40,
                head_repo="owner/repo",
                caller_run_id="1001",
                caller_run_attempt="2",
                timeout_seconds=0,
                poll_interval_seconds=0,
            )

    def test_verify_rejects_duplicate_correlated_runs(self) -> None:
        authority = _test_authorities()[0]
        run = _run(authority)
        with self.assertRaisesRegex(ContractError, "duplicate correlated"):
            verify_runs(
                authorities=[authority],
                required=[authority],
                client=_FakeClient({authority.workflow: [run, dict(run, id=11)]}),
                dispatch_result="success",
                pr_number="42",
                base_sha="a" * 40,
                head_sha="b" * 40,
                head_repo="owner/repo",
                caller_run_id="1001",
                caller_run_attempt="2",
                timeout_seconds=0,
                poll_interval_seconds=0,
            )

    def test_verify_rejects_failed_required_run(self) -> None:
        authority = _test_authorities()[0]
        with self.assertRaisesRegex(ContractError, "concluded 'failure'"):
            verify_runs(
                authorities=[authority],
                required=[authority],
                client=_FakeClient({authority.workflow: [_run(authority, conclusion="failure")]}),
                dispatch_result="success",
                pr_number="42",
                base_sha="a" * 40,
                head_sha="b" * 40,
                head_repo="owner/repo",
                caller_run_id="1001",
                caller_run_attempt="2",
                timeout_seconds=0,
                poll_interval_seconds=0,
            )

    def test_verify_rejects_non_applicable_correlated_run(self) -> None:
        authority = _test_authorities()[0]
        with self.assertRaisesRegex(ContractError, "unexpectedly materialized"):
            verify_runs(
                authorities=[authority],
                required=[],
                client=_FakeClient({authority.workflow: [_run(authority)]}),
                dispatch_result="success",
                pr_number="42",
                base_sha="a" * 40,
                head_sha="b" * 40,
                head_repo="owner/repo",
                caller_run_id="1001",
                caller_run_attempt="2",
                timeout_seconds=0,
                poll_interval_seconds=0,
            )

    def test_wrong_correlation_is_not_accepted(self) -> None:
        authority = _test_authorities()[0]
        wrong = dict(_run(authority), display_title="wrong")
        with self.assertRaisesRegex(ContractError, "timed out waiting"):
            verify_runs(
                authorities=[authority],
                required=[authority],
                client=_FakeClient({authority.workflow: [wrong]}),
                dispatch_result="success",
                pr_number="42",
                base_sha="a" * 40,
                head_sha="b" * 40,
                head_repo="owner/repo",
                caller_run_id="1001",
                caller_run_attempt="2",
                timeout_seconds=0,
                poll_interval_seconds=0,
            )


def run_self_tests() -> int:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(RuntimeContractTests)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan = subparsers.add_parser("plan")
    add_scope_args(plan)

    verify = subparsers.add_parser("verify")
    add_scope_args(verify)
    verify.add_argument("--dispatch-result", required=True)
    verify.add_argument("--base-sha", required=True)
    verify.add_argument("--head-sha", required=True)
    verify.add_argument("--head-repo", required=True)
    verify.add_argument("--pr-number", required=True)
    verify.add_argument("--caller-run-id", required=True)
    verify.add_argument("--caller-run-attempt", required=True)
    verify.add_argument("--timeout-seconds", type=float, default=300.0)
    verify.add_argument("--poll-interval-seconds", type=float, default=5.0)

    audit = subparsers.add_parser("audit-orphan-readonly")
    audit.add_argument("--policy", default=".github/merge-gate-policy.json")
    audit.add_argument("--authority-id", required=True)
    audit.add_argument("--pr-number", required=True, type=int)
    audit.add_argument("--old-caller-run-id", required=True, type=int)
    audit.add_argument("--current-caller-run-id", required=True, type=int)
    audit.add_argument("--dispatched-run-id", required=True, type=int)
    audit.add_argument("--expected-base-sha", required=True)
    audit.add_argument("--expected-current-head-sha", required=True)
    audit.add_argument("--trusted-main-sha", required=True)

    automatic = subparsers.add_parser("audit-orphan-auto-readonly")
    automatic.add_argument("--policy", default=".github/merge-gate-policy.json")
    automatic.add_argument("--authority-id", default="codeql")
    automatic.add_argument("--old-caller-run-id", required=True, type=int)
    automatic.add_argument("--trusted-main-sha", required=True)

    subparsers.add_parser("self-test")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "self-test":
            return run_self_tests()

        if args.command == "audit-orphan-auto-readonly":
            if (
                os.environ.get("GITHUB_REF") != "refs/heads/main"
                or os.environ.get("GITHUB_SHA") != args.trusted_main_sha
            ):
                raise ContractError("automatic orphan audit requires exact trusted main")
            client = GitHubActionsClient(
                api_url=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
                repository=os.environ.get("GITHUB_REPOSITORY", ""),
                token=os.environ.get("GH_TOKEN", ""),
            )
            result = audit_orphan_auto_readonly(
                client=client, authorities=load_authorities(args.policy),
                old_caller_run_id=args.old_caller_run_id,
                trusted_main_sha=args.trusted_main_sha,
                authority_id=args.authority_id,
            )
            print(json.dumps(result, separators=(",", ":")))
            return 0

        if args.command == "audit-orphan-readonly":
            if (
                os.environ.get("GITHUB_REF") != "refs/heads/main"
                or os.environ.get("GITHUB_SHA") != args.trusted_main_sha
            ):
                raise ContractError("orphan read-only audit requires exact trusted main checkout")
            authorities = load_authorities(args.policy)
            choices = [a for a in authorities if a.id == args.authority_id]
            if len(choices) != 1:
                raise ContractError("orphan read-only audit authority is not in trusted policy")
            client = GitHubActionsClient(
                api_url=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
                repository=os.environ.get("GITHUB_REPOSITORY", ""),
                token=os.environ.get("GH_TOKEN", ""),
            )
            report = audit_orphan_live_readonly(
                client=client, authority=choices[0], pr_number=args.pr_number,
                old_caller_run_id=args.old_caller_run_id,
                current_caller_run_id=args.current_caller_run_id,
                dispatched_run_id=args.dispatched_run_id,
                expected_base_sha=args.expected_base_sha,
                expected_current_head_sha=args.expected_current_head_sha,
                trusted_main_sha=args.trusted_main_sha,
            )
            print(json.dumps(report, separators=(",", ":")))
            return 0

        authorities, required = required_from_args(args)
        if args.command == "plan":
            print(json.dumps(plan_payload(authorities, required), separators=(",", ":")))
            return 0

        validate_correlation(
            base_sha=args.base_sha,
            head_sha=args.head_sha,
            head_repo=args.head_repo,
            pr_number=args.pr_number,
            caller_run_id=args.caller_run_id,
            caller_run_attempt=args.caller_run_attempt,
        )
        client = GitHubActionsClient(
            api_url=os.environ.get("GITHUB_API_URL", "https://api.github.com"),
            repository=os.environ.get("GITHUB_REPOSITORY", ""),
            token=os.environ.get("GH_TOKEN", ""),
        )
        result = verify_runs(
            authorities=authorities,
            required=required,
            client=client,
            dispatch_result=args.dispatch_result,
            pr_number=args.pr_number,
            base_sha=args.base_sha,
            head_sha=args.head_sha,
            head_repo=args.head_repo,
            caller_run_id=args.caller_run_id,
            caller_run_attempt=args.caller_run_attempt,
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
        print(json.dumps(result, separators=(",", ":")))
        return 0
    except ContractError as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
