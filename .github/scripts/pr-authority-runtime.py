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

    subparsers.add_parser("self-test")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    try:
        if args.command == "self-test":
            return run_self_tests()

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
