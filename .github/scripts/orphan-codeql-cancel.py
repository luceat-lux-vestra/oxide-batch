#!/usr/bin/env python3
"""Stage M5b: opt-in, exact-run CodeQL cancellation from trusted main only.

No PR-head execution, no force-cancel, no hidden retry. A GitHub run-ID cancel
cannot be conditional on run_attempt; GET->POST TOCTOU / ABA remain residual
risks. Do not enable outside a controlled experiment before M6 acceptance.
"""
from __future__ import annotations

import argparse
import copy
import importlib.util
import json
import os
import re
import sys
import time
import unittest
import urllib.request
from pathlib import Path

BASE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("oxide_authority_runtime", BASE / "pr-authority-runtime.py")
if spec is None or spec.loader is None:
    raise RuntimeError("trusted authority runtime import unavailable")
runtime = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = runtime
spec.loader.exec_module(runtime)
ContractError = runtime.ContractError
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
REPOSITORY = "luceat-lux-vestra/oxide-batch"


def ordinary_cancel(client: object, run_id: int) -> int:
    """The only write: POST /actions/runs/{id}/cancel. HTTP 202 is not completion."""
    if type(run_id) is not int or run_id <= 0 or client.repository != REPOSITORY:
        raise ContractError("M5 cancellation run/repository identity invalid")
    if client.api_url != "https://api.github.com":
        raise ContractError("M5 cancellation API origin is not trusted")
    url = f"{client.api_url}/repos/{REPOSITORY}/actions/runs/{run_id}/cancel"
    request = urllib.request.Request(
        url, data=b"", method="POST",
        headers={
            "Authorization": f"Bearer {client.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "oxide-batch-orphan-codeql-cancel",
        },
    )
    try:
        with client.opener(request, timeout=20) as response:
            status = response.status
    except Exception as exc:
        raise ContractError(f"M5 cancel request failed for run {run_id}: {type(exc).__name__}") from exc
    if status != 202:
        raise ContractError(f"M5 cancel run {run_id} returned HTTP {status}, expected 202")
    return status


def operate(
    *,
    client: object,
    authorities: list[object],
    old_caller_run_id: int,
    trusted_main_sha: str,
    mode: str,
    max_polls: int = 15,
    poll_delay: float = 4.0,
) -> dict[str, object]:
    if mode not in {"dry-run", "live"}:
        raise ContractError("M5 mode must be dry-run or live")
    if type(old_caller_run_id) is not int or old_caller_run_id <= 0:
        raise ContractError("M5 old caller run ID invalid")
    if not isinstance(trusted_main_sha, str) or not SHA_RE.fullmatch(trusted_main_sha):
        raise ContractError("M5 trusted main SHA invalid")
    if type(max_polls) is not int or not 1 <= max_polls <= 20:
        raise ContractError("M5 bounded poll count invalid")
    if not isinstance(poll_delay, (int, float)) or not 0 <= poll_delay <= 10:
        raise ContractError("M5 bounded poll delay invalid")
    matches = [a for a in authorities if a.id == "codeql"]
    if len(matches) != 1 or matches[0].workflow != ".github/workflows/codeql.yml":
        raise ContractError("M5 requires exact protected CodeQL authority")

    def report(decision: str, **data: object) -> dict[str, object]:
        return {
            "schema": "orphan-codeql-cancel-v1",
            "decision": decision,
            "old_caller_run_id": old_caller_run_id,
            "cancel_authorized": False,
            "transactional_snapshot": False,
            **data,
        }

    # Preflight itself re-discovers the target from GitHub and independently
    # audits main/PR/old+current caller/target. Never consume a saved M4 report.
    first = runtime.preflight_orphan_codeql_cancel(
        client=client, authorities=authorities,
        old_caller_run_id=old_caller_run_id, trusted_main_sha=trusted_main_sha,
    )
    if first.get("decision") == "NO_WRITE":
        return report("NO_WRITE", reason=first.get("reason", "no_candidate"))
    if first.get("decision") != "PREWRITE_CANDIDATE":
        raise ContractError("M5 preflight result not recognized")
    run_id = first.get("dispatched_run_id")
    if type(run_id) is not int or run_id <= 0:
        raise ContractError("M5 target run ID invalid")
    if mode == "dry-run":
        return report("DRY_RUN_CANDIDATE", dispatched_run_id=run_id,
                      old_head_sha=first["old_head_sha"],
                      current_head_sha=first["current_head_sha"])

    # A second independent full proof immediately before the write boundary.
    second = runtime.preflight_orphan_codeql_cancel(
        client=client, authorities=authorities,
        old_caller_run_id=old_caller_run_id, trusted_main_sha=trusted_main_sha,
    )
    if second != first:
        raise ContractError("M5 pre-write preflight identity drift: NO WRITE")

    # Final focused GETs narrow the race window; they cannot make it atomic.
    pr = client.pull_request(str(first["pr_number"]))
    main = client._read_json("branches/main")
    old = client._read_json(f"actions/runs/{old_caller_run_id}")
    current = client._read_json(f"actions/runs/{first['current_caller_run_id']}")
    target = client._read_json(f"actions/runs/{run_id}")
    phead, pbase = pr.get("head"), pr.get("base")
    main_commit = main.get("commit")
    if (
        pr.get("number") != first["pr_number"]
        or pr.get("state") != "open" or pr.get("draft") is not False
        or not isinstance(phead, dict) or not isinstance(pbase, dict)
        or phead.get("sha") != first["current_head_sha"]
        or pbase.get("sha") != first["base_sha"]
        or not isinstance(phead.get("repo"), dict)
        or not isinstance(pbase.get("repo"), dict)
        or phead["repo"].get("full_name") != REPOSITORY
        or pbase["repo"].get("full_name") != REPOSITORY
        or phead["repo"].get("id") != pbase["repo"].get("id")
        or not isinstance(main_commit, dict) or main_commit.get("sha") != trusted_main_sha
        or old.get("id") != old_caller_run_id
        or old.get("run_attempt") != 1
        or old.get("head_sha") != first["old_head_sha"]
        or old.get("status") != "completed" or old.get("conclusion") != "cancelled"
        or current.get("id") != first["current_caller_run_id"]
        or current.get("run_attempt") != 1
        or current.get("head_sha") != first["current_head_sha"]
        or current.get("status") not in {"in_progress", "completed"}
        or (current.get("status") == "completed" and current.get("conclusion") != "success")
        or target.get("id") != run_id
        or target.get("run_attempt") != 1
        or target.get("event") != "workflow_dispatch"
        or target.get("path") != matches[0].workflow
        or target.get("status") not in {"queued", "in_progress", "waiting", "pending", "requested"}
        or target.get("conclusion") is not None
        or target.get("display_title") != runtime.expected_run_name(
            matches[0], pr_number=str(first["pr_number"]),
            head_sha=first["old_head_sha"], caller_run_id=str(old_caller_run_id),
            caller_run_attempt="1",
        )
    ):
        raise ContractError("M5 final near-POST identity/state drift: NO WRITE")

    ordinary_cancel(client, run_id)
    # Never force-cancel or retry POST. A 202 only acknowledges the request.
    for poll in range(max_polls):
        observed = client._read_json(f"actions/runs/{run_id}")
        if (
            observed.get("id") != run_id or observed.get("run_attempt") != 1
            or observed.get("path") != matches[0].workflow
        ):
            raise ContractError("M5 postcondition run/attempt identity changed")
        if observed.get("status") == "completed":
            if observed.get("conclusion") != "cancelled":
                raise ContractError("M5 accepted cancel request but terminal state is NOT cancelled")
            return report("CANCELLED_CONFIRMED", dispatched_run_id=run_id,
                          run_attempt=1, terminal_status="completed",
                          terminal_conclusion="cancelled",
                          current_head_sha=first["current_head_sha"])
        if observed.get("status") not in {"queued", "in_progress", "waiting", "pending", "requested"}:
            raise ContractError("M5 postcondition status unknown")
        if poll + 1 < max_polls:
            time.sleep(poll_delay)
    raise ContractError("M5 HTTP 202 received, terminal cancelled state NOT VERIFIED")


class CancelContractTests(unittest.TestCase):
    """An actual mock POST and terminal-state test, not a real GitHub cancellation."""

    def test_live_dry_run_and_adversarial_no_write(self) -> None:
        repo = {"id": 1315088383, "full_name": REPOSITORY}
        base, old_sha, head = "a" * 40, "b" * 40, "c" * 40
        authority = runtime.Authority("codeql", ".github/workflows/codeql.yml", "non_docs")
        preflight = {
            "decision": "PREWRITE_CANDIDATE", "pr_number": 42, "base_sha": base,
            "old_caller_run_id": 1001, "old_head_sha": old_sha, "current_head_sha": head,
            "current_caller_run_id": 2001, "dispatched_run_id": 1002,
            "run_attempt": 1, "cancel_authorized": False, "transactional_snapshot": False,
        }
        title = runtime.expected_run_name(
            authority, pr_number="42", head_sha=old_sha,
            caller_run_id="1001", caller_run_attempt="1",
        )
        def fixture() -> dict[str, object]:
            return {
                "pulls/42": {
                    "number": 42, "state": "open", "draft": False,
                    "head": {"sha": head, "repo": dict(repo)},
                    "base": {"sha": base, "repo": dict(repo)},
                },
                "branches/main": {"commit": {"sha": base}},
                "actions/runs/1001": {
                    "id": 1001, "run_attempt": 1, "head_sha": old_sha,
                    "status": "completed", "conclusion": "cancelled",
                },
                "actions/runs/2001": {
                    "id": 2001, "run_attempt": 1, "head_sha": head,
                    "status": "in_progress", "conclusion": None,
                },
                "actions/runs/1002": {
                    "id": 1002, "run_attempt": 1, "event": "workflow_dispatch",
                    "path": authority.workflow, "display_title": title,
                    "status": "in_progress", "conclusion": None,
                },
            }

        class Response:
            status = 202
            def __enter__(self) -> "Response":
                return self
            def __exit__(self, *args: object) -> None:
                pass

        class Client:
            api_url = "https://api.github.com"
            repository = REPOSITORY
            token = "stub"
            def __init__(self, data: dict[str, object], terminal: str = "cancelled") -> None:
                self.data, self.terminal = data, terminal
                self.calls: list[str] = []
                self.response_status = 202
            def _read_json(self, path: str) -> dict[str, object]:
                self.calls.append("GET " + path)
                return copy.deepcopy(self.data[path])
            def pull_request(self, number: str) -> dict[str, object]:
                return self._read_json("pulls/" + number)
            def opener(self, request: urllib.request.Request, timeout: int) -> Response:
                self.calls.append(f"{request.get_method()} {request.full_url}")
                assert request.get_method() == "POST"
                assert request.full_url.endswith("/actions/runs/1002/cancel")
                assert timeout == 20
                self.data["actions/runs/1002"]["status"] = "completed"
                self.data["actions/runs/1002"]["conclusion"] = self.terminal
                response = Response()
                response.status = self.response_status
                return response

        original = runtime.preflight_orphan_codeql_cancel
        state = {"result": preflight, "calls": 0}
        def fake_preflight(**_: object) -> dict[str, object]:
            state["calls"] += 1
            return copy.deepcopy(state["result"])
        runtime.preflight_orphan_codeql_cancel = fake_preflight
        def invoke(client: Client, mode: str = "live") -> dict[str, object]:
            state["calls"] = 0
            return operate(
                client=client, authorities=[authority], old_caller_run_id=1001,
                trusted_main_sha=base, mode=mode, max_polls=1, poll_delay=0,
            )
        try:
            dry = Client(fixture())
            self.assertEqual("DRY_RUN_CANDIDATE", invoke(dry, "dry-run")["decision"])
            self.assertFalse(any(c.startswith("POST") for c in dry.calls))
            self.assertEqual(1, state["calls"])

            live = Client(fixture())
            self.assertEqual("CANCELLED_CONFIRMED", invoke(live)["decision"])
            self.assertEqual(2, state["calls"])
            self.assertEqual(1, len([c for c in live.calls if c.startswith("POST")]))
            self.assertTrue(live.calls[-1].startswith("GET actions/runs/1002"))

            bad_terminal = Client(fixture(), "success")
            with self.assertRaisesRegex(ContractError, "NOT cancelled"):
                invoke(bad_terminal)

            bad_status = Client(fixture())
            bad_status.response_status = 409
            with self.assertRaisesRegex(ContractError, "HTTP 409"):
                invoke(bad_status)

            for field, value in (
                ("pulls/42", ("head", {"sha": old_sha, "repo": dict(repo)})),
                ("actions/runs/1002", ("run_attempt", 2)),
                ("actions/runs/1002", ("status", "completed")),
                ("branches/main", ("commit", {"sha": "f" * 40})),
                ("actions/runs/2001", ("head_sha", old_sha)),
            ):
                data = fixture()
                key, changed = value
                data[field][key] = changed
                witness = Client(data)
                with self.subTest(field=field, key=key), self.assertRaises(ContractError):
                    invoke(witness)
                self.assertFalse(any(c.startswith("POST") for c in witness.calls))

            state["result"] = dict(preflight, dispatched_run_id=1003)
            with self.assertRaisesRegex(ContractError, "near-POST"):
                invoke(Client(fixture()))
            state["result"] = dict(preflight, decision="NO_WRITE", reason="already_completed")
            witness = Client(fixture())
            self.assertEqual("NO_WRITE", invoke(witness)["decision"])
            self.assertFalse(any(c.startswith("POST") for c in witness.calls))
            for mode in ("off", "force", "LIVE"):
                with self.assertRaises(ContractError):
                    invoke(Client(fixture()), mode)
        finally:
            runtime.preflight_orphan_codeql_cancel = original


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("execute", "self-test"))
    parser.add_argument("--policy", default=".github/merge-gate-policy.json")
    parser.add_argument("--mode", choices=("dry-run", "live"))
    parser.add_argument("--old-caller-run-id", type=int)
    parser.add_argument("--trusted-main-sha")
    args = parser.parse_args()
    if args.command == "self-test":
        result = unittest.TextTestRunner(verbosity=2).run(
            unittest.defaultTestLoader.loadTestsFromTestCase(CancelContractTests)
        )
        return 0 if result.wasSuccessful() else 1
    if (
        os.environ.get("GITHUB_EVENT_NAME") != "workflow_run"
        or os.environ.get("GITHUB_REF") != "refs/heads/main"
        or os.environ.get("GITHUB_REPOSITORY") != REPOSITORY
        or os.environ.get("GITHUB_RUN_ATTEMPT") != "1"
        or os.environ.get("GITHUB_SHA") != args.trusted_main_sha
        or not args.mode or not args.old_caller_run_id
        or (args.mode == "live" and (
            os.environ.get("ORPHAN_CODEQL_CANCEL_ENABLED") != "true"
            or os.environ.get("ORPHAN_CODEQL_CANCEL_MODE") != "live"
        ))
    ):
        raise ContractError("M5 opt-in trusted-main workflow_run execution boundary failed")
    client = runtime.GitHubActionsClient(
        api_url=os.environ.get("GITHUB_API_URL", ""),
        repository=REPOSITORY, token=os.environ.get("GH_TOKEN", ""),
    )
    result = operate(
        client=client, authorities=runtime.load_authorities(args.policy),
        old_caller_run_id=args.old_caller_run_id,
        trusted_main_sha=args.trusted_main_sha, mode=args.mode,
    )
    print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ContractError as exc:
        print(f"::error::{exc}", file=sys.stderr)
        raise SystemExit(1) from None
