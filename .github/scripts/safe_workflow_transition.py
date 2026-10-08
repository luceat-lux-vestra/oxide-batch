#!/usr/bin/env python3
"""Fail-closed, NON-AUTHORITATIVE prototype for one bounded workflow edit.

The future trusted-base caller MUST fetch PR metadata, complete changed-file
inventory, and both workflow blobs from GitHub, anchored to exact base/head
commits. Blob IDs must come independently from each corresponding Git tree.
This module alone NEVER proves check-run provenance or authorizes a merge.
"""

from __future__ import annotations

import hashlib
import re
import unittest
from pathlib import Path

WORKFLOW = ".github/workflows/pr-ci.yml"
SHA = re.compile(r"^[0-9a-f]{40}$")
JOB_TIMEOUT = re.compile(r"(?m)^    timeout-minutes: ([0-9]+)$")
POLL_TIMEOUT = re.compile(r"(?<!\S)--timeout-seconds ([0-9]+)(?=\s|$)")
# Constrained prototype, not an approved operational policy or a merge contract.
# Only paired configurations leave 120 seconds between the poll and job limit.
CANDIDATE_PROFILES = frozenset({(12, 600), (13, 660), (14, 720), (15, 780)})
CURRENT_TRUSTED_PROFILE = (15, 780)


class TransitionDenied(ValueError):
    """Candidate requires the existing independent full pre-admission route."""


def git_blob_id(value: str) -> str:
    if not isinstance(value, str):
        raise TransitionDenied("workflow contents must be UTF-8 text")
    encoded = value.encode("utf-8")
    return hashlib.sha1(b"blob " + str(len(encoded)).encode("ascii") + b"\0" + encoded).hexdigest()


def _identity(value: object, label: str) -> str:
    if not isinstance(value, str) or SHA.fullmatch(value) is None:
        raise TransitionDenied(f"{label}: invalid exact SHA")
    return value


def _proof_fields(text: str) -> tuple[str, tuple[int, int], str, str]:
    if not isinstance(text, str) or len(text.encode("utf-8")) > 131072:
        raise TransitionDenied("workflow text missing or oversized")
    marker = "  pr-proof:\n"
    if text.count(marker) != 1:
        raise TransitionDenied("missing or duplicate proof job")
    before, proof = text.split(marker, 1)
    if (
        proof.count("    name: pr-proof\n") != 1
        or proof.count("    runs-on: ubuntu-slim\n") != 1
        or proof.count("    needs: [scope, dispatch-authorities]\n") != 1
    ):
        raise TransitionDenied("unexpected trusted proof anchor")
    job = list(JOB_TIMEOUT.finditer(proof))
    poll = list(POLL_TIMEOUT.finditer(proof))
    if len(job) != 1 or len(poll) != 1:
        raise TransitionDenied("missing or duplicate proof budget")
    job_minutes = int(job[0].group(1))
    poll_seconds = int(poll[0].group(1))
    return before + marker, (job_minutes, poll_seconds), proof, (
        JOB_TIMEOUT.sub("    timeout-minutes: {job}", proof, count=1)
        .replace(poll[0].group(0), "--timeout-seconds {poll}", 1)
    )


def evaluate(
    *, repository: str, pr: dict, trusted_base_sha: str, expected_head_sha: str,
    files: list[dict], trusted_base_blob: str, expected_head_blob: str,
    base_text: str, head_text: str,
) -> dict:
    """Return an advisory safe-diff CANDIDATE, never an enforceable check."""
    base = _identity(trusted_base_sha, "base")
    head = _identity(expected_head_sha, "head")
    base_blob = _identity(trusted_base_blob, "base blob")
    head_blob = _identity(expected_head_blob, "head blob")
    if base == head or not isinstance(repository, str) or not re.fullmatch(
        r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository
    ):
        raise TransitionDenied("invalid independent repository/commit identity")
    if not isinstance(pr, dict):
        raise TransitionDenied("missing live PR metadata")
    if (
        type(pr.get("number")) is not int
        or pr.get("state") != "open"
        or pr.get("draft") is not False
        or pr.get("base", {}).get("sha") != base
        or pr.get("base", {}).get("ref") != "main"
        or pr.get("base", {}).get("repo", {}).get("full_name") != repository
        or pr.get("head", {}).get("sha") != head
        or pr.get("head", {}).get("repo", {}).get("full_name") != repository
    ):
        raise TransitionDenied("stale, untrusted or unsupported PR identity")
    if (
        type(pr.get("changed_files")) is not int
        or pr["changed_files"] != 1
        or not isinstance(files, list)
        or len(files) != 1
        or not isinstance(files[0], dict)
        or files[0].get("status") != "modified"
        or files[0].get("filename") != WORKFLOW
        or files[0].get("previous_filename") not in (None, "")
    ):
        raise TransitionDenied("incomplete, renamed, or noncanonical change inventory")
    if git_blob_id(base_text) != base_blob or git_blob_id(head_text) != head_blob:
        raise TransitionDenied("workflow blob not bound to independent Git tree")
    prefix, baseline, proof, placeholder = _proof_fields(base_text)
    if baseline != CURRENT_TRUSTED_PROFILE:
        raise TransitionDenied("unrecognized trusted-base profile")
    head_prefix, proposed, _, _ = _proof_fields(head_text)
    if head_prefix != prefix or proposed not in CANDIDATE_PROFILES or proposed == baseline:
        raise TransitionDenied("unauthorized proposed profile or trusted structure")
    expected = prefix + placeholder.format(job=proposed[0], poll=proposed[1])
    if head_text != expected:
        raise TransitionDenied("any other byte of workflow differs from trusted base")
    return {
        "classification": "SAFE_DIFF_CANDIDATE_ONLY",
        "merge_authorized": False,
        "checks_verified": False,
        "base_sha": base,
        "head_sha": head,
        "changed_path": WORKFLOW,
        "proposed_proof_budget": list(proposed),
    }


class SafeWorkflowTransitionTests(unittest.TestCase):
    BASE = "a" * 40
    HEAD = "b" * 40
    REPO = "owner/oxide-batch"

    @classmethod
    def setUpClass(cls) -> None:
        cls.original = (
            Path(__file__).resolve().parents[2] / WORKFLOW
        ).read_text(encoding="utf-8")
        _, baseline, _, _ = _proof_fields(cls.original)
        if baseline != CURRENT_TRUSTED_PROFILE:
            raise AssertionError("test fixture does not match actual active PR-CI contract")

    @classmethod
    def proposed(cls, job: int = 14, poll: int = 720) -> str:
        prefix, _, _, placeholder = _proof_fields(cls.original)
        return prefix + placeholder.format(job=job, poll=poll)

    def data(self, *, new_text: str | None = None) -> dict:
        candidate = self.proposed() if new_text is None else new_text
        return {
            "repository": self.REPO,
            "pr": {
                "number": 42, "state": "open", "draft": False, "changed_files": 1,
                "base": {"sha": self.BASE, "ref": "main", "repo": {"full_name": self.REPO}},
                "head": {"sha": self.HEAD, "repo": {"full_name": self.REPO}},
            },
            "trusted_base_sha": self.BASE, "expected_head_sha": self.HEAD,
            "files": [{"filename": WORKFLOW, "status": "modified"}],
            "trusted_base_blob": git_blob_id(self.original),
            "expected_head_blob": git_blob_id(candidate),
            "base_text": self.original, "head_text": candidate,
        }

    def denied(self, **changes) -> None:
        args = self.data()
        args.update(changes)
        with self.assertRaises(TransitionDenied):
            evaluate(**args)

    def test_safe_candidate_is_not_merge_authority(self):
        for minutes, seconds in ((14, 720), (12, 600)):
            with self.subTest(minutes=minutes):
                output = evaluate(**self.data(new_text=self.proposed(minutes, seconds)))
                self.assertEqual(output["classification"], "SAFE_DIFF_CANDIDATE_ONLY")
                self.assertFalse(output["merge_authorized"])
                self.assertFalse(output["checks_verified"])

    def test_forged_green_check_is_never_proof(self):
        args = self.data()
        args["pr"]["checks"] = [{"name": "merge-gate", "conclusion": "success"}]
        result = evaluate(**args)
        self.assertFalse(result["merge_authorized"])
        self.assertFalse(result["checks_verified"])

    def test_workflow_security_and_topology_mutations_denied(self):
        for old, new in (
            ("permissions:\n  contents: read", "permissions:\n  contents: write"),
            ("  pull_request:\n", "  pull_request_target:\n"),
            ("    runs-on: ubuntu-slim", "    runs-on: self-hosted"),
            ("    name: pr-proof", "    name: spoofed-merge-gate"),
            ("    needs: [scope, dispatch-authorities]", "    needs: [scope]"),
            ("persist-credentials: false", "persist-credentials: true"),
            ("--poll-interval-seconds 5", "--poll-interval-seconds 1"),
        ):
            with self.subTest(old=old):
                self.assertIn(old, self.original)
                changed = self.proposed().replace(old, new, 1)
                self.denied(head_text=changed, expected_head_blob=git_blob_id(changed))

    def test_extra_action_or_job_is_denied(self):
        for suffix in ("\n  unsafe:\n    runs-on: ubuntu-latest\n",
                       "\n# arbitrary new policy line\n"):
            with self.subTest(suffix=suffix):
                changed = self.proposed() + suffix
                self.denied(head_text=changed, expected_head_blob=git_blob_id(changed))

    def test_invalid_budget_profiles_denied(self):
        for minutes, seconds in ((16, 840), (14, 780), (14, 721), (9, 420), (15, 780)):
            with self.subTest(profile=(minutes, seconds)):
                changed = self.proposed(minutes, seconds)
                self.denied(head_text=changed, expected_head_blob=git_blob_id(changed))

    def test_altered_base_content_with_stale_tree_blob_denied(self):
        poisoned = self.original.replace("    runs-on: ubuntu-slim", "    runs-on: self-hosted", 1)
        self.denied(base_text=poisoned)

    def test_unexpected_security_policy_or_core_change_denied(self):
        for path in (".github/merge-gate-policy.json", "crates/oxide-batch/src/repository.rs",
                     ".github/scripts/verify-merge-gates.rb"):
            with self.subTest(path=path):
                args = self.data()
                args["files"].append({"filename": path, "status": "modified"})
                args["pr"]["changed_files"] = 2
                with self.assertRaises(TransitionDenied):
                    evaluate(**args)

    def test_rename_delete_copy_or_add_denied(self):
        for status in ("renamed", "removed", "copied", "added"):
            with self.subTest(status=status):
                args = self.data()
                args["files"][0]["status"] = status
                args["files"][0]["previous_filename"] = ".github/workflows/other.yml"
                with self.assertRaises(TransitionDenied):
                    evaluate(**args)

    def test_missing_or_wrong_blob_identity_denied(self):
        self.denied(trusted_base_blob="c" * 40)
        self.denied(expected_head_blob="c" * 40)
        self.denied(expected_head_blob="not-a-sha")

    def test_stale_base_head_repository_and_state_denied(self):
        for field, value in (("state", "closed"), ("draft", True),
                             ("changed_files", 2)):
            with self.subTest(field=field):
                args = self.data()
                args["pr"][field] = value
                with self.assertRaises(TransitionDenied):
                    evaluate(**args)
        for path, value in (
            (("head", "sha"), "c" * 40),
            (("base", "sha"), "c" * 40),
            (("base", "ref"), "other"),
            (("base", "repo", "full_name"), "attacker/repo"),
            (("head", "repo", "full_name"), "attacker/repo"),
        ):
            with self.subTest(path=path):
                args = self.data()
                obj = args["pr"]
                for key in path[:-1]:
                    obj = obj[key]
                obj[path[-1]] = value
                with self.assertRaises(TransitionDenied):
                    evaluate(**args)
        self.denied(expected_head_sha="c" * 40)

    def test_missing_or_duplicate_budget_is_denied(self):
        for replaced in (
            self.proposed().replace("--timeout-seconds 720", "--poll-deadline 720", 1),
            self.proposed() + "\n    timeout-minutes: 14\n",
        ):
            with self.subTest(replaced=replaced[-40:]):
                self.denied(head_text=replaced, expected_head_blob=git_blob_id(replaced))
