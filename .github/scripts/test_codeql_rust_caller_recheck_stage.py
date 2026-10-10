#!/usr/bin/env python3
"""Adversarial proof for the staged, fail-closed CodeQL caller recheck."""
from __future__ import annotations

import hashlib
import io
import json
import os
import textwrap
import time
import unittest
import urllib.request
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
ACTIVE = ROOT / ".github/workflows/codeql.yml"
STAGED = ROOT / "docs/engineering/ci-staging/codeql-rust-caller-recheck-candidate.yml"
POLICY = ROOT / ".github/merge-gate-policy.json"
FUTURE_BLOB = "af148fc7f0cee653e2f29e017e2fa73c928c910e"
REPO = "luceat-lux-vestra/oxide-batch"
BASE = "c888bc0d5c238ce68a745c1fc62c6b0b7207f1c1"
HEAD = "a" * 40
RUN_ID = 123456

def embedded(text: str) -> str:
    lines = text.splitlines(keepends=True)
    begin = next(i for i, s in enumerate(lines) if s.startswith("          def trusted_dispatch_identity():"))
    finish = next(i for i in range(begin + 1, len(lines)) if lines[i].startswith("          def fallback(reason):"))
    return textwrap.dedent("".join(lines[begin:finish]))

def without_caller(text: str) -> str:
    lines = text.splitlines(keepends=True)
    begin = next(i for i, s in enumerate(lines) if s.startswith("          def trusted_dispatch_identity():"))
    finish = next(i for i in range(begin + 1, len(lines)) if lines[i].startswith("          def fallback(reason):"))
    return ("".join(lines[:begin]) + "          # <<caller provenance isolated>>\n"
            + "".join(lines[finish:])).replace("          import time\n", "", 1)

class CallerRecheckContract(unittest.TestCase):
    def setUp(self):
        self.source = STAGED.read_text(encoding="utf-8")
        self.requests = []
        self.sleeps = []
        self.env = {
            "GITHUB_EVENT_NAME": "workflow_dispatch",
            "GITHUB_REF": "refs/heads/main",
            "GITHUB_SHA": BASE,
            "HEAD_REPO": REPO,
            "CALLER_RUN_ATTEMPT": "1",
            "CALLER_RUN_ID": str(RUN_ID),
            "HEAD_SHA": HEAD,
        }
        self.valid = {"id": RUN_ID, "event": "pull_request",
                      "run_attempt": 1, "path": ".github/workflows/pr-ci.yml",
                      "head_sha": HEAD, "status": "in_progress", "conclusion": None}
        namespace = {"os": os, "json": json, "time": time, "urllib": urllib,
                     "base_sha": BASE, "repository": REPO, "token": "offline-test-token",
                     "api_url": "https://api.invalid"}
        exec(compile(embedded(self.source), "<staged CodeQL caller>", "exec"), namespace)
        self.verify = namespace["trusted_dispatch_identity"]

    def call(self, records, *, env=None):
        replies = iter(records)
        def fake_urlopen(request, timeout):
            self.requests.append((request.full_url, timeout))
            self.assertEqual(request.full_url,
                             f"https://api.invalid/repos/{REPO}/actions/runs/{RUN_ID}")
            self.assertEqual(timeout, 20)
            return io.BytesIO(json.dumps(next(replies)).encode())
        with mock.patch.dict(os.environ, {**self.env, **(env or {})}, clear=True):
            with mock.patch.object(urllib.request, "urlopen", side_effect=fake_urlopen):
                with mock.patch.object(time, "sleep", side_effect=self.sleeps.append):
                    self.verify()

    def test_exact_stage_delta_and_preapproval(self):
        actual = self.source.encode()
        sha = hashlib.sha1(f"blob {len(actual)}\\0".encode().replace(b"\\0", bytes([0])) + actual).hexdigest()
        self.assertEqual(sha, FUTURE_BLOB)
        active = ACTIVE.read_text(encoding="utf-8")
        self.assertEqual(without_caller(active), without_caller(self.source))
        data = json.loads(POLICY.read_text(encoding="utf-8"))
        accepted = next(x["accepted_blobs"] for x in data["repository_merge_gate"]["protected_workflows"]
                        if x["workflow"] == ".github/workflows/codeql.yml")
        self.assertIn(FUTURE_BLOB, accepted)
        self.assertIn("894db7fb531721bbb961ce59231e08b9331b7444", accepted)
        self.assertIn('caller.get("head_sha") != os.environ["HEAD_SHA"]', self.source)
        self.assertIn('caller.get("run_attempt") != 1', self.source)
        self.assertIn('caller.get("event") != "pull_request"', self.source)
        self.assertIn('caller.get("status") not in {"in_progress", "completed"}', self.source)
        self.assertIn('for attempt in range(3):', self.source)
        self.assertNotIn('status") not in {"queued",', self.source)

    def test_valid_caller_passes_without_extra_get(self):
        self.call([self.valid])
        self.assertEqual(len(self.requests), 1)
        self.assertEqual(self.sleeps, [])

    def test_delayed_ready_caller_rechecks_then_passes(self):
        queued = {**self.valid, "status": "queued"}
        self.call([queued, self.valid])
        self.assertEqual(len(self.requests), 2)
        self.assertEqual(self.sleeps, [1])

    def test_completed_success_is_valid(self):
        self.call([{**self.valid, "status": "completed", "conclusion": "success"}])
        self.assertEqual(len(self.requests), 1)

    def test_unresolved_queued_remains_fail_closed(self):
        queued = {**self.valid, "status": "queued"}
        with self.assertRaisesRegex(RuntimeError, "after 3 read-only checks"):
            self.call([queued] * 3)
        self.assertEqual(len(self.requests), 3)
        self.assertEqual(self.sleeps, [1, 1])

    def test_mismatched_identity_never_authorized(self):
        for field, value in [("id", 8), ("event", "workflow_dispatch"),
                             ("run_attempt", 2), ("path", ".github/workflows/other.yml"),
                             ("head_sha", "b"*40), ("status", "cancelled")]:
            with self.subTest(field=field):
                self.requests.clear()
                self.sleeps.clear()
                invalid = {**self.valid, field: value}
                with self.assertRaises(RuntimeError):
                    self.call([invalid] * 3)
                self.assertEqual(len(self.requests), 3)

    def test_failed_terminal_caller_never_authorized(self):
        invalid = {**self.valid, "status": "completed", "conclusion": "failure"}
        with self.assertRaises(RuntimeError):
            self.call([invalid] * 3)
        self.assertEqual(len(self.requests), 3)

    def test_wrong_trust_context_rejected_before_network(self):
        bad = [("GITHUB_REF", "refs/heads/feature"),
               ("GITHUB_SHA", "b" * 40),
               ("HEAD_REPO", "other/repo"),
               ("CALLER_RUN_ATTEMPT", "2"),
               ("CALLER_RUN_ID", "0")]
        for field, value in bad:
            with self.subTest(field=field):
                self.requests.clear()
                with self.assertRaises(RuntimeError):
                    self.call([], env={field: value})
                self.assertEqual(self.requests, [])

def run_checks():
    result = unittest.TextTestRunner(verbosity=1).run(
        unittest.defaultTestLoader.loadTestsFromTestCase(CallerRecheckContract))
    if not result.wasSuccessful():
        raise AssertionError("staged CodeQL caller retry regression tests failed")

if __name__ == "__main__":
    run_checks()
