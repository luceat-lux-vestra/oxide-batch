#!/usr/bin/env python3
"""Exercise the deployed inline protected-base gate's real trusted-pin functions.

Extracts the embedded code from pr-labeler.yml, compiles its actual function
definitions, and supplies mocked GitHub API responses. Tests are hermetic and
execute no PR-head code from any checkout.
"""
import ast
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
import urllib.parse

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / ".github/workflows/pr-labeler.yml"
HELPER = ROOT / ".github/scripts/action_pin_only.py"
CODEQL = ".github/workflows/codeql.yml"
OLD_SHA = "24c54180a607b1449ed407dd24f251e4e9147c8d"
NEW_SHA = "2" * 40
BASE_SHA = "a" * 40
HEAD_SHA = "b" * 40


class GateError(RuntimeError):
    pass


def blob_sha(data):
    return hashlib.sha1(b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()


def inline_functions():
    lines = SCRIPT.read_text().splitlines()
    begin = next(i for i, row in enumerate(lines) if row.strip() == "python3 - <<'PY'")
    end = next(i for i in range(begin + 1, len(lines)) if lines[i].strip() == "PY")
    code = "\n".join(row[10:] for row in lines[begin + 1:end])
    tree = ast.parse(code)
    names = {"trusted_content", "trusted_pin_only"}
    constants = {"PIN_ONLY_HELPER_BLOB", "PIN_ONLY_WORKFLOWS", "pin_only_cache"}
    nodes = [
        node for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name in names
        or isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id in constants for target in node.targets)
    ]
    if {node.name for node in nodes if isinstance(node, ast.FunctionDef)} != names:
        raise AssertionError("protected gate functions are missing")
    return compile(ast.Module(body=nodes, type_ignores=[]), str(SCRIPT), "exec")


class InlineGateTests(unittest.TestCase):
    def setUp(self):
        self.workflow = (ROOT / CODEQL).read_bytes()
        self.helper = HELPER.read_bytes()
        self.files = {
            (".github/scripts/action_pin_only.py", BASE_SHA): self.helper,
            (CODEQL, BASE_SHA): self.workflow,
            (CODEQL, HEAD_SHA): self.workflow,
        }
        self.calls = []
        self.exit_code = 0
        self.verdict = {"classification": "patch-only", "workflow": CODEQL, "verified": 1}
        self.env = {
            "GateError": GateError,
            "api": self.fake_api,
            "base_sha": BASE_SHA,
            "head_sha": HEAD_SHA,
            "token": "fake-token",
            "base64": base64,
            "hashlib": hashlib,
            "urllib": urllib,
            "subprocess": SimpleNamespace(run=self.fake_subprocess),
            "tempfile": tempfile,
            "os": os,
            "json": json,
        }
        exec(inline_functions(), self.env)

    def fake_api(self, endpoint, params):
        if not endpoint.startswith("contents/") or not isinstance(params, dict):
            raise AssertionError(f"unexpected API request: {endpoint}")
        key = (urllib.parse.unquote(endpoint.removeprefix("contents/")), params.get("ref"))
        content = self.files[key]
        return {
            "type": "file",
            "encoding": "base64",
            "sha": blob_sha(content),
            "content": base64.b64encode(content).decode(),
        }

    def fake_subprocess(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        self.assertEqual(argv[0], "python3")
        self.assertEqual(argv[2:4], ["--workflow", CODEQL])
        self.assertEqual(kwargs["env"]["GH_TOKEN"], "fake-token")
        self.assertEqual(Path(argv[1]).read_bytes(), self.helper)
        self.assertEqual(Path(argv[5]).read_bytes(), self.workflow)
        self.assertEqual(Path(argv[7]).read_bytes(), self.files[(CODEQL, HEAD_SHA)])
        return SimpleNamespace(
            returncode=self.exit_code,
            stderr="upstream rejected",
            stdout=json.dumps(self.verdict),
        )

    def changed_workflow(self):
        needle = (OLD_SHA + " # v4.38.3").encode()
        self.assertEqual(self.workflow.count(needle), 4)
        self.files[(CODEQL, HEAD_SHA)] = self.workflow.replace(
            needle, (NEW_SHA + " # v4.38.4").encode()
        )

    def test_real_inline_gate_accepts_verified_patch_evidence(self):
        self.changed_workflow()
        self.assertTrue(self.env["trusted_pin_only"](CODEQL))
        self.assertEqual(len(self.calls), 1)
        self.assertTrue(self.env["trusted_pin_only"](CODEQL))
        self.assertEqual(len(self.calls), 1, "same exact-head result must be cached")

    def test_real_inline_gate_accepts_unchanged_protected_baseline(self):
        self.assertTrue(self.env["trusted_pin_only"](CODEQL))
        self.assertEqual(self.calls, [])

    def test_privileged_merge_gate_is_never_pin_only(self):
        self.assertFalse(self.env["trusted_pin_only"](".github/workflows/pr-labeler.yml"))

    def test_changed_helper_blob_fails_closed(self):
        self.changed_workflow()
        self.files[(".github/scripts/action_pin_only.py", BASE_SHA)] += b"\n# malicious\n"
        with self.assertRaises(GateError):
            self.env["trusted_pin_only"](CODEQL)

    def test_trusted_file_content_blob_mismatch_fails_closed(self):
        self.changed_workflow()
        self.env["api"] = lambda endpoint, params: {
            **self.fake_api(endpoint, params),
            "sha": "f" * 40,
        }
        with self.assertRaises(GateError):
            self.env["trusted_pin_only"](CODEQL)

    def test_release_verifier_process_failure_denied(self):
        self.changed_workflow()
        self.exit_code = 1
        with self.assertRaises(GateError):
            self.env["trusted_pin_only"](CODEQL)

    def test_forged_positive_verdict_denied(self):
        self.changed_workflow()
        for bad in (
            {"classification": "patch-only", "workflow": ".github/workflows/ci.yml", "verified": 1},
            {"classification": "patch-only", "workflow": CODEQL, "verified": 0},
            {"classification": "unverified", "workflow": CODEQL, "verified": 1},
        ):
            with self.subTest(bad=bad):
                self.verdict = bad
                with self.assertRaises(GateError):
                    self.env["trusted_pin_only"](CODEQL)

    def test_invalid_base64_from_api_denied(self):
        self.changed_workflow()
        self.env["api"] = lambda endpoint, params: {
            **self.fake_api(endpoint, params),
            "content": "%%INVALID%%",
        }
        with self.assertRaises(GateError):
            self.env["trusted_pin_only"](CODEQL)


if __name__ == "__main__":
    unittest.main()
