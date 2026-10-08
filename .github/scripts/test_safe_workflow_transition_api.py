#!/usr/bin/env python3
"""Mocked adversarial contract tests; no GitHub writes or external API calls."""

from __future__ import annotations

import base64
import copy
import unittest
from pathlib import Path

from safe_workflow_transition import (
    WORKFLOW, TransitionDenied, _proof_fields, git_blob_id,
)
from safe_workflow_transition_api import inspect

REPO = "owner/oxide-batch"
BASE = "a" * 40
HEAD = "b" * 40
ROOT_BASE, DOT_BASE, WORK_BASE = ("1" * 40, "2" * 40, "3" * 40)
ROOT_HEAD, DOT_HEAD, WORK_HEAD = ("4" * 40, "5" * 40, "6" * 40)


class FakeGitHub:
    repository = REPO

    def __init__(self, responses: dict):
        self.responses = responses
        self.calls = []
        self.pr_reads = 0
        self.main_reads = 0
        self.next_pr = None
        self.next_main = None

    def get(self, route: str):
        self.calls.append(route)
        if route == "pulls/42":
            self.pr_reads += 1
            if self.pr_reads == 2 and self.next_pr is not None:
                return copy.deepcopy(self.next_pr)
        if route == "branches/main":
            self.main_reads += 1
            if self.main_reads == 2 and self.next_main is not None:
                return copy.deepcopy(self.next_main)
        if route not in self.responses:
            raise TransitionDenied("mocked GitHub API read failed")
        return copy.deepcopy(self.responses[route])


def entry(path, sha, typ, mode):
    return {"path": path, "sha": sha, "type": typ, "mode": mode}


def tree(sha, node):
    return {"sha": sha, "truncated": False, "tree": [node]}


def blob(value):
    encoded = value.encode("utf-8")
    return {
        "sha": git_blob_id(value), "encoding": "base64",
        "size": len(encoded), "content": base64.b64encode(encoded).decode("ascii"),
    }


class SafeWorkflowTransitionApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_text = (Path(__file__).resolve().parents[2] / WORKFLOW).read_text(
            encoding="utf-8"
        )
        prefix, profile, _, template = _proof_fields(cls.base_text)
        if profile != (15, 780):
            raise AssertionError("trusted workflow fixture drift")
        cls.head_text = prefix + template.format(job=14, poll=720)

    def fixture(self):
        base_blob = blob(self.base_text)
        head_blob = blob(self.head_text)
        pr = {
            "number": 42, "state": "open", "draft": False, "merged": False,
            "changed_files": 1,
            "base": {"sha": BASE, "ref": "main", "repo": {"full_name": REPO}},
            "head": {"sha": HEAD, "repo": {"full_name": REPO}},
        }
        data = {
            "pulls/42": pr,
            "branches/main": {"commit": {"sha": BASE}},
            "pulls/42/files?per_page=100&page=1": [
                {"filename": WORKFLOW, "status": "modified", "sha": head_blob["sha"]}
            ],
            f"git/commits/{BASE}": {"sha": BASE, "tree": {"sha": ROOT_BASE}},
            f"git/commits/{HEAD}": {"sha": HEAD, "tree": {"sha": ROOT_HEAD}},
            f"git/trees/{ROOT_BASE}": tree(
                ROOT_BASE, entry(".github", DOT_BASE, "tree", "040000")
            ),
            f"git/trees/{DOT_BASE}": tree(
                DOT_BASE, entry("workflows", WORK_BASE, "tree", "040000")
            ),
            f"git/trees/{WORK_BASE}": tree(
                WORK_BASE, entry("pr-ci.yml", base_blob["sha"], "blob", "100644")
            ),
            f"git/trees/{ROOT_HEAD}": tree(
                ROOT_HEAD, entry(".github", DOT_HEAD, "tree", "040000")
            ),
            f"git/trees/{DOT_HEAD}": tree(
                DOT_HEAD, entry("workflows", WORK_HEAD, "tree", "040000")
            ),
            f"git/trees/{WORK_HEAD}": tree(
                WORK_HEAD, entry("pr-ci.yml", head_blob["sha"], "blob", "100644")
            ),
            f"git/blobs/{base_blob['sha']}": base_blob,
            f"git/blobs/{head_blob['sha']}": head_blob,
        }
        return FakeGitHub(data)

    def run_inspection(self, api):
        return inspect(api, number=42, trusted_base=BASE, expected_head=HEAD)

    def test_exact_tree_and_blob_candidate_still_not_authority(self):
        api = self.fixture()
        result = self.run_inspection(api)
        self.assertEqual(result["classification"], "SAFE_DIFF_CANDIDATE_ONLY")
        self.assertFalse(result["merge_authorized"])
        self.assertFalse(result["checks_verified"])
        self.assertTrue(result["api_identity_rechecked"])
        self.assertEqual(api.pr_reads, 2)
        self.assertEqual(api.main_reads, 2)
        self.assertIn(f"git/blobs/{git_blob_id(self.head_text)}", api.calls)

    def test_head_changed_during_git_inspection_denied(self):
        api = self.fixture()
        api.next_pr = api.responses["pulls/42"]
        api.next_pr["head"]["sha"] = "c" * 40
        with self.assertRaises(TransitionDenied):
            self.run_inspection(api)

    def test_main_advanced_during_inspection_denied(self):
        api = self.fixture()
        api.next_main = {"commit": {"sha": "c" * 40}}
        with self.assertRaises(TransitionDenied):
            self.run_inspection(api)

    def test_initial_stale_base_and_head_denied(self):
        for key, sha in (("base", "c" * 40), ("head", "d" * 40)):
            with self.subTest(key=key):
                api = self.fixture()
                api.responses["pulls/42"][key]["sha"] = sha
                with self.assertRaises(TransitionDenied):
                    self.run_inspection(api)

    def test_forged_green_cannot_authorize(self):
        api = self.fixture()
        api.responses["pulls/42"]["checks"] = [
            {"name": "merge-gate", "conclusion": "success"}
        ]
        result = self.run_inspection(api)
        self.assertFalse(result["merge_authorized"])

    def test_injected_core_or_policy_files_denied(self):
        for path in (".github/merge-gate-policy.json", "crates/core/src/lib.rs"):
            with self.subTest(path=path):
                api = self.fixture()
                api.responses["pulls/42"]["changed_files"] = 2
                api.responses["pulls/42/files?per_page=100&page=1"].append(
                    {"filename": path, "status": "modified"}
                )
                with self.assertRaises(TransitionDenied):
                    self.run_inspection(api)

    def test_incomplete_pagination_denied(self):
        api = self.fixture()
        api.responses["pulls/42"]["changed_files"] = 2
        with self.assertRaises(TransitionDenied):
            self.run_inspection(api)

    def test_full_page_requires_next_page_even_for_rejected_large_diff(self):
        api = self.fixture()
        api.responses["pulls/42"]["changed_files"] = 101
        page = [
            {"filename": f"docs/entry-{index}.md", "status": "modified"}
            for index in range(100)
        ]
        api.responses["pulls/42/files?per_page=100&page=1"] = page
        api.responses["pulls/42/files?per_page=100&page=2"] = [
            {"filename": WORKFLOW, "status": "modified",
             "sha": git_blob_id(self.head_text)}
        ]
        with self.assertRaises(TransitionDenied):
            self.run_inspection(api)
        self.assertIn("pulls/42/files?per_page=100&page=2", api.calls)

    def test_duplicate_file_records_denied(self):
        api = self.fixture()
        api.responses["pulls/42"]["changed_files"] = 2
        api.responses["pulls/42/files?per_page=100&page=1"] *= 2
        with self.assertRaises(TransitionDenied):
            self.run_inspection(api)

    def test_truncated_tree_denied(self):
        api = self.fixture()
        api.responses[f"git/trees/{ROOT_HEAD}"]["truncated"] = True
        with self.assertRaises(TransitionDenied):
            self.run_inspection(api)

    def test_symlink_or_submodule_denied(self):
        for typ, mode in (("blob", "120000"), ("commit", "160000")):
            with self.subTest(typ=typ):
                api = self.fixture()
                node = api.responses[f"git/trees/{WORK_HEAD}"]["tree"][0]
                node["type"] = typ
                node["mode"] = mode
                with self.assertRaises(TransitionDenied):
                    self.run_inspection(api)

    def test_mutated_git_blob_denied_even_if_api_claims_right_sha(self):
        api = self.fixture()
        target = api.responses[f"git/blobs/{git_blob_id(self.head_text)}"]
        target["content"] = blob(self.head_text + "\n")["content"]
        with self.assertRaises(TransitionDenied):
            self.run_inspection(api)

    def test_wrong_git_commit_tree_and_files_sha_denied(self):
        for mutation in ("commit", "tree", "files"):
            with self.subTest(mutation=mutation):
                api = self.fixture()
                if mutation == "commit":
                    api.responses[f"git/commits/{HEAD}"]["sha"] = "f" * 40
                elif mutation == "tree":
                    api.responses[f"git/trees/{ROOT_HEAD}"]["sha"] = "f" * 40
                else:
                    api.responses["pulls/42/files?per_page=100&page=1"][0]["sha"] = "f" * 40
                with self.assertRaises(TransitionDenied):
                    self.run_inspection(api)

    def test_invalid_base64_and_oversized_blob_denied(self):
        for mutation in ("base64", "size"):
            with self.subTest(mutation=mutation):
                api = self.fixture()
                target = api.responses[f"git/blobs/{git_blob_id(self.head_text)}"]
                if mutation == "base64":
                    target["content"] = "not-base64!!!"
                else:
                    target["size"] = 131073
                with self.assertRaises(TransitionDenied):
                    self.run_inspection(api)

    def test_missing_required_api_data_denied(self):
        for route in (
            f"git/commits/{BASE}", f"git/trees/{WORK_HEAD}",
            f"git/blobs/{git_blob_id(self.head_text)}",
        ):
            with self.subTest(route=route):
                api = self.fixture()
                del api.responses[route]
                with self.assertRaises(TransitionDenied):
                    self.run_inspection(api)
