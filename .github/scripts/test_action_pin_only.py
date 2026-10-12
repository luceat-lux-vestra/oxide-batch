#!/usr/bin/env python3
"""Deterministic negative tests for the non-authoritative pin-only helper."""

import unittest

from action_pin_only import ALLOWED_SITES, AdmissionError, Patch, classify, verify_release

OLD = "1" * 40
NEW = "2" * 40


def fixture(path, old_sha=OLD):
    sites = ALLOWED_SITES[path]
    lines = [f"# unchanged line {i}\n" for i in range(1, max(sites) + 2)]
    for number, action in sites.items():
        lines[number - 1] = f"        uses: {action}@{old_sha} # v7.0.1\n"
    return "".join(lines).encode()


def bump(path, old):
    return old.decode().replace(OLD + " # v7.0.1", NEW + " # v7.0.2").encode()


class PinOnlyTests(unittest.TestCase):
    def test_positive_both_workflows(self):
        for path in ALLOWED_SITES:
            with self.subTest(path=path):
                results = classify(path, fixture(path), bump(path, fixture(path)))
                self.assertTrue(results)
                self.assertEqual({p.new_sha for p in results}, {NEW})

    def test_non_action_change_rejected(self):
        path = ".github/workflows/codeql.yml"
        old = fixture(path)
        with self.assertRaises(AdmissionError):
            classify(path, old, bump(path, old).replace(b"# unchanged line 1", b"permissions: write-all"))

    def test_action_substitution_rejected(self):
        path = ".github/workflows/codeql.yml"
        old = fixture(path)
        with self.assertRaises(AdmissionError):
            classify(path, old, bump(path, old).replace(b"github/codeql-action/init@", b"attacker/codeql-action/init@"))

    def test_partial_update_rejected(self):
        path = ".github/workflows/codeql.yml"
        old = fixture(path)
        newer = bump(path, old).decode().splitlines(keepends=True)
        newer[148] = old.decode().splitlines(keepends=True)[148]
        with self.assertRaises(AdmissionError):
            classify(path, old, "".join(newer).encode())

    def test_same_sha_different_version_rejected(self):
        path = ".github/workflows/codeql.yml"
        old = fixture(path)
        with self.assertRaises(AdmissionError):
            classify(path, old, old.replace(b"# v7.0.1", b"# v7.0.2"))

    def test_short_sha_and_unsupported_path_rejected(self):
        path = ".github/workflows/codeql.yml"
        old = fixture(path)
        with self.assertRaises(AdmissionError):
            classify(path, old, bump(path, old).replace(NEW.encode(), b"v7.0.2"))
        with self.assertRaises(AdmissionError):
            classify(".github/workflows/untrusted.yml", old, bump(path, old))

    def test_minor_update_and_downgrade_rejected(self):
        path = ".github/workflows/codeql.yml"
        old = fixture(path)
        for version in (b"v7.1.0", b"v7.0.0"):
            with self.subTest(version=version), self.assertRaises(AdmissionError):
                classify(path, old, bump(path, old).replace(b"v7.0.2", version))

    def test_release_direct_and_annotated(self):
        patch = Patch("actions/upload-artifact", OLD, NEW, (7, 0, 1), (7, 0, 2))

        def upstream(path):
            if path.endswith("releases/tags/v7.0.2"):
                return {"draft": False, "prerelease": False, "tag_name": "v7.0.2"}
            if path.endswith("git/ref/tags/v7.0.2"):
                return {"ref": "refs/tags/v7.0.2", "object": {"type": "tag", "sha": "3" * 40}}
            if path.endswith("git/tags/" + "3" * 40):
                return {"object": {"type": "commit", "sha": NEW}}
            raise AssertionError(path)

        verify_release(patch, upstream)
        with self.assertRaises(AdmissionError):
            verify_release(patch, lambda _: {"tag_name": "v7.0.2"})

    def test_wrong_release_commit_rejected(self):
        patch = Patch("actions/upload-artifact", OLD, NEW, (7, 0, 1), (7, 0, 2))

        def fake(path):
            if "/releases/" in path:
                return {"draft": False, "prerelease": False, "tag_name": "v7.0.2"}
            return {"ref": "refs/tags/v7.0.2", "object": {"type": "commit", "sha": "4" * 40}}

        with self.assertRaises(AdmissionError):
            verify_release(patch, fake)


if __name__ == "__main__":
    unittest.main()
