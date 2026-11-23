#!/usr/bin/env python3
"""Negative controls for the dependency-update policy registry."""

from __future__ import annotations

import copy
import datetime as dt
import importlib.util
import tempfile
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).with_name("validate_dependency_update_policy.py")
SPEC = importlib.util.spec_from_file_location("dependency_update_policy", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
policy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(policy)


def valid_document() -> dict:
    return {
        "schema": "oxide-batch/dependency-update-policy/v1",
        "policy": "EG-DEP-001",
        "owner": "oxide-batch#718",
        "reviewed_at": "2026-11-24",
        "entries": [
            {
                "id": "fixture",
                "source": "test fixture",
                "source_paths": ["tracked.txt"],
                "strategy": "REVIEWED_FREEZE",
                "authority": "upstream releases",
                "mechanism": "verified immutable artifact",
                "owner": "oxide-batch#718",
                "validation": "test gate",
                "provenance": {
                    "version": "1.2.3",
                    "artifacts": {
                        "linux": {
                            "url": "https://example.invalid/tool-1.2.3.tar.gz",
                            "sha256": "a" * 64,
                        }
                    },
                },
                "revisit": {
                    "review_by": "2027-02-22",
                    "rationale": "bounded compatibility evidence",
                    "evidence": "fixture evidence",
                    "trigger": "new stable release",
                },
            }
        ],
    }


class DependencyUpdatePolicyTests(unittest.TestCase):
    def validate(self, document: dict, today: str = "2026-11-24") -> list[str]:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "tracked.txt").write_text("fixture")
            return policy.validate_document(
                document, root, dt.date.fromisoformat(today)
            )

    def test_valid_finite_freeze_passes(self) -> None:
        self.assertEqual(self.validate(valid_document()), [])

    def test_resolves_structured_provenance(self) -> None:
        document = valid_document()
        self.assertEqual(policy.resolve_value(document, "fixture", "version"), "1.2.3")
        self.assertEqual(
            policy.resolve_value(document, "fixture", "sha256", artifact="linux"),
            "a" * 64,
        )

    def test_expired_freeze_fails(self) -> None:
        errors = self.validate(valid_document(), today="2027-02-23")
        self.assertTrue(any("expired" in error for error in errors), errors)

    def test_freeze_longer_than_ninety_days_fails(self) -> None:
        document = valid_document()
        document["entries"][0]["revisit"]["review_by"] = "2027-02-23"
        errors = self.validate(document)
        self.assertTrue(any("maximum is 90" in error for error in errors), errors)

    def test_future_review_date_fails(self) -> None:
        document = valid_document()
        document["reviewed_at"] = "2026-11-25"
        errors = self.validate(document)
        self.assertTrue(any("cannot be in the future" in error for error in errors), errors)

    def test_missing_source_path_fails(self) -> None:
        document = valid_document()
        document["entries"][0]["source_paths"] = ["missing.txt"]
        errors = self.validate(document)
        self.assertTrue(any("does not exist" in error for error in errors), errors)

    def test_duplicate_entry_id_fails(self) -> None:
        document = valid_document()
        document["entries"].append(copy.deepcopy(document["entries"][0]))
        errors = self.validate(document)
        self.assertTrue(any("duplicates" in error for error in errors), errors)

    def test_invalid_artifact_checksum_fails(self) -> None:
        document = valid_document()
        document["entries"][0]["provenance"]["artifacts"]["linux"]["sha256"] = "bad"
        errors = self.validate(document)
        self.assertTrue(any("64 lowercase hex" in error for error in errors), errors)


if __name__ == "__main__":
    unittest.main()
